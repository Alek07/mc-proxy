use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const STATUS_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();

    let backend_addr = std::env::var("BACKEND_ADDR").expect("no BACKEND_ADDR in env");
    let port = std::env::var("PORT").unwrap_or_else(|_| "25565".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    println!("Starting proxy on {}", bind_addr);
    println!("Backend: {}", backend_addr);

    let listener = TcpListener::bind(&bind_addr).await?;
    let active = Arc::new(AtomicUsize::new(0));

    // Keepalive: only pings the backend while real players are connected
    {
        let active = active.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            loop {
                let count = active.load(Ordering::Acquire);
                if count > 0 {
                    println!("[keepalive] active={}, pinging backend...", count);

                    match TcpStream::connect(&backend_addr).await {
                        Ok(_) => println!("[keepalive] ping success"),
                        Err(e) => println!("[keepalive] ping failed: {}", e),
                    }
                }

                sleep(Duration::from_secs(60)).await;
            }
        });
    }

    loop {
        let (client, addr) = listener.accept().await?;
        println!("[conn] new client: {}", addr);

        let active = active.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(client, &backend_addr, active).await {
                println!("[conn] {}: {}", addr, e);
            }
        });
    }
}

struct Handshake {
    /// Raw bytes of the full handshake packet (length prefix included),
    /// so we can replay it to the backend verbatim on login.
    raw: Vec<u8>,
    protocol: i32,
    next_state: i32,
}

async fn handle_connection(
    mut client: TcpStream,
    backend_addr: &str,
    active: Arc<AtomicUsize>,
) -> io::Result<()> {
    let hs = timeout(HANDSHAKE_TIMEOUT, read_handshake(&mut client))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handshake timeout"))??;

    match hs.next_state {
        // Status ping (server list / Modrinth launcher polling)
        1 => {
            if active.load(Ordering::Acquire) > 0 {
                // Players are online, so the backend is awake anyway:
                // forward the status request so friends see the real MOTD/player count.
                println!("[conn] status ping — players online, forwarding to backend");
                let mut server = TcpStream::connect(backend_addr).await?;
                server.write_all(&hs.raw).await?;
                let _ = io::copy_bidirectional(&mut client, &mut server).await;
            } else {
                // Backend is (probably) asleep. Answer locally, never touch it.
                println!("[conn] status ping — answered locally, backend untouched");
                timeout(STATUS_TIMEOUT, handle_status_locally(&mut client, hs.protocol))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status timeout"))??;
            }
            Ok(())
        }

        // Login (2) or transfer (3, MC 1.20.5+): a real player joining -> wake the backend
        2 | 3 => {
            let current = active.fetch_add(1, Ordering::AcqRel) + 1;
            println!("[conn] login attempt, active connections: {}", current);

            let result = proxy_login(&mut client, backend_addr, &hs.raw).await;

            let current = active.fetch_sub(1, Ordering::AcqRel) - 1;
            println!("[conn] connection ended, active: {}", current);

            result
        }

        s => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected next_state {}", s),
        )),
    }
}

async fn proxy_login(
    client: &mut TcpStream,
    backend_addr: &str,
    handshake_raw: &[u8],
) -> io::Result<()> {
    println!("[conn] connecting to backend...");

    let mut server = connect_with_retry(backend_addr).await?;

    println!("[conn] backend connected, replaying handshake and starting proxy");

    // The backend never saw the handshake (we consumed it), so replay it first.
    server.write_all(handshake_raw).await?;

    match io::copy_bidirectional(client, &mut server).await {
        Ok((c2s, s2c)) => {
            println!(
                "[conn] closed (client->server {} bytes, server->client {} bytes)",
                c2s, s2c
            );
            Ok(())
        }
        Err(e) => {
            println!("[conn] proxy error: {}", e);
            Err(e)
        }
    }
}

/// Answers the status request + ping/pong exchange ourselves with a
/// "server is sleeping" MOTD, without waking the backend.
async fn handle_status_locally(client: &mut TcpStream, protocol: i32) -> io::Result<()> {
    loop {
        let payload = read_packet(client, 128).await?;
        let mut idx = 0;
        let packet_id = read_varint_slice(&payload, &mut idx)?;

        match packet_id {
            // Status Request -> Status Response
            0x00 => {
                // Report the real server version name (from the VERSION env var,
                // e.g. "1.20.1") so launchers like Modrinth don't flag the entry
                // as an incompatible version. Echo the client's protocol number
                // so vanilla server lists show it as compatible too.
                let version_name =
                    std::env::var("VERSION").unwrap_or_else(|_| "Sleeping".to_string());

                let json = format!(
                    concat!(
                        r#"{{"version":{{"name":"{}","protocol":{}}},"#,
                        r#""players":{{"max":0,"online":0}},"#,
                        r#""description":{{"text":"\u00a77\u26a1 Server is asleep \u2014 join to wake it up!"}}}}"#
                    ),
                    version_name, protocol
                );

                let mut body = Vec::with_capacity(json.len() + 8);
                write_varint(&mut body, 0x00);
                write_varint(&mut body, json.len() as i32);
                body.extend_from_slice(json.as_bytes());

                write_packet(client, &body).await?;
            }

            // Ping Request -> Pong (echo the 8-byte payload back)
            0x01 => {
                let mut body = Vec::with_capacity(9);
                write_varint(&mut body, 0x01);
                body.extend_from_slice(&payload[idx..]);

                write_packet(client, &body).await?;
                client.flush().await?;
                return Ok(()); // exchange complete, close
            }

            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected status packet id {}", other),
                ));
            }
        }
    }
}

/// Reads the initial handshake packet, keeping the raw bytes for replay.
async fn read_handshake(client: &mut TcpStream) -> io::Result<Handshake> {
    let first = client.read_u8().await?;

    // Legacy (pre-1.7) server list ping starts with 0xFE. Some tools still
    // send it. Just drop it — never wakes the backend.
    if first == 0xFE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy ping ignored",
        ));
    }

    let mut raw = Vec::with_capacity(64);
    let len = read_varint_stream(client, first, &mut raw).await?;

    if len <= 0 || len > 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad handshake length {}", len),
        ));
    }

    let mut payload = vec![0u8; len as usize];
    client.read_exact(&mut payload).await?;
    raw.extend_from_slice(&payload);

    let mut idx = 0;
    let packet_id = read_varint_slice(&payload, &mut idx)?;
    if packet_id != 0x00 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected handshake packet, got id {}", packet_id),
        ));
    }

    let protocol = read_varint_slice(&payload, &mut idx)?;

    // Skip server address (string) + port (u16)
    let addr_len = read_varint_slice(&payload, &mut idx)? as usize;
    idx = idx
        .checked_add(addr_len + 2)
        .filter(|&i| i <= payload.len())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated handshake"))?;

    let next_state = read_varint_slice(&payload, &mut idx)?;

    Ok(Handshake {
        raw,
        protocol,
        next_state,
    })
}

/// Reads one framed packet (VarInt length + payload) and returns the payload.
async fn read_packet(stream: &mut TcpStream, max_len: i32) -> io::Result<Vec<u8>> {
    let first = stream.read_u8().await?;
    let mut scratch = Vec::new();
    let len = read_varint_stream(stream, first, &mut scratch).await?;

    if len <= 0 || len > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad packet length {}", len),
        ));
    }

    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Frames `body` with a VarInt length prefix and writes it out.
async fn write_packet(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    let mut packet = Vec::with_capacity(body.len() + 5);
    write_varint(&mut packet, body.len() as i32);
    packet.extend_from_slice(body);
    stream.write_all(&packet).await
}

/// Reads a VarInt from the stream, starting with an already-read first byte.
/// Every consumed byte is appended to `raw` so callers can replay it.
async fn read_varint_stream(
    stream: &mut TcpStream,
    first: u8,
    raw: &mut Vec<u8>,
) -> io::Result<i32> {
    let mut num: i32 = 0;
    let mut shift = 0;
    let mut byte = first;

    loop {
        raw.push(byte);
        num |= ((byte & 0x7F) as i32) << shift;

        if byte & 0x80 == 0 {
            return Ok(num);
        }

        shift += 7;
        if shift >= 35 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"));
        }

        byte = stream.read_u8().await?;
    }
}

fn read_varint_slice(buf: &[u8], idx: &mut usize) -> io::Result<i32> {
    let mut num: i32 = 0;
    let mut shift = 0;

    loop {
        let byte = *buf
            .get(*idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated varint"))?;
        *idx += 1;

        num |= ((byte & 0x7F) as i32) << shift;

        if byte & 0x80 == 0 {
            return Ok(num);
        }

        shift += 7;
        if shift >= 35 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"));
        }
    }
}

fn write_varint(buf: &mut Vec<u8>, mut value: i32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value = ((value as u32) >> 7) as i32;

        if value != 0 {
            byte |= 0x80;
        }

        buf.push(byte);

        if value == 0 {
            return;
        }
    }
}

async fn connect_with_retry(addr: &str) -> io::Result<TcpStream> {
    let deadline = Duration::from_secs(60);

    println!("[retry] trying to connect to {}", addr);

    timeout(deadline, async {
        let mut attempt = 0;

        loop {
            attempt += 1;

            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    println!("[retry] connected after {} attempts", attempt);
                    return Ok(stream);
                }
                Err(e) => {
                    println!("[retry] attempt {} failed: {}", attempt, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await
    .map_err(|_| {
        println!("[retry] timeout reached, backend never woke up");
        io::Error::new(io::ErrorKind::TimedOut, "backend boot timeout")
    })?
}
