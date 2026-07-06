use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const STATUS_TIMEOUT: Duration = Duration::from_secs(10);
/// Budget for a full readiness probe (TCP connect + MC status round-trip).
const BACKEND_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the background wake task keeps retrying before giving up.
const WAKE_DEADLINE: Duration = Duration::from_secs(120);
/// Slightly under Railway's 10-min idle window, so we never claim
/// "up" when it has actually just gone back to sleep.
const AWAKE_WINDOW: Duration = Duration::from_secs(9 * 60);

#[derive(Clone, Copy)]
enum SleepState {
    Asleep,
    Waking,
    AwakeEmpty,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();

    let backend_addr = std::env::var("BACKEND_ADDR").expect("no BACKEND_ADDR in env");
    let port = std::env::var("PORT").unwrap_or_else(|_| "25565".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    // Optional: hold a joining player's connection up to this many seconds
    // hoping the backend comes up in time (seamless join). 0 = disabled,
    // players get the friendly "waking up" disconnect immediately.
    let login_hold_secs: u64 = std::env::var("LOGIN_HOLD_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    println!("Starting proxy on {}", bind_addr);
    println!("Backend: {}", backend_addr);
    println!("Login hold: {}s", login_hold_secs);

    let listener = TcpListener::bind(&bind_addr).await?;
    let active = Arc::new(AtomicUsize::new(0));
    let waking = Arc::new(AtomicBool::new(false));
    // When we last *knew* the backend was up (wake success, successful
    // probe, or a player session ending). None / expired = asleep.
    let awake_until = Arc::new(Mutex::new(None::<Instant>));

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
        let waking = waking.clone();
        let awake_until = awake_until.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                client,
                &backend_addr,
                active,
                waking,
                awake_until,
                login_hold_secs,
            )
            .await
            {
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
    waking: Arc<AtomicBool>,
    awake_until: Arc<Mutex<Option<Instant>>>,
    login_hold_secs: u64,
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
                // Backend may be asleep. Answer locally, never touch it —
                // any connection to the backend would reset Railway's idle timer.
                let state = if waking.load(Ordering::Acquire) {
                    SleepState::Waking
                } else if is_probably_awake(&awake_until) {
                    SleepState::AwakeEmpty
                } else {
                    SleepState::Asleep
                };

                println!("[conn] status ping — answered locally, backend untouched");
                timeout(
                    STATUS_TIMEOUT,
                    handle_status_locally(&mut client, hs.protocol, state),
                )
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status timeout"))??;
            }
            Ok(())
        }

        // Login (2) or transfer (3, MC 1.20.5+): a real player joining -> wake the backend
        2 | 3 => {
            // Readiness probe: a bare TCP connect is NOT trustworthy here —
            // Railway's edge accepts connections even while the service is
            // asleep/booting. Only a real MC status response proves the
            // server can take a login.
            if backend_is_ready(backend_addr).await {
                // Proven awake: open a fresh connection for the actual login
                // (the probe connection is spent — servers close after status).
                let server = TcpStream::connect(backend_addr).await?;

                waking.store(false, Ordering::Release);
                mark_awake(&awake_until);

                let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                println!("[conn] login attempt, active connections: {}", current);

                let result = proxy_login(&mut client, server, &hs.raw).await;

                let current = active.fetch_sub(1, Ordering::AcqRel) - 1;
                println!("[conn] connection ended, active: {}", current);
                // Server stays up ~10 more min after the last player leaves.
                mark_awake(&awake_until);

                result
            } else {
                // Asleep: kick off the wake in the background (idempotent),
                // then deal with the player gracefully.
                println!("[conn] login while backend asleep — triggering wake");
                spawn_wake(backend_addr.to_string(), waking.clone(), awake_until.clone());

                // Optionally hold the client, hoping the backend boots
                // before the vanilla client gives up (~30s). If it comes
                // up in time, the player joins seamlessly.
                if login_hold_secs > 0 {
                    println!("[conn] holding client up to {}s...", login_hold_secs);
                    if timeout(
                        Duration::from_secs(login_hold_secs),
                        wait_until_ready(backend_addr),
                    )
                    .await
                    .is_ok()
                    {
                        println!("[conn] backend came up during hold — seamless join");
                        let server = TcpStream::connect(backend_addr).await?;
                        mark_awake(&awake_until);

                        let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                        println!("[conn] login attempt, active connections: {}", current);

                        let result = proxy_login(&mut client, server, &hs.raw).await;

                        let current = active.fetch_sub(1, Ordering::AcqRel) - 1;
                        println!("[conn] connection ended, active: {}", current);
                        mark_awake(&awake_until);

                        return result;
                    }
                    println!("[conn] hold expired, sending wake message instead");
                }

                // Friendly disconnect instead of a timeout screen.
                send_login_disconnect(
                    &mut client,
                    concat!(
                        r"\u00a7e\u26a1 The server was asleep \u2014 waking it up now!\n",
                        r"\u00a77Rejoin in ~30 seconds and you're in."
                    ),
                )
                .await
            }
        }

        s => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected next_state {}", s),
        )),
    }
}

fn mark_awake(awake_until: &Mutex<Option<Instant>>) {
    *awake_until.lock().unwrap() = Some(Instant::now() + AWAKE_WINDOW);
}

fn is_probably_awake(awake_until: &Mutex<Option<Instant>>) -> bool {
    awake_until
        .lock()
        .unwrap()
        .map_or(false, |t| Instant::now() < t)
}

/// Returns true if the backend answers a real Minecraft status ping.
/// This is the only trustworthy readiness signal: a bare TCP connect
/// can succeed at Railway's edge even while the service is asleep or
/// the MC server is still booting.
async fn backend_is_ready(addr: &str) -> bool {
    let attempt = async {
        let mut s = TcpStream::connect(addr).await.ok()?;

        // Handshake: id 0x00, protocol 0, addr "probe", port 0, next_state 1 (status)
        let mut hs = Vec::new();
        write_varint(&mut hs, 0x00);
        write_varint(&mut hs, 0);
        write_varint(&mut hs, 5);
        hs.extend_from_slice(b"probe");
        hs.extend_from_slice(&0u16.to_be_bytes());
        write_varint(&mut hs, 1);
        write_packet(&mut s, &hs).await.ok()?;

        // Status Request (empty packet, id 0x00)
        let mut req = Vec::new();
        write_varint(&mut req, 0x00);
        write_packet(&mut s, &req).await.ok()?;

        // Any well-formed response means the MC server is alive.
        // Generous max_len: the status JSON can include a ~30 KB favicon.
        read_packet(&mut s, 64 * 1024).await.ok()?;
        Some(())
    };

    timeout(BACKEND_PROBE_TIMEOUT, attempt)
        .await
        .ok()
        .flatten()
        .is_some()
}

/// Loops until the backend passes a readiness probe. No internal
/// deadline; callers wrap it in `timeout(...)`. The repeated connection
/// attempts are also what trigger Railway to wake the service.
async fn wait_until_ready(addr: &str) {
    loop {
        if backend_is_ready(addr).await {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// Spawns (at most one) background task that probes the backend until
/// it's actually serving Minecraft, then clears the waking flag.
fn spawn_wake(
    backend_addr: String,
    waking: Arc<AtomicBool>,
    awake_until: Arc<Mutex<Option<Instant>>>,
) {
    // swap returns the previous value: if it was already true, a wake
    // task is running and we don't spawn a second one.
    if waking.swap(true, Ordering::AcqRel) {
        return;
    }

    tokio::spawn(async move {
        println!("[wake] starting wake attempts");
        match timeout(WAKE_DEADLINE, wait_until_ready(&backend_addr)).await {
            Ok(()) => {
                println!("[wake] backend is up and serving");
                mark_awake(&awake_until);
            }
            Err(_) => println!("[wake] gave up: backend never became ready"),
        }
        waking.store(false, Ordering::Release);
    });
}

/// Sends a Login Disconnect packet (login state, packet id 0x00) with a
/// JSON chat message, then closes gracefully.
///
/// Order matters: the client has already sent its Login Start packet,
/// which is sitting unread in our receive buffer. Dropping the socket
/// with unread data makes the OS send an RST, which can destroy the
/// disconnect message in flight — the client then sees nothing and
/// times out. So: drain first, write, FIN, wait for the client to
/// close its side.
async fn send_login_disconnect(client: &mut TcpStream, json_text: &str) -> io::Result<()> {
    // Drain the pending Login Start (and anything else queued).
    let _ = timeout(Duration::from_millis(500), read_packet(client, 4096)).await;

    let json = format!(r#"{{"text":"{}"}}"#, json_text);

    let mut body = Vec::with_capacity(json.len() + 8);
    write_varint(&mut body, 0x00); // Login Disconnect packet id
    write_varint(&mut body, json.len() as i32);
    body.extend_from_slice(json.as_bytes());

    write_packet(client, &body).await?;
    client.flush().await?;

    // Graceful close: send FIN after the data, then read until the
    // client closes, so the kernel never RSTs our message away.
    let _ = client.shutdown().await;
    let mut sink = [0u8; 128];
    let _ = timeout(Duration::from_secs(5), async {
        loop {
            match client.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;

    println!("[conn] wake disconnect delivered");
    Ok(())
}

async fn proxy_login(
    client: &mut TcpStream,
    mut server: TcpStream,
    handshake_raw: &[u8],
) -> io::Result<()> {
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

/// Escapes a plain-text string so it can be embedded inside a JSON string.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Answers the status request + ping/pong exchange ourselves,
/// without touching the backend. Asleep/waking show a status-only
/// MOTD; up-and-empty shows the shared MOTD so it looks exactly
/// like the real server.
async fn handle_status_locally(
    client: &mut TcpStream,
    protocol: i32,
    state: SleepState,
) -> io::Result<()> {
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

                let motd = match state {
                    SleepState::Waking => {
                        r"\u00a7e\u26a1 Waking up... refresh and join in a moment!".to_string()
                    }
                    SleepState::Asleep => {
                        r"\u00a77\u26a1 Server is asleep \u2014 join to wake it up!".to_string()
                    }
                    SleepState::AwakeEmpty => {
                        // Look exactly like the real server: just the shared MOTD
                        // (Railway shared variable, same value the itzg container uses).
                        std::env::var("MOTD")
                            .map(|m| json_escape(&m))
                            .unwrap_or_else(|_| "Minecraft Server".to_string())
                    }
                };

                let json = format!(
                    concat!(
                        r#"{{"version":{{"name":"{}","protocol":{}}},"#,
                        r#""players":{{"max":0,"online":0}},"#,
                        r#""description":{{"text":"{}"}}}}"#
                    ),
                    version_name, protocol, motd
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
