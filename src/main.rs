use tokio::{
    net::{TcpListener, TcpStream},
    io,
    time::{sleep, timeout}
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

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

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    loop {
        let (mut client, addr) = listener.accept().await?;
        println!("[conn] new client: {}", addr);

        let active = active.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            let current = active.fetch_add(1, Ordering::AcqRel) + 1;
            println!("[conn] active connections: {}", current);

            let result = async {
                println!("[conn] connecting to backend...");

                let mut server = connect_with_retry(&backend_addr).await?;

                println!("[conn] backend connected, starting proxy");

                match io::copy_bidirectional(&mut client, &mut server).await {
                    Ok((c2s, s2c)) => {
                        println!("[conn] closed (client->server {} bytes, server->client {} bytes)", c2s, s2c);
                    }
                    Err(e) => {
                        println!("[conn] proxy error: {}", e);
                        return Err(e);
                    }
                }

                Ok::<_, io::Error>(())
            }.await;

            let current = active.fetch_sub(1, Ordering::AcqRel) - 1;
            println!("[conn] connection ended, active: {}", current);

            if let Err(e) = result {
                eprintln!("[conn] error: {}", e);
            }
        });
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