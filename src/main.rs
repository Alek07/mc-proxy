use tokio::{
    net::{TcpListener, TcpStream},
    io,
    time::{sleep, timeout}
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    }, time::Duration,

};

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();

    let backend_addr = std::env::var("BACKEND_ADDR").expect("no BACKEND_ADDR in env");
    let port = std::env::var("PORT").unwrap_or_else(|_| "25565".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&bind_addr).await?;
    let active = Arc::new(AtomicUsize::new(0));

    {
        let active = active.clone();
        let backend_addr = backend_addr.clone();

        tokio::spawn(async move {
            loop {
                if active.load(Ordering::Acquire) > 0 {
                    let _ = TcpStream::connect(&backend_addr).await;
                }

                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
    }

    loop {
        let (mut client, _) = listener.accept().await?;
        let active = active.clone();

        let backend_addr = backend_addr.clone();
        tokio::spawn(async move {
            active.fetch_add(1, Ordering::AcqRel);

            let result = async {
                let mut server = connect_with_retry(&backend_addr).await?;

                io::copy_bidirectional(&mut client, &mut server).await?;

                Ok::<_, io::Error>(())
            }.await;

            active.fetch_sub(1, Ordering::AcqRel);

            if let Err(e) = result {
                eprintln!("connection error: {}", e);
            }
        });
    }
}


async fn connect_with_retry(addr: &str) -> io::Result<TcpStream> {
    let deadline = Duration::from_secs(60);

    timeout(deadline, async {
        loop {
            match TcpStream::connect(addr).await {
                Ok(stream) => return Ok(stream),
                Err(_) => {
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "backend boot timeout"))?
}