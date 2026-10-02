#![warn(clippy::nursery, clippy::pedantic)]

use bpaf::Bpaf;
use lazy_static::lazy_static;
use reqwest::Client;
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use uuid::Uuid;

const ALLOWED_CHARS: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890_";

const PLAYERDB_API: &str = "https://playerdb.co/api/player/minecraft";

lazy_static! {
    static ref CLIENT: Client = Client::builder()
        .pool_max_idle_per_host(2000)
        .timeout(Duration::from_secs(30))
        .user_agent("uuidump/1.0")
        .build()
        .expect("failed to build HTTP client");

    static ref UUID_COUNTER: Arc<AtomicUsize> =
        Arc::new(AtomicUsize::new(0));

    static ref UUID_ALL_COUNTER: Arc<AtomicUsize> =
        Arc::new(AtomicUsize::new(0));

    static ref REQ_COUNTER: Arc<AtomicUsize> =
        Arc::new(AtomicUsize::new(0));
}

#[derive(Debug, Clone, Bpaf)]
#[bpaf(options)]
struct Cli {
    #[bpaf(
        short('w'),
        long("wordlist-path"),
        fallback("wordlist.txt".to_string())
    )]
    wordlist_path: String,

    #[bpaf(
        short('o'),
        long("output"),
        fallback("uuid.txt".to_string())
    )]
    output_path: String,

    #[bpaf(
        short('t'),
        long("theaters"),
        fallback(2000)
    )]
    theaters: usize,

    #[bpaf(
        long("batch-size"),
        fallback(100)
    )]
    batch_size: usize,

    #[bpaf(
        long("limit"),
        fallback(200_000_000usize)
    )]
    limit: usize,

    #[bpaf(long("no-generate"), switch)]
    no_generate: bool,

    #[bpaf(long("no-query"), switch)]
    no_query: bool,

    #[bpaf(
        long("api-url"),
        fallback(PLAYERDB_API.to_string())
    )]
    api_url: String,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = cli().run();

    println!("========================================");
    println!("              UUIDump");
    println!("========================================");
    println!();
    println!("Wordlist:   {}", cli.wordlist_path);
    println!("Output:     {}", cli.output_path);
    println!("Workers:    {}", cli.theaters);
    println!("Batch size: {}", cli.batch_size);
    println!("Limit:      {}", cli.limit);
    println!("API:        {}", cli.api_url);
    println!();

    if !cli.no_generate && !Path::new(&cli.wordlist_path).exists() {
        eprintln!(
            "wordlist.txt does not exist. Use an existing wordlist with --no-generate."
        );
        return Ok(());
    }

    if !cli.no_query {
        query_wordlist(
            &cli.wordlist_path,
            &cli.output_path,
            cli.theaters,
            cli.batch_size,
            cli.limit,
            &cli.api_url,
        )
        .await?;
    }

    Ok(())
}

async fn query_wordlist(
    wordlist_path: &str,
    output_path: &str,
    theaters: usize,
    batch_size: usize,
    limit: usize,
    api_url: &str,
) -> eyre::Result<()> {
    if theaters == 0 || batch_size == 0 {
        eyre::bail!("theaters and batch-size must be greater than 0");
    }

    let file = tokio::fs::File::open(wordlist_path).await?;
    let reader = Arc::new(Mutex::new(BufReader::new(file)));

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let writer = Arc::new(Mutex::new(tokio::fs::File::create(output_path).await?));

    let status_task = tokio::spawn(async {
        display_status().await;
    });

    let mut workers = Vec::with_capacity(theaters);

    for worker_id in 0..theaters {
        let reader = Arc::clone(&reader);
        let tx = tx.clone();
        let api_url = api_url.to_string();

        workers.push(tokio::spawn(async move {
            worker(
                worker_id,
                reader,
                tx,
                batch_size,
                limit,
                &api_url,
            )
            .await
        }));
    }

    drop(tx);

    while let Some(uuid) = rx.recv().await {
        let mut writer = writer.lock().await;
        writer.write_all(uuid.as_bytes()).await?;
        writer.write_all(b"\\n").await?;
        UUID_COUNTER.fetch_add(1, Ordering::Relaxed);
    }

    for worker in workers {
        if let Err(error) = worker.await {
            eprintln!("Worker error: {error}");
        }
    }

    status_task.abort();

    println!();
    println!();
    println!("========================================");
    println!("Finished");
    println!("========================================");
    println!(
        "Requests: {}",
        REQ_COUNTER.load(Ordering::Relaxed)
    );
    println!(
        "UUIDs found: {}",
        UUID_COUNTER.load(Ordering::Relaxed)
    );
    println!(
        "Names checked: {}",
        UUID_ALL_COUNTER.load(Ordering::Relaxed)
    );

    Ok(())
}

async fn worker(
    _worker_id: usize,
    reader: Arc<Mutex<BufReader<tokio::fs::File>>>,
    tx: mpsc::UnboundedSender<String>,
    batch_size: usize,
    limit: usize,
    api_url: &str,
) -> eyre::Result<()> {
    loop {
        if UUID_ALL_COUNTER.load(Ordering::Relaxed) >= limit {
            break;
        }

        let mut names = Vec::with_capacity(batch_size);

        {
            let mut reader = reader.lock().await;

            for _ in 0..batch_size {
                if UUID_ALL_COUNTER.load(Ordering::Relaxed) >= limit {
                    break;
                }

                let mut line = String::new();
                let bytes = reader.read_line(&mut line).await?;

                if bytes == 0 {
                    break;
                }

                UUID_ALL_COUNTER.fetch_add(1, Ordering::Relaxed);

                let name = line.trim().to_string();

                if name.len() < 3 || name.len() > 16 {
                    continue;
                }

                if !name.chars().all(|c| ALLOWED_CHARS.contains(c)) {
                    continue;
                }

                names.push(name);
            }
        }

        if names.is_empty() {
            if UUID_ALL_COUNTER.load(Ordering::Relaxed) >= limit {
                break;
            }

            continue;
        }

        request_names(&names, api_url, &tx).await?;
    }

    Ok(())
}

async fn request_names(
    names: &[String],
    api_url: &str,
    tx: &mpsc::UnboundedSender<String>,
) -> eyre::Result<()> {
    for name in names {
        let url = format!(
            "{}/{}",
            api_url.trim_end_matches('/'),
            name
        );

        for attempt in 1..=3 {
            REQ_COUNTER.fetch_add(1, Ordering::Relaxed);

            let response = match CLIENT.get(&url).send().await {
                Ok(response) => response,
                Err(error) => {
                    if attempt == 3 {
                        eprintln!("Request error for {name}: {error}");
                        break;
                    }

                    sleep(Duration::from_millis(250 * attempt as u64)).await;
                    continue;
                }
            };

            if response.status() == reqwest::StatusCode::NOT_FOUND {
                break;
            }

            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(1);

                sleep(Duration::from_secs(retry_after)).await;
                continue;
            }

            if !response.status().is_success() {
                if attempt == 3 {
                    eprintln!(
                        "PlayerDB returned HTTP {} for {name}",
                        response.status()
                    );
                    break;
                }

                sleep(Duration::from_millis(250 * attempt as u64)).await;
                continue;
            }

            let json: Value = match response.json().await {
                Ok(json) => json,
                Err(error) => {
                    if attempt == 3 {
                        eprintln!("Invalid JSON for {name}: {error}");
                    }
                    continue;
                }
            };

            if json.get("code").and_then(Value::as_str) != Some("player.found") {
                break;
            }

            let Some(uuid_string) = json
                .get("data")
                .and_then(|data| data.get("player"))
                .and_then(|player| player.get("id"))
                .and_then(Value::as_str)
            else {
                eprintln!("No UUID in PlayerDB response for {name}");
                break;
            };

            match uuid_string.parse::<Uuid>() {
                Ok(uuid) => {
                    tx.send(uuid.to_string())?;
                }
                Err(error) => {
                    eprintln!("Invalid UUID for {name}: {error}");
                }
            }

            break;
        }
    }

    Ok(())
}

async fn display_status() {
    loop {
        print_status();
        sleep(Duration::from_secs(1)).await;
    }
}

fn print_status() {
    let requests = REQ_COUNTER.load(Ordering::Relaxed);
    let found = UUID_COUNTER.load(Ordering::Relaxed);
    let checked = UUID_ALL_COUNTER.load(Ordering::Relaxed);

    print!(
        "\rRequests: {:<12} | UUIDs found: {:<12} | Names checked: {:<12}",
        requests, found, checked
    );

    std::io::stdout().flush().ok();
}

#[allow(dead_code)]
fn _keep_writer_import() {
    let _ = BufWriter::new;
}
