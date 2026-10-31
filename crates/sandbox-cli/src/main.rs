//! Private Project API test client. Public operator commands live in hudson-cli.
use clap::{Args, Parser, Subcommand};
use sandbox_client::{Client, Error, Event, models::*, requests::*};
use serde::Serialize;
use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Parser)]
#[command(
    name = "sandbox-api-test-client",
    // `--version` prints the product: `hudsonbox X.Y.Z`.
    display_name = "hudsonbox",
    version,
    about = "Private Hudsonbox API test client; operators should use `hudson sandbox`"
)]
struct Cli {
    /// Private client configuration; contains the origin and credential file path.
    #[arg(long, global = true, env = "HUDSONBOX_CONFIG")]
    config: Option<PathBuf>,
    /// Machine-readable JSON. Stream always emits newline-delimited JSON events.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}
#[derive(Args)]
#[group(required = true, multiple = false)]
struct Key {
    /// Stable key for one logical mutation. Reuse with exactly the same payload.
    #[arg(long)]
    key: Option<String>,
    /// Existing file containing the stable key. Generate once with `key --to`.
    #[arg(long)]
    key_file: Option<PathBuf>,
}
impl Key {
    fn read(&self) -> Result<String, Error> {
        let key = if let Some(key) = &self.key {
            key.clone()
        } else if let Some(path) = &self.key_file {
            let bytes = sandbox_client::read_upload(path)?;
            if bytes.len() > 129 {
                return Err(Error::Request);
            }
            std::str::from_utf8(&bytes)
                .map_err(|_| Error::Request)?
                .trim_end_matches('\n')
                .to_owned()
        } else {
            return Err(Error::Request);
        };
        if !sandbox_client::valid_key(&key) {
            return Err(Error::Request);
        }
        Ok(key)
    }
}
#[derive(Args)]
struct Page {
    #[arg(long)]
    limit: Option<u64>,
    #[arg(long)]
    cursor: Option<String>,
}
#[derive(Subcommand)]
enum Command {
    /// Save a new mutation key before making a request. Refuses to overwrite.
    Key {
        #[arg(long)]
        to: PathBuf,
    },
    /// Admit creation from a saved JSON request. Returns before completion.
    Create {
        #[arg(long)]
        request: PathBuf,
        #[command(flatten)]
        key: Key,
    },
    /// List one page. Pass the returned cursor explicitly for the next page.
    List {
        #[command(flatten)]
        page: Page,
    },
    Get {
        sandbox_id: String,
    },
    /// Admit a command from saved JSON including an absolute deadline_unix_ms.
    Execute {
        sandbox_id: String,
        #[arg(long)]
        request: PathBuf,
        #[command(flatten)]
        key: Key,
    },
    Destroy {
        sandbox_id: String,
        #[command(flatten)]
        key: Key,
    },
    Operations {
        #[arg(long)]
        sandbox_id: Option<String>,
        #[command(flatten)]
        page: Page,
    },
    Operation {
        operation_id: String,
    },
    /// Poll only this operation. Timeout does not cancel it.
    Wait {
        operation_id: String,
        #[arg(long, default_value_t = 60)]
        seconds: u64,
    },
    /// Admit a separate cancellation operation; inspect both handles afterward.
    Cancel {
        operation_id: String,
        #[command(flatten)]
        key: Key,
    },
    /// Save one retained output range to a new local file, with framing metadata.
    Output {
        operation_id: String,
        #[arg(value_parser = ["stdout", "stderr"])]
        output_name: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 32768)]
        limit: u64,
        #[arg(long)]
        to: PathBuf,
    },
    /// Stream NDJSON with base64 bytes and resumable cursors. End is output completion.
    Stream {
        operation_id: String,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 120)]
        seconds: u64,
        #[arg(long, default_value_t = 3)]
        reconnects: u8,
    },
    /// Admit a bounded file upload; returns before guest publication.
    Upload {
        sandbox_id: String,
        path: String,
        #[arg(long)]
        from: PathBuf,
        #[arg(long, default_value = "0644", value_parser = ["0644", "0755"])]
        mode: String,
        #[command(flatten)]
        key: Key,
    },
    /// Capture once and verify size/SHA-256 before publishing a new private file.
    Download {
        sandbox_id: String,
        path: String,
        #[arg(long)]
        to: PathBuf,
    },
}
#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Cli::parse();
    match run(&args).await {
        Ok(code) => std::process::ExitCode::from(code),
        Err(error) => {
            if args.json {
                let status = match &error {
                    Error::Http { status, .. } => Some(*status),
                    _ => None,
                };
                eprintln!(
                    "{}",
                    serde_json::json!({"error": error.kind(), "http_status": status, "code":error.problem_code(), "operation_id":error.operation_id(), "message": error.to_string()})
                );
            } else {
                eprintln!(
                    "{error}{}{}",
                    error
                        .problem_code()
                        .map_or(String::new(), |c| format!(" ({c})")),
                    error
                        .operation_id()
                        .map_or(String::new(), |id| format!(" operation={id}"))
                );
            }
            std::process::ExitCode::from(match error {
                Error::WaitTimeout => 6,
                Error::Transport => 7,
                Error::Http { .. } => 8,
                _ => 1,
            })
        }
    }
}
fn emit<T: Serialize>(value: &T, human: &str, json: bool) -> Result<(), Error> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if json {
        serde_json::to_writer(&mut out, value).map_err(|_| Error::File)?;
    } else {
        write!(out, "{human}").map_err(|_| Error::File)?;
    }
    writeln!(out).map_err(|_| Error::File)?;
    out.flush().map_err(|_| Error::File)
}
fn escaped(value: &str) -> String {
    value.escape_default().to_string()
}
fn admitted(value: &AdmittedResponse, json: bool) -> Result<(), Error> {
    emit(
        value,
        &format!(
            "Admitted {} for {} ({})\nPoll with: sandbox-api-test-client wait {}",
            escaped(&value.operation_id),
            escaped(&value.sandbox_id),
            escaped(&value.status),
            escaped(&value.operation_id)
        ),
        json,
    )
}
fn operation(value: &OperationBody, json: bool) -> Result<(), Error> {
    let exit = value
        .result
        .as_ref()
        .and_then(|r| r.get("exit_code"))
        .and_then(|v| v.as_i64());
    emit(
        value,
        &format!(
            "{}: {} {} (command exit: {})",
            escaped(&value.operation_id),
            escaped(&value.kind),
            escaped(&value.status),
            exit.map_or("unavailable".into(), |c| c.to_string())
        ),
        json,
    )
}
fn operation_exit(value: &OperationBody) -> u8 {
    match value.status.as_str() {
        "succeeded" => 0,
        "failed" => {
            if value
                .result
                .as_ref()
                .and_then(|r| r.get("exit_code"))
                .and_then(|v| v.as_i64())
                .is_some_and(|v| v != 0)
            {
                10
            } else {
                3
            }
        }
        "cancelled" => 4,
        "unknown" => 5,
        _ => 6,
    }
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let bytes = sandbox_client::read_upload(path)?;
    if bytes.len() > 65536 {
        return Err(Error::Request);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Request)
}
fn publish(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(|_| Error::File)?;
    file.write_all(bytes).map_err(|_| Error::File)?;
    file.as_file().sync_all().map_err(|_| Error::File)?;
    file.persist_noclobber(path).map_err(|_| Error::File)?;
    Ok(())
}
async fn run(args: &Cli) -> Result<u8, Error> {
    if let Command::Key { to } = &args.command {
        publish(
            to,
            format!("{}\n", sandbox_client::new_idempotency_key()).as_bytes(),
        )?;
        emit(
            &serde_json::json!({"saved":true}),
            "Saved new key; reuse this file for retries of the same mutation.",
            args.json,
        )?;
        return Ok(0);
    }
    let client = Client::from_config(args.config.as_deref().ok_or(Error::Config)?)?;
    match &args.command {
        Command::Key { .. } => return Err(Error::Request),
        Command::Create { request, key } => admitted(
            &client
                .create_sandbox(CreateSandbox {
                    body: &read_json(request)?,
                    idempotency_key: &key.read()?,
                })
                .await?,
            args.json,
        )?,
        Command::List { page } => {
            let value = client
                .list_sandboxes(ListSandboxes {
                    limit: page.limit,
                    cursor: page.cursor.as_deref(),
                })
                .await?;
            let mut human = value
                .items
                .iter()
                .map(|s| {
                    format!(
                        "{} desired={} observed={}",
                        escaped(&s.sandbox_id),
                        escaped(&s.desired_state),
                        escaped(&s.observed_state)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            if let Some(cursor) = &value.next_cursor {
                human.push_str(&format!("\nNext cursor: {}", escaped(cursor)));
            }
            emit(&value, &human, args.json)?;
        }
        Command::Get { sandbox_id } => {
            let value = client.get_sandbox(GetSandbox { sandbox_id }).await?;
            emit(
                &value,
                &format!(
                    "{} desired={} observed={} generation={}",
                    escaped(&value.sandbox_id),
                    escaped(&value.desired_state),
                    escaped(&value.observed_state),
                    value.generation
                ),
                args.json,
            )?;
        }
        Command::Execute {
            sandbox_id,
            request,
            key,
        } => admitted(
            &client
                .execute_command(ExecuteCommand {
                    sandbox_id,
                    body: &read_json(request)?,
                    idempotency_key: &key.read()?,
                })
                .await?,
            args.json,
        )?,
        Command::Destroy { sandbox_id, key } => admitted(
            &client
                .destroy_sandbox(DestroySandbox {
                    sandbox_id,
                    body: &DestroyRequest {
                        correlation_id: None,
                    },
                    idempotency_key: &key.read()?,
                })
                .await?,
            args.json,
        )?,
        Command::Cancel { operation_id, key } => admitted(
            &client
                .cancel_command(CancelCommand {
                    operation_id,
                    body: &CancelRequest {},
                    idempotency_key: &key.read()?,
                })
                .await?,
            args.json,
        )?,
        Command::Operations { sandbox_id, page } => {
            let value = client
                .list_operations(ListOperations {
                    limit: page.limit,
                    cursor: page.cursor.as_deref(),
                    sandbox_id: sandbox_id.as_deref(),
                })
                .await?;
            let mut human = value
                .items
                .iter()
                .map(|s| {
                    format!(
                        "{} {} {}",
                        escaped(&s.operation_id),
                        escaped(&s.kind),
                        escaped(&s.status)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            if let Some(cursor) = &value.next_cursor {
                human.push_str(&format!("\nNext cursor: {}", escaped(cursor)));
            }
            emit(&value, &human, args.json)?;
        }
        Command::Operation { operation_id } => operation(
            &client.get_operation(GetOperation { operation_id }).await?,
            args.json,
        )?,
        Command::Wait {
            operation_id,
            seconds,
        } => {
            let value = client
                .wait(operation_id, Duration::from_secs(*seconds))
                .await?;
            operation(&value, args.json)?;
            return Ok(operation_exit(&value));
        }
        Command::Output {
            operation_id,
            output_name,
            offset,
            limit,
            to,
        } => {
            let value = client
                .read_output(ReadOutput {
                    operation_id,
                    output_name,
                    offset: Some(*offset),
                    limit: Some(*limit),
                })
                .await?;
            publish(to, &value.bytes)?;
            emit(
                &value,
                &format!(
                    "Saved {} bytes; next_offset={} size={} eof={} truncated={} simulated={}",
                    value.bytes.len(),
                    value.next_offset,
                    value.size,
                    value.eof,
                    value.truncated.unwrap_or(false),
                    value.simulated
                ),
                args.json,
            )?;
        }
        Command::Upload {
            sandbox_id,
            path,
            from,
            mode,
            key,
        } => admitted(
            &client
                .upload(
                    sandbox_id,
                    path,
                    &sandbox_client::read_upload(from)?,
                    mode,
                    &key.read()?,
                )
                .await?,
            args.json,
        )?,
        Command::Download {
            sandbox_id,
            path,
            to,
        } => {
            let value = client.download(sandbox_id, path, to).await?;
            emit(
                &value,
                &format!(
                    "Saved {} verified bytes; sha256={} simulated={} release_confirmed={}",
                    value.size, value.sha256, value.simulated, value.release_confirmed
                ),
                args.json,
            )?;
        }
        Command::Stream {
            operation_id,
            cursor,
            seconds,
            reconnects,
        } => {
            if !(1..=86400).contains(seconds) || *reconnects > 20 {
                return Err(Error::Request);
            }
            return tokio::time::timeout(
                Duration::from_secs(*seconds),
                stream(&client, operation_id, cursor.clone(), *reconnects),
            )
            .await
            .map_err(|_| Error::WaitTimeout)?;
        }
    }
    Ok(0)
}
async fn stream(
    client: &Client,
    operation_id: &str,
    mut cursor: Option<String>,
    reconnects: u8,
) -> Result<u8, Error> {
    for attempt in 0..=reconnects {
        let mut events = client
            .stream_output(StreamOutput {
                operation_id,
                cursor: cursor.as_deref(),
                last_event_id: None,
            })
            .await?;
        loop {
            match events.next().await {
                Ok(Some(event)) => {
                    emit(&event, "", true)?;
                    if let Some(next) = event.cursor() {
                        cursor = Some(next.into());
                    }
                    match event {
                        Event::End { .. } => return Ok(0),
                        Event::Gap { .. } | Event::Error { .. } => return Ok(9),
                        _ => (),
                    }
                }
                Ok(None) => break,
                Err(Error::Transport) if attempt < reconnects => break,
                Err(error) => return Err(error),
            }
        }
        if attempt < reconnects {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    Err(Error::WaitTimeout)
}
