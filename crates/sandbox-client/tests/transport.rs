#![allow(clippy::unwrap_used, clippy::expect_used)]
#[path = "support/server.rs"]
mod server;
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use sandbox_client::{Client, Error, Event, models::*, requests::*};
use serde_json::json;
use server::{Server, private};
use sha2::{Digest, Sha256};
use std::{
    os::unix::fs::PermissionsExt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn json_response(value: serde_json::Value) -> Response {
    ([("cache-control", "no-store")], axum::Json(value)).into_response()
}
fn operation(status: &str) -> serde_json::Value {
    json!({"operation_id":"op_fixture","sandbox_id":"sbx_fixture","kind":"execute","status":status,"created_at":"2026-09-21T00:00:00Z"})
}
fn stream_response(bytes: impl Into<Body>) -> Response {
    (
        [
            ("cache-control", "no-store"),
            ("content-type", "text/event-stream"),
        ],
        bytes.into(),
    )
        .into_response()
}
fn output_event(cursor: &str) -> String {
    format!(
        "event: output\nid: {cursor}\ndata: {}\n\n",
        json!({"stream":"stdout","offset":0,"next_offset":3,"data_base64":"YQD/","at_end":true,"complete":true,"seen":5,"stored":3,"truncated":true,"simulated":true,"guest_reported":true})
    )
}

#[tokio::test]
async fn bounded_operation_reads_reject_ignored_wire_bytes_before_decoding_or_eof() {
    use tokio_stream::StreamExt;
    for chunked in [false, true] {
        let mut value = operation("succeeded");
        value["future_field"] = "x".repeat(8192).into();
        let bytes = serde_json::to_vec(&value).unwrap();
        let exact = bytes.len();
        let s = Server::start(Router::new().route(
            "/v1/operations/op_fixture",
            get(move || {
                let bytes = bytes.clone();
                async move {
                    let body = if chunked {
                        // No Content-Length and no EOF: rejection must follow the
                        // byte bound without waiting for the request timeout.
                        Body::from_stream(
                            tokio_stream::iter([Ok::<_, std::io::Error>(bytes)])
                                .chain(tokio_stream::pending()),
                        )
                    } else {
                        Body::from(bytes)
                    };
                    (
                        [
                            ("cache-control", "no-store"),
                            ("content-type", "application/json"),
                        ],
                        body,
                    )
                }
            }),
        ))
        .await;
        let client = s.client();
        let rejected = tokio::time::timeout(
            Duration::from_secs(5),
            client.get_operation_bounded(
                GetOperation {
                    operation_id: "op_fixture",
                },
                4096,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(rejected, Err(Error::Protocol)));
        if !chunked {
            assert_eq!(
                client
                    .get_operation_bounded(
                        GetOperation {
                            operation_id: "op_fixture"
                        },
                        exact
                    )
                    .await
                    .unwrap()
                    .status,
                "succeeded"
            );
            // The generated method keeps its existing 2 MiB response contract.
            assert_eq!(
                client
                    .get_operation(GetOperation {
                        operation_id: "op_fixture"
                    })
                    .await
                    .unwrap()
                    .status,
                "succeeded"
            );
        }
    }
}

#[tokio::test]
async fn bounded_operation_reads_validate_limits_before_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let s = Server::start(Router::new().route(
        "/v1/operations/op_fixture",
        get(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                json_response(operation("succeeded"))
            }
        }),
    ))
    .await;
    for limit in [0, 2 * 1024 * 1024 + 1] {
        assert!(matches!(
            s.client()
                .get_operation_bounded(
                    GetOperation {
                        operation_id: "op_fixture"
                    },
                    limit
                )
                .await,
            Err(Error::Request)
        ));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bounded_operation_problem_keeps_status_without_oversized_body() {
    let s = Server::start(Router::new().route("/v1/operations/op_fixture", get(|| async {
        (StatusCode::GONE, axum::Json(json!({"status":410,"title":"expired","code":"response_expired","future_field":"x".repeat(8192)})))
    }))).await;
    assert!(matches!(
        s.client()
            .get_operation_bounded(
                GetOperation {
                    operation_id: "op_fixture"
                },
                4096
            )
            .await,
        Err(Error::Http {
            status: 410,
            code: None,
            operation_id: None
        })
    ));
}

#[tokio::test]
async fn configuration_rejects_unsafe_origins_permissions_symlinks_and_oversize() {
    let s = Server::start(Router::new()).await;
    for origin in [
        "http://localhost",
        "https://user:secret@localhost",
        "https://localhost/path",
        "https://localhost/?token=secret",
        "https://localhost/#fragment",
    ] {
        s.edit_config(|v| v["endpoint"] = origin.into());
        assert!(matches!(Client::from_config(&s.config), Err(Error::Config)));
    }
    s.edit_config(|v| v["endpoint"] = s.endpoint.clone().into());
    assert!(Client::from_config(&s.config).is_ok());
    let credential = s.directory.path().join("credential.json");
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Client::from_config(&s.config).is_err());
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
    let link = s.directory.path().join("link");
    std::os::unix::fs::symlink(&credential, &link).unwrap();
    s.edit_config(|v| v["credential_file"] = "link".into());
    assert!(Client::from_config(&s.config).is_err());
    s.edit_config(|v| v["credential_file"] = "credential.json".into());
    private(&credential, &vec![b'x'; 65537]);
    assert!(Client::from_config(&s.config).is_err());
    assert!(!format!("{:?}", Client::from_config(&s.config)).contains("test-private-credential"));
}
#[tokio::test]
async fn tls_hostname_and_ca_verification_are_required() {
    let s = Server::start(Router::new().route(
        "/v1/operations/op_fixture",
        get(|| async { json_response(operation("succeeded")) }),
    ))
    .await;
    assert!(
        s.client()
            .get_operation(GetOperation {
                operation_id: "op_fixture"
            })
            .await
            .is_ok()
    );
    s.edit_config(|v| v["endpoint"] = s.endpoint.replace("localhost", "127.0.0.1").into());
    assert!(matches!(
        s.client()
            .get_operation(GetOperation {
                operation_id: "op_fixture"
            })
            .await,
        Err(Error::Transport)
    ));
    s.edit_config(|v| {
        v["endpoint"] = s.endpoint.clone().into();
        v.as_object_mut().unwrap().remove("ca_file");
    });
    assert!(matches!(
        s.client()
            .get_operation(GetOperation {
                operation_id: "op_fixture"
            })
            .await,
        Err(Error::Transport)
    ));
}
#[tokio::test]
async fn redirects_are_not_followed_and_errors_redact_titles_codes_urls() {
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let s = Server::start(Router::new().route("/v1/operations/op_fixture",get(|| async {
        (StatusCode::TEMPORARY_REDIRECT, [("location","/leak")], axum::Json(json!({"title":"test-private-credential","status":307,"code":"test-private-credential"})))
    })).route("/leak",get(move || { let calls = calls.clone(); async move { calls.fetch_add(1,Ordering::SeqCst); "leaked" } }))).await;
    let error = s
        .client()
        .get_operation(GetOperation {
            operation_id: "op_fixture",
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Http { status: 307, .. }));
    assert!(!format!("{error:?} {error}").contains("private-credential"));
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn lost_mutation_reply_is_not_retried_or_given_a_new_key() {
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let s = Server::start(Router::new().route(
        "/v1/sandboxes",
        post(move |headers: HeaderMap, bytes: axum::body::Bytes| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(headers["idempotency-key"], "persistent-test-key");
                assert_eq!(headers["authorization"], "Bearer test-private-credential");
                assert!(serde_json::from_slice::<CreateRequest>(&bytes).is_ok());
                (
                    StatusCode::ACCEPTED,
                    [
                        ("cache-control", "no-store"),
                        ("content-type", "application/json"),
                    ],
                    Body::from_stream(tokio_stream::iter([
                        Ok::<_, std::io::Error>("{"),
                        Err(std::io::Error::other("lost response")),
                    ])),
                )
            }
        }),
    ))
    .await;
    let body: CreateRequest = serde_json::from_value(
        json!({"image_digest":"sha256:test","resources":{"vcpu":1,"memory_mib":128,"disk_mib":64}}),
    )
    .unwrap();
    assert!(
        s.client()
            .create_sandbox(CreateSandbox {
                idempotency_key: "persistent-test-key",
                body: &body
            })
            .await
            .is_err()
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn path_injection_is_rejected_before_a_request() {
    let s = Server::start(Router::new()).await;
    for id in [
        "..",
        "../other",
        "%2e%2e",
        "https://evil.test",
        "op_?x=1",
        "op_#fragment",
        "a\\b",
    ] {
        assert!(matches!(
            s.client()
                .get_operation(GetOperation { operation_id: id })
                .await,
            Err(Error::Request)
        ));
    }
}
#[tokio::test]
async fn response_caps_content_types_and_stalled_bodies_fail() {
    let s = Server::start(Router::new().route(
        "/v1/operations/{id}",
        get(
            |axum::extract::Path(id): axum::extract::Path<String>| async move {
                let body = match id.as_str() {
                    "large" => Body::from(vec![b'x'; 2 * 1024 * 1024 + 1]),
                    "stall" => Body::from_stream(tokio_stream::pending::<
                        Result<&'static str, std::io::Error>,
                    >()),
                    _ => Body::from("{\"secret\":true}"),
                };
                (
                    [
                        ("cache-control", "no-store"),
                        (
                            "content-type",
                            if id == "wrong" {
                                "text/html"
                            } else {
                                "application/json"
                            },
                        ),
                    ],
                    body,
                )
            },
        ),
    ))
    .await;
    for id in ["large", "wrong", "malformed"] {
        assert!(matches!(
            s.client()
                .get_operation(GetOperation { operation_id: id })
                .await,
            Err(Error::Protocol)
        ));
    }
    assert!(matches!(
        s.client()
            .get_operation(GetOperation {
                operation_id: "stall"
            })
            .await,
        Err(Error::Transport)
    ));
}
#[tokio::test]
async fn waiting_polls_only_the_same_operation_and_does_not_cancel() {
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let s = Server::start(Router::new().route(
        "/v1/operations/op_fixture",
        get(move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                json_response(operation("running"))
            }
        }),
    ))
    .await;
    let client = s.client();
    client
        .get_operation(GetOperation {
            operation_id: "op_fixture",
        })
        .await
        .unwrap();
    count.store(0, Ordering::SeqCst);
    assert!(matches!(
        client.wait("op_fixture", Duration::from_millis(80)).await,
        Err(Error::WaitTimeout)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn unknown_is_returned_without_retry_and_wrong_operation_is_rejected() {
    let s = Server::start(Router::new().route(
        "/v1/operations/{id}",
        get(|| async { json_response(operation("unknown")) }),
    ))
    .await;
    assert_eq!(
        s.client()
            .wait("op_fixture", Duration::from_secs(1))
            .await
            .unwrap()
            .status,
        "unknown"
    );
    assert!(matches!(
        s.client().wait("op_wrong", Duration::from_secs(1)).await,
        Err(Error::Protocol)
    ));
}
#[tokio::test]
async fn stream_preserves_binary_bytes_and_does_not_commit_partial_cursor() {
    let s = Server::start(Router::new().route(
        "/v1/operations/op_fixture/stream",
        get(|| async {
            stream_response(format!(
                ": heartbeat\n\n{}event: output\nid: do-not-commit\ndata:",
                output_event("committed")
            ))
        }),
    ))
    .await;
    let mut stream = s
        .client()
        .stream_output(StreamOutput {
            operation_id: "op_fixture",
            cursor: None,
            last_event_id: None,
        })
        .await
        .unwrap();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.cursor(), Some("committed"));
    assert!(
        matches!(event,Event::Output { data, .. } if data.data_base64 == "YQD/" && data.truncated)
    );
    assert!(stream.next().await.unwrap().is_none());
}
#[tokio::test]
async fn stream_reconnect_uses_opaque_cursor_and_gap_never_means_success() {
    let s = Server::start(Router::new().route(
        "/v1/operations/op_fixture/stream",
        get(
            |axum::extract::Query(q): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| async move {
                assert_eq!(q.get("cursor").unwrap(), "opaque+/?=&");
                stream_response("event: gap\ndata: {\"code\":\"output_expired\"}\n\n")
            },
        ),
    ))
    .await;
    let mut stream = s
        .client()
        .stream_output(StreamOutput {
            operation_id: "op_fixture",
            cursor: Some("opaque+/?=&"),
            last_event_id: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await.unwrap(),
        Some(Event::Gap { .. })
    ));
    assert!(stream.next().await.unwrap().is_none());
}
fn file_response(bytes: &'static [u8], sha: &str, next: usize) -> Response {
    let mut response = (
        [
            ("cache-control", "no-store"),
            ("content-type", "application/octet-stream"),
        ],
        bytes,
    )
        .into_response();
    for (key, value) in [
        ("x-file-offset", "0".into()),
        ("x-file-next-offset", next.to_string()),
        ("x-file-size", bytes.len().to_string()),
        ("x-file-eof", "true".into()),
        ("x-file-simulated", "true".into()),
        ("x-file-guest-reported", "true".into()),
        ("x-file-sha256", sha.to_string()),
    ] {
        response.headers_mut().insert(
            axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    response
}
async fn file_server(corrupt: bool, expire: bool, released: Arc<AtomicUsize>) -> Server {
    let sha = hex::encode(Sha256::digest(b"correct"));
    let capture_sha = sha.clone();
    Server::start(Router::new().route("/v1/sandboxes/sbx_fixture/files/captures",post(move || { let sha = capture_sha.clone(); async move {
        (StatusCode::CREATED, json_response(json!({"capture":"opaque-capture","size":7,"sha256":sha,"expires_unix_ms":4102444800000_i64,"chunk_size":32768,"simulated":true,"guest_reported":true})))
    }}).get(move |headers: HeaderMap| { let sha=sha.clone(); async move {
        assert_eq!(headers["x-file-capture"],"opaque-capture");
        if expire { return StatusCode::GONE.into_response(); }
        file_response(if corrupt { b"corrupt" } else { b"correct" },&sha,7)
    }}).delete(move |headers: HeaderMap| { let released = released.clone(); async move {
        assert_eq!(headers["x-file-capture"],"opaque-capture"); released.fetch_add(1,Ordering::SeqCst);
        (StatusCode::NO_CONTENT,[("cache-control","no-store")])
    }}))).await
}
#[tokio::test]
async fn download_verifies_digest_releases_and_refuses_overwrite() {
    let released = Arc::new(AtomicUsize::new(0));
    let s = file_server(false, false, released.clone()).await;
    let dest = s.directory.path().join("download");
    let receipt = s
        .client()
        .download("sbx_fixture", "file.bin", &dest)
        .await
        .unwrap();
    assert!(receipt.release_confirmed);
    assert_eq!(std::fs::read(&dest).unwrap(), b"correct");
    assert_eq!(
        std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(matches!(
        s.client().download("sbx_fixture", "file.bin", &dest).await,
        Err(Error::File)
    ));
    assert_eq!(released.load(Ordering::SeqCst), 2);
    assert_eq!(std::fs::read(&dest).unwrap(), b"correct");
}
#[tokio::test]
async fn corrupt_or_expired_capture_is_not_published_or_recaptured() {
    for expire in [false, true] {
        let released = Arc::new(AtomicUsize::new(0));
        let s = file_server(true, expire, released.clone()).await;
        let dest = s.directory.path().join("download");
        let error = s
            .client()
            .download("sbx_fixture", "file.bin", &dest)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Integrity | Error::Http { status: 410, .. }
        ));
        assert!(!dest.exists());
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read_dir(s.directory.path()).unwrap().count(), 3); // Config, credential, CA only.
    }
}
#[tokio::test]
async fn inconsistent_binary_range_is_rejected() {
    let s = Server::start(Router::new().route(
        "/v1/sandboxes/sbx_fixture/files/captures",
        get(|| async { file_response(b"correct", &hex::encode(Sha256::digest(b"correct")), 6) }),
    ))
    .await;
    assert!(matches!(
        s.client()
            .read_captured_file(ReadCapturedFile {
                sandbox_id: "sbx_fixture",
                x_file_capture: "opaque",
                offset: Some(0),
                limit: Some(32768)
            })
            .await,
        Err(Error::Protocol)
    ));
}

#[tokio::test]
async fn multi_chunk_download_verifies_one_capture_and_reports_failed_release() {
    let data = Arc::new((0..40000).map(|n| (n % 256) as u8).collect::<Vec<_>>());
    let sha = hex::encode(Sha256::digest(data.as_slice()));
    let capture_sha = sha.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let count = reads.clone();
    let s = Server::start(Router::new().route("/v1/sandboxes/sbx_fixture/files/captures",post(move || {
        let sha=capture_sha.clone(); async move {
            (StatusCode::CREATED,json_response(json!({"capture":"single-capture","size":40000,"sha256":sha,"expires_unix_ms":4102444800000_i64,"chunk_size":32768,"simulated":true,"guest_reported":true})))
        }
    }).get(move |headers: HeaderMap, axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String,String>>| {
        let data=data.clone(); let sha=sha.clone(); let reads=reads.clone(); async move {
            assert_eq!(headers["x-file-capture"],"single-capture");
            let offset:usize=q["offset"].parse().unwrap();
            assert_eq!(offset,reads.fetch_add(1,Ordering::SeqCst)*32768);
            let next=(offset+32768).min(data.len());
            let mut r=([( "cache-control","no-store"),("content-type","application/octet-stream")],data[offset..next].to_vec()).into_response();
            for (k,v) in [("x-file-offset",offset.to_string()),("x-file-next-offset",next.to_string()),("x-file-size",data.len().to_string()),("x-file-eof",(next==data.len()).to_string()),("x-file-simulated","true".into()),("x-file-guest-reported","true".into()),("x-file-sha256",sha)] {
                r.headers_mut().insert(axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),v.parse().unwrap());
            }
            r
        }
    }).delete(|| async { StatusCode::SERVICE_UNAVAILABLE }))).await;
    let path = s.directory.path().join("large.bin");
    let result = s
        .client()
        .download("sbx_fixture", "large.bin", &path)
        .await
        .unwrap();
    assert!(!result.release_confirmed);
    assert_eq!(result.size, 40000);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(
        hex::encode(Sha256::digest(std::fs::read(path).unwrap())),
        result.sha256
    );
}

#[tokio::test]
async fn expired_response_retains_original_handle_but_never_reflects_arbitrary_ids() {
    const ID: &str = "op_019a9fad-3000-7000-8000-000000000001";
    let s=Server::start(Router::new().route("/v1/operations/{id}",get(|axum::extract::Path(id): axum::extract::Path<String>| async move {
        (StatusCode::GONE,axum::Json(json!({"title":"private-backend-text","status":410,"code":"response_expired","operation_id":if id=="original" { ID } else { "private-backend-text" }})))
    }))).await;
    let error = s
        .client()
        .get_operation(GetOperation {
            operation_id: "original",
        })
        .await
        .unwrap_err();
    assert_eq!(error.operation_id(), Some(ID));
    assert_eq!(error.problem_code(), Some("response_expired"));
    let error = s
        .client()
        .get_operation(GetOperation {
            operation_id: "invalid",
        })
        .await
        .unwrap_err();
    assert_eq!(error.operation_id(), None);
    assert!(!format!("{error:?} {error}").contains("private-backend-text"));
}

#[tokio::test]
async fn identity_is_normalized_credential_free_and_bound_to_the_loaded_client() {
    let s = Server::start(Router::new()).await;
    s.edit_config(|v| v["endpoint"] = "https://EXAMPLE.COM:443".into());
    let client = s.client();
    assert_eq!(client.identity().origin, "https://example.com/");
    assert_eq!(client.identity().project_id, "prj_test");
    assert_eq!(client.clone().identity(), client.identity());
    assert!(!format!("{:?}", client.identity()).contains("test-private-credential"));
    let credential = s.directory.path().join("credential.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&credential).unwrap()).unwrap();
    value["token"] = "rotated-private-credential".into();
    private(&credential, &serde_json::to_vec(&value).unwrap());
    assert_eq!(s.client().identity(), client.identity());
    value["project_id"] = "prj_other".into();
    private(&credential, &serde_json::to_vec(&value).unwrap());
    assert_eq!(client.identity().project_id, "prj_test");
    assert_ne!(s.client().identity(), client.identity());
    s.edit_config(|v| v["endpoint"] = "https://example.com:8443".into());
    assert_eq!(s.client().identity().origin, "https://example.com:8443/");
    assert_eq!(client.identity().origin, "https://example.com/");
}
