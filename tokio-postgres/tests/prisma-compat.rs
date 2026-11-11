//! Prisma compatibility checks against an in-memory wire-protocol server.
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_postgres::config::SslMode;
use tokio_postgres::types::Kind;
use tokio_postgres::{Config, NoTls};

#[derive(Default)]
struct Observed {
    startup: HashMap<String, String>,
    metadata_prepares: usize,
    metadata_queries: usize,
    closes: usize,
}

async fn frame(stream: &mut DuplexStream, tag: u8, body: &[u8]) {
    stream.write_u8(tag).await.unwrap();
    stream.write_u32(body.len() as u32 + 4).await.unwrap();
    stream.write_all(body).await.unwrap();
}

fn cstring<'a>(body: &mut &'a [u8]) -> &'a str {
    let end = body.iter().position(|byte| *byte == 0).unwrap();
    let value = std::str::from_utf8(&body[..end]).unwrap();
    *body = &body[end + 1..];
    value
}

async fn server(mut stream: DuplexStream) -> Observed {
    let mut observed = Observed::default();
    let size = stream.read_u32().await.unwrap();
    let mut startup = vec![0; size as usize - 4];
    stream.read_exact(&mut startup).await.unwrap();
    assert_eq!(&startup[..4], &196608_u32.to_be_bytes());
    let mut params = &startup[4..];
    while params != [0] {
        let key = cstring(&mut params).to_owned();
        let value = cstring(&mut params).to_owned();
        observed.startup.insert(key, value);
    }
    frame(&mut stream, b'R', &0_u32.to_be_bytes()).await;
    frame(&mut stream, b'Z', b"I").await;

    let mut statements = HashMap::new();
    let mut bound_metadata = false;
    loop {
        let tag = stream.read_u8().await.unwrap();
        let size = stream.read_u32().await.unwrap();
        let mut body = vec![0; size as usize - 4];
        stream.read_exact(&mut body).await.unwrap();
        let mut body = body.as_slice();
        match tag {
            b'Q' => {
                let query = cstring(&mut body);
                if query == "DEALLOCATE ALL" {
                    statements.clear();
                }
                frame(&mut stream, b'C', b"SELECT 0\0").await;
                frame(&mut stream, b'Z', b"I").await;
            }
            b'P' => {
                let name = cstring(&mut body).to_owned();
                let query = cstring(&mut body);
                let metadata = query.contains("FROM pg_catalog.pg_type");
                observed.metadata_prepares += usize::from(metadata);
                statements.insert(name, metadata);
                frame(&mut stream, b'1', &[]).await;
            }
            b'D' => {
                assert_eq!(body[0], b'S');
                body = &body[1..];
                let metadata = statements[cstring(&mut body)];
                let oid = if metadata { 26_u32 } else { 60000_u32 };
                let mut params = 1_i16.to_be_bytes().to_vec();
                params.extend_from_slice(&oid.to_be_bytes());
                frame(&mut stream, b't', &params).await;
                if metadata {
                    let mut columns = 7_i16.to_be_bytes().to_vec();
                    for (name, oid) in [
                        ("typname", 25_u32),
                        ("typtype", 18),
                        ("typelem", 26),
                        ("rngsubtype", 26),
                        ("typbasetype", 26),
                        ("nspname", 25),
                        ("typrelid", 26),
                    ] {
                        columns.extend_from_slice(name.as_bytes());
                        columns.push(0);
                        columns.extend_from_slice(&0_u32.to_be_bytes());
                        columns.extend_from_slice(&0_i16.to_be_bytes());
                        columns.extend_from_slice(&oid.to_be_bytes());
                        columns.extend_from_slice(&(-1_i16).to_be_bytes());
                        columns.extend_from_slice(&(-1_i32).to_be_bytes());
                        columns.extend_from_slice(&1_i16.to_be_bytes());
                    }
                    frame(&mut stream, b'T', &columns).await;
                } else {
                    frame(&mut stream, b'n', &[]).await;
                }
            }
            b'B' => {
                cstring(&mut body);
                bound_metadata = statements[cstring(&mut body)];
                frame(&mut stream, b'2', &[]).await;
            }
            b'E' => {
                assert!(bound_metadata);
                observed.metadata_queries += 1;
                let zero = 0_u32.to_be_bytes();
                let mut row = 7_i16.to_be_bytes().to_vec();
                for value in [
                    Some(b"prisma_type".as_slice()),
                    Some(b"b".as_slice()),
                    Some(zero.as_slice()),
                    None,
                    Some(zero.as_slice()),
                    Some(b"custom_schema".as_slice()),
                    Some(zero.as_slice()),
                ] {
                    if let Some(value) = value {
                        row.extend_from_slice(&(value.len() as i32).to_be_bytes());
                        row.extend_from_slice(value);
                    } else {
                        row.extend_from_slice(&(-1_i32).to_be_bytes());
                    }
                }
                frame(&mut stream, b'D', &row).await;
                frame(&mut stream, b'C', b"SELECT 1\0").await;
            }
            b'C' => {
                assert_eq!(body[0], b'S');
                observed.closes += 1;
                frame(&mut stream, b'3', &[]).await;
            }
            b'S' => frame(&mut stream, b'Z', b"I").await,
            b'X' => return observed,
            _ => panic!("unexpected frontend message {tag}"),
        }
    }
}

async fn exercise(pgbouncer: bool, clear_types: bool) -> Observed {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (client_stream, server_stream) = tokio::io::duplex(8192);
        let server = tokio::spawn(server(server_stream));
        let mut config = Config::new();
        assert!(!config.get_pgbouncer_mode());
        assert_eq!(config.get_search_path(), None);
        config.user("prisma").ssl_mode(SslMode::Disable);
        config.pgbouncer_mode(pgbouncer);
        if pgbouncer {
            config.search_path("\"tenant\"".to_owned());
        }
        let (client, connection) = config.connect_raw(client_stream, NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        for index in 0..2 {
            if index == 1 {
                if pgbouncer {
                    client.batch_execute("DEALLOCATE ALL").await.unwrap();
                } else if clear_types {
                    client.clear_type_cache();
                }
            }
            let statement = client.prepare("SELECT $1").await.unwrap();
            assert!(!statement.name().is_empty());
            let ty = &statement.params()[0];
            assert_eq!(ty.name(), "prisma_type");
            assert_eq!(ty.schema(), "custom_schema");
            assert!(matches!(ty.kind(), Kind::Simple));
            drop(statement);
            // Exercise another request after closing a prepared statement.
            client.batch_execute("SELECT 1").await.unwrap();
        }
        drop(client);
        connection.await.unwrap().unwrap();
        server.await.unwrap()
    })
    .await
    .expect("wire-protocol compatibility check timed out")
}

#[tokio::test]
async fn default_mode_reuses_type_metadata_and_omits_search_path() {
    let observed = exercise(false, false).await;
    assert!(!observed.startup.contains_key("search_path"));
    assert_eq!(observed.metadata_prepares, 1);
    assert_eq!(observed.metadata_queries, 1);
    assert_eq!(observed.closes, 2);
}

#[tokio::test]
async fn clearing_types_preserves_the_cached_metadata_statement() {
    let observed = exercise(false, true).await;
    assert_eq!(observed.metadata_prepares, 1);
    assert_eq!(observed.metadata_queries, 2);
    assert_eq!(observed.closes, 2);
}

#[tokio::test]
async fn pgbouncer_mode_refreshes_metadata_after_deallocate_and_sends_search_path() {
    let observed = exercise(true, false).await;
    assert_eq!(observed.startup["search_path"], "\"tenant\"");
    assert_eq!(observed.metadata_prepares, 2);
    assert_eq!(observed.metadata_queries, 2);
    assert_eq!(observed.closes, 4);
}
