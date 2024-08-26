use std::net::SocketAddr;

use super::kv_store::KVStoreHandle;
use crate::common::{
    protocol::{Request, ServerProtoCodec},
    resp3::RESP3Value,
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub async fn handle(
    kv_store: KVStoreHandle,
    addr: SocketAddr,
    mut sink: SplitSink<Framed<TcpStream, ServerProtoCodec>, RESP3Value>,
    mut stream: SplitStream<Framed<TcpStream, ServerProtoCodec>>,
) {
    log::info!("Accepted connection from {}", addr);

    while let Some(result) = stream.next().await {
        let request = match result {
            Ok(request) => request,
            Err(err) => {
                log::error!("Failed to read from socket: {:?}", err);
                break;
            }
        };

        log::info!("Received request: {}", request);

        let response = match request {
            Request::Ping => RESP3Value::SimpleString("PONG".to_string()),
            Request::Echo(value) => value,
            Request::Set(key, value, expiration) => {
                let res = kv_store.set(key, value, expiration).await;

                match res {
                    Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                    Err(err) => {
                        log::error!("Failed to set key: {:?}", err);
                        continue;
                    }
                }
            }
            Request::Get(key) => {
                let res = kv_store.get(key).await;

                match res {
                    Ok(value) => value.unwrap_or(RESP3Value::Null),
                    Err(err) => {
                        log::error!("Failed to get key: {:?}", err);
                        continue;
                    }
                }
            }
            Request::Del(key) => {
                let res = kv_store.del(key).await;

                match res {
                    Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                    Err(err) => {
                        log::error!("Failed to del key: {:?}", err);
                        continue;
                    }
                }
            }
        };

        log::info!("Sending response: {}", response);

        let _ = sink
            .send(response)
            .await
            .inspect_err(|err| log::error!("Failed to send response: {:?}", err));
    }

    log::info!("Connection closed for {}", addr);
}
