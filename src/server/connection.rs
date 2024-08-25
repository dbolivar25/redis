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
    mut sink: SplitSink<Framed<TcpStream, ServerProtoCodec>, RESP3Value>,
    mut stream: SplitStream<Framed<TcpStream, ServerProtoCodec>>,
) {
    while let Some(res) = stream.next().await {
        let request = match res {
            Ok(request) => request,
            Err(err) => {
                eprintln!("Failed to read from socket: {:?}", err);
                break;
            }
        };

        let response = match request {
            Request::Ping => RESP3Value::SimpleString("PONG".to_string()),
            Request::Echo(value) => value,
            Request::Set(key, value, expiration) => {
                let res = kv_store.set(key, value, expiration).await;

                match res {
                    Ok(_) => RESP3Value::SimpleString("OK".to_string()),
                    Err(err) => {
                        eprintln!("Failed to set key: {:?}", err);
                        continue;
                    }
                }
            }
            Request::Get(key) => {
                let res = kv_store.get(key).await;

                match res {
                    Ok(value) => value.unwrap_or(RESP3Value::Null),
                    Err(err) => {
                        eprintln!("Failed to get key: {:?}", err);
                        continue;
                    }
                }
            }
            Request::Del(key) => {
                let res = kv_store.del(key).await;

                match res {
                    Ok(value) => value.unwrap_or(RESP3Value::Null),
                    Err(err) => {
                        eprintln!("Failed to del key: {:?}", err);
                        continue;
                    }
                }
            }
        };

        let _ = sink
            .send(response)
            .await
            .inspect_err(|err| eprintln!("Failed to send response: {:?}", err));
    }
}
