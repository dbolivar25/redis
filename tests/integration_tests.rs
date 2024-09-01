use futures::{SinkExt, StreamExt};
use redis::{
    common::{
        codec::{encode_request, RESP3Codec, Request, TTL},
        resp3::RESP3Value,
    },
    server::server::Server,
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::codec::Framed;

async fn start_server() -> (JoinHandle<()>, SocketAddr) {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    let server = Server::new(tcp_listener);

    let join = tokio::spawn(async move {
        server.run().await.unwrap();
    });

    (join, addr)
}

async fn connect_client(addr: SocketAddr) -> Framed<TcpStream, RESP3Codec> {
    let stream = TcpStream::connect(addr).await.unwrap();
    Framed::new(stream, RESP3Codec)
}

async fn send_command(client: &mut Framed<TcpStream, RESP3Codec>, command: Request) -> RESP3Value {
    let encoded = encode_request(&command);
    client.send(encoded).await.unwrap();
    let response = client.next().await.unwrap().unwrap();
    response
}

#[tokio::test]
async fn test_ping() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let response = send_command(&mut client, Request::Ping).await;
    assert_eq!(response, RESP3Value::SimpleString("PONG".to_string()));

    join_handle.abort();
}

#[tokio::test]
async fn test_echo() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let message = RESP3Value::BulkString(b"Hello, World!".to_vec());
    let response = send_command(&mut client, Request::Echo(message.clone())).await;
    assert_eq!(response, message);

    join_handle.abort();
}

#[tokio::test]
async fn test_set_get() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"mykey".to_vec());
    let value = RESP3Value::BulkString(b"myvalue".to_vec());

    let response = send_command(&mut client, Request::Set(key.clone(), value.clone(), None)).await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    let response = send_command(&mut client, Request::Get(key)).await;
    assert_eq!(response, value);

    join_handle.abort();
}

#[tokio::test]
async fn test_set_with_ttl() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"mykey".to_vec());
    let value = RESP3Value::BulkString(b"myvalue".to_vec());

    let response = send_command(
        &mut client,
        Request::Set(key.clone(), value, Some(TTL::Milliseconds(45))),
    )
    .await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = send_command(&mut client, Request::Get(key)).await;
    assert_eq!(response, RESP3Value::Null);

    join_handle.abort();
}

#[tokio::test]
async fn test_del() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"mykey".to_vec());
    let value = RESP3Value::BulkString(b"myvalue".to_vec());

    let response = send_command(&mut client, Request::Set(key.clone(), value, None)).await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    let response = send_command(&mut client, Request::Del(key.clone())).await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    let response = send_command(&mut client, Request::Get(key)).await;
    assert_eq!(response, RESP3Value::Null);

    join_handle.abort();
}
