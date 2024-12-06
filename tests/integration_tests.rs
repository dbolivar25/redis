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

/// Start a new server instance and return a handle to the server and its local address.
async fn start_server() -> (JoinHandle<()>, SocketAddr) {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();

    let server = Server::new(tcp_listener);

    let join = tokio::spawn(async move {
        server.run().await.unwrap();
    });

    (join, addr)
}

/// Connect to the server at the given address and return a framed connection.
async fn connect_client(addr: SocketAddr) -> Framed<TcpStream, RESP3Codec> {
    let stream = TcpStream::connect(addr).await.unwrap();
    Framed::new(stream, RESP3Codec)
}

/// Sends a command to the server and returns the response as a `RESP3Value`.
async fn send_command(client: &mut Framed<TcpStream, RESP3Codec>, command: Request) -> RESP3Value {
    let encoded = encode_request(&command);
    client.send(encoded).await.unwrap();

    #[allow(clippy::let_and_return)]
    let response = client.next().await.unwrap().unwrap();
    response
}

/// Test the ping functionality.
#[tokio::test(start_paused = true)]
async fn test_ping() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let response = send_command(&mut client, Request::Ping).await;
    assert_eq!(response, RESP3Value::SimpleString("PONG".to_string()));

    join_handle.abort();
}

/// Test the echo functionality.
#[tokio::test(start_paused = true)]
async fn test_echo() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let message = RESP3Value::BulkString(b"Hello, World!".to_vec());
    let response = send_command(&mut client, Request::Echo(message.clone())).await;
    assert_eq!(response, message);

    join_handle.abort();
}

/// Test the set and get functionality.
#[tokio::test(start_paused = true)]
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

/// Test the set functionality with TTL.
#[tokio::test(start_paused = true)]
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

/// Test the del functionality.
#[tokio::test(start_paused = true)]
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

/// Test the del functionality with multiple keys.
#[tokio::test(start_paused = true)]
async fn test_multiple_clients() {
    let (join_handle, server_addr) = start_server().await;
    let mut client1 = connect_client(server_addr).await;
    let mut client2 = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"shared_key".to_vec());
    let value1 = RESP3Value::BulkString(b"value1".to_vec());
    let value2 = RESP3Value::BulkString(b"value2".to_vec());

    // Client 1 sets a value
    let response = send_command(
        &mut client1,
        Request::Set(key.clone(), value1.clone(), None),
    )
    .await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    // Client 2 reads the value
    let response = send_command(&mut client2, Request::Get(key.clone())).await;
    assert_eq!(response, value1);

    // Client 2 updates the value
    let response = send_command(
        &mut client2,
        Request::Set(key.clone(), value2.clone(), None),
    )
    .await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    // Client 1 reads the updated value
    let response = send_command(&mut client1, Request::Get(key)).await;
    assert_eq!(response, value2);

    join_handle.abort();
}

/// Test that concurrent operations do not break the server.
#[tokio::test(start_paused = true)]
async fn test_concurrent_operations() {
    let (join_handle, server_addr) = start_server().await;
    let mut client1 = connect_client(server_addr).await;
    let mut client2 = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"concurrent_key".to_vec());
    let value1 = RESP3Value::BulkString(b"value1".to_vec());
    let value2 = RESP3Value::BulkString(b"value2".to_vec());

    // Concurrent SET operations
    let (response1, response2) = tokio::join!(
        send_command(
            &mut client1,
            Request::Set(key.clone(), value1.clone(), None)
        ),
        send_command(
            &mut client2,
            Request::Set(key.clone(), value2.clone(), None)
        )
    );

    assert!(matches!(response1, RESP3Value::SimpleString(_)));
    assert!(matches!(response2, RESP3Value::SimpleString(_)));

    // Check the final value (it should be one of the two values)
    let response = send_command(&mut client1, Request::Get(key)).await;
    assert!(response == value1 || response == value2);

    join_handle.abort();
}

/// Test that a key with a short TTL can be updated before it expires.
#[tokio::test(start_paused = true)]
async fn test_ttl_race_condition() {
    let (join_handle, server_addr) = start_server().await;
    let mut client1 = connect_client(server_addr).await;
    let mut client2 = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"ttl_key".to_vec());
    let value = RESP3Value::BulkString(b"ttl_value".to_vec());

    // Set a key with a short TTL
    let response = send_command(
        &mut client1,
        Request::Set(key.clone(), value.clone(), Some(TTL::Milliseconds(50))),
    )
    .await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    // Wait for almost the entire TTL duration
    tokio::time::sleep(Duration::from_millis(45)).await;

    // Concurrent GET and SET operations
    let (get_response, set_response) = tokio::join!(
        send_command(&mut client1, Request::Get(key.clone())),
        send_command(&mut client2, Request::Set(key.clone(), value.clone(), None))
    );

    // The GET operation might return the value or Null, depending on timing
    assert!(get_response == value || get_response == RESP3Value::Null);

    // The SET operation should always succeed
    assert_eq!(set_response, RESP3Value::SimpleString("OK".to_string()));

    let response = send_command(&mut client1, Request::Get(key.clone())).await;
    assert_eq!(response, value);

    // Wait a bit longer to ensure the original TTL has expired
    tokio::time::sleep(Duration::from_millis(10)).await;

    // The key should still exist due to the SET operation
    let response = send_command(&mut client1, Request::Get(key)).await;
    assert_eq!(response, value);

    join_handle.abort();
}

/// Test that the server can handle large data. We send the max amount of data (8 KB) in a single command.
#[tokio::test(start_paused = true)]
async fn test_large_data() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"large_key".to_vec());
    let large_value = RESP3Value::BulkString(vec![b'a'; 8_000]); // 8 KB of data is max

    // Set large value
    let response = send_command(
        &mut client,
        Request::Set(key.clone(), large_value.clone(), None),
    )
    .await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    // Get large value
    let response = send_command(&mut client, Request::Get(key)).await;
    assert_eq!(response, large_value);

    join_handle.abort();
}

/// Test the shutdown and startup of the server.
#[tokio::test(start_paused = true)]
async fn test_server_restart() {
    let (join_handle, server_addr) = start_server().await;
    let mut client = connect_client(server_addr).await;

    let key = RESP3Value::BulkString(b"restart_key".to_vec());
    let value = RESP3Value::BulkString(b"restart_value".to_vec());

    // Set a value
    let response = send_command(&mut client, Request::Set(key.clone(), value.clone(), None)).await;
    assert_eq!(response, RESP3Value::SimpleString("OK".to_string()));

    // Stop the server
    join_handle.abort();

    // Start a new server instance
    let (new_join_handle, new_server_addr) = start_server().await;
    let mut new_client = connect_client(new_server_addr).await;

    // Try to get the value (it should be gone after restart)
    let response = send_command(&mut new_client, Request::Get(key)).await;
    assert_eq!(response, RESP3Value::Null);

    new_join_handle.abort();
}
