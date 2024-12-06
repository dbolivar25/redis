# Redis Server and Client

This project implements a Redis server and client in Rust. It provides a subset
of Redis functionality, including basic operations like SET, GET, DEL, PING, and
ECHO, with support for key expiration and replication.

## Table of Contents

1. [Features](#features)
2. [Project Structure](#project-structure)
3. [Installation](#installation)
4. [Usage](#usage)
   - [Server](#server)
   - [CLI Client](#cli-client)
   - [TUI Client](#tui-client)
5. [Architecture](#architecture)
6. [Protocol](#protocol)
7. [Logging](#logging)
8. [Future Improvements](#future-improvements)

## Features

- In-memory key-value store
- Support for SET, GET, DEL, PING, and ECHO commands
- Key expiration (TTL) support
- Basic replication (master-replica setup)
- RESP3 protocol implementation
- Asynchronous server using Tokio
- CLI client for interacting with the server
- TUI client for interactive sessions
- Logging support

## Demo Video

- [Demo](https://asciinema.org/a/R7EQhDxdcRu1mgS82D2EOL5hO): 3 Minutes

- **Server Setup**

  - Master server starts on default port 6379
  - Replica server launches on port 6380, connecting to master
  - Both servers display successful startup logs

- **TUI Client Interaction**

  - Terminal-based UI connects to master server
  - Demonstrates basic Redis commands:
    - `PING` command shows server responsiveness
    - `SET hello world` demonstrates string storage
    - `GET hello` retrieves the stored value
    - `SET goodbye world EX 5` shows TTL functionality

- **CLI Client Operations**

  - Executes commands against both servers:
    - Direct SET/GET operations on master
    - Verification of replication on replica server
    - Shows identical data across both instances

- **Replication Features**

  - Changes on master instantly reflect on replica
  - Replica maintains read-only status
  - Demonstrates failover readiness

- **Data Management**

  - Shows key expiration in action
  - Demonstrates atomic operations

- **Protocol Handling**
  - RESP3 protocol messages visible in debug mode
  - Shows proper error handling
  - Demonstrates protocol's type system in action

## Project Structure

The project is organized into the following main components:

- `src/server/main.rs`: Entry point for the server application
- `src/cli_client/main.rs`: Entry point for the CLI client
- `src/tui_client/main.rs`: Entry point for the TUI client
- `src/server/`: Server-specific modules
- `src/common/`: Shared modules between server and client

Key modules include:

- `server/connection.rs`: Handles individual client connections
- `server/connection_manager.rs`: Manages all client connections
- `server/kv_store.rs`: Implements the key-value store logic
- `common/codec.rs`: Implements encoding and decoding of requests
- `common/resp3.rs`: Implements RESP3 serialization and deserialization

## Installation

To build and run the project, you need to have Rust and Cargo installed. If you
don't have them, you can install them from
[https://rustup.rs/](https://rustup.rs/).

Clone the repository and build the project:

```bash
git clone git@github.com:dbolivar25/redis.git
cd redis
cargo build --release
```

## Documentation

To generate the documentation for the project, run:

```bash
cargo doc --no-deps --open
```

This will generate and open the documentation in your default web browser.

## Usage

### Server

To start the server, run:

```bash
cargo run --release --bin redis
```

You can customize the host and port using the `--host` and `--port` options. To
set up a replica, use the `--master` option:

```bash
cargo run --release --bin redis -- --host 127.0.0.1 --port 6380 --master 127.0.0.1:6379
```

### CLI Client

To use the CLI client, run:

```bash
cargo run --release --bin redis_cli -- <COMMAND>
```

Available commands:

- `ping`: Ping the server
- `echo <message>`: Echo a message
- `set <key> <value> [EX seconds | PX milliseconds]`: Set a key-value pair with
  an optional time to live
- `get <key>`: Get the value of a key
- `del <key>`: Delete a key

Examples:

```bash
cargo run --release --bin redis_cli -- set mykey myvalue
cargo run --release --bin redis_cli -- set mykey myvalue ex 60
cargo run --release --bin redis_cli -- get mykey
cargo run --release --bin redis_cli -- del mykey
```

### TUI Client

To start the TUI (Text User Interface) client, run:

```bash
cargo run --release --bin redis_tui
```

This provides a terminal-based interface for interacting with the server. Type
commands as you would in the Redis CLI.

## Architecture

The project follows an asynchronous architecture using Tokio:

1. The server listens for incoming connections.
2. Each client connection is handled by a separate task.
3. The `ConnectionManager` coordinates multiple client connections and manages
   replication.
4. The `KVStore` manages the in-memory key-value store and handles expiration.
5. Communication between components is done using Tokio channels.

## Protocol

The project implements the RESP3 protocol for communication between the client
and server. The protocol supports the following data types:

- Simple String
- Simple Error
- Integer
- Bulk String
- Array
- Null

The `codec.rs` and `resp3.rs` modules handle encoding and decoding of requests.

## Logging

The project uses the `log4rs` crate for logging. The logging configuration is
defined in `config/log4rs.yml`. Logs are written to both the console and a
rolling file appender.

## Future Improvements

- Add support for more Redis commands (e.g., INCR, LPUSH, RPUSH)
- Implement persistence (AOF or RDB)
- Implement pub/sub functionality
- Enhance replication with full dataset synchronization
- Add clustering support
- Implement Redis Streams
- Add authentication and access control
