# Redis Server and Client

This project implements a Redis key-value store server and client in Rust. It provides a subset of Redis functionality, including basic operations like SET, GET, DEL, PING, and ECHO, with support for key expiration.

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
- Clustering and replication
- RESP3 protocol implementation
- Asynchronous server using Tokio
- CLI client for interacting with the server
- Logging support

## Project Structure

The project is organized into the following main components:

- `src/server_main.rs`: Entry point for the server application
- `src/cli_client_main.rs`: Entry point for the CLI client
- `src/tui_client_main.rs`: Entry point for the TUI client (to be implemented)
- `src/server/`: Server-specific modules
- `src/client/`: Client-specific modules
- `src/common/`: Shared modules between server and client

Key modules include:

- `server/connection.rs`: Handles individual client connections
- `server/connection_manager.rs`: Manages all client connections
- `server/kv_store.rs`: Implements the key-value store logic
- `common/codec.rs`: Implements encoding and decoding of requests
- `common/resp3.rs`: Implements RESP3 serialization and deserialization

## Installation

To build and run the project, you need to have Rust and Cargo installed. If you don't have them, you can install them from [https://rustup.rs/](https://rustup.rs/).

Clone the repository and build the project:

```bash
git clone https://github.com/dbolivar25/redis.git
cd redis
cargo build --release
```

## Usage

### Server

To start the server, run:

```bash
cargo run --bin redis
```

You can customize the host and port using the `--host` and `--port` options. These default to 127.0.0.1 and 6379, respectively.

### CLI Client

To use the CLI client, run:

```bash
cargo run --bin redis_cli -- <COMMAND>
```

Available commands:

- `ping`: Ping the server
- `echo <message>`: Echo a message
- `set <key> <value> [ttl]`: Set a key-value pair with an optional time to live subcommand
- `get <key>`: Get the value of a key
- `del <key>`: Delete a key

Examples:

```bash
cargo run --bin redis_cli -- set mykey myvalue
cargo run --bin redis_cli -- get mykey
cargo run --bin redis_cli -- del mykey
```

### TUI Client

The TUI (Text User Interface) client is not yet implemented. Once implemented, it will provide an interactive terminal-based interface for interacting with the server.

## Architecture

The project follows an asynchronous architecture using Tokio:

1. The server listens for incoming connections.
2. Each client connection is handled by a separate task.
3. The `ConnectionManager` coordinates multiple client connections.
4. The `KVStore` manages the in-memory key-value store and handles expiration.
5. Communication between components is done using Tokio channels.

## Protocol

The project implements the RESP3 protocol for communication between the client and server. The protocol supports the following data types:

- Simple String
- Simple Error
- Integer
- Bulk String
- Array
- Null

The `codec.rs` and `resp3.rs` modules handle encoding and decoding of requests.

## Logging

The project uses the `log4rs` crate for logging. The logging configuration is defined in `config/log4rs.yml`. Logs are written to both the console and a rolling file appender.

## Future Improvements

- Implement the TUI client for a more interactive experience
- Add support for more Redis commands (e.g., INCR, LPUSH, RPUSH)
- Implement persistence (AOF or RDB)
- Implement pub/sub functionality
