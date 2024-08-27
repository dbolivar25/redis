use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use std::{fmt::Display, io};
use tokio_util::codec::{Decoder, Encoder};

use super::resp3::{decode_resp3, encode_resp3, RESP3Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    // Hello,
    Ping,
    Echo(RESP3Value),
    Set(RESP3Value, RESP3Value, Option<TTL>),
    Get(RESP3Value),
    Del(RESP3Value),
    PSync(RESP3Value, RESP3Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TTL {
    Milliseconds(u64),
    Seconds(u64),
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Request::Hello => write!(f, "HELLO"),
            Request::Ping => write!(f, "PING"),
            Request::Echo(value) => write!(f, "ECHO {}", value),
            Request::Set(key, value, ttl) => match ttl {
                Some(TTL::Milliseconds(ms)) => write!(f, "SET {} {} PX {}", key, value, ms),
                Some(TTL::Seconds(s)) => write!(f, "SET {} {} EX {}", key, value, s),
                None => write!(f, "SET {} {}", key, value),
            },
            Request::Get(key) => write!(f, "GET {}", key),
            Request::Del(key) => write!(f, "DEL {}", key),
            Request::PSync(repl_id, offset) => write!(f, "PSYNC {} {}", repl_id, offset),
        }
    }
}

pub struct RESP3Codec;

impl Encoder<RESP3Value> for RESP3Codec {
    type Error = io::Error;

    fn encode(&mut self, item: RESP3Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = encode_resp3(&item);
        dst.extend_from_slice(encoded.as_bytes());
        Ok(())
    }
}

impl Decoder for RESP3Codec {
    type Item = RESP3Value;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match decode_resp3(src) {
            Ok((resp3, rest)) => {
                let len = src.len() - rest.len();
                src.advance(len);
                Ok(Some(resp3))
            }
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }
}

pub fn encode_request(request: &Request) -> RESP3Value {
    match request {
        Request::Ping => RESP3Value::Array(vec![RESP3Value::BulkString(b"PING".to_vec())]),
        Request::Echo(message) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"ECHO".to_vec()),
            message.clone(),
        ]),
        Request::Set(key, value, ttl) => match ttl {
            Some(TTL::Milliseconds(ms)) => RESP3Value::Array(vec![
                RESP3Value::BulkString(b"SET".to_vec()),
                key.clone(),
                value.clone(),
                RESP3Value::BulkString(b"PX".to_vec()),
                RESP3Value::BulkString(ms.to_string().into_bytes()),
            ]),
            Some(TTL::Seconds(s)) => RESP3Value::Array(vec![
                RESP3Value::BulkString(b"SET".to_vec()),
                key.clone(),
                value.clone(),
                RESP3Value::BulkString(b"EX".to_vec()),
                RESP3Value::BulkString(s.to_string().into_bytes()),
            ]),
            None => RESP3Value::Array(vec![
                RESP3Value::BulkString(b"SET".to_vec()),
                key.clone(),
                value.clone(),
            ]),
        },
        Request::Get(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"GET".to_vec()), key.clone()])
        }
        Request::Del(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"DEL".to_vec()), key.clone()])
        }
        Request::PSync(repl_id, offset) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"PSYNC".to_vec()),
            repl_id.clone(),
            offset.clone(),
        ]),
    }
}

pub fn decode_request(data: RESP3Value) -> Result<Request> {
    if let RESP3Value::Array(data) = data {
        if data.is_empty() {
            bail!("Invalid request");
        }

        let command = match &data[0] {
            RESP3Value::BulkString(s) => s.as_slice(),
            _ => bail!("Invalid command"),
        };

        match command {
            b"PING" => decode_ping_request(&data),
            b"ECHO" => decode_echo_request(&data),
            b"SET" => decode_set_request(&data),
            b"GET" => decode_get_request(&data),
            b"DEL" => decode_del_request(&data),
            b"PSYNC" => decode_psync_request(&data),
            _ => bail!("Invalid command"),
        }
    } else {
        bail!("Invalid request");
    }
}

fn decode_ping_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 1 {
        bail!("Invalid PING request");
    }

    Ok(Request::Ping)
}

fn decode_echo_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid ECHO request");
    }

    Ok(Request::Echo(data[1].clone()))
}

fn decode_set_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() < 3 {
        bail!("Invalid SET request");
    }

    let key = data[1].clone();
    let value = data[2].clone();

    let ttl = if data.len() == 3 {
        None
    } else if data.len() == 5 {
        decode_ttl(&data[3], &data[4])?
    } else {
        bail!("Invalid SET request");
    };

    Ok(Request::Set(key, value, ttl))
}

fn decode_ttl(ttl_type: &RESP3Value, ttl_value: &RESP3Value) -> Result<Option<TTL>> {
    let ttl_type_str = match ttl_type {
        RESP3Value::BulkString(s) => s,
        _ => bail!("Invalid TTL type"),
    };

    let ttl_value = match ttl_value {
        RESP3Value::BulkString(s) => String::from_utf8_lossy(s).parse::<u64>()?,
        _ => bail!("Invalid TTL value"),
    };

    match ttl_type_str.as_slice() {
        b"PX" => Ok(Some(TTL::Milliseconds(ttl_value))),
        b"EX" => Ok(Some(TTL::Seconds(ttl_value))),
        _ => bail!("Invalid TTL type"),
    }
}

fn decode_get_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid GET request");
    }

    Ok(Request::Get(data[1].clone()))
}

fn decode_del_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid DEL request");
    }

    Ok(Request::Del(data[1].clone()))
}

fn decode_psync_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid PSYNC request");
    }

    if let RESP3Value::BulkString(_) = data[1] {
    } else {
        bail!("Invalid PSYNC request");
    }

    if let RESP3Value::BulkString(_) = data[2] {
    } else {
        bail!("Invalid PSYNC request");
    }

    Ok(Request::PSync(data[1].clone(), data[2].clone()))
}
