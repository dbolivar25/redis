use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use std::{fmt::Display, io};
use tokio_util::codec::{Decoder, Encoder};

use super::resp3::{decode_resp3, encode_resp3, RESP3Value};

#[derive(Debug, PartialEq)]
pub enum Request {
    // Hello,
    Ping,
    Echo(RESP3Value),
    Set(RESP3Value, RESP3Value, Option<TTL>),
    Get(RESP3Value),
    Del(RESP3Value),
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
        }
    }
}

pub struct ClientProtoCodec;

impl Encoder<Request> for ClientProtoCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = encode_request(&item);
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

impl Decoder for ClientProtoCodec {
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
            Err(e) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
        }
    }
}

pub struct ServerProtoCodec;

impl Encoder<RESP3Value> for ServerProtoCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RESP3Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = encode_resp3(&item);
        dst.extend_from_slice(encoded.as_bytes());
        Ok(())
    }
}

impl Decoder for ServerProtoCodec {
    type Item = Request;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        match decode_request(src) {
            Ok((request, rest)) => {
                let len = src.len() - rest.len();
                src.advance(len);
                Ok(Some(request))
            }
            Err(e) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
        }
    }
}

pub fn encode_request(request: &Request) -> Vec<u8> {
    match request {
        Request::Ping => {
            let resp3 = RESP3Value::Array(vec![RESP3Value::BulkString(b"PING".to_vec())]);

            let encoded = encode_resp3(&resp3);
            encoded.into_bytes()
        }
        Request::Echo(message) => {
            let resp3 = RESP3Value::Array(vec![
                RESP3Value::BulkString(b"ECHO".to_vec()),
                message.clone(),
            ]);

            let encoded = encode_resp3(&resp3);
            encoded.into_bytes()
        }
        Request::Set(key, value, ttl) => {
            let resp3 = match ttl {
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
            };

            let encoded = encode_resp3(&resp3);
            encoded.into_bytes()
        }
        Request::Get(key) => {
            let resp3 =
                RESP3Value::Array(vec![RESP3Value::BulkString(b"GET".to_vec()), key.clone()]);

            let encoded = encode_resp3(&resp3);
            encoded.into_bytes()
        }
        Request::Del(key) => {
            let resp3 =
                RESP3Value::Array(vec![RESP3Value::BulkString(b"DEL".to_vec()), key.clone()]);

            let encoded = encode_resp3(&resp3);
            encoded.into_bytes()
        }
    }
}

pub fn decode_request(input: &[u8]) -> Result<(Request, &[u8])> {
    let (resp3, rest) = decode_resp3(input)?;

    let request = match resp3 {
        RESP3Value::Array(data) => decode_request_array(data)?,
        _ => bail!("Invalid request"),
    };

    Ok((request, rest))
}

fn decode_request_array(data: Vec<RESP3Value>) -> Result<Request> {
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
        _ => bail!("Invalid command"),
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
