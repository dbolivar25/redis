use super::resp3::{decode_resp3, encode_resp3, RESP3Value};
use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use std::time::Duration;
use std::{fmt::Display, io};
use tokio_util::codec::{Decoder, Encoder};

/// Represents a request that can be sent to the server.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    Ping,
    Echo(RESP3Value),
    Set(RESP3Value, RESP3Value, Option<TTL>),
    Get(RESP3Value),
    Del(RESP3Value),
    PSync(RESP3Value, RESP3Value),
    Incr(RESP3Value),
    Decr(RESP3Value),
    IncrBy(RESP3Value, i64),
    DecrBy(RESP3Value, i64),
    Append(RESP3Value, RESP3Value),
    StrLen(RESP3Value),
    Exists(Vec<RESP3Value>),
    Keys(RESP3Value),
    Rename(RESP3Value, RESP3Value),
    Type(RESP3Value),
    Expire(RESP3Value, u64),
    PExpire(RESP3Value, u64),
    ExpireAt(RESP3Value, u64),
    Ttl(RESP3Value),
    PTtl(RESP3Value),
    Persist(RESP3Value),
    MGet(Vec<RESP3Value>),
    MSet(Vec<(RESP3Value, RESP3Value)>),
    DbSize,
    FlushDb,
}

/// Represents a time-to-live value for a key in the KV store.
#[derive(Debug, Clone, PartialEq)]
pub enum TTL {
    Milliseconds(u64),
    Seconds(u64),
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::Ping => write!(f, "PING"),
            Request::Echo(value) => write!(f, "ECHO {value}"),
            Request::Set(key, value, ttl) => match ttl {
                Some(TTL::Milliseconds(ms)) => write!(f, "SET {key} {value} PX {ms}"),
                Some(TTL::Seconds(s)) => write!(f, "SET {key} {value} EX {s}"),
                None => write!(f, "SET {key} {value}"),
            },
            Request::Get(key) => write!(f, "GET {key}"),
            Request::Del(key) => write!(f, "DEL {key}"),
            Request::PSync(repl_id, offset) => write!(f, "PSYNC {repl_id} {offset}"),
            Request::Incr(key) => write!(f, "INCR {key}"),
            Request::Decr(key) => write!(f, "DECR {key}"),
            Request::IncrBy(key, delta) => write!(f, "INCRBY {key} {delta}"),
            Request::DecrBy(key, delta) => write!(f, "DECRBY {key} {delta}"),
            Request::Append(key, value) => write!(f, "APPEND {key} {value}"),
            Request::StrLen(key) => write!(f, "STRLEN {key}"),
            Request::Exists(keys) => {
                write!(f, "EXISTS")?;
                for key in keys {
                    write!(f, " {key}")?;
                }
                Ok(())
            }
            Request::Keys(pattern) => write!(f, "KEYS {pattern}"),
            Request::Rename(key, newkey) => write!(f, "RENAME {key} {newkey}"),
            Request::Type(key) => write!(f, "TYPE {key}"),
            Request::Expire(key, secs) => write!(f, "EXPIRE {key} {secs}"),
            Request::PExpire(key, ms) => write!(f, "PEXPIRE {key} {ms}"),
            Request::ExpireAt(key, ts) => write!(f, "EXPIREAT {key} {ts}"),
            Request::Ttl(key) => write!(f, "TTL {key}"),
            Request::PTtl(key) => write!(f, "PTTL {key}"),
            Request::Persist(key) => write!(f, "PERSIST {key}"),
            Request::MGet(keys) => {
                write!(f, "MGET")?;
                for key in keys {
                    write!(f, " {key}")?;
                }
                Ok(())
            }
            Request::MSet(pairs) => {
                write!(f, "MSET")?;
                for (key, value) in pairs {
                    write!(f, " {key} {value}")?;
                }
                Ok(())
            }
            Request::DbSize => write!(f, "DBSIZE"),
            Request::FlushDb => write!(f, "FLUSHDB"),
        }
    }
}

/// A unit struct that represents the codec for the RESP3 protocol.
pub struct RESP3Codec;

impl Encoder<RESP3Value> for RESP3Codec {
    type Error = io::Error;

    fn encode(&mut self, item: RESP3Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = encode_resp3(&item);
        dst.extend_from_slice(&encoded);
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
            Ok(Some((resp3, rest))) => {
                let consumed = src.len() - rest.len();
                src.advance(consumed);
                Ok(Some(resp3))
            }
            Ok(None) => Ok(None),
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
        Request::Incr(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"INCR".to_vec()), key.clone()])
        }
        Request::Decr(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"DECR".to_vec()), key.clone()])
        }
        Request::IncrBy(key, delta) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"INCRBY".to_vec()),
            key.clone(),
            RESP3Value::BulkString(delta.to_string().into_bytes()),
        ]),
        Request::DecrBy(key, delta) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"DECRBY".to_vec()),
            key.clone(),
            RESP3Value::BulkString(delta.to_string().into_bytes()),
        ]),
        Request::Append(key, value) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"APPEND".to_vec()),
            key.clone(),
            value.clone(),
        ]),
        Request::StrLen(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"STRLEN".to_vec()), key.clone()])
        }
        Request::Exists(keys) => {
            let mut arr = vec![RESP3Value::BulkString(b"EXISTS".to_vec())];
            arr.extend(keys.iter().cloned());
            RESP3Value::Array(arr)
        }
        Request::Keys(pattern) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"KEYS".to_vec()),
            pattern.clone(),
        ]),
        Request::Rename(key, newkey) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"RENAME".to_vec()),
            key.clone(),
            newkey.clone(),
        ]),
        Request::Type(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"TYPE".to_vec()), key.clone()])
        }
        Request::Expire(key, secs) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"EXPIRE".to_vec()),
            key.clone(),
            RESP3Value::BulkString(secs.to_string().into_bytes()),
        ]),
        Request::PExpire(key, ms) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"PEXPIRE".to_vec()),
            key.clone(),
            RESP3Value::BulkString(ms.to_string().into_bytes()),
        ]),
        Request::ExpireAt(key, ts) => RESP3Value::Array(vec![
            RESP3Value::BulkString(b"EXPIREAT".to_vec()),
            key.clone(),
            RESP3Value::BulkString(ts.to_string().into_bytes()),
        ]),
        Request::Ttl(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"TTL".to_vec()), key.clone()])
        }
        Request::PTtl(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"PTTL".to_vec()), key.clone()])
        }
        Request::Persist(key) => {
            RESP3Value::Array(vec![RESP3Value::BulkString(b"PERSIST".to_vec()), key.clone()])
        }
        Request::MGet(keys) => {
            let mut arr = vec![RESP3Value::BulkString(b"MGET".to_vec())];
            arr.extend(keys.iter().cloned());
            RESP3Value::Array(arr)
        }
        Request::MSet(pairs) => {
            let mut arr = vec![RESP3Value::BulkString(b"MSET".to_vec())];
            for (key, value) in pairs {
                arr.push(key.clone());
                arr.push(value.clone());
            }
            RESP3Value::Array(arr)
        }
        Request::DbSize => RESP3Value::Array(vec![RESP3Value::BulkString(b"DBSIZE".to_vec())]),
        Request::FlushDb => RESP3Value::Array(vec![RESP3Value::BulkString(b"FLUSHDB".to_vec())]),
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
            b"INCR" => decode_incr_request(&data),
            b"DECR" => decode_decr_request(&data),
            b"INCRBY" => decode_incrby_request(&data),
            b"DECRBY" => decode_decrby_request(&data),
            b"APPEND" => decode_append_request(&data),
            b"STRLEN" => decode_strlen_request(&data),
            b"EXISTS" => decode_exists_request(&data),
            b"KEYS" => decode_keys_request(&data),
            b"RENAME" => decode_rename_request(&data),
            b"TYPE" => decode_type_request(&data),
            b"EXPIRE" => decode_expire_request(&data),
            b"PEXPIRE" => decode_pexpire_request(&data),
            b"EXPIREAT" => decode_expireat_request(&data),
            b"TTL" => decode_ttl_request(&data),
            b"PTTL" => decode_pttl_request(&data),
            b"PERSIST" => decode_persist_request(&data),
            b"MGET" => decode_mget_request(&data),
            b"MSET" => decode_mset_request(&data),
            b"DBSIZE" => decode_dbsize_request(&data),
            b"FLUSHDB" => decode_flushdb_request(&data),
            _ => bail!("Invalid command"),
        }
    } else {
        bail!("Invalid request");
    }
}

/// Decodes a ping Request from an array of RESP3 values.
fn decode_ping_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 1 {
        bail!("Invalid PING request");
    }

    Ok(Request::Ping)
}

/// Decodes an echo Request from an array of RESP3 values.
fn decode_echo_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid ECHO request");
    }

    Ok(Request::Echo(data[1].clone()))
}

/// Decodes a set Request from an array of RESP3 values.
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

/// Decodes a TTL value from an array of RESP3 values.
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

/// Decodes a get Request from an array of RESP3 values.
fn decode_get_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid GET request");
    }

    Ok(Request::Get(data[1].clone()))
}

/// Decodes a del Request from an array of RESP3 values.
fn decode_del_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid DEL request");
    }

    Ok(Request::Del(data[1].clone()))
}

/// Decodes a PSYNC Request from an array of RESP3 values.
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

fn decode_incr_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid INCR request");
    }
    Ok(Request::Incr(data[1].clone()))
}

fn decode_decr_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid DECR request");
    }
    Ok(Request::Decr(data[1].clone()))
}

fn decode_incrby_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid INCRBY request");
    }
    let delta = match &data[2] {
        RESP3Value::BulkString(s) => String::from_utf8_lossy(s).parse::<i64>()?,
        RESP3Value::Integer(n) => *n,
        _ => bail!("Invalid INCRBY delta"),
    };
    Ok(Request::IncrBy(data[1].clone(), delta))
}

fn decode_decrby_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid DECRBY request");
    }
    let delta = match &data[2] {
        RESP3Value::BulkString(s) => String::from_utf8_lossy(s).parse::<i64>()?,
        RESP3Value::Integer(n) => *n,
        _ => bail!("Invalid DECRBY delta"),
    };
    Ok(Request::DecrBy(data[1].clone(), delta))
}

fn decode_append_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid APPEND request");
    }
    Ok(Request::Append(data[1].clone(), data[2].clone()))
}

fn decode_strlen_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid STRLEN request");
    }
    Ok(Request::StrLen(data[1].clone()))
}

fn decode_exists_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() < 2 {
        bail!("Invalid EXISTS request");
    }
    Ok(Request::Exists(data[1..].to_vec()))
}

fn decode_keys_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid KEYS request");
    }
    Ok(Request::Keys(data[1].clone()))
}

fn decode_rename_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid RENAME request");
    }
    Ok(Request::Rename(data[1].clone(), data[2].clone()))
}

fn decode_type_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid TYPE request");
    }
    Ok(Request::Type(data[1].clone()))
}

fn decode_expire_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid EXPIRE request");
    }
    let secs = match &data[2] {
        RESP3Value::BulkString(s) => String::from_utf8_lossy(s).parse::<u64>()?,
        RESP3Value::Integer(n) => *n as u64,
        _ => bail!("Invalid EXPIRE seconds"),
    };
    Ok(Request::Expire(data[1].clone(), secs))
}

fn decode_pexpire_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid PEXPIRE request");
    }
    let ms = match &data[2] {
        RESP3Value::BulkString(s) => String::from_utf8_lossy(s).parse::<u64>()?,
        RESP3Value::Integer(n) => *n as u64,
        _ => bail!("Invalid PEXPIRE milliseconds"),
    };
    Ok(Request::PExpire(data[1].clone(), ms))
}

fn decode_expireat_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 3 {
        bail!("Invalid EXPIREAT request");
    }
    let ts = match &data[2] {
        RESP3Value::BulkString(s) => String::from_utf8_lossy(s).parse::<u64>()?,
        RESP3Value::Integer(n) => *n as u64,
        _ => bail!("Invalid EXPIREAT timestamp"),
    };
    Ok(Request::ExpireAt(data[1].clone(), ts))
}

fn decode_ttl_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid TTL request");
    }
    Ok(Request::Ttl(data[1].clone()))
}

fn decode_pttl_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid PTTL request");
    }
    Ok(Request::PTtl(data[1].clone()))
}

fn decode_persist_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 2 {
        bail!("Invalid PERSIST request");
    }
    Ok(Request::Persist(data[1].clone()))
}

fn decode_mget_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() < 2 {
        bail!("Invalid MGET request");
    }
    Ok(Request::MGet(data[1..].to_vec()))
}

fn decode_mset_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() < 3 || (data.len() - 1) % 2 != 0 {
        bail!("Invalid MSET request");
    }
    let pairs: Vec<(RESP3Value, RESP3Value)> = data[1..]
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();
    Ok(Request::MSet(pairs))
}

fn decode_dbsize_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 1 {
        bail!("Invalid DBSIZE request");
    }
    Ok(Request::DbSize)
}

fn decode_flushdb_request(data: &[RESP3Value]) -> Result<Request> {
    if data.len() != 1 {
        bail!("Invalid FLUSHDB request");
    }
    Ok(Request::FlushDb)
}

pub fn encode_snapshot(entries: &[(RESP3Value, RESP3Value, Option<Duration>)]) -> RESP3Value {
    let items: Vec<RESP3Value> = entries
        .iter()
        .map(|(key, value, ttl)| {
            let ttl_ms = ttl.map(|d| d.as_millis() as i64).unwrap_or(-1);
            RESP3Value::Array(vec![
                key.clone(),
                value.clone(),
                RESP3Value::Integer(ttl_ms),
            ])
        })
        .collect();
    RESP3Value::Array(items)
}

pub fn decode_snapshot(
    data: RESP3Value,
) -> Result<Vec<(RESP3Value, RESP3Value, Option<Duration>)>> {
    let RESP3Value::Array(items) = data else {
        bail!("Snapshot must be an array");
    };

    items
        .into_iter()
        .map(|item| {
            let RESP3Value::Array(mut parts) = item else {
                bail!("Snapshot entry must be an array");
            };
            if parts.len() != 3 {
                bail!("Snapshot entry must have 3 elements");
            }

            let ttl_val = parts.pop().unwrap();
            let value = parts.pop().unwrap();
            let key = parts.pop().unwrap();

            let ttl = match ttl_val {
                RESP3Value::Integer(ms) if ms > 0 => Some(Duration::from_millis(ms as u64)),
                _ => None,
            };

            Ok((key, value, ttl))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_request() {
        let request = Request::Ping;
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![RESP3Value::BulkString(b"PING".to_vec())])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_echo_request() {
        let message = RESP3Value::BulkString(b"Hello, World!".to_vec());
        let request = Request::Echo(message.clone());
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"ECHO".to_vec()),
                message.clone()
            ])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_set_request_without_ttl() {
        let key = RESP3Value::BulkString(b"mykey".to_vec());
        let value = RESP3Value::BulkString(b"myvalue".to_vec());
        let request = Request::Set(key.clone(), value.clone(), None);
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"SET".to_vec()),
                key.clone(),
                value.clone()
            ])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_set_request_with_seconds_ttl() {
        let key = RESP3Value::BulkString(b"mykey".to_vec());
        let value = RESP3Value::BulkString(b"myvalue".to_vec());
        let ttl = Some(TTL::Seconds(60));
        let request = Request::Set(key.clone(), value.clone(), ttl.clone());
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"SET".to_vec()),
                key.clone(),
                value.clone(),
                RESP3Value::BulkString(b"EX".to_vec()),
                RESP3Value::BulkString(b"60".to_vec()),
            ])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_set_request_with_milliseconds_ttl() {
        let key = RESP3Value::BulkString(b"mykey".to_vec());
        let value = RESP3Value::BulkString(b"myvalue".to_vec());
        let ttl = Some(TTL::Milliseconds(1000));
        let request = Request::Set(key.clone(), value.clone(), ttl.clone());
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"SET".to_vec()),
                key.clone(),
                value.clone(),
                RESP3Value::BulkString(b"PX".to_vec()),
                RESP3Value::BulkString(b"1000".to_vec()),
            ])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_get_request() {
        let key = RESP3Value::BulkString(b"mykey".to_vec());
        let request = Request::Get(key.clone());
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![RESP3Value::BulkString(b"GET".to_vec()), key.clone()])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_del_request() {
        let key = RESP3Value::BulkString(b"mykey".to_vec());
        let request = Request::Del(key.clone());
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![RESP3Value::BulkString(b"DEL".to_vec()), key.clone()])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_psync_request() {
        let repl_id = RESP3Value::BulkString(b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_vec());
        let offset = RESP3Value::BulkString(b"1234".to_vec());
        let request = Request::PSync(repl_id.clone(), offset.clone());
        let encoded = encode_request(&request);
        assert_eq!(
            encoded,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"PSYNC".to_vec()),
                repl_id.clone(),
                offset.clone()
            ])
        );

        let decoded = decode_request(encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn test_invalid_request() {
        let invalid_request = RESP3Value::SimpleString("Invalid".to_string());
        assert!(decode_request(invalid_request).is_err());
    }

    #[test]
    fn test_invalid_command() {
        let invalid_command = RESP3Value::Array(vec![RESP3Value::BulkString(b"INVALID".to_vec())]);
        assert!(decode_request(invalid_command).is_err());
    }

    #[test]
    fn test_invalid_ping_request() {
        let invalid_ping = RESP3Value::Array(vec![
            RESP3Value::BulkString(b"PING".to_vec()),
            RESP3Value::BulkString(b"extra".to_vec()),
        ]);
        assert!(decode_request(invalid_ping).is_err());
    }

    #[test]
    fn test_invalid_echo_request() {
        let invalid_echo = RESP3Value::Array(vec![RESP3Value::BulkString(b"ECHO".to_vec())]);
        assert!(decode_request(invalid_echo).is_err());
    }

    #[test]
    fn test_invalid_set_request() {
        let invalid_set = RESP3Value::Array(vec![
            RESP3Value::BulkString(b"SET".to_vec()),
            RESP3Value::BulkString(b"key".to_vec()),
        ]);
        assert!(decode_request(invalid_set).is_err());
    }

    #[test]
    fn test_invalid_get_request() {
        let invalid_get = RESP3Value::Array(vec![RESP3Value::BulkString(b"GET".to_vec())]);
        assert!(decode_request(invalid_get).is_err());
    }

    #[test]
    fn test_invalid_del_request() {
        let invalid_del = RESP3Value::Array(vec![RESP3Value::BulkString(b"DEL".to_vec())]);
        assert!(decode_request(invalid_del).is_err());
    }

    #[test]
    fn test_invalid_psync_request() {
        let invalid_psync = RESP3Value::Array(vec![
            RESP3Value::BulkString(b"PSYNC".to_vec()),
            RESP3Value::BulkString(b"repl_id".to_vec()),
        ]);
        assert!(decode_request(invalid_psync).is_err());
    }
}
