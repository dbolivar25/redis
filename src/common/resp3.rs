use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::{fmt::Display, str};

/// Represents a RESP3 value. This is a simplified version of the RESP3 specification from Redis.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RESP3Value {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RESP3Value>),
    Null,
}

impl Display for RESP3Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RESP3Value::SimpleString(s) => write!(f, "\"{s}\""),
            RESP3Value::SimpleError(s) => write!(f, "Error: \"{s}\""),
            RESP3Value::Integer(n) => write!(f, "{n}"),
            RESP3Value::BulkString(data) => write!(f, "\"{}\"", data.escape_ascii()),
            RESP3Value::Array(data) => {
                let values = data.iter().map(|v| v.to_string()).join(", ");
                write!(f, "[{values}]")
            }
            RESP3Value::Null => write!(f, "Null"),
        }
    }
}

/// Encodes a RESP3 value into a byte vector.
/// Returns raw bytes to properly handle binary-safe bulk strings.
#[must_use]
pub fn encode_resp3(value: &RESP3Value) -> Vec<u8> {
    match value {
        RESP3Value::SimpleString(s) => format!("+{s}\r\n").into_bytes(),
        RESP3Value::SimpleError(s) => format!("-{s}\r\n").into_bytes(),
        RESP3Value::Integer(n) => format!(":{n}\r\n").into_bytes(),
        RESP3Value::BulkString(data) => {
            let len_str = data.len().to_string();
            let capacity = 1 + len_str.len() + 2 + data.len() + 2;
            let mut out = Vec::with_capacity(capacity);
            out.push(b'$');
            out.extend_from_slice(len_str.as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
            out
        }
        RESP3Value::Array(data) => {
            let len_str = data.len().to_string();
            let mut out = Vec::with_capacity(1 + len_str.len() + 2);
            out.push(b'*');
            out.extend_from_slice(len_str.as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in data {
                out.extend(encode_resp3(item));
            }
            out
        }
        RESP3Value::Null => b"_\r\n".to_vec(),
    }
}

/// Decodes a RESP3 string into a RESP3 value. Returns the decoded value and the remaining data.
/// Returns Ok(Some((value, rest))) on success, Ok(None) if data is incomplete, Err on malformed data.
pub fn decode_resp3(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    if input.is_empty() {
        return Ok(None);
    }

    match input.first() {
        Some(b'+') => decode_simple_string(&input[1..]),
        Some(b'-') => decode_simple_error(&input[1..]),
        Some(b':') => decode_integer(&input[1..]),
        Some(b'$') => decode_bulk_string(&input[1..]),
        Some(b'*') => decode_array(&input[1..]),
        Some(b'_') => decode_null(&input[1..]),
        Some(c) => bail!("Invalid RESP3 type marker: {}", *c as char),
        None => Ok(None),
    }
}

fn decode_simple_string(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    let Some((s, rest)) = read_through_crlf(input)? else {
        return Ok(None);
    };
    Ok(Some((RESP3Value::SimpleString(s), rest)))
}

fn decode_simple_error(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    let Some((s, rest)) = read_through_crlf(input)? else {
        return Ok(None);
    };
    Ok(Some((RESP3Value::SimpleError(s), rest)))
}

fn decode_integer(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    let Some((s, rest)) = read_through_crlf(input)? else {
        return Ok(None);
    };
    let n = s.parse::<i64>().map_err(|e| anyhow!(e))?;
    Ok(Some((RESP3Value::Integer(n), rest)))
}

fn decode_bulk_string(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    let Some((len_str, rest)) = read_through_crlf(input)? else {
        return Ok(None);
    };

    if len_str == "-1" {
        return Ok(Some((RESP3Value::Null, rest)));
    }

    let len = len_str.parse::<usize>().map_err(|e| anyhow!("Invalid bulk string length: {e}"))?;

    if rest.len() < len + 2 {
        return Ok(None);
    }

    let data = rest[..len].to_vec();
    Ok(Some((RESP3Value::BulkString(data), &rest[len + 2..])))
}

fn decode_array(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    let Some((len_str, mut rest)) = read_through_crlf(input)? else {
        return Ok(None);
    };

    if len_str == "-1" {
        return Ok(Some((RESP3Value::Null, rest)));
    }

    let len = len_str.parse::<usize>().map_err(|e| anyhow!("Invalid array length: {e}"))?;
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        match decode_resp3(rest)? {
            Some((value, new_rest)) => {
                values.push(value);
                rest = new_rest;
            }
            None => return Ok(None),
        }
    }

    Ok(Some((RESP3Value::Array(values), rest)))
}

fn decode_null(input: &[u8]) -> Result<Option<(RESP3Value, &[u8])>> {
    if input.len() < 2 {
        return Ok(None);
    }

    if &input[..2] == b"\r\n" {
        Ok(Some((RESP3Value::Null, &input[2..])))
    } else {
        bail!("Invalid null value")
    }
}

fn read_through_crlf(input: &[u8]) -> Result<Option<(String, &[u8])>> {
    match input.iter().find_position(|&&b| b == b'\r') {
        Some((pos, _)) => {
            if input.get(pos + 1) == Some(&b'\n') {
                let s = str::from_utf8(&input[..pos]).map_err(|e| anyhow!(e))?;
                Ok(Some((s.to_string(), &input[pos + 2..])))
            } else if input.len() == pos + 1 {
                Ok(None)
            } else {
                bail!("Expected LF after CR")
            }
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        let input = b"+OK\r\n";
        let (value, rest) = decode_resp3(input).unwrap().unwrap();
        assert_eq!(value, RESP3Value::SimpleString("OK".to_string()));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        assert_eq!(encoded.as_slice(), input.as_slice());
    }

    #[test]
    fn test_error() {
        let input = b"-Error message\r\n";
        let (value, rest) = decode_resp3(input).unwrap().unwrap();
        assert_eq!(value, RESP3Value::SimpleError("Error message".to_string()));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        assert_eq!(encoded.as_slice(), input.as_slice());
    }

    #[test]
    fn test_integer() {
        let input = b":1000\r\n";
        let (value, rest) = decode_resp3(input).unwrap().unwrap();
        assert_eq!(value, RESP3Value::Integer(1000));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        assert_eq!(encoded.as_slice(), input.as_slice());
    }

    #[test]
    fn test_bulk_string() {
        let input = b"$5\r\nhello\r\n";
        let (value, rest) = decode_resp3(input).unwrap().unwrap();
        assert_eq!(value, RESP3Value::BulkString(b"hello".to_vec()));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        assert_eq!(encoded.as_slice(), input.as_slice());
    }

    #[test]
    fn test_null() {
        let input = b"_\r\n";
        let (value, rest) = decode_resp3(input).unwrap().unwrap();
        assert_eq!(value, RESP3Value::Null);
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        assert_eq!(encoded.as_slice(), input.as_slice());
    }

    #[test]
    fn test_array() {
        let input = b"*2\r\n$5\r\nhello\r\n:10\r\n";
        let (value, rest) = decode_resp3(input).unwrap().unwrap();
        assert_eq!(
            value,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"hello".to_vec()),
                RESP3Value::Integer(10)
            ])
        );
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        assert_eq!(encoded.as_slice(), input.as_slice());
    }

    #[test]
    fn test_incomplete_simple_string() {
        assert!(decode_resp3(b"+OK").unwrap().is_none());
        assert!(decode_resp3(b"+OK\r").unwrap().is_none());
    }

    #[test]
    fn test_incomplete_bulk_string() {
        assert!(decode_resp3(b"$5\r\n").unwrap().is_none());
        assert!(decode_resp3(b"$5\r\nhel").unwrap().is_none());
        assert!(decode_resp3(b"$5\r\nhello").unwrap().is_none());
        assert!(decode_resp3(b"$5\r\nhello\r").unwrap().is_none());
    }

    #[test]
    fn test_incomplete_array() {
        assert!(decode_resp3(b"*2\r\n").unwrap().is_none());
        assert!(decode_resp3(b"*2\r\n$5\r\nhello\r\n").unwrap().is_none());
    }

    #[test]
    fn test_empty_input() {
        assert!(decode_resp3(b"").unwrap().is_none());
    }
}
