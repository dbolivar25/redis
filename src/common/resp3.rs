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
            RESP3Value::SimpleString(s) => write!(f, "\"{}\"", s),
            RESP3Value::SimpleError(s) => write!(f, "Error: \"{}\"", s),
            RESP3Value::Integer(n) => write!(f, "{}", n),
            RESP3Value::BulkString(data) => write!(f, "\"{}\"", data.escape_ascii()),
            RESP3Value::Array(data) => {
                let values = data.iter().map(|v| v.to_string()).join(", ");
                write!(f, "[{}]", values)
            }
            RESP3Value::Null => write!(f, "Null"),
        }
    }
}

/// Encodes a RESP3 value into a RESP3 string.
pub fn encode_resp3(value: &RESP3Value) -> String {
    match value {
        RESP3Value::SimpleString(s) => format!("+{}\r\n", s),
        RESP3Value::SimpleError(s) => format!("-{}\r\n", s),
        RESP3Value::Integer(n) => format!(":{}\r\n", n),
        RESP3Value::BulkString(data) => format!(
            "${}\r\n{}\r\n",
            data.len(),
            data.iter().map(|&b| b as char).collect::<String>()
        ),
        RESP3Value::Array(data) => format!(
            "*{}\r\n{}",
            data.len(),
            data.iter().map(encode_resp3).collect::<String>()
        ),
        RESP3Value::Null => "_\r\n".to_string(),
    }
}

/// Decodes a RESP3 string into a RESP3 value. Returns the decoded value and the remaining data.
pub fn decode_resp3(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    debug_assert!(!input.is_empty());

    match input.first() {
        Some(b'+') => decode_simple_string(&input[1..]),
        Some(b'-') => decode_simple_error(&input[1..]),
        Some(b':') => decode_integer(&input[1..]),
        Some(b'$') => decode_bulk_string(&input[1..]),
        Some(b'*') => decode_array(&input[1..]),
        Some(b'_') => decode_null(&input[1..]),
        _ => bail!("Invalid RESP3 value"),
    }
}

/// Decodes a simple string from a byte array after the identifying character has been removed.
/// Returns the decoded value and the remaining data.
fn decode_simple_string(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    let (s, rest) = read_through_crlf(input)?;

    debug_assert!(!s.contains('\r') && !s.contains('\n'));

    Ok((RESP3Value::SimpleString(s), rest))
}

/// Decodes a simple error from a byte array after the identifying character has been removed.
/// Returns the decoded value and the remaining data.
fn decode_simple_error(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    let (s, rest) = read_through_crlf(input)?;

    debug_assert!(!s.contains('\r') && !s.contains('\n'));

    Ok((RESP3Value::SimpleError(s), rest))
}

/// Decodes an integer from a byte array after the identifying character has been removed.
/// Returns the decoded value and the remaining data.
fn decode_integer(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    let (s, rest) = read_through_crlf(input)?;
    let n = s.parse::<i64>().map_err(|e| anyhow!(e))?;

    Ok((RESP3Value::Integer(n), rest))
}

/// Decodes a bulk string from a byte array after the identifying character has been removed.
/// Returns the decoded value and the remaining data.
fn decode_bulk_string(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    let (len_str, rest) = read_through_crlf(input)?;
    if len_str == "-1" {
        let rest = rest
            .get(2..)
            .ok_or_else(|| anyhow!("Invalid bulk string"))?;
        return Ok((RESP3Value::Null, rest));
    }
    let len = len_str.parse::<usize>().map_err(|e| anyhow!(e))?;
    if rest.len() < len + 2 {
        bail!("Insufficient data for bulk string");
    }
    let data = rest[..len].to_vec();

    debug_assert_eq!(data.len(), len);

    Ok((RESP3Value::BulkString(data), &rest[len + 2..]))
}

/// Decodes an array from a byte array after the identifying character has been removed.
/// Returns the decoded value and the remaining data.
fn decode_array(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    let (len_str, mut rest) = read_through_crlf(input)?;
    if len_str == "-1" {
        let rest = rest
            .get(2..)
            .ok_or_else(|| anyhow!("Invalid bulk string"))?;
        return Ok((RESP3Value::Null, rest));
    }
    let len = len_str.parse::<usize>().map_err(|e| anyhow!(e))?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        let (value, new_rest) = decode_resp3(rest)?;
        values.push(value);
        rest = new_rest;
    }

    debug_assert_eq!(values.len(), len);

    Ok((RESP3Value::Array(values), rest))
}

/// Decodes a null value from a byte array after the identifying character has been removed.
/// Returns the decoded value and the remaining data.
fn decode_null(input: &[u8]) -> Result<(RESP3Value, &[u8])> {
    match &input[..2] {
        b"\r\n" => Ok((RESP3Value::Null, &input[2..])),
        _ => bail!("Invalid null value"),
    }
}

/// Reads a string from a byte array until a CRLF sequence is found. Returns the string and the
/// remaining data.
fn read_through_crlf(input: &[u8]) -> Result<(String, &[u8])> {
    if let Some((pos, _)) = input.iter().find_position(|&&b| b == b'\r') {
        if let Some(&b'\n') = input.get(pos + 1) {
            let s = str::from_utf8(&input[..pos]).map_err(|e| anyhow!(e))?;
            Ok((s.to_string(), &input[pos + 2..]))
        } else {
            bail!("LF not found")
        }
    } else {
        bail!("CRLF not found")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() {
        let input = b"+OK\r\n";
        let (value, rest) = decode_resp3(input).unwrap();
        assert_eq!(value, RESP3Value::SimpleString("OK".to_string()));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        let expected = String::from_utf8_lossy(input);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_error() {
        let input = b"-Error message\r\n";
        let (value, rest) = decode_resp3(input).unwrap();
        assert_eq!(value, RESP3Value::SimpleError("Error message".to_string()));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        let expected = String::from_utf8_lossy(input);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_integer() {
        let input = b":1000\r\n";
        let (value, rest) = decode_resp3(input).unwrap();
        assert_eq!(value, RESP3Value::Integer(1000));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        let expected = String::from_utf8_lossy(input);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_bulk_string() {
        let input = b"$5\r\nhello\r\n";
        let (value, rest) = decode_resp3(input).unwrap();
        assert_eq!(value, RESP3Value::BulkString(b"hello".to_vec()));
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        let expected = String::from_utf8_lossy(input);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_null() {
        let input = b"_\r\n";
        let (value, rest) = decode_resp3(input).unwrap();
        assert_eq!(value, RESP3Value::Null);
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        let expected = String::from_utf8_lossy(input);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_array() {
        let input = b"*2\r\n$5\r\nhello\r\n:10\r\n";
        let (value, rest) = decode_resp3(input).unwrap();
        assert_eq!(
            value,
            RESP3Value::Array(vec![
                RESP3Value::BulkString(b"hello".to_vec()),
                RESP3Value::Integer(10)
            ])
        );
        assert!(rest.is_empty());

        let encoded = encode_resp3(&value);
        let expected = String::from_utf8_lossy(input);
        assert_eq!(encoded, expected);
    }
}
