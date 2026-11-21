//! Wire format shared by the TCP and UDP transports:
//! `len (u32 BE) | id (u32 BE) | type (4 bytes) | payload`.

/// Size of the fixed frame header (length, message id and message type).
pub const HEADER_LEN: usize = 12;

/// Builds a wire frame.
pub fn frame(id: u32, msg_type: &[u8; 4], payload: &[u8]) -> Vec<u8> {
    let len = (HEADER_LEN + payload.len()) as u32;
    let mut out = Vec::with_capacity(len as usize);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(msg_type);
    out.extend_from_slice(payload);
    out
}

/// Builds an `ERR ` frame carrying the ID of the request that failed.
pub fn create_error(id: u32, error: &str) -> Vec<u8> {
    frame(id, b"ERR ", error.as_bytes())
}

/// A complete frame split into its parts.
#[derive(Clone, Copy)]
pub struct ParsedFrame<'a> {
    pub id: u32,
    pub msg_type: &'a [u8],
    pub payload: &'a [u8],
}

/// Splits a complete frame (as delivered in one UDP datagram) into its parts.
/// Returns `None` if the declared length does not match the data.
pub fn parse_frame(data: &[u8]) -> Option<ParsedFrame<'_>> {
    if data.len() < HEADER_LEN
        || u32::from_be_bytes(data[..4].try_into().ok()?) as usize != data.len()
    {
        return None;
    }
    Some(ParsedFrame {
        id: u32::from_be_bytes(data[4..8].try_into().ok()?),
        msg_type: &data[8..12],
        payload: &data[HEADER_LEN..],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_creation_working() {
        let id = 12345;
        let error_message = "Very serious error";
        let error_vec: Vec<u8> = create_error(id, error_message);
        let len = error_vec.len();
        assert_eq!(
            u32::from_be_bytes(error_vec[..4].try_into().unwrap()),
            len as u32
        );
        assert_eq!(u32::from_be_bytes(error_vec[4..8].try_into().unwrap()), id);
        assert_eq!(str::from_utf8(&error_vec[12..len]), Ok(error_message));
    }

    #[test]
    fn parse_frame_round_trip_and_length_check() {
        let f = frame(7, b"TST ", b"hi");
        let parsed = parse_frame(&f).unwrap();
        assert_eq!(
            (parsed.id, parsed.msg_type, parsed.payload),
            (7, &b"TST "[..], &b"hi"[..])
        );
        assert!(parse_frame(&f[..f.len() - 1]).is_none());
        assert!(parse_frame(b"junk").is_none());
    }
}
