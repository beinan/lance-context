//! Bounded scatter/gather writes over borrowed payload owners.
//!
//! This preserves the existing length/header/body wire bytes. It does not make
//! writes atomic: errors and cancellation can leave a partial exchange, so the
//! protocol must discard that connection. Counts describe socket acceptance,
//! never remote receipt or durability. Callers enforce protocol size limits and
//! must obtain a prepared-write grant before calling write_bodies.

use bytes::Bytes;
use std::io::{self, IoSlice};
use tokio::io::{AsyncWrite, AsyncWriteExt};

const MAX_SLICES: usize = 64;

/// Write a big-endian u32 header length, the header, then all bodies in order.
/// The optional cumulative counter includes only successfully written body bytes,
/// including partial writes before an error or cancellation. Counter overflow
/// and an unrepresentable header length are rejected before writing anything.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: &[u8],
    bodies: &[Bytes],
    body_bytes: Option<&mut u64>,
) -> io::Result<()> {
    let size = u32::try_from(header.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "header length exceeds u32"))?;
    write_parts(writer, &size.to_be_bytes(), header, bodies, body_bytes).await
}

/// Write only bodies, for the second stage of an already granted exchange.
/// Interrupted writes are propagated, with earlier successful bytes retained
/// in the counter, just like other I/O errors.
pub async fn write_bodies<W: AsyncWrite + Unpin>(
    writer: &mut W,
    bodies: &[Bytes],
    body_bytes: Option<&mut u64>,
) -> io::Result<()> {
    write_parts(writer, &[], &[], bodies, body_bytes).await
}

fn part<'a>(prefix: &'a [u8], header: &'a [u8], bodies: &'a [Bytes], index: usize) -> &'a [u8] {
    match index {
        0 => prefix,
        1 => header,
        _ => &bodies[index - 2],
    }
}

async fn write_parts<W: AsyncWrite + Unpin>(
    writer: &mut W,
    prefix: &[u8],
    header: &[u8],
    bodies: &[Bytes],
    mut body_bytes: Option<&mut u64>,
) -> io::Result<()> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "frame byte count overflow");
    let parts = bodies.len().checked_add(2).ok_or_else(invalid)?;
    let total_body = bodies.iter().try_fold(0u64, |total, body| {
        total
            .checked_add(u64::try_from(body.len()).map_err(|_| invalid())?)
            .ok_or_else(invalid)
    })?;
    if let Some(count) = body_bytes.as_ref() {
        count.checked_add(total_body).ok_or_else(invalid)?;
    }
    let mut framing_left = prefix.len().checked_add(header.len()).ok_or_else(invalid)?;
    let (mut index, mut offset) = (0, 0);
    while index < parts {
        // Only descriptors are temporary; payloads are never concatenated or
        // cloned. A fixed stack array also bounds very fragmented body lists.
        let mut slices: [IoSlice<'_>; MAX_SLICES] = std::array::from_fn(|_| IoSlice::new(&[]));
        let (mut count, mut available) = (0, 0usize);
        for next in index..parts {
            let bytes = part(prefix, header, bodies, next);
            let bytes = if next == index {
                &bytes[offset..]
            } else {
                bytes
            };
            if bytes.is_empty() {
                continue;
            }
            available = available.checked_add(bytes.len()).ok_or_else(invalid)?;
            slices[count] = IoSlice::new(bytes);
            count += 1;
            if count == MAX_SLICES {
                break;
            }
        }
        if count == 0 {
            return Ok(());
        }
        let scalar = count == 1 || !writer.is_write_vectored();
        let supplied = if scalar { slices[0].len() } else { available };
        let written = if scalar {
            writer.write(&slices[0]).await?
        } else {
            writer.write_vectored(&slices[..count]).await?
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "writer made no progress",
            ));
        }
        if written > supplied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "writer exceeded supplied slices",
            ));
        }
        let payload = written.saturating_sub(framing_left);
        framing_left = framing_left.saturating_sub(written);
        if let Some(count) = body_bytes.as_mut() {
            **count += payload as u64; // Full body count was checked before I/O.
        }
        let mut remaining = written;
        while remaining > 0 {
            let bytes = part(prefix, header, bodies, index).len() - offset;
            if remaining < bytes {
                offset += remaining;
                break;
            }
            remaining -= bytes;
            index += 1;
            offset = 0;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
