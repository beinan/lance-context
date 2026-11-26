use super::*;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

enum Step {
    Limit(usize),
    Error(io::ErrorKind),
    Pending,
    Overreport,
}

struct Writer {
    vectored: bool,
    steps: VecDeque<Step>,
    bytes: Vec<u8>,
    calls: Vec<Vec<(usize, usize)>>,
    vector_calls: usize,
}

impl Writer {
    fn new(vectored: bool, steps: impl IntoIterator<Item = Step>) -> Self {
        Self {
            vectored,
            steps: steps.into_iter().collect(),
            bytes: Vec::new(),
            calls: Vec::new(),
            vector_calls: 0,
        }
    }

    fn accept(&mut self, slices: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        self.calls.push(
            slices
                .iter()
                .map(|s| (s.as_ptr() as usize, s.len()))
                .collect(),
        );
        let available = slices.iter().map(|s| s.len()).sum::<usize>();
        let limit = match self.steps.pop_front().unwrap_or(Step::Limit(usize::MAX)) {
            Step::Limit(limit) => limit.min(available),
            Step::Error(kind) => return Poll::Ready(Err(kind.into())),
            Step::Pending => return Poll::Pending,
            Step::Overreport => return Poll::Ready(Ok(available + 1)),
        };
        let mut left = limit;
        for slice in slices {
            let size = left.min(slice.len());
            self.bytes.extend_from_slice(&slice[..size]);
            left -= size;
        }
        Poll::Ready(Ok(limit))
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.accept(&[IoSlice::new(bytes)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        assert!(
            self.vectored,
            "scalar writer must not receive vectored calls"
        );
        self.vector_calls += 1;
        self.accept(slices)
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn wire(header: &[u8], bodies: &[Bytes]) -> Vec<u8> {
    let mut bytes = (header.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(header);
    for body in bodies {
        bytes.extend_from_slice(body);
    }
    bytes
}

#[tokio::test]
async fn short_writes_preserve_every_boundary_and_body_counter() {
    let header = b"header";
    let bodies = [
        Bytes::new(),
        Bytes::from_static(b"abc"),
        Bytes::new(),
        Bytes::from_static(b"defgh"),
    ];
    let expected = wire(header, &bodies);
    for vectored in [false, true] {
        for size in 1..=expected.len() {
            let mut writer = Writer::new(vectored, (0..expected.len()).map(|_| Step::Limit(size)));
            let mut count = 17;
            write_frame(&mut writer, header, &bodies, Some(&mut count))
                .await
                .unwrap();
            assert_eq!(writer.bytes, expected, "vectored={vectored}, size={size}");
            assert_eq!(count, 25);
            assert_eq!(writer.vector_calls > 0, vectored);
        }
    }
}

#[tokio::test]
async fn fragmented_payloads_borrow_original_storage_with_bounded_descriptors() {
    let bodies: Vec<_> = (0..193)
        .map(|i| {
            if i % 7 == 0 {
                Bytes::new()
            } else {
                Bytes::from(vec![i as u8; i % 13 + 1])
            }
        })
        .collect();
    let mut writer = Writer::new(true, []);
    let mut count = 0;
    write_frame(&mut writer, b"h", &bodies, Some(&mut count))
        .await
        .unwrap();
    assert_eq!(writer.bytes, wire(b"h", &bodies));
    assert_eq!(count, bodies.iter().map(|b| b.len() as u64).sum::<u64>());
    assert!(writer.calls.len() > 2);
    assert!(writer.calls.iter().all(|call| call.len() <= MAX_SLICES));
    assert_eq!(writer.calls[0].len(), MAX_SLICES);
    // The accepting writer sees the original body pointers, not a concatenated
    // temporary. Bytes owners stay usable after the exchange.
    for body in bodies.iter().filter(|b| !b.is_empty()) {
        assert!(writer
            .calls
            .iter()
            .flatten()
            .any(|&(ptr, len)| ptr == body.as_ptr() as usize && len == body.len()));
    }
}

#[tokio::test]
async fn empty_parts_and_body_only_exchange() {
    let mut writer = Writer::new(true, []);
    let mut count = 11;
    write_bodies(&mut writer, &[Bytes::new(), Bytes::new()], Some(&mut count))
        .await
        .unwrap();
    assert!(writer.calls.is_empty());
    write_frame(&mut writer, &[], &[], Some(&mut count))
        .await
        .unwrap();
    assert_eq!(writer.bytes, [0; 4]);
    assert_eq!(count, 11);
    let bodies = [
        Bytes::new(),
        Bytes::from_static(b"abc"),
        Bytes::from_static(b"def"),
        Bytes::new(),
    ];
    write_bodies(&mut writer, &bodies, Some(&mut count))
        .await
        .unwrap();
    assert_eq!(&writer.bytes[4..], b"abcdef");
    assert_eq!(count, 17);
}

#[tokio::test]
async fn errors_retain_only_accepted_body_bytes() {
    let header = b"abc";
    let bodies = [Bytes::from_static(b"def"), Bytes::from_static(b"ghijk")];
    let expected = wire(header, &bodies);
    for kind in [io::ErrorKind::BrokenPipe, io::ErrorKind::Interrupted] {
        for progress in 0..expected.len() {
            let steps = (0..progress)
                .map(|_| Step::Limit(1))
                .chain([Step::Error(kind)]);
            let mut writer = Writer::new(true, steps);
            let mut count = 19;
            let error = write_frame(&mut writer, header, &bodies, Some(&mut count))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(writer.bytes, expected[..progress]);
            assert_eq!(count, 19 + progress.saturating_sub(7) as u64);
        }
    }
}

#[tokio::test]
async fn zero_progress_and_invalid_writer_counts_fail_without_fabricating_progress() {
    for vectored in [false, true] {
        for (step, kind) in [
            (Step::Limit(0), io::ErrorKind::WriteZero),
            (Step::Overreport, io::ErrorKind::InvalidData),
        ] {
            let mut writer = Writer::new(vectored, [step]);
            let mut count = 3;
            let error = write_frame(
                &mut writer,
                b"header",
                &[Bytes::from_static(b"body")],
                Some(&mut count),
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(count, 3);
            assert!(writer.bytes.is_empty());
        }
    }
}

#[tokio::test]
async fn counter_overflow_is_rejected_before_framing_or_payload_io() {
    for frame in [false, true] {
        let mut writer = Writer::new(true, []);
        let mut count = u64::MAX - 2;
        let bodies = [Bytes::from_static(b"abc")];
        let result = if frame {
            write_frame(&mut writer, b"head", &bodies, Some(&mut count)).await
        } else {
            write_bodies(&mut writer, &bodies, Some(&mut count)).await
        };
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert!(writer.calls.is_empty());
        assert_eq!(count, u64::MAX - 2);
    }
}

#[test]
fn cancellation_keeps_partial_body_accounting() {
    let bodies = [Bytes::from_static(b"payload")];
    for frame in [false, true] {
        let mut writer = Writer::new(
            true,
            [Step::Limit(if frame { 9 } else { 3 }), Step::Pending],
        );
        let mut count = 20;
        {
            let future = async {
                if frame {
                    write_frame(&mut writer, b"hd", &bodies, Some(&mut count)).await
                } else {
                    write_bodies(&mut writer, &bodies, Some(&mut count)).await
                }
            };
            let mut future = std::pin::pin!(future);
            assert!(future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
        }
        assert_eq!(count, 23);
        assert!(writer.bytes.ends_with(b"pay"));
    }
}

#[tokio::test]
async fn tcp_frames_match_legacy_reader_across_multiple_exchanges() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let bodies: Vec<_> = (0..130).map(|i| Bytes::from(vec![i as u8; 4097])).collect();
    let expected: Vec<_> = bodies.iter().flat_map(|b| b.iter().copied()).collect();
    let reader = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..3 {
            assert_eq!(stream.read_u32().await.unwrap(), 6);
            let mut header = [0; 6];
            stream.read_exact(&mut header).await.unwrap();
            assert_eq!(&header, b"legacy");
            let mut received = vec![0; expected.len()];
            stream.read_exact(&mut received).await.unwrap();
            assert_eq!(received, expected);
            // Exercise the reverse direction with the existing scalar framing.
            stream.write_u32(2).await.unwrap();
            stream.write_all(b"ok").await.unwrap();
        }
    });
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut count = 0;
    for _ in 0..3 {
        write_frame(&mut stream, b"legacy", &bodies, Some(&mut count))
            .await
            .unwrap();
        assert_eq!(stream.read_u32().await.unwrap(), 2);
        let mut reply = [0; 2];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"ok");
    }
    assert_eq!(count, (3 * 130 * 4097) as u64);
    reader.await.unwrap();
}
