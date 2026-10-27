# Inspecting annotation owner storage waits

Refs #378. This diagnostic entry point calls the same `serve_owner` as the
normal executable, with the same configuration, admission limits, publication
policy and shutdown path. It installs the existing metrics recorder and exposes
the existing metrics router on a separate loopback listener:

```bash
cargo build --release --locked -p lance-context-ingest \
  --features service,lance-io-metrics --example annotation_io_owner
LANCE_OBJECT_STORE_METRICS_LABEL=scheme \
  target/release/examples/annotation_io_owner owner.json 127.0.0.1:7911
```

Scrape `http://127.0.0.1:7911/metrics` from the owner container while retaining
the normal timestamped phase observations. Use the existing
`lance_object_store_*` request counts, requested bytes, duration histograms,
in-flight gauges and retryable/throttle response counters. `scheme` labels keep
store identities out of the metrics and cardinality bounded. The listener
rejects non-loopback addresses; ordinary owner routes retain their workload
authentication. No metric is used to authorize publication or acknowledge a
mutation.

These are process-wide observations, including concurrent cold point reads and
apply work. Align scrape intervals with preparation/collection/compaction
observations; do not treat a delta as one transaction's exclusive I/O. Operation
latencies can overlap, multipart operations have separate labels, and SDK
retries may occur inside a measured operation. The HTTP retryable-response
counter counts observed responses, not application mutation retries. A successful
operation counter is not proof of durable annotation publication. Requested byte
counts are not a physical network/disk measurement.

The probe adds measurement overhead and does not by itself establish a
performance improvement. Keep workload, admission budgets and deadlines fixed
when comparing it with the normal owner. Its owner is exclusive: stop the old
owner before launching this entry point with the same table configuration.
