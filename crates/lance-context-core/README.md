# lance-context-core

Pure Rust engine for the lance-context project. This crate is free of Python
dependencies and is re-exported by the `lance-context` wrapper crate.

For a generic store, `store.get_many(&ids, Some(&["id".into()])).await?`
checks up to 1,024 IDs without fetching payloads or issuing a disk lookup per
ID. Found rows are returned once in first-requested order, including flushed
WAL updates. See [batch lookup](../../docs/benchmarks/batch-get.md) for the HTTP
and Rust client APIs, visibility rules, and a reproducible benchmark.

Enable the optional `lance-io-metrics` feature to record Lance 13 object-store
request counts, bytes, durations, errors, in-flight requests and cloud retryable
responses through the application's existing `metrics` recorder. Embedded users
can call `metrics::describe_lance_io_metrics()` after installing their recorder.
The REST server forwards this feature and registers descriptions automatically:

```sh
cargo build -p lance-context-server --features lance-io-metrics
```

Storage metrics then appear on the server's existing `/metrics` endpoint. Names
start with `lance_object_store_`; request metrics have an `operation` label and
use only the storage scheme as the default `base` label. Keep
`LANCE_OBJECT_STORE_METRICS_LABEL=scheme` for bounded cardinality; Lance's `full`
and `dataset` modes include storage identities and can create many more series.
This feature is off by default. Cargo features are additive, so enabling it in
one dependency enables Lance instrumentation throughout that binary.

Request durations can overlap across uploads and include retries; their sum is
neither elapsed ingestion time nor encoder CPU time. Local optimized writes
measure the writer lifetime, including encoding between writes. Retryable HTTP
response metrics cover native cloud stores, not every custom backend. These
signals complement application phase timings; they do not alone establish
network saturation or isolate encoding cost. See the [Lance metric catalog](https://docs.rs/lance/13.0.0/lance/metrics/index.html).
