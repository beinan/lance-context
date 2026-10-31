# Lance Context ingestion

The ingestion application provides `annotation-bootstrap CONFIG`,
`annotation-owner CONFIG` and `annotation-ingress CONFIG` for managed annotation
and mixed body/annotation bindings. Start with the
[annotation deployment guide](../../docs/design/annotation-application.md) for
binding, bootstrap, ownership, routing and recovery requirements.

Configuration belongs to the running process. Owners consume their own
`worker.publication` policy, including optional `turn_id_index` maintenance;
ingresses forward requests and do not run that maintenance. HTTP admission and
memory budgets are also local to each process. See
[role configuration](../../docs/design/annotation-application.md#configure-each-running-role)
before copying a bootstrap configuration or comparing worker counts.

The owner's periodic annotation observation reports `backpressure_reason` as
`apply_memory`, `source_window`, `staging`, or `null`. It identifies the last
admission gate that prevented ordered apply; progress clears it. The existing
`backpressured` flag is true whenever a reason is present. These are sampled
states, not refusal counts or blocked durations. In particular, staging pressure
can include values held by readers or an old frozen generation after publication.
