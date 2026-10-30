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
