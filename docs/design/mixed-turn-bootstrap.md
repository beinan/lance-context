# Initializing a mixed turn generation

Configure a single table/shard with the narrow annotation URI in `tables[0].uri`
and the immutable body URI in `annotations.body.uri`. Both locations must be
fresh, separate storage roots. The optional `storage_options` on each location
use the existing Lance credential handling. Do not combine `body` with `source`
or `projection`; this operation creates a new generation.

Persist the complete configuration, including `tables[0].bootstrap`, then run:

```sh
lance-context-ingest annotation-bootstrap config.json
```

The command reserves the mixed Arrow contract, creates an empty body table and
an empty narrow table with stable row IDs, and records the exact body snapshot
and shard cut zero in the narrow transaction. Only then does it select the
narrow manifest in the replicated catalog. This catalog selection makes the
pair available to `annotation-owner` and `annotation-ingress`.

After interruption, run the same command with the same configuration. It reuses
the original pair or validates the already selected state. It refuses a changed
bootstrap identity, changed destination, overlapping roots, foreign table, or
an existing binding with a different write contract. It does not import an
existing dataset. Storage credentials are excluded from persisted provenance.

Owner and ingress startup only open an already initialized generation. For
existing turns, later mixed ingestion preserves the first body and current
annotations; annotation updates remain independently mutable. Normal batch
publication, recovery, compaction, and reclamation use the selected body/narrow
pair and retained WAL suffix.
