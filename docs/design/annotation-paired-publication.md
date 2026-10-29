# Paired body and annotation publication

Mixed ingestion stores immutable turn bodies separately from the current narrow
annotations. A publication must select both at the same source cut: making a
narrow row visible without its body, or replacing its body reference during
recovery, would produce an incomplete turn.

`annotation_publication::mixed` supplies the publication adapter for an explicit
`TurnIngestion` WAL binding. The ordinary annotation adapter continues to reject
inserts and paired body metadata. Provision a new binding only after all voters
and application components support its contract. Mixed ingestion runtime and HTTP
wiring are separate integration steps; this adapter does not enable those paths.

For one frozen source prefix:

1. Prepare an append-only detached body snapshot from the first accepted source
   rows. Give it the frozen transaction identity and source cut. The caller must
   preserve the original row identities and ordering when selecting bodies.
2. Prepare the narrow generation with that exact body reference and its bounded
   application-result checkpoint. Reusing a preparation must reuse its files and
   body identity.
3. Persist the validated native descriptors in the quorum catalog. The adapter
   checks the authorized body URI and table incarnation, the predecessor body
   snapshot, transaction and cut equality, preserved prior fragments, and matching
   inserted row counts. These checks do not replace the caller's source-row proof.
4. Publish the narrow manifest through the fenced managed catalog. Its transaction
   selects the prepared body reference. Uploading files alone does not select them.
5. Observe and validate the selected manifest against the retained descriptors
   before installing it or closing the source prefix. If publication returned an
   unknown result, inspect that same publication; do not prepare replacement files
   or replay a client mutation to infer success.

The active generation continues receiving updates while the frozen generation is
published. Installing the selected pair retires only that frozen generation, so a
later score or status update remains visible. Recovery uses the selected narrow
snapshot and its body identity, then applies the remaining WAL suffix.

This adapter does not collect either dataset or publish body compactions. Retain
the original preparation, source prefix, and both datasets' required files until
publication, reader, recovery, and other consumer dependencies permit reclamation.
It neither requires permanent business history nor provides cross-shard atomicity.

The publication tests use three TCP WAL replicas and cover restart before
publication and a lost response after manifest selection, including a later update
to the same turn and retention of its source record.
