# Durable publication outcomes

The replicated catalog can now confirm that its selected immutable manifest
belongs to the exact pending plan and prepared descriptors. This records one
`Published` outcome through the catalog quorum. It provides the durable input
for source closure; closure and reclamation are not enabled by this increment.

`Node::confirm_publication` binds the dataset incarnation, transaction, plan and
preparation indices, descriptor digest, fixed target version and selected
manifest identity. The first confirmation requires the current publisher owner.
A mismatched or missing manifest cannot be converted into a successful outcome.
An identical retry resolves the original outcome and confirmation index even
after publisher takeover. This resolution grants no new write authority.

The outcome shares the immutable manifest record through `Arc`. Pending source
selection and prepared descriptor buffers retain their independent ownership.
Source positions and the publication checkpoint remain unchanged, and another
plan remains blocked. In particular, a catalog index is never used as a source
group cursor. No source pin, payload, descriptor, manifest or image is deleted.

Application identity and checkpoint format advance to v7. Older identities are
rejected without in-place migration. Images account for the bounded outcome
metadata and verify its exact selected manifest, pending plan, prepared digest
and catalog index ordering. Outcome metadata is bounded by active plans.

Validation includes rejected forged identities, stale-owner first confirmation,
exact retries after takeover, retained source dependencies, damaged checkpoint
state, and six-process tests with real Lance files. Those process tests discard
the confirmation reply, snapshot the catalog, restart all replicas and resolve
the same outcome before checking the actual transaction and full payloads.
These tests do not establish production integration or sustained throughput.

The next transition must durably fence closed source plans before releasing
their pins, preserve producer retry receipts after payload retirement, and retain
the selection until all source closures can be recovered. Physical collection
also needs active image/read ownership protection. Manifest history retention
and a production managed commit handler remain separate requirements.

Refs #378.
