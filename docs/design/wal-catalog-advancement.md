# Catalog advancement after source closure

Tracking: [#378](https://github.com/lance-format/lance-context/issues/378).

After a managed Lance publication, source groups can durably close their exact
pins and attest those closures. `AdvancePublication` moves the catalog past that
publication only when every selected source supplies its matching `ClosedProof`.
The command verifies both signatures, the table and publication identity, the
complete source set, each independent range, and the digest of the exact source
selection. A valid proof for a different selection is insufficient. Missing or
mismatched proofs leave the entire catalog unchanged.

One catalog Raft command updates each source checkpoint to that source's
`through` position, raises the minimum read version to the published manifest,
and clears the pending plan, prepared descriptors and unresolved outcome. Source
indices are never collapsed to a maximum or compared across independent groups.
The next publication can then select new records. Source closure and catalog
advancement remain separate durable transitions, so a materializer can resume
after any intermediate failure.

One bounded `Advanced` receipt per table retains the completed source-set digest,
source frontiers and shared publication record. It contains no payloads or
prepared fragment descriptors. Exact retries return the original completion even
after publisher takeover or while the next plan is pending. Refreshed proofs for
the same closures have the same request identity; their read cuts do not change
the receipt. Only the latest advancement receipt is retained. New state changes
still require the current publisher authority. Publication confirmation retries
can also resolve this latest completed outcome.

Application checkpoint table frames retain their exact previous bytes until an
advancement exists. A bounded `LCWALAD1` suffix then carries the receipt. New code
reads the old frame and the extension; unknown, truncated and trailing suffix
data are rejected. The metadata budget includes one receipt per table. This is
forward migration support, not mixed-binary operation: upgrade all replicas
before submitting the new command, and do not downgrade an advanced directory.
The serialized public `Table` query value gains a field; arbitrary old external
bincode consumers are not compatible. Existing replication policy identities
and tables without advancement keep their previous checkpoint representation.

Advancement does not delete log files or Lance objects. Physical collection still
requires a durable application checkpoint and the existing file-lifetime rules.
Catalog ownership, transport authentication and source-key protection remain
deployment responsibilities.

Validation is in progress. Rust tests cover all-source atomicity, independent
source cursors, stale owners, refreshed proofs, missing or wrong signed proofs,
next-plan admission and strict legacy-frame compatibility. Real TCP child-process
tests extend actual Lance publication recovery with a lost advancement reply,
catalog checkpoint, all-replica restart and exact completion lookup. Passing
results and public PR qualification are required before this change is ready.
