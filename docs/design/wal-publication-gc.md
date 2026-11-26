# Publication recovery and Lance version cleanup

Refs #378.

A durable WAL receipt does not by itself make the corresponding payload safe to
retire. The publisher also needs a durable outcome for its immutable source
selection, and the evidence used to establish that outcome must survive cleanup.
The optional publication work does not yet enable this reclamation protocol.

The Rust integration tests use real Lance 13 APIs, transactions, data files and
full payload reads on a local object store. They establish three storage boundaries:

- Cleaning an older target version removes its exact transaction proof while all
  rows in the latest version can remain readable. A missing target is not proof
  that an append never committed.
- A numeric `CleanupPolicy::before_version` floor preserves a pending base and
  target, including their transactions, while permitting older history to be
  collected. The floor alone does not coordinate concurrent plan admission.
- A delayed fixed-target commit can recreate a collected version slot. The tests
  pause the commit immediately before its object-store write, allow another
  transaction to publish that target and a successor, collect the target, and
  then release the delayed commit. Both conditional PUT and atomic rename accept
  the now-absent target. A fresh reader finds the delayed transaction UUID at that
  old version, but the latest version excludes its payload. Control cases retain
  the target and reject the delayed foreign transaction.

These are characterization tests for the pinned Lance version, not production
GC coordination or a cloud performance measurement. They intentionally make
changes to these storage semantics visible when upgrading Lance.

The integration must preserve the pending base and target until the publication
outcome is durable. Cleanup admission and new plan admission must agree on a
persistent lower bound; sampling the current pending plans and then running an
uncoordinated cleaner leaves a race. Every relevant cleanup path must participate,
including automatic cleanup triggered by other writers.

Staged, uncommitted data files require a separate lifetime protocol. A retained
version floor does not reference those files, and a finite orphan-file grace
period does not protect an arbitrarily delayed publisher. An epoch check before a
storage write also does not revoke a write already in flight. Retiring manifests
or staged files must account for delayed writers as well as active readers and
snapshot transfers. Until these dependencies are resolved, retain the plan and
source data rather than interpreting missing or recreated target evidence as a
publication acknowledgement.

Run the storage characterization tests with:

```bash
cargo test -p lance-context-core --test publication_gc_proof --test publication_gc_stale_commit
```
