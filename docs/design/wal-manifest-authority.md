# Replicated manifest selection

Refs #378.

A publisher epoch checked before a blob write cannot revoke a write already in
flight. The optional catalog manifest index instead selects a unique immutable
staging path through the same durable quorum as the publication plan. Managed
readers resolve that path from the catalog; they must not import a missing row
from object-store listing or a canonical version path.

`bootstrap_manifest` adopts one verified immutable manifest for an exclusively
managed dataset incarnation. The caller must verify its actual transaction and
exclude unmanaged writers and cleaners before adoption. This call does not
establish those external preconditions or validate object bytes on replicas.

`reserve_manifest` requires the current owner, incarnation, exact pending plan,
prepared descriptor index/digest, fixed target and current head identity. The
head comparison is stronger than merely finding an older retained predecessor.
The quorum chooses one path and advances the head in the same state transition.
An ETag is not used as logical version identity. Staging paths identify their
attached version; canonical paths cannot be selected for new publications.

An exact retry of an already selected object resolves its original record even
after publisher takeover. This does not grant a new write. Reporting that old
successful request as newly refused could cause its caller to delete a committed
staging object. A different object for an occupied target returns the chosen
record; it never replaces it. Unknown RPC outcomes require quorum readback and
retention, not a retargeted transaction.

`Query::Manifest` returns one reference-counted record, including for latest
lookup, rather than serializing all retained history. Captured table views share
the index and immutable records. There are at most 128 versions per table and a
configurable node policy bound of 4,096 records by default. Capacity refuses new
reservations while preserving exact retries. Application identity and checkpoint
format advance to v9 and include the index in bounds and integrity validation.

The real Lance test adapter writes unique staging manifests using the public
`ManifestWriter`, then reserves them through six persistent TCP WAL processes.
Its reader resolves latest and explicit versions strictly through quorum
queries. Tests cover snapshot/restart and a lost response after catalog commit,
and inspect actual Lance transaction UUIDs and complete payloads. A third test
pauses an old owner after its real manifest upload, commits a publisher takeover,
then resumes the old reservation. The old owner must leave the target vacant;
only the new owner may select a path. This adapter
is a test transport bridge, not the production ingestion service.

No index row, manifest, descriptor or source pin is collected in this increment.
Outcome advancement, source closure, active-reader protection, bounded orphan
handling and coordinated cleanup remain required. Production adapters must also
override version enumeration, detached-version behavior and maintenance APIs;
the test adapter is deliberately limited to publication and direct reads.
URI-only readers and unmanaged writers do not satisfy this managed contract.
