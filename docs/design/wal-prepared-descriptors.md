# Durable materialization descriptors

Refs #378.

After source materialization, a publisher must recover the same immutable files
and fixed transaction target across retries and ownership changes. Rewriting
files during every ambiguous retry wastes I/O and can change which files a
transaction means. `Node::prepare_publication` records one descriptor set for an
exact pending `(table, transaction, plan_index)` through the catalog quorum.

The current owner can register descriptors once. An exact retry returns the
original preparation index; different bytes are rejected. A stale owner is
rejected even when its bytes match. Takeover retains both the selected source
ranges and the chosen descriptor bytes. It does not reset the preparation slot.
Preparation and retries need no additional active-plan slot.

Descriptors have a format identifier, bounded bytes and SHA-256 digest. Format 1
is the materializer's JSON representation of Lance 13 fragments. WAL replicas
check the representation's length and digest without deserializing fragments or
Arrow bodies. The caller remains responsible for validating fragment semantics
and source dependencies. A stored descriptor is not proof that remote sources
have pinned their data, an external commit authorization, or a visibility ACK.

Captured table views share the prepared metadata through `Arc` and `Bytes`.
Adding descriptors does not copy the plan's selected record list. The default
per-plan descriptor budget is 256 KiB, with a 1 MiB protocol ceiling. Commands
have separate type bounds, and checkpoint metadata accounts for both plans and
descriptors. The application/checkpoint format advances to v5; older identities
and images are rejected rather than silently migrated.

The `lance-integration` feature adds real Lance qualification dependencies. It
is not required by ordinary WAL replicas. Process tests write Arrow IPC to a
source quorum, pin its selection, decode the recovered source, write real Lance
files, and record their descriptors in the catalog. After restarting all six
processes, they recover the descriptors and attempt the same fixed target.
A second writer occupying that target must not cause the recovered plan to
rebase to another version. Full payload reads and actual transaction identities
are checked independently of the commit response.

These tests retain manifests and data files and disable automatic cleanup.
External manifest authority, durable outcome resolution, source closure and
collection remain necessary before enabling publication-driven retirement.
The fixed-target test handler is not a production GC fence; the delayed-write
cleanup race in the publication-GC regressions still requires coordinated
version identity and admission state.
