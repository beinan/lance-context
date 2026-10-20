# Durable source closure

A catalog quorum read can issue a signed closure only after the exact pending
publication has a durable `Published` outcome. The compact statement binds the
table, transaction, source range and selection digest to the selected manifest
identity and dataset incarnation. Source replicas need only the public catalog
key. An old isolated catalog cannot issue a fresh quorum attestation.

`Node::close_source` replicates that statement in the source group. Apply checks
the exact durable pin, persists a closed-plan fence and source cursor, and then
releases the pin. Delayed pin permits for the closed plan cannot recreate it.
The latest exact closure retry returns the original source closure index, even
when its catalog attestation was refreshed after publisher takeover. Catalog
and source indices remain independent.

Closed-source records are bounded by registered-table capacity, separately from
active pin capacity. Raw unregistered stream writes cannot allocate closure
history. The compact closure retains no source record list or payload buffers.
Subsequent pins must start at the saved source cursor and advance the catalog
plan identity; retained history cannot be repinned from an old cursor.

Application identity and image format advance to v9, without in-place migration
from older identities. Snapshot validation verifies closure signatures, scope,
local index ordering and compatibility with any remaining active pins.

Closure now also retires its exact accepted payload range as described in
[source retirement](wal-source-retirement.md), preserving producer receipts and
sequence boundaries. Catalog selections, descriptors, manifests and existing
image files remain retained. Catalog advancement, physical disk reclamation and
production ingestion are still separate work.

Tests exercise closure after real Lance publication, dropped closure replies,
source snapshots, full six-process restart, delayed pin rejection and exact
closure retry. Source state tests also cover signature tampering, invalid
checkpoint fences and preservation of data outside the selected range.

Refs #378.
