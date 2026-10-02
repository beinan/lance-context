# Store registry migration to etcd

The rollout, generic and datagen registries map a name to a dataset URI and its
original creation timestamp. Payload data stays in object storage. The default
backend remains Lance. etcd provides point lookups without reading a Lance
manifest; existing worker store caches remain unchanged.

## Supported configurations

| REGISTRY_BACKEND | REGISTRY_MIRROR | Behavior |
|---|---|---|
| lance | unset | Existing Lance registry |
| lance | etcd | Lance authority, versioned etcd migration mirror |
| etcd | unset | Authoritative etcd registry; mirror writes sealed |

Other mirror combinations fail startup. In particular, etcd-to-Lance mirroring
and live rollback by flipping environment variables are **not supported**.
Both processes share the existing `ETCD_*` connection/TLS settings. No registry
backend is changed by merely deploying the binary.

## Versioned reconciliation

Each etcd entry lives at `<ETCD_PREFIX>/registry/<kind>/<name>`. Its JSON carries
`uri`, `created_at`, and, during migration, a Lance `source_version` and a
`deleted` tombstone flag. Tombstones are invisible to `get`, `contains` and
`list`. They retain ordering so a delayed upsert cannot resurrect a deletion.

A primary mutation commits to Lance first. The mirror then receives the entry
(or its absence) and version from a fresh source snapshot. Mirror failures are
logged without failing a successful primary mutation. Master startup and periodic
registry maintenance reconcile **all three kinds**, including datagen.
`POST /api/v1/registry/backfill?kind=rollout|generic|datagen` runs the same repair.
The `copied` response counts changed entries, including deletions and updates.

Reconciliation reads one consistent Lance snapshot, preserving URIs and creation
timestamps. Before applying it, it advances a persistent snapshot floor at
`<ETCD_PREFIX>/registry-migrations/<kind>`. Each destination write atomically
compares both that floor's etcd revision and the destination key's revision,
and rejects an older per-name source version. The floor also covers names never
seen by the destination: an old snapshot cannot insert a name that a newer
snapshot already observed as absent. A newer point update is protected against
an older snapshot's deletion pass. A crashed/partially applied reconciliation
can resume at the same source version; a newer snapshot can supersede it.

The source URI is bound into migration state. Do not recreate the Lance registry
at that URI or reuse an etcd namespace for a different source dataset. Use a
fresh namespace for a new source lineage. Ordinary version pruning does not
reset Lance versions. Retain migration state and tombstones; deleting them
removes the delayed-write protection.

`ETCD_PREFIX` also scopes master task records and locks. Do not rotate that
shared prefix merely to reset a registry mirror: preserving those records
requires a separate coordinated migration.

Once etcd is opened as authority, an atomic seal rejects all subsequent mirror
writes, including requests delayed in transit. Authoritative creation/discovery
can replace a migration tombstone. There is no read fallback to stale Lance.

## Cutover procedure

Mirroring repairs concurrent traffic; it does **not** make a rolling switch
between two authoritative backends safe. An empty live diff is an observation,
not cutover authorization.

1. Deploy compatible binaries with `REGISTRY_BACKEND=lance`,
   `REGISTRY_MIRROR=etcd`. Include every worker and auxiliary master. Keep one
   authoritative backend throughout this phase.
2. Stop admitting registry mutations across **every** writer and drain in-flight
   operations. This includes store create/delete, discovery/backfill and cold
   retirement in auxiliary masters. Stop all old writer processes before changing
   authority. External routing alone does not stop background registry writers.
   The application does not implement an automatic fleet-wide quiescence barrier.
3. While writers are quiescent, reconcile each of rollout, generic and datagen.
   Verify `GET /api/v1/registry/diff?kind=...` for each: `only_in_primary`,
   `only_in_mirror`, and `mismatched` must all be empty. `mismatched` includes
   both URI differences and creation-timestamp differences.
4. Start the new fleet with `REGISTRY_BACKEND=etcd` and **omit**
   `REGISTRY_MIRROR`. Before first activation, startup compares the destination
   against the Lance source and refuses mismatches. It then seals mirroring.
   Already active registries do not reconnect to Lance on each restart.
   Validate all three directories before reopening registry mutations.

Startup comparison cannot prove the absence of old in-flight Lance writes;
step 2 is required. First activation is per registry kind, so a startup failure
can leave some kinds sealed. Keep writers quiescent, inspect the failure and
repair the remaining unsealed kinds before retrying. Never clear a seal while
etcd writers might exist.

## Recovery and limitations

Losing an etcd mirror before cutover does not lose authoritative registry state:
rebuild it from Lance in a fresh etcd namespace. Snapshot repair handles missed
inserts, changed mappings, missed deletes and interrupted repair passes.

After cutover the Lance directories are historical snapshots, **not** rollback
copies. Returning to Lance requires a separate, verified export/import while
all registry writers are quiescent. Recreating entries from dataset directory
names alone cannot reproduce arbitrary URIs, original timestamps or deletions.
Back up etcd and do not describe directory discovery as a lossless rollback.

This changes registry metadata only. It does not fix WAL merge liveness,
compaction scheduling, or payload read failures.
