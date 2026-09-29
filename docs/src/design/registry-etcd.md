# Design: move the store registries from Lance tables to etcd

Status: proposal. Follows #274 (which makes the Lance registries sustainable
enough that this is not urgent).

## What the registry is and who touches it

Three tables, one per store kind, each a directory `{name -> uri, created_at}`
shared by every worker and the master:

| table | rows (prod) | writers | readers |
|---|---|---|---|
| `_registry.rollout.lance` | ~10.7k | worker create/delete, master retire | every worker miss on `get_or_open_*`, master scan (every 300 s, `list()`), discovery backfill |
| `_registry.generic.lance` | ~600 | same | same |
| `_registry.datagen.lance` | small | same | same |

Every call site today goes through `RolloutRegistry` (`crates/lance-context-core/src/registry.rs`):
`contains`, `get`, `list`, `upsert`, `remove`, `insert_missing`. Each read does
`checkout_latest` first, i.e. one manifest read from ADLS per call. Each `upsert`
is delete + append, two Lance commits with optimistic concurrency across 20
workers.

### Why the Lance table is the wrong tool here

- A directory is a key-value set with point lookups and rare listing. Lance is
  a columnar table optimised for scans; every point lookup pays a manifest read
  plus a scan, and every write pays a full commit protocol.
- Without maintenance it grows quadratically (#274 explains the 29 GB). With
  maintenance it still costs a manifest read per `contains`, ~0.7 s on ADLS
  today, 1.5 s+ under throttling, and 20 workers do it on every cache miss.
- Writes from 20 workers contend on one commit point. Lance retries, but a
  create under ADLS throttling took 47 s on average and failed 79 times in the
  last 15 h.
- Existence checks and store opens are the hot path for *every* API call that
  misses the worker's LRU. Registry latency is user-visible latency.

### What etcd gives

- Point `contains`/`get`: one round trip, single-digit ms, from any pod.
- `upsert`/`remove`: one transaction, no manifest, no commit retries.
- `list` with prefix: fine at 10k keys (etcd pages by default; the master
  scans every 300 s and already tolerates seconds).
- Watch: workers can invalidate their caches on create/delete instead of
  polling; not needed for v1 but free.
- The master already runs etcd (task store, coordination locks, cooldowns) and
  the cluster exposes `rocketkeep-etcd:2379` to the namespace. Workers do not
  link `etcd-client` yet.

### What etcd does not give, and how we cope

- **Not the source of truth for data.** The dataset on ADLS is. The registry
  only says "this name exists and lives here". If etcd is lost we must be able
  to rebuild it. The master already has `discovery.rs` (`insert_missing` from a
  directory listing); keep it and point it at etcd.
- **Size.** Value is `{uri, created_at}` (~200 B). 11k keys is ~2 MB. etcd is
  comfortable to a few hundred MB; irrelevant here.
- **Availability coupling.** A worker that cannot reach etcd cannot create or
  open a store it does not have cached. Today a worker that cannot reach ADLS
  is equally dead, and etcd is a 3-node cluster inside the same k8s cluster, so
  this is not a new failure domain in practice. Still: reads fall back to the
  in-process cache, and we keep the Lance table as a read-only fallback during
  the migration window (below).

## Data model

```
<prefix>/registry/<kind>/<name>   ->  {"uri": "...", "created_at": 1790000000000}
```

- `<prefix>` is the same `ETCD_PREFIX` the master uses (`/lance-context` by
  default), so one etcd holds one deployment's tasks and registries together.
- `<kind>` is `rollout`, `generic`, `datagen`.
- `<name>` is the store name, already validated to be a portable path segment
  (`validate_name`), so it is a safe key segment with no escaping.
- Value is JSON. No lease: registry entries are durable until explicitly
  removed.

## Code shape

A trait in `lance-context-core` so the server and master do not care which
backend they have:

```rust
#[async_trait]
pub trait StoreRegistry: Send + Sync {
    async fn contains(&self, name: &str) -> LanceResult<bool>;
    async fn get(&self, name: &str) -> LanceResult<Option<RegistryEntry>>;
    async fn list(&self) -> LanceResult<Vec<RegistryEntry>>;
    async fn upsert(&self, name: &str, uri: &str) -> LanceResult<()>;
    async fn remove(&self, name: &str) -> LanceResult<bool>;
    async fn insert_missing(&self, entries: &[(String, String)]) -> LanceResult<usize>;
}
```

Note `&self`, not `&mut self`: etcd needs no handle mutation, and the
`RwLock<RolloutRegistry>` everywhere today exists only because Lance handles
must `checkout_latest`. Dropping the lock removes a serialisation point every
worker request goes through.

Two impls:

- `LanceRegistry` — today's `RolloutRegistry`, wrapped. Keeps `maintain()`.
- `EtcdRegistry` — `etcd_client::Client` + prefix. `upsert` is a plain `put`
  (idempotent by construction; the delete-then-append dance goes away).
  `insert_missing` is a batch of `txn(version(key)==0 → put)`.

Selection by config: `REGISTRY_BACKEND=lance|etcd` (default `lance` until the
migration below is done), plus the existing `ETCD_ENDPOINTS`/`ETCD_PREFIX`/TLS
flags lifted from the master's config into core so the server can reuse them.
`etcd-client` becomes a dependency of `lance-context-core` (feature-gated
`etcd`, on by default).

Worker-side cache: `get_or_open_*` already has an LRU of open stores and only
asks the registry on a miss. Keep that. Add a small negative cache
(name → not-found, 5 s TTL) so a client hammering a non-existent name does not
turn into an etcd storm; cheap and safe because creates go through the same
worker fleet and invalidate locally, and a 5 s stale "not found" on another
worker is the same window Lance gives today.

## Migration

Zero-downtime, three deploys, each independently revertable.

1. **Dual-write** (`REGISTRY_BACKEND=lance`, `REGISTRY_MIRROR=etcd`): reads hit
   Lance as today; every `upsert`/`remove` also goes to etcd, best-effort with
   a warning on failure. Master runs a one-shot backfill on startup
   (`list()` from Lance → `insert_missing` into etcd) under the existing
   `state-init` coordination lock, and re-runs it every maintenance round so
   any mirror miss heals. Ship this, let it run a day, then compare: a
   `GET /api/v1/registry/diff` on the master lists keys present in one backend
   and not the other. Expect empty.
2. **Read from etcd** (`REGISTRY_BACKEND=etcd`, `REGISTRY_MIRROR=lance`):
   reads hit etcd; writes still mirror to Lance so a revert to step 1 loses
   nothing. Watch worker `rollout_store_cache_misses_total` latency and
   `POST /rollouts` latency drop. Run a few days.
3. **etcd only** (`REGISTRY_BACKEND=etcd`, no mirror): stop writing the Lance
   tables. Leave them on disk for a while as a cold backup; `discovery.rs` can
   rebuild etcd from the ADLS directory listing regardless.

Each step is env-only in mango; no image change between steps once the code is
in. Revert at any step is flipping the env back.

## Master changes

- `MasterState.registry` / `generic_registry` become `Arc<dyn StoreRegistry>`.
  The scanner's `list()` every 300 s is unchanged in shape.
- Registry maintenance from #274 stays for the Lance backend and becomes a
  no-op for etcd.
- New `GET /api/v1/registry/diff` (step 1 verification) and
  `POST /api/v1/registry/backfill` (manual re-sync).
- Retirement (`retire_cold_experiments`) calls `remove()` on the trait; no
  change.

## Worker changes

- `AppState.{rollout,generic,datagen}_registry` become `Arc<dyn StoreRegistry>`;
  the `RwLock` goes away.
- `create_*_store`: `contains` → open/create dataset → `upsert`. Same order;
  the dataset is still created before the registry row so a crash between them
  leaves an orphan dataset (discovery picks it up) rather than a dangling
  registry entry. Unchanged semantics, ~45 s less latency.
- `get_or_open_*`: `contains` on miss, as today. The double `contains` (before
  and after the open) can stay; it is now two 2 ms calls.
- `unregister_*`: `contains` → delete data → `remove`. Unchanged.

## Consistency

- **Create/create race** on the same name from two workers: today, both pass
  `contains`, both create the dataset (Lance `Create` is put-if-absent so one
  fails), the winner upserts. With etcd, same: dataset creation is still the
  arbiter; the loser's `contains` re-check after the failed create returns
  true and it answers 409. No change in outcome.
- **Create/delete race**: today serialised per name only within one worker
  (`rollout_handles.lock(name)`); across workers it is last-writer-wins on the
  Lance table. etcd is the same but with a smaller window. If we want it
  strictly ordered we can use `txn(mod_revision == expected)`; not proposed
  for v1 because the current behaviour has not caused a problem.
- **Read-your-writes**: etcd reads are linearisable by default. A worker that
  just created a store sees it from any other worker immediately, which is
  *better* than today (Lance `checkout_latest` is also strongly consistent, so
  no regression, but etcd removes the 0.7 s manifest read).

## Rollback

- Step 1 → nothing to roll back; the mirror is additive.
- Step 2 → flip `REGISTRY_BACKEND=lance`. Lance was kept current by the mirror.
- Step 3 → re-enable the mirror, run backfill etcd → Lance (the same
  `insert_missing` path in reverse, exposed as
  `POST /api/v1/registry/backfill?to=lance`), flip.

## Effort

- core: trait + `EtcdRegistry` + config plumbing, ~400 lines incl. tests
  (tests run against the etcd the master tests already require).
- server: swap type, drop the `RwLock`, mirror logic, negative cache, ~150 lines.
- master: swap type, backfill, diff/backfill routes, ~150 lines.
- mango: three env flips.

Roughly two days of work plus the staged rollout. Not blocking anything
today: #274 takes the registry from "quadratic and failing" to "linear and
slow", and the etcd move takes it to "fast".

## Not in scope

- Moving `_stats` to etcd. It is a 10k-row table rewritten as one snapshot per
  scan round and read for range queries (`list_above_fragment_count`,
  `list_above_pending_wal`), which etcd cannot do; it stays in Lance.
- Moving MemWAL shard manifests. They are per-shard, written by one writer,
  and read by the LSM scanner; different problem.
