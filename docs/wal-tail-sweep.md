# Automatic service for low-count WAL tails

A count-only merge trigger can leave one to seven sealed generations indefinitely
when ingestion stops below the default eight-generation threshold. The optional
master tail sweep periodically enqueues this work through the existing durable
scheduler. It does not use table names or table modification timestamps as a
proxy for the actual age of a WAL generation.

Set `MERGE_WAL_TAIL_INTERVAL_SECS=3600` on ordinary merge-scheduling masters to
revisit low-count tails after each rotation. The default is zero (disabled).
`MERGE_WAL_INTERVAL_SECS` must also be nonzero and workers must be configured;
maintenance-only replicas remain disabled. This does not enable the Kubernetes
catch-up controller or change any table's merge protocol.

The sweep uses the existing in-memory scalar stats snapshot, with
`MERGE_WAL_TAIL_STATS_MAX_AGE_SECS=900` by default. It never opens a table, scans
payloads or lists WAL objects. Missing, future or stale observations are excluded.
Only positive counts below `MERGE_WAL_MIN_GENERATIONS` enter this rotation; hot
work keeps the normal sweep. An ingestion/write/read hot path gains no new I/O.

An etcd CAS reserves at most `MERGE_WAL_TAIL_BATCH_SIZE=16` candidates per
fleet-wide 30-second batch, hard capped at 64. The durable name cursor prevents
busy early targets from hiding later targets and survives master replacement.
The interval applies after the last page of a rotation; it is not an exact age
measurement or a guaranteed completion deadline for every table. Existing task
cooldowns, active tasks, draining targets and dedicated catch-up owners are
respected. No lock, owner, attempt record or failure ledger is deleted or reset.

Page reservation precedes enqueue. A lost response or process crash may defer a
page to the next rotation, but never causes immediate replay of an uncertain
mutation or one replica's workload to be multiplied by every other replica.
An enqueue error is isolated to its target; other candidates continue. A new
rotation evaluates current fresh demand through normal scheduler deduplication.
`master_wal_tail_enqueue_requests_total` counts accepted enqueue requests, not
completed generations or necessarily newly created tasks during concurrent
normal scheduling.

This sweep covers low-count ordinary-worker WAL tails. It deliberately leaves
externally registered persistent publisher lifecycle to its current owner; those
tables need the separate desired-coverage migration before external babysitting
can be retired. Bounded rolling metadata coverage remains required.
