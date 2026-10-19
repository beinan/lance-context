# Guarded experiment process runner

Tracking: https://github.com/lance-format/lance-context/issues/378

A disconnected terminal or timed-out remote command can leave an ingestion
experiment running. Starting another attempt changes the workload and can hide
the original failure. The optional Linux `context-wal-runner` records one launch
per operation directory and keeps observation separate from execution.

```sh
cargo build --release -p lance-context-wal --features runner --bin context-wal-runner
context-wal-runner start /tmp/example-operation \
  --executable /absolute/path/to/program --sha256 <executable-sha256> -- <arguments>
context-wal-runner observe /tmp/example-operation
context-wal-runner stop /tmp/example-operation
```

Use an existing parent directory and a fresh absolute operation path. The
create-only directory, synced request and locked supervisor journal prevent
concurrent launch or replay. Existing operations are observed; a different
executable/hash/argument request is rejected. Environment variables and working
directory are inherited from the first launcher, not part of the stored request;
callers must freeze and separately record their nonsecret configuration.

The supervisor detaches its streams and session, ignores hangup, checks the
executable hash again, records the child's boot/PID/start-time identity and waits
for completion. It syncs stdout/stderr and records their sizes/hashes plus exit
code, signal or spawn error. Callers still verify retrieved logs against those
hashes before treating a remote download as complete.

An `unknown` or `orphaned_running` result is not permission to restart. A crash
between process creation and identity recording can leave an unidentifiable
child; the tool preserves that uncertainty. `stop` explicitly marks the operation
and uses a Linux pidfd plus incarnation checks to signal a recorded child without
signaling a reused PID. It does not stop an entire descendant process tree.
Completed journals remain authoritative after process reaping.

This is a trusted, isolated experiment tool, not a service supervisor or an
exactly-once business operation mechanism. Do not remove/reuse operation
directories to recover an uncertain result. Keep them on durable storage if
their evidence must survive container/node loss. The executable and evidence
directory must not be modified by other actors; the hash is a reproducibility
check, not protection against executable replacement races or malicious input.
Arguments and logs can contain deployment information, so keep raw journals and
configuration private. The public tests use synthetic child processes only.
