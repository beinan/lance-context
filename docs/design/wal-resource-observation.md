# Observe one Rust experiment process

Tracking: https://github.com/lance-format/lance-context/issues/378

The optional Linux resource sampler observes an existing operation from
`context-wal-runner`. It never launches, restarts or stops the target:

```sh
cargo build --release -p lance-context-wal --features runner --bin context-wal-resource
context-wal-resource --operation /tmp/example-operation --interval-ms 500
```

The operation must already have a recorded child identity. The sampler validates
the boot ID, PID and process start time. It checks that identity before and after
each sample and emits a completion record when the recorded incarnation ends.
PID reuse cannot silently switch the measured process. Run the sampler in the
same container as the target: it requires cgroup v2 with a namespace rooted at
`0::/` and validates that target and observer belong to the same cgroup.
It also verifies that the mounted cgroup root directly contains the sampler,
so entering a cgroup namespace alone cannot attribute an ancestor's counters.

JSON Lines output records process CPU ticks, RSS and procfs I/O; cgroup CPU usage,
throttling, current memory and memory events; actual elapsed times and collection
duration. An optional `--disk-directory /tmp/example-wal` counts allocated blocks
in a flat directory, deduplicates hard links and rejects symlinks or subdirectories.
Files removed concurrently by collection may disappear during the scan.

The target can lose procfs I/O access during exit before its process state shows
a zombie. On a permission-denied sample, the observer waits at most 100 ms for
positive evidence that the exact incarnation ended. Only then may it discard the
incomplete sample and finish. A permission error for a still-live target remains
an error. Read failures include the location being observed.

These are sampled observations, not continuous peaks, quotas or physical device
write amplification. Directory scans and counter reads are not atomic. Process
and cgroup counters have different scopes and must not be added: other processes
and the sampler itself can contribute to the container counters. Collection starts
after the target identity is available and can miss startup or short-lived peaks.
Record the actual observation interval; its CPU total is not an ingestion-only
cost unless that interval has independently been aligned to ingestion. Observer
overhead is included and needs a separate comparison to quantify.

Five unit tests cover stat parsing, PID reuse, directory accounting and real
procfs permission transitions for exiting versus still-live targets. A separate
process test verifies sampling and completion after target exit. CI explicitly
enables these optional-feature tests; the integration test runs unprivileged in
private mount/cgroup namespaces with a namespace-rooted cgroup2 mount. The
ignored test fixture is invoked by the
permission regressions. The tool does not change production ingestion semantics.
