# Give prepared compaction the next commit turn

A prepared compaction can repeatedly exhaust its ownership wait budget when a
persistent publisher starts another merge pass as soon as the previous one ends.
The prepared files are discarded and rebuilt while the table keeps accumulating
fragments. Extending a timeout does not give the compactor a turn.

Once nonempty compaction output is ready, the master publishes a commit-ready
marker under the existing preparation claim lease. Subsequent MergeWal admissions
for that table yield to the matching live preparation. This includes native
catchup target claims. The claim transaction checks the marker again, so a
request arriving after the admission read cannot be missed by a stale claim.
Preparation alone creates no marker: merge can continue throughout file encoding.
An already admitted merge is never cancelled by this scheduling hint.

The compactor still obtains the normal exclusive write claim and storage fence
before commit. Its existing bounded commit wait applies. Finishing, abandoning
or losing the preparation lease removes the marker; it is not an independent
persistent table lock. The marker is ignored if its token does not match the live
preparation. Other tables continue normally. An unresolved merge execution can
still be claimed for recovery even when compaction is waiting, so the hint cannot
block the storage recovery needed to make the table writable.

No new per-file or per-generation polling is added: ready state is published once
and read with the existing admission snapshot transaction. The counter
`master_merge_yield_to_compaction_total` counts admission deferrals, not failures.
Deploy compatible scheduling/catchup binaries for every contender to honor the
hint. It does not forcibly interrupt old binaries or guarantee completion before
a progressing current merge finishes. Legacy worker self-merges outside master
admission retain their existing coordination limitations.
