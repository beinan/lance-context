# Durable table bindings and publisher epochs

Refs #378.

A publisher restart must recover the same table generation and distinguish its
authority from a previous owner's. The optional replication component now puts
table registration and expected-epoch acquisition through the catalog group's
normal Ready/commit/apply path. The lowest configured group is the one catalog
group for the layout. Independent source groups cannot register separate owners
for the same destination.

`Node::acquire_publisher` queues an `Acquire` command. Only
`Outcome::Committed(Applied::Publisher(Reply::Acquired(...)))` grants its authority;
the authority includes cluster, layout, group, table, owner, epoch and original
grant index. An exact retry of the current grant still goes through consensus and
returns that same index. An isolated former leader cannot complete it from local
state. A new request must compare the exact current epoch before advancing it.
Older grants do not have unlimited retained request history.

Registration binds the table UUID to a canonical destination and complete
`IngestContract`. Both remain immutable across takeovers. Destinations pair a
stable storage-namespace UUID with the catalog's canonical dataset key. A second
table cannot claim an already bound destination. Caller authentication and alias
canonicalization belong to the authorized catalog integration; raw client URIs
are not storage identities. The contract's record ceiling must fit the configured
replication record limit.

Acquisition commands have an 8 KiB size limit. The immutable replication policy bounds
registered tables (default 1,024, maximum 16,384). Reaching the limit rejects new
bindings while preserving retries and takeovers of existing tables. Control
commands are not accepted data records or producer-lane entries. They do not
increase data-page counts or keep their old command bodies in application packs.

Application images preserve the complete catalog at the applied cut, validate
scope, epochs, grant positions, destination uniqueness and contracts, then replay
the committed suffix. Metadata contains bounded, ordered table frames alongside
the existing data records and producer tails. Payload packs still contain only
opaque accepted data. The current replication identity and application image
format are v5, including [immutable publication plans](wal-publication-plans.md)
and [source pins](wal-source-pins.md).
Earlier v1/v2/v3/v4 application directories are rejected; this optional component does
not provide an in-place migration or change the production ingestion path.

`Node::publisher` exposes only a local applied view. It is not a quorum read,
lease, authorization check or proof of Lance visibility. A granted epoch alone
does not stop an old owner from writing outside this protocol. Durable publication
plans check the current authority at apply. Source pins verify the immutable
selection against the complete source prefix. Subsequent work must commit a
pinned Lance target, resolve its immutable outcome and authorize retirement from
that outcome. Table closing/rebinding, table-bound data admission and integration
with the actual context catalog remain separate requirements.

Tests cover competing acquisitions, immutable bindings, namespace collisions,
capacity, malformed scopes, schema/size limits and checkpoint validation. A real
three-process TCP test covers the quorum boundary, leader loss, exact grant retry,
snapshot transfer to a lagging replica, isolated-owner retry, takeover and a full
restart with the same grant index. Existing opaque payload/snapshot tests remain
part of the regression suite.
