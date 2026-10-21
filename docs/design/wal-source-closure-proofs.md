# Quorum-confirmed source closure proofs

Tracking: [#378](https://github.com/lance-format/lance-context/issues/378).

A catalog `ClosurePermit` authorizes closing an exact source pin after a durable
publication outcome. It does not establish that the source committed that closure.
`Query::ClosedProof { table }` returns the latest source closure after a safe
ReadIndex barrier, signed with the source group's own Ed25519 key. Before closure
it returns `None`. An isolated former leader cannot refresh a proof from local
state. Missing signers are rejected before reserving read capacity.

The proof binds the cluster, layout, immutable policy, source read cut, original
closure index and pin index, and the complete catalog-signed closure permit.
Verification checks the source signature, nested catalog signature and their
scope. Source log positions are not compared with catalog log positions. A
checkpoint and restart preserve the original closure; a later read can attest
that same closure at a newer source cut.

## Provisioning and compatibility

Set `Limits::source_keys` to distinct public keys for every configured group,
including groups that also host the catalog. Each key must differ from the catalog
key. Provision the corresponding private key only to that group's replicas using
`Node::set_source_signer`. Source-only nodes do not need the catalog private key.
Splitting a node into group actors retains only the appropriate source signer in
each actor. Private key holders are neither serializable nor printable.

An empty `source_keys` map disables these queries and preserves the exact v9
replication policy identity. Existing JSON configurations may omit the field.
Enabling or changing source keys uses a different immutable policy identity;
existing directories reject a mismatched policy. There is no online key rotation
or existing-directory migration in this change. The application checkpoint state
has not changed. This compatibility guarantee concerns persisted policy identity,
not arbitrary external bincode serialization of the public `Limits` struct.

These are attestations issued by trusted replicas after their consensus read
barrier, not threshold signatures or Byzantine quorum certificates. Deployment
must protect private keys and authenticate transport independently.

## Consumer boundary

Call `ClosedProof::verify` with the expected layout and policy, then match the
expected table, transaction, plan, source range and publication outcome before
using the statement. A previously issued valid proof remains a statement about
that exact closure; it does not establish that no later publication exists.

This increment does not advance catalog checkpoints, grant publisher authority,
or authorize deletion of Lance objects or log files. Catalog advancement must
separately require the exact closure from every selected source. Physical
reclamation still requires the corresponding durable checkpoint and file-lifetime
rules.

## Validation

Rust tests cover independent v9 policy-byte compatibility, key-policy rejection,
group-actor key isolation, missing-signer admission, proof tampering, an isolated
source leader, and checkpoint/restart preservation of the original closure.
The actual TCP child-process and Lance publication tests also request and verify
the source proof after publication/closure and after every replica restarts, while
ensuring source-only replicas receive no catalog private key. These tests do not
establish throughput or production key-management behavior.
