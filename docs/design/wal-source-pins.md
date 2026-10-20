# Catalog attestations and durable source pins

Refs #378.

A catalog plan identifies source records but cannot prove that a source still has
its complete selected prefix. `Node::pin_source` now verifies a catalog attestation
and commits the exact source dependency through that source group's quorum.
The source rejects omitted, changed or future records. Independent catalog and
source indices remain separate; only a source's own `through` position must
precede its pin entry.

A `Query::Pin` request names an exact table, transaction, catalog plan index and
source group. Only safe ReadIndex completion at the catalog leader can issue its
Ed25519 attestation. The signed metadata includes cluster/layout/policy scope,
the confirmed catalog cut, ingestion contract and source selection. A caller
cannot turn a serialized local view into a valid attestation. The signature is
on publication metadata, not on individual data payloads. Replication continues
to treat Arrow bodies as opaque bytes.

The optional immutable `Limits::catalog_key` contains the public verification
key. Trusted catalog replicas install a matching PKCS#8 private key with
`set_catalog_signer`; source-only replicas need no signing key. Private bytes
are never included in the application identity, snapshots, queries or commands.
Key provisioning and rotation are deployment responsibilities. Changing the
public key changes the durable policy and requires an explicit migration.
This is an attestation by a trusted catalog service after quorum confirmation,
not Byzantine consensus, transport authentication or authorization for callers.
Existing peer and producer transports still require their own authentication.

A refreshed observation or publisher takeover retains the immutable selection.
Its retry traverses source consensus and returns the original pin index. Shared
`Arc` ownership avoids copying a pin's selected metadata into captured views.
Pins survive local checkpoints, received snapshots and retained-storage restart;
checkpoint validation verifies both signatures and complete source prefixes.
Command and image metadata lengths and the number of active pins are bounded.

The application/checkpoint format advances to v8 and rejects older identities
and images. There is no in-place migration in this optional component.

This increment retains pins and payloads. It does not enable release, advance
source publication cursors or permit Lance commits or deletion. All initial
source prefixes begin at zero. Outcome resolution must subsequently add release
fences, including for sources that never pinned, before catalog plans may be
removed or their slots reused. The cleanup and recovery boundary
requires separate coordination for manifests and staged files; a signed pin is
not a publisher lease or proof of Lance visibility.

Validation exercises disjoint catalog/source replica sets, safe query completion,
changed and omitted records, signature tampering, independent numeric indices,
source snapshot transfer, publisher takeover and full process restart. The six
TCP processes use real persistent Raft logs; source replicas hold only the public
key and recover the original pin index and exact source bytes.
