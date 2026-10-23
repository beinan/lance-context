# Managed Lance compaction

Part of [#378](https://github.com/lance-format/lance-context/issues/378).

Compacting a managed table must publish through its catalog, just as ingestion
does. Calling an ordinary URI-based compactor would bypass the selected manifest
and publisher fencing.

The catalog represents compaction as a publication plan with no WAL sources.
It retains the existing fixed read version, transaction, target version and
publisher authority. Only descriptor format 2, the JSON representation of a
Lance 13 `RewriteResult`, may prepare this plan. Append plans require format 1
fragment descriptors. Replicas check this distinction and the descriptor bounds
and digest; the materializer must validate the actual fragment rewrite.

Preparation, manifest selection and confirmation retain their existing exact
identities across takeover and restart. Completion requires an empty closure set,
advances the minimum Lance read version, and leaves every WAL source cursor
unchanged. A compaction plan cannot issue a source closure permit. Append
completion still requires every selected source's signed closure. The retained
completion digest distinguishes an empty source set from a damaged append receipt.

This increment adds catalog support only. File preparation and automatic worker
scheduling are not enabled. It does not delete old Lance files. Upgrade all WAL
nodes and publishers for a table before creating compaction plans: previous
binaries reject the new plan and descriptor values and cannot resume them.
Existing append commands and checkpoint encodings remain unchanged.

Catalog tests cover prepare-format mismatches, empty-proof rejection for append,
takeover with retained prepared output, checkpoint round trips through publication,
unchanged source cursors, and a completion retry while the next append is pending.
