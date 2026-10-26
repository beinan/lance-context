# Annotation file reclamation boundaries

Refs #378. These native cleanup experiments characterize the hazards relevant to
item 6 of the single-shard annotation loop. The managed annotation owner now has
an opt-in collector with its own fencing and reader protection; see
[Physical file collection](annotation-application.md#physical-file-collection).
The experiments below do not authorize using native cleanup on managed tables.

A column rewrite uploads a new annotation file for each affected fragment.
Lance 13 prunes fully superseded file references when building the selected
manifest. The original ID/body file remains referenced, as does the latest
annotation file. Older annotation objects still exist in storage. Empty staging,
source-WAL retirement and catalog-history retirement do not delete those objects.

The local Rust tests in `annotation_staging/tests/reclamation.rs` exercise three
boundaries with actual Lance column updates and actual file deletion:

- Retaining the version captured by an old point read preserves its annotation
  file. After releasing the reader and old base, advancing the cleanup floor
  removes that file. Reopening verifies current annotations and every body byte.
  This case runs with both stable and address-based row IDs.
- Native cleanup does not see an application's captured point-read handle.
  Removing its version deletes the old annotation file and breaks that read,
  even though the current table remains completely readable. The reader uses
  an independent cold Session; the writer's cached old column data can otherwise
  hide the deleted object.
- An uploaded, uncommitted annotation file looks unreferenced to native cleanup.
  Enabling deletion of unverified files removes it while leaving the selected
  base readable. Current-table readback alone cannot validate recovery safety.

Only disposable, exclusively owned local datasets use the native cleanup API in
these tests. No managed catalog, owner takeover, distributed reader pins or blob
service is qualified by them. The production `ManagedManifest` handler must
continue rejecting native enumeration and cleanup.

Managed collection must bind a deletion boundary to the table incarnation
and coordinate three sets of references: captured readers, selected manifests
and recovery evidence, and uploads from unresolved publications. Reader
registration and advancing the boundary must be ordered so a new reader cannot
acquire a version that has already become deletable. Existing readers must keep
their protection through I/O, including publication handoff and owner replacement.
An expired publisher lease alone is not evidence that an old reader released its
base. The chosen protocol must also bound abandoned-reader retention.

Keep this accounting at the owner/version level rather than adding a consensus
write per point read. In addition to replaced annotation data, unselected manifests
and uploads additionally need durable attempt identity and resolution. A blob
listing or age threshold cannot supply that identity. Until these conditions
are implemented, tested across restart, and connected to the managed catalog,
a cleanup path cannot safely reclaim historical or orphaned managed objects.
