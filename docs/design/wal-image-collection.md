# Collection of obsolete application snapshots

Tracking: [#378](https://github.com/lance-format/lance-context/issues/378).

After checkpoint installation, old application `.state` and `.pack` files can
outlive their recovery purpose. `Node::prepare_image_collection` returns a
cursor whose `run_batch` scans at most the requested number of directory entries
(1–4096). It removes only canonical image files for groups hosted by that node.
Unrecognized files, other groups, directories, symlinks and hard links remain.
The caller runs batches on a bounded blocking worker, retains the cursor between
batches, and drains or drops it before preparing another image.

The node shares one image registry and worker permit across split group owners.
The registry protects every durably installed recovery image, including one
whose group owner has been dropped while sibling groups continue. Weak entries
also protect any older image retained by a reader, outgoing transfer or abandoned
prepared-image handle. Builders, receivers and checkpoint installation exclude
collection through the same permit. Ordinary Raft append and poll work continues.

Captured images and collection cursors retain the native engine lock. Drop all
readers, transfers and cursors before reopening a failed node directory; a new
incarnation cannot delete files still used by the old one. After a process crash,
reopening validates and registers each installed snapshot before collection is
available. Complete uninstalled candidates and interrupted receive files are
then eligible for collection. An uncertain engine write fences collection too.

Every successful deleting batch syncs the directory. Its byte count is the sum
of removed file lengths, not device free space or reclaimed physical blocks.
No payload is scanned or rewritten. This operation does not collect Raft-engine
segments, Lance manifests or data files; those require their own dependency and
retention rules. Automatic scheduling and a complete disk quota remain separate
integration work.

Tests exercise bounded scanning with actual files, ordinary writes while a
collector is retained on another thread, captured-reader retention, dropped
sibling owners, engine-lock lifetime and uncertain-sync fencing. TCP child-process
tests cover uninstalled and partially received snapshots after SIGKILL, and old
source-pack removal after real Lance publication and all-replica restart. The
managed Lance reader then verifies the manifest, transaction and full payloads.
