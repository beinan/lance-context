# Bucket locality during distributed preparation

The optional Rust ingestion application can assign each bucket's records to one
preparer within a publication. Set `preparation.partitioning` to
`"bucket_bytes_v1"` after upgrading every preparer. The default,
`"record_stripe"`, retains the existing round-robin record assignment and request
identity.

The publisher uses at most one part per distinct bucket. Each preparer derives
the same assignment from the immutable publication plan: total payload bytes per
bucket, largest bucket first, then the least-loaded part. Bucket ID and part
number break ties. Source metadata is sorted by bucket within each part; payloads
are fetched and decoded through the existing path without a repartitioning copy.

This reduces scattering across preparers. A preparer with multiple file writers
can still spread a bucket across files. Using one file writer per preparer is a
separate tuning choice to measure against write throughput. Assignment is local
to a publication; it does not change WAL routing or establish persistent worker
ownership. A hot bucket stays whole and may limit parallelism. Existing workspace
admission and publication-size bounds remain in effect.

The versioned assignment is part of the authenticated task identity and cached
result key. Legacy tasks omit the field; unsupported versions are rejected.
Changing the mode cannot reuse a partial result from the other mode. Publication
still requires every part, and a durably prepared aggregate survives loss of all
preparer caches.

Recovery coverage uses independent HTTP preparers, an unavailable endpoint,
partial-result reuse, actual WAL reopen and full-column Lance verification. File
membership is checked separately. This does not establish sustained throughput
or a production query speedup.
