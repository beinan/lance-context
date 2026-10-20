# Source payload retirement with durable retry receipts

Applying a verified source closure now retires only the exact records selected
by its durable pin. Each affected producer lane retains a compact sequence/index
boundary, while its latest accepted metadata and original ACK index remain
independent of payload retention. Retirement walks the bounded selection; it
does not scan all accepted records, decode Arrow or copy payload buffers.

An exact latest-sequence retry returns the original accepted index after
retirement, snapshot and restart, without recreating its payload. Changed
content at that sequence remains rejected. New writes continue from the latest
producer sequence and can use the released accepted-record capacity. Records
for other tables and a suffix outside the closed selection remain retained.

Application identity and image format advance to v9, without in-place migration
from older identities. A retired boundary must belong to an existing producer
receipt and a verified source closure. Retained records must continue from that
boundary without gaps and end at the latest receipt. A producer with no retained
records must have its latest sequence and original index exactly at its retired
boundary. Missing payloads do not justify arbitrary or fabricated receipt state.
Boundary capacity is bounded by producer capacity and adds no per-session state.

New application images omit retired bodies and may have an empty payload pack
while preserving producer receipts. Existing captured images and in-flight
checkpoint builders retain their own data ownership. Tests exercise a builder
captured before closure but completed afterward, an older image read across
installation of an empty image and restart, restored retry behavior, interleaved
table and suffix preservation, and new writes after a full record cap is freed.
Real Lance six-process tests verify an empty new source payload image, full
restart, the original ACK on retry and actual published Lance payloads.

This is logical retirement and smaller replacement images. Existing Raft-engine
files and older images are not physically collected yet. Producer identities are
also retained within their configured bound. Catalog progression, coordinated
manifest history retention, production service integration and sustained
throughput/disk bounds remain unfinished.

Refs #378.
