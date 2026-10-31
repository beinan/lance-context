# Bound work before durable Raft progress

The runtime polls Raft after draining a batch of control, peer and data work.
Without a time bound, a proposal can wait behind subsequent expensive work
before its replication messages are emitted, or before its completion is
delivered. A command-count bound alone does not distinguish small metadata
messages from large payload validation and local reads.

The worker now ends the drain batch after a round has consumed `batch_time`
(1 ms by default), or after the configured `batch_requests` count, whichever
comes first. Service JSON represents `batch_time` as milliseconds, from 1 to 1000.
Cheap work can still share a poll. A round visits the existing snapshot-result,
control, peer and data lanes in the same order. The budget is checked between
rounds: it cannot interrupt one blocking operation, bound disk latency, or
guarantee a request deadline. There is no new timer task, sleep or storage format.

Polling retains all existing persistence, quorum, apply and failure barriers.
Credits remain owned through the poll, and client completions follow the
successful poll exactly as before. More frequent polling can increase native
write/sync or transport activity; a mixed-load comparison must assess throughput,
refusals, tail latency and sustained materialization before recommending it.

The regression uses actual RF3 workers and engines. A controlled slow round
queues an append ahead of the next blocked control request. Replica traffic
must be emitted before that next request is released, while the append stays
unacknowledged until its actual quorum/apply completion. The test reopens every
original engine and checks full payload bytes. The old count-only scheduler
fails the intended progress assertion after cleaning up all original workers.
