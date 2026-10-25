# Turn annotation mutation contract

Tracking: [#378](https://github.com/lance-format/lance-context/issues/378).
Related protocol: [#564](https://github.com/lance-format/lance-context/pull/564).

The first implementation target is a single-shard loop: bounded staging,
durable point updates, immediately visible point reads, batch publication, and
recovery/backpressure. Mixed import, asynchronous filtering indexes and shard
expansion follow that loop. This change supplies shared Rust value/mutation
types and deterministic field semantics. It does not enable an HTTP endpoint or
prove durability, concurrent application or staging retirement.

## Values and operations

`lance_context_api::annotation::TurnAnnotation` contains only `labels`, `status`
and `score`. It has no body, tokens or business history. The complete current
annotation can be empty: an empty label set and null status/score.

`TurnAnnotationPatch` has these operations:

| Field | Operation | Effect |
| --- | --- | --- |
| `add_labels` | Set of strings | Add to the existing label set; adding an existing label has no additional effect. |
| `remove_labels` | Set of strings | Remove from the existing label set; removing an absent label has no additional effect. |
| `status` | `{"op":"set","value":"reviewed"}` | Set the status string. |
| `score` | `{"op":"set","value":0.9}` | Set a finite Float32 score, matching the current rollout score type. No implicit range restriction. |
| `status` or `score` | `{"op":"clear"}` | Set that field to null. |
| Omitted field, or `{"op":"keep"}` | No update | Preserve the field's previous value. |

For example, this single patch adds a label and updates a score while preserving
status and all other labels:

```json
{
  "add_labels": ["reviewed"],
  "score": {"op":"set","value":0.9}
}
```

Labels compare exactly, without case folding or Unicode normalization. Duplicate
labels in one input become one set member. Empty labels are rejected. Adding and
removing the same label in one patch is rejected, without changing any fields.
An empty status string, score zero, and clearing either field remain distinct.
Bare JSON null is not a field operation; explicit `clear` avoids silently
confusing missing and null. Non-finite scores and unknown request fields are
rejected. This JSON representation describes API values, not a new WAL format.

`apply_to` validates the entire patch before changing the current annotation and
preserves every unsupplied field. The store must serialize applications to the
same turn; this method does not provide synchronization. An empty/no-effect
patch is valid and must still have request identity and progress when accepted.
CAS is outside the first implementation.

## Required service and retry semantics

The following remain requirements for the service integration:

- Resolve the authorized table incarnation and stable session/turn key before
  admission. A point update changes an existing turn; it does not create a turn
  or replace its body. The service supplies byte/cardinality limits and reserves
  capacity for the resulting annotation before accepting the write.
- Apply requests in the shard's committed order. If active has no value, use
  frozen and then the compatible Lance base before merging the patch. Two
  concurrent field updates must merge against that ordered current value, not
  two independent reads of an old base.
- Reuse the existing scoped producer generation/sequence identity. The client
  retains the exact identity and payload before sending. Unknown outcomes are
  reconciled with those original bytes; a new identity is a new mutation.
- An exact retained retry returns the original result without applying its
  mutation again. Changed bytes at an occupied identity are rejected. Older
  identities outside supported result retention are rejected, never silently
  treated as new writes. Do not infer deduplication from equal annotation values.
  In particular, retrying an old `score=0.9` after `score=0.5` must not restore 0.9.
- Successful mutation acknowledgement requires quorum durability and visibility
  of its applied result. A lost response may leave an unknown outcome. Validation
  failure before proposal is a definitive rejection; service errors must retain
  the distinction between rejection and an unknown committed outcome.
- A point read through another ingress observes every successful mutation that
  preceded the read. It addresses bounded staging by turn key, and projects only
  annotation columns on a Lance miss. Recovery/background apply may consume the
  WAL suffix; an ordinary point read must not scan that suffix. A lagging view
  must catch up or fail the read rather than return a stale annotation.

The publication/recovery protocol must retain request results and application
progress independently of dirty turn values. Publishing one frozen generation
does not retire later requests, even if the final annotation values are equal.

## Current checks and remaining gates

The API tests cover independent-field merging, set/remove label behavior,
explicit null clearing, empty/zero versus missing, malformed operations,
non-finite scores, whole-patch validation, and serialization round trips.
These are contract tests, not distributed concurrency or restart tests.

Before treating the single-shard loop as complete, use the actual Rust service,
RF3 WAL and Lance publication path to demonstrate bounded staging draining under
sustained updates, cross-ingress point reads without WAL-tail scans/body reads,
updates during frozen publication, unknown publication reconciliation, and
restart/GC/backpressure correctness. Later features do not substitute for these
gates.
