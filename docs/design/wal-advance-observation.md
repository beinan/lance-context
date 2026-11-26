# Observe uncertain publication advancement

After a Lance publication is confirmed and its sources are closed, the publisher
submits `Advance` to move the materialization cursors. A lost or unknown reply can
stop the annotation runtime even when the catalog has completed that operation.

After a submission error, the publisher makes one bounded quorum observation.
It checks the current owner, table binding and incarnation, then matches the
catalog's existing `Advanced` receipt against the original publication index and
complete source closure identity. A match returns the original completion so the
runtime can install the published prefix and continue draining the active suffix.
This adds no command resubmission and no new persistent format or history.

A missing or different receipt, unavailable quorum, or fenced owner still returns
an error. Observation does not establish failure and does not authorize discarding
staging or replaying an uncertain client mutation. This only resolves outcomes
already proven by the catalog; it is not a guarantee of progress through a quorum
outage.

RF3 service tests withhold an actual committed reply, synthesize an unknown reply
without submitting the command, present a different completion, and fence the
owner before observation. A runtime test acknowledges concurrent status and score
updates while the first completion is withheld, then verifies current reads and
draining after observation. CI explicitly runs these service-enabled tests.

Refs #378.
