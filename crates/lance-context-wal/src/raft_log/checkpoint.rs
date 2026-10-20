use super::*;

pub(super) fn validate(snapshot: &Snapshot, conf: &ConfState) -> Result<()> {
    let metadata = snapshot.get_metadata();
    ensure!(
        metadata.index > 0
            && metadata.index < u64::MAX
            && metadata.term > 0
            && metadata.get_conf_state() == conf
            && !snapshot.data.is_empty()
            && snapshot.compute_size() as usize <= MAX_SNAPSHOT_BYTES,
        "invalid checkpoint cut, static membership or bounded descriptor"
    );
    Ok(())
}

impl GroupLog {
    /// Durably replace a committed prefix with an application checkpoint.
    ///
    /// The application MUST first validate the descriptor's scope and complete
    /// state at this exact applied index, fsync every referenced immutable file
    /// and its directory, and retain dependencies of both the old and new
    /// descriptors on an uncertain write. This layer treats Snapshot.data as
    /// opaque; it cannot establish application recovery or Lance publication.
    /// Its entire encoded Snapshot is limited to 4 KiB, for a small state or a
    /// descriptor rather than bulk payload transfer.
    ///
    /// This synchronously installs the descriptor, truncation boundary and term
    /// index in the same engine batch as logical compaction. It preserves the
    /// committed/uncommitted suffix and HardState. The same cut may only be
    /// retried with the identical descriptor. Remote snapshot installation and
    /// physical segment deletion are separate operations, not supplied here.
    /// The current replication::Node does not invoke this interface.
    pub fn checkpoint(&self, snapshot: &Snapshot) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| anyhow::anyhow!("group state lock poisoned"))?;
        self.node.check()?;
        validate(snapshot, &state.conf)?;
        ensure!(
            self.node.engine.get(self.group, b"application").is_some(),
            "bind the application before checkpointing"
        );
        let metadata = snapshot.get_metadata();
        ensure!(
            metadata.index >= state.boundary()
                && metadata.index <= state.hard.commit
                && state.term(metadata.index) == Some(metadata.term),
            "checkpoint is not a retained durable committed cut"
        );
        if metadata.index == state.boundary() {
            ensure!(
                *snapshot == state.snapshot,
                "checkpoint changed at the same cut"
            );
            return self.node.check();
        }
        let mut terms = state.terms.clone();
        terms.retain(|range| range.last > metadata.index);
        if let Some(first) = terms.first_mut() {
            first.first = metadata.index + 1;
        }
        let next = GroupState {
            hard: state.hard.clone(),
            conf: state.conf.clone(),
            terms,
            snapshot: snapshot.clone(),
        };
        next.validate()?;
        let mut batch = LogBatch::default();
        batch.put_message(self.group, SNAPSHOT.to_vec(), snapshot)?;
        batch.put(self.group, TERMS.to_vec(), encode_terms(&next.terms)?)?;
        // Engine::compact_to writes without sync and suppresses errors. Keep
        // the command and its recovery metadata under our shared failure fence.
        batch.add_command(
            self.group,
            raft_engine::Command::Compact {
                index: metadata.index + 1,
            },
        );
        self.node.write(&mut batch)?;
        *state = next;
        Ok(())
    }
}
