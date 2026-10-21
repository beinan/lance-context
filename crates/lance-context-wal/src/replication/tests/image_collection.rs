use super::*;
use std::fs;

fn open(directory: &Path) -> Result<Vec<Node>> {
    (1..=3)
        .map(|id| {
            Node::open(
                &directory.join(id.to_string()),
                layout(),
                id,
                Limits::default(),
            )
        })
        .collect()
}

fn append(nodes: &mut [Node], group: u64, sequence: u64, token: u64) -> Result<()> {
    let mut input = batch(sequence);
    input.meta.bucket = u32::from(group == 20);
    nodes[0].propose(group, token, input)?;
    assert!(pump(nodes)?
        .iter()
        .any(|c| c.request == token
            && matches!(c.outcome, Outcome::Committed(Applied::Accepted { .. }))));
    Ok(())
}

fn checkpoint(node: &mut Node, group: u64, id: u128) -> Result<Arc<checkpoint::Image>> {
    let prepared = node.prepare_checkpoint(group, Uuid::from_u128(id))?.run()?;
    let image = prepared.image();
    node.install_checkpoint(prepared)?;
    Ok(image)
}

fn path(directory: &Path, group: u64, id: u128, part: &str) -> std::path::PathBuf {
    directory.join(format!(
        "context-application-{group}-{}.{part}",
        Uuid::from_u128(id)
    ))
}

fn drain(mut task: ImageCollection) -> Result<ImageCollectionProgress> {
    assert!(task.run_batch(0).is_err());
    assert!(task.run_batch(4097).is_err());
    let mut total = ImageCollectionProgress::default();
    for _ in 0..512 {
        let progress = task.run_batch(1)?;
        assert!(progress.scanned_entries <= 1);
        total.scanned_entries += progress.scanned_entries;
        total.retained_files += progress.retained_files;
        total.removed_files += progress.removed_files;
        total.removed_bytes += progress.removed_bytes;
        if progress.finished {
            total.finished = true;
            assert_eq!(
                task.run_batch(1)?,
                ImageCollectionProgress {
                    finished: true,
                    ..Default::default()
                }
            );
            return Ok(total);
        }
    }
    anyhow::bail!("collection cursor made no bounded progress")
}

#[test]
fn collection_preserves_live_images_sibling_groups_and_concurrent_appends() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("1");
    let mut nodes = open(directory.path())?;
    nodes[0].campaign(10)?;
    nodes[0].campaign(20)?;
    pump(&mut nodes)?;
    append(&mut nodes, 10, 1, 1)?;
    append(&mut nodes, 20, 1, 2)?;
    drop(checkpoint(&mut nodes[0], 20, 1000)?);
    let captured = checkpoint(&mut nodes[0], 10, 1001)?;
    append(&mut nodes, 10, 2, 3)?;
    drop(checkpoint(&mut nodes[0], 10, 1002)?);
    append(&mut nodes, 10, 3, 4)?;
    let candidate = nodes[0]
        .prepare_checkpoint(10, Uuid::from_u128(1003))?
        .run()?;
    let abandoned = candidate.image();
    assert!(nodes[0].prepare_image_collection().is_err());
    drop(candidate);

    for part in ["state", "pack"] {
        fs::write(path(&root, 10, 1004, part), b"orphan")?;
    }
    let foreign = path(&root, 99, 1005, "pack");
    let unknown = root.join("context-application-unrecognized.pack");
    fs::write(&foreign, b"foreign")?;
    fs::write(&unknown, b"unknown")?;
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&foreign, path(&root, 10, 1006, "pack"))?;
        fs::hard_link(&foreign, path(&root, 10, 1007, "pack"))?;
    }
    fs::create_dir(path(&root, 10, 1008, "state"))?;

    let mut task = nodes[0].prepare_image_collection()?;
    let (ready, wait_ready) = std::sync::mpsc::sync_channel(1);
    let (resume, wait_resume) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || -> Result<ImageCollectionProgress> {
        // Inspect one entry, then keep the actual collector alive across writes.
        let first = task.run_batch(1)?;
        ready.send(())?;
        wait_resume.recv_timeout(std::time::Duration::from_secs(30))?;
        let mut rest = drain(task)?;
        rest.scanned_entries += first.scanned_entries;
        rest.retained_files += first.retained_files;
        rest.removed_files += first.removed_files;
        rest.removed_bytes += first.removed_bytes;
        Ok(rest)
    });
    wait_ready.recv_timeout(std::time::Duration::from_secs(30))?;
    assert!(nodes[0].prepare_image_collection().is_err());
    assert!(nodes[0]
        .prepare_checkpoint(20, Uuid::from_u128(1009))
        .is_err());
    append(&mut nodes, 10, 4, 5)?;
    resume.send(())?;
    let first = worker.join().unwrap()?;
    assert_eq!(first.removed_files, 2);
    assert_eq!(first.removed_bytes, 12);
    assert_eq!(first.retained_files, 8);
    assert_eq!(
        captured.chunk(checkpoint::Part::Payload, 0, 4096)?,
        batch(1).body
    );
    for id in 1000..=1003 {
        assert!(path(&root, if id == 1000 { 20 } else { 10 }, id, "state").exists());
    }
    assert_eq!(fs::read(&foreign)?, b"foreign");
    assert_eq!(fs::read(&unknown)?, b"unknown");
    drop(captured);
    drop(abandoned);

    // The collector of one group must protect the other split owner's image.
    let mut workers = nodes.remove(0).into_groups();
    drop(workers.pop().unwrap());
    let second = drain(workers[0].prepare_image_collection()?)?;
    assert_eq!(second.removed_files, 4);
    assert_eq!(second.retained_files, 4);
    assert!(path(&root, 20, 1000, "state").exists());
    assert!(path(&root, 10, 1002, "pack").exists());
    assert!(!path(&root, 10, 1001, "pack").exists());
    assert!(!path(&root, 10, 1003, "state").exists());
    let cursor = workers[0].prepare_image_collection()?;
    drop(workers);
    drop(nodes);
    assert!(Node::open(&root, layout(), 1, Limits::default()).is_err());
    drop(cursor);
    let recovered = Node::open(&root, layout(), 1, Limits::default())?;
    for ((index, _), sequence) in recovered.accepted(10, 0, 128)?.entries.iter().zip(1..=4) {
        assert_eq!(recovered.read_accepted(10, *index)?, Some(batch(sequence)));
    }
    assert_eq!(recovered.accepted(10, 0, 128)?.entries.len(), 4);
    assert_eq!(recovered.accepted(20, 0, 128)?.entries.len(), 1);
    Ok(())
}

#[test]
fn abandoned_receiver_files_are_collected_and_uncertain_sync_fences_collection() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut nodes = open(directory.path())?;
    nodes[0].campaign(10)?;
    pump(&mut nodes)?;
    append(&mut nodes, 10, 1, 1)?;
    let image = checkpoint(&mut nodes[0], 10, 2000)?;
    let mut message = Message {
        from: 1,
        to: 2,
        term: nodes[0].status(10)?.term,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgSnapshot);
    message.set_snapshot(image.snapshot()?);
    let packet = Packet {
        cluster: nodes[0].layout.cluster,
        layout: nodes[0].layout_hash,
        policy: nodes[0].policy,
        group: 10,
        message,
    };
    let mut receiver = nodes[1].receive_snapshot(packet, Uuid::from_u128(2001))?;
    receiver.write(
        checkpoint::Part::Metadata,
        0,
        &image.chunk(checkpoint::Part::Metadata, 0, 8)?,
    )?;
    assert!(nodes[1].prepare_image_collection().is_err());
    drop(receiver);
    assert_eq!(
        drain(nodes[1].prepare_image_collection()?)?.removed_files,
        2
    );
    let accepted = nodes[1].accepted(10, 0, 128)?;
    assert_eq!(
        nodes[1].read_accepted(10, accepted.entries[0].0)?,
        Some(batch(1))
    );

    let root = directory.path().join("1");
    let orphan = path(&root, 10, 2002, "pack");
    fs::write(&orphan, b"retain after uncertain sync")?;
    let mut collection = nodes[0].prepare_image_collection()?;
    nodes[0].groups[&10].raw.store().fail_next_sync_for_test();
    nodes[0].propose(10, 2, batch(2))?;
    assert!(nodes[0].poll().is_err());
    assert!(nodes[0].prepare_image_collection().is_err());
    assert!(collection.run_batch(4096).is_err());
    assert!(orphan.exists());
    Ok(())
}
