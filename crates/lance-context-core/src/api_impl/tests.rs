use super::*;

#[test]
fn owned_rollout_conversion_moves_buffers_and_preserves_every_field() {
    let mut request: AddRolloutRequest = serde_json::from_value(serde_json::json!({
        "id":"row-1", "rollout_id":"session-1", "problem_id":"problem-1",
        "dataset":"synthetic", "sequence_order":7, "role":"artifact",
        "created_at":"2026-01-01T00:00:00Z", "content":"text",
        "content_type":"application/octet-stream", "model_input_string":"input",
        "model_output_string":"output", "rationale":"reason", "problem_text":"problem",
        "user_metadata":"user", "input_tokens":[1,2], "output_tokens":[3,4],
        "num_input_tokens":2, "num_output_tokens":2,
        "output_logprobs":[-0.5,-0.25], "input_logprobs":[-1.0,-0.5],
        "ref_logprobs":[-2.0,-1.0], "loss_mask":[1,0],
        "advantage":0.5, "reward":1.0, "raw_reward":2.0, "grader_id":"grader",
        "score":0.25, "include_in_training":false, "exclude_reason":"test",
        "policy_version":"v1", "relationships":[{"target_id":"parent","relation":"parent","weight":0.5}],
        "payload_size":1048576, "payload_checksum":"synthetic-checksum",
        "artifact_type":"test", "metadata":{"nested":{"key":"value"}}
    })).unwrap();
    request.binary_payload = Some(vec![91; 1024 * 1024]);
    let expected = serde_json::to_value(&request).unwrap();
    let blob = request.binary_payload.as_ref().unwrap().as_ptr();
    let tokens = request.input_tokens.as_ref().unwrap().as_ptr();
    let probabilities = request.output_logprobs.as_ref().unwrap().as_ptr();
    let text = request.model_input_string.as_ref().unwrap().as_ptr();
    let record = RolloutRecord::from(request);
    assert_eq!(record.binary_payload.as_ref().unwrap().as_ptr(), blob);
    assert_eq!(record.input_tokens.as_ref().unwrap().as_ptr(), tokens);
    assert_eq!(
        record.output_logprobs.as_ref().unwrap().as_ptr(),
        probabilities
    );
    assert_eq!(record.model_input_string.as_ref().unwrap().as_ptr(), text);
    assert_eq!(
        serde_json::to_value(rollout_record_to_dto(record)).unwrap(),
        expected
    );
}

#[test]
fn owned_rollout_conversion_preserves_defaults_and_explicit_empty_problem() {
    for (problem, expected) in [(None, "session"), (Some(""), ""), (Some("p"), "p")] {
        let before = Utc::now();
        let record = RolloutRecord::from(AddRolloutRequest {
            rollout_id: "session".to_owned(),
            problem_id: problem.map(str::to_owned),
            ..Default::default()
        });
        assert_eq!(record.problem_id, expected);
        assert!(record.created_at >= before && record.created_at <= Utc::now());
        assert!(record.binary_payload.is_none());
    }
}
