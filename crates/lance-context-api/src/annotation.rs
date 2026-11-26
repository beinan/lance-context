//! Current turn annotations and field-level mutations.
//!
//! These types define mutation semantics. A store must serialize application
//! per turn, persist the request identity/result, and make the applied result
//! queryable before acknowledging it. This module does not supply that store.
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{ContextError, ContextResult};

/// One current, narrow annotation. The immutable turn body is stored separately
/// from this value, even when both use different columns of the same dataset.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnAnnotation {
    pub labels: BTreeSet<String>,
    pub status: Option<String>,
    pub score: Option<f32>,
}

/// Explicit field operations avoid confusing an omitted update with clearing a
/// nullable field. JSON examples: `{"op":"set","value":0.9}`, `{"op":"clear"}`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "op",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AnnotationFieldUpdate<T> {
    #[default]
    Keep,
    Set(T),
    Clear,
}

impl<T> AnnotationFieldUpdate<T> {
    fn is_keep(&self) -> bool {
        matches!(self, Self::Keep)
    }
}

/// All operations in one patch apply atomically to the same turn. Labels have
/// set semantics; adding an existing label or removing an absent one is valid.
/// Adding and removing the same label in one request is rejected.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnAnnotationPatch {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub add_labels: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub remove_labels: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "AnnotationFieldUpdate::is_keep")]
    pub status: AnnotationFieldUpdate<String>,
    #[serde(default, skip_serializing_if = "AnnotationFieldUpdate::is_keep")]
    pub score: AnnotationFieldUpdate<f32>,
}

impl TurnAnnotationPatch {
    /// Validate all operations before changing the current value. Byte and
    /// cardinality admission limits belong to the store's bounded write path.
    pub fn validate(&self) -> ContextResult<()> {
        if !self.add_labels.is_disjoint(&self.remove_labels) {
            return Err(ContextError::InvalidRequest(
                "a label cannot be added and removed in the same annotation patch".into(),
            ));
        }
        if self
            .add_labels
            .iter()
            .chain(&self.remove_labels)
            .any(|label| label.is_empty())
        {
            return Err(ContextError::InvalidRequest(
                "annotation labels must not be empty".into(),
            ));
        }
        if let AnnotationFieldUpdate::Set(score) = self.score {
            if !score.is_finite() {
                return Err(ContextError::InvalidRequest(
                    "annotation score must be finite".into(),
                ));
            }
        }
        Ok(())
    }

    /// Apply to the complete current annotation under the store's per-turn
    /// ordering. An error leaves the value unchanged. Only supplied fields are
    /// changed, and no immutable body is accessed or copied.
    ///
    /// Value-level idempotence is NOT retry deduplication: replaying an old
    /// assignment after a newer request would overwrite the newer value. The
    /// store must resolve an exact request retry before calling this method.
    pub fn apply_to(&self, current: &mut TurnAnnotation) -> ContextResult<()> {
        self.validate()?;
        for label in &self.remove_labels {
            current.labels.remove(label);
        }
        current.labels.extend(self.add_labels.iter().cloned());
        match &self.status {
            AnnotationFieldUpdate::Keep => {}
            AnnotationFieldUpdate::Set(value) => current.status = Some(value.clone()),
            AnnotationFieldUpdate::Clear => current.status = None,
        }
        match self.score {
            AnnotationFieldUpdate::Keep => {}
            AnnotationFieldUpdate::Set(value) => current.score = Some(value),
            AnnotationFieldUpdate::Clear => current.score = None,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn patch(value: serde_json::Value) -> TurnAnnotationPatch {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn independent_fields_merge_and_explicit_clear_preserves_other_fields() {
        let mut current = TurnAnnotation::default();
        patch(json!({"status":{"op":"set","value":"reviewed"},"add_labels":["good"]}))
            .apply_to(&mut current)
            .unwrap();
        patch(json!({"score":{"op":"set","value":0.9}}))
            .apply_to(&mut current)
            .unwrap();
        assert_eq!(current.status.as_deref(), Some("reviewed"));
        assert_eq!(current.score, Some(0.9));
        assert_eq!(current.labels, BTreeSet::from(["good".into()]));
        patch(json!({"status":{"op":"clear"},"score":{"op":"clear"}}))
            .apply_to(&mut current)
            .unwrap();
        assert_eq!(current.status, None);
        assert_eq!(current.score, None);
        assert_eq!(current.labels, BTreeSet::from(["good".into()]));
    }

    #[test]
    fn empty_status_zero_score_and_missing_operations_remain_distinct() {
        let mut current = TurnAnnotation::default();
        patch(json!({"status":{"op":"set","value":""},"score":{"op":"set","value":0}}))
            .apply_to(&mut current)
            .unwrap();
        patch(json!({})).apply_to(&mut current).unwrap();
        assert_eq!(current.status.as_deref(), Some(""));
        assert_eq!(current.score, Some(0.0));
        assert_eq!(
            serde_json::to_value(TurnAnnotationPatch::default()).unwrap(),
            json!({})
        );
        for invalid in [
            json!({"score":null}),
            json!({"score":{"op":"set","value":null}}),
            json!({"score":{"op":"clear","value":1}}),
            json!({"status":{"op":"set"}}),
            json!({"unexpected":1}),
        ] {
            assert!(serde_json::from_value::<TurnAnnotationPatch>(invalid).is_err());
        }
    }

    #[test]
    fn labels_are_sets_and_ambiguous_operations_do_not_partially_apply() {
        let mut current = TurnAnnotation::default();
        let add = patch(json!({"add_labels":["b","a","a"],"remove_labels":["absent"]}));
        add.apply_to(&mut current).unwrap();
        add.apply_to(&mut current).unwrap();
        assert_eq!(current.labels, BTreeSet::from(["a".into(), "b".into()]));
        let before = current.clone();
        for invalid in [
            json!({"add_labels":["a"],"remove_labels":["a"],"status":{"op":"set","value":"bad"}}),
            json!({"add_labels":[""],"remove_labels":["b"]}),
        ] {
            assert!(patch(invalid).apply_to(&mut current).is_err());
            assert_eq!(current, before);
        }
        patch(json!({"remove_labels":["a"]}))
            .apply_to(&mut current)
            .unwrap();
        assert_eq!(current.labels, BTreeSet::from(["b".into()]));
    }

    #[test]
    fn invalid_numeric_values_leave_the_whole_annotation_unchanged() {
        let mut current = TurnAnnotation::default();
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mutation = TurnAnnotationPatch {
                add_labels: BTreeSet::from(["must-not-appear".into()]),
                score: AnnotationFieldUpdate::Set(value),
                ..Default::default()
            };
            assert!(mutation.apply_to(&mut current).is_err());
            assert_eq!(current, TurnAnnotation::default());
        }
    }

    #[test]
    fn serialization_preserves_every_operation_and_later_field_assignment_wins() {
        let original = patch(json!({"add_labels":["a"],"remove_labels":["b"],
            "status":{"op":"clear"},"score":{"op":"set","value":-1.5}}));
        let decoded: TurnAnnotationPatch =
            serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
        assert_eq!(original, decoded);
        let mut current = TurnAnnotation::default();
        original.apply_to(&mut current).unwrap();
        patch(json!({"score":{"op":"set","value":0.5}}))
            .apply_to(&mut current)
            .unwrap();
        assert_eq!(current.score, Some(0.5));
        assert_eq!(current.labels, BTreeSet::from(["a".into()]));
    }
}
