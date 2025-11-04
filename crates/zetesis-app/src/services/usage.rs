use std::collections::HashMap;
use std::ops::{Add, AddAssign};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modality {
    Text,
    Image,
    Video,
    Audio,
    Document,
    Other(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub requests: u64,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub input_tokens_by_modality: HashMap<Modality, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub output_tokens_by_modality: HashMap<Modality, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub cache_tokens_by_modality: HashMap<Modality, u64>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub tool_tokens_by_modality: HashMap<Modality, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thoughts_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl Usage {
    pub fn new() -> Self {
        debug_assert!(std::mem::size_of::<Self>() > 0, "usage struct must exist");
        debug_assert!(std::mem::size_of::<Modality>() > 0, "modality must exist");
        Self::default()
    }

    pub fn input_tokens(&self) -> u64 {
        debug_assert!(self.input_tokens_by_modality.len() <= usize::MAX / 2);
        debug_assert!(
            self.input_tokens_by_modality
                .values()
                .all(|value| *value <= u64::MAX)
        );
        self.input_tokens_by_modality.values().sum()
    }

    pub fn output_tokens(&self) -> u64 {
        debug_assert!(self.output_tokens_by_modality.len() <= usize::MAX / 2);
        debug_assert!(
            self.output_tokens_by_modality
                .values()
                .all(|value| *value <= u64::MAX)
        );
        self.output_tokens_by_modality.values().sum()
    }

    pub fn cache_tokens(&self) -> u64 {
        debug_assert!(self.cache_tokens_by_modality.len() <= usize::MAX / 2);
        debug_assert!(
            self.cache_tokens_by_modality
                .values()
                .all(|value| *value <= u64::MAX)
        );
        self.cache_tokens_by_modality.values().sum()
    }

    pub fn tool_tokens(&self) -> u64 {
        debug_assert!(self.tool_tokens_by_modality.len() <= usize::MAX / 2);
        debug_assert!(
            self.tool_tokens_by_modality
                .values()
                .all(|value| *value <= u64::MAX)
        );
        self.tool_tokens_by_modality.values().sum()
    }

    pub fn total_tokens(&self) -> u64 {
        debug_assert!(self.requests <= u64::MAX);
        debug_assert!(self.thoughts_tokens.unwrap_or(0) <= u64::MAX);
        self.input_tokens() + self.output_tokens() + self.thoughts_tokens.unwrap_or(0)
    }

    pub fn effective_input_tokens(&self) -> u64 {
        debug_assert!(self.cache_creation_tokens.unwrap_or(0) <= self.input_tokens());
        debug_assert!(self.cache_creation_tokens.unwrap_or(0) <= u64::MAX);
        self.input_tokens()
            .saturating_sub(self.cache_creation_tokens.unwrap_or(0))
    }

    pub fn total_cache_tokens(&self) -> u64 {
        debug_assert!(self.cache_read_tokens.unwrap_or(0) <= u64::MAX);
        debug_assert!(self.cache_creation_tokens.unwrap_or(0) <= u64::MAX);
        self.cache_read_tokens.unwrap_or(0) + self.cache_creation_tokens.unwrap_or(0)
    }
}

impl Add for Usage {
    type Output = Usage;

    fn add(mut self, other: Usage) -> Usage {
        debug_assert!(self.requests <= u64::MAX - other.requests);
        debug_assert!(self.input_tokens() <= u64::MAX - other.input_tokens());
        self.requests += other.requests;
        merge_maps(
            &mut self.input_tokens_by_modality,
            &other.input_tokens_by_modality,
        );
        merge_maps(
            &mut self.output_tokens_by_modality,
            &other.output_tokens_by_modality,
        );
        merge_maps(
            &mut self.cache_tokens_by_modality,
            &other.cache_tokens_by_modality,
        );
        merge_maps(
            &mut self.tool_tokens_by_modality,
            &other.tool_tokens_by_modality,
        );
        self.cache_read_tokens = add_optional(self.cache_read_tokens, other.cache_read_tokens);
        self.cache_creation_tokens =
            add_optional(self.cache_creation_tokens, other.cache_creation_tokens);
        self.thoughts_tokens = add_optional(self.thoughts_tokens, other.thoughts_tokens);
        self.details = merge_details(self.details, other.details);
        self
    }
}

impl AddAssign for Usage {
    fn add_assign(&mut self, rhs: Self) {
        debug_assert!(self.requests <= u64::MAX - rhs.requests);
        debug_assert!(self.input_tokens() <= u64::MAX - rhs.input_tokens());
        self.requests += rhs.requests;
        merge_maps(
            &mut self.input_tokens_by_modality,
            &rhs.input_tokens_by_modality,
        );
        merge_maps(
            &mut self.output_tokens_by_modality,
            &rhs.output_tokens_by_modality,
        );
        merge_maps(
            &mut self.cache_tokens_by_modality,
            &rhs.cache_tokens_by_modality,
        );
        merge_maps(
            &mut self.tool_tokens_by_modality,
            &rhs.tool_tokens_by_modality,
        );
        self.cache_read_tokens = add_optional(self.cache_read_tokens, rhs.cache_read_tokens);
        self.cache_creation_tokens =
            add_optional(self.cache_creation_tokens, rhs.cache_creation_tokens);
        self.thoughts_tokens = add_optional(self.thoughts_tokens, rhs.thoughts_tokens);
        self.details = merge_details(self.details.take(), rhs.details);
    }
}

fn merge_maps(lhs: &mut HashMap<Modality, u64>, rhs: &HashMap<Modality, u64>) {
    debug_assert!(lhs.len() <= usize::MAX - rhs.len());
    debug_assert!(rhs.values().all(|value| *value <= u64::MAX));
    for (key, value) in rhs {
        *lhs.entry(key.clone()).or_default() += value;
    }
}

fn add_optional(lhs: Option<u64>, rhs: Option<u64>) -> Option<u64> {
    debug_assert!(lhs.unwrap_or(0) <= u64::MAX);
    debug_assert!(rhs.unwrap_or(0) <= u64::MAX);
    match (lhs, rhs) {
        (Some(a), Some(b)) => Some(a + b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn merge_details(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    debug_assert!(lhs.as_ref().map_or(true, Value::is_object));
    match (lhs, rhs) {
        (Some(Value::Object(mut first)), Some(Value::Object(second))) => {
            for (key, value) in second {
                first.insert(key, value);
            }
            Some(Value::Object(first))
        }
        (Some(value), None) => Some(value),
        (None, Some(value)) => Some(value),
        (None, None) => None,
        (_, Some(value)) => Some(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_adds_totals() {
        let mut first = Usage::new();
        first.requests = 1;
        first.input_tokens_by_modality.insert(Modality::Text, 100);
        first.output_tokens_by_modality.insert(Modality::Text, 50);
        let mut second = Usage::new();
        second.requests = 2;
        second.input_tokens_by_modality.insert(Modality::Image, 40);
        second.output_tokens_by_modality.insert(Modality::Image, 10);

        let combined = first + second;

        assert_eq!(combined.requests, 3);
        assert_eq!(combined.input_tokens(), 140);
        assert_eq!(combined.output_tokens(), 60);
    }
}
