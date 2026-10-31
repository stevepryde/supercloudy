//! Shared ordered scheduling only; services own rule schemas, validation and effects.
use std::sync::{Arc, Mutex};

pub(crate) fn validate_ordinals(first: u64, repeat: u64) -> Result<(), &'static str> {
    if first == 0 || repeat == 0 {
        return Err("first_request and repeat must be positive");
    }
    first
        .checked_add(repeat - 1)
        .ok_or("request ordinal range overflows")?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct Schedule<T>(Arc<Mutex<Vec<RuleState<T>>>>);
struct RuleState<T> {
    rule: T,
    first: u64,
    repeat: u64,
    requests: u64,
}
impl<T> Default for Schedule<T> {
    fn default() -> Self {
        Self(Arc::default())
    }
}
impl<T: Clone> Schedule<T> {
    /// Call with validated ordinals only.
    pub(crate) fn new(rules: impl IntoIterator<Item = (u64, u64, T)>) -> Self {
        Self(Arc::new(Mutex::new(
            rules
                .into_iter()
                .map(|(first, repeat, rule)| RuleState {
                    rule,
                    first,
                    repeat,
                    requests: 0,
                })
                .collect(),
        )))
    }
    pub(crate) fn select(&self, eligible: impl Fn(&T) -> bool) -> Option<T> {
        let mut rules = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut selected = None;
        for state in rules.iter_mut() {
            if !eligible(&state.rule) {
                continue;
            }
            state.requests = state.requests.saturating_add(1);
            if selected.is_none()
                && state.requests >= state.first
                && state.requests - state.first < state.repeat
            {
                selected = Some(state.rule.clone());
            }
        }
        selected
    }
}
