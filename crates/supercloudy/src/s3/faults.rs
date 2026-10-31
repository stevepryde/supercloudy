use crate::fault_config::{FaultEffect, FaultRule, Faults};

pub(crate) fn validate_rule(rule: &FaultRule) -> Result<(), &'static str> {
    if rule.table.is_some()
        || !matches!(
            rule.effect,
            FaultEffect::Throttle | FaultEffect::InternalServerError
        )
    {
        return Err("S3 rules support bucket/key selectors and throttle/internal errors only");
    }
    if !matches!(
        rule.operation.as_str(),
        "CreateBucket"
            | "PutObject"
            | "GetObject"
            | "HeadObject"
            | "CopyObject"
            | "DeleteObject"
            | "ListObjectsV2"
    ) {
        return Err("unsupported S3 operation");
    }
    if rule.bucket.as_ref().is_some_and(|v| v.is_empty())
        || rule.key.as_ref().is_some_and(|v| v.is_empty())
    {
        return Err("bucket and key selectors must not be empty");
    }
    Ok(())
}

impl Faults {
    pub(crate) fn s3_effect(
        &self,
        operation: &str,
        bucket: &str,
        key: Option<&str>,
    ) -> Option<&'static str> {
        self.select("s3", operation, |rule| {
            rule.bucket.as_deref().is_none_or(|b| b == bucket)
                && rule.key.as_deref().is_none_or(|k| Some(k) == key)
        })
        .map(|effect| match effect {
            FaultEffect::Throttle => "SlowDown",
            _ => "InternalError",
        })
    }
}
