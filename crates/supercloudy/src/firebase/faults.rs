//! Issuer-local rejection variants; these do not change shared AWS fault rules.
use super::Claims;
use jsonwebtoken::Header;
use serde::Deserialize;

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Variant {
    Expired,
    BadKid,
    WrongAud,
}

pub(super) fn validate(
    variant: Option<Variant>,
    kid: Option<&str>,
    time_overrides: bool,
) -> Result<(), &'static str> {
    if kid.is_some_and(|v| v.is_empty() || v.len() > 128) {
        return Err("kid must contain 1 to 128 bytes");
    }
    if variant == Some(Variant::BadKid) && kid.is_some() {
        return Err("bad_kid cannot be combined with kid");
    }
    if variant == Some(Variant::Expired) && time_overrides {
        return Err("expired cannot be combined with ttl, iat or exp");
    }
    Ok(())
}

impl Variant {
    pub(super) fn apply(self, claims: &mut Claims, header: &mut Header, now: u64) {
        match self {
            Self::Expired => {
                claims.iat = now.saturating_sub(7200);
                claims.auth_time = claims.iat;
                claims.exp = now.saturating_sub(3600);
            }
            Self::BadKid => header.kid = Some("synthetic-unknown-key".into()),
            Self::WrongAud => claims.aud.push_str("-wrong-audience"),
        }
    }
}
