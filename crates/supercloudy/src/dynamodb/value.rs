use super::error::{Error, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use bigdecimal::{BigDecimal, Signed, Zero};
use serde_json::{Value as Json, json};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

pub(crate) type Item = BTreeMap<String, Value>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    S(String),
    N(BigDecimal),
    B(Vec<u8>),
    Bool(bool),
    Null,
    Ss(BTreeSet<String>),
    Ns(BTreeSet<BigDecimal>),
    Bs(BTreeSet<Vec<u8>>),
    L(Vec<Value>),
    M(Item),
}

pub(crate) fn number(s: &str) -> Result<BigDecimal> {
    // Bound the exponent before normalization so malformed input cannot request
    // enormous allocations. DynamoDB allows 38 digits and magnitude < 1E126.
    let parts: Vec<_> = s.split(['e', 'E']).collect();
    if parts.len() > 2 || parts.first().is_none_or(|v| v.is_empty()) {
        return Err(Error::validation("Invalid number"));
    }
    if parts.len() == 2
        && parts[1]
            .parse::<i32>()
            .ok()
            .is_none_or(|n| !(-1000..=1000).contains(&n))
    {
        return Err(Error::validation("Number exponent out of range"));
    }
    let mantissa = parts[0].strip_prefix(['-', '+']).unwrap_or(parts[0]);
    if mantissa.matches('.').count() > 1
        || !mantissa.chars().all(|c| c.is_ascii_digit() || c == '.')
        || !mantissa.chars().any(|c| c.is_ascii_digit())
    {
        return Err(Error::validation("Invalid number"));
    }
    let n = BigDecimal::from_str(s)
        .map_err(|_| Error::validation("Invalid number"))?
        .normalized();
    validate_number(&n)?;
    Ok(n)
}
pub(crate) fn validate_number(n: &BigDecimal) -> Result<()> {
    if n.is_zero() {
        return Ok(());
    }
    let n = n.normalized();
    let (digits, scale) = n.as_bigint_and_exponent();
    let precision = digits.to_string().trim_start_matches('-').len() as i64;
    let magnitude = precision - scale - 1;
    if precision > 38 || !(-130..=125).contains(&magnitude) {
        return Err(Error::validation(
            "Number exceeds DynamoDB precision or range",
        ));
    }
    Ok(())
}
impl Value {
    pub fn size_bytes(&self) -> usize {
        match self {
            Self::S(v) => v.len(),
            Self::N(v) => number_size_bytes(v),
            Self::B(v) => v.len(),
            Self::Bool(_) | Self::Null => 1,
            Self::Ss(v) => v.iter().map(String::len).sum(),
            Self::Ns(v) => v.iter().map(number_size_bytes).sum(),
            Self::Bs(v) => v.iter().map(Vec::len).sum(),
            Self::L(v) => 3 + v.iter().map(|v| 1 + v.size_bytes()).sum::<usize>(),
            Self::M(v) => 3 + v.len() + item_size_bytes(v),
        }
    }
    fn parse_at(v: &Json, depth: usize) -> Result<Self> {
        if depth > 32 {
            return Err(Error::validation("Document depth exceeds 32"));
        }
        let o = v
            .as_object()
            .filter(|o| o.len() == 1)
            .ok_or_else(|| Error::validation("AttributeValue must have exactly one type"))?;
        let (tag, v) = o.iter().next().unwrap();
        let string = || {
            v.as_str()
                .ok_or_else(|| Error::validation("Expected string"))
        };
        let array = || {
            v.as_array()
                .ok_or_else(|| Error::validation("Expected array"))
        };
        let binary = |s: &str| {
            STANDARD
                .decode(s)
                .map_err(|_| Error::validation("Invalid base64"))
        };
        Ok(match tag.as_str() {
            "S" => Self::S(string()?.into()),
            "N" => Self::N(number(string()?)?),
            "B" => Self::B(binary(string()?)?),
            "BOOL" => Self::Bool(
                v.as_bool()
                    .ok_or_else(|| Error::validation("Expected boolean"))?,
            ),
            "NULL" if v.as_bool() == Some(true) => Self::Null,
            "SS" | "NS" | "BS" => {
                let a = array()?;
                if a.is_empty() {
                    return Err(Error::validation("Sets cannot be empty"));
                }
                let strings = a
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| Error::validation("Expected set string"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let result = match tag.as_str() {
                    "SS" => Self::Ss(strings.iter().map(|s| s.to_string()).collect()),
                    "NS" => Self::Ns(strings.iter().map(|s| number(s)).collect::<Result<_>>()?),
                    _ => Self::Bs(strings.iter().map(|s| binary(s)).collect::<Result<_>>()?),
                };
                if result.len() != a.len() {
                    return Err(Error::validation("Duplicate set member"));
                }
                result
            }
            "L" => Self::L(
                array()?
                    .iter()
                    .map(|v| Self::parse_at(v, depth + 1))
                    .collect::<Result<_>>()?,
            ),
            "M" => Self::M(parse_item_at(v, depth + 1)?),
            _ => return Err(Error::validation("Invalid AttributeValue type")),
        })
    }
    pub fn wire(&self) -> Json {
        match self {
            Self::S(v) => json!({"S":v}),
            Self::N(v) => json!({"N":v.normalized().to_plain_string()}),
            Self::B(v) => json!({"B":STANDARD.encode(v)}),
            Self::Bool(v) => json!({"BOOL":v}),
            Self::Null => json!({"NULL":true}),
            Self::Ss(v) => json!({"SS":v}),
            Self::Ns(v) => {
                json!({"NS":v.iter().map(|n|n.normalized().to_plain_string()).collect::<Vec<_>>()})
            }
            Self::Bs(v) => json!({"BS":v.iter().map(|b|STANDARD.encode(b)).collect::<Vec<_>>()}),
            Self::L(v) => json!({"L":v.iter().map(Self::wire).collect::<Vec<_>>()}),
            Self::M(v) => json!({"M":wire_item(v)}),
        }
    }
    pub fn tag(&self) -> &'static str {
        match self {
            Self::S(_) => "S",
            Self::N(_) => "N",
            Self::B(_) => "B",
            Self::Bool(_) => "BOOL",
            Self::Null => "NULL",
            Self::Ss(_) => "SS",
            Self::Ns(_) => "NS",
            Self::Bs(_) => "BS",
            Self::L(_) => "L",
            Self::M(_) => "M",
        }
    }
    pub fn len(&self) -> usize {
        match self {
            // DynamoDB expression size counts UTF-16 code units for strings.
            // Key limits still use UTF-8 byte length in Scalar::from_value.
            Self::S(v) => v.encode_utf16().count(),
            Self::B(v) => v.len(),
            Self::Ss(v) => v.len(),
            Self::Ns(v) => v.len(),
            Self::Bs(v) => v.len(),
            Self::L(v) => v.len(),
            Self::M(v) => v.len(),
            _ => 0,
        }
    }
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::S(a), Self::S(b)) => Some(a.cmp(b)),
            (Self::N(a), Self::N(b)) => Some(a.cmp(b)),
            (Self::B(a), Self::B(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
    pub fn begins(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::S(a), Self::S(b)) => a.starts_with(b),
            (Self::B(a), Self::B(b)) => a.starts_with(b),
            _ => false,
        }
    }
    pub fn contains(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::S(a), Self::S(b)) => a.contains(b),
            (Self::Ss(a), Self::S(b)) => a.contains(b),
            (Self::Ns(a), Self::N(b)) => a.contains(b),
            (Self::Bs(a), Self::B(b)) => a.contains(b),
            (Self::L(a), b) => a.contains(b),
            _ => false,
        }
    }
    pub fn validate_depth(&self, depth: usize) -> Result<()> {
        if depth > 32 {
            return Err(Error::validation("Document depth exceeds 32"));
        }
        match self {
            Self::L(values) => values.iter().try_for_each(|v| v.validate_depth(depth + 1)),
            Self::M(values) => values
                .values()
                .try_for_each(|v| v.validate_depth(depth + 1)),
            _ => Ok(()),
        }
    }
}
fn number_size_bytes(n: &BigDecimal) -> usize {
    n.normalized().digits().div_ceil(2) as usize + 1 + usize::from(n.is_negative())
}
pub(crate) fn item_size_bytes(item: &Item) -> usize {
    item.iter().map(|(k, v)| k.len() + v.size_bytes()).sum()
}
fn parse_item_at(v: &Json, depth: usize) -> Result<Item> {
    v.as_object()
        .ok_or_else(|| Error::validation("Expected attribute map"))?
        .iter()
        .map(|(k, v)| {
            if k.is_empty() {
                return Err(Error::validation("Attribute names cannot be empty"));
            }
            Ok((k.clone(), Value::parse_at(v, depth)?))
        })
        .collect()
}
pub(crate) fn parse_item(v: &Json) -> Result<Item> {
    parse_item_at(v, 0)
}
pub(crate) fn wire_item(v: &Item) -> Json {
    Json::Object(v.iter().map(|(k, v)| (k.clone(), v.wire())).collect())
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Scalar {
    S(String),
    N(BigDecimal),
    B(Vec<u8>),
}
impl Scalar {
    pub fn from_value(v: &Value, tag: &str, max: usize) -> Result<Self> {
        if v.tag() != tag {
            return Err(Error::validation("Key type does not match schema"));
        }
        match v {
            Value::S(s) if !s.is_empty() && s.len() <= max => Ok(Self::S(s.clone())),
            Value::B(b) if !b.is_empty() && b.len() <= max => Ok(Self::B(b.clone())),
            Value::N(n) => Ok(Self::N(n.clone())),
            _ => Err(Error::validation("Invalid key value")),
        }
    }
}
