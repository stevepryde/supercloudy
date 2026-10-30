use super::{
    error::{Error, Result},
    value::{Item, Value, number, validate_number},
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Segment {
    Name(String),
    Index(usize),
}
pub(crate) type Path = Vec<Segment>;
#[derive(Clone, Debug)]
pub(crate) enum Operand {
    Path(Path),
    Literal(Value),
    Function(String, Vec<Operand>),
    Arithmetic(Box<Operand>, bool, Box<Operand>),
}
#[derive(Clone, Debug)]
pub(crate) enum Condition {
    Compare(Operand, String, Operand),
    Between(Operand, Operand, Operand),
    In(Operand, Vec<Operand>),
    Function(String, Vec<Operand>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}
#[derive(Clone, Debug)]
pub(crate) struct Action {
    pub kind: String,
    pub path: Path,
    pub value: Option<Operand>,
}

pub(crate) struct Parser {
    tokens: Vec<String>,
    pos: usize,
    names: BTreeMap<String, String>,
    values: Item,
    used_names: BTreeSet<String>,
    used_values: BTreeSet<String>,
}
fn invalid() -> Error {
    Error::validation("Invalid or unsupported expression")
}
fn validate_ordered_literals(operands: &[&Operand]) -> Result<()> {
    if operands.iter().any(|operand| {
        matches!(operand, Operand::Literal(value) if !matches!(value, Value::S(_) | Value::N(_) | Value::B(_)))
    }) {
        return Err(Error::validation("Ordering requires string, number, or binary operands"));
    }
    Ok(())
}
fn tokenize(s: &str) -> Result<Vec<String>> {
    if s.is_empty() || s.len() > 4096 {
        return Err(invalid());
    }
    let mut chars = s.chars().peekable();
    let mut out = Vec::new();
    let mut depth = 0usize;
    while let Some(c) = chars.next() {
        if c.is_ascii_whitespace() {
            continue;
        }
        let mut t = c.to_string();
        if c.is_ascii_alphanumeric() || c == '_' || c == '#' || c == ':' {
            while chars
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
            {
                t.push(chars.next().unwrap());
            }
        } else if ['<', '>'].contains(&c)
            && chars
                .peek()
                .is_some_and(|n| *n == '=' || (c == '<' && *n == '>'))
        {
            t.push(chars.next().unwrap());
        } else if !"=<>(),.[]+-".contains(c) {
            return Err(invalid());
        }
        if t == "(" {
            depth += 1;
            if depth > 32 {
                return Err(invalid());
            }
        }
        if t == ")" {
            depth = depth.checked_sub(1).ok_or_else(invalid)?;
        }
        out.push(t);
    }
    if depth != 0 {
        return Err(invalid());
    }
    Ok(out)
}
impl Parser {
    pub fn new(names: BTreeMap<String, String>, values: Item) -> Result<Self> {
        if names
            .iter()
            .any(|(k, v)| !valid_placeholder(k, '#') || v.is_empty())
            || values.keys().any(|k| !valid_placeholder(k, ':'))
        {
            return Err(invalid());
        }
        Ok(Self {
            tokens: vec![],
            pos: 0,
            names,
            values,
            used_names: BTreeSet::new(),
            used_values: BTreeSet::new(),
        })
    }
    fn start(&mut self, s: &str) -> Result<()> {
        self.tokens = tokenize(s)?;
        self.pos = 0;
        Ok(())
    }
    fn peek(&self) -> &str {
        self.tokens.get(self.pos).map(String::as_str).unwrap_or("")
    }
    fn take(&mut self) -> Result<String> {
        let t = self.tokens.get(self.pos).cloned().ok_or_else(invalid)?;
        self.pos += 1;
        Ok(t)
    }
    fn eat(&mut self, s: &str) -> bool {
        if self.peek().eq_ignore_ascii_case(s) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, s: &str) -> Result<()> {
        if self.eat(s) { Ok(()) } else { Err(invalid()) }
    }
    fn end(&self) -> Result<()> {
        if self.pos == self.tokens.len() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
    pub fn finish(&self) -> Result<()> {
        if self.names.len() != self.used_names.len() || self.values.len() != self.used_values.len()
        {
            return Err(Error::validation(
                "Unused expression attribute names or values",
            ));
        }
        Ok(())
    }
    fn name(&mut self) -> Result<String> {
        let t = self.take()?;
        if t.starts_with('#') {
            self.used_names.insert(t.clone());
            self.names.get(&t).cloned().ok_or_else(invalid)
        } else if t.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
            && t.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            && !include_str!("reserved.txt")
                .split_ascii_whitespace()
                .any(|s| s.eq_ignore_ascii_case(&t))
        {
            Ok(t)
        } else {
            Err(Error::validation(
                "Invalid or reserved attribute name; use an alias",
            ))
        }
    }
    fn path(&mut self) -> Result<Path> {
        let mut path = vec![Segment::Name(self.name()?)];
        loop {
            if self.eat(".") {
                path.push(Segment::Name(self.name()?));
            } else if self.eat("[") {
                let n = self.take()?.parse().map_err(|_| invalid())?;
                self.expect("]")?;
                path.push(Segment::Index(n));
            } else {
                break;
            }
            if path.len() > 33 {
                return Err(invalid());
            }
        }
        Ok(path)
    }
    fn operand(&mut self, update: bool) -> Result<Operand> {
        if self.peek().starts_with(':') {
            let t = self.take()?;
            self.used_values.insert(t.clone());
            return Ok(Operand::Literal(
                self.values.get(&t).cloned().ok_or_else(invalid)?,
            ));
        }
        if self.tokens.get(self.pos + 1).is_some_and(|t| t == "(") {
            let name = self.take()?;
            if !(if update {
                ["if_not_exists", "list_append"].contains(&name.as_str())
            } else {
                name == "size"
            }) {
                return Err(invalid());
            }
            self.expect("(")?;
            let a = self.operand(update)?;
            let args = if name == "size" {
                vec![a]
            } else {
                self.expect(",")?;
                vec![a, self.operand(update)?]
            };
            self.expect(")")?;
            if args
                .iter()
                .any(|arg| matches!(arg, Operand::Function(f, _) if f == "list_append"))
            {
                return Err(Error::validation(
                    "list_append cannot be nested inside another function",
                ));
            }
            if (name == "size" || name == "if_not_exists") && !matches!(args[0], Operand::Path(_)) {
                return Err(invalid());
            }
            return Ok(Operand::Function(name, args));
        }
        Ok(Operand::Path(self.path()?))
    }
    pub fn condition(&mut self, s: &str) -> Result<Condition> {
        self.start(s)?;
        let c = self.or()?;
        self.end()?;
        Ok(c)
    }
    fn or(&mut self) -> Result<Condition> {
        let mut c = self.and()?;
        while self.eat("OR") {
            c = Condition::Or(Box::new(c), Box::new(self.and()?));
        }
        Ok(c)
    }
    fn and(&mut self) -> Result<Condition> {
        let mut c = self.unary(0)?;
        while self.eat("AND") {
            c = Condition::And(Box::new(c), Box::new(self.unary(0)?));
        }
        Ok(c)
    }
    fn unary(&mut self, depth: usize) -> Result<Condition> {
        if depth > 32 {
            return Err(invalid());
        }
        if self.eat("NOT") {
            return Ok(Condition::Not(Box::new(self.unary(depth + 1)?)));
        }
        if self.eat("(") {
            let c = self.or()?;
            self.expect(")")?;
            return Ok(c);
        }
        if [
            "attribute_exists",
            "attribute_not_exists",
            "attribute_type",
            "begins_with",
            "contains",
        ]
        .contains(&self.peek())
        {
            let f = self.take()?;
            self.expect("(")?;
            let mut args = vec![Operand::Path(self.path()?)];
            if f != "attribute_exists" && f != "attribute_not_exists" {
                self.expect(",")?;
                args.push(self.operand(false)?);
            }
            self.expect(")")?;
            if f == "begins_with"
                && matches!(&args[1], Operand::Literal(v) if !matches!(v, Value::S(_) | Value::B(_)))
            {
                return Err(Error::validation(
                    "begins_with requires a string or binary prefix",
                ));
            }
            if f == "attribute_type"
                && !matches!(&args[1],Operand::Literal(Value::S(s)) if ["S","N","B","BOOL","NULL","SS","NS","BS","L","M"].contains(&s.as_str()))
            {
                return Err(invalid());
            }
            if f == "contains"
                && matches!((&args[0], &args[1]), (Operand::Path(a), Operand::Path(b)) if a == b)
            {
                return Err(Error::validation("contains operands must be distinct"));
            }
            return Ok(Condition::Function(f, args));
        }
        let a = self.operand(false)?;
        if self.eat("BETWEEN") {
            let b = self.operand(false)?;
            self.expect("AND")?;
            let c = self.operand(false)?;
            validate_ordered_literals(&[&a, &b, &c])?;
            if let (Operand::Literal(low), Operand::Literal(high)) = (&b, &c)
                && (low.tag() != high.tag() || low.compare(high) == Some(Ordering::Greater))
            {
                return Err(Error::validation("Invalid BETWEEN bounds"));
            }
            return Ok(Condition::Between(a, b, c));
        }
        if self.eat("IN") {
            self.expect("(")?;
            let mut list = vec![self.operand(false)?];
            while self.eat(",") {
                list.push(self.operand(false)?);
                if list.len() > 100 {
                    return Err(invalid());
                }
            }
            self.expect(")")?;
            return Ok(Condition::In(a, list));
        }
        let op = self.take()?;
        if !["=", "<>", "<", "<=", ">", ">="].contains(&op.as_str()) {
            return Err(invalid());
        }
        let b = self.operand(false)?;
        if op != "=" && op != "<>" {
            validate_ordered_literals(&[&a, &b])?;
        }
        Ok(Condition::Compare(a, op, b))
    }
    pub fn updates(&mut self, s: &str) -> Result<Vec<Action>> {
        self.start(s)?;
        let mut actions = vec![];
        let mut clauses = BTreeSet::new();
        while !self.peek().is_empty() {
            let kind = self.take()?.to_uppercase();
            if !["SET", "REMOVE", "ADD", "DELETE"].contains(&kind.as_str())
                || !clauses.insert(kind.clone())
            {
                return Err(invalid());
            }
            loop {
                let path = self.path()?;
                if (kind == "ADD" || kind == "DELETE") && path.len() != 1 {
                    return Err(invalid());
                }
                let value = if kind == "REMOVE" {
                    None
                } else if kind == "SET" {
                    self.expect("=")?;
                    let mut o = self.operand(true)?;
                    if self.peek() == "+" || self.peek() == "-" {
                        let plus = self.take()? == "+";
                        o = Operand::Arithmetic(Box::new(o), plus, Box::new(self.operand(true)?));
                    }
                    Some(o)
                } else {
                    let o = self.operand(false)?;
                    if !matches!(o, Operand::Literal(_)) {
                        return Err(invalid());
                    }
                    Some(o)
                };
                if actions
                    .iter()
                    .any(|a: &Action| a.path.starts_with(&path) || path.starts_with(&a.path))
                {
                    return Err(Error::validation("Overlapping update paths"));
                }
                actions.push(Action {
                    kind: kind.clone(),
                    path,
                    value,
                });
                if !self.eat(",") {
                    break;
                }
            }
        }
        if actions.is_empty() {
            return Err(invalid());
        }
        Ok(actions)
    }
}
fn valid_placeholder(s: &str, prefix: char) -> bool {
    s.starts_with(prefix)
        && s.len() > 1
        && s[1..]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
pub(crate) fn get_path<'a>(item: &'a Item, path: &[Segment]) -> Option<&'a Value> {
    let (Segment::Name(name), tail) = path.split_first()? else {
        return None;
    };
    let mut v = item.get(name)?;
    for p in tail {
        v = match (v, p) {
            (Value::M(m), Segment::Name(n)) => m.get(n)?,
            (Value::L(l), Segment::Index(i)) => l.get(*i)?,
            _ => return None,
        };
    }
    Some(v)
}
impl Operand {
    pub fn eval(&self, item: &Item) -> Result<Option<Value>> {
        Ok(match self {
            Self::Path(p) => get_path(item, p).cloned(),
            Self::Literal(v) => Some(v.clone()),
            Self::Function(f, args) => {
                let a = args[0].eval(item)?;
                match f.as_str() {
                    "size" => match a {
                        Some(v) if !matches!(v, Value::N(_) | Value::Bool(_) | Value::Null) => {
                            Some(Value::N(number(&v.len().to_string())?))
                        }
                        _ => None,
                    },
                    "if_not_exists" => {
                        let b = args[1].eval(item)?;
                        a.or(b)
                    }
                    "list_append" => match (a, args[1].eval(item)?) {
                        (Some(Value::L(mut a)), Some(Value::L(b))) => {
                            a.extend(b);
                            Some(Value::L(a))
                        }
                        _ => return Err(invalid()),
                    },
                    _ => return Err(invalid()),
                }
            }
            Self::Arithmetic(a, plus, b) => match (a.eval(item)?, b.eval(item)?) {
                (Some(Value::N(a)), Some(Value::N(b))) => {
                    let n = if *plus { a + b } else { a - b };
                    validate_number(&n)?;
                    Some(Value::N(n))
                }
                _ => return Err(invalid()),
            },
        })
    }
}
impl Condition {
    pub fn eval(&self, item: &Item) -> Result<bool> {
        Ok(match self {
            Self::And(a, b) => a.eval(item)? & b.eval(item)?,
            Self::Or(a, b) => a.eval(item)? | b.eval(item)?,
            Self::Not(a) => !a.eval(item)?,
            Self::Compare(a, op, b) => match (a.eval(item)?, b.eval(item)?) {
                (Some(a), Some(b)) => match op.as_str() {
                    "=" => a == b,
                    "<>" => a != b,
                    "<" => a.compare(&b) == Some(Ordering::Less),
                    "<=" => a.compare(&b).is_some_and(|o| o != Ordering::Greater),
                    ">" => a.compare(&b) == Some(Ordering::Greater),
                    ">=" => a.compare(&b).is_some_and(|o| o != Ordering::Less),
                    _ => false,
                },
                (None, Some(_)) | (Some(_), None) => op == "<>",
                _ => false,
            },
            Self::Between(a, b, c) => {
                let (a, b, c) = (a.eval(item)?, b.eval(item)?, c.eval(item)?);
                if let (Some(low), Some(high)) = (&b, &c)
                    && low.compare(high) == Some(Ordering::Greater)
                {
                    return Err(Error::validation("Invalid BETWEEN bounds"));
                }
                match (a, b, c) {
                    (Some(a), Some(b), Some(c)) => {
                        a.compare(&b).is_some_and(|o| o != Ordering::Less)
                            && a.compare(&c).is_some_and(|o| o != Ordering::Greater)
                    }
                    _ => false,
                }
            }
            Self::In(a, list) => {
                let a = a.eval(item)?;
                let values = list
                    .iter()
                    .map(|b| b.eval(item))
                    .collect::<Result<Vec<_>>>()?;
                a.is_some() && values.contains(&a)
            }
            Self::Function(f, args) => {
                let a = args[0].eval(item)?;
                let b = if args.len() > 1 {
                    args[1].eval(item)?
                } else {
                    None
                };
                match f.as_str() {
                    "attribute_exists" => a.is_some(),
                    "attribute_not_exists" => a.is_none(),
                    "attribute_type" => matches!((a,b),(Some(a),Some(Value::S(b))) if a.tag()==b),
                    "begins_with" => match (a, b) {
                        (Some(a), Some(b)) => a.begins(&b),
                        _ => false,
                    },
                    "contains" => match (a, b) {
                        (Some(a), Some(b)) => a.contains(&b),
                        _ => false,
                    },
                    _ => return Err(invalid()),
                }
            }
        })
    }
}

pub(crate) fn set_path(item: &mut Item, path: &[Segment], value: Option<Value>) -> Result<()> {
    let Some(Segment::Name(name)) = path.first() else {
        return Err(invalid());
    };
    if path.len() == 1 {
        if let Some(v) = value {
            item.insert(name.clone(), v);
        } else {
            item.remove(name);
        }
        return Ok(());
    }
    let mut current = item
        .get_mut(name)
        .ok_or_else(|| Error::validation("Invalid document path"))?;
    for segment in &path[1..path.len() - 1] {
        current = match (current, segment) {
            (Value::M(m), Segment::Name(n)) => m.get_mut(n),
            (Value::L(l), Segment::Index(i)) => l.get_mut(*i),
            _ => None,
        }
        .ok_or_else(|| Error::validation("Invalid document path"))?;
    }
    match (current, path.last().unwrap()) {
        (Value::M(m), Segment::Name(n)) => {
            if let Some(v) = value {
                m.insert(n.clone(), v);
            } else {
                m.remove(n);
            }
        }
        (Value::L(l), Segment::Index(i)) => {
            if let Some(v) = value {
                if *i >= l.len() {
                    l.push(v);
                } else {
                    l[*i] = v;
                }
            } else if *i < l.len() {
                l.remove(*i);
            }
        }
        _ => return Err(Error::validation("Invalid document path")),
    }
    Ok(())
}

pub(crate) fn apply_updates(old: &Item, actions: &[Action]) -> Result<Item> {
    let mut candidate = old.clone();
    let mut resolved = vec![];
    for a in actions {
        if a.path.len() > 1 {
            let parent = get_path(old, &a.path[..a.path.len() - 1]);
            if !matches!(
                (parent, a.path.last()),
                (Some(Value::M(_)), Some(Segment::Name(_)))
                    | (Some(Value::L(_)), Some(Segment::Index(_)))
            ) {
                return Err(Error::validation("Invalid document path"));
            }
        }
        let value = match a.kind.as_str() {
            "SET" => Some(
                a.value
                    .as_ref()
                    .unwrap()
                    .eval(old)?
                    .ok_or_else(|| Error::validation("Update operand does not exist"))?,
            ),
            "REMOVE" => None,
            kind => {
                let right = a.value.as_ref().unwrap().eval(old)?.ok_or_else(invalid)?;
                let left = get_path(old, &a.path);
                match (kind, left, right) {
                    ("ADD", None, Value::N(n)) => Some(Value::N(n)),
                    ("ADD", Some(Value::N(a)), Value::N(b)) => {
                        let n = a + b;
                        validate_number(&n)?;
                        Some(Value::N(n))
                    }
                    ("ADD", None, v @ (Value::Ss(_) | Value::Ns(_) | Value::Bs(_))) => Some(v),
                    ("DELETE", None, Value::Ss(_) | Value::Ns(_) | Value::Bs(_)) => None,
                    (kind, Some(Value::Ss(a)), Value::Ss(b)) => {
                        let v: BTreeSet<_> = if kind == "ADD" {
                            a.union(&b).cloned().collect()
                        } else {
                            a.difference(&b).cloned().collect()
                        };
                        if v.is_empty() {
                            None
                        } else {
                            Some(Value::Ss(v))
                        }
                    }
                    (kind, Some(Value::Ns(a)), Value::Ns(b)) => {
                        let v: BTreeSet<_> = if kind == "ADD" {
                            a.union(&b).cloned().collect()
                        } else {
                            a.difference(&b).cloned().collect()
                        };
                        if v.is_empty() {
                            None
                        } else {
                            Some(Value::Ns(v))
                        }
                    }
                    (kind, Some(Value::Bs(a)), Value::Bs(b)) => {
                        let v: BTreeSet<_> = if kind == "ADD" {
                            a.union(&b).cloned().collect()
                        } else {
                            a.difference(&b).cloned().collect()
                        };
                        if v.is_empty() {
                            None
                        } else {
                            Some(Value::Bs(v))
                        }
                    }
                    _ => return Err(Error::validation("Invalid ADD or DELETE operands")),
                }
            }
        };
        // An absent original list index must not remove a newly appended value.
        if a.kind != "REMOVE" || get_path(old, &a.path).is_some() {
            resolved.push((a, value));
        }
    }
    // Resolve descendants before removing lower sibling indices, so every
    // document path addresses the original list. Append SETs in index order.
    resolved.sort_by(
        |(a, _), (b, _)| match (a.kind == "REMOVE", b.kind == "REMOVE") {
            (true, true) => b.path.cmp(&a.path),
            (false, false) => a.path.cmp(&b.path),
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
        },
    );
    for (a, v) in resolved {
        set_path(&mut candidate, &a.path, v)?;
    }
    for v in candidate.values() {
        v.validate_depth(0)?;
    }
    Ok(candidate)
}
