use super::*;
use crate::dynamodb::{Service, operations, value::parse_item};
use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngSeed},
};
use serde_json::json;

fn config() -> Config {
    Config {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(96),
        rng_seed: std::env::var("PROPTEST_RNG_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(RngSeed::Fixed(0x208e_7011)),
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/dynamodb-expressions.txt",
        ))),
        ..Config::default()
    }
}

fn parser() -> Parser {
    Parser::new(
        BTreeMap::from([("#n".into(), "literal.dot".into())]),
        Item::new(),
    )
    .unwrap()
}

// A small integer/boolean model supplies the oracle, independently of the
// expression AST and decimal evaluator used by production.
#[derive(Clone, Debug)]
enum Predicate {
    Compare(u8, i32),
    Between(i32, i32),
    In(Vec<i32>),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl Predicate {
    fn expected(&self, n: i32) -> bool {
        match self {
            Self::Compare(op, v) => match op {
                0 => n == *v,
                1 => n != *v,
                2 => n < *v,
                3 => n <= *v,
                4 => n > *v,
                _ => n >= *v,
            },
            Self::Between(a, b) => (*a.min(b)..=*a.max(b)).contains(&n),
            Self::In(v) => v.contains(&n),
            Self::Not(p) => !p.expected(n),
            Self::And(a, b) => a.expected(n) && b.expected(n),
            Self::Or(a, b) => a.expected(n) || b.expected(n),
        }
    }

    fn render(&self, values: &mut Item) -> String {
        fn literal(values: &mut Item, n: i32) -> String {
            let key = format!(":v{}", values.len());
            values.insert(key.clone(), Value::N(n.into()));
            key
        }
        match self {
            Self::Compare(op, n) => format!(
                "#n {} {}",
                ["=", "<>", "<", "<=", ">", ">="][*op as usize],
                literal(values, *n)
            ),
            Self::Between(a, b) => format!(
                "#n BETWEEN {} AND {}",
                literal(values, *a.min(b)),
                literal(values, *a.max(b))
            ),
            Self::In(v) => format!(
                "#n IN ({})",
                v.iter()
                    .map(|n| literal(values, *n))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Not(p) => format!("NOT ({})", p.render(values)),
            Self::And(a, b) => format!("({}) AND ({})", a.render(values), b.render(values)),
            Self::Or(a, b) => format!("({}) OR ({})", a.render(values), b.render(values)),
        }
    }
}

fn predicate() -> impl Strategy<Value = Predicate> {
    prop_oneof![
        (0u8..6, -100i32..101).prop_map(|(op, n)| Predicate::Compare(op, n)),
        (-100i32..101, -100i32..101).prop_map(|(a, b)| Predicate::Between(a, b)),
        prop::collection::vec(-100i32..101, 1..6).prop_map(Predicate::In),
    ]
    .prop_recursive(5, 40, 2, |inner| {
        prop_oneof![
            inner.clone().prop_map(|p| Predicate::Not(Box::new(p))),
            (inner.clone(), inner.clone())
                .prop_map(|(a, b)| Predicate::And(Box::new(a), Box::new(b))),
            (inner.clone(), inner).prop_map(|(a, b)| Predicate::Or(Box::new(a), Box::new(b))),
        ]
    })
}

fn json_input() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        ".{0,40}".prop_map(|s| json!(s)),
    ]
    .prop_recursive(6, 80, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..8).prop_map(|v| json!(v)),
            prop::collection::btree_map("[A-Za-z_]{0,24}", inner, 0..8).prop_map(|m| json!(m)),
        ]
    })
}

const OPERATIONS: [&str; 13] = [
    "CreateTable",
    "DescribeTable",
    "DeleteTable",
    "GetItem",
    "PutItem",
    "UpdateItem",
    "DeleteItem",
    "Query",
    "Scan",
    "TransactWriteItems",
    "TransactGetItems",
    "BatchGetItem",
    "BatchWriteItem",
];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn table(service: &Service) {
    service.dispatch("CreateTable", br#"{"TableName":"synthetic-properties","BillingMode":"PAY_PER_REQUEST","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]}"#).await.unwrap();
}

#[test]
fn mixed_group_and_not_depth_does_not_overflow_the_stack() {
    // Parentheses and NOT each previously had a local depth budget. Combining
    // them exercises recursion that neither a flat NOT chain nor ASCII fuzzing reaches.
    let expression = std::env::var("SUPERCLOUDY_PARSER_PROBE").unwrap_or_else(|_| {
        format!(
            "{}attribute_exists(#n){}",
            format!("{}(", "NOT ".repeat(30)).repeat(30),
            ")".repeat(30)
        )
    });
    if let Ok(condition) = parser().condition(&expression) {
        let _ = condition.eval(&Item::new());
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn isolated_mixed_expression_parser_never_aborts(groups in 0usize..33, nots in 0usize..32) {
        let expression = format!("{}attribute_exists(#n){}", format!("{}(", "NOT ".repeat(nots)).repeat(groups), ")".repeat(groups));
        // A Rust stack overflow aborts the process instead of unwinding. Run
        // deep mixed inputs in a worker so proptest can observe and shrink it.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "dynamodb::expressions::properties::mixed_group_and_not_depth_does_not_overflow_the_stack", "--nocapture"])
            .env("SUPERCLOUDY_PARSER_PROBE", expression)
            .output()
            .unwrap();
        prop_assert!(output.status.success(), "worker exited {}: {}", output.status, String::from_utf8_lossy(&output.stderr));
    }

    #[test]
    fn arbitrary_expression_text_never_panics(input in ".{0,4300}") {
        if let Ok(c) = parser().condition(&input) { let _ = c.eval(&Item::new()); }
        if let Ok(actions) = parser().updates(&input) { let _ = apply_updates(&Item::new(), &actions); }
        if let Ok(paths) = parser().projection(&input) {
            for path in paths { let _ = get_path(&Item::new(), &path); }
        }
    }

    #[test]
    fn generated_conditions_match_integer_boolean_model(p in predicate(), n in -100i32..101) {
        let mut values = Item::new();
        let expression = p.render(&mut values);
        let mut parser = Parser::new(BTreeMap::from([("#n".into(), "literal.dot".into())]), values).unwrap();
        let condition = parser.condition(&expression).unwrap();
        parser.finish().unwrap();
        let item = BTreeMap::from([("literal.dot".into(), Value::N(n.into()))]);
        prop_assert_eq!(condition.eval(&item).unwrap(), p.expected(n), "{}", expression);
    }

    #[test]
    fn generated_updates_read_the_pre_update_item(a in -1000i32..1001, b in -1000i32..1001, delta in -100i32..101, replacement in -1000i32..1001, plus in any::<bool>()) {
        let old = Item::from([
            ("first.dot".into(), Value::N(a.into())),
            ("second".into(), Value::N(b.into())),
            ("gone".into(), Value::Bool(true)),
        ]);
        let names = BTreeMap::from([("#a".into(), "first.dot".into()), ("#b".into(), "second".into()), ("#g".into(), "gone".into())]);
        let values = Item::from([ (":delta".into(), Value::N(delta.into())), (":new".into(), Value::N(replacement.into())) ]);
        let mut parser = Parser::new(names, values).unwrap();
        let expression = format!("SET #b = :new, #a = #b {} :delta REMOVE #g", if plus { "+" } else { "-" });
        let actions = parser.updates(&expression).unwrap();
        parser.finish().unwrap();
        let updated = apply_updates(&old, &actions).unwrap();
        let expected = if plus { b + delta } else { b - delta };
        prop_assert_eq!(updated.get("first.dot"), Some(&Value::N(expected.into())));
        prop_assert_eq!(updated.get("second"), Some(&Value::N(replacement.into())));
        prop_assert!(!updated.contains_key("gone"));
    }

    #[test]
    fn generated_projection_paths_recover_nested_values(indices in prop::collection::vec(0usize..4, 0..20), leaf in any::<i32>()) {
        let mut nested = Value::N(leaf.into());
        for index in indices.iter().rev() {
            let mut list = vec![Value::Null; index + 1];
            list[*index] = nested;
            nested = Value::L(list);
        }
        let item = Item::from([("literal.dot".into(), nested)]);
        let expression = format!("#n{}", indices.iter().map(|i| format!("[{i}]")).collect::<String>());
        let mut parser = parser();
        let paths = parser.projection(&expression).unwrap();
        parser.finish().unwrap();
        prop_assert_eq!(get_path(&item, &paths[0]), Some(&Value::N(leaf.into())));
    }

    #[test]
    fn structured_expression_depths_do_not_panic(groups in 0usize..45, nots in 0usize..45, functions in 0usize..45, dereferences in 0usize..45, chain in 1usize..250) {
        let grouped = format!("{}{}attribute_exists(#n){}", "(".repeat(groups), "NOT ".repeat(nots), ")".repeat(groups));
        if let Ok(c) = parser().condition(&grouped) { let _ = c.eval(&Item::new()); }
        let mixed = format!("{}attribute_exists(#n){}", format!("{}(", "NOT ".repeat(nots)).repeat(groups), ")".repeat(groups));
        if let Ok(c) = parser().condition(&mixed) { let _ = c.eval(&Item::new()); }
        let chained = vec!["attribute_exists(#n)"; chain].join(" AND ");
        if let Ok(c) = parser().condition(&chained) { let _ = c.eval(&Item::new()); }
        let update = format!("SET #n = {}#n{}", "if_not_exists(#n,".repeat(functions), ")".repeat(functions));
        if let Ok(actions) = parser().updates(&update) { let _ = apply_updates(&Item::new(), &actions); }
        let projection = format!("#n{}", "[0]".repeat(dereferences));
        let result = parser().projection(&projection);
        prop_assert_eq!(result.is_ok(), dereferences <= 32);
    }

    #[test]
    fn arbitrary_json_request_bytes_do_not_panic(bytes in prop::collection::vec(any::<u8>(), 0..8192)) {
        runtime().block_on(async {
            let service = Service::default();
            for op in OPERATIONS { let _ = service.dispatch(op, &bytes).await; }
        });
    }

    #[test]
    fn arbitrary_key_conditions_do_not_panic(input in ".{0,4300}", depth in 0usize..45) {
        runtime().block_on(async {
            let service = Service::default();
            table(&service).await;
            let structured = format!("#pk = :pk AND {}#sk >= :bound{}", "(".repeat(depth), ")".repeat(depth));
            for expression in [input, structured] {
                let request = json!({"TableName":"synthetic-properties", "KeyConditionExpression":expression, "ExpressionAttributeNames":{"#pk":"pk","#sk":"sk"}, "ExpressionAttributeValues":{":pk":{"S":"partition"},":bound":{"N":"0"}}});
                let _ = service.dispatch("Query", &serde_json::to_vec(&request).unwrap()).await;
            }
        });
    }

    #[test]
    fn structured_json_requests_do_not_panic(input in json_input(), operation in 0usize..OPERATIONS.len(), depth in 0usize..150) {
        let body = serde_json::to_vec(&input).unwrap();
        let nested = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
        runtime().block_on(async {
            let service = Service::default();
            let _ = service.dispatch(OPERATIONS[operation], &body).await;
            let _ = service.dispatch(OPERATIONS[operation], nested.as_bytes()).await;
            // Preserve a recognized field envelope so generated typed values
            // reach AttributeValue validation rather than unknown-field rejection.
            let item = json!({"TableName":"synthetic-properties", "Item":{"pk":{"S":"partition"},"sk":{"N":"0"},"payload": input}});
            table(&service).await;
            let _ = service.dispatch("PutItem", &serde_json::to_vec(&item).unwrap()).await;
        });
    }

    #[test]
    fn json_body_decodes_generated_nested_attributes(text in ".{0,80}", n in -10000i32..10001, flag in any::<bool>(), depth in 0usize..20) {
        let mut wire = json!({"M":{"text":{"S":text},"number":{"N":format!("{n}.00")},"flag":{"BOOL":flag}}});
        let mut expected = Value::M(Item::from([
            ("text".into(), Value::S(text)),
            ("number".into(), Value::N(n.into())),
            ("flag".into(), Value::Bool(flag)),
        ]));
        for _ in 0..depth {
            wire = json!({"L":[wire]});
            expected = Value::L(vec![expected]);
        }
        let body = serde_json::to_vec(&json!({"TableName":"synthetic-properties","Item":{"payload":wire}})).unwrap();
        let request = operations::decode("PutItem", &body).unwrap();
        let decoded = parse_item(request.item.as_ref().unwrap()).unwrap();
        prop_assert_eq!(decoded.get("payload"), Some(&expected));
    }

    #[test]
    fn generated_key_conditions_select_expected_rows(bound in -10i32..11, op in 0usize..6, reversed in any::<bool>()) {
        runtime().block_on(async {
            let service = Service::default();
            table(&service).await;
            for score in -8..=8 {
                let request = json!({"TableName":"synthetic-properties","Item":{"pk":{"S":"partition"},"sk":{"N":score.to_string()}}});
                service.dispatch("PutItem", &serde_json::to_vec(&request).unwrap()).await.unwrap();
            }
            let operators = ["=", "<", "<=", ">", ">=", "BETWEEN"];
            let sort = if op == 5 {
                "#sk BETWEEN :lo AND :hi".to_owned()
            } else if reversed {
                format!(":bound {} #sk", operators[op])
            } else {
                format!("#sk {} :bound", operators[op])
            };
            let mut values = json!({":pk":{"S":"partition"}});
            if op == 5 {
                values[":lo"] = json!({"N":bound.min(0).to_string()});
                values[":hi"] = json!({"N":bound.max(0).to_string()});
            } else {
                values[":bound"] = json!({"N":bound.to_string()});
            }
            let request = json!({"TableName":"synthetic-properties","KeyConditionExpression":format!("#pk = :pk AND {sort}"),"ExpressionAttributeNames":{"#pk":"pk","#sk":"sk"},"ExpressionAttributeValues":values});
            let response = service.dispatch("Query", &serde_json::to_vec(&request).unwrap()).await.unwrap();
            let actual = response["Items"].as_array().unwrap().iter().map(|item| item["sk"]["N"].as_str().unwrap().parse::<i32>().unwrap()).collect::<Vec<_>>();
            let expected = (-8..=8).filter(|score| {
                let (a,b) = if reversed { (bound,*score) } else { (*score,bound) };
                match op { 0 => a == b, 1 => a < b, 2 => a <= b, 3 => a > b, 4 => a >= b, _ => (bound.min(0)..=bound.max(0)).contains(score) }
            }).collect::<Vec<_>>();
            prop_assert_eq!(actual, expected);
            Ok(())
        })?;
    }
}
