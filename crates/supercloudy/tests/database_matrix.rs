#[path = "database_matrix/mod.rs"]
mod matrix;

#[tokio::test]
async fn mongodb_missing_arrays_and_numeric_types() {
    let server = supercloudy::mongodb::start(0).await.unwrap();
    let client = mongodb::Client::with_uri_str(format!(
        "mongodb://{}/?directConnection=true",
        server.address()
    ))
    .await
    .unwrap();
    let cases = matrix::mongo(&client).await;
    for (name, expected) in [
        ("null-or-missing", vec![1, 3]),
        ("not-null", vec![2, 4]),
        ("missing", vec![3]),
        ("array-element-range", vec![1]),
        ("array-all", vec![1]),
        ("nested-array-path", vec![1, 4]),
        ("negated-array-membership", vec![2, 3]),
        ("cross-numeric-equality", vec![1, 4]),
        ("regex", vec![4]),
        ("regex-array", vec![1, 4]),
        ("regex-membership-array", vec![1, 4]),
        ("array-numeric-type", vec![1, 2, 4]),
    ] {
        let rows = &cases.iter().find(|(label, _)| label == name).unwrap().1;
        assert_eq!(
            rows.iter()
                .map(|row| row.get_i32("_id").unwrap())
                .collect::<Vec<_>>(),
            expected,
            "{name}"
        );
    }
    let missing = &cases
        .iter()
        .find(|(name, _)| name == "missing-expression")
        .unwrap()
        .1;
    assert!(!missing[2].contains_key("copy"));
    assert_eq!(missing[2].get_str("kind").unwrap(), "missing");
    for name in ["count-type", "count-accumulator-type"] {
        let rows = &cases.iter().find(|(label, _)| label == name).unwrap().1;
        assert_eq!(rows[0].get_i32("n").unwrap(), 4);
    }
    let unwound = &cases
        .iter()
        .find(|(name, _)| name == "unwind-preserved-index")
        .unwrap()
        .1;
    assert!(
        unwound
            .iter()
            .all(|row| row.get("idx") == Some(&bson::Bson::Null))
    );
    let stored = &cases
        .iter()
        .find(|(name, _)| name == "stored-numeric-promotion")
        .unwrap()
        .1;
    assert_eq!(stored[0].get_i64("n").unwrap(), 4);
    assert_eq!(stored[1].get_i64("n").unwrap(), 4);
    assert_eq!(stored[2].get_f64("n").unwrap(), 5.5);
    assert!(matches!(
        stored[3].get("n"),
        Some(bson::Bson::Decimal128(_))
    ));
    client.shutdown().await;
    server.shutdown().await.unwrap();
}
