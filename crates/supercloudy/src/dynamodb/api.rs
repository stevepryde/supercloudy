use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Request {
    pub table_name: String,
    pub key: Option<Value>,
    pub item: Option<Value>,
    pub key_schema: Option<Vec<KeySchema>>,
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    pub billing_mode: Option<String>,
    pub provisioned_throughput: Option<Throughput>,
    pub condition_expression: Option<String>,
    pub update_expression: Option<String>,
    pub key_condition_expression: Option<String>,
    #[serde(default)]
    pub expression_attribute_names: BTreeMap<String, String>,
    pub expression_attribute_values: Option<Value>,
    pub return_values: Option<String>,
    pub limit: Option<i64>,
    pub exclusive_start_key: Option<Value>,
    pub scan_index_forward: Option<bool>,
    pub select: Option<String>,
    pub consistent_read: Option<bool>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct KeySchema {
    pub attribute_name: String,
    pub key_type: String,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: String,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Throughput {
    pub read_capacity_units: i64,
    pub write_capacity_units: i64,
}
