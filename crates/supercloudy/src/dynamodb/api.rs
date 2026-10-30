use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Deserialize)]
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
    pub return_values_on_condition_check_failure: Option<String>,
    pub limit: Option<i64>,
    pub exclusive_start_key: Option<Value>,
    pub scan_index_forward: Option<bool>,
    pub select: Option<String>,
    pub consistent_read: Option<bool>,
    pub index_name: Option<String>,
    pub projection_expression: Option<String>,
    pub filter_expression: Option<String>,
    pub segment: Option<i64>,
    pub total_segments: Option<i64>,
    pub global_secondary_indexes: Option<Vec<IndexDefinition>>,
    pub local_secondary_indexes: Option<Vec<IndexDefinition>>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub(crate) struct KeySchema {
    pub attribute_name: String,
    pub key_type: String,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub(crate) struct AttributeDefinition {
    pub attribute_name: String,
    pub attribute_type: String,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub(crate) struct Throughput {
    pub read_capacity_units: i64,
    pub write_capacity_units: i64,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub(crate) struct IndexDefinition {
    pub index_name: String,
    pub key_schema: Vec<KeySchema>,
    pub projection: Projection,
    pub provisioned_throughput: Option<Throughput>,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub(crate) struct Projection {
    pub projection_type: String,
    pub non_key_attributes: Option<Vec<String>>,
}
