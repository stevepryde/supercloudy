use super::{
    api::{AttributeDefinition, KeySchema, Throughput},
    error::{Error, Result},
    value::{Item, Scalar},
};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
#[derive(Default)]
pub(crate) struct Database {
    pub tables: BTreeMap<String, Table>,
}
pub(crate) type KeyTuple = (Scalar, Option<Scalar>);
pub(crate) struct Table {
    pub name: String,
    pub schema: Vec<KeySchema>,
    pub definitions: Vec<AttributeDefinition>,
    pub billing: String,
    pub throughput: Option<Throughput>,
    pub created: f64,
    pub items: BTreeMap<KeyTuple, Item>,
}
impl Table {
    pub fn key(&self, item: &Item, exact: bool) -> Result<KeyTuple> {
        if exact && item.len() != self.schema.len() {
            return Err(Error::validation("Key does not match schema"));
        }
        let scalar = |i: usize| {
            let k = &self.schema[i];
            let value = item
                .get(&k.attribute_name)
                .ok_or_else(|| Error::validation("Missing key attribute"))?;
            let tag = &self
                .definitions
                .iter()
                .find(|d| d.attribute_name == k.attribute_name)
                .unwrap()
                .attribute_type;
            Scalar::from_value(value, tag, if i == 0 { 2048 } else { 1024 })
        };
        Ok((
            scalar(0)?,
            if self.schema.len() == 2 {
                Some(scalar(1)?)
            } else {
                None
            },
        ))
    }
    pub fn key_item(&self, item: &Item) -> Item {
        self.schema
            .iter()
            .map(|k| (k.attribute_name.clone(), item[&k.attribute_name].clone()))
            .collect()
    }
    pub fn description(&self) -> Json {
        let mut d = json!({"TableName":self.name,"TableStatus":"ACTIVE","CreationDateTime":self.created,
            "TableArn":format!("arn:aws:dynamodb:us-east-1:000000000000:table/{}",self.name),
            "KeySchema":self.schema.iter().map(|k|json!({"AttributeName":k.attribute_name,"KeyType":k.key_type})).collect::<Vec<_>>(),
            "AttributeDefinitions":self.definitions.iter().map(|d|json!({"AttributeName":d.attribute_name,"AttributeType":d.attribute_type})).collect::<Vec<_>>(),
            "ItemCount":self.items.len(),"TableSizeBytes":0});
        if self.billing == "PAY_PER_REQUEST" {
            d["BillingModeSummary"] = json!({"BillingMode":"PAY_PER_REQUEST"});
        }
        if let Some(t) = &self.throughput {
            d["ProvisionedThroughput"] = json!({"ReadCapacityUnits":t.read_capacity_units,"WriteCapacityUnits":t.write_capacity_units,"NumberOfDecreasesToday":0});
        }
        d
    }
}
