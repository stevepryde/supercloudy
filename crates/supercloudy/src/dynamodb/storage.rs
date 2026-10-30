use super::{
    api::{AttributeDefinition, KeySchema, Projection, Throughput},
    error::{Error, Result},
    value::{Item, Scalar, item_size_bytes},
};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
#[derive(Default)]
pub(crate) struct Database {
    pub tables: BTreeMap<String, Table>,
    pub tokens: BTreeMap<String, super::transactions::TokenRecord>,
}
pub(crate) type KeyTuple = (Scalar, Option<Scalar>);
pub(crate) struct Index {
    pub name: String,
    pub local: bool,
    pub schema: Vec<KeySchema>,
    pub projection: Projection,
    pub throughput: Option<Throughput>,
}
pub(crate) struct Table {
    pub name: String,
    pub schema: Vec<KeySchema>,
    pub definitions: Vec<AttributeDefinition>,
    pub indexes: Vec<Index>,
    pub billing: String,
    pub throughput: Option<Throughput>,
    pub created: f64,
    pub items: BTreeMap<KeyTuple, Item>,
}
impl Table {
    pub fn key(&self, item: &Item, exact: bool) -> Result<KeyTuple> {
        self.schema_key(item, &self.schema, exact)
    }
    pub fn schema_key(&self, item: &Item, schema: &[KeySchema], exact: bool) -> Result<KeyTuple> {
        if exact && item.len() != schema.len() {
            return Err(Error::validation("Key does not match schema"));
        }
        let scalar = |i: usize| {
            let k = &schema[i];
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
            if schema.len() == 2 {
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
    pub fn index_key(&self, item: &Item, index: &Index) -> Result<Option<KeyTuple>> {
        // Validate every present indexed attribute, including a partial sparse key.
        for (position, key) in index.schema.iter().enumerate() {
            if let Some(value) = item.get(&key.attribute_name) {
                let tag = &self
                    .definitions
                    .iter()
                    .find(|d| d.attribute_name == key.attribute_name)
                    .unwrap()
                    .attribute_type;
                Scalar::from_value(value, tag, if position == 0 { 2048 } else { 1024 })?;
            }
        }
        if index
            .schema
            .iter()
            .any(|key| !item.contains_key(&key.attribute_name))
        {
            return Ok(None);
        }
        self.schema_key(item, &index.schema, false).map(Some)
    }
    pub fn projected(&self, item: &Item, index: &Index) -> Item {
        if index.projection.projection_type == "ALL" {
            return item.clone();
        }
        item.iter()
            .filter(|(name, _)| {
                self.schema
                    .iter()
                    .chain(&index.schema)
                    .any(|k| &k.attribute_name == *name)
                    || index
                        .projection
                        .non_key_attributes
                        .as_ref()
                        .is_some_and(|attrs| attrs.contains(name))
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
    pub fn validate_candidate(&self, item: &Item) -> Result<()> {
        self.key(item, false)?;
        let size = item_size_bytes(item);
        let mut combined = size;
        for index in &self.indexes {
            if self.index_key(item, index)?.is_some() && index.local {
                combined += item_size_bytes(&self.projected(item, index));
            }
        }
        if size > 400 * 1024 || combined > 400 * 1024 {
            return Err(Error::validation(
                "Item and local index entries exceed 400 KB",
            ));
        }
        Ok(())
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
        for (local, field) in [
            (false, "GlobalSecondaryIndexes"),
            (true, "LocalSecondaryIndexes"),
        ] {
            let indexes: Vec<_> = self.indexes.iter().filter(|i| i.local == local).map(|i| {
                let mut projection = json!({"ProjectionType":i.projection.projection_type});
                if let Some(attrs) = &i.projection.non_key_attributes { projection["NonKeyAttributes"] = json!(attrs); }
                let mut value = json!({"IndexName":i.name,"KeySchema":i.schema.iter().map(|k|json!({"AttributeName":k.attribute_name,"KeyType":k.key_type})).collect::<Vec<_>>(),"Projection":projection,"IndexArn":format!("arn:aws:dynamodb:us-east-1:000000000000:table/{}/index/{}",self.name,i.name),"IndexSizeBytes":0,"ItemCount":self.items.values().filter(|item| self.index_key(item,i).ok().flatten().is_some()).count()});
                if !local { value["IndexStatus"] = json!("ACTIVE"); }
                if let Some(t) = &i.throughput { value["ProvisionedThroughput"] = json!({"ReadCapacityUnits":t.read_capacity_units,"WriteCapacityUnits":t.write_capacity_units,"NumberOfDecreasesToday":0}); }
                value
            }).collect();
            if !indexes.is_empty() {
                d[field] = json!(indexes);
            }
        }
        d
    }
}
