use datafusion::arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct JsonData {
    pub(crate) catalog_metadata: CatalogMeta,
    pub(crate) schema_providers: Vec<SerializableSchemaProvider>,
}

impl JsonData {
    pub(crate) fn new(name: String, path: String) -> Self {
        Self {
            catalog_metadata: CatalogMeta { name, path },
            schema_providers: vec![],
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct CatalogMeta {
    pub(crate) name: String,
    pub(crate) path: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct SerializableSchemaProvider {
    pub(crate) tables: Vec<SerializableTableAndSchema>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub(crate) struct SerializableTableAndSchema {
    pub(crate) table_name: String,
    pub(crate) schema: SchemaRef,
}