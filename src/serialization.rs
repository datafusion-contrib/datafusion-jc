use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Deserialize, Serialize, Debug)]
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

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct CatalogMeta {
    pub(crate) name: String,
    pub(crate) path: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct SerializableSchemaProvider {
    pub(crate) schema_name: String,
    pub(crate) tables: Vec<SerializableTableAndSchema>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SerializableTableAndSchema {
    pub(crate) table_name: String,
    pub(crate) schema: SchemaRef,
    pub(crate) provider: Arc<dyn SerializableTableProvider>,
}

#[typetag::serde(tag = "type")]
pub trait SerializableTableProvider: TableProvider {}
