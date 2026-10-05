use std::fmt::Debug;
use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use serde::{Deserialize, Serialize};

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
pub struct SerializableTableAndSchema {
    pub table_name: String,
    pub schema: SchemaRef,
}

pub trait JsonSerializableTableProvider: Send + Sync + Debug {
    fn table_provider(&self) -> Arc<dyn TableProvider>;

    fn serialize(&self) -> datafusion::common::Result<SerializableTableAndSchema>;

    fn deserialize(&self, input: &str) -> datafusion::common::Result<()>;
}
