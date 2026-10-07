# DataFusion JSON Catalog

Note: This is not an official Apache Software Foundation project.

Native JSON catalog for Apache DataFusion.

Usage

```rust
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_jc::{DatafusionJsonCatalog, SerializableTableProvider};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::vec;

#[derive(Serialize, Deserialize, Debug)]
struct MyTableProvider {
    name: String,
    schema: SchemaRef,
}

impl MyTableProvider {
    fn new(name: String, schema: SchemaRef) -> Self {
        Self { name, schema }
    }
}

#[async_trait]
impl TableProvider for MyTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

#[typetag::serde]
impl SerializableTableProvider for MyTableProvider {}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    // Create a new DatafusionJsonCatalog, we will use this to register schemas and tables.
    let schema_name = "foo_bar";
    let catalog =
        DatafusionJsonCatalog::new(String::from("tadashi_catalog"), String::from("foo/"))?;

    // Register a new schema with the catalog.
    catalog.register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))?;

    // Register a table with the catalog.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let table = Arc::new(MyTableProvider::new("foo".to_string(), schema.clone()));
    catalog.register_table(schema_name, String::from("cat_metrics"), table)?;

    // Encode the catalog to JSON.
    let json = catalog.encode_json().await?;
    let expected = r#"{"catalog_metadata":{"name":"tadashi_catalog","path":"foo/"},"schema_providers":[{"schema_name":"foo_bar","tables":[{"table_name":"cat_metrics","schema":{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}},"provider":{"type":"MyTableProvider","name":"foo","schema":{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}}}]}]}"#;
    assert_eq!(json, expected);

    Ok(())
}
```


