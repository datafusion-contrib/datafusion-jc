use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{
    CatalogProvider, MemorySchemaProvider, Session, TableProvider, TableProviderFactory,
};
use datafusion::logical_expr::CreateExternalTable;
use datafusion_jc::DatafusionJsonCatalog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::vec;

#[derive(Serialize, Deserialize, Debug)]
struct MyTableProvider {
    name: String,
    schema: SchemaRef,
    file: String,
}

impl MyTableProvider {
    fn new(name: String, schema: SchemaRef) -> Self {
        // Hardcode the path for this example.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/userdata.parquet");
        Self {
            name,
            schema,
            file: path.to_string(),
        }
    }
}

#[async_trait]
impl TableProviderFactory for MyTableProvider {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        todo!()
    }
}

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

    let new_catalog = DatafusionJsonCatalog::from_json(expected);
    let schemas = new_catalog?.schema_names();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas, catalog.schema_names());

    Ok(())
}
