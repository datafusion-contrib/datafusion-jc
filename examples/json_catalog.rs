use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemTable, MemorySchemaProvider};
use datafusion::execution::context::SessionContext;
use datafusion_jc::DatafusionJsonCatalog;
use std::sync::Arc;
use std::vec;

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let session = SessionContext::new();
    // Create a new DatafusionJsonCatalog, we will use this to register schemas and tables.
    let schema_name = "foo_bar";
    let catalog = DatafusionJsonCatalog::new(
        String::from("tadashi_catalog"),
        String::from("foo/"),
        session.state().clone(),
    )?;

    // Register a new schema with the catalog.
    catalog.register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))?;

    // Register a table with the catalog.
    let id_array = Int32Array::from(vec![1, 2, 3, 4]);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)])?;
    let table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![record_batch]])?);
    catalog.register_table(schema_name, String::from("cat_metrics"), table)?;

    // Encode the catalog to JSON.
    let json = catalog.encode_json().await?;
    let expected = r#"{"catalog_metadata":{"name":"tadashi_catalog","path":"foo/"},"schema_providers":[{"schema_name":"foo_bar","tables":[{"table_name":"cat_metrics","schema":{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}}]}]}"#;
    assert_eq!(json, expected);

    let new_catalog = DatafusionJsonCatalog::from_json(expected, session.state().clone()).await?;
    let schemas = new_catalog.schema_names();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas, catalog.schema_names());

    Ok(())
}
