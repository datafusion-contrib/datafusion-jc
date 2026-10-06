use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, MemTable, MemorySchemaProvider};
use datafusion_jc::DatafusionJsonCatalog;
use std::sync::Arc;
use std::vec;

#[derive(Debug)]
struct MyTableProvider {
    inner: Arc<MemTable>,
}

impl MyTableProvider {
    fn new(schema: SchemaRef, records: Vec<RecordBatch>) -> Self {
        let inner = Arc::new(MemTable::try_new(schema, vec![records]).unwrap());
        Self { inner }
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

    // Build a record batch to generate a table.
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = vec![RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array)],
    )?];

    // Register a table with the catalog.
    let table = Arc::new(MyTableProvider::new(schema.clone(), batch));
    catalog.register_table(schema_name, String::from("cat_metrics"), table.inner.clone())?;

    // Encode the catalog to JSON.
    let json = catalog.encode_json().await?;
    let expected = r#"{"catalog_metadata":{"name":"tadashi_catalog","path":"foo/"},"schema_providers":[{"tables":[{"table_name":"cat_metrics","schema":{"fields":[{"name":"id","data_type":"Int32","nullable":false,"dict_id":0,"dict_is_ordered":false,"metadata":{}}],"metadata":{}}}]}]}"#;
    assert_eq!(json, expected);

    Ok(())
}
