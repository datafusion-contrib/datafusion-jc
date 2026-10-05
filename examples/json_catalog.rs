use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, MemTable, MemorySchemaProvider, TableProvider};
use datafusion_jc::{DatafusionJsonCatalog, JsonSerializableTableProvider, SerializableTableAndSchema};
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

impl JsonSerializableTableProvider for MyTableProvider {
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        self.inner.clone()
    }

    fn serialize(&self) -> datafusion::common::Result<SerializableTableAndSchema> {
        todo!()
    }

    fn deserialize(&self, input: &str) -> datafusion::common::Result<()> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let schema_name = "foo_bar";
    let catalog =
        DatafusionJsonCatalog::new(String::from("tadashi_catalog"), String::from("foo/")).unwrap();
    catalog
        .register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))
        .unwrap();

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let batch = vec![RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap()];

    let table = Arc::new(MyTableProvider::new(schema.clone(), batch));
    catalog
        .register_table(schema_name, String::from("cat_metrics"), table.clone())
        .unwrap();

    let json = catalog.encode_json().await?;
    println!("{}", json);

    Ok(())
}
