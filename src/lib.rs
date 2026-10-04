use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion_proto::logical_plan::file_formats::JsonLogicalExtensionCodec;
use datafusion_session::CatalogProvider;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct DatafusionJsonCatalog {
    inner: Arc<Mutex<DatafusionJsonCatalogInner>>,
}

#[derive(Debug)]
struct DatafusionJsonCatalogInner {
    catalog_name: String,
    catalog_path: String,
    codec: JsonLogicalExtensionCodec,
    schema_providers: DashMap<String, Arc<DatafusionJsonSchema>>,
}

#[async_trait]
impl CatalogProvider for DatafusionJsonCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .schema_providers
            .iter()
            .map(|(r)| r.key().clone())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(schema_provider) = self.inner
            .lock()
            .unwrap()
            .schema_providers
            .get(name)
            .map(|r| r.value().clone()) {
            let a: Arc<dyn SchemaProvider> = schema_provider;
            Some(a)
        } else {
            None
        }
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.inner.lock().unwrap().schema_providers.insert(name.to_string(), schema))
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        todo!()
    }
}

/// DatafusionJsonSchema is a schema provider for Datafusion that provides access to JSON files.
#[derive(Debug)]
pub struct DatafusionJsonSchema {
    inner: Arc<Mutex<DatafusionJsonSchemaInner>>,
}

#[derive(Debug)]
struct DatafusionJsonSchemaInner {
    table_names_cache: DashMap<String, Arc<dyn TableProvider>>,
}

impl DatafusionJsonSchema {
    pub fn new(path: String, catalog_name: String) -> Self {
        let inner = Arc::new(Mutex::new(DatafusionJsonSchemaInner {
            table_names_cache: DashMap::<String, Arc<dyn TableProvider>>::new(),
        }));

        Self { inner }
    }
}

#[async_trait]
impl SchemaProvider for DatafusionJsonSchema {
    fn table_names(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .table_names_cache
            .iter()
            .map(|key_value_ref| key_value_ref.key().clone())
            .collect::<Vec<String>>()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.inner
            .lock()
            .unwrap()
            .table_names_cache
            .get(name)
            .map(|table| Ok(Some(table.clone())))
            .unwrap_or(Ok(None))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .table_names_cache
            .insert(name, table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        todo!()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .table_names_cache
            .contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion_proto::logical_plan::LogicalExtensionCodec;

    #[test]
    fn build_a_json_catalog() {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let catalog = DatafusionJsonSchema::new(
            tmp_file.path().to_str().unwrap().to_owned(),
            "test".to_owned(),
        );

        assert!(catalog.table_names().is_empty());
    }

    #[tokio::test]
    async fn write_to_json_catalog() {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let catalog = DatafusionJsonSchema::new(
            tmp_file.path().to_str().unwrap().to_owned(),
            "test".to_owned(),
        );

        assert!(catalog.table_names().is_empty());

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema_ref = SchemaRef::new(schema);
        let memtable = MemTable::try_new(
            schema_ref.clone(),
            vec![vec![RecordBatch::new_empty(schema_ref)]],
        )
            .unwrap();
        catalog
            .register_table("tadashi".to_owned(), Arc::new(memtable))
            .unwrap();

        let schema_2 = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema_ref_2 = SchemaRef::new(schema_2);
        let memtable_2 = MemTable::try_new(
            schema_ref_2.clone(),
            vec![vec![RecordBatch::new_empty(schema_ref_2)]],
        )
            .unwrap();
        catalog
            .register_table("mizu".to_owned(), Arc::new(memtable_2))
            .unwrap();

        let tables = catalog.table_names();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"tadashi".to_owned()));
        assert!(tables.contains(&"mizu".to_owned()));

        let table = catalog.table("tadashi").await.unwrap();
        assert!(table.is_some());
        let table = table.unwrap();
        let table_ref = datafusion_common::TableReference::from("tadashi");
        let mut buf = Vec::new();

        let codec = JsonLogicalExtensionCodec {};
        let r = codec.try_encode_table_provider(&table_ref, table, &mut buf);
        println!("{:?}", r);

        assert!(r.is_ok());
        assert!(!buf.is_empty());
    }
}
