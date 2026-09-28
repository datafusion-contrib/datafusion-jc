use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct DatafusionJsonCatalog {
    inner: Arc<Mutex<DatafusionJsonCatalogInner>>,
}

#[derive(Debug)]
struct DatafusionJsonCatalogInner {
    catalog_name: String,
    table_names_cache: DashMap<String, Arc<dyn TableProvider>>,
}

impl DatafusionJsonCatalog {
    pub fn new(path: String, catalog_name: String) -> Self {
        let inner = Arc::new(Mutex::new(DatafusionJsonCatalogInner {
            catalog_name,
            table_names_cache: DashMap::<String, Arc<dyn TableProvider>>::new(),
        }));

        Self { inner }
    }
}

#[async_trait]
impl SchemaProvider for DatafusionJsonCatalog {
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
        todo!()
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
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;

    #[test]
    fn build_a_json_catalog() {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let catalog = DatafusionJsonCatalog::new(
            tmp_file.path().to_str().unwrap().to_owned(),
            "test".to_owned(),
        );

        assert!(catalog.table_names().is_empty());
    }

    #[test]
    fn write_to_json_catalog() {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let catalog = DatafusionJsonCatalog::new(
            tmp_file.path().to_str().unwrap().to_owned(),
            "test".to_owned(),
        );

        assert!(catalog.table_names().is_empty());

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema_ref = SchemaRef::new(schema);
        let memtable = MemTable::try_new(schema_ref.clone(), vec![vec![RecordBatch::new_empty(schema_ref)]]).unwrap();
        catalog.register_table("tadashi".to_owned(), Arc::new(memtable)).unwrap();

        let schema_2 = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema_ref_2 = SchemaRef::new(schema_2);
        let memtable_2 = MemTable::try_new(schema_ref_2.clone(), vec![vec![RecordBatch::new_empty(schema_ref_2)]]).unwrap();
        catalog.register_table("mizu".to_owned(), Arc::new(memtable_2)).unwrap();

        let tables = catalog.table_names();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"tadashi".to_owned()));
        assert!(tables.contains(&"mizu".to_owned()));
    }
}
