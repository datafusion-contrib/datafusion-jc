use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use redb::Database;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct DatafusionJsonCatalog {
    inner: Arc<DatafusionJsonCatalogInner>,
}

#[derive(Debug)]
struct DatafusionJsonCatalogInner {
    db: Database,
    catalog_name: String,
    table_names_cache: Vec<String>,
}

impl DatafusionJsonCatalog {
    pub fn new(path: String, catalog_name: String) -> Self {
        let db = Database::create(path.as_str()).expect("Could not create catalog database");
        let inner = Arc::new(DatafusionJsonCatalogInner {
            db,
            catalog_name,
            table_names_cache: vec![],
        });

        Self { inner }
    }
}

#[async_trait]
impl SchemaProvider for DatafusionJsonCatalog {
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names_cache.clone()
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
        todo!()
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

    #[test]
    fn build_a_json_catalog() {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let catalog = DatafusionJsonCatalog::new(
            tmp_file.path().to_str().unwrap().to_owned(),
            "test".to_owned(),
        );

        assert!(catalog.table_names().is_empty());
    }
}
