use crate::serialization::SerializableTableProvider;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::sync::{Arc, Mutex};

/// DatafusionJsonSchema is a schema provider for Datafusion that provides access to JSON files.
#[derive(Debug)]
pub struct DatafusionJsonSchema {
    inner: Arc<Mutex<DatafusionJsonSchemaInner>>,
}

#[derive(Debug)]
struct DatafusionJsonSchemaInner {
    table_names_cache: DashMap<String, Arc<dyn SerializableTableProvider>>,
}

impl DatafusionJsonSchema {
    /// Creates an empty schema with no tables.
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(DatafusionJsonSchemaInner {
            table_names_cache: DashMap::<String, Arc<dyn SerializableTableProvider>>::new(),
        }));

        Self { inner }
    }

    pub fn get_serializable_table_provider(
        &self,
        key: String,
    ) -> Option<Arc<dyn SerializableTableProvider>> {
        match self.inner.lock().unwrap().table_names_cache.get(&key) {
            None => None,
            Some(table_provider) => Some(table_provider.clone()),
        }
    }

    /// Registers `table` under `name`, replacing any existing table with that
    /// name. Returns the registered table.
    pub fn register_table(
        &self,
        name: String,
        table: Arc<dyn SerializableTableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let inner = self.inner.lock().unwrap();
        inner.table_names_cache.insert(name, table.clone());

        Ok(Some(table))
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
            .map(|table| Ok(Some(table.clone() as Arc<dyn TableProvider>)))
            .unwrap_or(Ok(None))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .table_names_cache
            .contains_key(name)
    }
}
