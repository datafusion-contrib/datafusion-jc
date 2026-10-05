mod serialization;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
pub use crate::serialization::{JsonSerializableTableProvider, SerializableTableAndSchema};
use crate::serialization::{JsonData, SerializableSchemaProvider};

#[derive(Debug)]
struct DatafusionJsonCatalogInner {
    catalog_name: String,
    catalog_path: String,
    schema_providers: DashMap<String, Arc<DatafusionJsonSchema>>,
}

#[derive(Debug)]
pub struct DatafusionJsonCatalog {
    inner: Arc<Mutex<DatafusionJsonCatalogInner>>,
}

impl DatafusionJsonCatalog {
    pub fn new(catalog_name: String, catalog_path: String) -> datafusion::common::Result<Self> {
        let inner = Arc::new(Mutex::new(DatafusionJsonCatalogInner {
            catalog_name,
            catalog_path,
            schema_providers: Default::default(),
        }));
        Ok(Self { inner })
    }

    pub fn from_json(json: &str) -> datafusion::common::Result<Self> {
        let json_data: JsonData = serde_json::from_str(json).unwrap();
        let inner = Arc::new(Mutex::new(DatafusionJsonCatalogInner {
            catalog_name: json_data.catalog_metadata.name,
            catalog_path: json_data.catalog_metadata.path,
            schema_providers: Default::default(),
        }));

        for schema_provider in json_data.schema_providers {
            let schema_provider_ref = Arc::new(DatafusionJsonSchema::new());
            for table in schema_provider.tables {}
        }

        Ok(Self { inner })
    }

    pub fn register_table(
        &self,
        schema_name: &str,
        name: String,
        table: Arc<dyn JsonSerializableTableProvider>,
    ) -> datafusion::common::Result<()> {
        let inner = self.inner.lock().unwrap();
        match inner.schema_providers.get_mut(schema_name) {
            None => {}
            Some(schema_provider) => {
                schema_provider.register_table(name, table)?;
            }
        }

        Ok(())
    }

    pub async fn encode_json(&self) -> datafusion::common::Result<String> {
        let inner = self.inner.lock().unwrap();
        let mut data = JsonData::new(inner.catalog_name.clone(), inner.catalog_path.clone());
        for provider in inner.schema_providers.iter() {
            let mut provider_tables = vec![];
            for name in provider.table_names() {
                let schema = provider.table(&name).await?.unwrap().schema();
                let s = SerializableTableAndSchema {
                    table_name: name.clone(),
                    schema: schema.clone(),
                };
                provider_tables.push(s);
            }

            data.schema_providers.push(SerializableSchemaProvider {
                tables: provider_tables,
            })
        }

        let output_json = serde_json::to_string(&data).unwrap();

        Ok(output_json)
    }
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
        if let Some(schema_provider) = self
            .inner
            .lock()
            .unwrap()
            .schema_providers
            .get(name)
            .map(|r| r.value().clone())
        {
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
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        let inner = self.inner.lock().unwrap();
        let schema_provider = Arc::new(DatafusionJsonSchema::new());
        inner.schema_providers.insert(name.to_string(), schema_provider);

        Ok(Some(schema.clone()))
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
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
    table_names_cache: DashMap<String, Arc<dyn JsonSerializableTableProvider>>,
}

impl DatafusionJsonSchema {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(DatafusionJsonSchemaInner {
            table_names_cache: DashMap::<String, Arc<dyn JsonSerializableTableProvider>>::new(),
        }));

        Self { inner }
    }

    pub fn register_table(
        &self,
        name: String,
        table: Arc<dyn JsonSerializableTableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let inner = self.inner.lock().unwrap();
        inner.table_names_cache.insert(name, table.clone());

        Ok(Some(table.table_provider().clone()))
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
            .map(|table| Ok(Some(table.table_provider().clone())))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_a_json_catalog() {
        let tmp_file = tempfile::NamedTempFile::new().unwrap();
        let catalog = DatafusionJsonSchema::new();

        assert!(catalog.table_names().is_empty());
    }

    // #[tokio::test]
    // async fn write_to_json_catalog() {
    //     let tmp_file = tempfile::NamedTempFile::new().unwrap();
    //     let catalog = DatafusionJsonSchema::new();
    //
    //     assert!(catalog.table_names().is_empty());
    //
    //     let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    //     let schema_ref = SchemaRef::new(schema);
    //     let memtable = MemTable::try_new(
    //         schema_ref.clone(),
    //         vec![vec![RecordBatch::new_empty(schema_ref)]],
    //     )
    //         .unwrap();
    //     catalog
    //         .register_table("tadashi".to_owned(), Arc::new(memtable))
    //         .unwrap();
    //
    //     let schema_2 = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    //     let schema_ref_2 = SchemaRef::new(schema_2);
    //     let memtable_2 = MemTable::try_new(
    //         schema_ref_2.clone(),
    //         vec![vec![RecordBatch::new_empty(schema_ref_2)]],
    //     )
    //         .unwrap();
    //     catalog
    //         .register_table("mizu".to_owned(), Arc::new(memtable_2))
    //         .unwrap();
    //
    //     let tables = catalog.table_names();
    //     assert_eq!(tables.len(), 2);
    //     assert!(tables.contains(&"tadashi".to_owned()));
    //     assert!(tables.contains(&"mizu".to_owned()));
    //
    //     let table = catalog.table("tadashi").await.unwrap();
    //     assert!(table.is_some());
    //     let table = table.unwrap();
    //     let table_ref = datafusion::common::TableReference::from("tadashi");
    // }
}
