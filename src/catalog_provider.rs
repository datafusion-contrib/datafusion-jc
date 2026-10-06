use crate::schema_provider::DatafusionJsonSchema;
use crate::serialization::{JsonData, SerializableSchemaProvider, SerializableTableAndSchema};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct DatafusionJsonCatalogInner {
    catalog_name: String,
    catalog_path: String,
    schema_providers: DashMap<String, Arc<DatafusionJsonSchema>>,
}

/// A DataFusion [`CatalogProvider`] whose catalog metadata and table schemas
/// can be serialized to and loaded from JSON.
#[derive(Debug)]
pub struct DatafusionJsonCatalog {
    inner: Arc<Mutex<DatafusionJsonCatalogInner>>,
}

impl DatafusionJsonCatalog {
    /// Creates an empty catalog with the given name and storage path.
    pub fn new(catalog_name: String, catalog_path: String) -> datafusion::common::Result<Self> {
        let inner = Arc::new(Mutex::new(DatafusionJsonCatalogInner {
            catalog_name,
            catalog_path,
            schema_providers: Default::default(),
        }));
        Ok(Self { inner })
    }

    /// Builds a catalog from JSON produced by [`Self::encode_json`].
    ///
    /// Currently only the catalog name and path are restored; schemas and
    /// tables in the JSON are not yet registered.
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

    /// Registers `table` under `name` in the schema named `schema_name`.
    ///
    /// Does nothing if no schema with that name is registered.
    pub fn register_table(
        &self,
        schema_name: &str,
        name: String,
        table: Arc<dyn TableProvider>,
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

    /// Serializes the catalog metadata and the Arrow schema of every
    /// registered table to a JSON string.
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

