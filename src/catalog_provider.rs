use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProviderFactory};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::datasource::TableProvider;
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionState;
use datafusion::logical_expr::CreateExternalTable;
use std::sync::{Arc, Mutex};

use crate::error::DatafusionJsonError;
use crate::schema_provider::DatafusionJsonSchema;
use crate::serialization::{JsonData, SerializableSchemaProvider, SerializableTableAndSchema};

#[derive(Debug)]
struct DatafusionJsonCatalogInner {
    catalog_name: String,
    catalog_path: String,
    schema_providers: DashMap<String, Arc<DatafusionJsonSchema>>,
    #[allow(dead_code)]
    session_state: SessionState,
}

/// A DataFusion [`CatalogProvider`] whose catalog metadata and table schemas
/// can be serialized to and loaded from JSON.
#[derive(Debug)]
pub struct DatafusionJsonCatalog {
    inner: Arc<Mutex<DatafusionJsonCatalogInner>>,
}

impl DatafusionJsonCatalog {
    /// Creates an empty catalog with the given name and storage path.
    pub fn new(
        catalog_name: String,
        catalog_path: String,
        session_state: SessionState,
    ) -> datafusion::common::Result<Self> {
        let inner = Arc::new(Mutex::new(DatafusionJsonCatalogInner {
            catalog_name,
            catalog_path,
            schema_providers: Default::default(),
            session_state,
        }));
        Ok(Self { inner })
    }

    /// Builds a catalog from JSON produced by [`Self::encode_json`].
    pub async fn from_json(
        json: &str,
        session_state: SessionState,
    ) -> datafusion::common::Result<Self> {
        let json_data: JsonData = serde_json::from_str(json).unwrap();
        let inner = Arc::new(Mutex::new(DatafusionJsonCatalogInner {
            catalog_name: json_data.catalog_metadata.name,
            catalog_path: json_data.catalog_metadata.path,
            schema_providers: Default::default(),
            session_state: session_state.clone(),
        }));

        // TODO: Instead of serializing the entire table provider I need to make it so we can just
        // rebuild the table_provider based on the data serialized in the JSON.
        for schema_provider in json_data.schema_providers {
            let schema_provider_ref =
                Arc::new(DatafusionJsonSchema::new(schema_provider.schema_name));
            // TODO: Fix me!
            let table_factory = DefaultTableFactory::default();
            for table in schema_provider.tables {
                let table_builder = CreateExternalTable::builder(
                    table.table_name.clone(),
                    String::from("foo"),
                    String::from("parquet"),
                    Arc::new(DFSchema::try_from_qualified_schema(
                        table.table_name.clone(),
                        table.schema.as_ref(),
                    )?),
                )
                .build();
                let table_provider = table_factory.create(&session_state, &table_builder).await?;
                schema_provider_ref.register_table(table.table_name, table_provider.clone())?;
            }
            inner
                .lock()
                .unwrap()
                .schema_providers
                .insert(schema_provider_ref.name(), schema_provider_ref);
        }

        Ok(Self { inner })
    }

    /// Serializes the catalog metadata and the Arrow schema of every
    /// registered table to a JSON string.
    pub async fn encode_json(&self) -> datafusion::common::Result<String> {
        match self.inner.lock() {
            Ok(inner) => {
                let schema_providers = self
                    .get_serializable_schemas(&inner.schema_providers)
                    .map_err(|err| DataFusionError::Execution(err.to_string()))?;
                let data = JsonData::new(
                    inner.catalog_name.clone(),
                    inner.catalog_path.clone(),
                    schema_providers,
                );
                let output_json = serde_json::to_string(&data).unwrap();

                Ok(output_json)
            }
            Err(err) => Err(DataFusionError::Execution(err.to_string())),
        }
    }

    /// Registers `table` under `name` in the schema named `schema_name`.
    pub fn register_table(
        &self,
        schema_name: &str,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<()> {
        let inner = self.inner.lock().unwrap();
        match inner.schema_providers.get_mut(schema_name) {
            None => Err(DataFusionError::Plan(format!(
                "Schema '{}' not found",
                schema_name
            ))),
            Some(schema_provider) => {
                schema_provider.register_table(name, table)?;
                Ok(())
            }
        }
    }

    fn get_serializable_schemas(
        &self,
        schema_providers: &DashMap<String, Arc<DatafusionJsonSchema>>,
    ) -> Result<Vec<SerializableSchemaProvider>, DatafusionJsonError> {
        let mut serializable_schemas = vec![];
        for entry in schema_providers.iter() {
            let schema_provider = entry.value();
            let tables = schema_provider
                .table_names()
                .iter()
                .map(|table| {
                    schema_provider
                        .get_serializable_table(table.clone())
                        .unwrap()
                })
                .collect::<Vec<SerializableTableAndSchema>>();
            let name = schema_provider.name();
            serializable_schemas.push(SerializableSchemaProvider {
                schema_name: name,
                tables,
            });
        }

        Ok(serializable_schemas)
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
            .map(|r| r.key().clone())
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
        let schema_provider = Arc::new(DatafusionJsonSchema::new(name.to_string()));
        inner
            .schema_providers
            .insert(name.to_string(), schema_provider);

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
