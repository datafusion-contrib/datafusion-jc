use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum DatafusionJsonError {
    #[error("No TableProvider found for `{0}`")]
    NoTableProviderFound(String),

    #[error("No SchemaProvider found for `{0}`")]
    NoSchemaProviderFound(String),

    #[error("No SchemaProviders")]
    NoSchemaProvidersFound,

    #[error("DatafusinoJson error: `{0}`")]
    GenericError(String),
}
