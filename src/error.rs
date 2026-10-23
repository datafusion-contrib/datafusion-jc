use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum DatafusionJsonError {
    #[error("No TableProvider found for `{0}`")]
    NoTableProviderFound(String),
}
