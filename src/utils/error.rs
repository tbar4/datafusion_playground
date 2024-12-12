use deltalake::DeltaTableError;
use datafusion::error::DataFusionError;
use thiserror::Error;

use std::fmt;

#[derive(Debug)]
pub struct InputError;

impl fmt::Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Incorrect Input Type")
    }
}

#[derive(Error, Debug)]
pub enum PlaygroundError {
    #[error("Incorrect Input Error")]
    IncorrectType(InputError),
    #[error("DataFusion Error")]
    DataFusionError(#[from] DataFusionError),
    #[error("Delta Table Error")]
    DeltaTableError(#[from] DeltaTableError),
}