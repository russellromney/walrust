//! Python bindings for walrust

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::sync;

/// Python wrapper for walrust operations
#[pyclass]
pub struct Walrust {
    runtime: Arc<Runtime>,
    bucket: String,
    endpoint: Option<String>,
}

/// Database info from listing
#[pyclass]
#[derive(Clone)]
pub struct DatabaseInfo {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub snapshot_count: usize,
    #[pyo3(get)]
    pub wal_count: usize,
}

#[pymethods]
impl Walrust {
    /// Create a new Walrust instance
    ///
    /// Args:
    ///     bucket: S3 bucket (e.g., "s3://my-bucket/prefix" or "my-bucket")
    ///     endpoint: Optional S3 endpoint URL (for Tigris/MinIO)
    #[new]
    #[pyo3(signature = (bucket, endpoint=None))]
    fn new(bucket: &str, endpoint: Option<&str>) -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        Ok(Walrust {
            runtime: Arc::new(runtime),
            bucket: bucket.to_string(),
            endpoint: endpoint.map(|s| s.to_string()),
        })
    }

    /// Take a snapshot of a database and upload to S3
    ///
    /// Args:
    ///     database: Path to the SQLite database file
    fn snapshot(&self, database: &str) -> PyResult<()> {
        let bucket = self.bucket.clone();
        let endpoint = self.endpoint.clone();
        let database = PathBuf::from(database);

        self.runtime
            .block_on(async move { sync::snapshot(&database, &bucket, endpoint.as_deref()).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Snapshot failed: {}", e)))
    }

    /// Restore a database from S3
    ///
    /// Args:
    ///     name: Database name (as stored in S3)
    ///     output: Output path for the restored database
    ///     point_in_time: Optional TXID/sequence number (as a string, e.g. "42")
    ///         to restore through. Timestamp-based PITR is not implemented;
    ///         object keys carry only TXID/sequence range, not wall-clock time.
    #[pyo3(signature = (name, output, point_in_time=None))]
    fn restore(&self, name: &str, output: &str, point_in_time: Option<&str>) -> PyResult<()> {
        let bucket = self.bucket.clone();
        let endpoint = self.endpoint.clone();
        let name = name.to_string();
        let output = PathBuf::from(output);
        let pit = point_in_time.map(|s| s.to_string());

        self.runtime
            .block_on(async move {
                sync::restore(
                    &name,
                    &output,
                    &bucket,
                    endpoint.as_deref(),
                    pit.as_deref(),
                    None,
                    None,
                )
                .await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Restore failed: {}", e)))
    }

    /// List databases in the S3 bucket
    fn list(&self) -> PyResult<Vec<String>> {
        let bucket = self.bucket.clone();
        let endpoint = self.endpoint.clone();

        self.runtime
            .block_on(async move {
                // Get the list output as database names
                let (bucket_name, prefix) = crate::s3::parse_bucket(&bucket);
                let client = crate::s3::create_client(endpoint.as_deref()).await?;

                let objects = crate::s3::list_objects(&client, &bucket_name, &prefix).await?;

                // Extract unique database names
                let mut dbs: std::collections::HashSet<String> = std::collections::HashSet::new();
                for key in &objects {
                    if let Some(rest) = key.strip_prefix(&prefix) {
                        if let Some(name) = rest.split('/').next() {
                            if !name.is_empty() {
                                dbs.insert(name.to_string());
                            }
                        }
                    }
                }

                Ok(dbs.into_iter().collect())
            })
            .map_err(|e: anyhow::Error| PyRuntimeError::new_err(format!("List failed: {}", e)))
    }
}

/// Convenience function to take a snapshot
#[pyfunction]
#[pyo3(signature = (database, bucket, endpoint=None))]
fn snapshot(database: &str, bucket: &str, endpoint: Option<&str>) -> PyResult<()> {
    let runtime = Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

    let database = PathBuf::from(database);
    let bucket = bucket.to_string();
    let endpoint = endpoint.map(|s| s.to_string());

    runtime
        .block_on(async move { sync::snapshot(&database, &bucket, endpoint.as_deref()).await })
        .map_err(|e| PyRuntimeError::new_err(format!("Snapshot failed: {}", e)))
}

/// Convenience function to restore a database
#[pyfunction]
#[pyo3(signature = (name, output, bucket, endpoint=None, point_in_time=None))]
fn restore(
    name: &str,
    output: &str,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
) -> PyResult<()> {
    let runtime = Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

    let name = name.to_string();
    let output = PathBuf::from(output);
    let bucket = bucket.to_string();
    let endpoint = endpoint.map(|s| s.to_string());
    let pit = point_in_time.map(|s| s.to_string());

    runtime
        .block_on(async move {
            sync::restore(
                &name,
                &output,
                &bucket,
                endpoint.as_deref(),
                pit.as_deref(),
                None,
                None,
            )
            .await
        })
        .map_err(|e| PyRuntimeError::new_err(format!("Restore failed: {}", e)))
}

/// Convenience function to list databases
#[pyfunction]
#[pyo3(signature = (bucket, endpoint=None))]
fn list_databases(bucket: &str, endpoint: Option<&str>) -> PyResult<Vec<String>> {
    let runtime = Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

    let bucket = bucket.to_string();
    let endpoint = endpoint.map(|s| s.to_string());

    runtime
        .block_on(async move {
            let (bucket_name, prefix) = crate::s3::parse_bucket(&bucket);
            let client = crate::s3::create_client(endpoint.as_deref()).await?;

            let objects = crate::s3::list_objects(&client, &bucket_name, &prefix).await?;

            let mut dbs: std::collections::HashSet<String> = std::collections::HashSet::new();
            for key in &objects {
                if let Some(rest) = key.strip_prefix(&prefix) {
                    if let Some(name) = rest.split('/').next() {
                        if !name.is_empty() {
                            dbs.insert(name.to_string());
                        }
                    }
                }
            }

            Ok(dbs.into_iter().collect())
        })
        .map_err(|e: anyhow::Error| PyRuntimeError::new_err(format!("List failed: {}", e)))
}

/// Python module
#[pymodule]
fn walrust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Walrust>()?;
    m.add_class::<DatabaseInfo>()?;
    m.add_function(wrap_pyfunction!(snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(restore, m)?)?;
    m.add_function(wrap_pyfunction!(list_databases, m)?)?;
    Ok(())
}
