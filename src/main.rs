use std::{collections::HashMap, sync::Arc};

use anyhow::{Ok, Result};
use datafusion::execution::{disk_manager::DiskManagerConfig, object_store as df_object_store, runtime_env::{RuntimeEnv, RuntimeEnvBuilder}};
use deltalake::{arrow::array::RecordBatch, DeltaOps};
use object_store::aws::AmazonS3Builder;

use datafusion::prelude::*;

const REGION: &str = "us-west-1";
const BUCKET_NAME: &str = "data";
const ACCESS_KEY_ID: &str = "";
const SECRET_KEY: &str = "";
const ENDPOINT: &str = "http://10.0.0.24:9000";
const ALLOW_HTTP: bool = true;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. **** OBJECT STORE CREATION ****//
    // Create the Object Store
    let s3_object_store = AmazonS3Builder::new()
        .with_region(REGION)
        .with_bucket_name(BUCKET_NAME)
        .with_access_key_id(ACCESS_KEY_ID)
        .with_secret_access_key(SECRET_KEY)
        .with_endpoint(ENDPOINT)
        .with_allow_http(ALLOW_HTTP)
        .build()
        .unwrap();

    // Path to data
    let bucket = df_object_store::ObjectStoreUrl::parse("s3://data/").unwrap();

    // 2. **** CONTEXT CREATION ****//
    // Create Session
    let dm_config = DiskManagerConfig::new();
    let rte = RuntimeEnvBuilder::new()
        .with_disk_manager(dm_config)
        .with_memory_limit(2_000_000_000, 0.75);
    let runtime = Arc::new(RuntimeEnv::try_new(rte)?);
    let config = SessionConfig::new();
    
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    // Register the object store
    ctx.register_object_store(bucket.as_ref(), Arc::new(s3_object_store.clone()));
    

    // Create the query for the table creation
    let table_create_query = r#"
            CREATE EXTERNAL TABLE yellow_taxi (
                vendor_id VARCHAR,
                tpep_pickup_datetime VARCHAR,
                tpep_dropoff_datetime VARCHAR,
                passenger_count TINYINT,
                trip_distance FLOAT,
                pu_location_id VARCHAR,
                do_location_id VARCHAR,
                ratecodeid VARCHAR,
                store_and_fwd_flag CHAR,
                payment_type TINYINT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                improvement_surcharge FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT,
            )
            STORED AS PARQUET 
            LOCATION 's3://data/nyc_taxi_data/taxi_data/yellow_taxi/'
        "#;
    // 3a. **** QUERY VIA SQL ****//
    // Create a table that has taxi data from 2020 and on
    ctx.sql(table_create_query).await?.show().await?;

    // Select All Yellow Taxi Data
    let query = r#"SELECT
        *
        FROM yellow_taxi
        "#;

    // Figuring this out took way to long and its nuts the
    // answer is buried in an issue from nearly a year ago
    deltalake::aws::register_handlers(None);

    // Create Storage Options for Delta Lake
    let mut storage_options: HashMap<String, String> = HashMap::new();

    // Provide the Minio credentials
    storage_options.insert("AWS_REGION".to_string(), REGION.to_owned());
    storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), ACCESS_KEY_ID.to_owned());
    storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), SECRET_KEY.to_owned());
    storage_options.insert("ALLOW_HTTP".to_string(), ALLOW_HTTP.to_string());
    storage_options.insert("ENDPOINT".to_string(), ENDPOINT.to_string());
    storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());

    // Path to the delta table
    let delta_path = "s3://data/nyc_taxi_data/taxi_data/delta/yellow_taxi/";

    // Create the plan
    let df = ctx.sql(query).await?.repartition(Partitioning::RoundRobinBatch(16))?;

    // Execute the plan into a stream
    let record_batch: Vec<RecordBatch> = df.collect().await?; //.collect().await?;
    let ops =
        DeltaOps::try_from_uri_with_storage_options(delta_path, storage_options.clone()).await?;

    let _table = ops.write(record_batch);

    Ok(())
}
