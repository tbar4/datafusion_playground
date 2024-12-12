use std::path::PathBuf;
use std::{collections::HashMap, sync::Arc};

use std::result::Result;

use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::{object_store as df_object_store, runtime_env::RuntimeEnv};

use deltalake::{arrow::array::RecordBatch, DeltaOps};
use object_store::aws::AmazonS3Builder;

use datafusion::prelude::*;
use dotenv;

mod utils;
use utils::error::PlaygroundError;

mod env_builder;
use env_builder::s3_env_builder::*;

const MEMORY_LIMIT: usize = 16 * 1024 * 1024 * 1024; // 8GB

#[tokio::main]
async fn main() -> Result<(), PlaygroundError> {
    // 0. **** Load S3/Minio Credentials ****//
    // Create the Object Store
    let region = dotenv::var("REGION").unwrap();
    let bucket = dotenv::var("BUCKET_NAME").unwrap().clone();
    let access_key = dotenv::var("ACCESS_KEY_ID").unwrap().clone();
    let secret_key = dotenv::var("SECRET_KEY").unwrap().clone();
    let endpoint = dotenv::var("ENDPOINT").unwrap().clone();
    let allow_http = dotenv::var("ALLOW_HTTP").unwrap().clone();
    
    let vars = S3FromEnvBuilder::new()
        .set_region(region)?
        .set_bucket_name(bucket)?
        .set_access_key(access_key)?
        .set_secret_key(secret_key)?
        .set_endpoint(endpoint)?
        .allow_http(allow_http)?
        .build();
    
    // 1. **** OBJECT STORE CREATION ****//
    // Create the Object Store
    
    let s3_object_store = AmazonS3Builder::new()
        .with_region(vars.region.clone())
        .with_bucket_name(vars.bucket_name)
        .with_access_key_id(vars.access_key_id.clone().unwrap())
        .with_secret_access_key(vars.secret_key.clone().unwrap())
        .with_endpoint(vars.endpoint.clone().unwrap())
        .with_allow_http(vars.allow_http)
        .build()
        .unwrap();
    
    // Path to data
    let bucket = df_object_store::ObjectStoreUrl::parse("s3://data/").unwrap();

    // 2. **** CONTEXT CREATION ****//
    // Create MemoryPool
    let memory_pool = Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(MEMORY_LIMIT));
    
    // Configure Runtime to use memory pool
    let rt_config = RuntimeConfig::new()
        .with_memory_pool(memory_pool)
        .with_temp_file_path(PathBuf::from("./tmp"));
    
    // Create the Runtime
    let runtime = Arc::new(RuntimeEnv::try_new(rt_config)?);
    
    // Cerate the default Session Config
    let config = SessionConfig::new();
    
    // Create the Context with config and runtime
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
    
    // 3. **** QUERY VIA SQL ****//
    // Create a table that has taxi data from 2020 and on
    ctx.sql(table_create_query).await?.show().await?;

    // Select All Yellow Taxi Data
    let query = r#"SELECT
        *
        FROM yellow_taxi
        "#;

    // 4. **** Create the Delta Table ****//
    // Figuring this out took way to long and its nuts the
    // answer is buried in an issue from nearly a year ago
    deltalake::aws::register_handlers(None);

    // Create Storage Options for Delta Lake
    let mut storage_options: HashMap<String, String> = HashMap::new();

    // Provide the Minio credentials
    storage_options.insert("AWS_REGION".to_string(), vars.region.to_string());
    storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), vars.access_key_id.unwrap().to_string());
    storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), vars.secret_key.unwrap().to_string());
    storage_options.insert("ALLOW_HTTP".to_string(), vars.allow_http.to_string());
    storage_options.insert("ENDPOINT".to_string(), vars.endpoint.unwrap().to_string());
    storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
    

    // Path to the delta table
    let delta_path = "s3://data/nyc_taxi_data/taxi_data/delta/yellow_taxi/";

    // Create the plan
    let df = ctx.sql(query).await?;

    // Execute the plan into a stream
    let record_batch: Vec<RecordBatch> = df.collect().await?;
    let ops =
        DeltaOps::try_from_uri_with_storage_options(delta_path, storage_options.clone()).await?;

    let _table = ops.write(record_batch);

    Ok(())
}
