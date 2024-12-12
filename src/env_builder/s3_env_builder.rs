use crate::utils::error::PlaygroundError;

#[derive(Clone)]
pub struct S3FromEnv {
    pub region: String,
    pub bucket_name: String,
    pub access_key_id: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub allow_http: bool,
}

#[derive(Clone)]
pub struct S3FromEnvBuilder {
    pub region: Option<String>,
    pub bucket_name: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint: Option<String>,
    pub allow_http: Option<bool>,
}

impl Default for S3FromEnvBuilder {
    fn default() -> Self {
        Self {
            region: Some("us-west-1".to_string()),
            bucket_name: None,
            access_key_id: None,
            secret_key: None,
            endpoint: None,
            allow_http: Some(false),
        }
    }
}

impl S3FromEnvBuilder {
    pub fn new() -> Self {
        S3FromEnvBuilder::default()
    }
    
    pub fn set_region(mut self, region: String) -> Result<Self, PlaygroundError> {
        self.region = Some(region);
        
        Ok(self)
    }
    
    pub fn set_bucket_name(mut self, bucket_name: String) -> Result<Self, PlaygroundError> {
        self.bucket_name = Some(bucket_name);
        
        Ok(self)
    }
    
    pub fn set_access_key(mut self, access_key_id: String) -> Result<Self, PlaygroundError> {
        self.access_key_id = Some(access_key_id);
        
        Ok(self)
    }
    
    pub fn set_secret_key(mut self, secret_key: String) -> Result<Self, PlaygroundError> {
        self.secret_key = Some(secret_key);
        
        Ok(self)
    }
    
    pub fn set_endpoint(mut self, endpoint: String) -> Result<Self, PlaygroundError> {
        self.endpoint = Some(endpoint);
        
        Ok(self)
    }
    
    pub fn allow_http(mut self, allow_http: String) -> Result<Self, PlaygroundError> {
        match allow_http.as_str() {
            "true" => self.allow_http = Some(true),
            _ => self.allow_http = Some(false)
        }
        
        Ok(self)
    }
    
    pub fn build(&self) -> S3FromEnv {
        S3FromEnv {
            region: self.region.clone().expect("Region must be provided").to_string(),
            bucket_name: self.bucket_name.clone().expect("No bucket provided").to_string(),
            access_key_id: self.access_key_id.clone(),
            secret_key: self.secret_key.clone(),
            endpoint: self.endpoint.clone(),
            allow_http: self.allow_http.expect("Need to provied `true` or `false`"),
        }
    }
}