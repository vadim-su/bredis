use apistos::ApiComponent;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
#[serde(untagged)]
pub enum IntOrString {
    Int(i64),
    String(String),
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct SetRequest {
    pub key: String,
    pub value: IntOrString,

    #[serde(default = "default_ttl")]
    pub ttl: i64,
}

const fn default_ttl() -> i64 {
    return -1;
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct DeleteKeysRequest {
    #[serde(default)]
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct GetResponse {
    pub value: Option<IntOrString>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct OperationSuccessResponse {
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct GetAllKeysResponse {
    pub keys: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
#[serde(untagged)]
pub enum ApiResponse<T: JsonSchema> {
    Success(T),
    ErrorResponse(ErrorResponse),
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct GetAllKeysQuery {
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct InfoResponse {
    pub version: String,
    pub rustc: String,
    pub build_date: String,
    pub backend: String,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct IncrementRequest {
    pub value: i64,
    #[serde(default)]
    pub default: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct IncrementResponse {
    pub value: i64,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct DecrementRequest {
    pub value: i64,
    #[serde(default)]
    pub default: i64,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct DecrementResponse {
    pub value: i64,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct GetTtlResponse {
    pub ttl: i64,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, ApiComponent)]
pub struct SetTtlRequest {
    pub ttl: i64,
}
