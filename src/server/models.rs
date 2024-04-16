use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteKeysRequest {
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetResponse {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationSuccessResponse {
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetAllKeysResponse {
    pub keys: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ApiResponse<T> {
    Success(T),
    ErrorResponse(ErrorResponse),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetAllKeysQuery {
    pub prefix: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InfoResponse {
    pub version: String,
    pub rustc: String,
}
