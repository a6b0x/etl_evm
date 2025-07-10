use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;

pub struct Tsdb {
    pub httpclient: Client,
    pub url: String,
}

impl Tsdb {
    pub fn new(url: &str, auth_token: &str) -> Self {
        let mut headers = HeaderMap::new();
        let auth_header_name =
            HeaderName::from_bytes(b"Authorization").expect("Invalid header name");
        let auth_header_value =
            HeaderValue::from_str(&format!("Bearer {}", auth_token)).expect("Invalid header value");
        headers.insert(auth_header_name, auth_header_value);

        Self {
            httpclient: Client::builder().default_headers(headers)
                .build()
                .unwrap(),
            url: url.to_string(),
        }
    }
    
    pub async fn query(&self, database: &str, sql: &str) -> Result<String, reqwest::Error> {
        let params = [("db", database), ("q", sql)];
        let response = self.httpclient.get(&self.url)
            .query(&params)
            .send()
            .await?
            .text()
            .await?;
        Ok(response)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        extract::RpcClient, extract::UniswapV2, init::AppConfig, transform::transform_block,
        transform::transform_burn_event, transform::transform_mint_event,
        transform::transform_pair_created_event, transform::transform_swap_event,
    };
    use alloy::primitives::{address, Address};
    use eyre::{Ok, Result};
    use log::info;

    #[tokio::test]
    async fn test_Tsdb() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let auth_token = "apiv3_di3lJBckgHFT2cJc5VLkKwsWsVEwI3XZsefjifwwLNR8kruGfhazhZ3tGBvIPZIquaFlbnqHJgTDdaLUFgIzrw";
        let url = "http://tsdb:8181/api/v3/query_sql";
        let database = "evm";
        let sql = "SELECT * FROM uniswap_v2";

        let tsdb = Tsdb::new(url, auth_token);
        let response = tsdb.query(database,sql).await.unwrap();
        info!("response: {:#?}", response);
    }

}
