//! Exercise the actual Azure SDK's request batching and response parser.
use super::*;
use object_store::{
    azure::MicrosoftAzureBuilder,
    client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody,
        HttpService,
    },
    ClientOptions, RetryConfig,
};
use std::sync::Mutex;

#[derive(Clone, Debug)]
struct Transport {
    failure: u8,
    requests: Arc<Mutex<Vec<usize>>>,
}

impl HttpConnector for Transport {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl HttpService for Transport {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        assert_eq!(request.method().as_str(), "POST");
        assert!(request
            .uri()
            .query()
            .unwrap()
            .split('&')
            .any(|part| part == "comp=batch"));
        let body = HttpResponseBody::new(request.into_body()).bytes().await?;
        let body = std::str::from_utf8(&body).unwrap();
        let count = body.matches("DELETE ").count();
        assert_eq!(
            count, 256,
            "collector split a sealed batch into individual calls"
        );
        for index in 0..256 {
            assert!(body.contains(&format!("/container/owned/data/{index:04}.lance ")));
        }
        self.requests.lock().unwrap().push(count);
        let mut reply = String::new();
        if self.failure == 2 {
            reply.push_str("<Error><Code>ServerBusy</Code><Message>injected whole batch failure</Message></Error>");
        } else {
            for index in 0..count {
                let status = if index == 0 {
                    "404 Not Found"
                } else if self.failure == 1 && index == 1 {
                    "403 Forbidden"
                } else if self.failure == 3 && index == 1 {
                    "503 Service Unavailable"
                } else {
                    "202 Accepted"
                };
                reply.push_str(&format!("--response\r\nContent-Type: application/http\r\nContent-ID: {index}\r\n\r\nHTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n\r\n"));
            }
            reply.push_str("--response--\r\n");
        }
        let mut response = HttpResponse::new(HttpResponseBody::from(reply));
        *response.status_mut() = if self.failure == 2 { 503 } else { 202 }
            .try_into()
            .unwrap();
        response.headers_mut().insert(
            "content-type",
            "multipart/mixed; boundary=response".parse().unwrap(),
        );
        Ok(response)
    }
}

#[tokio::test]
async fn azure_bulk_collection_keeps_per_path_and_whole_request_failures() {
    for failure in 0..=3 {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let store = MicrosoftAzureBuilder::new()
            .with_account("account")
            .with_container_name("container")
            .with_skip_signature(true)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .with_http_connector(Transport {
                failure,
                requests: requests.clone(),
            })
            .build()
            .unwrap();
        let batch = (0..256)
            .map(|index| {
                (
                    Path::from(format!("owned/data/{index:04}.lance")),
                    100 + index,
                )
            })
            .collect::<Vec<_>>();
        let outcomes = delete_batch(Arc::new(store), &batch).await;
        assert_eq!(*requests.lock().unwrap(), vec![256]);
        if failure == 2 {
            assert_eq!(outcomes.len(), 1);
            assert!(
                outcomes[0].is_err(),
                "an unknown whole-batch result must fail collection"
            );
        } else {
            assert_eq!(outcomes.len(), 256);
            assert_eq!(
                outcomes[0].as_ref().unwrap(),
                &100,
                "an already absent path is reclaimed"
            );
            assert_eq!(
                outcomes.iter().filter(|r| r.is_err()).count(),
                usize::from(failure != 0)
            );
            for (index, outcome) in outcomes.iter().enumerate().skip(2) {
                assert_eq!(*outcome.as_ref().unwrap(), 100 + index as u64);
            }
        }
    }
}
