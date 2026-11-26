//! Preserve the real SDK's error boundaries through the outer throttle layer.
//! The transport injects error classes without a network or credential lookup.
//! Attempt counts are evidence; injected timeouts do not measure storage latency.
use super::*;
use lance_io::object_store::{
    throttle::{is_throttle_error, AimdThrottleConfig, AimdThrottledStore},
    StorageOptions,
};
use object_store::{
    azure::MicrosoftAzureBuilder,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService,
    },
    path::Path,
    ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug)]
enum Fault {
    Status(u16),
    Timeout,
}

#[derive(Clone, Debug)]
struct Transport {
    fault: Fault,
    create: bool,
    attempts: Arc<AtomicUsize>,
}

impl HttpConnector for Transport {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl HttpService for Transport {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        assert_eq!(
            request.method().as_str(),
            if self.create { "PUT" } else { "GET" }
        );
        if self.create {
            assert_eq!(request.headers().get("if-none-match").unwrap(), "*");
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        match self.fault {
            Fault::Timeout => Err(HttpError::new(
                HttpErrorKind::Timeout,
                std::io::Error::other("injected transport timeout"),
            )),
            Fault::Status(code) => {
                let mut reply =
                    HttpResponse::new(HttpResponseBody::from("injected response".to_owned()));
                *reply.status_mut() = code.try_into().unwrap();
                Ok(reply)
            }
        }
    }
}

async fn observe(fault: Fault, create: bool, wrapped: bool) -> Result<(usize, bool)> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let options = StorageOptions::default();
    assert_eq!(options.client_max_retries(), 3);
    assert_eq!(options.client_retry_timeout(), 180);
    let sdk = MicrosoftAzureBuilder::new()
        .with_account("account")
        .with_container_name("container")
        .with_skip_signature(true)
        .with_client_options(options.client_options()?)
        .with_retry(RetryConfig {
            max_retries: options.client_max_retries(),
            retry_timeout: Duration::from_secs(options.client_retry_timeout()),
            ..Default::default()
        })
        .with_http_connector(Transport {
            fault,
            create,
            attempts: attempts.clone(),
        })
        .build()?;
    let sdk: Arc<dyn ObjectStore> = Arc::new(sdk);
    let store: Arc<dyn ObjectStore> = if wrapped {
        Arc::new(AimdThrottledStore::new(sdk, AimdThrottleConfig::default())?)
    } else {
        sdk
    };
    let path = Path::from("owned/data/column.lance");
    let result = if create {
        store
            .put_opts(
                &path,
                Bytes::from_static(b"immutable column bytes").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
    } else {
        store.get(&path).await.map(|_| ())
    };
    let error = result.unwrap_err();
    let count = attempts.load(Ordering::SeqCst);
    let throttle = is_throttle_error(&error);
    println!(
        "{}",
        serde_json::json!({"fault":format!("{fault:?}"),"create":create,"outer_throttle":wrapped,"attempts":count,"classified_throttle":throttle,"error":error.to_string()})
    );
    Ok((count, throttle))
}

#[tokio::test]
async fn annotation_storage_retry_preserves_sdk_error_boundaries() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(45), async {
        // Keep native SDK retries for server/transport errors, but do not
        // restart an exhausted SDK loop without a real throttle response.
        for (fault, create, sdk_count, outer_count, throttle) in [
            (Fault::Status(500), false, 4, 4, false),
            (Fault::Timeout, false, 4, 4, false),
            (Fault::Status(429), false, 4, 16, true),
            (Fault::Status(503), false, 4, 16, true),
            (Fault::Status(403), false, 1, 1, false),
            (Fault::Status(500), true, 4, 4, false),
            (Fault::Timeout, true, 1, 1, false),
        ] {
            assert_eq!(observe(fault, create, false).await?, (sdk_count, throttle));
            assert_eq!(observe(fault, create, true).await?, (outer_count, throttle));
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
