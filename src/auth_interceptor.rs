use std::sync::Arc;

use futures::future::BoxFuture;
use gcp_auth::TokenProvider;
use http::Request;
use tonic::body::BoxBody;
use tower::Service;

const HEADER_AUTHORIZATION: &str = "authorization";
const HEADER_REQUEST_PARAMS: &str = "x-goog-request-params";
const AUTH_SCOPE: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

#[derive(Clone)]
pub struct AuthInterceptor<I> {
    inner: I,
    token_provider: Arc<dyn TokenProvider>,
    request_params: String,
}

impl<I> AuthInterceptor<I> {
    pub fn new(
        inner: I,
        project_id: &str,
        database_id: Option<&str>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        let request_params = match database_id {
            Some(database_id) => format!("project_id={}&database_id={}", project_id, database_id),
            None => format!("project_id={}", project_id),
        };
        AuthInterceptor {
            inner,
            token_provider,
            request_params,
        }
    }
}

impl<I> Service<Request<BoxBody>> for AuthInterceptor<I>
where
    I: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>> + Send + Clone + 'static,
    I::Future: Send + 'static,
{
    type Response = I::Response;
    type Error = I::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<BoxBody>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let token_provider = self.token_provider.clone();
        let request_params = self.request_params.clone();
        Box::pin(async move {
            let token = token_provider.token(AUTH_SCOPE).await.unwrap();
            req.headers_mut().insert(
                HEADER_AUTHORIZATION,
                format!("Bearer {}", token.as_str()).parse().unwrap(),
            );

            req.headers_mut()
                .insert(HEADER_REQUEST_PARAMS, request_params.parse().unwrap());
            let response = inner.call(req).await?;
            Ok(response)
        })
    }
}
