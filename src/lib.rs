use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use gcp_auth::{Token, TokenProvider};
use google::datastore::v1::{
    commit_request::Mode, datastore_client::DatastoreClient, mutation::Operation, CommitRequest,
    CommitResponse, Entity, Key, Mutation,
};
use tonic::{
    metadata::MetadataValue,
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig},
    Request, Status,
};

const AUTH_SCOPE: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
const DOMAIN_NAME: &str = "datastore.googleapis.com";
const HTTP_ENDPOINT: &str = "https://datastore.googleapis.com";

pub mod google {
    #[path = ""]
    pub mod datastore {
        #[path = "google.datastore.v1.rs"]
        pub mod v1;
    }

    #[path = "google.api.rs"]
    pub mod api;

    #[path = "google.r#type.rs"]
    pub mod r#type;
}

#[derive(Debug)]
pub struct TryFromEntityError;
impl Error for TryFromEntityError {}
impl Display for TryFromEntityError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Failed to convert entity to struct")
    }
}

pub trait TryFromEntity: Sized {
    fn try_from_entity(entity: Entity) -> Result<Self, TryFromEntityError>;
}

#[derive(Clone)]
struct TokenInterceptor {
    token_provider: Arc<dyn TokenProvider>,
}

impl Interceptor for TokenInterceptor {
    // The `call` method is called for each request. We use it inject a bearer token into the request
    // metadata. Since the `call` method is synchronous, use `futures::executor::block_on` to run.
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        // Retrieve a token from the token provider, using the async runtime.
        let token: Result<Arc<Token>, Status> = futures::executor::block_on(async {
            let token =
                self.token_provider.token(AUTH_SCOPE).await.map_err(|e| {
                    Status::internal(format!("Failed to get token: {}", e.to_string()))
                })?;
            Ok(token)
        });

        // Propagate any errors that occurred while retrieving the token.
        let token = token?;

        // Transform the token into a request metadata value.
        let bearer_token = format!("Bearer {}", token.as_str());
        let header_value: MetadataValue<_> = bearer_token
            .parse()
            .map_err(|_| Status::internal(format!("Failed to parse token")))?;

        // Insert the token into the request metadata.
        request.metadata_mut().insert("authorization", header_value);
        Ok(request)
    }
}

///
/// Wrapper around the Datastore API.
///
#[derive(Clone, Debug)]
pub struct Datastore {
    project_id: String,
    service: DatastoreClient<InterceptedService<Channel, TokenInterceptor>>,
}

impl Datastore {
    ///
    /// Create a new Datastore instance.
    ///
    pub async fn new(
        project_id: String,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, Box<dyn Error>> {
        let tls_config = ClientTlsConfig::new()
            .with_native_roots()
            .domain_name(DOMAIN_NAME);

        let channel = Channel::from_shared(HTTP_ENDPOINT)?
            .tls_config(tls_config)?
            .connect()
            .await?;

        let interceptor = TokenInterceptor { token_provider };

        let service = DatastoreClient::with_interceptor(channel, interceptor);

        let datastore = Datastore {
            project_id,
            service,
        };

        Ok(datastore)
    }

    ///
    /// Upsert an entity.
    ///
    pub async fn upsert_entity(
        &mut self,
        entity: impl Into<Entity>,
    ) -> Result<CommitResponse, Box<dyn Error>> {
        let entity = entity.into();

        let request = CommitRequest {
            project_id: self.project_id.clone(),
            database_id: "".to_string(), // use empty string '' to refer the default database.
            mode: Mode::NonTransactional as i32,
            mutations: vec![Mutation {
                operation: Some(Operation::Upsert(entity)),
                ..Default::default()
            }],
            ..Default::default()
        };

        Ok(self.service.commit(request).await?.into_inner())
    }

    ///
    /// Load an entity.
    ///
    pub async fn lookup_entity<T: TryFromEntity>(
        &mut self,
        key: impl Into<Key>,
    ) -> Result<Option<T>, Box<dyn Error>> {
        let key = key.into();

        let request = google::datastore::v1::LookupRequest {
            project_id: self.project_id.clone(),
            keys: vec![key],
            ..Default::default()
        };

        let response = self.service.lookup(request).await?.into_inner();

        let result = response.found.into_iter().next().unwrap().entity;
        let result = result.map(|e| T::try_from_entity(e)).transpose();

        match result {
            Ok(Some(entity)) => Ok(Some(entity)),
            Ok(None) => Ok(None),
            Err(e) => Err(Box::new(e)),
        }
    }
}
