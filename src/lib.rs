mod error;

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use error::CloudDatastoreError;
use gcp_auth::{Token, TokenProvider};
use google::datastore::v1::{
    commit_request::Mode,
    datastore_client::DatastoreClient,
    key::{path_element::IdType, PathElement},
    mutation::Operation,
    value::ValueType,
    ArrayValue, CommitRequest, CommitResponse, Entity, Key, Mutation, RunQueryRequest,
    RunQueryResponse, Value,
};
use tonic::{
    metadata::MetadataValue,
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig},
    Request, Status,
};

const AUTH_SCOPE: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
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
pub enum TryFromEntityError {
    KeyError(KeyError),
    EntityValueError(EntityValueError),
    Other(String),
}

impl Error for TryFromEntityError {}
impl Display for TryFromEntityError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Failed to convert entity to struct")
    }
}

impl From<KeyError> for TryFromEntityError {
    fn from(e: KeyError) -> Self {
        TryFromEntityError::KeyError(e)
    }
}

impl From<EntityValueError> for TryFromEntityError {
    fn from(e: EntityValueError) -> Self {
        TryFromEntityError::EntityValueError(e)
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
    ) -> Result<Self, CloudDatastoreError> {
        let tls_config = ClientTlsConfig::new().with_native_roots();

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
    ) -> Result<CommitResponse, CloudDatastoreError> {
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
    ) -> Result<Option<T>, CloudDatastoreError> {
        let key = key.into();

        let request = google::datastore::v1::LookupRequest {
            project_id: self.project_id.clone(),
            keys: vec![key],
            ..Default::default()
        };

        let response = self.service.lookup(request).await?.into_inner();

        let Some(result) = response.found.into_iter().next() else {
            return Ok(None);
        };

        let result = result.entity.map(|e| T::try_from_entity(e)).transpose();

        match result {
            Ok(Some(entity)) => Ok(Some(entity)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Run a query. The provided query has the project_id set to the project_id of the Datastore instance.
    /// The query is specified in the `RunQueryRequest` parameter.
    /// The result is returned as a `RunQueryResponse`.
    pub async fn run_query(
        &mut self,
        mut request: RunQueryRequest,
    ) -> Result<RunQueryResponse, CloudDatastoreError> {
        request.project_id = self.project_id.clone();
        Ok(self.service.run_query(request).await?.into_inner())
    }
}

/// Builder for creating an entity.
pub struct EntityBuilder {
    entity: Entity,
}

impl EntityBuilder {
    pub fn new() -> Self {
        EntityBuilder {
            entity: Default::default(),
        }
    }

    /// Set the key of the entity.
    pub fn with_key_name<T: Into<String>>(self, kind: T, name: T) -> Self {
        let key = Key {
            path: vec![PathElement {
                kind: kind.into(),
                id_type: Some(IdType::Name(name.into())),
            }],
            ..Default::default()
        };
        self.with_key(key)
    }

    /// Set the key of the entity.
    pub fn with_key(mut self, key: Key) -> Self {
        self.entity.key = Some(key);
        self
    }

    /// Add a string property to the entity.
    pub fn add_string<T: Into<String>>(self, name: T, value: T, indexed: bool) -> Self {
        self.opt_string(name, Some(value), indexed)
    }

    /// Add an optional string property to the entity.
    pub fn opt_string<T: Into<String>>(mut self, name: T, value: Option<T>, indexed: bool) -> Self {
        let Some(value) = value else {
            return self;
        };

        self.entity.properties.insert(
            name.into(),
            Value {
                exclude_from_indexes: !indexed,
                value_type: Some(ValueType::StringValue(value.into())),
                ..Default::default()
            },
        );
        self
    }

    /// Add an integer property to the entity.
    pub fn add_string_array<T: Into<String>>(mut self, name: T, values: Vec<String>) -> Self {
        self.entity.properties.insert(
            name.into(),
            Value {
                value_type: Some(ValueType::ArrayValue(ArrayValue {
                    values: values
                        .into_iter()
                        .map(|v| {
                            let v: String = v.into();
                            Value {
                                value_type: Some(ValueType::StringValue(v)),
                                ..Default::default()
                            }
                        })
                        .collect(),
                })),
                ..Default::default()
            },
        );
        self
    }

    /// Builds the entity.
    pub fn build(self) -> Entity {
        self.entity
    }
}

#[derive(Debug)]
pub struct EntityValueError(String);
impl Error for EntityValueError {}
impl Display for EntityValueError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Entity {
    pub fn builder() -> EntityBuilder {
        EntityBuilder::new()
    }

    pub fn req_key(&self, kind: &str) -> Result<&Key, EntityValueError> {
        let key = self
            .key
            .as_ref()
            .ok_or(EntityValueError(format!("Missing Key")))?;

        let key_kind = key.kind().map_err(|e| EntityValueError(e.to_string()))?;

        if key_kind == kind {
            Ok(key)
        } else {
            return Err(EntityValueError(format!(
                "Invalid Key Kind. Expected '{}'.",
                kind,
            )));
        }
    }

    pub fn req_string(&self, name: &str) -> Result<String, EntityValueError> {
        self.opt_string(name)
            .and_then(|v| v.ok_or(EntityValueError("missing required field".to_string())))
    }

    pub fn opt_string(&self, name: &str) -> Result<Option<String>, EntityValueError> {
        let value_type = self.properties.get(name).and_then(|v| v.value_type.clone());

        let Some(value_type) = value_type else {
            return Ok(None);
        };

        match value_type {
            ValueType::StringValue(s) => Ok(Some(s.clone())),
            _ => Err(EntityValueError(format!("Field {} is not a string", name))),
        }
    }

    pub fn opt_string_array(&self, name: &str) -> Result<Option<Vec<String>>, EntityValueError> {
        let value_type = self.properties.get(name).and_then(|v| v.value_type.clone());

        let Some(value_type) = value_type else {
            return Ok(None);
        };

        match value_type {
            ValueType::ArrayValue(array) => Ok(Some(
                array
                    .values
                    .iter()
                    .map(|v| match &v.value_type {
                        Some(ValueType::StringValue(s)) => Ok(s.clone()),
                        _ => Err(EntityValueError(format!("Field {} is not a string", name))),
                    })
                    .collect::<Result<Vec<String>, EntityValueError>>()?,
            )),
            _ => Err(EntityValueError(format!("Field {} is not an array", name))),
        }
    }

    pub fn req_string_array(&self, name: &str) -> Result<Vec<String>, EntityValueError> {
        self.opt_string_array(name)
            .and_then(|v| v.ok_or(EntityValueError("missing required field".to_string())))
    }
}

#[derive(Debug)]
pub struct KeyError(String);

impl Error for KeyError {}

impl Display for KeyError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Key {
    pub fn kind(&self) -> Result<&str, KeyError> {
        if self.path.is_empty() {
            return Err(KeyError("Key has no path".to_string()));
        }

        Ok(&self.path[0].kind)
    }

    pub fn name(&self) -> Result<&str, KeyError> {
        if self.path.is_empty() {
            return Err(KeyError("Key has no path".to_string()));
        }

        let id_type = self.path[0]
            .id_type
            .as_ref()
            .ok_or(KeyError("Key has no name".to_string()))?;

        match id_type {
            IdType::Name(name) => Ok(name),
            _ => Err(KeyError("Key has no name".to_string())),
        }
    }
}
