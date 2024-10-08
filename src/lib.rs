mod auth_interceptor;
mod error;

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use auth_interceptor::AuthInterceptor;
pub use error::CloudDatastoreError;
use gcp_auth::TokenProvider;
use google::datastore::v1::{
    commit_request::{Mode as CommitMode, TransactionSelector},
    datastore_client::DatastoreClient,
    key::{path_element::IdType, PathElement},
    mutation::Operation,
    run_query_request::QueryType,
    transaction_options::Mode as TransactionMode,
    value::ValueType,
    ArrayValue, CommitRequest, CommitResponse, Entity, Key, KindExpression, Mutation, Query,
    RunQueryRequest, RunQueryResponse, TransactionOptions, Value,
};

use tonic::transport::{Channel, ClientTlsConfig};
use tower::ServiceBuilder;
use tracing::debug;

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

impl From<String> for ValueType {
    fn from(s: String) -> Self {
        ValueType::StringValue(s)
    }
}

#[cfg(feature = "time")]
impl From<time::OffsetDateTime> for ValueType {
    fn from(t: time::OffsetDateTime) -> Self {
        ValueType::TimestampValue(prost_types::Timestamp {
            seconds: t.unix_timestamp(),
            nanos: 0,
        })
    }
}

pub trait TryFromEntity: Sized {
    fn try_from_entity(entity: Entity) -> Result<Self, TryFromEntityError>;
}

pub trait Kind {
    fn kind() -> &'static str;
}

///
/// Wrapper around the Datastore API.
///
#[derive(Clone)]
pub struct Datastore {
    project_id: String,
    database_id: String,
    service: DatastoreClient<AuthInterceptor<Channel>>,
}

impl Datastore {
    ///
    /// Create a new Datastore instance.
    ///
    pub async fn new(
        project_id: String,
        database_id: Option<String>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self, CloudDatastoreError> {
        let tls_config = ClientTlsConfig::new().with_native_roots();

        let channel = Channel::from_shared(HTTP_ENDPOINT)?
            .tls_config(tls_config)?
            .connect()
            .await?;

        let auth_svc = ServiceBuilder::new()
            .layer_fn(|c| {
                AuthInterceptor::new(
                    c,
                    &project_id,
                    database_id.as_deref(),
                    token_provider.clone(),
                )
            })
            .service(channel);

        let service = DatastoreClient::new(auth_svc);

        let datastore = Datastore {
            project_id,
            database_id: database_id.unwrap_or_default(),
            service,
        };

        Ok(datastore)
    }

    pub async fn upsert_entities(
        &mut self,
        entities: Vec<impl Into<Entity>>,
    ) -> Result<CommitResponse, CloudDatastoreError> {
        let mutations: Vec<Mutation> = entities
            .into_iter()
            .map(|e| Mutation {
                operation: Some(Operation::Upsert(e.into())),
                ..Default::default()
            })
            .collect();

        let request = CommitRequest {
            project_id: self.project_id.clone(),
            database_id: self.database_id.clone(), // use empty string '' to refer the default database.
            mode: CommitMode::Transactional as i32,
            transaction_selector: Some(TransactionSelector::SingleUseTransaction(
                TransactionOptions {
                    mode: Some(TransactionMode::ReadWrite(Default::default())),
                },
            )),
            mutations,
        };

        Ok(self.service.commit(request).await?.into_inner())
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
            database_id: self.database_id.clone(), // use empty string '' to refer the default database.
            mode: CommitMode::NonTransactional as i32,
            mutations: vec![Mutation {
                operation: Some(Operation::Upsert(entity)),
                ..Default::default()
            }],
            ..Default::default()
        };

        Ok(self.service.commit(request).await?.into_inner())
    }

    ///
    /// Delete an entity.
    ///
    pub async fn delete_entity(&mut self, key: impl Into<Key>) -> Result<(), CloudDatastoreError> {
        let key = key.into();
        let request = CommitRequest {
            project_id: self.project_id.clone(),
            database_id: self.database_id.clone(), // use empty string '' to refer the default database.
            mode: CommitMode::NonTransactional as i32,
            mutations: vec![Mutation {
                operation: Some(Operation::Delete(key)),
                ..Default::default()
            }],
            ..Default::default()
        };
        self.service.commit(request).await?;
        Ok(())
    }

    ///
    /// Delete entities.
    ///
    pub async fn delete_entities(
        &mut self,
        keys: Vec<impl Into<Key>>,
    ) -> Result<(), CloudDatastoreError> {
        let mutations: Vec<Mutation> = keys
            .into_iter()
            .map(|k| Mutation {
                operation: Some(Operation::Delete(k.into())),
                ..Default::default()
            })
            .collect();

        let request = CommitRequest {
            project_id: self.project_id.clone(),
            database_id: self.database_id.clone(), // use empty string '' to refer the default database.
            mode: CommitMode::Transactional as i32,
            transaction_selector: Some(TransactionSelector::SingleUseTransaction(
                TransactionOptions {
                    mode: Some(TransactionMode::ReadWrite(Default::default())),
                },
            )),
            mutations,
        };

        self.service.commit(request).await?;
        Ok(())
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
            database_id: self.database_id.clone(),
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

    /// Load all entities of a given kind.
    pub async fn load_entities<T: TryFromEntity + Kind>(
        &mut self,
    ) -> Result<Vec<T>, CloudDatastoreError> {
        let request = RunQueryRequest {
            project_id: self.project_id.clone(),
            database_id: self.database_id.clone(),
            query_type: Some(QueryType::Query(Query {
                kind: vec![KindExpression {
                    name: T::kind().to_string(),
                }],
                ..Default::default()
            })),
            ..Default::default()
        };

        let response = self.run_query(request).await?;
        let Some(batch) = response.batch else {
            return Ok(vec![]);
        };

        let entities = batch
            .entity_results
            .into_iter()
            .filter_map(|found| found.entity)
            .map(|entity| T::try_from_entity(entity))
            .collect::<Result<Vec<T>, TryFromEntityError>>()?;

        Ok(entities)
    }

    /// Run a query. The provided query has the project_id set to the project_id of the Datastore instance.
    /// The query is specified in the `RunQueryRequest` parameter.
    /// The result is returned as a `RunQueryResponse`.
    pub async fn run_query(
        &mut self,
        mut request: RunQueryRequest,
    ) -> Result<RunQueryResponse, CloudDatastoreError> {
        request.project_id = self.project_id.clone();
        request.database_id = self.database_id.clone();
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

    /// Add a value to the entity.
    pub fn add_value<T: Into<String>, V: Into<ValueType>>(
        mut self,
        name: T,
        value: V,
        indexed: bool,
    ) -> Self {
        self.entity.properties.insert(
            name.into(),
            Value {
                exclude_from_indexes: !indexed,
                value_type: Some(value.into()),
                ..Default::default()
            },
        );
        self
    }

    /// Add an optional value to the entity.
    pub fn opt_value<T: Into<String>, V: Into<ValueType>>(
        self,
        name: T,
        value: Option<V>,
        indexed: bool,
    ) -> Self {
        match value {
            Some(value) => self.add_value(name, value, indexed),
            None => self,
        }
    }

    /// Add a string property to the entity.
    pub fn add_string<T: Into<String>>(self, name: T, value: T, indexed: bool) -> Self {
        self.add_value(name, value.into(), indexed)
    }

    /// Add an optional string property to the entity.
    pub fn opt_string<T: Into<String>>(self, name: T, value: Option<T>, indexed: bool) -> Self {
        self.opt_value(name, value.map(Into::into), indexed)
    }

    pub fn add_bool<T: Into<String>>(self, name: T, value: bool, indexed: bool) -> Self {
        self.add_value(name, ValueType::BooleanValue(value), indexed)
    }

    pub fn opt_bool<T: Into<String>>(self, name: T, value: Option<bool>, indexed: bool) -> Self {
        self.opt_value(name, value.map(ValueType::BooleanValue), indexed)
    }

    #[cfg(feature = "time")]
    pub fn add_offset_date_time<T: Into<String>>(
        self,
        name: T,
        value: time::OffsetDateTime,
        indexed: bool,
    ) -> Self {
        self.add_value(
            name,
            <time::OffsetDateTime as Into<ValueType>>::into(value),
            indexed,
        )
    }

    #[cfg(feature = "time")]
    pub fn opt_offset_date_time<T: Into<String>>(
        self,
        name: T,
        value: Option<time::OffsetDateTime>,
        indexed: bool,
    ) -> Self {
        self.opt_value(
            name,
            value.map(<time::OffsetDateTime as Into<ValueType>>::into),
            indexed,
        )
    }

    /// Add an integer property to the entity.
    pub fn add_string_array<T: Into<String>>(mut self, name: T, values: Vec<String>) -> Self {
        self.entity.properties.insert(
            name.into(),
            Value {
                value_type: Some(ValueType::ArrayValue(ArrayValue {
                    values: values
                        .into_iter()
                        .map(|v| Value {
                            value_type: Some(v.into()),
                            ..Default::default()
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
            .ok_or(EntityValueError("Missing Key".to_string()))?;

        let key_kind = key.kind().map_err(|e| EntityValueError(e.to_string()))?;

        if key_kind == kind {
            Ok(key)
        } else {
            Err(EntityValueError(format!(
                "Invalid Key Kind. Expected '{}'.",
                kind,
            )))
        }
    }

    pub fn req_string(&self, name: &str) -> Result<String, EntityValueError> {
        self.opt_string(name)
            .and_then(|v| v.ok_or(EntityValueError("missing required field".to_string())))
    }

    pub fn opt_string(&self, name: &str) -> Result<Option<String>, EntityValueError> {
        match self.properties.get(name) {
            Some(Value {
                meaning: _,
                exclude_from_indexes: _,
                value_type: Some(ValueType::StringValue(value)),
            }) => Ok(Some(value.clone())),
            None => Ok(None),
            _ => Err(EntityValueError(format!("Field {name} is not a string"))),
        }
    }

    pub fn opt_bool(&self, name: &str) -> Result<Option<bool>, EntityValueError> {
        match self.properties.get(name) {
            Some(Value {
                meaning: _,
                exclude_from_indexes: _,
                value_type: Some(ValueType::BooleanValue(value)),
            }) => Ok(Some(*value)),
            None => Ok(None),
            _ => Err(EntityValueError(format!("Field {name} is not a boolean"))),
        }
    }

    pub fn req_bool(&self, name: &str) -> Result<bool, EntityValueError> {
        self.opt_bool(name)
            .and_then(|v| v.ok_or(EntityValueError("missing required field".to_string())))
    }

    #[cfg(feature = "time")]
    pub fn opt_offset_date_time(
        &self,
        name: &str,
    ) -> Result<Option<time::OffsetDateTime>, EntityValueError> {
        match self.properties.get(name) {
            Some(Value {
                meaning: _,
                exclude_from_indexes: _,
                value_type: Some(ValueType::TimestampValue(timestamp)),
            }) => Ok(time::OffsetDateTime::from_unix_timestamp(timestamp.seconds).ok()),
            None => Ok(None),
            _ => Err(EntityValueError(format!("Field {name} is not a Timestamp"))),
        }
    }

    #[cfg(feature = "time")]
    pub fn req_offset_date_time(
        &self,
        name: &str,
    ) -> Result<time::OffsetDateTime, EntityValueError> {
        self.opt_offset_date_time(name)
            .and_then(|v| v.ok_or(EntityValueError("missing required field".to_string())))
    }

    pub fn opt_string_array(&self, name: &str) -> Result<Option<Vec<String>>, EntityValueError> {
        let value_type = self.properties.get(name).and_then(|v| v.value_type.clone());

        let Some(value_type) = value_type else {
            debug!(field = name, "No value found for field.");
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
        let result = self.opt_string_array(name)?;
        result.ok_or_else(|| EntityValueError(format!("Entity missing required field '{}'", name)))
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
