use std::{collections::HashMap, error::Error};

use cloud_datastore_rs::google::datastore::{
    self,
    v1::{
        commit_request::Mode,
        datastore_client::DatastoreClient,
        key::{path_element::IdType, PathElement},
        mutation::Operation,
        run_query_request::QueryType,
        CommitRequest, Entity, GqlQuery, Key, LookupRequest, Mutation, RunQueryRequest, Value,
    },
};
use tonic::{
    metadata::MetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, ClientTlsConfig},
    Request, Status,
};

const AUTH_SCOPE: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let project_id = std::env::var("PROJECT_ID")?;

    let token_provider = gcp_auth::provider().await?;
    let token = token_provider.token(AUTH_SCOPE).await?;

    let bearer_token = format!("Bearer {}", token.as_str());
    let header_value: MetadataValue<_> = bearer_token.parse()?;

    let http_endpoint = format!("https://datastore.googleapis.com");
    let tls_config = ClientTlsConfig::new().with_native_roots();
    let channel = Channel::from_shared(http_endpoint)?
        .tls_config(tls_config)?
        .connect()
        .await?;

    let mut service = DatastoreClient::with_interceptor(channel, |mut req: Request<()>| {
        req.metadata_mut()
            .insert("authorization", header_value.clone());
        Ok(req)
    });

    upsert_entity(&mut service, project_id.clone()).await?;

    load_entity(&mut service, project_id.clone()).await?;

    let query_response = service
        .run_query(RunQueryRequest {
            project_id,
            database_id: "".to_string(),
            query_type: Some(QueryType::GqlQuery(GqlQuery {
                query_string: "select * from Book".to_string(),
                ..Default::default()
            })),

            ..Default::default()
        })
        .await?;

    println!("{query_response:?}");

    Ok(())
}

async fn upsert_entity(
    service: &mut DatastoreClient<
        InterceptedService<Channel, impl Fn(Request<()>) -> Result<Request<()>, Status>>,
    >,
    project_id: String,
) -> Result<(), Box<dyn Error>> {
    let key = Key {
        path: vec![PathElement {
            kind: "Book".to_string(),
            id_type: Some(IdType::Id(1)),
        }],
        ..Default::default()
    };

    let value = Value {
        value_type: Some(datastore::v1::value::ValueType::StringValue(
            "A Title".to_string(),
        )),
        ..Default::default()
    };

    let entity = Entity {
        key: Some(key),
        properties: HashMap::from([("title".to_string(), value)]),
        ..Default::default()
    };

    let request = CommitRequest {
        project_id: project_id,
        database_id: "".to_string(), // use empty string '' to refer the default database.
        mode: Mode::NonTransactional as i32,
        mutations: vec![Mutation {
            operation: Some(Operation::Upsert(entity)),
            ..Default::default()
        }],
        ..Default::default()
    };

    let result = service.commit(request).await?;

    println!("{:?}", result);
    Ok(())
}

async fn load_entity(
    service: &mut DatastoreClient<
        InterceptedService<Channel, impl Fn(Request<()>) -> Result<Request<()>, Status>>,
    >,
    project_id: String,
) -> Result<(), Box<dyn Error>> {
    let key = Key {
        path: vec![PathElement {
            kind: "Book".to_string(),
            id_type: Some(IdType::Id(1)),
        }],
        ..Default::default()
    };

    let request = LookupRequest {
        project_id: project_id,
        keys: vec![key],
        ..Default::default()
    };

    let response = service.lookup(request).await?;
    let entity = response.into_inner().found;
    let first_entity = entity.into_iter().next().unwrap();
    first_entity
        .entity
        .unwrap()
        .properties
        .iter()
        .for_each(|(k, v)| {
            println!("{}: {:?}", k, v);
        });

    Ok(())
}
