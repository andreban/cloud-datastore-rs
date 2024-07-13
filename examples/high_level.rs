use std::{collections::HashMap, error::Error};

use cloud_datastore_rs::{
    google::datastore::v1::{
        key::{path_element::IdType, PathElement},
        value::ValueType,
        Entity, Key, Value,
    },
    Datastore, TryFromEntity, TryFromEntityError,
};

struct BookKey(String);

impl From<BookKey> for Key {
    fn from(book_key: BookKey) -> Self {
        Key {
            path: vec![PathElement {
                kind: "Book".to_string(),
                id_type: Some(IdType::Name(book_key.0)),
            }],
            ..Default::default()
        }
    }
}

#[derive(Debug)]
struct Book {
    id: String,
    title: String,
}

impl TryFromEntity for Book {
    fn try_from_entity(value: Entity) -> Result<Self, TryFromEntityError> {
        let key = value.key.unwrap();
        let title = value
            .properties
            .get("title")
            .ok_or(TryFromEntityError)?
            .value_type
            .as_ref()
            .ok_or(TryFromEntityError)?;

        let IdType::Name(id) = key
            .path
            .get(0)
            .ok_or(TryFromEntityError)?
            .id_type
            .as_ref()
            .ok_or(TryFromEntityError)?
            .clone()
        else {
            return Err(TryFromEntityError);
        };

        let ValueType::StringValue(title) = title.clone() else {
            return Err(TryFromEntityError);
        };

        Ok(Book { id, title })
    }
}

impl From<Book> for Entity {
    fn from(book: Book) -> Self {
        let key = Key {
            path: vec![PathElement {
                kind: "Book".to_string(),
                id_type: Some(IdType::Name(book.id)),
            }],
            ..Default::default()
        };

        let value = Value {
            value_type: Some(ValueType::StringValue(book.title)),
            ..Default::default()
        };

        let entity = Entity {
            key: Some(key),
            properties: HashMap::from([("title".to_string(), value)]),
            ..Default::default()
        };

        entity
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let project_id = std::env::var("PROJECT_ID")?;
    let token_provider = gcp_auth::provider().await?;

    let mut datastore = Datastore::new(project_id, token_provider).await?;

    let book = Book {
        id: "book_one".to_string(),
        title: "Book One Title".to_string(),
    };

    let result = datastore.upsert_entity(book).await?;
    println!("{:?}", result);

    let book: Option<Book> = datastore
        .lookup_entity(BookKey("book_one".to_string()))
        .await?;

    println!("{:?}", book);
    Ok(())
}
