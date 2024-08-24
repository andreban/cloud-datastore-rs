use std::error::Error;

use cloud_datastore_rs::{
    google::datastore::v1::{
        key::{path_element::IdType, PathElement},
        Entity, Key,
    },
    Datastore, Kind, TryFromEntity, TryFromEntityError,
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
    tags: Vec<String>,
}

impl TryFromEntity for Book {
    fn try_from_entity(value: Entity) -> Result<Self, TryFromEntityError> {
        println!("{:?}", value);
        let id = value.req_key("Book")?.name()?.to_string(); // Ensure the key is of kind 'Book'
        let title = value.req_string("title")?;
        let tags = value.req_string_array("tags")?;
        Ok(Book { id, title, tags })
    }
}

impl Kind for Book {
    fn kind() -> &'static str {
        "Book"
    }
}

impl From<Book> for Entity {
    fn from(book: Book) -> Self {
        Entity::builder()
            .with_key_name("Book", &book.id)
            .add_string("title", &book.title, true)
            .add_string_array("tags", book.tags)
            .build()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let project_id = std::env::var("PROJECT_ID")?;
    let database_id = std::env::var("DATABASE_ID").ok();
    println!("Database ID: {:?}", database_id);
    let token_provider = gcp_auth::provider().await?;

    let mut datastore = Datastore::new(project_id, database_id, token_provider).await?;

    let book = Book {
        id: "book_one".to_string(),
        title: "Book One Title".to_string(),
        tags: vec!["tag_one".to_string(), "tag_two".to_string()],
    };

    let result = datastore.upsert_entity(book).await?;
    println!("{:?}", result);

    let book: Option<Book> = datastore
        .lookup_entity(BookKey("book_one".to_string()))
        .await?;
    println!("{:?}", book);

    let result = datastore
        .upsert_entities(vec![
            Book {
                id: "book_three".to_string(),
                title: "Book Three Title".to_string(),
                tags: vec!["tag_three".to_string()],
            },
            Book {
                id: "book_four".to_string(),
                title: "Book Four Title".to_string(),
                tags: vec!["tag_four".to_string()],
            },
        ])
        .await?;

    println!("{:?}", result);

    let all_books = datastore.load_entities::<Book>().await?;
    println!("{:?}", all_books);

    Ok(())
}
