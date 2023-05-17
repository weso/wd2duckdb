use chrono::{DateTime, Datelike, Utc};
use duckdb::{params, Appender, Error, Transaction};
use lazy_static::lazy_static;
use std::{collections::HashMap, slice::Iter};
use wikidata::ClaimValueData;

use crate::{id::Id, LANG};

/// The `AppenderHelper` struct contains a hashmap of `Appender` structs with string
/// keys.
///
/// Properties:
///
/// * `appenders`: `appenders` is a property of type `HashMap<&'a str,
/// Appender<'a>>` in a struct called `AppenderHelper`. It is a hash map that stores
/// references to `Appender` objects, with keys of type `&'a str`.
pub struct AppenderHelper<'a> {
    appenders: HashMap<&'a str, Appender<'a>>,
}

/// The above code is implementing a new method for the `AppenderHelper` struct in
/// Rust. The method takes a reference to a `Transaction` object and creates a new
/// instance of `AppenderHelper` struct. Inside the method, a new `HashMap` is
/// created to store appenders for each table. The `Table::iterator()` method is
/// called to iterate over all tables, and for each table, the
/// `transaction.appender()` method is called to get the appender for that table. If
/// the appender is successfully obtained, it is inserted into the `appenders`
/// HashMap with the
impl<'a> AppenderHelper<'a> {
    pub fn new(transaction: &'a Transaction) -> Self {
        let mut appenders = HashMap::new();
        Table::iterator().for_each(|table| {
            if let Ok(appender) = transaction.appender(table.as_ref()) {
                appenders.insert(table.as_ref(), appender);
            }
        });
        Self { appenders }
    }
}

/// The above code is defining an enum called `Table` in Rust programming language.
/// The enum has several variants including `Vertex` which has fields `id`, `label`,
/// and `description`, `Entity` which has a single field `u64`, `String` which has a
/// single field `String`, `Coordinates` which has fields `latitude`, `longitude`,
/// `precision`, and `globe_id`, `Quantity` which has fields `amount`,
/// `lower_bound`, `upper_bound`, and `unit_id`, `Time` which has fields `time` and
/// `precision`, `Unknown`,
pub enum Table {
    Vertex {
        id: u64,
        label: String,
        description: String,
    },
    Entity(u64),
    String(String),
    Coordinates {
        latitude: f64,
        longitude: f64,
        precision: f64,
        globe_id: u64,
    },
    Quantity {
        amount: f64,
        lower_bound: Option<f64>,
        upper_bound: Option<f64>,
        unit_id: Option<u64>,
    },
    Time {
        time: DateTime<Utc>,
        precision: u8,
    },
    Unknown,
    None,
}

impl Table {
    /// The function returns an iterator over a static array of tables in Rust.
    ///
    /// Returns:
    ///
    /// The function `iterator` returns an iterator over a static array of `Table`
    /// values.
    pub fn iterator() -> Iter<'static, Table> {
        lazy_static! {
            static ref TABLES: [Table; 8] = [
                Table::Vertex {
                    id: 0,
                    description: String::default(),
                    label: String::default()
                },
                Table::String(String::new()),
                Table::Entity(0),
                Table::Coordinates {
                    latitude: 0.0,
                    longitude: 0.0,
                    precision: 0.0,
                    globe_id: 0,
                },
                Table::Quantity {
                    amount: 0.0,
                    lower_bound: None,
                    upper_bound: None,
                    unit_id: None,
                },
                Table::Time {
                    time: Default::default(),
                    precision: 0,
                },
                Table::None,
                Table::Unknown,
            ];
        }
        TABLES.iter()
    }

    /// Returns the table name and column definitions for the given entity type as a tuple.
    ///
    /// According to the so-called database structure we are going to describe in here, all the
    /// edges will have the following 3 columns: src_id, property_id and dst_id. Not only that,
    /// but implementing inheritance in a relational database can be done through several
    /// alternatives. What we have chosen so far is 'Table-Per-Concrete', where each entity will
    /// have its corresponding fully formed table with no references to any of the other sub-types.
    /// Note that all of those will have the same 3 columns: src_id, property_id and dst_id.
    /// However, due to the fact that some datum can possibly reference a yet not parsed value,
    /// we cannot use primary keys. Hence, indices will be created for easier accessing :D
    ///
    /// Returns:
    ///
    /// A tuple containing the name of the table as a `&str` and a vector of column definitions
    /// as tuples, where each tuple contains the column name as a `&str` and the column type as a `&str`.
    ///
    /// # Example
    ///
    /// ```
    /// let table = Table::String("Hello world".to_string());
    /// let (table_name, columns) = table.table_definition();
    /// println!("Table name: {}", table_name);
    /// println!("Columns: {:?}", columns);
    /// ```
    ///
    /// Output:
    /// ```
    /// Table name: string
    /// Columns: [("src_id", "UBIGINT NOT NULL"), ("property_id", "UBIGINT NOT NULL"), ("dst_id", "UBIGINT NOT NULL"), ("string", "TEXT NOT NULL")]
    /// ```
    fn table_definition(&self) -> (&str, Vec<(&str, &str)>) {
        if let Table::Vertex { .. } = self {
            // Early return in case we find a Vertex :D
            return (
                Table::iterator().next().unwrap().as_ref(),
                vec![
                    ("id", "INTEGER NOT NULL"),
                    ("label", "TEXT"),
                    ("description", "TEXT"),
                ],
            );
        }

        let mut columns: Vec<(&str, &str)> = vec![
            ("src_id", "UBIGINT NOT NULL"),
            ("property_id", "UBIGINT NOT NULL"),
            ("dst_id", "UBIGINT NOT NULL"),
        ];

        // For the sake of simplicity, those entities that annotate no additional value; that is,
        // Entity, None and Unknown, will be all of those stored in the same table called Edge. Thus,
        // we are avoiding the creation of 3 tables with the exact same structure as a whole. More
        // in more, notice that the dst_id of all the relationships, but for Entity, will be the
        // src_id, as we are annotating additional information to the node itself :D
        let mut value_columns = match self {
            Table::String(_) => vec![("string", "TEXT NOT NULL")],
            Table::Coordinates { .. } => vec![
                ("latitude", "DOUBLE NOT NULL"),
                ("longitude", "DOUBLE NOT NULL"),
                ("precision", "DOUBLE NOT NULL"),
                ("globe_id", "INTEGER NOT NULL"),
            ],
            Table::Quantity { .. } => vec![
                ("amount", "DOUBLE NOT NULL"),
                ("lower_bound", "DOUBLE"),
                ("upper_bound", "DOUBLE"),
                ("unit_id", "INTEGER"),
            ],
            Table::Time { .. } => vec![
                ("time", "DATETIME NOT NULL"),
                ("precision", "INTEGER NOT NULL"),
            ],
            _ => vec![], // For Entity, Unknown and None we create only one table...
        };

        // Lastly, we have to extend the common columns with the rest of the body of the entities.
        // In this manner, we can create as many tables as we wish, all of them following the
        // previously described inheritance policy :D

        columns.append(&mut value_columns);

        (self.as_ref(), columns)
    }

    /// This function inserts data into a knowledge graph database.
    ///
    /// Arguments:
    ///
    /// * `appender_helper`: A mutable reference to an `AppenderHelper` struct,
    /// which is used to append rows to the various tables in the database.
    /// * `src_id`: The ID of the source vertex in the knowledge graph.
    /// * `label`: An optional reference to a String that represents the label of
    /// the vertex being inserted into the database.
    /// * `description`: An optional string parameter that represents the
    /// description of a vertex in a knowledge graph.
    /// * `property_id`: The ID of the property being inserted into the database.
    ///
    /// Returns:
    ///
    /// a `Result` with the `Ok` variant containing an empty tuple `()` if the
    /// function executes successfully, and the `Err` variant containing an `Error`
    /// if there is an error during execution.
    pub fn insert(
        &self,
        appender_helper: &mut AppenderHelper,
        src_id: u64,
        label: Option<&String>,
        description: Option<&String>,
        property_id: u64,
    ) -> Result<(), Error> {
        // Note the schema of the Database we are working with. In this regard, we have two main
        // entities which include Vertex and Edge; those act as the two pieces that together form
        // a Knowledge Graph out of the JSON dump we are willing to process. Apart from that, we
        // need to store data types that are more complex; that is, qualifiers may annotate the
        // relationships and we want to preserve that kind of information. Thus, some entities arise
        // which model those extensions to the data model. This may be expanded in the future ;D
        //
        // ACK: See https://github.com/angelip2303/wd2duckdb#database-structure for a more detailed
        // description of the data model we are creating with this tool.

        // 1. First, we have to create the Vertex entry in the database
        appender_helper
            .appenders
            .get_mut("vertex")
            .unwrap()
            .append_row(params![src_id, label, description])?;

        // 2. Second, we create the edge
        match self {
            Table::Entity(dst_id) => appender_helper
                .appenders
                .get_mut(self.as_ref())
                .unwrap()
                .append_row(params![src_id, property_id, dst_id])?,
            Table::None => appender_helper
                .appenders
                .get_mut(self.as_ref())
                .unwrap()
                .append_row(params![src_id, property_id, src_id])?,
            Table::Unknown => appender_helper
                .appenders
                .get_mut(self.as_ref())
                .unwrap()
                .append_row(params![src_id, property_id, src_id])?,
            Table::String(string) => appender_helper
                .appenders
                .get_mut(self.as_ref())
                .unwrap()
                .append_row(params![src_id, property_id, src_id, string])?,
            Table::Coordinates {
                latitude,
                longitude,
                precision,
                globe_id,
            } => appender_helper
                .appenders
                .get_mut(self.as_ref())
                .unwrap()
                .append_row(params![
                    src_id,
                    property_id,
                    src_id,
                    latitude,
                    longitude,
                    precision,
                    globe_id
                ])?,
            Table::Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit_id,
            } => appender_helper
                .appenders
                .get_mut(self.as_ref())
                .unwrap()
                .append_row(params![
                    src_id,
                    property_id,
                    src_id,
                    amount,
                    lower_bound,
                    upper_bound,
                    unit_id
                ])?,
            Table::Time { time, precision } => {
                // We have to handle years wich are greater than the maximum possible value :D
                if time.year() < 9999 {
                    appender_helper
                        .appenders
                        .get_mut(self.as_ref())
                        .unwrap()
                        .append_row(params![src_id, property_id, src_id, time, precision])?
                } else {
                    appender_helper
                        .appenders
                        .get_mut(self.as_ref())
                        .unwrap()
                        .append_row(params![src_id, property_id, src_id, "infinity", precision])?
                }
            }
            _ => return Err(Error::AppendError),
        }

        Ok(())
    }

    /// This function creates a table in a database using the provided transaction and
    /// table definition.
    ///
    /// Arguments:
    ///
    /// * `transaction`: A reference to a transaction object that is used to execute the
    /// SQL query to create a table in a database. The transaction object is typically
    /// created by starting a transaction on a database connection and then passing it
    /// to this function.
    ///
    /// Returns:
    ///
    /// a `Result` object with the `Ok` variant containing an empty tuple `()` if the
    /// table creation is successful, or an `Error` object if there is an error during
    /// the execution of the SQL statement.
    pub fn create_table(&self, transaction: &Transaction) -> Result<(), Error> {
        let (table_name, columns) = self.table_definition();
        transaction.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {} ({});",
            table_name,
            columns
                .iter()
                .map(|(column_name, column_type)| format!("{} {}", column_name, column_type))
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    /// The function creates indices for specific columns in a table using SQL
    /// statements.
    ///
    /// Arguments:
    ///
    /// * `transaction`: A reference to a transaction object that is used to execute SQL
    /// queries on a database.
    ///
    /// Returns:
    ///
    /// a `Result` enum with either an `Ok(())` value indicating that the indices were
    /// successfully created, or an `Err` value containing an `Error` object if an error
    /// occurred during the execution of the function.
    pub fn create_indices(&self, transaction: &Transaction) -> Result<(), Error> {
        let (table_name, columns) = self.table_definition();

        for (column_name, _) in columns {
            // We are interested in creating indices only for two columns: src_id and dst_id. Hence,
            // we check if the column_name is any of those. In the previous version loads of clutter
            // was created by creating indices for all the columns. Notice that we are not interested
            // in querying over columns that just annotate the node with additional information, such
            // as the description, or the label in a certain language :(
            if column_name == "src_id" || column_name == "dst_id" {
                transaction.execute_batch(&format!(
                    "CREATE INDEX IF NOT EXISTS {}_{}_index ON {} ({});",
                    table_name, column_name, table_name, column_name,
                ))?;
            }
        }

        Ok(())
    }
}

/// The above code is implementing the `AsRef` trait for the `Table` enum in Rust.
/// This trait allows a type to be borrowed as a reference to another type. In this
/// implementation, the `as_ref` method returns a string slice that represents the
/// type of the `Table` enum variant. The method matches each variant of the enum
/// and returns a string slice that corresponds to the variant.
impl AsRef<str> for Table {
    fn as_ref(&self) -> &str {
        match self {
            Table::Vertex { .. } => "vertex",
            Table::Entity(_) => "edge",
            Table::String(_) => "string",
            Table::Coordinates { .. } => "coordinates",
            Table::Quantity { .. } => "quantity",
            Table::Time { .. } => "time",
            Table::Unknown => "edge",
            Table::None => "edge",
        }
    }
}

impl From<ClaimValueData> for Table {
    fn from(claim_value_data: ClaimValueData) -> Self {
        use ClaimValueData::*;

        match claim_value_data {
            CommonsMedia(string) => Self::String(string),
            GlobeCoordinate {
                lat,
                lon,
                precision,
                globe,
            } => Self::Coordinates {
                latitude: lat,
                longitude: lon,
                precision,
                globe_id: u64::from(Id::Qid(globe)),
            },
            Item(id) => Self::Entity(u64::from(Id::Qid(id))),
            Property(id) => Self::Entity(u64::from(Id::Pid(id))),
            String(string) => Self::String(string),
            MonolingualText(text) => Self::String(text.text),
            MultilingualText(texts) => {
                for text in texts {
                    if text.lang.0 == LANG.0 {
                        return Self::String(text.text);
                    }
                }
                Self::None
            }
            ExternalID(string) => Self::String(string),
            Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit,
            } => Self::Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit_id: unit.map(|id| u64::from(Id::Qid(id))),
            },
            DateTime {
                date_time,
                precision,
            } => Self::Time {
                time: date_time,
                precision,
            },
            Url(string) => Self::String(string),
            MathExpr(string) => Self::String(string),
            GeoShape(string) => Self::String(string),
            MusicNotation(string) => Self::String(string),
            TabularData(string) => Self::String(string),
            Lexeme(id) => Self::Entity(u64::from(Id::Lid(id))),
            Form(id) => Self::Entity(u64::from(Id::Fid(id))),
            Sense(id) => Self::Entity(u64::from(Id::Sid(id))),
            NoValue => Self::None,
            UnknownValue => Self::Unknown,
        }
    }
}
