use chrono::{DateTime, Datelike, Utc};
use duckdb::{params, DuckdbConnectionManager, Params};
use lazy_static::lazy_static;
use r2d2::PooledConnection;
use std::slice::Iter;
use wikidata::ClaimValueData;

use crate::{id::Id, LANG};

/// The `Table` enum defines the different types of data that can be stored in the
/// DuckDB database for a Wikidata item. Each variant of the enum corresponds to a
/// different type of data, such as an `Entity`, a `String`, `Coordinate`, a `Quantity`,
/// or a `Tune`. The `Unknown` variant is used for data types that are not recognized,
/// and the `None` variant is used for cases where no data is present. The enum also
/// provides methods for creating tables and indices in the database, as well as
/// inserting data into the tables.
pub enum Table {
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
    /// The function returns an iterator over a static array of seven different types of
    /// tables.
    ///
    /// Returns:
    ///
    /// The function `iterator` returns an iterator over a static array of `Table`
    /// values. The `lazy_static` macro is used to create a static reference to the
    /// array, which is then iterated over and returned by the function.
    pub fn iterator() -> Iter<'static, Table> {
        lazy_static! {
            static ref TABLES: [Table; 7] = [
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
                Table::Unknown
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

        let (table_name, mut value_columns) = match self {
            Table::String(_) => ("string", vec![("string", "TEXT NOT NULL")]),
            Table::Coordinates { .. } => (
                "coordinate",
                vec![
                    ("latitude", "DOUBLE NOT NULL"),
                    ("longitude", "DOUBLE NOT NULL"),
                    ("precision", "DOUBLE NOT NULL"),
                    ("globe_id", "INTEGER NOT NULL"),
                ],
            ),
            Table::Quantity { .. } => (
                "quantity",
                vec![
                    ("amount", "DOUBLE NOT NULL"),
                    ("lower_bound", "DOUBLE"),
                    ("upper_bound", "DOUBLE"),
                    ("unit_id", "INTEGER"),
                ],
            ),
            Table::Time { .. } => (
                "time",
                vec![
                    ("time", "DATETIME NOT NULL"),
                    ("precision", "INTEGER NOT NULL"),
                ],
            ),
            _ => ("edge", vec![]), // For Entity, Unknown and None we create only one table...
        };

        // Lastly, we have to extend the common columns with the rest of the body of the entities.
        // In this manner, we can create as many tables as we wish, all of them following the
        // previously described inheritance policy :D

        columns.append(&mut value_columns);

        (table_name, columns)
    }

    /// This function creates a table in a DuckDB database with the specified table name
    /// and columns.
    ///
    /// Arguments:
    ///
    /// * `connection`: `connection` is a reference to a `PooledConnection` object from
    /// the `DuckdbConnectionManager` type. It is used to establish a connection to a
    /// DuckDB database and execute SQL queries on it.
    ///
    /// Returns:
    ///
    /// The `create_table` function is returning a `duckdb::Result<()>`, which is a type
    /// alias for `Result<(), duckdb::Error>`. This means that the function returns a
    /// `Result` object that either contains a `()` value (i.e. nothing) if the table
    /// creation was successful, or a `duckdb::Error` object if an error occurred.
    pub fn create_table(
        &self,
        connection: &PooledConnection<DuckdbConnectionManager>,
    ) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();
        connection.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {} ({});",
            table_name,
            columns
                .iter()
                .map(|(column_name, column_type)| format!("{} {}", column_name, column_type))
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    /// The function creates indices for specific columns in a table using a connection
    /// to a DuckDB database.
    ///
    /// Arguments:
    ///
    /// * `connection`: The `connection` parameter is a reference to a
    /// `PooledConnection` object from the `DuckdbConnectionManager` type. It is used to
    /// execute SQL queries on a DuckDB database.
    ///
    /// Returns:
    ///
    /// a `duckdb::Result<()>`, which is a result type indicating success or failure of
    /// the operation. The `()` inside the `Result` indicates that the function returns
    /// no meaningful value on success, but may return an error if the operation fails.
    pub fn create_indices(
        &self,
        connection: &PooledConnection<DuckdbConnectionManager>,
    ) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();

        for (column_name, _) in columns {
            // We are interested in creating indices only for two columns: src_id and dst_id. Hence,
            // we check if the column_name is any of those. In the previous version loads of clutter
            // was created by creating indices for all the columns. Notice that we are not interested
            // in querying over columns that just annotate the node with additional information, such
            // as the description, or the label in a certain language :(
            if column_name == "src_id" || column_name == "dst_id" {
                connection.execute_batch(&format!(
                    "CREATE INDEX IF NOT EXISTS {}_{}_index ON {} ({});",
                    table_name, column_name, table_name, column_name,
                ))?;
            }
        }

        Ok(())
    }

    /// This function inserts data into a specified table using a prepared SQL
    /// statement.
    ///
    /// Arguments:
    ///
    /// * `connection`: A reference to a pooled connection to a DuckDB database.
    ///
    /// * `params`: `params` is a parameter of the `insert` function that takes an
    /// implementation of the `Params` trait. This trait is used to specify the values
    /// to be inserted into the database table.
    ///
    /// Returns:
    ///
    /// The `insert` function returns a `duckdb::Result<()>`, which is an alias for
    /// `Result<(), duckdb::Error>`. This means that the function returns a result that
    /// can either be Ok(()) if the operation was successful, or an error of type
    /// `duckdb::Error` if something went wrong.
    fn insert(
        &self,
        connection: &PooledConnection<DuckdbConnectionManager>,
        params: impl Params,
    ) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();

        connection
            .prepare_cached(&format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                columns
                    .iter()
                    .map(|(column_name, _)| column_name.to_owned())
                    .collect::<Vec<_>>()
                    .join(", "),
                (0..columns.len())
                    .map(|i| format!("?{}", i + 1))
                    .collect::<Vec<_>>()
                    .join(", "),
            ))?
            .execute(params)?;

        Ok(())
    }

    /// This function stores data in a DuckDB database based on the type of data
    /// provided.
    ///
    /// Arguments:
    ///
    /// * `connection`: A connection to a DuckDB database, obtained from a connection
    /// pool.
    ///
    /// * `src_id`: The ID of the source entity in the knowledge graph.
    ///
    /// * `property_id`: The ID of the property being stored in the database.
    ///
    /// Returns:
    ///
    /// a `duckdb::Result<()>`, which is a result type indicating success or failure of
    /// the database operation. The `()` indicates that the function does not return any
    /// meaningful value on success.
    pub fn store(
        &self,
        connection: &PooledConnection<DuckdbConnectionManager>,
        src_id: u64,
        property_id: u64,
    ) -> duckdb::Result<()> {
        // Note the schema of the Database we are working with. In this regard, we have two main
        // entities which include Vertex and Edge; those act as the two pieces that together form
        // a Knowledge Graph out of the JSON dump we are willing to process. Apart from that, we
        // need to store data types that are more complex; that is, qualifiers may annotate the
        // relationships and we want to preserve that kind of information. Thus, some entities arise
        // which model those extensions to the data model. This may be expanded in the future ;D
        //
        // ACK: See https://github.com/angelip2303/wd2duckdb#database-structure for a more detailed
        // description of the data model we are creating with this tool.

        match self {
            Table::Entity(dst_id) => self.insert(connection, params![src_id, property_id, dst_id]),
            Table::None => self.insert(connection, params![src_id, property_id, src_id]),
            Table::Unknown => self.insert(connection, params![src_id, property_id, src_id]),
            Table::String(string) => {
                self.insert(connection, params![src_id, property_id, src_id, string])
            }
            Table::Coordinates {
                latitude,
                longitude,
                precision,
                globe_id,
            } => self.insert(
                connection,
                params![
                    src_id,
                    property_id,
                    src_id,
                    latitude,
                    longitude,
                    precision,
                    globe_id
                ],
            ),
            Table::Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit_id,
            } => self.insert(
                connection,
                params![
                    src_id,
                    property_id,
                    src_id,
                    amount,
                    lower_bound,
                    upper_bound,
                    unit_id
                ],
            ),
            Table::Time { time, precision } => {
                // We have to handle years wich are greater than the maximum possible value :D
                if time.year() < 9999 {
                    self.insert(
                        connection,
                        params![src_id, property_id, src_id, time, precision],
                    )
                } else {
                    self.insert(
                        connection,
                        params![src_id, property_id, src_id, "infinity", precision],
                    )
                }
            }
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
