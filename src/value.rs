use chrono::{DateTime, Utc};
use duckdb::{DuckdbConnectionManager, params, Params};
use lazy_static::lazy_static;
use std::slice::Iter;
use r2d2::PooledConnection;
use wikidata::ClaimValueData;

use crate::id::{f_id, l_id, p_id, q_id, s_id};
use crate::LANG;

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
    None
}

impl Table {
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

    fn table_definition(&self) -> (&str, Vec<(&str, &str)>) {
        // According to the so-called database structure we are going to describe in here, all the
        // edges will have the following 3 columns: src_id, property_id and dst_id. Not only that,
        // but implementing inheritance in a relational database can be done through several
        // alternatives. What we have chosen so far is 'Table-Per-Concrete', where each entity will
        // have its corresponding fully formed table with no references to any of the other sub-types.
        // Note that all of those will have the same 3 columns: src_id, property_id and dst_id.
        // However, due to the fact that some datum can possibly reference a yet not parsed value,
        // we cannot use primary keys. Hence, indices will be created for easier accessing :D

        let mut columns = vec![
            ("src_id", "INTEGER NOT NULL"),
            ("property_id", "INTEGER NOT NULL"),
            ("dst_id", "INTEGER NOT NULL")
        ];

        // For the sake of simplicity, those entities that annotate no additional value; that is,
        // Entity, None and Unknown, will be all of those stored in the same table called Edge. Thus,
        // we are avoiding the creation of 3 tables with the exact same structure as a whole. More
        // in more, notice that the dst_id of all the relationships but for Entity, will be the
        // src_id, as we are annotating additional information to the node itself :D

        let (table_name, mut value_columns) = match self {
            Table::String(_) => ("string", vec![("string", "TEXT NOT NULL")]),
            Table::Coordinates { .. } => (
                "coordinate",
                vec![
                    ("latitude", "REAL NOT NULL"),
                    ("longitude", "REAL NOT NULL"),
                    ("precision", "REAL NOT NULL"),
                    ("globe_id", "INTEGER NOT NULL"),
                ],
            ),
            Table::Quantity { .. } => (
                "quantity",
                vec![
                    ("amount", "REAL NOT NULL"),
                    ("lower_bound", "REAL"),
                    ("upper_bound", "REAL"),
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

        // Lastly, we have to extend the primary keys with the rest of the body of the entities.
        // In this manner, we can create as many tables as we wish, all of them following the
        // previously described inheritance policy :D

        columns.append(&mut value_columns);

        (table_name, columns)
    }

    pub fn create_table(&self, connection: &PooledConnection<DuckdbConnectionManager>) -> duckdb::Result<()> {
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

    pub fn create_indices(&self, connection: &PooledConnection<DuckdbConnectionManager>) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();

        for (column_name, _) in columns {
            // We are interested in creating indices only for two columns: src_id and dst_id. Hence,
            // we check if the column_name is any of those. In some previous version loads of clutter
            // was created by creating indices for all the columns :(
            if column_name == "src_id" || column_name == "dst_id" {
                connection.execute_batch(&format!(
                    "CREATE INDEX IF NOT EXISTS {}_{}_index ON {} ({});",
                    table_name,
                    column_name,
                    table_name,
                    column_name,
                ))?;
            }
        }

        Ok(())
    }

    fn insert(&self, connection: &PooledConnection<DuckdbConnectionManager>, params: impl Params) -> duckdb::Result<()> {
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
        // description of the data model we are creating with this tool

        match self {
            Table::Entity(dst_id) => self.insert(
                connection,
                params![src_id, property_id, dst_id]
            ),
            Table::None => self.insert(
                connection,
                params![src_id, property_id, src_id]
            ),
            Table::Unknown => self.insert(
                connection,
                params![src_id, property_id, src_id]
            ),
            Table::String(string) => self.insert(
                connection,
                params![src_id, property_id, src_id, string]
            ),
            Table::Coordinates {
                latitude,
                longitude,
                precision,
                globe_id,
            } => self.insert(
                connection,
                params![src_id, property_id, src_id, latitude, longitude, precision, globe_id],
            ),
            Table::Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit_id,
            } => self.insert(
                connection,
                params![src_id, property_id, src_id, amount, lower_bound, upper_bound, unit_id],
            ),
            Table::Time {
                time,
                precision
            } => self.insert(
                connection,
                params![src_id, property_id, src_id, time, precision]
            )
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
                globe_id: q_id(globe),
            },
            Item(id) => Self::Entity(q_id(id)),
            Property(id) => Self::Entity(p_id(id)),
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
                unit_id: unit.map(q_id),
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
            Lexeme(id) => Self::Entity(l_id(id)),
            Form(id) => Self::Entity(f_id(id)),
            Sense(id) => Self::Entity(s_id(id)),
            NoValue => Self::None,
            UnknownValue => Self::Unknown,
        }
    }
}
