// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2022  Philipp Emanuel Weidmann <pew@worldwidemann.com>

use chrono::{DateTime, Utc};
use duckdb::{params, Params, Transaction};
use lazy_static::lazy_static;
use std::slice::Iter;
use wikidata::ClaimValueData;

use crate::id::{f_id, l_id, p_id, q_id, s_id};
use crate::LANG;

pub enum Table {
    String(String),
    Entity(u64),
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
    None,
    Unknown,
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
                Table::Unknown,
            ];
        }
        TABLES.iter()
    }

    fn table_definition(&self) -> (&str, Vec<(&str, &str)>) {
        use Table::*;

        let mut columns = vec![
            ("id", "INTEGER NOT NULL"),
            ("property_id", "INTEGER NOT NULL"),
        ];

        let (table_name, mut value_columns) = match self {
            String(_) => ("string", vec![("string", "TEXT NOT NULL")]),
            Entity(_) => ("entity", vec![("entity_id", "INTEGER NOT NULL")]),
            Coordinates { .. } => (
                "coordinates",
                vec![
                    ("latitude", "REAL NOT NULL"),
                    ("longitude", "REAL NOT NULL"),
                    ("precision", "REAL NOT NULL"),
                    ("globe_id", "INTEGER NOT NULL"),
                ],
            ),
            Quantity { .. } => (
                "quantity",
                vec![
                    ("amount", "REAL NOT NULL"),
                    ("lower_bound", "REAL"),
                    ("upper_bound", "REAL"),
                    ("unit_id", "INTEGER"),
                ],
            ),
            Time { .. } => (
                "time",
                vec![
                    ("time", "DATETIME NOT NULL"),
                    ("precision", "INTEGER NOT NULL"),
                ],
            ),
            None => ("none", vec![]),
            Unknown => ("unknown", vec![]),
        };

        columns.append(&mut value_columns);

        (table_name, columns)
    }

    pub fn create_table(&self, transaction: &Transaction) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();

        transaction.execute_batch(&format!(
            "CREATE TABLE {} ({});",
            table_name,
            columns
                .iter()
                .map(|(column_name, column_type)| format!("{} {}", column_name, column_type))
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    pub fn create_indices(&self, transaction: &Transaction) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();

        for (column_name, _) in columns {
            transaction.execute_batch(&format!(
                "CREATE INDEX {}_{}_index ON {} ({});",
                table_name, column_name, table_name, column_name,
            ))?;
        }

        Ok(())
    }

    fn insert(&self, transaction: &Transaction, params: impl Params) -> duckdb::Result<()> {
        let (table_name, columns) = self.table_definition();

        transaction
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
        transaction: &Transaction,
        id: u64,
        property_id: u64,
    ) -> duckdb::Result<()> {
        use Table::*;

        match self {
            String(string) => self.insert(transaction, params![id, property_id, string]),
            Entity(entity_id) => self.insert(transaction, params![id, property_id, entity_id]),
            Coordinates {
                latitude,
                longitude,
                precision,
                globe_id,
            } => self.insert(
                transaction,
                params![id, property_id, latitude, longitude, precision, globe_id],
            ),
            Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit_id,
            } => self.insert(
                transaction,
                params![id, property_id, amount, lower_bound, upper_bound, unit_id],
            ),
            Time { time, precision } => {
                self.insert(transaction, params![id, property_id, time, precision])
            }
            None => self.insert(transaction, params![id, property_id]),
            Unknown => self.insert(transaction, params![id, property_id]),
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
