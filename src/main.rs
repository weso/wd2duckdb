mod id;
mod value;

use crate::value::VALUE_TYPES;
use clap::Parser;
use duckdb::{params, Connection, Error, Transaction};
use lazy_static::lazy_static;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use wikidata::{Entity, Lang};

// Allows the declaration of Global variables using functions inside of them. In this case,
// lazy_static! environment allows calling the to_owned function.
lazy_static! {
    static ref LANG: Lang = Lang("en".to_owned());
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input JSON file
    #[arg(short, long)]
    json: String,

    /// File of the output database
    #[arg(short, long)]
    database: String,
}

fn create_tables(transaction: &Transaction) -> Result<(), Error> {
    // TODO: make this work with transaction and all in one?
    transaction
        .execute_batch("CREATE TABLE meta(id INTEGER NOT NULL, label TEXT, description TEXT);")?;

    for value_type in VALUE_TYPES.iter() {
        value_type.create_table(transaction)?;
    }

    Ok(())
}

fn store_entity(transaction: &Transaction, entity: Entity) -> Result<(), Error> {
    use wikidata::WikiId::*;

    transaction
        .prepare_cached("INSERT INTO meta (id, label, description) VALUES (?1, ?2, ?3)")?
        .execute(params![
            match entity.id {
                EntityId(Qid) => Qid.0,
                PropertyId(Pid) => Pid.0,
                LexemeId(Lid) => Lid.0,
            },
            entity.labels.get(&LANG),
            entity.descriptions.get(&LANG),
        ])?;

    Ok(())
}

fn insert_entities(transaction: &Transaction, lines: Vec<String>) -> Result<(), Error> {
    let mut line_number = 0;

    for mut line in lines {
        // TODO: skip delimiters
        if line.is_empty() || line == "[" || line == "]" {
            continue;
        }

        // We increase the line count by 1. Thus, errors can be prompted in a prettier way, indicating
        // where in the document the error was invoked.
        line_number += 1;

        // Remove trailing comma. This is extremely important for simd_json to process the lines
        // properly
        if line.ends_with(',') {
            line.truncate(line.len() - 1);
        }

        let value = match unsafe { simd_json::from_str(&mut line) } {
            Ok(value) => value,
            Err(error) => {
                eprintln!("Error parsing JSON at line {}: {}", line_number, error);
                continue;
            }
        };

        let entity = match Entity::from_json(value) {
            Ok(entity) => entity,
            Err(error) => {
                eprintln!("Error parsing JSON at line {}: {:?}", line_number, error);
                continue;
            }
        };

        if let Err(error) = store_entity(&transaction, entity) {
            eprintln!("\nError storing entity at line {}: {}", line_number, error);
        }
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Args = Args::parse();

    // TODO: check what expect is
    let json_file = File::open(&args.json).expect("Unable to read the given file.");
    let reader = BufReader::new(json_file);
    let lines: Vec<String> = reader
        .lines()
        .map(|l| l.expect("Unable to parse line."))
        .collect();

    let database_path: &Path = Path::new(&args.database);
    if database_path.exists() {
        // TODO: panic?
        panic!("ERROR: Existing Databases cannot be handled by the application.");
    }

    // We open a database connection. We are attempting to put the outcome of the JSON processing
    // into a .db file. As a result, the data must be saved to disk. In fact, the result will be
    // saved in the path specified by the user.
    let mut connection = Connection::open(database_path)?;

    // --**-- BEGIN TRANSACTION --**--
    let transaction = connection.transaction()?;

    if let Err(error) = create_tables(&transaction) {
        eprintln!("Error creating tables: {}", error);
        return Err(error);
    }

    if let Err(error) = insert_entities(&transaction, lines) {
        eprintln!("Error parsing Entity: {}", error);
        return Err(error);
    }

    transaction.commit()?;
    // --**-- END TRANSACTION --**--

    return match connection.close() {
        Ok(..) => Ok(()),
        Err(error) => Err(error.1),
    };
}
