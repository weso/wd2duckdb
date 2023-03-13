mod id;
mod value;

use clap::Parser;
use duckdb::{params, Connection, Error, Transaction};
use lazy_static::lazy_static;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use wikidata::{Entity, Lang, Rank};

use crate::id::{l_id, p_id, q_id};
use crate::value::{Value, VALUE_TYPES};

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
    transaction // TODO: fix this two into one :(
        .execute_batch("CREATE TABLE meta(id INTEGER NOT NULL, label TEXT, description TEXT);")?;

    for table in VALUE_TYPES.iter() {
        table.create_table(transaction)?;
    }

    Ok(())
}

fn create_indices(transaction: &Transaction) -> duckdb::Result<()> {
    transaction.execute_batch(
        "
        CREATE INDEX meta_id_index ON meta (id);
        CREATE INDEX meta_label_index ON meta (label);
        CREATE INDEX meta_description_index ON meta (description);
        ",
    )?;

    for value_type in VALUE_TYPES.iter() {
        value_type.create_indices(transaction)?;
    }

    Ok(())
}

fn store_entity(transaction: &Transaction, entity: Entity) -> Result<(), Error> {
    use wikidata::WikiId::*;

    let id = match entity.id {
        EntityId(id) => q_id(id),
        PropertyId(id) => p_id(id),
        LexemeId(id) => l_id(id),
    };

    transaction
        .prepare_cached("INSERT INTO meta(id, label, description) VALUES (?1, ?2, ?3)")?
        .execute(params![
            // Allows the use of heterogeneous data as parameters to the prepared statement
            id,                             // identifier of the entity
            entity.labels.get(&LANG),       // label of the entity for a certain language
            entity.descriptions.get(&LANG), // description of the entity for a certain language
        ])?;

    for (property_id, claim_value) in entity.claims {
        // In case the claim value stores some outdated or wrong information, we ignore it. The
        // deprecated annotation indicates that this piece of information should be ignored
        if claim_value.rank != Rank::Deprecated {
            Value::from(claim_value.data).store(transaction, id, p_id(property_id))?;
        }
    }

    Ok(())
}

fn insert_entities(transaction: &Transaction, lines: Vec<String>) -> Result<(), Error> {
    let mut line_number = 0;

    for mut line in lines {
        // We have to remove the delimiters so the JSON parsing is performed in a safe environment
        if line.is_empty() || line.trim() == "[" || line.trim() == "]" {
            continue;
        }

        // We increase the line count by 1. Thus, errors can be prompted in a prettier way, indicating
        // where in the document the error was invoked
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
            eprintln!("Error storing entity at line {}: {}", line_number, error);
            continue;
        }
    }
    Ok(())
}

fn main() -> Result<(), String> {
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
        return Err("Cannot open an already created database".to_string());
    }

    // We open a database connection. We are attempting to put the outcome of the JSON processing
    // into a .db file. As a result, the data must be saved to disk. In fact, the result will be
    // saved in the path specified by the user
    let mut connection = match Connection::open(database_path) {
        Ok(connection) => connection,
        Err(error) => return Err(format!("Error opening connection. {}", error)),
    };

    // --**-- BEGIN TRANSACTION --**--
    let transaction = match connection.transaction() {
        Ok(transaction) => transaction,
        Err(error) => return Err(format!("Error creating Transaction. {}", error)),
    };

    if let Err(error) = create_tables(&transaction) {
        return Err(format!("Error creating tables. {}", error));
    }

    if let Err(error) = insert_entities(&transaction, lines) {
        return Err(format!("Error parsing Entity. {}", error));
    }

    if let Err(error) = create_indices(&transaction) {
        return Err(format!("Error creating indices. {}", error));
    }

    if let Err(error) = transaction.commit() {
        return Err(format!("Error committing transaction. {}", error));
    };
    // --**-- END TRANSACTION --**--

    return match connection.close() {
        Ok(..) => Ok(()),
        Err(error) => Err(format!("Error terminating connection. {}", error.1)),
    };
}
