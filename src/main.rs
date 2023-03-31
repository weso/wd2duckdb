mod id;
mod value;
mod my_reader;

use clap::Parser;
use duckdb::{params, Connection, Error, Transaction};
use humantime::format_duration;
use lazy_static::lazy_static;
use std::io::{stdout, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use wikidata::{Entity, Lang, Rank};

use crate::id::{l_id, p_id, q_id};
use crate::my_reader::BufReader;
use crate::value::Table;

// Allows the declaration of Global variables using functions inside of them. In this case,
// lazy_static! environment allows calling the to_owned function
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
    transaction // TODO: fix this two into one? :(
        .execute_batch(
            "CREATE TABLE meta(id INTEGER NOT NULL, label TEXT, description TEXT);",
        )?;

    for table in Table::iterator() {
        table.create_table(transaction)?;
    }

    Ok(())
}

fn create_indices(transaction: &Transaction) -> duckdb::Result<()> {
    transaction.execute_batch(
        // TODO: fix this two into one? :(
        "
        CREATE INDEX meta_id_index ON meta (id);
        CREATE INDEX meta_label_index ON meta (label);
        CREATE INDEX meta_description_index ON meta (description);
        ",
    )?;

    for table in Table::iterator() {
        table.create_indices(transaction)?;
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

    // TODO: fix this two into one? :(
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
            Table::from(claim_value.data).store(transaction, id, p_id(property_id))?;
        }
    }

    Ok(())
}

fn insert_entity(transaction: &Transaction, mut line: String, line_number: i32) -> Result<(), String> {
    // We have to remove the delimiters so the JSON parsing is performed in a safe environment
    line = line.trim().parse().unwrap(); // we remove possible blanks both at the end or at the beginning of each line
    if line.is_empty() || line == "[" || line == "]" {
        return Ok(()); // we just skip the line. It is not needed :D
    }

    // Remove the trailing comma and newline character. This is extremely important for simd_json to
    // process the lines properly. In general, a processing of the lines is required for simd_json
    // to work. We are making sure that the last character is a closing bracket; that is, the line
    // is a valid JSON
    while !line.ends_with('}') {
        line.pop();
    }

    let value = match unsafe { simd_json::from_str( &mut line) } {
        Ok(value) => value,
        Err(error) => return Err(format!("Error parsing JSON at line {}: {}", line_number, error))
    };

    let entity = match Entity::from_json(value) {
        Ok(entity) => entity,
        Err(error) => return Err(format!("Error parsing Entity at line {}: {:?}", line_number, error))
    };

    if let Err(error) = store_entity(&transaction, entity) {
        return Err(format!("Error storing entity at line {}: {}", line_number, error));
    }

    Ok(())
}

fn main() -> Result<(), String> {
    let args: Args = Args::parse();

    let start_time = Instant::now();
    let print_progress = |line_number| {
        print!(
            "\x1B[2K\r{} entities processed in {}.",
            line_number,
            format_duration(Duration::new(start_time.elapsed().as_secs(), 0))
        );
        let _ = stdout().flush();
    };

    // We open the JSON file. Notice that some error handling has to be performed as errors may
    // occur in the process of opening the file provided by the user :(
    let mut buffer = String::new();
    let mut reader = match BufReader::open(args.json) {
        Ok(reader) => reader,
        Err(error) => return Err(format!("Error opening JSON file. {}", error)),
    };

    // We have to check if the database already exists; that is, if the file given by the user is
    // an already existing file, an error is prompted in screen; execution is resumed otherwise
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

    // Once the file is opened, the reader is initialized provided such a file. After so, we start
    // to read such a file line-by-line. That is, provided a JSON line with n lines, we read one at
    // at a time. This is a requirement for us to read huge files that cannot be loaded into memory
    // as a whole. Instead of creating a Vec<String> with all the lines of the file, we read all of
    // them separately :D
    let mut line_number = 0;
    while let Some(line) = reader.read_line(&mut buffer) {
        line_number += 1;

        let line = match line {
            Ok(line) => line.to_owned(),
            Err(error) => {
                eprintln!("Error parsing line {}. {}", line_number, error);
                continue;
            }
        };

        if let Err(error) = insert_entity(&transaction, line, line_number) {
            eprintln!("{}", error);
            continue;
        }
    }

    if let Err(error) = create_indices(&transaction) {
        return Err(format!("Error creating indices. {}", error));
    }

    if let Err(error) = transaction.commit() {
        return Err(format!("Error committing transaction. {}", error));
    };
    // --**-- END TRANSACTION --**--

    print_progress(line_number);

    match connection.close() {
        Ok(_) => Ok(()),
        Err(error) => Err(format!("Error terminating connection. {}", error.1)),
    }
}
