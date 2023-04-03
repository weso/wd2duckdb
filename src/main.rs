#![feature(byte_slice_trim_ascii)]

mod id;
mod value;

use std::fs::File;
use clap::Parser;
use duckdb::{params, DuckdbConnectionManager, Error};
use humantime::format_duration;
use lazy_static::lazy_static;
use r2d2::{Pool, PooledConnection};
use rayon::prelude::*;
use std::io::{BufRead, BufReader, stdout, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use wikidata::{Entity, Lang, Rank};

use crate::id::{l_id, p_id, q_id};
use crate::value::Table;

// Allows the declaration of Global variables using functions inside of them. In this case,
// lazy_static! environment allows calling the to_owned function
lazy_static! {
    static ref LANG: Lang = Lang("en".to_owned());
    static ref CHUNK_SIZE: usize = 50_000_000;
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

fn create_tables(connection: &PooledConnection<DuckdbConnectionManager>) -> Result<(), Error> {
    connection // TODO: fix this two into one? :(
        .execute_batch(
            "CREATE TABLE meta(id INTEGER NOT NULL, label TEXT, description TEXT);",
        )?;

    for table in Table::iterator() {
        table.create_table(connection)?;
    }

    Ok(())
}

fn create_indices(connection: &PooledConnection<DuckdbConnectionManager>) -> duckdb::Result<()> {
    connection.execute_batch(
        // TODO: fix this two into one? :(
        "
        CREATE INDEX meta_id_index ON meta (id);
        CREATE INDEX meta_label_index ON meta (label);
        CREATE INDEX meta_description_index ON meta (description);
        ",
    )?;

    for table in Table::iterator() {
        table.create_indices(connection)?;
    }

    Ok(())
}

fn store_entity(connection: &PooledConnection<DuckdbConnectionManager>, entity: Entity) -> Result<(), Error> {
    use wikidata::WikiId::*;

    let id = match entity.id {
        EntityId(id) => q_id(id),
        PropertyId(id) => p_id(id),
        LexemeId(id) => l_id(id),
    };

    // TODO: fix this two into one? :(
    connection
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
            Table::from(claim_value.data).store(connection, id, p_id(property_id))?;
        }
    }

    Ok(())
}

fn insert_entity(
    connection: &Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    mut line: String,
    line_number: i32
) -> Result<(), String> {
    let conn = match connection {
        Ok(connection) => connection,
        Err(error) => return Err(format!("Error opening connection. {}", error)),
    };

    // We have to remove the delimiters so the JSON parsing is performed in a safe environment. For
    // us to do so, we remove possible blanks both at the end and at the beginning of each line.
    // After such, we check if the line is empty or any of the possible delimiters ('[' or ']').
    // Hence, what we are ensuring is that the JSON line is as safe as possible
    line = line.trim().parse().unwrap(); //
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

    let value = match unsafe { simd_json::from_str(&mut line) } {
        Ok(value) => value,
        Err(error) => return Err(format!("Error parsing JSON at line {}: {}", line_number, error))
    };

    let entity = match Entity::from_json(value) {
        Ok(entity) => entity,
        Err(error) => return Err(format!("Error parsing Entity at line {}: {:?}", line_number, error))
    };

    if let Err(error) = store_entity(conn, entity) {
        return Err(format!("Error storing entity at line {}: {}", line_number, error));
    }

    Ok(())
}

fn print_progress(line_number: i32, start_time: Instant) -> () {
    print!(
        "\x1B[2K\r{} entities processed in {}.",
        line_number,
        format_duration(Duration::new(start_time.elapsed().as_secs(), 0))
    );
    let _ = stdout().flush();
}

fn main() -> Result<(), String> {
    let args: Args = Args::parse();

    // We have to check if the database already exists; that is, if the file given by the user is
    // an already existing file, an error is prompted in screen; execution is resumed otherwise
    let database_path: &Path = Path::new(&args.database);
    if database_path.exists() {
        return Err("Cannot open an already created database".to_string());
    }

    // We open the JSON file. Notice that some error handling has to be performed as errors may
    // occur in the process of opening the file provided by the user :(
    let json_file = match File::open(&args.json) {
        Ok(file) => file,
        Err(error) => return Err(format!("Error opening JSON file. {}", error)),
    };
    let reader = BufReader::new(json_file);

    // We open a database connection. We are attempting to put the outcome of the JSON processing
    // into a .db file. As a result, the data must be saved to disk. In fact, the result will be
    // saved in the path specified by the user
    let manager = match DuckdbConnectionManager::file(database_path) {
        Ok(manager) => manager,
        Err(error) => return Err(format!("Error creating the DuckDB connection manager. {}", error)),
    };
    let pool =  match Pool::new(manager) {
        Ok(pool) => pool,
        Err(error) => return Err(format!("Error creating the connection pool. {}", error)),
    };
    let connection = match pool.get() {
        Ok(connection) => connection,
        Err(error) => return Err(format!("Error opening connection. {}", error)),
    };

    let start_time = Instant::now();

    if let Err(error) = create_tables(&connection) {
        return Err(format!("Error creating tables. {}", error));
    }

    reader
        .lines()
        .par_bridge()
        .for_each(
            |line|
                if let Err(error) =  insert_entity( & pool.get(), line.unwrap(), 0) {
                    eprintln!("Error inserting entity. {}", error);
                }
        );

    if let Err(error) = create_indices(&connection) {
        return Err(format!("Error creating indices. {}", error));
    }

    print_progress(0, start_time);

    Ok(())
}
