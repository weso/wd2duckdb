#![feature(byte_slice_trim_ascii)]

mod id;
mod value;

use clap::Parser;
use duckdb::{params, DuckdbConnectionManager, Error};
use humantime::format_duration;
use lazy_static::lazy_static;
use r2d2::{Pool, PooledConnection};
use std::fs::File;
use std::io::{stdin, stdout, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use wikidata::{Entity, Lang, Rank};

use crate::id::Id;
use crate::value::Table;

// Allows the declaration of Global variables using functions inside of them. In this case,
// lazy_static! environment allows calling the to_owned function
lazy_static! {
    static ref LANG: Lang = Lang("en".to_owned());
    static ref CHUNK_SIZE: usize = 50_000_000;
    static ref INSERTS_PER_TRANSACTION: usize = 1_000;
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

/// The function creates tables in a database connection using SQL queries.
///
/// Arguments:
///
/// * `connection`: The `connection` parameter is a reference to a
/// `PooledConnection` object from the `DuckdbConnectionManager` type. This object
/// represents a connection to a DuckDB database and is used to execute SQL queries
/// and commands on that database. The `create_tables` function uses this connection
/// to create the tables according to the Wikidata entity data model.
///
/// Returns:
///
/// The function `create_tables` is returning a `Result` with an empty tuple `()` as
/// the success value and an `Error` as the error value.
fn create_tables(connection: &PooledConnection<DuckdbConnectionManager>) -> Result<(), Error> {
    connection.execute_batch("CREATE TABLE vertex (id INTEGER, label TEXT, description TEXT);")?;

    for table in Table::iterator() {
        table.create_table(connection)?;
    }

    Ok(())
}

/// The function creates an index for the id column in the vertices table and calls
/// the create_indices function for other tables.
///
/// Arguments:
///
/// * `connection`: A reference to a `PooledConnection` object from the
/// `DuckdbConnectionManager` type, which represents a connection to a DuckDB
/// database.
///
/// Returns:
///
/// The function `create_indices` is returning a `duckdb::Result<()>`, which is a
/// type alias for `Result<(), duckdb::Error>`. This means that the function returns
/// a result that can either be Ok(()) if the execution was successful, or an error
/// of type `duckdb::Error` if something went wrong.
fn create_indices(connection: &PooledConnection<DuckdbConnectionManager>) -> duckdb::Result<()> {
    // We are interested only in creating an index for the id column in the vertices table, as we
    // will only query over it. The rest of the data that is stored just extends the knowledge that
    // we store, but has no relevance in regards with future processing :D
    connection.execute_batch("CREATE INDEX vertex_id_index ON vertex (id);")?;

    for table in Table::iterator() {
        table.create_indices(connection)?;
    }

    Ok(())
}

/// The function stores an entity and its associated properties in a database.
///
/// Arguments:
///
/// * `connection`: A connection to a DuckDB database, which is used to execute SQL
/// queries.
///
/// * `entity`: The `entity` parameter is an instance of the `Entity` struct, which
/// represents a Wikidata entity (such as an item, property, or lexeme) and contains
/// information about its labels, descriptions, and claims. The function
/// `store_entity` takes this entity and stores its information in a DuckDB database.
///
/// Returns:
///
/// The function `store_entity` returns a `Result` with an empty tuple `()` as the
/// success value or an `Error` if an error occurs during the execution of the
/// function.
fn store_entity(
    connection: &PooledConnection<DuckdbConnectionManager>,
    entity: Entity,
) -> Result<(), String> {
    use wikidata::WikiId::*;

    let src_id = u64::from(match entity.id {
        EntityId(id) => Id::Qid(id),
        PropertyId(id) => Id::Pid(id),
        LexemeId(id) => Id::Lid(id),
    });

    // TODO: try to check for the error here ExpectedString
    let mut insert_into = match connection
        .prepare_cached("INSERT INTO vertex (id, label, description) VALUES (?1, ?2, ?3)")
    {
        Ok(insert_into) => insert_into,
        Err(error) => return Err(format!("Error preparing statement: {:?}", error)),
    };

    if let Err(error) = insert_into.execute(params![
        // Allows the use of heterogeneous data as parameters to the prepared statement
        src_id,                         // identifier of the entity
        entity.labels.get(&LANG),       // label of the entity for a certain language
        entity.descriptions.get(&LANG), // description of the entity for a certain language
    ]) {
        return Err(format!("Error inserting into TABLE VERTEX: {:?}", error));
    };

    for (property_id, claim_value) in entity.claims {
        // In case the claim value stores some outdated or wrong information, we ignore it. The
        // deprecated annotation indicates that this piece of information should be ignored
        if claim_value.rank != Rank::Deprecated {
            if let Err(error) = Table::from(claim_value.data).store(
                connection,
                src_id,
                u64::from(Id::Pid(property_id)),
            ) {
                return Err(format!("Error inserting into TABLE: {:?}", error));
            }
        }
    }

    Ok(())
}

/// The function parses a JSON string, transforms it into a Wikidata entity, and
/// stores it in a database.
///
/// Arguments:
///
/// * `connection`: A reference to a connection to a database, specifically a DuckDB
/// database, wrapped in a `Result` type that can either contain the connection or
/// an error if the connection could not be established.
///
/// * `line`: A string representing a single line of a Wikidata dump file, which
/// contains a JSON object representing a Wikidata entity.
///
/// * `line_number`: The line number parameter represents the line number of the
/// current line being processed in a file. It is used for error reporting purposes,
/// to help identify which line caused an error if one occurs during the processing
/// of the file.
///
/// Returns:
///
/// a `Result` with an empty tuple `()` as the success value and a `String` as the
/// error value.
fn insert_entity(
    connection: &Result<PooledConnection<DuckdbConnectionManager>, r2d2::Error>,
    mut line: String,
    line_number: u32,
) -> Result<(), String> {
    // We try to open a connection. This should be done as we are in the context of a multi-threaded
    // program. Thus, for us to avoid races, a new connection from the pool has to be retrieved :D
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

    // By using simd_json we parse the string to a Value. In this regard, the line has to be a valid
    // JSON by itself. As we are sure that Wikidata dumps are an enumeration of JSON objects: one
    // per line in the document, we can use this algorithm for retrieving each entity in the dump
    let value = match serde_json::from_str(&line) {
        Ok(value) => value,
        Err(error) => {
            return Err(format!(
                "Error parsing JSON at line {}: {}",
                line_number, error
            ))
        }
    };

    // Once we have the JSON value parsed, we try to transform it into a Wikidata entity, that will
    // be stored later. This is basically the same object as before, but arranged in a better manner
    let entity = match Entity::from_json(value) {
        Ok(entity) => entity,
        Err(error) => {
            return Err(format!(
                "Error parsing Entity at line {}: {:?}",
                line_number, error
            ))
        }
    };

    if let Err(error) = store_entity(conn, entity) {
        return Err(format!(
            "Error storing entity at line {}: {}",
            line_number, error
        ));
    }

    Ok(())
}

/// The function prints the progress of entity processing with the current line
/// number and elapsed time.
///
/// Arguments:
///
/// * `line_number`: An integer representing the current line number or the number
/// of entities processed so far.
///
/// * `start_time`: `start_time` is a variable of type `Instant` which represents
/// the point in time when a certain process started. It is used in the
/// `print_progress` function to calculate the elapsed time since the process
/// started.
fn print_progress(line_number: u32, start_time: Instant) {
    print!(
        "\x1B[2K\r{} entities processed in {}.",
        line_number,
        format_duration(Duration::new(start_time.elapsed().as_secs(), 0))
    );
    let _ = stdout().flush();
}

/// This function reads a JSON file, creates a new DuckDB database, and inserts the
/// data from the JSON file into the database in parallel.
///
/// Returns:
///
/// a `Result` type with the `Ok` variant containing an empty tuple `()` and the
/// `Err` variant containing a `String` with an error message if any error occurs
/// during the execution of the function.
fn main() -> Result<(), String> {
    let args: Args = Args::parse();

    // We have to check if the database already exists; that is, if the file given by the user is
    // an already existing file, an error is prompted in screen and execution is halted; otherwise,
    // execution is resumed :D
    let database_path: &Path = Path::new(&args.database);
    if database_path.exists() {
        return Err("Cannot open an already created database".to_string());
    }

    // We open the JSON file. Notice that some error handling has to be performed as errors may
    // occur in the process of opening the file provided by the user. More in more, we have to
    // check if the file is the standard input or a file in the file system. In the first case, we
    // use the standard input as the reader; otherwise, we use the file provided by the user :D
    let reader: Box<dyn Read + Send> = if args.json == "-" {
        Box::new(stdin())
    } else {
        Box::new(match File::open(&args.json) {
            Ok(file) => file,
            Err(error) => return Err(format!("Error opening JSON file. {}", error)),
        })
    };
    let reader = BufReader::new(reader);

    // We open a database connection. We are attempting to put the outcome of the JSON processing
    // into a .duckdb file. As a result, the data must be saved to disk. In fact, the result will be
    // saved in the path specified by the user. Some IOErrors may occurs and should be handled
    let manager = match DuckdbConnectionManager::file(database_path) {
        Ok(manager) => manager,
        Err(error) => {
            return Err(format!(
                "Error creating the DuckDB connection manager. {}",
                error
            ))
        }
    };
    let pool = match Pool::new(manager) {
        Ok(pool) => pool,
        Err(error) => return Err(format!("Error creating the connection pool. {}", error)),
    };
    let connection = match pool.get() {
        Ok(connection) => connection,
        Err(error) => return Err(format!("Error opening connection. {}", error)),
    };

    // -*- JSON to .DUCKDB ALGORITHM Starts here -*-

    // We start computing the initial time at which it starts the execution of the algorithm
    let start_time = Instant::now();

    // We create the tables of the database so the elements can be inserted. For us to do so, we
    // are creating one table per each primitive type that can be stored in Wikidata. For more
    // details, refer to value.rs file in this same directory
    if let Err(error) = create_tables(&connection) {
        return Err(format!("Error creating tables. {}", error));
    }

    match pool.get() {
        Ok(connection) => {
            if let Err(error) = connection.execute_batch("BEGIN TRANSACTION;") {
                return Err(format!("Error starting transaction: {}", error));
            }
        }
        Err(error) => return Err(format!("Error opening connection. {}", error)),
    };

    reader
        .lines() // we retrieve the iterator over the lines in the
        .enumerate() // we enumerate the iterator so we can know the line number
        .for_each(
            // for each line in the parallel iterator ...
            |(line_number, line)| {
                // try to insert the entity in the database and handle errors appropriately
                if let Err(error) = insert_entity(&pool.get(), line.unwrap(), line_number as u32) {
                    // do not halt execution in case an error happens, just warn the user :D
                    eprintln!("Error inserting entity. {}", error);
                }

                // Transactions can improve performance by reducing the number of disk
                // writes and network round trips. When you wrap multiple inserts within a transaction,
                // the database can optimize the write operations by batching them together and
                // committing them as a single unit. This can reduce the overhead of repeated disk I/O
                // operations and improve overall insert speed.
                if line_number > 0 && line_number % INSERTS_PER_TRANSACTION.to_owned() == 0 {
                    if let Ok(connection) = &pool.get() {
                        if let Err(error) = connection.execute_batch(
                            "
                            END TRANSACTION;
                            BEGIN TRANSACTION;
                            ",
                        ) {
                            eprintln!(
                                "\nError committing transaction at line {}: {}",
                                line_number, error,
                            );
                        }
                        print_progress(line_number as u32, start_time);
                    } else {
                        eprintln!("Error creating the connection for the transaction");
                    }
                }
            },
        );

    if let Err(error) = connection.execute_batch("END TRANSACTION;") {
        return Err(format!("Error committing transaction: {}", error));
    }

    if let Err(error) = create_indices(&connection) {
        return Err(format!("Error creating indices. {}", error));
    }

    // -*- JSON to .DUCKDB ALGORITHM Ends here -*-

    print_progress(0, start_time);

    Ok(())
}
