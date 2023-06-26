#![feature(byte_slice_trim_ascii)]

use clap::Parser;
use duckdb::{params, Connection, DropBehavior, Error};
use humantime::format_duration;
use std::fs::File;
use std::io::{stdin, stdout, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use wikidata::{Entity, Rank};

use wikidata_rs::id::Id;
use wikidata_rs::value::AppenderHelper;
use wikidata_rs::value::Table;
use wikidata_rs::{INSERTS_PER_TRANSACTION, LANG};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOCATOR: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
fn create_tables(connection: &mut Connection) -> Result<(), Error> {
    let transaction = match connection.transaction() {
        Ok(transaction) => transaction,
        Err(_) => return Err(Error::AppendError),
    };

    for table in Table::iterator() {
        table.create_table(&transaction)?;
    }

    transaction.commit()
}

/// This function creates indices for the id column in the vertices table.
///
/// Arguments:
///
/// * `transaction`: A reference to a Transaction object, which is used to perform
/// database operations.
///
/// Returns:
///
/// The function `create_indices` returns a `Result` enum with either an `Ok(())`
/// value indicating that the function executed successfully, or an `Err` value
/// containing an `Error` object if an error occurred during execution.
fn create_indices(connection: &Connection) -> Result<(), Error> {
    // We are interested only in creating an index for the id column in the vertices table, as we
    // will only query over it. The rest of the data that is stored just extends the knowledge that
    // we store, but has no relevance in regards with future processing :D
    for table in Table::iterator() {
        table.create_indices(connection)?;
    }
    Ok(())
}

/// The function parses and stores Wikidata entities from a JSON dump file.
///
/// Arguments:
///
/// * `appender_helper`: A mutable reference to an AppenderHelper struct, which is
/// used to append entities to a storage backend.
///
/// * `line`: A string representing a line of JSON data from a Wikidata dump file.
///
/// * `line_number`: The line number of the current line being processed in the
/// input file.
///
/// Returns:
///
/// a `Result` type with the `Ok` variant containing an empty tuple `()` if the
/// function executes successfully, and the `Err` variant containing a `String` with
/// an error message if an error occurs during execution.
fn insert_entity(
    appender_helper: &mut AppenderHelper,
    mut line: String,
    line_number: u32,
) -> Result<(), String> {
    // We have to remove the delimiters so the JSON parsing is performed in a safe environment. For
    // us to do so, we remove possible blanks both at the end and at the beginning of each line.
    // After such, we check if the line is empty or any of the possible delimiters ('[' or ']').
    // Hence, what we are ensuring is that the JSON line is as safe as possible
    line = line.trim().parse().unwrap(); //
    if line.is_empty() || line == "[" || line == "]" {
        return Ok(()); // we just skip the line. It is not needed :D
    }

    // Remove the trailing comma and newline character. This is extremely important for serde_json to
    // process the lines properly. In general, a processing of the lines is required for serde_json
    // to work. We are making sure that the last character is a closing bracket; that is, the line
    // is a valid JSON
    if line.ends_with(',') {
        line.truncate(line.len() - 1);
    }

    // By using simd_json we parse the string to a Value. In this regard, the line has to be a valid
    // JSON by itself. As we are sure that Wikidata dumps are an enumeration of JSON objects: one
    // per line in the document, we can use this algorithm for retrieving each entity in the dump
    let value = match unsafe { simd_json::from_str(&mut line) } {
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

    if let Err(error) = store_entity(appender_helper, entity) {
        return Err(format!(
            "Error storing entity at line {}: {}",
            line_number, error
        ));
    }

    Ok(())
}

/// This function stores entity information in a table, ignoring deprecated
/// information.
///
/// Arguments:
///
/// * `appender_helper`: A mutable reference to an AppenderHelper struct, which is
/// used to append data to a database table.
///
/// * `entity`: An object representing a Wikidata entity, which can be an item,
/// property, or lexeme. It contains information such as the entity's ID, labels,
/// descriptions, and claims (which are statements about the entity, such as its
/// properties and values).
///
/// Returns:
///
/// a `Result` type with either an empty `Ok(())` value indicating success or a
/// `String` value containing an error message in case of failure.
fn store_entity(appender_helper: &mut AppenderHelper, entity: Entity) -> Result<(), String> {
    use wikidata::WikiId::*;

    let src_id = u32::from(match entity.id {
        EntityId(id) => Id::Qid(id),
        PropertyId(id) => Id::Pid(id),
        LexemeId(id) => Id::Lid(id),
    });

    // We are only interested in the English label and description of the entity. This is because
    // the rest of the information is not relevant for the processing that we are going to perform
    // in the future. In this regard, we are only storing the English label and description of the
    // entity in the vertices table of the database :D
    if appender_helper
        .appenders
        .get_mut("vertex")
        .unwrap()
        .append_row(params![
            src_id,
            entity.labels.get(&LANG),
            entity.descriptions.get(&LANG)
        ])
        .is_err()
    {
        return Err(format!("Error inserting into VERTEX: {:?}", entity.id));
    }

    for (property_id, claim_value) in entity.claims {
        // In case the claim value stores some outdated or wrong information, we ignore it. The
        // deprecated annotation indicates that this piece of information should be ignored
        if claim_value.rank != Rank::Deprecated {
            if let Err(error) = Table::from(claim_value.data).insert(
                appender_helper,
                src_id, // identifier of the entity
                u32::from(Id::Pid(property_id)),
            ) {
                return Err(format!("Error inserting into TABLE: {:?}", error));
            }
        }
    }

    Ok(())
}

/// The function prints the progress of entity processing with the line number and
/// elapsed time.
///
/// Arguments:
///
/// * `line_number`: An unsigned 32-bit integer representing the current line number
/// being processed.
///
/// * `start_time`: The `start_time` parameter is an instance of the `Instant`
/// struct, which represents a point in time. It is used to calculate the duration
/// of time that has elapsed since a certain point in time, which is typically the
/// start of a process or operation. In this case, it is used
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
    let mut connection = match Connection::open(database_path) {
        Ok(connection) => connection,
        Err(error) => return Err(format!("Error opening connection. {}", error)),
    };

    // -*- JSON to .DUCKDB ALGORITHM Starts here -*-

    // We start computing the initial time at which it starts the execution of the algorithm
    let start_time = Instant::now();

    // We create the tables of the database so the elements can be inserted. For us to do so, we
    // are creating one table per each primitive type that can be stored in Wikidata. For more
    // details, refer to value.rs file in this same directory
    if let Err(error) = create_tables(&mut connection) {
        return Err(format!("Error creating tables. {}", error));
    }

    if let Err(error) = create_indices(&connection) {
        return Err(format!("Error creating indices. {}", error));
    }

    // Transactions can improve performance by reducing the number of disk
    // writes and network round trips. When you wrap multiple inserts within a transaction,
    // the database can optimize the write operations by batching them together and
    // committing them as a single unit. This can reduce the overhead of repeated disk I/O
    // operations and improve overall insert speed.
    let mut transaction = match connection.transaction() {
        Ok(transaction) => transaction,
        Err(error) => return Err(format!("Error opening transaction. {}", error)),
    };

    // We set the drop behavior to commit so that the transaction is committed when it is dropped.
    transaction.set_drop_behavior(DropBehavior::Commit);

    // Appenders also allow inserting entities in a better fashion. This allows a faster
    // performance and an easier implementation of the algorithm
    let mut appender_helper = AppenderHelper::new(&transaction);
    reader
        .lines() // we retrieve the iterator over the lines in the
        .enumerate() // we enumerate the iterator so we can know the line number
        .for_each(|(line_number, line)| {
            // try to insert the entity in the database and handle errors appropriately
            if let Err(error) =
                insert_entity(&mut appender_helper, line.unwrap(), line_number as u32)
            {
                // do not halt execution in case an error happens, just warn the user :D
                eprintln!("Error inserting entity. {}", error);
            }

            if line_number > 0 && line_number % INSERTS_PER_TRANSACTION.to_owned() == 0 {
                print_progress(line_number as u32, start_time);
            }
        });

    // -*- JSON to .DUCKDB ALGORITHM Ends here -*-

    Ok(())
}
