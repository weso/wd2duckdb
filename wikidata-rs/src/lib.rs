use lazy_static::lazy_static;
use wikidata::Lang;

/// `pub mod dtype;` is creating a public module named `dtype`. This module can be
/// accessed from other parts of the codebase and contains code related to data
/// types.
pub mod dtype;
/// `pub mod id;` is creating a public module named `id`. This module
/// contains code related to generating and managing Wikibase unique identifiers
/// or IDs within the codebase.
pub mod id;
/// `pub mod value;` is creating a public module named `value`. This module contains
/// code related to representing and manipulating Wikibase values, such as strings,
/// numbers, and dates.
pub mod value;

// Allows the declaration of Global variables using functions inside of them. In this case,
// lazy_static! environment allows calling the to_owned function
lazy_static! {
    pub static ref LANG: Lang = Lang("en".to_owned());
    pub static ref INSERTS_PER_TRANSACTION: usize = 1_000;
}
