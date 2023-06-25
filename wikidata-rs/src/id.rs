use crate::dtype::DataType;
use wikidata::{Fid, Lid, Pid, Qid, Sid};

/// The `Id` enum is defining different types of identifiers that can be used in the
/// Wikidata database. Each variant of the enum corresponds to a different type of
/// identifier: `Fid` for a form ID, `Lid` for a lexeme ID, `Pid` for a property ID,
/// `Qid` for a item ID, and `Sid` for a sense ID. This enum is used to represent
/// and manipulate these different types of IDs in the code.
pub enum Id {
    Fid(Fid),
    Lid(Lid),
    Pid(Pid),
    Qid(Qid),
    Sid(Sid),
    DataType(DataType),
}

/// This code defines a conversion function from a string slice (`&str`) to an `Id`
/// enum. The function takes a string slice as input and matches the first character
/// of the string to determine the type of ID. If the first character is "L", "P",
/// "Q", "F", or "S", the function creates a corresponding `Lid`, `Pid`, `Qid`,
/// `Fid`, or `Sid` value and returns it wrapped in the `Id` enum. If the first
/// character is anything else, the function panics with an error message. The
/// function is implemented using the `From` trait, which allows for automatic
/// conversion between types.
impl<'a> From<&'a str> for Id {
    fn from(value: &'a str) -> Self {
        match value.get(0..1) {
            Some("L") => Self::Lid(Lid(value[1..].parse::<u64>().unwrap())),
            Some("P") => Self::Pid(Pid(value[1..].parse::<u64>().unwrap())),
            Some("Q") => Self::Qid(Qid(value[1..].parse::<u64>().unwrap())),
            Some("F") => {
                let mut parts = value[1..].split('-');
                Self::Fid(Fid(
                    Lid(parts.next().unwrap().parse::<u64>().unwrap()),
                    parts.next().unwrap()[1..].parse::<u16>().unwrap(),
                ))
            }
            Some("S") => {
                let mut parts = value[1..].split('-');
                Self::Sid(Sid(
                    Lid(parts.next().unwrap().parse::<u64>().unwrap()),
                    parts.next().unwrap()[1..].parse::<u16>().unwrap(),
                ))
            }
            Some("@") => match &value[1..] {
                "Quantity" => Self::DataType(DataType::Quantity),
                "Coordinate" => Self::DataType(DataType::Coordinate),
                "String" => Self::DataType(DataType::String),
                "DateTime" => Self::DataType(DataType::DateTime),
                "Entity" => Self::DataType(DataType::Entity),
                &_ => panic!("Unknown data type: {}", value),
            },
            _ => panic!("Not valid value: {}", value),
        }
    }
}

/// This code defines a conversion function from an `Id` enum to a `u32` integer.
/// The function takes an `Id` value as input and matches on its variant to
/// determine the type of ID. Depending on the type of ID, the function performs a
/// different calculation to convert it to a `u32` integer. For example, if the `Id`
/// is a `Fid` (form ID), the function converts its corresponding `Lid` (lexeme ID)
/// to a `u32` integer and adds the form ID's numeric suffix multiplied by 100
/// billion. The resulting `u32` integer is returned. This conversion function
/// allows for easy comparison and manipulation of different types of IDs in the
/// code.
impl From<Id> for u32 {
    fn from(id: Id) -> Self {
        match id {
            Id::Fid(fid) => u32::from(Id::Lid(fid.0)) + (fid.1 as u32 + 3_000_000_000),
            Id::Lid(lid) => lid.0 as u32 + 2_000_000_000,
            Id::Pid(pid) => pid.0 as u32 + 1_000_000_000,
            Id::Qid(qid) => qid.0 as u32,
            Id::Sid(sid) => {
                u32::from(Id::Lid(sid.0)) + (sid.1 as u32 + 3_000_000_000) + 500_000_000
            }
            Id::DataType(dt) => u8::from(&dt) as u32 + 4_000_000_000,
        }
    }
}
