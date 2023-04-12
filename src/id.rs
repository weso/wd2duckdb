// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2022  Philipp Emanuel Weidmann <pew@worldwidemann.com>

use wikidata::{Fid, Lid, Pid, Qid, Sid};

/// The function `q_id` takes a `Qid` struct and returns its `id` field as a `u64`
/// value.
///
/// Arguments:
///
/// * `id`: The parameter `id` is of type `Qid`, which is a tuple struct containing
/// a single field of type `u64`. The function `q_id` takes this `id` parameter and
/// returns the value of its first field, which is of type `u64`.
///
/// Returns:
///
/// The function `q_id` is returning an unsigned 64-bit integer which is the value
/// of the first field (`id.0`) of the `Qid` struct passed as an argument.
pub fn q_id(id: Qid) -> u64 {
    id.0
}

/// The function takes a Pid struct as input and returns its ID plus 1 billion.
///
/// Arguments:
///
/// * `id`: The parameter `id` is of type `Pid`, which is likely a custom struct or
/// type defined elsewhere in the codebase. The function `p_id` takes an instance of
/// `Pid` as input and returns an unsigned 64-bit integer. The implementation of the
/// function adds the value of the
///
/// Returns:
///
/// The function `p_id` takes a parameter `id` of type `Pid` and returns an unsigned
/// 64-bit integer. The returned value is the sum of the first element of the tuple
/// `id` and `1_000_000_000`.
pub fn p_id(id: Pid) -> u64 {
    id.0 + 1_000_000_000
}

/// The function takes an input of type `Lid` and returns an output of type `u64` by
/// adding 2 billion to the value of the input.
///
/// Arguments:
///
/// * `id`: The parameter `id` is of type `Lid`, which is a tuple struct with a
/// single field of type `u64`.
///
/// Returns:
///
/// The function `l_id` takes an argument of type `Lid` and returns an unsigned
/// 64-bit integer. The returned value is the sum of the first element of the tuple
/// `id` and the constant value `2_000_000_000`.
pub fn l_id(id: Lid) -> u64 {
    id.0 + 2_000_000_000
}

/// The function takes a tuple of two values and returns a u64 value by adding the
/// `LexemeId` to the `FormId` multiplied by 100 billion.
///
/// Arguments:
///
/// * `id`: The parameter `id` is of type `Fid`, which takes two parameters, a
/// `LexemeId` and a `FormId`.
///
/// Returns:
///
/// The function `f_id` takes a tuple `id` of two values, where the first value is
/// of type `u32` and the second value is of type `u8`. The function returns a value
/// of type `u64` which is the result of adding the output of the function `l_id`
/// when called with the first value of the tuple `id.0`, and the `FormId` multiplied
/// by 100 billion.
pub fn f_id(id: Fid) -> u64 {
    l_id(id.0) + (id.1 as u64 * 100_000_000_000)
}

/// The function takes a tuple of two values and returns a u64 value by adding the
/// `LexemeId` to the `SenseId` multiplied by 100 billion and to 10 million.
///
///
/// Arguments:
///
/// * `id`: The parameter `id` is of type `Sid`, which takes two parameters, a
/// `LexemeId` and a `SenseId`.
///
/// Returns:
///
/// The function `s_id` is returning an unsigned 64-bit integer.
pub fn s_id(id: Sid) -> u64 {
    l_id(id.0) + (id.1 as u64 * 100_000_000_000) + 10_000_000_000
}
