# `wd2duckdb`

`wd2duckdb` is a tool transforming
[Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) JSON dumps
into a fully indexed DuckDB database ~80% smaller than the original
dump, yet contains most of its information. Note that only the English version of
the Wikidata items are stored. For you to change so, please refer to 
[this line in the code](https://github.com/angelip2303/wd2duckdb/blob/777f47d4ed386e79dba0d8529fced0efb78c6325/src/main.rs#LL23C1).
The resulting database enables high-performance queries to be executed on commodity
hardware without the need to install and configure specialized triplestore software. 
This project is heavily based on [wd2sql](https://github.com/p-e-w/wd2sql).

## Installation

Make sure that you install the latest stable version of 
[Rust](https://www.rust-lang.org/); that is, as of May the 5th, version 1.69 or
later, then run:

```
cargo install wd2duckdb
```

This will compile `wd2duckdb` for your native architecture, increasing the performance.

## Usage

```
wd2duckdb --json <JSON_FILE> --database <DUCKDB_FILE>
```

Use `-` as `<JSON_FILE>` to read from standard input instead of from a file.
This makes it possible to build a pipeline that processes JSON data as it is
being decompressed, without having to decompress the full dump to disk. In case
of a `.bz2` file, you can use the following instruction:

```
bzcat latest-all.json.bz2 | wd2duckdb --json - --database <DUCKDB_FILE>
```

In case of a `.gz` compressed file, the following is required:

```
gunzip latest-all.json.gz | wd2duckdb --json - --database <DUCKDB_FILE>
```

In case you want to write changes directly to the standard ouput; that is, without
creating a file for the uncompressed `.json`, you can do the following:

```
gunzip -c latest-all.json.gz | wd2duckdb --json - --database <DUCKDB_FILE>
```

If you are working with large dumps where the uncompressed `.json` file size is in
the order of Terabytes, it is best to choose the last option. The `.duckdb` file,
which is more memory-efficient, may thus be created immediately.

## Database structure

<p align="center">
  <img src="https://github.com/angelip2303/wd2duckdb/assets/65736636/d1380df4-834e-44a6-9b44-b6943ab1afc5" />
</p>

## Acknowledgments

Without the efforts of the countless people who built Wikidata and its
contents, `wd2duckdb` would be useless. It's truly impossible to praise
this amazing open data project enough.

## Related projects

1. [wd2sql](https://github.com/p-e-w/wd2sql) is this project's main 
inspiration.

## License

Copyright &copy; 2023 Ángel Iglesias Préstamo (<angel.iglesias.prestamo@gmail.com>)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

**By contributing to this project, you agree to release your
contributions under the same license.**
