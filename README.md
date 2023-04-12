# `wd2duckdb`

`wd2duckdb` is a tool transforming
[Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) JSON dumps
into a fully indexed DuckDB database ~80% smaller than the original
dump, yet contains most of its information. The resulting database enables
high-performance queries to be executed on commodity hardware without the
need to install and configure specialized triplestore software. This project
is heavily based on [wd2sql](https://github.com/p-e-w/wd2sql).

## Installation

TBD

## Usage

```
wd2duckdb --json <JSON_FILE> --database <DUCKDB_FILE>
```

Use `-` as `<JSON_FILE>` to read from standard input instead of from a file.
This makes it possible to build a pipeline that processes JSON data as it is
being decompressed, without having to decompress the full dump to disk:

```
bzcat latest-all.json.bz2 | wd2duckdb --json - --database <DUCKDB_FILE>
```

## Database structure

<p align="center">
  <img src="https://user-images.githubusercontent.com/65736636/231005674-15ea422c-2830-4c1d-a925-3da16da79b39.png" />
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
