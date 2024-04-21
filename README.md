# Alex

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Alex, allow you to create an ALEX indexing structure for your database tables in DuckDB.


### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/alex/alex.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `alex.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

The commands can be of the form : 

pragma create_alex_index('table_name','col_id');

pragma search_alex('table_name','col_id',key);

Currently working on loading benchmark datasets with the command :

pragma benchmark('lognormal_benchmark','lognormal');

Please download the lognormal dataset and place it in the same directory as alex_extension.cpp to use the benchmark command : otherwise will not work.


```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

