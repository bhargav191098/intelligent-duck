# Alex Extension for DuckDB 

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.


---

The alex extension allows the user to create alex index structure for any integer-based data column of the table.

## Benchmark for YCSB Dataset:

![requests](https://github.com/bhargav191098/intelligent-duck/blob/main/graph-images/ycsb_lookup.png)

![requests](https://github.com/bhargav191098/intelligent-duck/blob/main/graph-images/ycsb_memory_consumption.png)

The Learned Indexing provides great throughput with less memory overhead. Obviously running it on x86 based CPUs allows even further boosts.


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

pragma benchmark('lognormal_benchmark','lognormal');

Currently there are four benchmarking datasets - Lognormal, Longitudes, Longlat and YCSB.

Please download the respective benchmarking datasets and place it in the same directory as alex_extension.cpp to use the benchmark command : otherwise will not work.


```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

