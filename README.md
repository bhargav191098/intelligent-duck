# Alex Extension for DuckDB 🦆

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension 🚀

The alex extension allows the user to create alex index structure for any integer-based data column of the table.


### Build steps
Remember to clone all the sub-modules using 

```
git clone --recurse-submodules https://github.com/bhargav191098/intelligent-duck.git
```

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


Please download the respective benchmarking datasets and place it in the same directory as alex_extension.cpp to use the benchmark command : otherwise the benchmarking functionality will not work. 

- `create_alex_index` : This pragma call automates the creation of ALEX indexes for specified columns in DuckDB tables. It validates table and column existence, identifies column types, and initiates bulk loading of the ALEX index based on the column's data type.

- `alex_find` : This function facilitates key-based searches within ALEX indexes in DuckDB. It extracts the payload associated with the provided key. If a payload is found, it is displayed; otherwise, a message indicating that the payload was not found is returned. This pragma function streamlines the process of querying ALEX indexes for specific values, enhancing the efficiency of key-based retrievals in DuckDB.

- `alex_size` : 
alex_size pragma function retrieves and displays the total size, including model and data sizes, of the ALEX index structure in DuckDB, converting the sizes to megabytes for clarity.

- `load_benchmark` : It facilitates the creation of a SQL table and loads data from one of four benchmark sources into the specified table, enabling subsequent indexing using the create_alex_index pragma function.

- `create_art_index`: Similar to create_alex_index pragma function. This function is used to create an ART index, facilitating benchmarking and comparison between ART and Alex indexes for performance evaluation in DuckDB.

- `insert_into_table` : Unlike bulk load, insert_into_table pragma function inserts key-value pairs individually into the table and concurrently adds the key to the Alex index, mimicking the behavior of the standard SQL insert command in a database.

- `auxillary_storage_size` : auxillary_storage_size pragma function calculates the total size of auxiliary storage used by alex index structure.

## Benchmark datasets

- <a href = "https://drive.google.com/file/d/1zc90sD6Pze8UM_XYDmNjzPLqmKly8jKl/view">Longitudes (200M 8-byte floats)</a>
- <a href = "https://drive.google.com/file/d/1mH-y_P>cLQ6p8kgAz9SB7ME4KeYAfRfmR/view">Longlat (200M 8-byte floats)</a>
- <a href = "https://drive.google.com/file/d/1y-UBf8CuuFgAZkUg_2b_G8zh4iF_N-mq/view">Lognormal (190M 8-byte signed ints)</a>
- <a href = "https://drive.google.com/file/d/1Q89-v4FJLEwIKL3YY3oCeOEs0VUuv5bD/view">YCSB (200M 8-byte unsigned ints)</a>


## Evaluation graphs

<img src = "https://github.com/bhargav191098/intelligent-duck/blob/main/graph-images/benchmarks.png" height="800" width="600" />

The Learned Indexing provides great throughput with less memory overhead. Obviously running it on x86 based CPUs allows even further boosts.
