#define DUCKDB_EXTENSION_MAIN

#include "alex_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/types/vector.hpp"
// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include<map>
#include "ALEX/src/core/alex.h"
#include "utils.h"
#include <chrono>
#include <numeric>

#define DOUBLE_KEY_TYPE double
#define GENERAL_PAYLOAD_TYPE double
#define KEY_TYPE int

#define INDEX_PAYLOAD_TYPE double

#define INT64_KEY_TYPE int64_t
#define INT_KEY_TYPE int
#define UNSIGNED_INT64_KEY_TYPE uint64_t
#define PAYLOAD_TYPE double

#define HUNDRED_MILLION 100000000
#define TEN_MILLION 10000000


namespace duckdb {

alex::Alex<DOUBLE_KEY_TYPE, INDEX_PAYLOAD_TYPE> double_alex_index;
alex::Alex<INT64_KEY_TYPE, INDEX_PAYLOAD_TYPE> big_int_alex_index;
alex::Alex<UNSIGNED_INT64_KEY_TYPE, INDEX_PAYLOAD_TYPE> unsigned_big_int_alex_index;
alex::Alex<INT_KEY_TYPE, INDEX_PAYLOAD_TYPE> index;

static QualifiedName GetQualifiedName(ClientContext &context, const string &qname_str) {
	auto qname = QualifiedName::Parse(qname_str);
	if (qname.schema == INVALID_SCHEMA) {
		qname.schema = ClientData::Get(context).catalog_search_path->GetDefaultSchema(qname.catalog);
	}
	return qname;
}

string doesitexist = "";

static void CheckIfTableExists(ClientContext &context, QualifiedName &qname) {
	//std::cout<< "Internal call "<<Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name).name;
    unique_ptr<FunctionData> bind_data;
    //auto scan_function = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name).GetScanFunction(context, bind_data);
    //std::cout<<"Bind info "<<scan_function.get_bind_info;
    //std::cout<<"Bind data "<<bind_data->table;
    //std::cout << "Table exists: " << tbEntry->name << std::endl;
}

inline void AlexDummy(DataChunk &args,ExpressionState &state,Vector&result){
    auto data = args.data;
    std::cout<<"Dummy function called "<<"\n";
}


inline void AlexScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Alex "+name.GetString()+" 🐥");;
        });
}

inline void AlexOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Alex " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}


int load_end_point = 0;
std::vector<vector<unique_ptr<Base> > > results;


void functionTryAlex(){}

void display_row(int row_id){
    vector<unique_ptr<Base>>& vec = results.at(row_id);
    for(int i=0;i<vec.size();i++){
        if (auto* intData = dynamic_cast<IntData*>(vec[i].get())) {
            std::cout << "Int Value: " << intData->value << std::endl;
        } else if (auto* doubleData = dynamic_cast<DoubleData*>(vec[i].get())) {
            std::cout << "Double Value: " << doubleData->value << std::endl;
        } else if (auto* stringData = dynamic_cast<StringData*>(vec[i].get())) {
            std::cout << "String Value: " << stringData->value << std::endl;
        } else if (auto* boolData = dynamic_cast<BoolData*>(vec[i].get())) {
            std::cout << "Boolean Value: " << boolData->value << std::endl;
        }
    }
}

void executeQuery(duckdb::Connection& con,string QUERY){
    unique_ptr<MaterializedQueryResult> result = con.Query(QUERY);
    if(result->HasError()){
        std::cout<<"Query execution failed "<<"\n";
    }
    else{
        std::cout<<"Query execution successful! "<<"\n";
    }
}

/**
 * Loading Benchmark into the tables of DuckDB.
 * 
*/

template <typename K,typename P>
void load_benchmark_data_into_table(std::string benchmarkFile,std::string benchmarkFileType,duckdb::Connection& con,std::string tableName,int NUM_KEYS,int num_batches_insert,int per_batch){
    //This function will load a key and payload type agnostic data into the database.
    int starting = 0;
    int ending = 0;

    auto keys = new K[NUM_KEYS];
    bool res = load_binary_data(keys,NUM_KEYS,benchmarkFile);

    std::cout<<"Res of loading from benchmark file "<<res<<"\n"; 

    string query = "INSERT INTO "+tableName+" VALUES ";


    for(int i=0;i<num_batches_insert;i++){
        std::cout<<"Inserting batch "<<i<<"\n";
        
        //KeyType batch_keys[per_batch];  // Replace KeyType with the actual type of keys
        starting = i*per_batch;

        ending = starting + per_batch;
        string tuple_string = "";

        // std::cout<<"Starting "<<starting<<" Ending "<<ending<<"\n";
        
        auto values = new std::pair<K, P>[per_batch];
        std::mt19937_64 gen_payload(std::random_device{}());


        for (int vti = starting; vti < ending; vti++) {
            //values[vti].first = keys[vti];
            K key = keys[vti];
            P random_payload = static_cast<P>(gen_payload());
            //std::cout<<"dae key "<<key<<"\n";
            std::ostringstream stream;
            if(typeid(K)==typeid(DOUBLE_KEY_TYPE)){
                stream << std::setprecision(std::numeric_limits<K>::max_digits10) << key;
            }
            else{
                stream << key;
            }
            std::string ressy = stream.str();
            tuple_string = tuple_string + "(" + ressy + "," + std::to_string(random_payload) + ")";
            //std::cout<<"Tuple string "<<tuple_string<<"\n";
            if(vti!=ending-1){
                tuple_string = tuple_string + ",";
            }
        }
        string to_execute_query = query + tuple_string + ";";

        auto res = con.Query(to_execute_query);
        if(!res->HasError()){
            std::cout<<"Batch inserted successfully "<<"\n";
        }else{
            std::cout<<"Error inserting batch "<<i<<"\n";
        }
    }
}

void functionLoadBenchmark(ClientContext &context, const FunctionParameters &parameters){
    std::string tableName = parameters.values[0].GetValue<string>();
    std::string benchmarkName = parameters.values[1].GetValue<string>();
    int benchmark_size = parameters.values[2].GetValue<int>();
    int num_batches_insert = parameters.values[3].GetValue<int>();

    std::cout<<"Loading benchmark data - "<<benchmarkName<<"into table "<<tableName<<"\n";
    std::cout<<"The schema of the table will be {key,payload}\n";
    std::cout<<"Number of keys  "<<benchmark_size<<"\n";
    
    load_end_point = benchmark_size;
    std::string benchmarkFile = "";
    std::string benchmarkFileType = "";
    const int NUM_KEYS = benchmark_size;

    //Establish a connection with the Database.
    duckdb::Connection con(*context.db);

    /**
     * Create a table with the table name.
    */
    std::string CREATE_QUERY = "";
    
    //int num_batches_insert = 1000;
    int per_batch = NUM_KEYS/num_batches_insert;
    std::cout<<"Per batch insertion "<<per_batch<<"\n";
    
    std::cout<<"Benchmark name "<<benchmarkName<<"\n";
    if(benchmarkName.compare("lognormal")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin.data";
        benchmarkFileType = "binary";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key BIGINT, payload double);";
        executeQuery(con,CREATE_QUERY);
        load_benchmark_data_into_table<INT64_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }
    else if(benchmarkName.compare("longitudes")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin.data";
        benchmarkFileType = "binary";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key double, payload double);";
        executeQuery(con,CREATE_QUERY);
        load_benchmark_data_into_table<DOUBLE_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }
    else if(benchmarkName.compare("longlat")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longlat-200M.bin.data";
        benchmarkFileType = "binary";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key double, payload double);";
        executeQuery(con,CREATE_QUERY);
        load_benchmark_data_into_table<DOUBLE_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }
    else if(benchmarkName.compare("ycsb")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/ycsb-200M.bin.data";
        benchmarkFileType = "binary";
        std::cout<<"Table name "<<tableName<<"\n";
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key UBIGINT , payload double);";
        executeQuery(con,CREATE_QUERY);
        //Args: Benchmark Key Type, Benchmark Payload Type, Benchmark File, Benchmark File Type, conn object, table name, NUM_KEYS,num_batches_insert, per_batch
        load_benchmark_data_into_table<UNSIGNED_INT64_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }

}

void functionSearchAlex(ClientContext &context, const FunctionParameters &parameters){
    std::cout<<"Within search alex call "<<std::endl;
}

double calculateAverage(const std::vector<double>& v) {
    return std::accumulate(v.begin(), v.end(), 0.0) / v.size();
}

/**
 * 
 * Function to run one batch of benchmark - Lookup
*/

/**
 * 
 * Correctness verification :)
*/
template <typename K>
void runLookupBenchmarkOneBatchAlex(duckdb::Connection &con,std::string table_name){
    std::cout<<"Hey this is the general template";
}
template<>
void runLookupBenchmarkOneBatchAlex<INT64_KEY_TYPE>(duckdb::Connection& con,std::string table_name){
    std::cout<<"Running benchmark with one batch";
    /*
    My rationale here - I will run the benchmark for one batch - read a defined number of keys and count the time needed to do that.
    */

   // Create a random number generator
    std::random_device rd;
    std::mt19937 g(rd());

   vector<double> payloads;
   vector<INT64_KEY_TYPE>keys;
   for(int i=0;i<results.size();i++){
        vector<unique_ptr<Base>>& vec = results.at(i);
        keys.push_back(dynamic_cast<BigIntData*>(vec[0].get())->value);
        payloads.push_back(dynamic_cast<DoubleData*>(vec[1].get())->value);
    }
    double sum = 0;
    // for(int i=0;i<payloads.size();i++){
    //     sum += payloads[i];
    // }
    // std::cout<<"Sum of payloads "<<sum<<"\n";
    // std::cout<<"Average "<<sum/payloads.size()<<"\n";

    std::shuffle(keys.begin(), keys.end(), g);
    std::cout<<"Keys have been shuffled!\n";
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<keys.size();i++){
        auto key = keys[i];
        auto it = big_int_alex_index.find(key);
        if (it != big_int_alex_index.end()) {
            double payload = it.payload();
            sum+=payload;
        }
    }
    std::cout<<"Average : "<<sum/keys.size()<<"\n";
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "Time taken to lookup "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
    std::cout<<"Checking Correctness: \n";
    std::string query = "SELECT AVG(payload) FROM "+table_name+";";

    start = std::chrono::high_resolution_clock::now();
    auto res = con.Query(query);
    if(!res->HasError()){
        res->Print();
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed_seconds = end - start;
    std::cout << "Time taken to avg from DuckDB is "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
}

template<>
void runLookupBenchmarkOneBatchAlex<DOUBLE_KEY_TYPE>(duckdb::Connection& con,std::string table_name){
    std::cout<<"Running benchmark with one batch";
    /*
    My rationale here - I will run the benchmark for one batch - read a defined number of keys and count the time needed to do that.
    */

   // Create a random number generator
    std::random_device rd;
    std::mt19937 g(rd());

   vector<double> payloads;
   vector<DOUBLE_KEY_TYPE>keys;
   for(int i=0;i<results.size();i++){
        vector<unique_ptr<Base>>& vec = results.at(i);
        keys.push_back(dynamic_cast<DoubleData*>(vec[0].get())->value);
        payloads.push_back(dynamic_cast<DoubleData*>(vec[1].get())->value);
    }
    double sum = 0;
    // for(int i=0;i<payloads.size();i++){
    //     sum += payloads[i];
    // }
    // std::cout<<"Sum of payloads "<<sum<<"\n";
    // std::cout<<"Average "<<sum/payloads.size()<<"\n";

    std::shuffle(keys.begin(), keys.end(), g);
    std::cout<<"Keys have been shuffled!\n";
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<keys.size();i++){
        auto key = keys[i];
        auto it = double_alex_index.find(key);
        if (it != double_alex_index.end()) {
            double payload = it.payload();
            sum+=payload;
        }
    }
    std::cout<<"Average : "<<sum/keys.size()<<"\n";
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "Time taken to lookup "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
    std::cout<<"Checking Correctness: \n";
    std::string query = "SELECT AVG(payload) FROM "+table_name+";";

    start = std::chrono::high_resolution_clock::now();
    auto res = con.Query(query);
    if(!res->HasError()){
        res->Print();
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed_seconds = end - start;
    std::cout << "Time taken to avg from DuckDB is "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
}

template<>
void runLookupBenchmarkOneBatchAlex<UNSIGNED_INT64_KEY_TYPE>(duckdb::Connection& con,std::string table_name){
    std::cout<<"Running benchmark with one batch";
    /*
    My rationale here - I will run the benchmark for one batch - read a defined number of keys and count the time needed to do that.
    */

   // Create a random number generator
    std::random_device rd;
    std::mt19937 g(rd());

   vector<double> payloads;
   vector<UNSIGNED_INT64_KEY_TYPE>keys;
   for(int i=0;i<results.size();i++){
        vector<unique_ptr<Base>>& vec = results.at(i);
        keys.push_back(dynamic_cast<UBigIntData*>(vec[0].get())->value);
        payloads.push_back(dynamic_cast<DoubleData*>(vec[1].get())->value);
    }
    double sum = 0;
    // for(int i=0;i<payloads.size();i++){
    //     sum += payloads[i];
    // }
    // std::cout<<"Sum of payloads "<<sum<<"\n";
    // std::cout<<"Average "<<sum/payloads.size()<<"\n";

    std::shuffle(keys.begin(), keys.end(), g);
    std::cout<<"Keys have been shuffled!\n";
    auto start = std::chrono::high_resolution_clock::now();
    for(int i=0;i<keys.size();i++){
        auto key = keys[i];
        auto it = unsigned_big_int_alex_index.find(key);
        if (it != unsigned_big_int_alex_index.end()) {
            double payload = it.payload();
            sum+=payload;
        }
    }
    std::cout<<"Average : "<<sum/keys.size()<<"\n";
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "Time taken to lookup "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
    std::cout<<"Checking Correctness: \n";
    std::string query = "SELECT AVG(payload) FROM "+table_name+";";

    start = std::chrono::high_resolution_clock::now();
    auto res = con.Query(query);
    if(!res->HasError()){
        res->Print();
    }
    end = std::chrono::high_resolution_clock::now();
    elapsed_seconds = end - start;
    std::cout << "Time taken to avg from DuckDB is "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
}

template<typename K>
void runLookupBenchmarkOneBatchART(duckdb::Connection& con,std::string benchmark_name){
    std::cout<<"Running benchmark with one batch";
    /*
    My rationale here - I will run the benchmark for one batch - read a defined number of keys and count the time needed to do that.
    */

   // Create a random number generator
    std::random_device rd;
    std::mt19937 g(rd());

   vector<double> payloads;
   vector<K>keys;
   std::cout<<"benchmark name "<<benchmark_name<<"\n";
   
   //std::string lookup_query = "SELECT key from "+benchmark_name+" where key = ";
   vector<K>query_keys;
   std::string in_clause = "";
   for(int i=0;i<results.size();i++){
        vector<unique_ptr<Base>>& vec = results.at(i);
        if(typeid(K).name() == typeid(INT64_KEY_TYPE).name()){
            auto key = dynamic_cast<BigIntData*>(vec[0].get())->value;
            query_keys.push_back(key);
        }
        else if(typeid(K).name() == typeid(UNSIGNED_INT64_KEY_TYPE).name()){
            auto key = dynamic_cast<UBigIntData*>(vec[0].get())->value;
            query_keys.push_back(key);
        }
        else{
            auto key = dynamic_cast<DoubleData*>(vec[0].get())->value;
            query_keys.push_back(key);
        }
        
        // std::ostringstream stream;
        // stream << key;
        // std::string ressy = stream.str();
        if(i!=0){
            in_clause += ",";
        }
        in_clause += std::to_string(query_keys[i]);
        
    }
    double sum = 0;
    std::unique_ptr<PreparedStatement> prepare = con.Prepare("SELECT payload FROM "+benchmark_name+" WHERE key IN (" + in_clause + ")");
    // for(int i=0;i<payloads.size();i++){
    //     sum += payloads[i];
    // }
    // std::cout<<"Sum of payloads "<<sum<<"\n";
    // std::cout<<"Average "<<sum/payloads.size()<<"\n";


    //std::shuffle(keys.begin(), keys.end(), g);
    std::cout<<"Keys have been shuffled!\n";
    auto start = std::chrono::high_resolution_clock::now();
    std::unique_ptr<QueryResult> res = prepare->Execute();
    if(res->HasError()){
        std::cout<<"Error in query "<<"\n";
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "Time taken to lookup "<<results.size()<<" keys is "<< elapsed_seconds.count() << " seconds\n";
    
}



/*
Run the Benchmarks on different indexes.
*/

template <typename K>
void runLookupBenchmarkAlex(K *keys);

template <>
void runLookupBenchmarkAlex(double *keys){

    if(double_alex_index.size()==0){
        std::cout<<"Index is empty. Please load the data into the index first."<<"\n";
        return;
    }

    std::cout<<"Running benchmark workload "<<"\n";
    int init_num_keys = load_end_point;
    int total_num_keys = 40000;
    int batch_size = 10000;
    double insert_frac = 0.5;
    string lookup_distribution = "zipf";
    int i = init_num_keys;
    long long cumulative_inserts = 0;
    long long cumulative_lookups = 0;
    int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    int num_lookups_per_batch = batch_size - num_inserts_per_batch;
    double cumulative_insert_time = 0;
    double cumulative_lookup_time = 0;
    double time_limit = 0.1;
    bool print_batch_stats = true;
    


    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    INDEX_PAYLOAD_TYPE sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);

    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {
        double* lookup_keys = nullptr;
        if (lookup_distribution == "uniform") {
            lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
        } else if (lookup_distribution == "zipf") {
            lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
        } else {
            std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
                    << std::endl;
            //return 1;
        }
        auto lookups_start_time = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < num_lookups_per_batch; j++) {
            double key = lookup_keys[j];
            INDEX_PAYLOAD_TYPE* payload = double_alex_index.get_payload(key);
            // std::cout<<"Key "<<key<<" Payload "<<*payload<<"\n";
            if (payload) {
                std::cout<<"Payload is there! "<<"\n";
                sum += *payload;
            }
            else{
                std::cout<<"Payload is not here!! "<<"\n";
            }
        }
        auto lookups_end_time = std::chrono::high_resolution_clock::now();
        batch_lookup_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                lookups_end_time - lookups_start_time)
                                .count();
        cumulative_lookup_time += batch_lookup_time;
        cumulative_lookups += num_lookups_per_batch;
        delete[] lookup_keys;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
        break;
        }
        if (print_batch_stats) {
        int num_batch_operations = num_lookups_per_batch;
        double batch_time = batch_lookup_time;
        long long cumulative_operations = cumulative_lookups;
        double cumulative_time = cumulative_lookup_time;
        std::cout << "Batch " << batch_no
                    << ", cumulative ops: " << cumulative_operations
                    << "\n\tbatch throughput:\t"
                    << num_lookups_per_batch / batch_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_lookups / cumulative_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                    << std::endl;
        }
    }
    long long cumulative_operations = cumulative_lookups + cumulative_inserts;
    double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
    std::cout << "Cumulative stats: " << batch_no << " batches, "
                << cumulative_operations << " ops (" << cumulative_lookups
                << " lookups, " << cumulative_inserts << " inserts)"
                << "\n\tcumulative throughput:\t"
                << cumulative_lookups / cumulative_lookup_time * 1e9
                << " lookups/sec,\t"
                << cumulative_inserts / cumulative_insert_time * 1e9
                << " inserts/sec,\t"
                << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                << std::endl;

    delete[] keys;
}



template <>
void runLookupBenchmarkAlex(INT64_KEY_TYPE *keys){


    if(big_int_alex_index.size()==0){
        std::cout<<"Index is empty. Please load the data into the index first."<<"\n";
        return;
    }

    std::cout<<"Running benchmark workload "<<"\n";

    int init_num_keys = load_end_point;
    int total_num_keys = 400000;
    int batch_size = 100000;
    double insert_frac = 0.5;
    string lookup_distribution = "zipf";
    int i = init_num_keys;
    long long cumulative_inserts = 0;
    long long cumulative_lookups = 0;
    int num_lookups_per_batch = batch_size;
    double cumulative_lookup_time = 0;
    double time_limit = 0.1;
    bool print_batch_stats = true;
    double elapsed_time_seconds = 0;

    std::cout<<"Num lookups per batch "<<num_lookups_per_batch<<"\n";

    
    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    INDEX_PAYLOAD_TYPE sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);

    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {
        int64_t* lookup_keys = nullptr;
        if (lookup_distribution == "uniform") {
            lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
        } else if (lookup_distribution == "zipf") {
            lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
        } else {
            std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
                    << std::endl;
            //return 1;
        }
        auto lookups_start_time = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < num_lookups_per_batch; j++) {
            int64_t key = lookup_keys[j];
            INDEX_PAYLOAD_TYPE* payload = big_int_alex_index.get_payload(key);
            //std::cout<<"Key "<<key<<" Payload "<<*payload<<"\n";
            if (payload) {
                //std::cout<<"Payload is there! "<<"\n";
                sum += *payload;
            }
            else{
                std::cout<<"Payload is not here!! "<<"\n";
            }
        }
        auto lookups_end_time = std::chrono::high_resolution_clock::now();

        auto elapsed_seconds = lookups_end_time - lookups_start_time;
        elapsed_time_seconds = elapsed_seconds.count();

        batch_lookup_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                lookups_end_time - lookups_start_time)
                                .count();
        cumulative_lookup_time += batch_lookup_time;
        cumulative_lookups += num_lookups_per_batch;
        delete[] lookup_keys;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
            break;
        }
        if (print_batch_stats) {
            int num_batch_operations = num_lookups_per_batch;
            double batch_time = batch_lookup_time;
            long long cumulative_operations = cumulative_lookups;
            double cumulative_time = cumulative_lookup_time;
            std::cout << "Batch " << batch_no
                        <<"Batch lookup time "<<elapsed_time_seconds<<"\n"
                        << ", cumulative ops: " << cumulative_operations
                        << "\n\tbatch throughput:\t"
                        << num_lookups_per_batch / batch_lookup_time * 1e9
                        << " lookups/sec,\t"
                        << cumulative_lookups / cumulative_lookup_time * 1e9
                        << " lookups/sec,\t"
                        << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                        << std::endl;
            }
    }
    // long long avg_lookup_time_per_batch = cumulative_lookups/batch_no;
    // std::cout<<"Average number of lookups per batch "<<avg_lookup_operations_per_batch<<"\n";
    long long cummulative_time = cumulative_lookup_time;
    std::cout<<"Cumulative time "<<cummulative_time<<"\n";
    std::cout<<"Cumulative lookups "<<cumulative_lookups<<"\n";
    std::cout<<"Throughput : "<< cumulative_lookups / cumulative_lookup_time * 1e9<<"ops/sec\n";
    // long long cumulative_operations = cumulative_lookups + cumulative_inserts;
    // double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
    // std::cout << "Cumulative stats: " << batch_no << " batches, "
    //             << cumulative_operations << " ops (" << cumulative_lookups
    //             << " lookups, " << cumulative_inserts << " inserts)"
    //             << "\n\tcumulative throughput:\t"
    //             << cumulative_lookups / cumulative_lookup_time * 1e9
    //             << " lookups/sec,\t"
    //             << cumulative_inserts / cumulative_insert_time * 1e9
    //             << " inserts/sec,\t"
    //             << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
    //             << std::endl;

    delete[] keys;
}

template <typename K>
void runLookupBenchmarkArt(K *keys,duckdb::Connection &con,std::string benchmark_name){


    std::string lookup_query = "SELECT key from "+benchmark_name+"_benchmark where key = ";

    std::cout<<"Running benchmark workload "<<"\n";
    int init_num_keys = load_end_point;
    int total_num_keys = 4000000;
    int batch_size = 100000;
    double insert_frac = 0.5;
    string lookup_distribution = "zipf";
    int i = init_num_keys;
    int sum = 0;
    int lookup_count = 0;
    long long cumulative_inserts = 0;
    long long cumulative_lookups = 0;
    int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    int num_lookups_per_batch = batch_size - num_inserts_per_batch;
    double cumulative_insert_time = 0;
    double cumulative_lookup_time = 0;
    double time_limit = 0.3;
    bool print_batch_stats = true;
    


    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);
    std::unique_ptr<PreparedStatement> prepare = con.Prepare("SELECT payload FROM "+benchmark_name+"_benchmark WHERE key = $1");

    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {

        K* lookup_keys = nullptr;
        if (lookup_distribution == "uniform") {
            lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
        } else if (lookup_distribution == "zipf") {
            lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
        } else {
            std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
                    << std::endl;
            //return 1;
        }
        vector<K>query_vector;

        for (int j = 0; j < num_lookups_per_batch; j++) {
            K key = lookup_keys[j];

            // std::ostringstream stream;
            // if(typeid(K)==typeid(DOUBLE_KEY_TYPE)){
            //     stream << std::setprecision(std::numeric_limits<K>::max_digits10) << key;
            // }
            // else{
            //     stream << key;
            // }
            // std::string ressy = stream.str();
            // std::string query = lookup_query + ressy + ";";
            query_vector.push_back(key);
        }

        auto lookups_start_time = std::chrono::high_resolution_clock::now();
        for (int j = 0; j < num_lookups_per_batch; j++) {
            K query = query_vector[j];
            auto result = prepare->Execute(query);
            lookup_count+=1;    
            if(!result->HasError()){
                sum+=1;
            }
            else{
                std::cout<<"Error in lookup "<<lookup_count<<"\n";
            }
            
        }
        auto lookups_end_time = std::chrono::high_resolution_clock::now();
        batch_lookup_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                lookups_end_time - lookups_start_time)
                                .count();
        cumulative_lookup_time += batch_lookup_time;
        cumulative_lookups += num_lookups_per_batch;
        delete[] lookup_keys;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
        break;
        }
        if (print_batch_stats) {
        int num_batch_operations = num_lookups_per_batch;
        double batch_time = batch_lookup_time;
        long long cumulative_operations = cumulative_lookups;
        double cumulative_time = cumulative_lookup_time;
        std::cout << "Batch " << batch_no
                    << ", cumulative ops: " << cumulative_operations
                    << "\n\tbatch throughput:\t"
                    << num_lookups_per_batch / batch_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_lookups / cumulative_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                    << std::endl;
        }
    }

    long long cummulative_time = cumulative_lookup_time;
    std::cout<<"Cumulative time "<<cummulative_time<<"\n";
    std::cout<<"Cumulative lookups "<<cumulative_lookups<<"\n";
    std::cout<<"Throughput : "<< cumulative_lookups / cumulative_lookup_time * 1e9<<"ops/sec\n";
    
    delete[] keys;

    if(lookup_count == sum){
        std::cout<<"All operations done!\n";
    }

}

void functionRunLookupBenchmark(ClientContext &context, const FunctionParameters &parameters){
    std::cout<<"Running lookup benchmark"<<"\n";
    std::string benchmarkName = parameters.values[0].GetValue<string>();
    std::string index = parameters.values[1].GetValue<string>();
    duckdb::Connection con(*context.db);

    std::string keys_file_path = "";
    if(benchmarkName == "lognormal"){
        keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin.data";
        auto keys = new INT64_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
        if(index == "alex"){
            runLookupBenchmarkAlex<INT64_KEY_TYPE>(keys);
        }
        else{
            runLookupBenchmarkArt<INT64_KEY_TYPE>(keys,con,benchmarkName);
        }
    }
    else if(benchmarkName == "longlat"){
        keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longlat-200M.bin.data";
        auto keys = new DOUBLE_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
        if(index == "alex"){
            runLookupBenchmarkAlex<DOUBLE_KEY_TYPE>(keys);
        }
        else{
            runLookupBenchmarkArt<DOUBLE_KEY_TYPE>(keys,con,benchmarkName);
        }
    }
    else if(benchmarkName=="ycsb"){
        keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/ycsb-200M.bin.data";
        auto keys = new INT64_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
        if(index == "alex"){
            runLookupBenchmarkAlex<INT64_KEY_TYPE>(keys);
        }
        else{
            runLookupBenchmarkArt<INT64_KEY_TYPE>(keys,con,benchmarkName);
        }
    }
    else if(benchmarkName == "longitudes"){
        keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin.data";
        auto keys = new DOUBLE_KEY_TYPE[load_end_point];
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path); 
        if(index == "alex"){
            runLookupBenchmarkAlex<DOUBLE_KEY_TYPE>(keys);
        }
        else{
            runLookupBenchmarkArt<DOUBLE_KEY_TYPE>(keys,con,benchmarkName);
        }
    }

}


template <typename K>
void print_stats(){
    if(typeid(K)==typeid(DOUBLE_KEY_TYPE)){
        auto stats = double_alex_index.get_stats();
        std::cout<<"Stats about the index \n";
        std::cout<<"Number of keys : "<<stats.num_keys<<"\n";
        std::cout<<"Number of model nodes : "<<stats.num_model_nodes<<"\n";
        std::cout<<"Number of data nodes: "<<stats.num_data_nodes<<"\n";
        std::cout<<"Cost computation time : "<<stats.cost_computation_time<<"\n"; 
        std::cout<<"Index size after bulk loading "<<double_alex_index.size()<<"\n";
        std::cout<<"Num expand and scales :"<<stats.num_expand_and_scales<<"\n";
        std::cout<<"Num expand and retrain :"<<stats.num_expand_and_retrains<<"\n";
        std::cout<<"Num downward splits : "<<stats.num_downward_splits<<"\n";
        std::cout<<"Num sideways splits : "<<stats.num_sideways_splits<<"\n";
        std::cout<<"Num model splits : "<<stats.num_model_node_splits<<"\n";
        std::cout<<"Num model expansions : "<<stats.num_model_node_expansions<<"\n";
        std::cout<<"Index created successfully "<<"\n";
    }
    else if(typeid(K)==typeid(UNSIGNED_INT64_KEY_TYPE)){
        auto stats = unsigned_big_int_alex_index.get_stats();
        std::cout<<"Stats about the index \n";
        std::cout<<"Number of keys : "<<stats.num_keys<<"\n";
        std::cout<<"Number of model nodes : "<<stats.num_model_nodes<<"\n";
        std::cout<<"Number of data nodes: "<<stats.num_data_nodes<<"\n";
        std::cout<<"Cost computation time : "<<stats.cost_computation_time<<"\n";
        std::cout<<"Num expand and scales :"<<stats.num_expand_and_scales<<"\n";
        std::cout<<"Num expand and retrain :"<<stats.num_expand_and_retrains<<"\n";
        std::cout<<"Num downward splits : "<<stats.num_downward_splits<<"\n";
        std::cout<<"Num sideways splits : "<<stats.num_sideways_splits<<"\n";
        std::cout<<"Num model splits : "<<stats.num_model_node_splits<<"\n";
        std::cout<<"Num model expansions : "<<stats.num_model_node_expansions<<"\n"; 
        std::cout<<"Index size after bulk loading "<<unsigned_big_int_alex_index.size()<<"\n";
        std::cout<<"Index created successfully "<<"\n";
    }
    else if(typeid(K)==typeid(INT_KEY_TYPE)){
        auto stats = index.get_stats();
        std::cout<<"Stats about the index \n";
        std::cout<<"Number of keys : "<<stats.num_keys<<"\n";
        std::cout<<"Number of model nodes : "<<stats.num_model_nodes<<"\n";
        std::cout<<"Number of data nodes: "<<stats.num_data_nodes<<"\n";
        std::cout<<"Cost computation time : "<<stats.cost_computation_time<<"\n";
        std::cout<<"Num expand and scales :"<<stats.num_expand_and_scales<<"\n";
        std::cout<<"Num expand and retrain :"<<stats.num_expand_and_retrains<<"\n";
        std::cout<<"Num downward splits : "<<stats.num_downward_splits<<"\n";
        std::cout<<"Num sideways splits : "<<stats.num_sideways_splits<<"\n";
        std::cout<<"Num model splits : "<<stats.num_model_node_splits<<"\n";
        std::cout<<"Num model expansions : "<<stats.num_model_node_expansions<<"\n"; 
        std::cout<<"Index size after bulk loading "<<index.size()<<"\n";
        std::cout<<"Index created successfully "<<"\n";
    }
    else{
        auto stats = big_int_alex_index.get_stats();
        std::cout<<"Stats about the index \n";
        std::cout<<"Number of keys : "<<stats.num_keys<<"\n";
        std::cout<<"Number of model nodes : "<<stats.num_model_nodes<<"\n";
        std::cout<<"Number of data nodes: "<<stats.num_data_nodes<<"\n";
        std::cout<<"Cost computation time : "<<stats.cost_computation_time<<"\n";
        std::cout<<"Num expand and scales :"<<stats.num_expand_and_scales<<"\n";
        std::cout<<"Num expand and retrain :"<<stats.num_expand_and_retrains<<"\n";
        std::cout<<"Num downward splits : "<<stats.num_downward_splits<<"\n";
        std::cout<<"Num sideways splits : "<<stats.num_sideways_splits<<"\n";
        std::cout<<"Num model splits : "<<stats.num_model_node_splits<<"\n";
        std::cout<<"Num model expansions : "<<stats.num_model_node_expansions<<"\n"; 
        std::cout<<"Index size after bulk loading "<<big_int_alex_index.size()<<"\n";
        std::cout<<"Index created successfully "<<"\n";
    }

}

/*
Bulk Load into Index functions
*/

template <typename K,typename P>
void bulkLoadIntoIndex(duckdb::Connection & con,std::string table_name,int column_index){
    std::cout<<"General Function with no consequence.\n"; 
}

template<>
void bulkLoadIntoIndex<double,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
   std::pair<double,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<double,INDEX_PAYLOAD_TYPE>[num_keys];
    std::cout<<"Col index "<<column_index<<"\n";    
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto rrr = results[i][column_index].get();
        
        double key_ = dynamic_cast<DoubleData*>(rrr)->value;
        double value_ = dynamic_cast<DoubleData*>(results[i][column_index+1].get())->value;
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values[i] = {key_,value_};
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */

    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values,bulk_load_values+num_keys,[](auto const& a, auto const& b) { return a.first < b.first; });

    /*
    Phase 4: Bulk load the sorted values into the index.
    */
    
    double_alex_index.bulk_load(bulk_load_values, num_keys);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() << " seconds\n";
    std::cout<<"Bulk Loaded! \n";
    print_stats<DOUBLE_KEY_TYPE>();
}

template<>
void bulkLoadIntoIndex<int64_t,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
   std::pair<int64_t,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<int64_t,INDEX_PAYLOAD_TYPE>[num_keys];
    std::cout<<"Col index "<<column_index<<"\n";    
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto rrr = results[i][column_index].get();
        
        int64_t key_ = dynamic_cast<BigIntData*>(rrr)->value;
        double value_ = dynamic_cast<DoubleData*>(results[i][column_index+1].get())->value;
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values[i] = {key_,value_};
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */
   //Measure time 
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values,bulk_load_values+num_keys,[](auto const& a, auto const& b) { return a.first < b.first; });
    
    /*
    Phase 4: Bulk load the sorted values into the index.
    */
    
    big_int_alex_index.bulk_load(bulk_load_values, num_keys);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() << " seconds\n";
    std::cout<<"Bulk Loaded! \n";
    print_stats<INT64_KEY_TYPE>();
}

template<>
void bulkLoadIntoIndex<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
   std::pair<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>[num_keys];
    std::cout<<"Col index "<<column_index<<"\n";    
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto rrr = results[i][column_index].get();
        
        UNSIGNED_INT64_KEY_TYPE key_ = dynamic_cast<UBigIntData*>(rrr)->value;
        double value_ = dynamic_cast<DoubleData*>(results[i][column_index+1].get())->value;
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values[i] = {key_,value_};
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */
   //Measure time 
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values,bulk_load_values+num_keys,[](auto const& a, auto const& b) { return a.first < b.first; });
    
    /*
    Phase 4: Bulk load the sorted values into the index.
    */
    
    unsigned_big_int_alex_index.bulk_load(bulk_load_values, num_keys);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() << " seconds\n";
    std::cout<<"Bulk Loaded! \n";
    print_stats<UNSIGNED_INT64_KEY_TYPE>();
}

template<>
void bulkLoadIntoIndex<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>(duckdb::Connection & con,std::string table_name,int column_index){
/*
    Phase 1: Load the data from the table.
    */
    string query = "SELECT * FROM "+table_name+";";
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

   /*
    Phase 2: Bulk load the data from the results vector into the pair array that goes into the index.
   */
   std::pair<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>[num_keys];
    std::cout<<"Col index "<<column_index<<"\n";    
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto rrr = results[i][column_index].get();
        
        INT_KEY_TYPE key_ = dynamic_cast<IntData*>(rrr)->value;
        double value_ = dynamic_cast<DoubleData*>(results[i][column_index+1].get())->value;
        
        //std::cout<<"after key"<<"\n";
        bulk_load_values[i] = {key_,i};
    }
    /**
     Phase 3: Sort the bulk load values array based on the key values.
    */
   //Measure time 
    
    auto start_time = std::chrono::high_resolution_clock::now();
    std::sort(bulk_load_values,bulk_load_values+num_keys,[](auto const& a, auto const& b) { return a.first < b.first; });
    
    /*
    Phase 4: Bulk load the sorted values into the index.
    */
    
    index.bulk_load(bulk_load_values, num_keys);
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout << "Time taken to bulk load: " << elapsed_seconds.count() << " seconds\n";
    std::cout<<"Bulk Loaded! \n";
    print_stats<INT_KEY_TYPE>();
}

/**
 * Index Creation
 * 
*/

void functionCreateARTIndex(ClientContext &context, const FunctionParameters &parameters){

    std::string table_name = parameters.values[0].GetValue<string>();
    std::string column_name = parameters.values[1].GetValue<string>();

    QualifiedName qname = GetQualifiedName(context, table_name);
    CheckIfTableExists(context, qname);
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    auto &columnList = table.GetColumns();

    vector<string>columnNames = columnList.GetColumnNames();
    for(int i=0;i<columnNames.size();i++){
        std::cout<<"Column name "<<columnNames[i]<<"\n";
        if(column_name == columnNames[i]){
            std::cout<<"Column name found "<<"\n";
            duckdb::Connection con(*context.db);
            std::cout<<"Creating an ART index for this column"<<"\n";
            string query = "CREATE INDEX "+column_name+"_art_index ON "+table_name+"("+column_name+");";
            //Measure time 
            auto start_time = std::chrono::high_resolution_clock::now();
            auto result = con.Query(query);
            auto end_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed_seconds = end_time - start_time;
            if(!result->HasError()){
                std::cout<<"Index created successfully "<<"\n";
            }
            else{
                std::cout<<"Index creation failed "<<"\n";
            }
            // Print the time taken to execute the query
            std::cout << "Time taken to execute the query: " << elapsed_seconds.count() << " seconds\n";
        }
        else{
            std::cout<<"Column name not found "<<"\n";
        }
    }
}

void createAlexIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters){
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();



    QualifiedName qname = GetQualifiedName(context, table_name);
    CheckIfTableExists(context, qname);
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    auto &columnList = table.GetColumns(); 

    vector<string>columnNames = columnList.GetColumnNames();
    vector<LogicalType>columnTypes = columnList.GetColumnTypes();
    int count = 0;
    int column_index = -1;
    LogicalType column_type;
    int col_i = 0;
    for(col_i=0;col_i<columnNames.size();col_i++){
        string curr_col_name = columnNames[col_i];
        LogicalType curr_col_type = columnTypes[col_i];
        std::cout<<"Column name "<<curr_col_name<<"\n";
        if(curr_col_name == column_name){
            column_index = count;
            column_type = curr_col_type;
            std::cout<<"Column name found "<<count<<"\n";
        }
        count++;
    }

    if(column_index == -1){
        std::cout<<"Column not found "<<"\n";
    }
    else{
        duckdb::Connection con(*context.db);
        std::cout<<"Column found at index "<<column_index<<"\n";
        std::cout<<"Creating an alex index for this column"<<"\n";
        std::cout<<"Column Type "<<typeid(column_type).name()<<"\n";
        std::cout<<"Column Type "<<typeid(double).name()<<"\n";
        std::cout<<"Column type to string "<<column_type.ToString()<<"\n";
        std::string columnTypeName = column_type.ToString();
        if(columnTypeName == "DOUBLE"){
            bulkLoadIntoIndex<DOUBLE_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        }
        else if(columnTypeName == "BIGINT"){
            bulkLoadIntoIndex<INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        }
        else if(columnTypeName == "UBIGINT"){
            bulkLoadIntoIndex<UNSIGNED_INT64_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        }
        else if(columnTypeName == "INTEGER"){
            bulkLoadIntoIndex<INT_KEY_TYPE,INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
        }
        else{
            std::cout<<"Unsupported column type for alex indexing (for now) "<<"\n";
        }
        //bulkLoadIntoIndex<typeid(column_type).name(),INDEX_PAYLOAD_TYPE>(con,table_name,column_index);
    }
}

template<typename K>
void functionInsertIntoTableAndIndex(duckdb::Connection &con,std::string table_name,K key,DOUBLE_KEY_TYPE value){
    std::cout<<"General template function \n";
}

template<>
void functionInsertIntoTableAndIndex<DOUBLE_KEY_TYPE>(duckdb::Connection &con,std::string table_name,DOUBLE_KEY_TYPE key,DOUBLE_KEY_TYPE value){
    //std::cout<<"Insert into table and index for double key type"<<"\n";
    // std::string query = "INSERT INTO "+table_name+" VALUES(";
    // query+=std::to_string(key)+","+std::to_string(value)+");";

    std::string query = "INSERT INTO " + table_name + " VALUES (?, ?)";
    auto result = con.Query(query, key, value);
    if(!result->HasError()){
        //std::cout<<"Insertion successful "<<"\n";
        if(double_alex_index.size()>0){
            std::vector<unique_ptr<Base>> dataVector;
            dataVector.push_back(make_uniq<DoubleData>(key));
            dataVector.push_back(make_uniq<DoubleData>(value));
            results.push_back(std::move(dataVector));
            double_alex_index.insert({key,value});
        }
        else{
            std::cout<<"Index is empty. So not updating it."<<"\n";
        }
    }
    else{
        std::cout<<"Insertion failed "<<"\n";
    }
}

template<>
void functionInsertIntoTableAndIndex<INT64_KEY_TYPE>(duckdb::Connection &con,std::string table_name,INT64_KEY_TYPE key,DOUBLE_KEY_TYPE value){
    std::cout<<"Insert into table and index for double key type"<<"\n";
    // std::string query = "INSERT INTO "+table_name+" VALUES(";
    // query+=std::to_string(key)+","+std::to_string(value)+");";

    std::string query = "INSERT INTO " + table_name + " VALUES (?, ?)";
    auto result = con.Query(query, key, value);
    if(!result->HasError()){
        std::cout<<"Insertion successful "<<"\n";
        if(big_int_alex_index.size()>0){
            std::vector<unique_ptr<Base>> dataVector;
            dataVector.push_back(make_uniq<BigIntData>(key));
            dataVector.push_back(make_uniq<BigIntData>(value));
            results.push_back(std::move(dataVector));
            big_int_alex_index.insert({key,value});
        }
        else{
            std::cout<<"Index is empty. So not updating it."<<"\n";
        }
    }
    else{
        std::cout<<"Insertion failed "<<"\n";
    }
}

template<>
void functionInsertIntoTableAndIndex<UNSIGNED_INT64_KEY_TYPE>(duckdb::Connection &con,std::string table_name,UNSIGNED_INT64_KEY_TYPE key,DOUBLE_KEY_TYPE value){
    // std::string query = "INSERT INTO "+table_name+" VALUES(";
    // query+=std::to_string(key)+","+std::to_string(value)+");";

    std::string query = "INSERT INTO " + table_name + " VALUES (?, ?)";
    auto result = con.Query(query, key, value);
    if(!result->HasError()){
        std::cout<<"Insertion successful "<<"\n";
        if(unsigned_big_int_alex_index.size()>0){
            std::vector<unique_ptr<Base>> dataVector;
            dataVector.push_back(make_uniq<UBigIntData>(key));
            dataVector.push_back(make_uniq<UBigIntData>(value));
            results.push_back(std::move(dataVector));
            unsigned_big_int_alex_index.insert({key,value});
        }
        else{
            std::cout<<"Index is empty. So not updating it."<<"\n";
        }
    }
    else{
        std::cout<<"Insertion failed "<<"\n";
    }
}

void functionInsertIntoTable(ClientContext &context, const FunctionParameters &parameters){
    std::string table_name = parameters.values[0].GetValue<string>();
    std::string key_type = parameters.values[1].GetValue<string>();
    double key = parameters.values[2].GetValue<double>();
    double value = parameters.values[3].GetValue<double>();
    duckdb::Connection con(*context.db);
    if(key_type=="double"){
        functionInsertIntoTableAndIndex<double>(con,table_name,key,value);
    }
    else if(key_type=="bigint"){
        functionInsertIntoTableAndIndex<INT64_KEY_TYPE>(con,table_name,key,value);
    }
    else{
        functionInsertIntoTableAndIndex<UNSIGNED_INT64_KEY_TYPE>(con,table_name,key,value);
    }
    
    //For double index:
}

void functionRunBenchmarkOneBatch(ClientContext &context, const FunctionParameters &parameters){
    std::string benchmark_name = parameters.values[0].GetValue<string>();
    std::string index = parameters.values[1].GetValue<string>();
    std::string table_name = benchmark_name+"_benchmark";
    std::string data_type = parameters.values[2].GetValue<string>();
    duckdb::Connection con(*context.db);
    if(index == "alex"){
        if(data_type == "double"){
            runLookupBenchmarkOneBatchAlex<double>(con,table_name);
        }
        else if(data_type=="bigint"){
            runLookupBenchmarkOneBatchAlex<int64_t>(con,table_name);
        }
        else{
            runLookupBenchmarkOneBatchAlex<uint64_t>(con,table_name);
        }
    }
    else{
        if(data_type == "double"){
            runLookupBenchmarkOneBatchART<double>(con,table_name);
        }
        else if(data_type=="bigint"){
            runLookupBenchmarkOneBatchART<int64_t>(con,table_name);
        }
        else{
            runLookupBenchmarkOneBatchART<uint64_t>(con,table_name);
        }
    }

}

template<typename K>
void runInsertionBenchmarkWorkload(duckdb::Connection& con,std::string benchmarkName,std::string table_name,std::string data_type, int to_insert){
    /**
     * Load the keys into a vector based on the data_type
     * 
    */
    std::string benchmarkFile = "";
    std::string benchmarkFileType = "";

    if(benchmarkName.compare("lognormal")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin.data";
        benchmarkFileType = "binary";
    }
    else if(benchmarkName.compare("longitudes")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin.data";
        benchmarkFileType = "binary";
    }
    else if(benchmarkName.compare("longlat")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longlat-200M.bin.data";
        benchmarkFileType = "binary";
    }
    else if(benchmarkName.compare("ycsb")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/ycsb-200M.bin.data";
        benchmarkFileType = "binary";
    }

    int new_key_count = load_end_point + to_insert;
    std::cout<<"New key count "<<new_key_count<<"\n";
    auto keys = new K[new_key_count];
    std::string keys_file_type = "binary";
    if (keys_file_type == "binary") {
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, new_key_count, benchmarkFile);
    } else if (keys_file_type == "text") {
        load_text_data(keys, new_key_count, benchmarkFile);
    } else {
        std::cerr << "--keys_file_type must be either 'binary' or 'text'"
                << std::endl;
    }

    
    auto values = new std::pair<K, double>[to_insert];
    std::mt19937_64 gen_payload(std::random_device{}());


    for (int vti = 0; vti < to_insert; vti++) {
        //values[vti].first = keys[vti];
        K key = keys[vti+load_end_point];
        std::cout<<key<<"\n";
        double random_payload = static_cast<double>(gen_payload());
        values[vti] = {key,random_payload};
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    for(int i=0;i<to_insert;i++){
        K key = values[i].first;
        double value = values[i].second;
        if(data_type == "double"){
            functionInsertIntoTableAndIndex<double>(con,table_name,key,value);
        }
        else if(data_type == "bigint"){
            functionInsertIntoTableAndIndex<int64_t>(con,table_name,key,value);
        }
        else{
            functionInsertIntoTableAndIndex<uint64_t>(con,table_name,key,value);
        }
    }
    load_end_point = results.size();
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout<<"Time taken to insert "<<to_insert<<" keys" << elapsed_seconds.count() << " seconds\n";
}

template<typename K>
void runInsertionBenchmarkWorkloadART(duckdb::Connection& con,std::string benchmarkName,std::string table_name,std::string data_type, int to_insert){
    /**
     * Load the keys into a vector based on the data_type
     * 
    */
    std::string benchmarkFile = "";
    std::string benchmarkFileType = "";

    if(benchmarkName.compare("lognormal")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin.data";
        benchmarkFileType = "binary";
    }
    else if(benchmarkName.compare("longitudes")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin.data";
        benchmarkFileType = "binary";
    }
    else if(benchmarkName.compare("longlat")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longlat-200M.bin.data";
        benchmarkFileType = "binary";
    }
    else if(benchmarkName.compare("ycsb")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/ycsb-200M.bin.data";
        benchmarkFileType = "binary";
    }

    int new_key_count = load_end_point + to_insert;
    std::cout<<"New key count "<<new_key_count<<"\n";
    auto keys = new K[new_key_count];
    std::string keys_file_type = "binary";
    if (keys_file_type == "binary") {
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, new_key_count, benchmarkFile);
    } else if (keys_file_type == "text") {
        load_text_data(keys, new_key_count, benchmarkFile);
    } else {
        std::cerr << "--keys_file_type must be either 'binary' or 'text'"
                << std::endl;
    }

    
    auto values = new std::pair<K, double>[to_insert];
    std::mt19937_64 gen_payload(std::random_device{}());


    for (int vti = 0; vti < to_insert; vti++) {
        //values[vti].first = keys[vti];
        K key = keys[vti+load_end_point];
        std::cout<<key<<"\n";
        double random_payload = static_cast<double>(gen_payload());
        values[vti] = {key,random_payload};
    }
    std::string query = "INSERT INTO " + table_name + " VALUES (?, ?)";
    
    auto start_time = std::chrono::high_resolution_clock::now();
    for(int i=0;i<to_insert;i++){
        K key = values[i].first;
        double value = values[i].second;
        // if(data_type == "double"){
        //     functionInsertIntoTableAndIndex<double>(con,table_name,key,value);
        // }
        // else if(data_type == "bigint"){
        //     functionInsertIntoTableAndIndex<int64_t>(con,table_name,key,value);
        // }
        // else{
        //     functionInsertIntoTableAndIndex<uint64_t>(con,table_name,key,value);
        // }
        auto result = con.Query(query, key, value);
        if(result->HasError()){
            std::cout<<"Insertion failed "<<"\n";
        }
    }
    load_end_point = results.size();
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;
    std::cout<<"Time taken to insert "<<to_insert<<" keys" << elapsed_seconds.count() << " seconds\n";

}

void functionRunInsertionBenchmark(ClientContext &context, const FunctionParameters &parameters){
    std::string benchmark_name = parameters.values[0].GetValue<string>();
    std::string table_name = benchmark_name+"_benchmark";
    std::string data_type = parameters.values[1].GetValue<string>();
    std::string index = parameters.values[2].GetValue<string>();
    int to_insert = parameters.values[3].GetValue<int>();


    duckdb::Connection con(*context.db);

    int init_num_keys = load_end_point;

    if(index == "alex"){
        if(data_type=="double"){
            runInsertionBenchmarkWorkload<double>(con,benchmark_name,table_name,data_type,to_insert);
        }
        else if(data_type=="bigint"){
            runInsertionBenchmarkWorkload<int64_t>(con,benchmark_name,table_name,data_type,to_insert);
        }
        else{
            runInsertionBenchmarkWorkload<uint64_t>(con,benchmark_name,table_name,data_type,to_insert);
        }
    }
    else{
        if(data_type=="double"){
            runInsertionBenchmarkWorkloadART<double>(con,benchmark_name,table_name,data_type,to_insert);
        }
        else if(data_type=="bigint"){
            runInsertionBenchmarkWorkloadART<int64_t>(con,benchmark_name,table_name,data_type,to_insert);
        }
        else{
            runInsertionBenchmarkWorkloadART<uint64_t>(con,benchmark_name,table_name,data_type,to_insert);
        }
    }
}

void functionAlexFind(ClientContext &context, const FunctionParameters &parameters){
    std::string index_type = parameters.values[0].GetValue<string>();
    std::string key = parameters.values[1].GetValue<string>();

    if(index_type == "double"){
        double key_ = std::stod(key);
        auto payload = double_alex_index.get_payload(key_);
        if(payload){
            std::cout<<"Payload found "<<*payload<<"\n";
        }
        else{
            std::cout<<"Payload not found "<<"\n";
        }
    }
    else if(index_type=="bigint"){
        int64_t key_ = std::stoll(key);
        auto payload = big_int_alex_index.get_payload(key_);
        if(payload){
            std::cout<<"Payload found "<<*payload<<"\n";
        }
        else{
            std::cout<<"Payload not found "<<"\n";
        }
    }
    else if(index_type=="int"){
        int key_ = std::stoi(key);
        auto payload = index.get_payload(key_);
        if(payload){
            std::cout<<"Payload found \n";
            display_row(*payload);
        }
        else{
            std::cout<<"Key not found!\n";
        }
    }
    else{
        uint64_t key_ = std::stoull(key);
        auto payload = unsigned_big_int_alex_index.get_payload(key_);
        if(payload){
            std::cout<<"Payload found "<<*payload<<"\n";
        }
        else{
            std::cout<<"Payload not found "<<"\n";
        }
    }
}

void functionAlexSize(ClientContext &context, const FunctionParameters &parameters){
    std::string index_type = parameters.values[0].GetValue<string>();
    long long total_size = 0;
    long long model_size = 0;
    long long data_size = 0;
    if(index_type == "double"){
        model_size = double_alex_index.model_size();
        data_size = double_alex_index.data_size();
        //std::cout<<"Model size "<<model_size<<"\n";
        //std::cout<<"Data size "<<data_size<<"\n";
        total_size = model_size + data_size;

    }
    else if(index_type == "bigint"){
        model_size = big_int_alex_index.model_size();
        data_size = big_int_alex_index.data_size();
        //std::cout<<"Model size "<<model_size<<"\n";
        //std::cout<<"Data size "<<data_size<<"\n";
        total_size = model_size + data_size;
    }
    else{
        model_size = unsigned_big_int_alex_index.model_size();
        data_size = unsigned_big_int_alex_index.data_size();
        //std::cout<<"Model size "<<model_size<<"\n";
        //std::cout<<"Data size "<<data_size<<"\n";
        total_size = model_size + data_size;
    }
    //return static_cast<LogicalType::BIGINT>(total_size);
    double model_size_in_mb = static_cast<double>(model_size) / (1024 * 1024);
    double data_size_in_mb = static_cast<double>(data_size) / (1024 * 1024);
    std::cout<<"Model size "<<model_size_in_mb<<" MB\n";
    std::cout<<"Data size "<<data_size_in_mb<<" MB\n";
    double total_size_in_mb = static_cast<double>(total_size) / (1024 * 1024);
    std::cout<<"Size of the Indexing structure "<<total_size_in_mb<<" MB\n";
}

void functionAuxStorage(ClientContext &context, const FunctionParameters &parameters){
    std::string index_type = parameters.values[0].GetValue<string>();
    long long total_size = 0;
    for(const auto& inner_vector:results){
        total_size+=inner_vector.size()*sizeof(inner_vector[0]);
    }
    double total_size_in_mb = static_cast<double>(total_size) / (1024 * 1024);
    std::cout<<"Auxillary storage size "<<total_size_in_mb<<" MB\n";
}


/**
 * Load Functions: 
 * 
*/

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto alex_scalar_function = ScalarFunction("alex", {LogicalType::VARCHAR}, LogicalType::VARCHAR, AlexScalarFun);
    ExtensionUtil::RegisterFunction(instance, alex_scalar_function);

    // Register another scalar function
    auto alex_openssl_version_scalar_function = ScalarFunction("alex_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, AlexOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, alex_openssl_version_scalar_function);

    auto create_alex_index_function = PragmaFunction::PragmaCall("create_alex_index", createAlexIndexPragmaFunction, {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::INVALID);
    ExtensionUtil::RegisterFunction(instance, create_alex_index_function);

    auto searchAlexDummy = PragmaFunction::PragmaCall("search_alex", functionSearchAlex, {LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::INTEGER},{});
    ExtensionUtil::RegisterFunction(instance, searchAlexDummy);

    // The arguments for the load benchmark data function are the table name, benchmark name and the number of elements to bulk load.
    auto loadBenchmarkData = PragmaFunction::PragmaCall("load_benchmark",functionLoadBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::INTEGER,LogicalType::INTEGER},{});
    ExtensionUtil::RegisterFunction(instance,loadBenchmarkData);

    auto runBenchmarkWorkload = PragmaFunction::PragmaCall("run_lookup_benchmark",functionRunLookupBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,runBenchmarkWorkload);

    auto create_art_index_function = PragmaFunction::PragmaCall("create_art_index",functionCreateARTIndex,{LogicalType::VARCHAR,LogicalType::VARCHAR},LogicalType::INVALID); 
    ExtensionUtil::RegisterFunction(instance,create_art_index_function);
    
    auto insert_into_table_function = PragmaFunction::PragmaCall("insert_into_table",functionInsertIntoTable,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::DOUBLE,LogicalType::DOUBLE},{});
    ExtensionUtil::RegisterFunction(instance,insert_into_table_function);

    //Benchmark name,index.
    auto runBenchmarkOneBatch = PragmaFunction::PragmaCall("run_benchmark_one_batch",functionRunBenchmarkOneBatch,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,runBenchmarkOneBatch);

    auto searchUsingAlexIndex = PragmaFunction::PragmaCall("alex_find",functionAlexFind,{LogicalType::VARCHAR,LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,searchUsingAlexIndex);

    auto findSize = PragmaFunction::PragmaCall("alex_size",functionAlexSize,{LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,findSize);

    auto auxillaryStorageSizes = PragmaFunction::PragmaCall("auxillary_storage_size",functionAuxStorage,{LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,auxillaryStorageSizes);

    auto runInsertionBenchmark = PragmaFunction::PragmaCall("run_insertion_benchmark",functionRunInsertionBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::VARCHAR, LogicalType::INTEGER},{});
    ExtensionUtil::RegisterFunction(instance,runInsertionBenchmark);
   

}

void AlexExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string AlexExtension::Name() {
	return "alex";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void alex_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::AlexExtension>(); // this function pretty much loads the extension onto the db call.
}

DUCKDB_EXTENSION_API const char *alex_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
