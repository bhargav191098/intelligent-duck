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


#define DOUBLE_KEY_TYPE double
#define GENERAL_PAYLOAD_TYPE double
#define KEY_TYPE int

#define INDEX_PAYLOAD_TYPE int

#define INT64_KEY_TYPE int64_t
#define PAYLOAD_TYPE double

#define HUNDRED_MILLION 100000000
#define TEN_MILLION 10000000


namespace duckdb {

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

alex::Alex<DOUBLE_KEY_TYPE, INDEX_PAYLOAD_TYPE> index;
int load_end_point = 0;
std::vector<vector<unique_ptr<Base> > > results;


void functionTryAlex(){
    // std::cout<<"Simply trying out alex "<<"\n";
    // const int num_keys = 100;
    // std::pair<KEY_TYPE, PAYLOAD_TYPE> values[num_keys];
    // std::mt19937_64 gen(std::random_device{}());
    // std::uniform_real_distribution<PAYLOAD_TYPE> dis;
    // vector<int>arr1,arr2;
    // for (int i = 0; i < num_keys; i++) {
    //     values[i].first = i;
    //     values[i].second = dis(gen);
    //     arr1.push_back(values[i].first);
    //     arr2.push_back(values[i].second);
    // }
    
    // //Bulk load the keys [0, 100)
    // index.bulk_load(values, num_keys);

    // // Insert the keys [100, 200). Now there are 200 keys.
    // for (int i = num_keys; i < 2 * num_keys; i++) {
    //     KEY_TYPE new_key = i;
    //     PAYLOAD_TYPE new_payload = dis(gen);
    //     index.insert(new_key, new_payload);
    // }

    // // Erase the keys [0, 10). Now there are 190 keys.
    // for (int i = 0; i < 10; i++) {
    //     index.erase(i);
    // }
}

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
            std::cout<<"Key "<<ressy<<"\n";
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
    
    int num_batches_insert = 10000;
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
        CREATE_QUERY = "CREATE TABLE "+tableName+"(key BIGINT, payload double);";
        executeQuery(con,CREATE_QUERY);
        //Args: Benchmark Key Type, Benchmark Payload Type, Benchmark File, Benchmark File Type, conn object, table name, NUM_KEYS,num_batches_insert, per_batch
        load_benchmark_data_into_table<INT64_KEY_TYPE,GENERAL_PAYLOAD_TYPE>(benchmarkFile,benchmarkFileType,con,tableName,NUM_KEYS,num_batches_insert,per_batch);
    }

}

void functionSearchAlex(ClientContext &context, const FunctionParameters &parameters){
    std::cout<<"Within search alex call "<<std::endl;
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();
    int search_key = parameters.values[2].GetValue<int>();
    std::cout<<"Searching for "<<search_key<<"\n";

    auto it = index.find(search_key);
    if (it != index.end()) {
        int row_id = it.payload();
        display_row(row_id);
    }
}

void functionRunBenchmark(ClientContext &context, const FunctionParameters &parameters){

    std::string keys_file_path = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/longitudes-200M.bin.data";
    auto keys = new DOUBLE_KEY_TYPE[load_end_point];
    std::string keys_file_type = "binary";
    if (keys_file_type == "binary") {
        std::cout<<"Loading binary data "<<std::endl;
        load_binary_data(keys, load_end_point, keys_file_path);
    } else if (keys_file_type == "text") {
        load_text_data(keys, load_end_point, keys_file_path);
    } else {
        std::cerr << "--keys_file_type must be either 'binary' or 'text'"
                << std::endl;
        //return 1;
    }


    std::cout<<"Running benchmark workload "<<"\n";
    std::string benchmarkName = parameters.values[0].GetValue<string>();
    int init_num_keys = load_end_point;
    int total_num_keys = 4000000;
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
    double time_limit = 0.5;
    bool print_batch_stats = true;
    


    auto workload_start_time = std::chrono::high_resolution_clock::now();
    int batch_no = 0;
    PAYLOAD_TYPE sum = 0;
    std::cout << std::scientific;
    std::cout << std::setprecision(3);
    while (true) {
        batch_no++;

        // Do lookups
        double batch_lookup_time = 0.0;
        if (i > 0) {
        DOUBLE_KEY_TYPE* lookup_keys = nullptr;
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
            DOUBLE_KEY_TYPE key = lookup_keys[j];
            INDEX_PAYLOAD_TYPE* payload = index.get_payload(key);
            if (payload) {
            sum += *payload;
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

        // Do inserts
        int num_actual_inserts =
            std::min(num_inserts_per_batch, total_num_keys - i);
        int num_keys_after_batch = i + num_actual_inserts;
        auto inserts_start_time = std::chrono::high_resolution_clock::now();
        for (; i < num_keys_after_batch; i++) {
        index.insert({keys[i], i});
        }
        auto inserts_end_time = std::chrono::high_resolution_clock::now();
        double batch_insert_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(inserts_end_time -
                                                                inserts_start_time)
                .count();
        cumulative_insert_time += batch_insert_time;
        cumulative_inserts += num_actual_inserts;

        if (print_batch_stats) {
        int num_batch_operations = num_lookups_per_batch + num_actual_inserts;
        double batch_time = batch_lookup_time + batch_insert_time;
        long long cumulative_operations = cumulative_lookups + cumulative_inserts;
        double cumulative_time = cumulative_lookup_time + cumulative_insert_time;
        std::cout << "Batch " << batch_no
                    << ", cumulative ops: " << cumulative_operations
                    << "\n\tbatch throughput:\t"
                    << num_lookups_per_batch / batch_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << num_actual_inserts / batch_insert_time * 1e9
                    << " inserts/sec,\t" << num_batch_operations / batch_time * 1e9
                    << " ops/sec"
                    << "\n\tcumulative throughput:\t"
                    << cumulative_lookups / cumulative_lookup_time * 1e9
                    << " lookups/sec,\t"
                    << cumulative_inserts / cumulative_insert_time * 1e9
                    << " inserts/sec,\t"
                    << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
                    << std::endl;
        }

        // Check for workload end conditions
        if (num_actual_inserts < num_inserts_per_batch) {
        // End if we have inserted all keys in a workload with inserts
        break;
        }
        double workload_elapsed_time =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - workload_start_time)
                .count();
        if (workload_elapsed_time > time_limit * 1e9 * 60) {
        break;
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
    //delete[] values;

}


void createAlexIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters){
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();


    QualifiedName qname = GetQualifiedName(context, table_name);
    CheckIfTableExists(context, qname);
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    auto &columnList = table.GetColumns();

    vector<string>columnNames = columnList.GetColumnNames();
    int count = 0;
    int column_index = -1;
    for(auto item:columnNames){
        std::cout<<"Column name "<<item<<"\n";
        if(item == column_name){
            column_index = count;
            std::cout<<"Column name found "<<count<<"\n";
        }
        count++;
    }

    string query = "SELECT * FROM "+table_name+";";

    // std::cout<<"Query "<<query<<"\n";
    // //auto prepare = context.Prepare(query);
    vector<int>keys;
    vector<int>payloads;
    duckdb::Connection con(*context.db);
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    
    results = result->getContents();
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";

    std::pair<DOUBLE_KEY_TYPE, INDEX_PAYLOAD_TYPE>* bulk_load_values = new std::pair<DOUBLE_KEY_TYPE, INDEX_PAYLOAD_TYPE>[num_keys];
    std::cout<<"Col index "<<column_index<<"\n";    
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        //std::cout<<"before key"<<"\n";
        auto rrr = results[i][column_index].get();
    
        int key_ = (dynamic_cast<DoubleData*>(rrr)->value);
        //std::cout<<"after key"<<"\n";
        bulk_load_values[i] = {key_,row_id};
    }
    std::cout<<"Bulk Loading column data into index "<<"\n";
    std::sort(bulk_load_values,bulk_load_values+num_keys,[](auto const& a, auto const& b) { return a.first < b.first; });
    std::cout<<"Index size before bulk loading "<<index.size()<<"\n";
    
    index.bulk_load(bulk_load_values, num_keys);
    auto stats = index.get_stats();
    std::cout<<"Stats about the index \n";
    std::cout<<"Number of keys : "<<stats.num_keys<<"\n";
    std::cout<<"Number of model nodes : "<<stats.num_model_nodes<<"\n";
    std::cout<<"Number of data nodes: "<<stats.num_data_nodes<<"\n";
    std::cout<<"Cost computation time : "<<stats.cost_computation_time<<"\n"; 
    std::cout<<"Index size after bulk loading "<<index.size()<<"\n";
    std::cout<<"Index created successfully "<<"\n";

}

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
    auto loadBenchmarkData = PragmaFunction::PragmaCall("load_benchmark",functionLoadBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::INTEGER},{});
    ExtensionUtil::RegisterFunction(instance,loadBenchmarkData);

    auto runBenchmarkWorkload = PragmaFunction::PragmaCall("run_benchmark",functionRunBenchmark,{LogicalType::VARCHAR},{});
    ExtensionUtil::RegisterFunction(instance,runBenchmarkWorkload);
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
