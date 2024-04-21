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


#define KEY_TYPE double
#define PAYLOAD_TYPE double


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

alex::Alex<KEY_TYPE, PAYLOAD_TYPE> index;
std::vector<vector<unique_ptr<Base> > > results;

void functionTryAlex(){
    std::cout<<"Simply trying out alex "<<"\n";
    const int num_keys = 100;
    std::pair<KEY_TYPE, PAYLOAD_TYPE> values[num_keys];
    std::mt19937_64 gen(std::random_device{}());
    std::uniform_real_distribution<PAYLOAD_TYPE> dis;
    vector<int>arr1,arr2;
    for (int i = 0; i < num_keys; i++) {
        values[i].first = i;
        values[i].second = dis(gen);
        arr1.push_back(values[i].first);
        arr2.push_back(values[i].second);
    }
    
    //Bulk load the keys [0, 100)
    index.bulk_load(values, num_keys);

    // Insert the keys [100, 200). Now there are 200 keys.
    for (int i = num_keys; i < 2 * num_keys; i++) {
        KEY_TYPE new_key = i;
        PAYLOAD_TYPE new_payload = dis(gen);
        index.insert(new_key, new_payload);
    }

    // Erase the keys [0, 10). Now there are 190 keys.
    for (int i = 0; i < 10; i++) {
        index.erase(i);
    }
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
    // for (vector<unique_ptr<Base>> vec : results[row_id]) {
    //     for(const auto &item : vec){
    //         if (auto* intData = dynamic_cast<IntData*>(item.get())) {
    //         std::cout << "Int Value: " << intData->value << std::endl;
    //         } else if (auto* doubleData = dynamic_cast<DoubleData*>(item.get())) {
    //         std::cout << "Double Value: " << doubleData->value << std::endl;
    //         } else if (auto* stringData = dynamic_cast<StringData*>(item.get())) {
    //         std::cout << "String Value: " << stringData->value << std::endl;
    //         } else if (auto* boolData = dynamic_cast<BoolData*>(item.get())) {
    //         std::cout << "Boolean Value: " << boolData->value << std::endl;
    //         }
    //     }
        
    // }
}

void functionLoadBenchmark(ClientContext &context, const FunctionParameters &parameters){
    std::string tableName = parameters.values[0].GetValue<string>();
    std::string benchmarkName = parameters.values[1].GetValue<string>();
    int benchmark_size = parameters.values[2].GetValue<int>();
    std::cout<<"Loading benchmark data - "<<benchmarkName<<"into table "<<tableName<<"\n";
    std::cout<<"The schema of the table will be {key,payload}\n";
    std::cout<<"Number of keys  "<<benchmark_size<<"\n";
    std::string benchmarkFile = "";
    std::string benchmarkFileType = "";
    const int NUM_KEYS = benchmark_size;

    

    /**
     * Create a table with the table name.
    */
    std::string CREATE_QUERY = "CREATE TABLE "+tableName+"(key double, payload double);";
    duckdb::Connection con(*context.db);
    unique_ptr<MaterializedQueryResult> result = con.Query(CREATE_QUERY);
    std::cout<<"Table created successfully "<<"\n";
    int num_batches_insert = 10;
    int per_batch = NUM_KEYS/num_batches_insert;
    std::cout<<"Per batch insertion "<<"\n";
    int starting = 0;
    int ending = 0;
    std::cout<<"Benchmark name "<<benchmarkName<<"\n";
    if(benchmarkName.compare("lognormal")==0){
        benchmarkFile = "/Users/bhargavkrish/Desktop/USC/Duck_Extension/trial-3/intelligent-duck/src/lognormal-190M.bin.data";
        benchmarkFileType = "binary";
    }

    auto keys = new KEY_TYPE[NUM_KEYS];
    bool res = load_binary_data(keys,NUM_KEYS,benchmarkFile);

    std::cout<<"Res of loading "<<res<<"\n"; 

    for(int i=0;i<100;i++){
        std::cout<<"keys "<<keys[i]<<"\n";
    }

    string query = "INSERT INTO "+tableName+" VALUES ";

    for(int i=0;i<num_batches_insert;i++){
        std::cout<<"Inserting batch "<<i<<"\n";
        
        //KeyType batch_keys[per_batch];  // Replace KeyType with the actual type of keys
        starting = i*per_batch;

        ending = starting + per_batch;
        string tuple_string = "";

        std::cout<<"Starting "<<starting<<" Ending "<<ending<<"\n";
        
        auto values = new std::pair<KEY_TYPE, PAYLOAD_TYPE>[per_batch];
        std::mt19937_64 gen_payload(std::random_device{}());


        for (int vti = starting; vti < ending; vti++) {
            //values[vti].first = keys[vti];
            KEY_TYPE key = keys[vti];
            PAYLOAD_TYPE random_payload = static_cast<PAYLOAD_TYPE>(gen_payload());
            std::cout<<"dae key "<<key<<"\n";
            std::ostringstream stream;
            stream << std::setprecision(std::numeric_limits<double>::max_digits10) << key;
            std::string ressy = stream.str();
            tuple_string = tuple_string + "(" + ressy + "," + std::to_string(random_payload) + ")";
            std::cout<<"Tuple string "<<tuple_string<<"\n";
            if(vti!=ending-1){
                tuple_string = tuple_string + ",";
            }
        }
        string to_execute_query = query + tuple_string + ";";

        std::cout<<"Inserting the Batch "<<i<<"into the database! ";
        std::cout<<"Query used to insert "<<query<<"\n";
        auto res = con.Query(to_execute_query);
        if(!res->HasError()){
            std::cout<<"Batch inserted successfully "<<"\n";
        }else{
            std::cout<<"Error inserting batch "<<i<<"\n";
        }
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


void createAlexIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters){
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();

    std::cout<<"String does it exist : "<<doesitexist<<"\n";
    doesitexist = "Yes";

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
    

    map<KEY_TYPE, PAYLOAD_TYPE > values;
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        std::pair<KEY_TYPE, PAYLOAD_TYPE> value;
        int row_id = i;
        int key_ = (dynamic_cast<IntData*>(results[i][column_index].get())->value);
        values.insert({key_,i});
    }
    std::cout<<"Bulk Loading column data into index "<<"\n";
    std::pair<KEY_TYPE, PAYLOAD_TYPE> bulk_load_values[num_keys];

    int pos = 0;
    for(auto it= values.begin();it!=values.end();it++){
        bulk_load_values[pos] = {it->first,it->second};
        pos++;
    }
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

    auto loadBenchmarkData = PragmaFunction::PragmaCall("benchmark",functionLoadBenchmark,{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::INTEGER},{});
    ExtensionUtil::RegisterFunction(instance,loadBenchmarkData);
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
