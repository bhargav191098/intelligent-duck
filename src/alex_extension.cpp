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
#include "ALEX/src/core/alex.h"
#define KEY_TYPE int
#define PAYLOAD_TYPE int

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
    std::uniform_int_distribution<PAYLOAD_TYPE> dis;
    for (int i = 0; i < num_keys; i++) {
        values[i].first = i;
        values[i].second = dis(gen);
    }

    // Bulk load the keys [0, 100)
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
    std::cout<<" Through toString "<<"\n";
    std::cout<<result->ToString()<<"\n";
    results = result->getContents();
    std::cout<<"Through custom function "<<"\n";
    int num_keys = results.size();
    std::cout<<"Num Keys : "<<num_keys<<"\n";
    std::pair<KEY_TYPE, PAYLOAD_TYPE> values[num_keys];
    int max_key = INT_MIN;
    for (int i=0;i<results.size();i++){
        int row_id = i;
        int key_ = (dynamic_cast<IntData*>(results[i][column_index].get())->value);
        values[i].first = key_;
        if(values[i].first>max_key){
            max_key = values[i].first;
        }
        std::cout<<"Key being loaded into the index : "<<values[i].first<<"\n";
        values[i].second = i;
        index.insert(key_,i);
    }
    std::cout<<"Max key : "<<max_key<<"\n";
    std::cout<<"Bulk Loading data ... "<<"\n";

    auto stats = index.get_stats();
    std::cout << "Final num keys: " << stats.num_keys
            << std::endl;  // expected: 199
    //index.bulk_load(values, max_key+1);

    


    // for (const auto& item : results) {

    //     if (auto* intData = dynamic_cast<IntData*>(item.get())) {
    //         std::cout << "Int Value: " << intData->value << std::endl;
    //     } else if (auto* doubleData = dynamic_cast<DoubleData*>(item.get())) {
    //         std::cout << "Double Value: " << doubleData->value << std::endl;
    //     } else if (auto* stringData = dynamic_cast<StringData*>(item.get())) {
    //         std::cout << "String Value: " << stringData->value << std::endl;
    //     } else if (auto* boolData = dynamic_cast<BoolData*>(item.get())) {
    //         std::cout << "Boolean Value: " << boolData->value << std::endl;
    //     }
    // }

    // ColumnDataCollection col = result->Collection();
    // std::cout<<"Collection got \n";
    // std::cout<<"Success\n";
    // std::cout<<"Column count : "<<col.ColumnCount()<<"\n";
    // std::cout<<"Row count : "<<col.Count()<<"\n";
    // for (auto &row : col.Rows()) {
    //     for (idx_t col_idx = 0; col_idx < col.ColumnCount(); col_idx++) {
    //         auto val = row.GetValue(col_idx);
    //         auto row_address = row.RowIndex();
    //         std::cout<<"Row address : "<<row_address<<"\n";
    //         if(val.IsNull()){
    //             std::cout<<"NULL\n";
    //         }
    //         else{
    //             std::cout<<val.ToString()<<"\n";
    //         }
    //         //result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
    //         std::cout<<"Result inter "<<result<<" "<<row_address<<"\n";
    //         //result += string(row_address);
    //     }
    // }
   
    //functionTryAlex();

    std::cout <<"Alex index pragma function called with table name: " << table_name << " and column name: " << column_name << std::endl;
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
