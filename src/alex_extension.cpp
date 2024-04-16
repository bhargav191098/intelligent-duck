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


void createAlexIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters){
    string table_name = parameters.values[0].GetValue<string>();
    string column_name = parameters.values[1].GetValue<string>();

    std::cout<<"String does it exist : "<<doesitexist<<"\n";
    doesitexist = "Yes";

    QualifiedName qname = GetQualifiedName(context, table_name);
    CheckIfTableExists(context, qname);
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    
    string query = "SELECT * FROM "+table_name+";";
    // std::cout<<"Query "<<query<<"\n";
    // //auto prepare = context.Prepare(query);

    duckdb::Connection con(*context.db);
    unique_ptr<MaterializedQueryResult> result = con.Query(query);
    std::cout<<" Through toString "<<"\n";
    std::cout<<result->ToString()<<"\n";
    std::vector<unique_ptr<Base>> results= result->getContents();
    std::cout<<"Through custom function "<<"\n";
    for (const auto& item : results) {
       
    //    std::cout<<string(item.get())<<std::endl;

        if (auto* intData = dynamic_cast<IntData*>(item.get())) {
            std::cout << "Int Value: " << intData->value << std::endl;
        } else if (auto* doubleData = dynamic_cast<DoubleData*>(item.get())) {
            std::cout << "Double Value: " << doubleData->value << std::endl;
        } else if (auto* stringData = dynamic_cast<StringData*>(item.get())) {
            std::cout << "String Value: " << stringData->value << std::endl;
        } else if (auto* boolData = dynamic_cast<BoolData*>(item.get())) {
            std::cout << "Boolean Value: " << boolData->value << std::endl;
        }
    }

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
