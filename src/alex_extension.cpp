#define DUCKDB_EXTENSION_MAIN

#include "alex_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

// string CreateAlexIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
//     string table_name = parameters.values[0].GetValue<string>();
//     string column1_name = parameters.values[1].GetValue<string>();
//     string column2_name = parameters.values[2].GetValue<string>();
//     string column3_name = parameters.values[3].GetValue<string>();
    
//     // Perform the necessary steps to create the index
//     // For example:
//     string query = "CREATE INDEX alex_index ON " + table_name + "(" + column1_name + ", " + column2_name + ", " + column3_name + ")";
//     // context.TryExecuteQuery(query);

//     return "Index creation pragma called with parameters: " + table_name + ", " + column1_name + ", " + column2_name + ", " + column3_name;
// }

void CreateAlexIndexPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
    // Extract parameters from the FunctionParameters object
    string table_name = parameters.values[0].GetValue<string>();
    string column1_name = parameters.values[1].GetValue<string>();
    string column2_name = parameters.values[2].GetValue<string>();
    string column3_name = parameters.values[3].GetValue<string>();

    // Perform the necessary steps to create the index
    // For example:
    string query = "CREATE INDEX alex_index ON " + table_name + "(" + column1_name + ", " + column2_name + ", " + column3_name + ")";
    // Execute the query (uncomment the line when you're ready to execute the query)
    // context.TryExecuteQuery(query);

    // Print a message indicating that the index was created
    std::cout << "Index 'alex_index' created on table '" + table_name + "' successfully." << std::endl;
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

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto alex_scalar_function = ScalarFunction("alex", {LogicalType::VARCHAR}, LogicalType::VARCHAR, AlexScalarFun);
    ExtensionUtil::RegisterFunction(instance, alex_scalar_function);

    // Register another scalar function
    auto alex_openssl_version_scalar_function = ScalarFunction("alex_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, AlexOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, alex_openssl_version_scalar_function);

	auto create_alex_index_func =
	            PragmaFunction::PragmaCall("create_alex_index", CreateAlexIndexPragmaFunction,
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::VARCHAR);

	ExtensionUtil::RegisterFunction(instance, create_alex_index_func);
}

void AlexExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string AlexExtension::Name() {
	return "AAAAAAlex";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void alex_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::AlexExtension>();
}

DUCKDB_EXTENSION_API const char *alex_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
