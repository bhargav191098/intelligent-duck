#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

static QualifiedName GetQualifiedName(ClientContext &context, const string &qname_str) {
	auto qname = QualifiedName::Parse(qname_str);
	if (qname.schema == INVALID_SCHEMA) {
		qname.schema = ClientData::Get(context).catalog_search_path->GetDefaultSchema(qname.catalog);
	}
	return qname;
}

static string GetFTSSchema(QualifiedName &qname) {
	auto result = qname.catalog == INVALID_CATALOG ? "" : StringUtil::Format("%s.", qname.catalog);
	result += StringUtil::Format("fts_%s_%s", qname.schema, qname.name);
	return result;
}


// string CreateFTSIndexQuery(ClientContext &context, const FunctionParameters &parameters) {

// 	return "Hello";
// }

} // namespace duckdb
