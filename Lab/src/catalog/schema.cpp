/**
 * schema.cpp
 */

#include <algorithm>
#include <cassert>
#include <sstream>

#include "catalog/schema.h"

namespace cmudb {

// Construct schema from vector of Column
Schema::Schema(const std::vector<Column> &columns) : tuple_is_inlined(true) {
  int32_t column_offset = 0;
  for (size_t index = 0; index < columns.size(); index++) {
    Column column = columns[index];
    // handle uninlined column
    if (column.IsInlined() == false) {
      tuple_is_inlined = false;
      uninlined_columns.push_back(index);
    }
    // set column offset
    column.column_offset = column_offset;
    column_offset += column.GetFixedLength();

    // add column
    this->columns.push_back(std::move(column));
  }
  // set tuple length
  length = column_offset;
}

/*
 * CopySchema() - Copies the schema into a new schema object with index_list
 *                as indices to copy
 * The returned schema is created by new operator, and the caller is responsible
 * for destroying it.
 */
Schema *Schema::CopySchema(const Schema *schema, const std::vector<int> &ids) {
  std::vector<Column> column_list;
  // Reserve some space to avoid multiple malloc() calls
  column_list.reserve(ids.size());
  // For each column index, push the column
  for (int id : ids) {
    // Make sure the index does not refer to invalid element
    assert(id < schema->GetColumnCount());
    column_list.push_back(schema->columns[id]);
  }

  Schema *ret_schema = new Schema(column_list);
  return ret_schema;
}

std::string Schema::ToString() const {
  std::ostringstream os;

  os << "Schema["
     << "NumColumns:" << GetColumnCount() << ", "
     << "IsInlined:" << tuple_is_inlined << ", "
     << "Length:" << length << "]";

  bool first = true;
  os << " :: (";
  for (int i = 0; i < GetColumnCount(); i++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    os << columns[i].ToString();
  }
  os << ")";

  return os.str();
}

// Compare two schemas
bool Schema::operator==(const Schema &other) const {
  if (other.GetColumnCount() != GetColumnCount() ||
      other.IsInlined() != IsInlined()) {
    return false;
  }

  for (int column_itr = 0; column_itr < other.GetColumnCount(); column_itr++) {
    const Column &column_info = other.GetColumn(column_itr);
    const Column &other_column_info = GetColumn(column_itr);

    if (column_info != other_column_info)
      return false;
  }

  return true;
}

bool Schema::operator!=(const Schema &other) const { return !(*this == other); }

} // namespace cmudb
