/**
 * schema.h
 */

#pragma once

#include <memory>
#include <vector>

#include "catalog/column.h"
#include "type/type.h"

namespace cmudb {

class Schema {
public:
  //===--------------------------------------------------------------------===//
  // Static factory methods to construct schema objects
  //===--------------------------------------------------------------------===//
  // Construct schema from vector of Column
  Schema(const std::vector<Column> &columns);

  // Copy Schema, use to construct indexed tuple
  static Schema *CopySchema(const Schema *schema, const std::vector<int> &ids);

  // Compare two schemas
  bool operator==(const Schema &other) const;

  bool operator!=(const Schema &other) const;

  //===--------------------------------------------------------------------===//
  // Schema accessors
  //===--------------------------------------------------------------------===//

  inline int32_t GetOffset(const int column_id) const {
    return columns[column_id].GetOffset();
  }

  inline TypeId GetType(const int column_id) const {
    return columns[column_id].GetType();
  }

  // Return appropriate length based on whether column is inlined
  inline int32_t GetAppropriateLength(const int column_id) const {
    auto is_inlined = columns[column_id].IsInlined();
    int32_t column_length;

    if (is_inlined) {
      column_length = GetLength(column_id);
    } else {
      column_length = GetVariableLength(column_id);
    }

    return column_length;
  }

  // Returns fixed length
  inline int32_t GetLength(const int column_id) const {
    return columns[column_id].GetFixedLength();
  }

  inline int32_t GetVariableLength(const int column_id) const {
    return columns[column_id].GetVariableLength();
  }

  inline bool IsInlined(const int column_id) const {
    return columns[column_id].IsInlined();
  }

  inline const Column GetColumn(const int column_id) const {
    return columns[column_id];
  }

  // column id start with 0
  inline int GetColumnID(std::string col_name) const {
    int i;
    for (i = 0; i < GetColumnCount(); ++i) {
      if (columns[i].GetName() == col_name) {
        return i;
      }
    }
    return -1;
  }

  inline const std::vector<int> &GetUnlinedColumns() const {
    return uninlined_columns;
  }

  inline const std::vector<Column> &GetColumns() const { return columns; }

  // Return the number of columns in the schema for the tuple.
  inline int GetColumnCount() const { return static_cast<int>(columns.size()); }

  inline int GetUnlinedColumnCount() const {
    return static_cast<int>(uninlined_columns.size());
  }

  // Return the number of bytes used by one tuple.
  inline int32_t GetLength() const { return length; }

  // Returns a flag indicating whether all columns are inlined
  inline bool IsInlined() const { return tuple_is_inlined; }

  // Get a string representation for debugging
  std::string ToString() const;

private:
  // size of fixed length columns
  int32_t length;

  // all inlined and uninlined columns in the tuple
  std::vector<Column> columns;

  // are all columns inlined
  bool tuple_is_inlined;

  // keeps track of unlined columns, using logical position(start with 0)
  std::vector<int> uninlined_columns;

  // keeps track of indexed columns in original table
  // std::vector<int> indexed_columns_;
};

} // namespace cmudb
