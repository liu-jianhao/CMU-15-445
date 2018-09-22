/**
 * column.h
 */

#pragma once

#include <cstdint>

#include "common/exception.h"
#include "type/type.h"

namespace cmudb {

class Column {
  friend class Schema;

public:
  Column() : column_type(TypeId::INVALID), fixed_length(-1) {
    // Nothing to see...
  }

  Column(TypeId value_type, int32_t column_length, std::string column_name)
      : column_type(value_type), fixed_length(-1), column_name(column_name) {
    // only VARCHAR type is not inlined store
    SetInlined();
    // We should not have an inline value of length 0
    if (is_inlined && column_length <= 0)
      throw Exception(EXCEPTION_TYPE_CONSTRAINT,
                      "inline type must pass in column_length");
    SetLength(column_length);
  }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Set inlined
  void SetInlined();

  // Set the appropriate column length
  void SetLength(int32_t column_length);

  // set column offset in schema
  int32_t GetOffset() const { return column_offset; }

  std::string GetName() const { return column_name; }

  int32_t GetLength() const {
    if (is_inlined)
      return fixed_length;
    else
      return variable_length;
  }

  int32_t GetFixedLength() const { return fixed_length; }

  int32_t GetVariableLength() const { return variable_length; }

  inline TypeId GetType() const { return column_type; }

  inline bool IsInlined() const { return is_inlined; }

  // Compare two column objects
  bool operator==(const Column &other) const {
    if (other.column_type != column_type || other.is_inlined != is_inlined) {
      return false;
    }
    return true;
  }

  bool operator!=(const Column &other) const { return !(*this == other); }

  // Get a string representation for debugging
  std::string ToString() const;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

private:
  // value type of column
  TypeId column_type; //  = TypeId::INVALID;

  // if the column is not inlined, this is set to pointer size
  // else, it is set to length of the fixed length column
  int32_t fixed_length;

  // if the column is inlined, this is set to 0
  // else, it is set to length of the variable length column
  int32_t variable_length = -1;

  // name of the column
  std::string column_name;

  // is the column inlined ?
  bool is_inlined = false;

  // offset of column in tuple
  int32_t column_offset = -1;
};

} // namespace cmudb