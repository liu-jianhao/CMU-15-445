/**
 * column.cpp
 */

#include <sstream>

#include "catalog/column.h"

namespace cmudb {

void Column::SetLength(int32_t column_length) {
  // Set the column length based on whether it is inlined
  if (is_inlined) {
    fixed_length = column_length;
    variable_length = 0;
  } else {
    fixed_length = sizeof(int32_t);
    variable_length = column_length;
  }
}

void Column::SetInlined() {
  switch (column_type) {
  case TypeId::VARCHAR:
    is_inlined = false;
    break;

  default:
    is_inlined = true;
    break;
  }
}

std::string Column::ToString() const {
  std::ostringstream os;

  os << "Column[" << column_name << ", " << Type::TypeIdToString(column_type)
     << ", "
     << "Offset:" << column_offset << ", ";

  if (is_inlined) {
    os << "FixedLength:" << fixed_length;
  } else {
    os << "VarLength:" << variable_length;
  }
  os << "]";
  return (os.str());
}

} // namespace cmudb