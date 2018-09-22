/**
 * integer_parent_type.cpp
 */
#include <cassert>
#include <cmath>

#include "type/integer_parent_type.h"

namespace cmudb {
IntegerParentType::IntegerParentType(TypeId type) : NumericType(type) {}

Value IntegerParentType::Min(const Value &left, const Value &right) const {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (left.CompareLessThan(right) == CMP_TRUE)
    return left.Copy();
  return right.Copy();
}

Value IntegerParentType::Max(const Value &left, const Value &right) const {
  assert(left.CheckInteger());
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);

  if (left.CompareGreaterThanEquals(right) == CMP_TRUE)
    return left.Copy();
  return right.Copy();
}
} // namespace cmudb
