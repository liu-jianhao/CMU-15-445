/**
 * boolean_type.h
 */
#pragma once
#include "common/exception.h"
#include "type/type.h"
#include "type/value.h"

namespace cmudb {
// A boolean value isn't a real SQL type, but we treat it as one to keep
// consistent in the expression subsystem.
class BooleanType : public Type {
public:
  ~BooleanType() {}
  BooleanType();

  // Comparison functions
  CmpBool CompareEquals(const Value &left, const Value &right) const override;
  CmpBool CompareNotEquals(const Value &left,
                           const Value &right) const override;
  CmpBool CompareLessThan(const Value &left, const Value &right) const override;
  CmpBool CompareLessThanEquals(const Value &left,
                                const Value &right) const override;
  CmpBool CompareGreaterThan(const Value &left,
                             const Value &right) const override;
  CmpBool CompareGreaterThanEquals(const Value &left,
                                   const Value &right) const override;

  // Decimal types are always inlined
  bool IsInlined(const Value &) const override { return true; }

  // Debug
  std::string ToString(const Value &val) const override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value &val, char *storage) const override;

  // Deserialize a value of the given type from the given storage space.
  Value DeserializeFrom(const char *storage) const override;

  // Create a copy of this value
  Value Copy(const Value &val) const override;

  Value CastAs(const Value &val, const TypeId type_id) const override;
};
} // namespace cmudb
