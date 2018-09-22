/**
 * varlen_type.cpp
 */
#include <algorithm>

#include "common/exception.h"
#include "type/type_util.h"
#include "type/varlen_type.h"

namespace cmudb {
#define VARLEN_COMPARE_FUNC(OP)                                                \
  const char *str1 = left.GetData();                                           \
  uint32_t len1 = GetLength(left) - 1;                                         \
  const char *str2;                                                            \
  uint32_t len2;                                                               \
  if (right.GetTypeId() == TypeId::VARCHAR) {                                  \
    str2 = right.GetData();                                                    \
    len2 = GetLength(right) - 1;                                               \
    return GetCmpBool(TypeUtil::CompareStrings(str1, len1, str2, len2) OP 0);  \
  } else {                                                                     \
    auto r_value = right.CastAs(TypeId::VARCHAR);                              \
    str2 = r_value.GetData();                                                  \
    len2 = GetLength(r_value) - 1;                                             \
    return GetCmpBool(TypeUtil::CompareStrings(str1, len1, str2, len2) OP 0);  \
  }

VarlenType::VarlenType(TypeId type) : Type(type) {}

VarlenType::~VarlenType() {}

// Access the raw variable length data
const char *VarlenType::GetData(const Value &val) const {
  return val.value_.varlen;
}

// Get the length of the variable length data (including the length field)
uint32_t VarlenType::GetLength(const Value &val) const { return val.size_.len; }

CmpBool VarlenType::CompareEquals(const Value &left, const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) == GetLength(right));
  }

  VARLEN_COMPARE_FUNC(==);
}

CmpBool VarlenType::CompareNotEquals(const Value &left,
                                     const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) != GetLength(right));
  }

  VARLEN_COMPARE_FUNC(!=);
}

CmpBool VarlenType::CompareLessThan(const Value &left,
                                    const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) < GetLength(right));
  }

  VARLEN_COMPARE_FUNC(<);
}

CmpBool VarlenType::CompareLessThanEquals(const Value &left,
                                          const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) <= GetLength(right));
  }

  VARLEN_COMPARE_FUNC(<=);
}

CmpBool VarlenType::CompareGreaterThan(const Value &left,
                                       const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) > GetLength(right));
  }

  VARLEN_COMPARE_FUNC(>);
}

CmpBool VarlenType::CompareGreaterThanEquals(const Value &left,
                                             const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return CMP_NULL;
  if (GetLength(left) == PELOTON_VARCHAR_MAX_LEN ||
      GetLength(right) == PELOTON_VARCHAR_MAX_LEN) {
    return GetCmpBool(GetLength(left) >= GetLength(right));
  }

  VARLEN_COMPARE_FUNC(>=);
}

Value VarlenType::Min(const Value &left, const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);
  if (left.CompareLessThan(right) == CMP_TRUE)
    return left.Copy();
  return right.Copy();
}

Value VarlenType::Max(const Value &left, const Value &right) const {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull())
    return left.OperateNull(right);
  if (left.CompareGreaterThan(right) == CMP_TRUE)
    return left.Copy();
  return right.Copy();
}

std::string VarlenType::ToString(const Value &val) const {
  uint32_t len = GetLength(val);

  if (val.IsNull())
    return "varlen_null";
  if (len == PELOTON_VARCHAR_MAX_LEN)
    return "varlen_max";
  if (len == 0) {
    return "";
  }
  return std::string(GetData(val), len - 1);
}

void VarlenType::SerializeTo(const Value &val, char *storage) const {
  uint32_t len = GetLength(val);
  if (len == PELOTON_VALUE_NULL) {
    memcpy(storage, &len, sizeof(uint32_t));
    return;
  } else {
    memcpy(storage, &len, sizeof(uint32_t));
    memcpy(storage + sizeof(uint32_t), val.value_.varlen, len);
  }
}

// Deserialize a value of the given type from the given storage space.
Value VarlenType::DeserializeFrom(const char *storage) const {
  uint32_t len = *reinterpret_cast<const uint32_t *>(storage);
  if (len == PELOTON_VALUE_NULL) {
    return Value(type_id_, nullptr, len, false);
  }
  // set manage_data as true
  return Value(type_id_, storage + sizeof(uint32_t), len, true);
}

Value VarlenType::Copy(const Value &val) const { return Value(val); }

Value VarlenType::CastAs(const Value &value, const TypeId type_id) const {
  std::string str;
  // switch begins
  switch (type_id) {
  case TypeId::BOOLEAN: {
    str = value.ToString();
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    if (str == "true" || str == "1" || str == "t")
      return Value(type_id, 1);
    else if (str == "false" || str == "0" || str == "f")
      return Value(type_id, 0);
    else
      throw Exception("Boolean value format error.");
  }
  case TypeId::TINYINT: {
    str = value.ToString();
    int8_t tinyint = 0;
    try {
      tinyint = stoi(str);
    } catch (std::out_of_range &e) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    }
    if (tinyint < PELOTON_INT8_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    return Value(type_id, tinyint);
  }
  case TypeId::SMALLINT: {
    str = value.ToString();
    int16_t smallint = 0;
    try {
      smallint = stoi(str);
    } catch (std::out_of_range &e) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    }
    if (smallint < PELOTON_INT16_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    return Value(type_id, smallint);
  }
  case TypeId::INTEGER: {
    str = value.ToString();
    int32_t integer = 0;
    try {
      integer = stoi(str);
    } catch (std::out_of_range &e) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    }
    if (integer > PELOTON_INT32_MAX || integer < PELOTON_INT32_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    return Value(type_id, integer);
  }
  case TypeId::BIGINT: {
    str = value.ToString();
    int64_t bigint = 0;
    try {
      bigint = stoll(str);
    } catch (std::out_of_range &e) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    }
    if (bigint > PELOTON_INT64_MAX || bigint < PELOTON_INT64_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    return Value(type_id, bigint);
  }
  case TypeId::DECIMAL: {
    str = value.ToString();
    double res = 0;
    try {
      res = stod(str);
    } catch (std::out_of_range &e) {
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    }
    if (res > PELOTON_DECIMAL_MAX || res < PELOTON_DECIMAL_MIN)
      throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                      "Numeric value out of range.");
    return Value(type_id, res);
  }
  case TypeId::VARCHAR:
    return value.Copy();
  default:
    break;
  }
  throw Exception("VARCHAR is not coercable to " + TypeIdToString(type_id));
}
} // namespace cmudb
