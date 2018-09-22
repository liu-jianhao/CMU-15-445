/**
 * type.h
 */
#pragma once

#include <cstdint>
#include <string>

#include "type/type_id.h"

namespace cmudb {

class Value;

enum CmpBool { CMP_FALSE = 0, CMP_TRUE = 1, CMP_NULL = 2 };

class Type {
public:
  Type(TypeId type_id) : type_id_(type_id) {}

  virtual ~Type() {}
  // Get the size of this data type in bytes
  static uint64_t GetTypeSize(TypeId type_id);

  // Is this type coercable from the other type
  bool IsCoercableFrom(const TypeId type_id) const;

  // Debug info
  static std::string TypeIdToString(const TypeId type_id);

  static Value GetMinValue(TypeId type_id);
  static Value GetMaxValue(TypeId type_id);

  inline static Type *GetInstance(TypeId type_id) { return kTypes[type_id]; }

  inline TypeId GetTypeId() const { return type_id_; }

  // Comparison functions
  //
  // NOTE:
  // We could get away with only CompareLessThan() being purely virtual, since
  // the remaining comparison functions can derive their logic from
  // CompareLessThan(). For example:
  //
  //    CompareEquals(o) = !CompareLessThan(o) && !o.CompareLessThan(this)
  //    CompareNotEquals(o) = !CompareEquals(o)
  //    CompareLessThanEquals(o) = CompareLessThan(o) || CompareEquals(o)
  //    CompareGreaterThan(o) = !CompareLessThanEquals(o)
  //    ... etc. ...
  //
  // We don't do this for two reasons:
  // (1) The redundant calls to CompareLessThan() may be a performance problem,
  //     and since Value is a core component of the execution engine, we want to
  //     make it as performant as possible.
  // (2) Keep the interface consistent by making all functions purely virtual.
  virtual CmpBool CompareEquals(const Value &left, const Value &right) const;
  virtual CmpBool CompareNotEquals(const Value &left, const Value &right) const;
  virtual CmpBool CompareLessThan(const Value &left, const Value &right) const;
  virtual CmpBool CompareLessThanEquals(const Value &left,
                                        const Value &right) const;
  virtual CmpBool CompareGreaterThan(const Value &left,
                                     const Value &right) const;
  virtual CmpBool CompareGreaterThanEquals(const Value &left,
                                           const Value &right) const;

  // Other mathematical functions
  virtual Value Add(const Value &left, const Value &right) const;
  virtual Value Subtract(const Value &left, const Value &right) const;
  virtual Value Multiply(const Value &left, const Value &right) const;
  virtual Value Divide(const Value &left, const Value &right) const;
  virtual Value Modulo(const Value &left, const Value &right) const;
  virtual Value Min(const Value &left, const Value &right) const;
  virtual Value Max(const Value &left, const Value &right) const;
  virtual Value Sqrt(const Value &val) const;
  virtual Value OperateNull(const Value &val, const Value &right) const;
  virtual bool IsZero(const Value &val) const;

  // Is the data inlined into this classes storage space, or must it be accessed
  // through an indirection/pointer?
  virtual bool IsInlined(const Value &val) const;

  // Return a stringified version of this value
  virtual std::string ToString(const Value &val) const;

  // Serialize this value into the given storage space. The inlined parameter
  // indicates whether we are allowed to inline this value into the storage
  // space, or whether we must store only a reference to this value. If inlined
  // is false, we may use the provided data pool to allocate space for this
  // value, storing a reference into the allocated pool space in the storage.
  virtual void SerializeTo(const Value &val, char *storage) const;

  // Deserialize a value of the given type from the given storage space.
  virtual Value DeserializeFrom(const char *storage) const;

  // Create a copy of this value
  virtual Value Copy(const Value &val) const;

  virtual Value CastAs(const Value &val, const TypeId type_id) const;

  // Access the raw variable length data
  virtual const char *GetData(const Value &val) const;

  // Get the length of the variable length data
  virtual uint32_t GetLength(const Value &val) const;

  // Access the raw varlen data stored from the tuple storage
  virtual char *GetData(char *storage);

protected:
  // The actual type ID
  TypeId type_id_;
  // Singleton instances.
  static Type *kTypes[14];
};
} // namespace cmudb
