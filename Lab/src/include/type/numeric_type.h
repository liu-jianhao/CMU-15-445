/**
 * numeric_type.h
 */
#pragma once
#include <cmath>

#include "type/value.h"

namespace cmudb {
// A numeric value is an abstract type representing a number. Numerics can be
// either integral or non-integral (decimal), but must provide arithmetic
// operations on its value.
class NumericType : public Type {
public:
  NumericType(TypeId type) : Type(type) {}
  ~NumericType() {}

  // Other mathematical functions
  virtual Value Add(const Value &left, const Value &right) const = 0;
  virtual Value Subtract(const Value &left, const Value &right) const = 0;
  virtual Value Multiply(const Value &left, const Value &right) const = 0;
  virtual Value Divide(const Value &left, const Value &right) const = 0;
  virtual Value Modulo(const Value &left, const Value &right) const = 0;
  virtual Value Min(const Value &left, const Value &right) const = 0;
  virtual Value Max(const Value &left, const Value &right) const = 0;
  virtual Value Sqrt(const Value &val) const = 0;
  virtual Value OperateNull(const Value &left, const Value &right) const = 0;
  virtual bool IsZero(const Value &val) const = 0;

protected:
  static inline double ValMod(double x, double y) {
    return x - std::trunc((double)x / (double)y) * y;
  }
};
} // namespace cmudb
