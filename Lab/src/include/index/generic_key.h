/**
 * generic_key.h
 *
 * Key used for indexing with opaque data
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instantiated
 * with a template argument.
 */

#pragma once

#include <cstring>

#include "table/tuple.h"
#include "type/value.h"

namespace cmudb {
template <size_t KeySize> class GenericKey {
public:
  inline void SetFromKey(const Tuple &tuple) {
    // initialize to 0
    memset(data, 0, KeySize);
    memcpy(data, tuple.GetData(), tuple.GetLength());
  }

  // NOTE: for test purpose only
  inline void SetFromInteger(int64_t key) {
    memset(data, 0, KeySize);
    memcpy(data, &key, sizeof(int64_t));
  }

  inline Value ToValue(Schema *schema, int column_id) const {
    const char *data_ptr;
    const TypeId column_type = schema->GetType(column_id);
    const bool is_inlined = schema->IsInlined(column_id);
    if (is_inlined) {
      data_ptr = (data + schema->GetOffset(column_id));
    } else {
      int32_t offset = *reinterpret_cast<int32_t *>(
          const_cast<char *>(data + schema->GetOffset(column_id)));
      data_ptr = (data + offset);
    }
    return Value::DeserializeFrom(data_ptr, column_type);
  }

  // NOTE: for test purpose only
  // interpret the first 8 bytes as int64_t from data vector
  inline int64_t ToString() const {
    return *reinterpret_cast<int64_t *>(const_cast<char *>(data));
  }

  // NOTE: for test purpose only
  // interpret the first 8 bytes as int64_t from data vector
  friend std::ostream &operator<<(std::ostream &os, const GenericKey &key) {
    os << key.ToString();
    return os;
  }

  // actual location of data, extends past the end.
  char data[KeySize];
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <size_t KeySize> class GenericComparator {
public:
  inline int operator()(const GenericKey<KeySize> &lhs,
                        const GenericKey<KeySize> &rhs) const {
    int column_count = key_schema_->GetColumnCount();

    for (int i = 0; i < column_count; i++) {
      Value lhs_value = (lhs.ToValue(key_schema_, i));
      Value rhs_value = (rhs.ToValue(key_schema_, i));

      if (lhs_value.CompareLessThan(rhs_value) == CMP_TRUE)
        return -1;

      if (lhs_value.CompareGreaterThan(rhs_value) == CMP_TRUE)
        return 1;
    }
    // equals
    return 0;
  }

  GenericComparator(const GenericComparator &other) {
    this->key_schema_ = other.key_schema_;
  }

  // constructor
  GenericComparator(Schema *key_schema) : key_schema_(key_schema) {}

private:
  Schema *key_schema_;
};

} // namespace cmudb
