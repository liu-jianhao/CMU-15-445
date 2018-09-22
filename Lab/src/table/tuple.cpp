/**
 * tuple.cpp
 */

#include <cassert>
#include <cstdlib>
#include <sstream>

#include "common/logger.h"
#include "table/tuple.h"

namespace cmudb {

Tuple::Tuple(std::vector<Value> values, Schema *schema) : allocated_(true) {
  assert((int) values.size() == schema->GetColumnCount());

  // step1: calculate size of the tuple
  int32_t tuple_size = schema->GetLength();
  for (auto &i : schema->GetUnlinedColumns())
    tuple_size += (values[i].GetLength() + sizeof(uint32_t));
  // allocate memory using new, allocated_ flag set as true
  size_ = tuple_size;
  data_ = new char[size_];

  // step2: Serialize each column(attribute) based on input value
  int column_count = schema->GetColumnCount();
  int32_t offset = schema->GetLength();
  for (int i = 0; i < column_count; i++) {
    if (!schema->IsInlined(i)) {
      // Serialize relative offset, where the actual varchar data is stored
      *reinterpret_cast<int32_t *>(data_ + schema->GetOffset(i)) = offset;
      // Serialize varchar value, in place(size+data)
      values[i].SerializeTo(data_ + offset);
      offset += (values[i].GetLength() + sizeof(uint32_t));
    } else {
      values[i].SerializeTo(data_ + schema->GetOffset(i));
    }
  }
}

// Copy constructor
Tuple::Tuple(const Tuple &other)
    : allocated_(other.allocated_), rid_(other.rid_), size_(other.size_) {
  // deep copy
  if (allocated_ == true) {
    // LOG_DEBUG("tuple deep copy");
    data_ = new char[size_];
    memcpy(data_, other.data_, size_);
  } else {
    // LOG_DEBUG("tuple shallow copy");
    data_ = other.data_;
  }
}

Tuple &Tuple::operator=(const Tuple &other) {
  allocated_ = other.allocated_;
  rid_ = other.rid_;
  size_ = other.size_;
  // deep copy
  if (allocated_ == true) {
    // LOG_DEBUG("tuple deep copy");
    data_ = new char[size_];
    memcpy(data_, other.data_, size_);
  } else {
    // LOG_DEBUG("tuple shallow copy");
    data_ = other.data_;
  }

  return *this;
}

// Get the value of a specified column (const)
Value Tuple::GetValue(Schema *schema, const int column_id) const {
  assert(schema);
  assert(data_);
  const TypeId column_type = schema->GetType(column_id);
  const char *data_ptr = GetDataPtr(schema, column_id);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

const char *Tuple::GetDataPtr(Schema *schema, const int column_id) const {
  assert(schema);
  assert(data_);
  bool is_inlined = schema->IsInlined(column_id);
  // for inline type, data are stored where they are
  if (is_inlined)
    return (data_ + schema->GetOffset(column_id));
  else {
    // step1: read relative offset from tuple data
    int32_t offset =
        *reinterpret_cast<int32_t *>(data_ + schema->GetOffset(column_id));
    // step 2: return beginning address of the real data for VARCHAR type
    return (data_ + offset);
  }
}

std::string Tuple::ToString(Schema *schema) const {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";
  os << " Tuple size is " << size_;

  return os.str();
}

void Tuple::SerializeTo(char *storage) const {
  memcpy(storage, &size_, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_, size_);
}

void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = *reinterpret_cast<const int32_t *>(storage);
  // construct a tuple
  this->size_ = size;
  if (this->allocated_)
    delete[] this->data_;
  this->data_ = new char[this->size_];
  memcpy(this->data_, storage + sizeof(int32_t), this->size_);
  this->allocated_ = true;
}

} // namespace cmudb
