/**
 * common.h
 */

#pragma once

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sys/time.h>

#include "table/tuple.h"

namespace cmudb {

// use a fixed schema to construct a random tuple
Tuple ConstructTuple(Schema *schema) {
  std::vector<Value> values;
  Value v(TypeId::INVALID);

  struct timeval t1;
  gettimeofday(&t1, NULL);
  srand(t1.tv_usec * t1.tv_sec);

  for (int i = 0; i < schema->GetColumnCount(); i++) {
    // get type
    TypeId type = schema->GetType(i);
    switch (type) {
    case TypeId::BOOLEAN:
      v = Value(type, rand() % 2);
      break;
    case TypeId::TINYINT:
      v = Value(type, (int8_t)rand() % 1000);
      break;
    case TypeId::SMALLINT:
    case TypeId::INTEGER:
      v = Value(type, (int32_t)rand() % 1000);
      break;
    case TypeId::BIGINT:
      v = Value(type, (int64_t)rand() % 100000);
      break;
    case TypeId::VARCHAR: {
      static const char alphanum[] = "0123456789"
                                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                     "abcdefghijklmnopqrstuvwxyz";
      int len = 1 + rand() % 9;
      char s[10];
      for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
      }
      s[len] = 0;
      v = Value(type, s, len + 1, true);
      break;
    }
    default:
      break;
    }
    values.emplace_back(v);
  }
  return Tuple(values, schema);
}

} // namespace cmudb
