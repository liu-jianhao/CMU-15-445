#pragma once

namespace cmudb {
// Every possible SQL type ID
enum TypeId {
  INVALID = 0,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  VARCHAR,
  TIMESTAMP,
};
} // namespace cmudb
