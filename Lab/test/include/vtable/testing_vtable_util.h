/**
 * testing_vtable_util.h
 */

#pragma once

#include <cstdio>
#include <iostream>
#include <string>

#include "sqlite/sqlite3.h"
#include "gtest/gtest.h"

namespace cmudb {

// For printing result
int ExecCallback(void *NotUsed, int argc, char **argv, char **azColName) {
  int i;
  for (i = 0; i < argc; i++) {
    std::printf("%10s|", azColName[i]);
  }
  std::printf("\n");
  for (i = 0; i < argc; i++) {
    std::printf("%10s|", argv[i] ? argv[i] : "NULL");
  }
  std::printf("\n");
  return 0;
}

bool ExecSQL(sqlite3 *db, std::string sql) {
  char *zErrMsg = 0;
  int rc = sqlite3_exec(db, sql.c_str(), ExecCallback, 0, &zErrMsg);
  if (rc != SQLITE_OK) {
    std::cerr << "SQL error: " + std::string(zErrMsg) << std::endl;
    sqlite3_free(zErrMsg);
    return false;
  }
  return true;
}

} // namespace cmudb