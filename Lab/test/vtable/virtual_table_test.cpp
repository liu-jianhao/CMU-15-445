/**
 * virtual_table_test.cpp
 */
#include "vtable/testing_vtable_util.h"

namespace cmudb {
/** Load the virtual table extension
 *  Ref: https://sqlite.org/c3ref/load_extension.html
 */

TEST(VtableTest, BasicTest) {
  EXPECT_TRUE(sqlite3_threadsafe());
  std::string db_file = "sqlite.db";
  remove(db_file.c_str());
  remove("vtable.db");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(db_file.c_str(), &db);
  EXPECT_EQ(rc, SQLITE_OK);

  rc = sqlite3_enable_load_extension(db, 1);
  EXPECT_EQ(rc, SQLITE_OK);

  const char *zFile = "libvtable"; // shared library name
  const char *zProc = 0;           // entry point within library
  char *zErrMsg = 0;
  rc = sqlite3_load_extension(db, zFile, zProc, &zErrMsg);
  EXPECT_EQ(rc, SQLITE_OK);

  EXPECT_TRUE(ExecSQL(
      db, "CREATE VIRTUAL TABLE foo USING vtable('a int, b varchar(13)','foo_pk a')"));
  EXPECT_TRUE(ExecSQL(db, "INSERT INTO foo VALUES(1, 'hello')"));
  EXPECT_TRUE(ExecSQL(db, "SELECT * FROM foo ORDER BY a"));
  EXPECT_TRUE(ExecSQL(db, "DROP TABLE foo"));

  rc = sqlite3_close(db);
  EXPECT_EQ(rc, SQLITE_OK);

  remove(db_file.c_str());
  remove("vtable.db");
  return;
}

TEST(VtableTest, CreateTest) {
  EXPECT_TRUE(sqlite3_threadsafe());
  std::string db_file = "sqlite.db";
  remove(db_file.c_str());
  remove("vtable.db");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(db_file.c_str(), &db);
  EXPECT_EQ(rc, SQLITE_OK);

  rc = sqlite3_enable_load_extension(db, 1);
  EXPECT_EQ(rc, SQLITE_OK);

  const char *zFile = "libvtable"; // shared library name
  const char *zProc = 0;           // entry point within library
  char *zErrMsg = 0;
  rc = sqlite3_load_extension(db, zFile, zProc, &zErrMsg);
  EXPECT_EQ(rc, SQLITE_OK);

  EXPECT_TRUE(ExecSQL(
      db, "CREATE VIRTUAL TABLE foo1 USING vtable ('a INT, b "
          "int, c smallint, d varchar, e bigint, f bool', 'foo1_pk b')"));
  EXPECT_TRUE(ExecSQL(db, "INSERT INTO foo1 VALUES(1, 2, 3, 'hello', 2,1)"));
  EXPECT_TRUE(ExecSQL(db, "INSERT INTO foo1 VALUES(3, 4, 5, 'Nihao',4, 1)"));
  EXPECT_TRUE(ExecSQL(db, "INSERT INTO foo1 VALUES(2, 3, 4, 'world',3, 1)"));
  EXPECT_TRUE(ExecSQL(db, "SELECT * FROM foo1"));
  EXPECT_TRUE(ExecSQL(db, "DELETE FROM foo1 WHERE b = 2"));
  EXPECT_TRUE(ExecSQL(db, "SELECT * FROM foo1"));
  EXPECT_TRUE(ExecSQL(db, "DROP TABLE foo1"));

  rc = sqlite3_close(db);
  EXPECT_EQ(rc, SQLITE_OK);

  remove(db_file.c_str());
  remove("vtable.db");
  return;
}
} // namespace cmudb
