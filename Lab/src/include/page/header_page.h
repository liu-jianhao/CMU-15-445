/**
 * header_page.h
 *
 * Database use the first page (page_id = 0) as header page to store metadata, in
 * our case, we will contain information about table/index name (length less than
 * 32 bytes) and their corresponding root_id
 *
 * Format (size in byte):
 *  -----------------------------------------------------------------
 * | RecordCount (4) | Entry_1 name (32) | Entry_1 root_id (4) | ... |
 *  -----------------------------------------------------------------
 */

#pragma once

#include "page/page.h"

#include <cstring>

namespace cmudb {

class HeaderPage : public Page {
public:
  void Init() { SetRecordCount(0); }
  /**
   * Record related
   */
  bool InsertRecord(const std::string &name, const page_id_t root_id);
  bool DeleteRecord(const std::string &name);
  bool UpdateRecord(const std::string &name, const page_id_t root_id);

  // return root_id if success
  bool GetRootId(const std::string &name, page_id_t &root_id);
  int GetRecordCount();

private:
  /**
   * helper functions
   */
  int FindRecord(const std::string &name);

  void SetRecordCount(int record_count);
};
} // namespace cmudb
