/**
 * disk_manager.h
 *
 * Disk manager takes care of the allocation and deallocation of pages within a
 * database. It also performs read and write of pages to and from disk, and
 * provides a logical file layer within the context of a database management
 * system.
 */

#pragma once
#include <atomic>
#include <fstream>
#include <future>
#include <string>

#include "common/config.h"

namespace cmudb {

class DiskManager {
public:
  DiskManager(const std::string &db_file);
  ~DiskManager();

  void WritePage(page_id_t page_id, const char *page_data);
  void ReadPage(page_id_t page_id, char *page_data);

  void WriteLog(char *log_data, int size);
  bool ReadLog(char *log_data, int size, int offset);

  page_id_t AllocatePage();
  void DeallocatePage(page_id_t page_id);

  int GetNumFlushes() const;
  bool GetFlushState() const;
  inline void SetFlushLogFuture(std::future<void> *f) { flush_log_f_ = f; }
  inline bool HasFlushLogFuture() { return flush_log_f_ != nullptr; }

private:
  int GetFileSize(const std::string &name);
  // stream to write log file
  std::fstream log_io_;
  std::string log_name_;
  // stream to write db file
  std::fstream db_io_;
  std::string file_name_;
  std::atomic<page_id_t> next_page_id_;
  int num_flushes_;
  bool flush_log_;
  std::future<void> *flush_log_f_;
};

} // namespace cmudb