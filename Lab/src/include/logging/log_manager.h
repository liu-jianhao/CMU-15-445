/**
 * log_manager.h
 * log manager maintain a separate thread that is awaken when the log buffer is
 * full or time out(every X second) to write log buffer's content into disk log
 * file.
 */

#pragma once
#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>
#include "disk/disk_manager.h"
#include "logging/log_record.h"

namespace cmudb {

class LogManager {
 public:
  LogManager(DiskManager *disk_manager)
      : next_lsn_(0), persistent_lsn_(INVALID_LSN),
        disk_manager_(disk_manager) {
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_thread_on = false;
  }

  ~LogManager() {
    StopFlushThread();
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }
  // spawn a separate thread to wake up periodically to flush
  void RunFlushThread();
  void StopFlushThread();
  void FlushNowBlocking();
  void SwapBuffer();
  void GetBgTaskToWork();
  void WaitUntilBgTaskFinish();
  int lastLsn(char* buff, int size);

  // guess this is the SerializeLogRecord mentioned project brief but doesn't show up in code base
  // append a log record into log buffer
  lsn_t AppendLogRecord(LogRecord &log_record);

  // get/set helper functions
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }

  void bgFsync();
 private:
  // TODO: you may add your own member variables
  // also remember to change constructor accordingly

  // atomic counter, record the next log sequence number
  std::atomic<lsn_t> next_lsn_;
  // log records before & include persistent_lsn_ have been written to disk
  std::atomic<lsn_t> persistent_lsn_;
  // log buffer related
  char *log_buffer_;
  char *flush_buffer_;
  // latch to protect shared member variables
  std::mutex latch_;
  // flush thread
  std::thread *flush_thread_;
  // for notifying flush thread
  std::condition_variable cv_;
  // disk manager
  DiskManager *disk_manager_;

  //========new member==========
  std::atomic<bool> flush_thread_on;
  std::condition_variable flushed;
  std::mutex log_mtx_;
  int flush_buffer_size_{0};
  int log_buffer_size_{0};
};

} // namespace cmudb
