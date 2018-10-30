/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
  std::lock_guard<std::mutex> guard(latch_);
  if (flush_thread_on == false) {
    ENABLE_LOGGING = true;

    // true表明flush thread 在运行
    flush_thread_on = true;
    flush_thread_ = new std::thread(&LogManager::bgFsync, this);
  }
}


void LogManager::bgFsync() {
  while (flush_thread_on) {
    {
      std::unique_lock<std::mutex> lock(latch_);
      while (log_buffer_size_ == 0) {
        auto ret = cv_.wait_for(lock, LOG_TIMEOUT);
        if (ret == std::cv_status::no_timeout || flush_thread_on == false) {
          break;
        }
      }
      SwapBuffer();
    }

    // 写入磁盘
    disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
    std::unique_lock<std::mutex> lock(latch_);

    auto lsn = lastLsn(flush_buffer_, flush_buffer_size_);
    if (lsn != INVALID_LSN) {
      SetPersistentLSN(lsn);
    }

    flush_buffer_size_ = 0;
    flushed.notify_all();
  }
}

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  std::unique_lock<std::mutex> lock(latch_);
  if (flush_thread_on == true) {
    flush_thread_on = false;
    ENABLE_LOGGING = false;

    lock.unlock();
    cv_.notify_all();

    flush_thread_->join();
    lock.lock();
    delete flush_thread_;
  }
}

// 阻塞地完成flush
void LogManager::FlushNowBlocking() {
  GetBgTaskToWork();
  WaitUntilBgTaskFinish();
}

// 交换log_buffer_和flush_buffer_
void LogManager::SwapBuffer() {
  std::swap(flush_buffer_, log_buffer_);
  flush_buffer_size_ = log_buffer_size_;
  log_buffer_size_ = 0;
}

// 启动后台线程
void LogManager::GetBgTaskToWork() {
  cv_.notify_all();
}

// 等待flush任务的结束
void LogManager::WaitUntilBgTaskFinish() {
  std::unique_lock<std::mutex> condWait(latch_);
  while (flush_buffer_size_ != 0) {
    flushed.wait(condWait);
  }
}

int LogManager::lastLsn(char *buff, int size) {
  lsn_t cur = INVALID_LSN;
  char *ptr = buff;
  while (ptr < buff + size) {
    auto rec = reinterpret_cast<LogRecord *>(ptr);
    cur = rec->GetLSN();
    auto len = rec->GetSize();
    ptr = ptr + len;
  }
  return cur;
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  auto size = log_record.GetSize();
  std::unique_lock<std::mutex> guard(log_mtx_);
  std::unique_lock<std::mutex> guard2(latch_);
  log_record.lsn_ = next_lsn_++;
  if (size + log_buffer_size_ > LOG_BUFFER_SIZE) {
    // 叫醒后台线程
    GetBgTaskToWork();
    guard2.unlock();
    WaitUntilBgTaskFinish();
    assert(log_buffer_size_ == 0);
    guard2.lock();
  }

  int pos = log_buffer_size_;
  memcpy(log_buffer_ + pos, &log_record, LogRecord::HEADER_SIZE);
  pos += LogRecord::HEADER_SIZE;

  if (log_record.log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
    pos += sizeof(RID);

    // tuple提供了序列化的函数
    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE
      || log_record.log_record_type_ == LogRecordType::MARKDELETE
      || log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
    memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += log_record.old_tuple_.GetLength() + sizeof(int32_t);
    log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
    memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(log_record.prev_page_id_));
  }

  log_buffer_size_ += size;
  return log_record.lsn_;
}

} // namespace cmudb
