/**
 * log_recovery.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {

/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                       LogRecord &log_record) {
  // deserialize header, must have fields
  int32_t size_ = *reinterpret_cast<const int *>(data);
  lsn_t lsn_ = *reinterpret_cast<const lsn_t *>(data + 4);;
  txn_id_t txn_id_ = *reinterpret_cast<const lsn_t *>(data + 8);
  lsn_t prev_lsn_ = *reinterpret_cast<const lsn_t *>(data + 12);
  LogRecordType log_record_type_ = *reinterpret_cast<const LogRecordType *>(data + 16);

  if (size_ < 0 || lsn_ == INVALID_LSN || txn_id_ == INVALID_TXN_ID ||
      log_record_type_ == LogRecordType::INVALID) {
    return false;
  }

  // HEADER, 20bytes
  log_record.size_ = size_;
  log_record.lsn_ = lsn_;
  log_record.txn_id_ = txn_id_;
  log_record.prev_lsn_ = prev_lsn_;
  log_record.log_record_type_ = log_record_type_;

  switch (log_record_type_) {
  case LogRecordType::INSERT: {
    log_record.insert_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.insert_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    break;
  }
  case LogRecordType::MARKDELETE:
  case LogRecordType::ROLLBACKDELETE:
  case LogRecordType::APPLYDELETE: {
    log_record.delete_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.delete_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    break;
  }
  case LogRecordType::UPDATE: {
    log_record.update_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.old_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    log_record.new_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID) +
        log_record.old_tuple_.GetLength());
    break;
  }
  case LogRecordType::NEWPAGE: {
    log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(
        data + LogRecord::HEADER_SIZE);
    break;
  }
  default:break;
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  // no checkpoint support, always replay history from start
  offset_ = 0;

  // ENABLE_LOGGING must be false when recovery
  assert(ENABLE_LOGGING == false);

  // have more log?
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
    LogRecord log;
    int buffer_offset_ = 0;
    while (DeserializeLogRecord(log_buffer_ + buffer_offset_, log)) {
      // lsn -> offset mapping in WAL log
      lsn_mapping_[log.GetLSN()] = offset_ + buffer_offset_;

      if (log.GetLogRecordType() == LogRecordType::COMMIT ||
          log.GetLogRecordType() == LogRecordType::ABORT) {
        active_txn_.erase(log.GetTxnId());

      } else {
        active_txn_[log.GetTxnId()] = log.GetLSN();

        // Begin and NewPage logs can be ignored
        if (log.GetLogRecordType() == LogRecordType::INSERT) {
          RID rid = log.GetInsertRID();
          auto *page = reinterpret_cast<TablePage *>(
              buffer_pool_manager_->FetchPage(rid.GetPageId()));
          assert(page != nullptr);

          // log is newer than disk page?
          if (log.GetLSN() > page->GetLSN()) {
            page->WLatch();
            auto res = page->InsertTuple(log.GetInserteTuple(), rid, nullptr, nullptr, nullptr);
            assert(res);
            page->WUnlatch();
          }
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

        } else if (log.GetLogRecordType() == LogRecordType::MARKDELETE ||
            log.GetLogRecordType() == LogRecordType::ROLLBACKDELETE ||
            log.GetLogRecordType() == LogRecordType::APPLYDELETE) {
          RID rid = log.GetDeleteRID();

          auto *page = reinterpret_cast<TablePage *>(
              buffer_pool_manager_->FetchPage(rid.GetPageId()));
          assert(page != nullptr);

          // log is newer than disk page?
          if (log.GetLSN() > page->GetLSN()) {
            page->WLatch();
            if (log.GetLogRecordType() == LogRecordType::MARKDELETE) {
              auto res = page->MarkDelete(rid, nullptr, nullptr, nullptr);
              assert(res);
            } else if (log.GetLogRecordType() == LogRecordType::ROLLBACKDELETE) {
              page->RollbackDelete(rid, nullptr, nullptr);
            } else {
              page->ApplyDelete(rid, nullptr, nullptr);
            }
            page->WUnlatch();
          }
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

        } else if (log.GetLogRecordType() == LogRecordType::UPDATE) {
          RID rid = log.GetUpdateRID();
          auto *page = reinterpret_cast<TablePage *>(
              buffer_pool_manager_->FetchPage(rid.GetPageId()));
          assert(page != nullptr);

          // log is newer than disk page?
          if (log.GetLSN() > page->GetLSN()) {
            page->WLatch();
            auto res = page->UpdateTuple(log.GetUpdateNewTuple(), log.GetUpdateOldTuple(),
                                         rid, nullptr, nullptr, nullptr);
            assert(res);
            page->WUnlatch();
          }
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

        } else if (log.GetLogRecordType() == LogRecordType::NEWPAGE) {
          page_id_t pre_page_id = log.prev_page_id_;
          TablePage *page;

          // the first page
          if (pre_page_id == INVALID_PAGE_ID) {
            page = reinterpret_cast<TablePage *>(
                buffer_pool_manager_->NewPage(pre_page_id));
            assert(page != nullptr);
            page->WLatch();
            page->Init(pre_page_id, PAGE_SIZE, INVALID_PAGE_ID, nullptr, nullptr);
            page->WUnlatch();
          } else {
            page = reinterpret_cast<TablePage *>(
                buffer_pool_manager_->FetchPage(pre_page_id));
            assert(page != nullptr);

            if (page->GetNextPageId() == INVALID_PAGE_ID) {
              // alloc a new page
              page_id_t new_page_id;
              auto *new_page = reinterpret_cast<TablePage *>(
                  buffer_pool_manager_->NewPage(new_page_id));
              assert(new_page != nullptr);
              page->WLatch();
              page->SetNextPageId(new_page_id);
              page->WUnlatch();

              buffer_pool_manager_->UnpinPage(new_page_id, false);
            }
          }
          buffer_pool_manager_->UnpinPage(pre_page_id, true);
        }
      }
      buffer_offset_ += log.GetSize();
    }
    offset_ += LOG_BUFFER_SIZE;
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  // ENABLE_LOGGING must be false when recovery
  assert(ENABLE_LOGGING == false);

  char buffer[PAGE_SIZE];

  for (auto it = active_txn_.begin(); it != active_txn_.end(); ++it) {
    auto offset_ = lsn_mapping_[it->second];
    LogRecord log;

    // read log record, undo it, then get the pre_lsn
    disk_manager_->ReadLog(buffer, offset_, PAGE_SIZE);
    while (DeserializeLogRecord(buffer, log)) {
      if (log.log_record_type_ == LogRecordType::BEGIN) {
        // current txn is done
        break;
      }

      // undo
      if (log.log_record_type_ == LogRecordType::INSERT) {
        RID rid = log.GetInsertRID();
        auto *page = reinterpret_cast<TablePage *>(
            buffer_pool_manager_->FetchPage(rid.GetPageId()));
        page->WLatch();
        page->ApplyDelete(rid, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);

      } else if (log.log_record_type_ == LogRecordType::MARKDELETE ||
          log.log_record_type_ == LogRecordType::ROLLBACKDELETE ||
          log.log_record_type_ == LogRecordType::APPLYDELETE) {

        RID rid = log.GetDeleteRID();
        auto *page = reinterpret_cast<TablePage *>(
            buffer_pool_manager_->FetchPage(rid.GetPageId()));

        page->WLatch();
        if (log.log_record_type_ == LogRecordType::MARKDELETE) {
          page->RollbackDelete(rid, nullptr, nullptr);
        } else if (log.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
          page->MarkDelete(rid, nullptr, nullptr, nullptr);
        } else {
          page->InsertTuple(log.delete_tuple_, rid, nullptr, nullptr, nullptr);
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);

      } else if (log.log_record_type_ == LogRecordType::UPDATE) {
        RID rid = log.GetUpdateRID();
        auto *page = reinterpret_cast<TablePage *>(
            buffer_pool_manager_->FetchPage(rid.GetPageId()));
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);

        page->WLatch();
        page->UpdateTuple(log.old_tuple_, log.new_tuple_, rid,
                          nullptr, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
      }

      offset_ = lsn_mapping_[log.prev_lsn_];
      disk_manager_->ReadLog(buffer, offset_, PAGE_SIZE);
    }
  }

  active_txn_.clear();
  lsn_mapping_.clear();
}

} // namespace cmudb
