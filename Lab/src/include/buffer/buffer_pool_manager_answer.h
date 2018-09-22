/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once

#include <list>
#include <mutex>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "logging/log_manager.h"
#include "page/page.h"

namespace cmudb {

class BufferPoolManager {
public:
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager,
                    LogManager *log_manager = nullptr);

  ~BufferPoolManager();

  // disable copy
  BufferPoolManager(BufferPoolManager const &) = delete;
  BufferPoolManager &operator=(BufferPoolManager const &) = delete;

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

  // for debug
  bool Check() const {
    //std::cerr << "table: " << page_table_->Size() << " replacer: "
    //          << replacer_->Size() << std::endl;
    // +1 for header_page, in the test environment,
    // header_page is out the replacer's control
    return page_table_->Size() == (replacer_->Size() + 1);
  }

private:
  size_t pool_size_;                         // number of pages in buffer pool
  Page *pages_;                              // array of pages
  DiskManager *disk_manager_;
  LogManager *log_manager_;

  HashTable<page_id_t, Page *> *page_table_; // to keep track of pages that are currently in memory

  Replacer<Page *> *replacer_;               // to find an unpinned page for replacement
  std::list<Page *> *free_list_;             // to find a free page for replacement

  std::mutex latch_;                         // to protect shared data structure
};

} // namespace cmudb
