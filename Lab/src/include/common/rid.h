/**
 * rid.h
 */

#pragma once

#include <cstdint>
#include <sstream>

#include "common/config.h"

namespace cmudb {

class RID {
public:
  RID() : page_id_(INVALID_PAGE_ID), slot_num_(-1) {}; // invalid rid

  RID(page_id_t page_id, int slot_num) : page_id_(page_id), slot_num_(slot_num) {};

  RID(int64_t rid) : page_id_(rid >> 32), slot_num_(rid) {};

  inline int64_t Get() const { return ((int64_t) page_id_) << 32 | slot_num_; }

  inline page_id_t GetPageId() const { return page_id_; }

  inline int GetSlotNum() const { return slot_num_; }

  inline void Set(page_id_t page_id, int slot_num) {
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

  inline std::string ToString() const {
    std::stringstream os;
    os << "page_id: " << page_id_;
    os << " slot_num: " << slot_num_;

    return os.str();
  }

  friend std::ostream &operator<<(std::ostream &os, const RID &rid) {
    return os << rid.ToString();
  }

  bool operator==(const RID &other) const {
    return page_id_ == other.page_id_ && slot_num_ == other.slot_num_;
  }

private:
  page_id_t page_id_;
  int slot_num_;      // logical offset from 0, 1...
};

} // namespace cmudb

namespace std {
template <> struct hash<cmudb::RID> {
  size_t operator()(const cmudb::RID &obj) const {
    return hash<int64_t>()(obj.Get());
  }
};
} // namespace std
