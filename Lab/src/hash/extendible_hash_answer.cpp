#include <cassert>
#include <functional>
#include <list>
#include <bitset>
#include <iostream>

#include "hash/extendible_hash.h"
#include "page/page.h"
#include "common/logger.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size):
    bucket_size_(size), bucket_count_(0), depth(0),
    pair_count(0) {
  directory_.emplace_back(new Bucket(0, 0));
  // initial: 1 bucket
  bucket_count_ = 1;
}

/*
 * helper function to calculate the hashing address of input key
 * std::hash<>: assumption already has specialization for type K
 * namespace std have standard specializations for basic types.
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  assert(0 <= bucket_id && bucket_id < static_cast<int>(directory_.size()));
  if (directory_[bucket_id]) {
    return directory_[bucket_id]->depth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return bucket_count_;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = bucketIndex(key);

  if (directory_[index]) {
    auto bucket = directory_[index];
    if (bucket->items.find(key) != bucket->items.end()) {
      value = bucket->items[key];
      return true;
    }
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  size_t cnt = 0;
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = bucketIndex(key);

  if (directory_[index]) {
    auto bucket = directory_[index];
    cnt += bucket->items.erase(key);
    pair_count -= cnt;
  }
  return cnt != 0;
}

/*
 * helper function to split a bucket when is full, overflow if necessary
 */
template <typename K, typename V>
std::unique_ptr<typename ExtendibleHash<K, V>::Bucket>
ExtendibleHash<K, V>::split(std::shared_ptr<Bucket> &b) {
  auto res = std::make_unique<Bucket>(0, b->depth);
  while (res->items.empty()) {
    ++b->depth;
    ++res->depth;
    for (auto it = b->items.begin(); it != b->items.end();) {
      if (HashKey(it->first) & (1 << (b->depth - 1))) {
        res->items.insert(*it);
        res->id = HashKey(it->first) & ((1 << b->depth) - 1);
        it = b->items.erase(it);
      } else {
        ++it;
      }
    }
    if (b->items.empty()) {
      b->items.swap(res->items);
      // update id;
      b->id = res->id;
    }
    // which all keys in current bucket have same hash value, should be a rare case
    if (b->depth == sizeof(size_t)*8) {
      b->overflow = true;
      return nullptr;
    }
  }

  // maintain bucket count current in use
  ++bucket_count_;
  return res;
}

/*
 * helper function to find bucket index
 * should be called when holding the lock
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::bucketIndex(const K &key) {
  return HashKey(key) & ((1 << depth) - 1);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t bucket_id = bucketIndex(key);

  assert(bucket_id < directory_.size());
  if (directory_[bucket_id] == nullptr) {
    directory_[bucket_id] = std::make_shared<Bucket>(bucket_id, depth);
    ++bucket_count_;
  }
  auto bucket = directory_[bucket_id];

  // already in bucket, override
  if (bucket->items.find(key) != bucket->items.end()) {
    bucket->items[key] = value;
    return;
  }

  // insert to target bucket
  bucket->items.insert({key, value});
  ++pair_count;

  // may need split & redistribute bucket
  if (bucket->items.size() > bucket_size_ && !bucket->overflow) {
    // record original bucket index and local depth
    auto old_index = bucket->id;
    auto old_depth = bucket->depth;

    std::shared_ptr<Bucket> new_bucket = split(bucket);

    // if overflow restore original local depth
    if (new_bucket == nullptr) {
      bucket->depth = old_depth;
      return;
    }

    // rearrange pointers, may need increase global depth
    if (bucket->depth > depth) {
      auto size = directory_.size();
      auto factor = (1 << (bucket->depth - depth));

      // global depth always greater equal than local depth
      depth = bucket->depth;
      directory_.resize(directory_.size()*factor);

      // fill original/new bucket in directory
      directory_[bucket->id] = bucket;
      directory_[new_bucket->id] = new_bucket;

      // update to right index: for buckets not the split point
      for (size_t i = 0; i < size; ++i) {
        if (directory_[i]) {
          // clear stale relation
          if (i < directory_[i]->id ||
              // important filter: not prefix any more
              ((i & ((1 << directory_[i]->depth) - 1)) != directory_[i]->id)) {
            directory_[i].reset();
          } else {
            auto step = 1 << directory_[i]->depth;
            for (size_t j = i + step; j < directory_.size(); j += step) {
              directory_[j] = directory_[i];
            }
          }
        }
      }
    } else {
      // reset directory entries which points to the same bucket before split
      for (size_t i = old_index; i < directory_.size(); i += (1 << old_depth)) {
        directory_[i].reset();
      }

      // add all two new buckets to directory
      directory_[bucket->id] = bucket;
      directory_[new_bucket->id] = new_bucket;

      auto step = 1 << bucket->depth;
      for (size_t i = bucket->id + step; i < directory_.size(); i += step) {
        directory_[i] = bucket;
      }
      for (size_t i = new_bucket->id + step; i < directory_.size(); i += step) {
        directory_[i] = new_bucket;
      }
    }
  }

  // for debug
  //dump(key);
}

/*
 * for debug, dump internal data structure
 */
//template <typename K, typename V> void ExtendibleHash<K, V>::dump(const K &key) {
//  LOG_DEBUG("---------------------------------------------------------------");
//  LOG_DEBUG("Global depth: %d, key: %d (%#x)", depth, key, key);
//  int i = 0;
//  for (auto b: directory_) {
//    if (b != nullptr) {
//      LOG_DEBUG("bucket: %d (%zu) -> %p, local depth: %d", i, b->id, b.get(), b->depth);
//      for (auto item: b->items) {
//        LOG_DEBUG("key: %#x, hash: %#zx", item.first, HashKey(item.first) & ((1 << b->depth) - 1));
//      }
//      LOG_DEBUG("\n");
//    } else {
//      LOG_DEBUG("bucket: %d -> nullptr ", i);
//    }
//    i++;
//  }
//}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
