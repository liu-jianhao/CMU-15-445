/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "hash/hash_table.h"

namespace cmudb {

// only support unique key
template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  struct Bucket {
    Bucket() = default;
    explicit Bucket(size_t i, int d) : id(i), depth(d) {}
    std::map<K, V> items;          // key-value pairs
    bool overflow = false;         // overflow
    size_t id = 0;                 // id of Bucket
    int depth = 0;                 // local depth counter
  };
public:
  // constructor
  explicit ExtendibleHash(size_t size);

  // disable copy
  ExtendibleHash(const ExtendibleHash &) = delete;
  ExtendibleHash &operator=(const ExtendibleHash &) = delete;

  // helper function to generate hash addressing
  size_t HashKey(const K &key);

  // helper function to get global & local depth
  int GetGlobalDepth() const;

  int GetLocalDepth(int bucket_id) const;

  int GetNumBuckets() const;

  // lookup and modifier
  bool Find(const K &key, V &value) override;

  bool Remove(const K &key) override;

  void Insert(const K &key, const V &value) override;

  size_t Size() const override { return pair_count; }

private:
  std::unique_ptr<Bucket> split(std::shared_ptr<Bucket> &);
  size_t bucketIndex(const K &key);
  //void dump(const K &key);

  mutable std::mutex mutex_;  // to protect shared data structure
  const size_t bucket_size_;  // largest number of elements in a bucket
  int bucket_count_;          // number of buckets in use
  int depth;                  // global depth
  size_t pair_count;          // key-value number in table
  std::vector<std::shared_ptr<Bucket>> directory_;  // smart pointer for auto memory management
};

} // namespace cmudb
