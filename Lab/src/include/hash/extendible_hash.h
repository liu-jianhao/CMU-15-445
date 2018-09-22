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
    size_t id = 0;                 // id of Bucket
    int depth = 0;                 // local depth counter
  };
public:
    // 构造函数
    ExtendibleHash(size_t size);

    // 返回桶的偏移量
    size_t HashKey(const K &key);

    // 查找桶里的哈希表是否有该值
    bool Find(const K &key, V &value);

    // 插入元素
    void Insert(const K &key, const V &value);

    // 移除元素
    bool Remove(const K &key);

    size_t Size() const { return pair_count_; }

    // 返回哈希表当前深度
    int GetGlobalDepth() const;

    // 返回给定偏移的局部深度
    int GetLocalDepth(int bucket_id) const;

    // 返回桶总数
    int GetNumBuckets() const;

private:
    mutable std::mutex mutex_;      // 注意要加mutable

    const size_t bucket_size_;    // 每个桶能容纳的元素个数

    int bucket_count_;   // 在用的桶数

    size_t pair_count_;     // 哈希表中键值对的个数

    int depth;              // 全局的桶的深度

    std::vector<std::shared_ptr<Bucket>> bucket_;    // 桶数组

    std::shared_ptr<Bucket> split(std::shared_ptr<Bucket> &); // 分裂新桶
};

} // namespace cmudb
