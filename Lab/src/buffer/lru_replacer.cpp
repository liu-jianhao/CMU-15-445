/**
 * LRU implementation
 */
#include <cassert>

#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() : size_(0) {
        head_ = new node();
        tail_ = head_;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() = default;

    /*
     * Insert value into LRU
     */
    template <typename T> void LRUReplacer<T>::Insert(const T &value) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = table_.find(value);
        if(it == table_.end()) {
            tail_->next = new node(value, tail_);
            tail_ = tail_->next;
            table_.emplace(value, tail_);
            ++size_;
        } else {
            // 该页面是新插入的，如果本来就在队列尾就不用重新操作指针了
            if(it->second != tail_) {
                // 先从原位置移除
                node *pre = it->second->pre;
                node *cur = pre->next;
                pre->next = std::move(cur->next);
                pre->next->pre = pre;

                // 再放到尾部
                cur->pre = tail_;
                tail_->next = std::move(cur);
                tail_ = tail_->next;
            }
        }
    }

    /* If LRU is non-empty, pop the head member from LRU to argument "value", and
     * return true. If LRU is empty, return false
     */
    template <typename T> bool LRUReplacer<T>::Victim(T &value) {
        std::lock_guard<std::mutex> lock(mutex_);

        if(size_ == 0) {
            return false;
        }

        value = head_->next->data;
        head_->next = head_->next->next;
        if(head_->next != nullptr) {
            head_->next->pre = head_;
        }

        table_.erase(value);
        if(--size_ == 0) {
            tail_ = head_;
        }

        return true;
    }

    /*
     * Remove value from LRU. If removal is successful, return true, otherwise
     * return false
     */
    template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = table_.find(value);
        if(it != table_.end()) {
            if(it->second != tail_) {
                node *pre = it->second->pre;
                node *cur = pre->next;
                pre->next = std::move(cur->next);
                pre->next->pre = pre;
            } else {
                tail_ = tail_->pre;
                delete tail_->next;
            }

            table_.erase(value);
            if(--size_ == 0) {
                tail_ = head_;
            }
            return true;
        }

        return false;
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        std::lock_guard<std::mutex> lock(mutex_);
        return size_;
    }


    template class LRUReplacer<Page *>;
    // test only
    template class LRUReplacer<int>;

} // namespace cmudb
