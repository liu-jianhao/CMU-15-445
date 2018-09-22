/**
 * rwmutex_test.cpp
 */

#include <thread>

#include "common/rwmutex.h"
#include "gtest/gtest.h"

namespace cmudb {

class Counter {
public:
  Counter() : count_(0), mutex{} {}
  void Add(int num) {
    mutex.WLock();
    count_ += num;
    mutex.WUnlock();
  }
  int Read() {
    int res;
    mutex.RLock();
    res = count_;
    mutex.RUnlock();
    return res;
  }
private:
  int count_;
  RWMutex mutex;
};

TEST(RWMutexTest, BasicTest) {
  int num_threads = 100;
  Counter counter{};
  counter.Add(5);
  std::vector<std::thread> threads;
  for (int tid = 0; tid < num_threads; tid++) {
    if (tid%2 == 0) {
      threads.push_back(std::thread([&counter]() {
        counter.Read();
      }));
    } else {
      threads.push_back(std::thread([&counter]() {
        counter.Add(1);
      }));
    }
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  EXPECT_EQ(counter.Read(), 55);
}
}
