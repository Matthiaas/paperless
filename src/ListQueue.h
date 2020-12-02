#ifndef PAPERLESS_LISTQUEUE_H
#define PAPERLESS_LISTQUEUE_H

#include <list>
#include <memory>
#include <shared_mutex>
#include <condition_variable>

#include "Element.h"
#include "Status.h"
#include "Types.h"

template <typename MemoryTable>
class ListQueue {
private:
  using List = typename std::list<std::unique_ptr<MemoryTable>>;
  using ListIterator = typename List::iterator;

public:
  class Chunk {
    public:
      Chunk(ListQueue<MemoryTable> *wbuffer, ListIterator it) : wbuffer_(wbuffer), it_(it) {}
      MemoryTable* Get() {
        return it_->get();
      }
      void Clear() {
        std::lock_guard<std::shared_mutex> lock(wbuffer_->mutex_);
        wbuffer_->list_.erase(it_);
        if (wbuffer_->list_.size() == 1) {  // Only dummy entry is left.
          wbuffer_->all_processed_cv_.notify_all();
        }
      }
      bool IsPoisonPill() {
        return *it_ == nullptr;
      }

    private:
      ListQueue<MemoryTable> *wbuffer_; // Must outlive the Chunk.
      ListIterator it_;
  };

  ListQueue() {
    next_dequeue_ = tail_dummy_ = list_.insert(list_.begin(), nullptr);
  }

  // Enqueues a MemoryTable.
  void Enqueue(std::unique_ptr<MemoryTable> mtable) {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto new_it = list_.insert(tail_dummy_, std::move(mtable));
    if (next_dequeue_ == tail_dummy_) {
      next_dequeue_ = new_it;
    }
    can_dequeue_cv_.notify_one();
  }

  // Enqueues `count` poison pills into the queue.
  void EnqueuePoisonPills(int count) {
    for (int i = 0; i < count; ++i) Enqueue(nullptr);
  }

  // Dequeues a MemoryTable. Blocking.
  Chunk Dequeue() {
    // Only acquire W lock when there's something to dequeue.
    while (true) {
      {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        can_dequeue_cv_.wait(lock, [this]{return next_dequeue_ != tail_dummy_;});
      }
      std::lock_guard<std::shared_mutex> lock(mutex_);
      if (next_dequeue_ != tail_dummy_) {
        return Chunk(this, next_dequeue_++);
      }
    }
  }

  // Gets element, allocates memory for result.
  QueryResult Get(const ElementView& key, Hash hash, Owner owner) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (auto const& mtable : list_) {
      // Skip the dummy entry.
      if (mtable == nullptr) continue;
      auto result = mtable->get(key);
      if (result != QueryStatus::NOT_FOUND) return result;
    }
    return QueryStatus::NOT_FOUND;
  }

  // Gets element, stores result in the user-provided `buffer`.
  std::pair<QueryStatus, size_t>  Get(const ElementView& key, char* value,
    size_t value_len, Hash hash, Owner owner) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (auto const& mtable : list_) {
      // Skip the dummy entry.
      if (mtable == nullptr) continue;
      auto result = mtable->get(key, value, value_len);
      if (result.first != QueryStatus::NOT_FOUND) return result;
    }
    return {QueryStatus::NOT_FOUND,0};
  }

  // Blocks until all of `wbuffer_` gets dequeued.
  void WaitUntilEmpty() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    // Wait until the list contains only the dummy entry.
    all_processed_cv_.wait(lock, [this]{return list_.size() == 1;});
  }

  size_t Size() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    // Decrement to account for the dummy tail.
    return list_.size() - 1;
  }

private:
  mutable std::shared_mutex mutex_;
  List list_;

  std::condition_variable_any can_dequeue_cv_;
  std::condition_variable_any all_processed_cv_;
  ListIterator next_dequeue_;
  ListIterator tail_dummy_;
};

// This is a copypaste of the above that uses W lock instead of RW lock.
template <typename MemoryTable>
class MutexedListQueue {
private:
  using List = typename std::list<std::unique_ptr<MemoryTable>>;
  using ListIterator = typename List::iterator;

public:
  class Chunk {
    public:
      Chunk(MutexedListQueue<MemoryTable> *wbuffer, ListIterator it) : wbuffer_(wbuffer), it_(it) {}
      MemoryTable* Get() {
        return it_->get();
      }
      void Clear() {
        std::lock_guard<std::mutex> lock(wbuffer_->mutex_);
        wbuffer_->list_.erase(it_);
        if (wbuffer_->list_.size() == 1) {  // Only dummy entry is left.
          wbuffer_->all_processed_cv_.notify_all();
        }
      }
      bool IsPoisonPill() {
        return *it_ == nullptr;
      }

    private:
      MutexedListQueue<MemoryTable> *wbuffer_; // Must outlive the Chunk.
      ListIterator it_;
  };

  MutexedListQueue() {
    next_dequeue_ = tail_dummy_ = list_.insert(list_.begin(), nullptr);
  }

  // Enqueues a MemoryTable.
  void Enqueue(std::unique_ptr<MemoryTable> mtable) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto new_it = list_.insert(tail_dummy_, std::move(mtable));
    if (next_dequeue_ == tail_dummy_) {
      next_dequeue_ = new_it;
    }
    can_dequeue_cv_.notify_one();
  }

  // Enqueues `count` poison pills into the queue.
  void EnqueuePoisonPills(int count) {
    for (int i = 0; i < count; ++i) Enqueue(nullptr);
  }

  // Dequeues a MemoryTable. Blocking.
  Chunk Dequeue() {
    std::unique_lock<std::mutex> lock(mutex_);
    can_dequeue_cv_.wait(lock, [this]{return next_dequeue_ != tail_dummy_;});
    return Chunk(this, next_dequeue_++);
  }

  // Gets element, allocates memory for result.
  QueryResult Get(const ElementView& key, Hash hash, Owner owner) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto const& mtable : list_) {
      // Skip the dummy entry.
      if (mtable == nullptr) continue;
      auto result = mtable->get(key);
      if (result != QueryStatus::NOT_FOUND) return result;
    }
    return QueryStatus::NOT_FOUND;
  }

  // Gets element, stores result in the user-provided `buffer`.
  std::pair<QueryStatus, size_t>  Get(const ElementView& key, char* value,
    size_t value_len, Hash hash, Owner owner) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto const& mtable : list_) {
      // Skip the dummy entry.
      if (mtable == nullptr) continue;
      auto result = mtable->get(key, value, value_len);
      if (result.first != QueryStatus::NOT_FOUND) return result;
    }
    return {QueryStatus::NOT_FOUND,0};
  }

  // Blocks until all of `wbuffer_` gets dequeued.
  void WaitUntilEmpty() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Wait until the list contains only the dummy entry.
    all_processed_cv_.wait(lock, [this]{return list_.size() == 1;});
  }

  size_t Size() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Decrement to account for the dummy tail.
    return list_.size() - 1;
  }

private:
  mutable std::mutex mutex_;
  List list_;

  std::condition_variable can_dequeue_cv_;
  std::condition_variable all_processed_cv_;
  ListIterator next_dequeue_;
  ListIterator tail_dummy_;
};


#endif //PAPERLESS_LISTQUEUE_H
