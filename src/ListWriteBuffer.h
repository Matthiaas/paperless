#ifndef PAPERLESS_LISTWRITEBUFFER_H
#define PAPERLESS_LISTWRITEBUFFER_H

#include <list>
#include <memory>
#include <condition_variable>

#include "Element.h"
#include "Types.h"
#include "Status.h"

template <typename MemoryTable>
class ListWriteBuffer {
private:
  using List = typename std::list<std::unique_ptr<MemoryTable>>;
  using ListIterator = typename List::iterator;

public:
  class Chunk {
    public:
      Chunk(ListWriteBuffer<MemoryTable> *wbuffer, ListIterator it) : wbuffer_(wbuffer), it_(it) {}
      MemoryTable* get() {
        return it_->get();
      }
      void clear() {
        std::lock_guard<std::mutex> lock(wbuffer_->mutex_);
        wbuffer_->list_.erase(it_);
        if (wbuffer_->list_.size() == 1) {  // Only dummy entry is left.
          wbuffer_->all_processed_cv_.notify_one();
        }
      }
    private:
      ListWriteBuffer<MemoryTable> *wbuffer_; // Must outlive the Chunk.
      ListIterator it_;
  };

  ListWriteBuffer() {
    next_dequeue_ = tail_dummy_ = list_.insert(list_.begin(), nullptr);
  }

  // Enqueues a MemoryTable.
  void enqueue(std::unique_ptr<MemoryTable> mtable) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto new_it = list_.insert(tail_dummy_, std::move(mtable));
    if (next_dequeue_ == tail_dummy_) {
      next_dequeue_ = new_it;
      can_dequeue_cv_.notify_one();
    }
  }

  // Dequeues a MemoryTable. Blocking.
  Chunk dequeue() {
    std::unique_lock<std::mutex> lock(mutex_);
    can_dequeue_cv_.wait(lock, [this]{return next_dequeue_ != tail_dummy_;});
    return Chunk(this, next_dequeue_++);
  }

  // Gets element, allocates memory for result.
  QueryResult get(const NonOwningElement& key, Hash hash, Owner owner) const {
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
  QueryStatus get(const NonOwningElement& key, Hash hash, Owner owner, Element buffer) const {
    throw "Hey you smartass, implement me.";
  }

  // Blocks until all of `wbuffer_` gets dequeued.
  void WaitUntilEmpty() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Wait until the list contains only the dummy entry.
    all_processed_cv_.wait(lock, [this]{return list_.size() == 1;});
  }

private:
  mutable std::mutex mutex_;
  List list_;

  std::condition_variable can_dequeue_cv_;
  std::condition_variable all_processed_cv_;
  ListIterator next_dequeue_;
  ListIterator tail_dummy_;
};

#endif //PAPERLESS_LISTWRITEBUFFER_H
