//
// Created by matthias and roman on 08.11.20.
// this is the true manager
//

#ifndef PAPERLESS_MEMORYTABLEMANAGER_H
#define PAPERLESS_MEMORYTABLEMANAGER_H

#include <mutex>
#include <memory>

#include "Element.h"
#include "Types.h"
#include "Status.h"

// MemoryTableManager operates the in-memory layers of Paperless. Entries are
// stored in MemoryTable and once it overflows, the whole MemoryTable is enqueued
// to Queue and a new MemoryTable gets allocates. 
// Thread-safe.
template <typename MemoryTable, typename Queue>
class MemoryTableManager {
public:
  MemoryTableManager(int consumer_count, int max_mtable_size)
    : consumer_count_(consumer_count), max_mtable_size_(max_mtable_size)
   {
    mtable_ = std::make_unique<MemoryTable>();
  }

  // Inserts element.
  void Put(const Element& key, Tomblement value, Hash hash, Owner owner) {
    // Faster implementation that doesn't require locking the whole memory table.
    // auto cur_mtable = mtable_;
    // cur_mtable_->writers++;
    // size_t sz = cur_mtable->put();
    // if (sz < limit) { cur_mtable->writers--; return Status::OK; }
    // bool am_i_the_first_to_seal_this_bad_boy = cur_mtable->seal();
    // if (!am_i_the_first_to_seal_this_bad_boy) { cur_mtable->writers--; return Status::OK; }
    // auto x = new MemoryTable();
    // wbuffer_->enqueue(cur_mtable);
    // mtable_ = x;
    std::lock_guard<std::mutex> lock(mtable_mutex_);
    mtable_->put(key, std::move(value));
    if (mtable_->size() > max_mtable_size_) {
      wbuffer_.Enqueue(std::move(mtable_));
      mtable_ = std::make_unique<MemoryTable>();
    }
  }

  // Gets element, MemoryTable allocates memory for the result.
  QueryResult Get(const Element& key, Hash hash, Owner owner) const {
    {
      std::lock_guard<std::mutex> lock(mtable_mutex_);
      QueryResult result = mtable_->get(key);
      if (result != QueryStatus::NOT_FOUND) return result;
    }
    QueryResult result = wbuffer_.Get(key, hash, owner);
    if (result != QueryStatus::NOT_FOUND) return result;

    return QueryStatus::NOT_FOUND;
  }

  // Gets element, stores result in the user-provided `buffer`.
  QueryStatus Get(const Element& key, Hash hash, Owner owner, Element buffer) const {
    throw "Shape that diamond and implement me.";
  }

  // Retrieves part of `wbuffer_` that can be flushed. Blocking.
  typename Queue::Chunk GetChunk() {
    return wbuffer_.Dequeue();
  }

  // Blocks until all values inside MemoryTableManager get flushed. Concurrent writes
  // during Flush() call lead to undefined behaviour.
  void Flush() {
    std::lock_guard<std::mutex> lock(mtable_mutex_);
    wbuffer_.Enqueue(std::move(mtable_));
    mtable_ = std::make_unique<MemoryTable>();
    wbuffer_.WaitUntilEmpty();
  }

  // Flushes all values and sends poison pills to consumers. Concurrent writes during
  // Shutdown() call lead to undefined behaviour.
  void Shutdown() {
    std::lock_guard<std::mutex> lock(mtable_mutex_);
    wbuffer_.Enqueue(std::move(mtable_));
    mtable_ = nullptr;
    wbuffer_.EnqueuePoisonPills(consumer_count_);
    wbuffer_.WaitUntilEmpty();
  }

  // Returns number of memory tables stored in the manager.
  size_t Size() {
    size_t ret = 0;
    {
      std::lock_guard<std::mutex> lock(mtable_mutex_);
      if (mtable_ != nullptr && mtable_->size() > 0) ++ret;
    }
    ret += wbuffer_.Size();
    return ret;
  }

private:
  std::unique_ptr<MemoryTable> mtable_;
  Queue wbuffer_;

  const int consumer_count_;
  const size_t max_mtable_size_;
  mutable std::mutex mtable_mutex_;
};


#endif //PAPERLESS_MEMORYTABLEMANAGER_H
