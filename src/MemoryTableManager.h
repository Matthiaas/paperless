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
// to WriteBuffer and a new MemoryTable gets allocates. 
// Thread-safe.
template <typename MemoryTable, typename WriteBuffer>
class MemoryTableManager {
public:
  MemoryTableManager(int rank_size, int max_mtable_size)
    : rank_size_(rank_size), max_mtable_size_(max_mtable_size)
   {
    mtable_ = std::make_unique<MemoryTable>();
  }

  // Inserts element.
  void put(std::shared_ptr<Element> key, std::shared_ptr<ElementWithTombstone>  value, Hash hash, Owner owner, bool tombstone) {
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
    mtable_->put(key, value, tombstone);
    if (mtable_->size() > max_mtable_size_) {
      wbuffer_.enqueue(std::move(mtable_));
      mtable_ = std::make_unique<MemoryTable>();
    }
  }

  // Gets element, allocates memory for result.
  QueryResult get(Element key, Hash hash, Owner owner) const {
    {
      std::lock_guard<std::mutex> lock(mtable_mutex_);
      auto result = mtable_->get(key);
      if (result != QueryStatus::NOT_FOUND) return result;
    }
    auto result = wbuffer_.get(key, hash, owner);
    if (result != QueryStatus::NOT_FOUND) return result;

    return QueryStatus::NOT_FOUND;
  }

  // Gets element, stores result in the user-provided `buffer`.
  QueryStatus get(Element key, Hash hash, Owner owner, Element buffer) const {
    throw "Shape that diamond and implement me.";
  }

  // Retrieves part of `wbuffer_` that can be flushed. Blocking.
  typename WriteBuffer::Chunk getChunk() {
    return wbuffer_.dequeue();
  }

  // Blocks until all of `wbuffer_` gets flushed. There should be no concurrent writes
  // with a `Flush()` call. 
  void Flush() {
    wbuffer_.WaitUntilEmpty();
  }

private:
  std::unique_ptr<MemoryTable> mtable_;
  WriteBuffer wbuffer_;

  const int rank_size_;
  const int max_mtable_size_;
  mutable std::mutex mtable_mutex_;
};


#endif //PAPERLESS_MEMORYTABLEMANAGER_H
