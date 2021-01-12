//
// Created by matthias on 20.11.20.
//

#include "LRUTreeCache.h"
#include <mpi.h>
#include <iostream>

LRUTreeCache::LRUTreeCache(size_t max_size) :
  max_byte_size_(max_size), cur_byte_size_(0) {}

void LRUTreeCache::put(const ElementView& key, Hash h, const QueryResult& value) {
  put(Element::copyElementContent(key), h, value);
}

void LRUTreeCache::put(Element&& key, Hash, const QueryResult& value) {
  std::lock_guard<std::mutex> lck(m_);
  queue.push_front(key.GetView());
  auto it = queue.begin();

  // Insert Elements
  if(value.hasValue())
    cur_byte_size_ += value->Length();
  size_t key_length = key.Length();
  QueryResult new_value = value.hasValue() ?
      QueryResult(Element::copyElementContent(value.Value())):
      QueryResult(value.Status());

  Value v{std::move(new_value), it};
  auto emplace_result = container_.try_emplace(std::move(key), std::move(v));
  if (!emplace_result.second) {  // The key was in the map already.
    auto it = emplace_result.first;
    queue.erase(it->second.it);
    // try_emplace doesn't move the value unless insertion actually happened.
    cur_byte_size_ -= it->second.Length();
    // We need to set the front Element view in the queue
    // to the actual key that is in the map.
    queue.front() = it->first.GetView();
    it->second = std::move(v);  // NOLINT(bugprone-use-after-move)
  } else {
    cur_byte_size_ += key_length;
  }
  CleanUp();
}

std::optional<QueryResult> LRUTreeCache::get(const ElementView& key, Hash) {
  std::lock_guard<std::mutex> lck(m_);
  auto it = container_.find(key);
  if(it == container_.end()) {
    return std::nullopt;
  }
  queue.erase(it->second.it);
  queue.push_front(it->first.GetView());
  it->second.it = queue.begin();
  const QueryResult& value = it->second.v;
  QueryResult res = value.hasValue() ?
      QueryResult(Element::copyElementContent(value.Value())):
      QueryResult(value.Status());
  return res;
}

std::pair<QueryStatus, size_t> LRUTreeCache::get(const ElementView& key, Hash, const ElementView& value_buff) {
  std::lock_guard<std::mutex> lck(m_);
  auto it = container_.find(key);
  if(it == container_.end()) {
    return {QueryStatus::NOT_IN_CACHE, 0};
  }
  queue.erase(it->second.it);
  queue.push_front(it->first.GetView());
  it->second.it = queue.begin();
  const QueryResult& value = it->second.v;
  if(value.hasValue()) {
    if(value->Length() > value_buff.Length()) {
      return {QueryStatus::BUFFER_TOO_SMALL, value->Length()};
    } else {
      std::memcpy(value_buff.Value(), value->Value(), value->Length());
      return {QueryStatus::FOUND, value->Length()};
    }
  } else {
    return {value.Status(), 0};
  }
}
void LRUTreeCache::CleanUp() {
  // Delete Elements until cache has OK size.
  while(cur_byte_size_ > max_byte_size_ && !queue.empty()) {
    auto it = queue.end();
    --it;
    auto e_it = container_.find(*it);
    cur_byte_size_ -= e_it->second.Length();
    cur_byte_size_ -= e_it->first.Length();
    container_.erase(e_it);
    queue.erase(it);
  }
}
size_t LRUTreeCache::size() {
  std::lock_guard<std::mutex> lck(m_);
  return cur_byte_size_;
}

void LRUTreeCache::clear() {
  std::lock_guard<std::mutex> lck(m_);
  queue.clear();
  container_.clear();
  cur_byte_size_ = 0;
}

void LRUTreeCache::invalidateAll(const RBTreeMemoryTable& mtable, const ::HashFunction&) {
  std::lock_guard<std::mutex> lck(m_);
  auto it_cache = container_.begin();
  auto it_mtable = mtable.begin();
  while (it_cache != container_.end() && it_mtable != mtable.end()) {
    if (it_cache->first < it_mtable->first)
      ++it_cache;
    else if (it_mtable->first < it_cache->first)
      ++it_mtable;
    else {
      queue.erase(it_cache->second.it);
      container_.erase(it_cache++);
      ++it_mtable;
    }
  }
}
