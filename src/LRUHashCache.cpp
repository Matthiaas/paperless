#include "LRUHashCache.h"
#include <mpi.h>
#include <iostream>

LRUHashCache::LRUHashCache(size_t max_size, size_t avg_key_size) :
    container_((max_size/avg_key_size) * 1.3 ), max_byte_size_(max_size), cur_byte_size_(0) {
  container_.max_load_factor(0.8);
}

void LRUHashCache::put(const ElementView& key, Hash h, const QueryResult& value) {
  put(Element::copyElementContent(key), h, value);
}

void LRUHashCache::put(Element&& key, Hash h, const QueryResult& value) {
  std::lock_guard<std::mutex> lck(m_);

  // Insert Elements
  if(value.hasValue())
    cur_byte_size_ += value->Length();
  size_t key_length = key.Length();
  QueryResult new_value = value.hasValue() ?
                          QueryResult(Element::copyElementContent(value.Value())):
                          QueryResult(value.Status());

  Key k {key.GetView(), h};
  queue.push_front(k);
  auto queue_it = queue.begin();
  Value v{std::move(key), std::move(new_value), queue_it};

  auto emplace_result = container_.try_emplace(std::move(k), std::move(v));
  if (!emplace_result.second) {  // The key was in the map already.
    auto it = emplace_result.first;
    queue.erase(it->second.it);
    // try_emplace doesn't move the value unless insertion actually happened.
    cur_byte_size_ -= it->second.Length();
    // We need to set the front Element view in the queue
    // to the actual key that is in the map.
    queue.front() = it->first;
    it->second.value = std::move(v.value);  // NOLINT(bugprone-use-after-move)
    it->second.it = queue_it;
  } else {
    cur_byte_size_ += key_length;
  }
  CleanUp();
}

std::optional<QueryResult> LRUHashCache::get(const ElementView& key, Hash h) {
  std::lock_guard<std::mutex> lck(m_);
  Key kv{key, h};
  auto it = container_.find(kv);
  if(it == container_.end()) {
    return std::nullopt;
  }
  queue.erase(it->second.it);
  queue.push_front(it->first);
  it->second.it = queue.begin();
  const QueryResult& value = it->second.value;
  QueryResult res = value.hasValue() ?
                    QueryResult(Element::copyElementContent(value.Value())):
                    QueryResult(value.Status());
  return res;
}

std::pair<QueryStatus, size_t>
LRUHashCache::get(const ElementView& key, Hash h,
                  const ElementView& value_buff) {
  std::lock_guard<std::mutex> lck(m_);
  Key kv{key, h};
  auto it = container_.find(kv);
  if(it == container_.end()) {
    return {QueryStatus::NOT_IN_CACHE, 0};
  }
  queue.erase(it->second.it);
  queue.push_front(it->first);
  it->second.it = queue.begin();
  const QueryResult& value = it->second.value;
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
void LRUHashCache::CleanUp() {
  // Delete Elements until cache has OK size.
  while(cur_byte_size_ > max_byte_size_ && !queue.empty()) {
    auto it = queue.end();
    --it;
    auto e_it = container_.find(*it);
    cur_byte_size_ -= e_it->second.Length();
    cur_byte_size_ -= e_it->first.el.Length();
    container_.erase(e_it);
    queue.erase(it);
  }
}
size_t LRUHashCache::size() {
  std::lock_guard<std::mutex> lck(m_);
  return cur_byte_size_;
}

void LRUHashCache::clear() {
  std::lock_guard<std::mutex> lck(m_);
  queue.clear();
  container_.clear();
  cur_byte_size_ = 0;
}
