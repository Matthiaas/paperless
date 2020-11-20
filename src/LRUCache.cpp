//
// Created by matthias on 20.11.20.
//

#include "LRUCache.h"

LRUCache::LRUCache(size_t max_size) :
  max_byte_size_(max_size), cur_byte_size_(0) {}

void LRUCache::put(const ElementView& key, Tomblement&& value) {
  put(Element::copyElementContent(key), std::move(value));
}

void LRUCache::put(Element&& key, Tomblement&& value) {
  std::lock_guard<std::mutex> lck(m_);
  queue.push_front(key.GetView());
  auto it = queue.begin();


  // Insert Elements
  cur_byte_size_ += value.GetBufferLen();
  size_t key_length = key.Length();
  Value v{std::move(value), it};
  auto emplace_result = container_.try_emplace(std::move(key), std::move(v));
  if (!emplace_result.second) {  // The key was in the map already.
    auto it = emplace_result.first;
    // try_emplace doesn't move the value unless insertion actually happened.
    cur_byte_size_ -= it->second.GetBufferLen();
    it->second = std::move(v);  // NOLINT(bugprone-use-after-move)
  } else {
    cur_byte_size_ += key_length;
  }
  CleanUp();
}

QueryResult LRUCache::get(const ElementView& key) {
  std::lock_guard<std::mutex> lck(m_);
  auto it = container_.find(key);
  if(it == container_.end()) {
    return QueryStatus::NOT_FOUND;
  }
  queue.erase(it->second.it);
  queue.push_front(it->first.GetView());
  it->second.it = queue.begin();
  if(it->second.v.Tombstone()) {
    return QueryStatus::DELETED;
  } else {
    return it->second.v.CopyToElement();
  }
}

std::pair<QueryStatus, size_t> LRUCache::get(const ElementView& key,
                                             char* value, size_t value_len) {
  std::lock_guard<std::mutex> lck(m_);
  return std::pair<QueryStatus, size_t>();
}
void LRUCache::CleanUp() {
  // Delete Elements until cache has OK size.
  while(cur_byte_size_ > max_byte_size_ && !queue.empty()) {
    auto it = queue.end();
    --it;
    auto e_it = container_.find(*it);
    cur_byte_size_ -= e_it->second.GetBufferLen();
    cur_byte_size_ -= e_it->first.Length();
    container_.erase(e_it);
    queue.erase(it);
  }
}
size_t LRUCache::size() const { return cur_byte_size_; }
