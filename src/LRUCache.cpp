//
// Created by matthias on 20.11.20.
//

#include "LRUCache.h"
/*
void LRUCache::put(const ElementView& key, Tomblement&& value) {
  put(Element::copyElementContent(key), std::move(value));
}

void LRUCache::put(Element&& key, Tomblement&& value) {
  queue.push_back(key.GetView());
  std::list<ElementView>::iterator it = queue.end();
  Value v{std::move(value), it};
  memoryTable.put(std::move(key), std::move(v));
  while(memoryTable.size() > max_size && !queue.empty()) {
    ElementView e = queue.front();
    memoryTable.Delete(e);
  }
}

size_t LRUCache::size() const {
  return memoryTable.size();
}
QueryResult LRUCache::get(const ElementView& key) {
  return memoryTable.get(key);
}

std::pair<QueryStatus, size_t> LRUCache::get(const ElementView& key,
                                             char* value, size_t value_len) {
  return std::pair<QueryStatus, size_t>();
}
*/