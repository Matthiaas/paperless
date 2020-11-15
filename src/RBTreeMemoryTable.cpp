#include "RBTreeMemoryTable.h"


void RBTreeMemoryTable::put(const NonOwningElement& key, Tomblement&& value) {
  container.emplace(Element::copyElementContent(key), std::move(value));
}

QueryResult RBTreeMemoryTable::get(const NonOwningElement& key) {
  // TODO: Dont copy here.
  auto k = Element::copyElementContent(key);
  auto it = container.find(k);
  if (it == container.end()) {
    return QueryStatus::NOT_FOUND;
  } else {
    auto& entry = it->second;
    if (entry.Tombstone()) { // Check tombstone bit.
      return QueryStatus::DELETED;
    } else { // Return the value.
      return entry.ToElement();
    }
  }
}

int RBTreeMemoryTable::size() const {
  return container.size();
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::begin() const {
  return container.begin();
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::end() const {
  return container.end();
}

