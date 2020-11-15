#include "RBTreeMemoryTable.h"


void RBTreeMemoryTable::put(const Element& key, Tomblement value) {
  container_.emplace(OwningElement::copyElementContent(key), std::move(value));
}

QueryResult RBTreeMemoryTable::get(const Element& key) {
  auto it = container_.find(key);
  if (it == container_.end()) {
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
  return container_.size();
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::begin() const {
  return container_.begin();
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::end() const {
  return container_.end();
}

