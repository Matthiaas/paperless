#include "RBTreeMemoryTable.h"


void RBTreeMemoryTable::put(Element key, Tomblement&& value) {
  container.emplace(key, std::move(value));
}

QueryResult RBTreeMemoryTable::get(Element key) {
  auto it = container.find(key);
  if (it == container.end()) {
    return QueryStatus::NOT_FOUND;
  } else {
    auto& entry = it->second;
    if (entry.Tombstone()) { // Check tombstone bit.
      return QueryStatus::DELETED;
    } else { // Return the value.
      return entry;
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

