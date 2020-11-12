#include "RBTreeMemoryTable.h"

void RBTreeMemoryTable::put(Element key, Element value, bool tombstone) {
  container.emplace(key, ElementWithTombstone(value, tombstone));
}

QueryResult RBTreeMemoryTable::get(Element key) {
  auto it = container.find(key);
  if (it == container.end()) {
    return QueryStatus::NOT_FOUND;
  } else {
    auto entry = it->second;
    if (entry.tombstone) { // Check tombstone bit.
      return QueryStatus::DELETED;
    } else { // Return the value.
      return entry.element;
    }
  }
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::begin() const {
  return container.begin();
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::end() const {
  return container.end();
}

