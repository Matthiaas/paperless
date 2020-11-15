#include "RBTreeMemoryTable.h"


void RBTreeMemoryTable::put(std::shared_ptr<Element> key, std::shared_ptr<ElementWithTombstone> value) {
  container.emplace(key, value);
}

QueryResult RBTreeMemoryTable::get(Element key) {
  //TODO: Comparator for finding them directly without creating shared and copy it.
  auto k = std::make_shared<Element>(Element::copyElementContent(key));
  auto it = container.find(k);
  if (it == container.end()) {
    return QueryStatus::NOT_FOUND;
  } else {
    auto entry = it->second;
    if (entry->Tombstone()) { // Check tombstone bit.
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

