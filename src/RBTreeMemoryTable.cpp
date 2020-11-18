#include "RBTreeMemoryTable.h"

void RBTreeMemoryTable::put(const Element& key, Tomblement value) {
  total_bytes += value.Length();
  auto emplace_result = container_.try_emplace(
      OwningElement::copyElementContent(key), std::move(value));

  if (!emplace_result.second) {  // The key was in the map already.
    auto it = emplace_result.first;
    // try_emplace doesn't move the value unless insertion actually happened,
    // so value can be used here.
    total_bytes -= it->second.Length();
    it->second = std::move(value); // NOLINT(bugprone-use-after-move)
  } else {
    total_bytes += key.Length() + 1;  // +1 for Tombstone bit.
  }
}

QueryResult RBTreeMemoryTable::get(const Element& key) {
  auto it = container_.find(key);
  if (it == container_.end()) {
    return QueryStatus::NOT_FOUND;
  } else {
    auto& entry = it->second;
    if (entry.Tombstone()) {  // Check tombstone bit.
      return QueryStatus::DELETED;
    } else {  // Return the value.
      return entry.ToElement();
    }
  }
}

size_t RBTreeMemoryTable::size() const { return total_bytes; }

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::begin() const {
  return container_.begin();
}

RBTreeMemoryTable::const_iterator RBTreeMemoryTable::end() const {
  return container_.end();
}
