#include "RBTreeMemoryTable.h"

void RBTreeMemoryTable::put(Element&& key, Tomblement&& value) {
  total_bytes += value.Length();
  size_t key_len = key.Length();
  auto emplace_result =
      container_.try_emplace(std::move(key), std::move(value));

  if (!emplace_result.second) {  // The key was in the map already.
    auto it = emplace_result.first;
    // try_emplace doesn't move the value unless insertion actually happened,
    // so value can be used here.
    total_bytes -= it->second.Length();
    it->second = std::move(value);  // NOLINT(bugprone-use-after-move)
  } else {
    total_bytes += key_len + 1;  // +1 for Tombstone bit.
  }
}

void RBTreeMemoryTable::put(const ElementView& key, Tomblement&& value) {
  put(Element::copyElementContent(key), std::move(value));
}

QueryResult RBTreeMemoryTable::get(const ElementView& key) {
  auto it = container_.find(key);
  if (it == container_.end()) {
    return QueryStatus::NOT_FOUND;
  } else {
    auto& entry = it->second;
    if (entry.Tombstone()) {  // Check tombstone bit.
      return QueryStatus::DELETED;
    } else {  // Return the value.
      return entry.CopyToElement();
    }
  }
}

std::pair<QueryStatus, size_t> RBTreeMemoryTable::get(const ElementView& key,
                                                      char* value,
                                                      size_t value_len) {
  auto it = container_.find(key);
  if (it == container_.end()) {
    return {QueryStatus::NOT_FOUND, 0};
  } else {
    auto& entry = it->second;
    if (entry.Tombstone()) {  // Check tombstone bit.
      return {QueryStatus::DELETED, 0};
    } else {  // Return the value.
      if (entry.Length() <= value_len) {
        memcpy(value, entry.Value(), entry.Length());
        return {QueryStatus::FOUND, entry.Length()};
      } else { // User-provided buffer is too small.
        return {QueryStatus::BUFFER_TOO_SMALL, entry.Length()};
      }
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
