#include "RBTreeMemoryTable.h"

template <class Value>
void RBTreeMemoryTable<Value>::put(Element&& key, Value&& value) {
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

template <class Value>
void RBTreeMemoryTable<Value>::put(const ElementView& key, Value&& value) {
  put(Element::copyElementContent(key), std::move(value));
}
template <class Value>
QueryResult RBTreeMemoryTable<Value>::get(const ElementView& key) {
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

template <class Value>
std::pair<QueryStatus, size_t> RBTreeMemoryTable<Value>::get(
    const ElementView& key, char* value, size_t value_len) {
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

template <class Value>
size_t RBTreeMemoryTable<Value>::size() const { return total_bytes; }

template <class Value>
typename RBTreeMemoryTable<Value>::const_iterator
RBTreeMemoryTable<Value>::begin() const {
  return container_.begin();
}
template <class Value>
typename RBTreeMemoryTable<Value>::const_iterator
RBTreeMemoryTable<Value>::end() const {
  return container_.end();
}

template class RBTreeMemoryTable<Tomblement>;

