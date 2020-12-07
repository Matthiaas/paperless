#ifndef PAPERLESS_RBTREEMEMORYTABLE_H
#define PAPERLESS_RBTREEMEMORYTABLE_H

#include <atomic>
#include <map>
#include <memory>
#include <mutex>

#include "Element.h"
#include "Status.h"

class RBTreeMemoryTable {
 private:
  struct Comparator {
    using is_transparent = std::true_type;
    bool operator()(const Element& lhs, const Element& rhs) const {
      return lhs < rhs;
    }
    bool operator()(const Element& lhs, const ElementView& rhs) const {
      return lhs < rhs;
    }
    bool operator()(const ElementView& lhs, const Element& rhs) const {
      return lhs < rhs;
    }
  };

 public:
  using container_t = std::map<Element, Tomblement, Comparator>;
  using const_iterator = typename container_t::const_iterator;

  RBTreeMemoryTable() : total_bytes(0){};

  void put(const ElementView& key, Tomblement&& value);

  void put(Element&& key, Tomblement&& value);

  size_t ByteSize() const;
  size_t Count() const;

  // Returns a copy of the value element.
  QueryResult get(const ElementView& key);

  // If found, copies the value into the user-provided buffer and returns OK,
  // value length. If the buffer is too small, returns BUFFER_TOO_SMALL and
  // value length.
  std::pair<QueryStatus, size_t> get(const ElementView& key, char* value,
                                     size_t value_len);

  // Iterating over the container is not thread-safe!
  const_iterator begin() const;

  const_iterator end() const;

 private:
  container_t container_;
  std::mutex mutex_;
  std::atomic<size_t> total_bytes;
};

#endif  // PAPERLESS_RBTREEMEMORYTABLE_H
