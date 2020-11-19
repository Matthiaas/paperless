#ifndef PAPERLESS_RBTREEMEMORYTABLE_H
#define PAPERLESS_RBTREEMEMORYTABLE_H

#include <map>
#include <memory>

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

  size_t size() const;

  QueryResult get(const ElementView& key);

  const_iterator begin() const;

  const_iterator end() const;

 private:
  container_t container_;
  size_t total_bytes;
};

#endif  // PAPERLESS_RBTREEMEMORYTABLE_H
