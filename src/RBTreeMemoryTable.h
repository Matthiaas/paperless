#ifndef PAPERLESS_RBTREEMEMORYTABLE_H
#define PAPERLESS_RBTREEMEMORYTABLE_H

#include <map>
#include <memory>

#include "Element.h"
#include "Status.h"

class RBTreeMemoryTable {
 public:
  using container_t = std::map<Element, Tomblement>;
  using const_iterator = typename container_t::const_iterator;

  RBTreeMemoryTable() = default;

  void put(Element&& key, Tomblement&& value);

  // TODO: This is a hotfix so that MemoryTableManager compiles.
  // MemoryTableManager allocates new memory table if size
  // of the current one goes above some threshold.
  int size() const;

  QueryResult get(const NonOwningElement& key);

  const_iterator begin() const;

  const_iterator end() const;

 private:
  struct Comparator
  {
    using is_transparent = std::true_type;
    bool operator()(const Element& lhs, const Element& rhs) const {
      return lhs < rhs;
    }
    bool operator()(const Element& lhs, const NonOwningElement& rhs) const {
      return lhs < rhs;
    }
    bool operator()(const NonOwningElement& lhs, const Element& rhs) const {
      return lhs < rhs;
    }
  };
  std::map<Element, Tomblement, Comparator> container;
};

#endif  // PAPERLESS_RBTREEMEMORYTABLE_H
