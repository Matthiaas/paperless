#ifndef PAPERLESS_RBTREEMEMORYTABLE_H
#define PAPERLESS_RBTREEMEMORYTABLE_H

#include <map>

#include "Element.h"
#include "Status.h"

class RBTreeMemoryTable {
 public:
  using container_t = std::map<Element, ElementWithTombstone>;
  using const_iterator = typename container_t::const_iterator;

  RBTreeMemoryTable() = default;

  void put(Element key, Element value, bool tombstone);

  // TODO: This is a hotfix so that MemoryTableManager compiles.
  // MemoryTableManager allocates new memory table if size
  // of the current one goes above some threshold.
  int size() const;

  QueryResult get(Element key);

  const_iterator begin() const;

  const_iterator end() const;

 private:
  std::map<Element, ElementWithTombstone> container;
};

#endif  // PAPERLESS_RBTREEMEMORYTABLE_H
