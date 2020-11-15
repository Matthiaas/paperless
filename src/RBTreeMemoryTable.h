#ifndef PAPERLESS_RBTREEMEMORYTABLE_H
#define PAPERLESS_RBTREEMEMORYTABLE_H

#include <map>
#include <memory>

#include "Element.h"
#include "Status.h"

class RBTreeMemoryTable {
 public:
  using container_t = std::map<std::shared_ptr<Element>, std::shared_ptr<ElementWithTombstone>>;
  using const_iterator = typename container_t::const_iterator;

  RBTreeMemoryTable() = default;

  void put(std::shared_ptr<Element> key, std::shared_ptr<ElementWithTombstone> value);

  // TODO: This is a hotfix so that MemoryTableManager compiles.
  // MemoryTableManager allocates new memory table if size
  // of the current one goes above some threshold.
  int size() const;

  QueryResult get(std::shared_ptr<Element> key);

  const_iterator begin() const;

  const_iterator end() const;

 private:
  std::map<std::shared_ptr<Element>, std::shared_ptr<ElementWithTombstone>> container;
};

#endif  // PAPERLESS_RBTREEMEMORYTABLE_H
