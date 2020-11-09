//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_MEMORYTABLE_H
#define PAPERLESS_MEMORYTABLE_H

#include <cstddef>
#include <functional>
#include "Element.h"
#include "Status.h"

// Should this be a pure virtual class? So we can extend it as a RBTree Memory
// table?
// We could also not make this virtual and just manually enforce the interface this is better for performance!
class MemoryTable {
public:
  explicit MemoryTable(std::function<int(Element,Element)> comparator);
  MemoryTable(); // should define a standard comparator;


  // Whats about the tombstone bit?=
  void put(Element key, Element val);
  QueryResult get(Element key);


  // Required for flushing to disk in the StorageManager
  class iterator {
   public:
    iterator(const iterator&);
    ~iterator();
    iterator& operator=(const iterator&);
    iterator& operator++(); //prefix increment
    bool operator==(const iterator&);
    bool operator!=(const iterator&);
    std::pair<Element, Element> operator*() const;
    friend void swap(iterator& lhs, iterator& rhs);
  };
  iterator begin() const;
  iterator end() const;

private:
  std::function<int(Element,Element)> comparator_;
};


#endif //PAPERLESS_MEMORYTABLE_H
