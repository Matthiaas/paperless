#ifndef PAPERLESS_MEMORYTABLE_H
#define PAPERLESS_MEMORYTABLE_H

#include <cstddef>
#include <functional>
#include <memory>

#include "Element.h"
#include "Status.h"

// This is just a specification for MemoryTables. Compliancy with this is NOT
// enforced anywhere.
class MemoryTable {
 public:
  explicit MemoryTable(std::function<int(ElementView, ElementView)> comparator);

  MemoryTable();  // should define a standard comparator;

  virtual void put(ElementView key, Tomblement value, bool tombstone) = 0;
  virtual QueryResult get(ElementView key) = 0;

  // Required for flushing to disk in the StorageManager.
  class const_iterator {
   public:
    const_iterator(const const_iterator&);
    virtual ~const_iterator() = 0;
    const_iterator& operator=(const const_iterator&);
    virtual const_iterator& operator++() = 0;  // prefix increment
    virtual bool operator==(const const_iterator&) = 0;
    virtual bool operator!=(const const_iterator&) = 0;
    virtual std::pair<ElementView, ElementView> operator*() const = 0;
    friend void swap(const_iterator& lhs, const_iterator& rhs);
  };
  virtual const_iterator& begin() const = 0;
  virtual const_iterator& end() const = 0;

 private:
  std::function<int(ElementView, ElementView)> comparator_;
};

#endif  // PAPERLESS_MEMORYTABLE_H
