//
// Created by matthias on 20.11.20.
//

#ifndef PAPERLESS_LRUCACHE_H
#define PAPERLESS_LRUCACHE_H

#include <list>
#include <mutex>
#include "Element.h"
#include "RBTreeMemoryTable.h"
#include "Status.h"

class LRUCache {

public:
  explicit LRUCache(size_t max_size);
  void put(const ElementView& key, const QueryResult& value);

  void put(Element&& key, const QueryResult& value);

  size_t size();

  // Returns a copy of the value element.
  std::optional<QueryResult> get(const ElementView& key);

  // If found, copies the value into the user-provided buffer and returns OK,
  // value length. If the buffer is too small, returns BUFFER_TOO_SMALL and
  // value length.
  std::pair<QueryStatus, size_t> get(const ElementView& key, char* value,
                                     size_t value_len);

  void clear();
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

  struct Value {
    QueryResult v;
    std::list<ElementView>::iterator it;

    [[nodiscard]] size_t Length() const {
      if(v.hasValue())
        return v->Length();
      else
        return 0;
    }
  };

  void CleanUp();

  std::map<Element, Value, Comparator> container_;
  std::list<ElementView> queue;
  std::mutex m_;
  size_t max_byte_size_;
  size_t cur_byte_size_;

};


#endif  // PAPERLESS_LRUCACHE_H
