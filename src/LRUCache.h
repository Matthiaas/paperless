//
// Created by matthias on 20.11.20.
//

#ifndef PAPERLESS_LRUCACHE_H
#define PAPERLESS_LRUCACHE_H


#include "Element.h"
#include "Status.h"
#include <queue>


template
<class T>
class LRUCache {
public:
  void put(const ElementView& key, Tomblement&& value);

  void put(Element&& key, Tomblement&& value);

  size_t size() const;

  // Returns a copy of the value element.
  QueryResult get(const ElementView& key);

  // If found, copies the value into the user-provided buffer and returns OK,
  // value length. If the buffer is too small, returns BUFFER_TOO_SMALL and
  // value length.
  std::pair<QueryStatus, size_t> get(const ElementView& key, char* value,
                                     size_t value_len);
private:
  T memoryTable;
  std::deque<ElementView> queue;

};

#endif  // PAPERLESS_LRUCACHE_H
