//
// Created by matthias and roman on 08.11.20.
// this is the true manager
//

#ifndef PAPERLESS_MEMORYTABLEMANAGER_H
#define PAPERLESS_MEMORYTABLEMANAGER_H


#include "Element.h"
#include "Types.h"
#include "Status.h"

template <class KVContainer>
class MemoryTableManager {
public:
  MemoryTableManager(int rankcount);
  void put(Element key, Element value, Hash hash, Owner owner);
  // @ roman lets use std::optional for now and change that later.
  QueryResult get(Element key, Hash hash, Owner owner);
  QueryStatus get(Element key, Hash hash, Owner owner, Element buffer);


  // Shouold be blocking ig there is no data.
  typename KVContainer::Handler getDataChunck();

  /*
   class Handler {
      public:
        class iterator {
          iterator(const iterator&);
          ~iterator();
          iterator& operator=(const iterator&);
          iterator& operator++(); //prefix increment
          std::pair<Element,Element> operator*() const;
          friend void swap(iterator& lhs, iterator& rhs);
        };

        void clear();
   }
   *
   */

};


#endif //PAPERLESS_MEMORYTABLEMANAGER_H
