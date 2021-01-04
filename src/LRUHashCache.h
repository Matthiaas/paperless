//
// Created by matthias on 30.11.20.
//

#ifndef PAPERLESS_LRUHASHCACHE_H
#define PAPERLESS_LRUHASHCACHE_H

#include "Element.h"
#include "Status.h"
#include "Types.h"

#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>

class LRUHashCache {
 public:
  LRUHashCache(size_t max_size, size_t avg_key_size);
  void put(const ElementView& key, Hash h, const QueryResult& value);

  void put(Element&& key, Hash h, const QueryResult& value);

  size_t size();

  // Returns a copy of the value element.
  std::optional<QueryResult> get(const ElementView& key, Hash h);

  // If found, copies the value into the user-provided buffer and returns OK,
  // value length. If the buffer is too small, returns BUFFER_TOO_SMALL and
  // value length.
  std::pair<QueryStatus, size_t> get(const ElementView& key, Hash h,
                                     const ElementView& value_buff);

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

  struct Key {
    ElementView el;
    Hash hash;
    bool operator==(const Key &other) const {
      return (hash == other.hash && el == other.el);
    }
  };

  struct Value {
    // This is a disgusting hack but we need an owner of that data.
    Element key;
    QueryResult value;
    std::list<Key>::iterator it;

    [[nodiscard]] size_t Length() const {
      if(value.hasValue())
        return value->Length();
      else
        return 0;
    }
  };



  struct HashFunction {
    using is_transparent = void;
    std::size_t operator()(const Key& k) const{
      return k.hash;
    }
  };

  void CleanUp();

  std::unordered_map<Key, Value, HashFunction> container_;
  std::list<Key> queue;
  std::mutex m_;
  size_t max_byte_size_;
  size_t cur_byte_size_;
};

#endif  // PAPERLESS_LRUHASHCACHE_H
