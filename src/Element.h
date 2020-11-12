//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_ELEMENT_H
#define PAPERLESS_ELEMENT_H

#include <cstddef>
#include <cstdlib>
#include <cstring>

// We might want to rename that.
class Element {
 public:
  Element(char* v, size_t len) : value(v), len(len) {}
  Element() : value(nullptr), len(0){};
  Element(const Element& other) = default;//: value(other.value), len(other.len) {}
  char* value;
  const size_t len;

  static Element copyElementContent(Element e) {
    Element res(static_cast<char*>(std::malloc(e.len)), e.len);
    std::memcpy(res.value, e.value, e.len);
    return res;
  }

  bool operator==(const Element& rhs) const {
    if (len != rhs.len) return false;
    for (size_t i = 0; i < len; i++) {
      if (value[i] != rhs.value[i]) return false;
    }
    return true;
  }

  bool operator<(const Element& other) const {
    if (len != other.len) return len < other.len;
    for (size_t i = 0; i < len; i++) {
      if (value[i] != other.value[i])
        return value[i] < other.value[i];
    }
    return false;
  }

};

class ElementWithTombstone {
 public:
  ElementWithTombstone(Element element, bool tombstone)
      : element(element), tombstone(tombstone) {}

  ElementWithTombstone(char *val, size_t val_len, bool tombstone)
      : element(Element(val, val_len)), tombstone(tombstone) {}

  Element element;
  bool tombstone;
};

#endif  // PAPERLESS_ELEMENT_H
