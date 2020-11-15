//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_ELEMENT_H
#define PAPERLESS_ELEMENT_H

#include <cstddef>
#include <cstdlib>
#include <cstring>

class Element;


class NonOwningElement {
 public:
  NonOwningElement(char* v, size_t len) : value(v), len(len) {}
  NonOwningElement() : value(nullptr), len(0){};
  NonOwningElement(const NonOwningElement& other) = delete;
  NonOwningElement(NonOwningElement&& other) = default;
  NonOwningElement& operator=(const NonOwningElement&) = default;
  NonOwningElement& operator=(NonOwningElement&&) = default;

  char* Value() const {
    return value;
  }

  size_t Length() {
    return len;
  }

 private:
  friend Element;
  char* value;
  size_t len;



  /*
  static Element copyElementContent(NonOwningElement e) {
    Element res(static_cast<char*>(std::malloc(e.len)), e.len);
    std::memcpy(res.value, e.value, e.len);
    return res;
  }
   */

  bool operator==(const NonOwningElement& rhs) const {
    if (len != rhs.len) return false;
    for (size_t i = 0; i < len; i++) {
      if (value[i] != rhs.value[i]) return false;
    }
    return true;
  }

  bool operator<(const NonOwningElement& other) const {
    if (len != other.len) return len < other.len;
    for (size_t i = 0; i < len; i++) {
      if (value[i] != other.value[i])
        return value[i] < other.value[i];
    }
    return false;
  }
};

class Element {
 public:
  Element(char* v, size_t len) : el_(v, len) {}
  Element() {};
  Element(const Element& other) = delete;
  Element(Element&& other) {
    el_ = std::move(other.el_);
    other.el_.value = nullptr;
  };

  ~Element() {
    free(el_.value);
  }

  bool operator==(const Element& rhs) const {
    return el_ == rhs.el_;
  }
  bool operator<(const Element& other) const {
    return el_ < other.el_;
  }

  char* Value() const {
    return el_.value;
  }

  size_t Length() const {
    return el_.len;
  }

 private:
  NonOwningElement el_;
};




class Tomblement {
 public:
  Tomblement(char* v, size_t len) : len_(len) {
    value_ = static_cast<char*>(std::malloc(len_ + 1));
    value_[0] = 0;
    std::memcpy(value_ + 1, value_, len);
  }

  ~Tomblement() {
    free(value_);
  }

  Tomblement() :
      Tomblement(nullptr, 0) {};

  Tomblement(const Tomblement& other) = delete;
  Tomblement(Tomblement&& other) = default;

  static Tomblement getATombstone() {
    Tomblement res;
    res.value_[0] = false;
    return res;
  }

  char* Value() const {
    return value_ + 1;
  }

  size_t Length() const {
    return len_;
  }

  bool Tombstone() const {
    return value_[0];
  }

  char* GetBuffer() const {
    return value_;
  }

  size_t GetBufferLen() const {
    return len_ + 1;
  }

 private:
  char* value_;
  size_t len_;
};

/*
class ElementWithTombstone {
 public:
  ElementWithTombstone(Element element, bool tombstone)
      : element(element), tombstone(tombstone) {}

  ElementWithTombstone(char *val, size_t val_len, bool tombstone)
      : element(Element(val, val_len)), tombstone(tombstone) {}

  Element element;
  bool tombstone;
};

 */

#endif  // PAPERLESS_ELEMENT_H
