//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_ELEMENT_H
#define PAPERLESS_ELEMENT_H

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <utility>

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

  size_t Length() const {
    return len;
  }

 private:
  friend bool operator==(const NonOwningElement& lhs, const NonOwningElement& rhs);
  friend bool operator<(const NonOwningElement& lhs, const NonOwningElement& rhs);
  friend Element;
  char* value;
  size_t len;

};

class Element {
 public:
  Element(char* v, size_t len) {
    el_.value = static_cast<char*>(std::malloc(len));
    el_.len = len;
    std::memcpy(el_.value, v, len);
  }
  Element(const Element& other) = delete;
  Element(Element&& other) {
    el_ = std::move(other.el_);
    other.el_.value = nullptr;
  };

  ~Element() {
    free(el_.value);
  }

  static Element copyElementContent(const NonOwningElement& e) {
    Element res(static_cast<char*>(std::malloc(e.len)), e.len);
    std::memcpy(res.Value(), e.value, e.len);
    return res;
  }

  static Element copyElementContent(const Element& e) {
    Element res(static_cast<char*>(std::malloc(e.Length())), e.Length());
    std::memcpy(res.Value(), e.Value(), e.Length());
    return res;
  }

  char* Value() const {
    return el_.value;
  }

  size_t Length() const {
    return el_.len;
  }

 private:
  friend bool operator<(const Element& lhs, const Element& rhs);
  friend bool operator<(const NonOwningElement& lhs, const Element& rhs);
  friend bool operator<(const Element& lhs, const NonOwningElement& rhs);
  friend bool operator==(const Element& lhs, const NonOwningElement& rhs);
  friend bool operator==(const NonOwningElement& lhs, const Element& rhs);
  friend bool operator==(const Element& lhs, const Element& rhs);
  NonOwningElement el_;
};


class Tomblement {
 public:
  Tomblement(char* v, size_t len) : len_(len) {
    value_ = static_cast<char*>(std::malloc(len_ + 1));
    value_[0] = 0;
    std::memcpy(value_ + 1, v, len);
  }

  ~Tomblement() {
    free(value_);
  }

  Tomblement() :
      Tomblement(nullptr, 0) {};

  Tomblement(const Tomblement& other) = delete;
  Tomblement(Tomblement&& other)  {
    value_ = other.value_;
    len_= other.len_;
    other.value_ = nullptr;
  }

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

  bool operator==(const Tomblement& rhs) const {
    if (len_ != rhs.len_) return false;
    for (size_t i = 0; i < len_ + 1; i++) {
      if (value_[i] != rhs.value_[i]) return false;
    }
    return true;
  }

  bool operator<(const Tomblement& other) const {
    if (len_ != other.len_) return len_ < other.len_;
    for (size_t i = 0; i < len_ + 1; i++) {
      if (value_[i] != other.value_[i])
        return value_[i] < other.value_[i];
    }
    return false;
  }

 private:
  char* value_;
  size_t len_;
};

bool operator<(const NonOwningElement& lhs, const NonOwningElement& rhs);
bool operator<(const Element& lhs, const Element& rhs);
bool operator<(const NonOwningElement& lhs, const Element& rhs);
bool operator<(const Element& lhs, const NonOwningElement& rhs);
bool operator==(const NonOwningElement& lhs, const NonOwningElement& rhs);
bool operator==(const Element& lhs, const Element& rhs);
bool operator==(const Element& lhs, const NonOwningElement& rhs);
bool operator==(const NonOwningElement& lhs, const Element& rhs);




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
