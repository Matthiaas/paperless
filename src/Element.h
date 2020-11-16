//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_ELEMENT_H
#define PAPERLESS_ELEMENT_H

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <utility>

class OwningElement;

class Element {
 public:
  Element(char* v, size_t len) : value_(v), len_(len) {}
  Element() : value_(nullptr), len_(0){};
  // TODO: Shouldn't all of these just use defualt implementation? (roman)
  Element(const Element& other) = delete;
  Element(Element&& other) = default;
  Element& operator=(const Element&) = default;
  Element& operator=(Element&&) = default;

  char* Value() const {
    return value_;
  }

  size_t Length() const {
    return len_;
  }

 private:
  friend bool operator==(const Element& lhs, const Element& rhs);
  friend bool operator<(const Element& lhs, const Element& rhs);
  friend OwningElement;
  char* value_;
  size_t len_;

};

class OwningElement {
 public:
  OwningElement(char* v, size_t len) {
    el_.value_ = static_cast<char*>(std::malloc(len));
    el_.len_ = len;
    std::memcpy(el_.value_, v, len);
  }
  OwningElement(const OwningElement& other) = delete;
  OwningElement(OwningElement&& other) {
    el_ = std::move(other.el_);
    other.el_.value_ = nullptr;
    other.el_.len_ = 0;
  };

  ~OwningElement() {
    free(el_.value_);
  }

  static OwningElement createFromBuffWithoutCopy(char* v, size_t len) {
    OwningElement res;
    res.el_.value_ = v;
    res.el_.len_ = len;
    return res;
  }

  static OwningElement copyElementContent(const Element& e) {
    return OwningElement(e.Value(), e.Length());
  }

  static OwningElement copyElementContent(const OwningElement& e) {
    return OwningElement(e.Value(), e.Length());
  }

  char* Value() const {
    return el_.value_;
  }

  size_t Length() const {
    return el_.len_;
  }

 private:
  OwningElement() {};
  friend bool operator<(const OwningElement& lhs, const OwningElement& rhs);
  friend bool operator<(const Element& lhs, const OwningElement& rhs);
  friend bool operator<(const OwningElement& lhs, const Element& rhs);
  friend bool operator==(const OwningElement& lhs, const Element& rhs);
  friend bool operator==(const Element& lhs, const OwningElement& rhs);
  friend bool operator==(const OwningElement& lhs, const OwningElement& rhs);
  Element el_;
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
    other.len_ = 0;
  }

  OwningElement ToElement() const {
    return OwningElement(Value(), len_);
  }

  Tomblement Clone() const {
    Tomblement ret(Value(), Length());
    ret.value_[0] = value_[0];
    return ret;
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

bool operator<(const Element& lhs, const Element& rhs);
bool operator<(const OwningElement& lhs, const OwningElement& rhs);
bool operator<(const Element& lhs, const OwningElement& rhs);
bool operator<(const OwningElement& lhs, const Element& rhs);
bool operator==(const Element& lhs, const Element& rhs);
bool operator==(const OwningElement& lhs, const OwningElement& rhs);
bool operator==(const OwningElement& lhs, const Element& rhs);
bool operator==(const Element& lhs, const OwningElement& rhs);




/*
class OwningElementWithTombstone {
 public:
  OwningElementWithTombstone(Element OwningElement, bool tombstone)
      : OwningElement(element), tombstone(tombstone) {}

  OwningElementWithTombstone(char *val, size_t val_len, bool tombstone)
      : OwningElement(Element(val, val_len)), tombstone(tombstone) {}

  OwningElement OwningElement;
  bool tombstone;
};

 */

#endif  // PAPERLESS_ELEMENT_H
