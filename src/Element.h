//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_ELEMENT_H
#define PAPERLESS_ELEMENT_H

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <cassert>

#include "Common.h"

class Element;

class ElementView {
 public:
  ElementView(const char *v, size_t len)
      : value_(const_cast<char *>(v)), len_(len) {
#if not defined(NDEBUG) && defined(VECTORIZE)
    assert(0 == ((size_t) v % PAPERLESS::kStride)
               && R"(
                  ERROR: v needs to be 32byte aligned. Use PAPERLESS::malloc() or alignas(32).
                  Also make sure the length is a multiple of 32.
                  Example:
                  alignas(32) key[32] = "__key__";
 )");
#endif
  }
  ElementView() : value_(nullptr), len_(0) {};
  // TODO: Shouldn't all of these just use defualt implementation? (roman)
  ElementView(const ElementView &other) = default;
  ElementView(ElementView &&other) = default;
  ElementView &operator=(const ElementView &) = default;
  ElementView &operator=(ElementView &&) = default;

  [[nodiscard]] char *Value() const {
    return value_;
  }

  [[nodiscard]] size_t Length() const {
    return len_;
  }

 private:
  friend bool operator==(const ElementView &lhs, const ElementView &rhs);
  friend bool operator<(const ElementView &lhs, const ElementView &rhs);
  friend Element;
  char *value_;
  size_t len_;

};

class Element {
 public:
  Element(const char *v, const size_t len) {
    el_.value_ = static_cast<char *>(PAPERLESS::malloc(len));
    el_.len_ = len;
    std::memcpy(el_.value_, v, len);
  }

  Element(const size_t len) {
    el_.value_ = static_cast<char *>(PAPERLESS::malloc(len));
    el_.len_ = len;
  }

  Element(const Element &other) = delete;
  Element(Element &&other) noexcept {
    el_ = std::move(other.el_);
    other.el_.value_ = nullptr;
    other.el_.len_ = 0;
  };

  Element& operator=(Element &&other) noexcept {
    free(el_.value_);
    el_ = std::move(other.el_);
    other.el_.value_ = nullptr;
    other.el_.len_ = 0;
    return *this;
  };

  ~Element() {
    free(el_.value_);
  }

  [[nodiscard]]  static Element createFromBuffWithoutCopy(char *v, size_t len) {
    Element res;
    res.el_.value_ = v;
    res.el_.len_ = len;
    return res;
  }

  [[nodiscard]]  static Element copyElementContent(const ElementView &e) {
    return Element(e.Value(), e.Length());
  }

  [[nodiscard]]  static Element copyElementContent(const Element &e) {
    return Element(e.Value(), e.Length());
  }

  [[nodiscard]] const ElementView &GetView() const {
    return el_;
  }

  [[nodiscard]] char *Value() const {
    return el_.value_;
  }

  [[nodiscard]] size_t Length() const {
    return el_.len_;
  }

 private:
  Element() = default;;
  friend bool operator<(const Element &lhs, const Element &rhs);
  friend bool operator<(const ElementView &lhs, const Element &rhs);
  friend bool operator<(const Element &lhs, const ElementView &rhs);
  friend bool operator==(const Element &lhs, const ElementView &rhs);
  friend bool operator==(const ElementView &lhs, const Element &rhs);
  friend bool operator==(const Element &lhs, const Element &rhs);
  ElementView el_;
};

class Tomblement {
 public:
  Tomblement(const char *v, const size_t len) : len_(len) {
    value_ = static_cast<char *>(PAPERLESS::malloc(len_ + 1));
    value_[0] = 0;
    std::memcpy(value_ + 1, v, len);
  }

  Tomblement(const size_t len) : len_(len) {
    value_ = static_cast<char *>(PAPERLESS::malloc(len_ + 1));
  }

  ~Tomblement() {
    free(value_);
  }

  Tomblement(const Tomblement &other) = delete;
  Tomblement(Tomblement &&other) noexcept {
    value_ = other.value_;
    len_ = other.len_;
    other.value_ = nullptr;
    other.len_ = 0;
  }

  Tomblement &operator=(Tomblement &&other) noexcept {
    free(value_);
    value_ = other.value_;
    len_ = other.len_;
    other.value_ = nullptr;
    other.len_ = 0;
    return *this;
  }

  [[nodiscard]]  static Tomblement createFromBuffWithoutCopy(char *v, size_t len) {
    Tomblement res;
    res.value_ = v;
    res.len_ = len - 1;
    return res;
  }

  [[nodiscard]]  static Tomblement createFromBuffWithCopy(char *v, size_t len) {
    Tomblement res;
    res.value_ = static_cast<char *>(PAPERLESS::malloc(len));
    std::memcpy(res.value_, v, len);
    res.len_ = len - 1;
    return res;
  }

  [[nodiscard]] Element CopyToElement() const {
    return Element(Value(), len_);
  }

  [[nodiscard]] Tomblement Clone() const {
    Tomblement ret(Value(), Length());
    ret.value_[0] = value_[0];
    return ret;
  }

  static Tomblement getATombstone() {
    Tomblement res(nullptr, 0);
    res.value_[0] = true;
    return res;
  }

  [[nodiscard]] char *Value() const {
    return value_ + 1;
  }

  [[nodiscard]] size_t Length() const {
    return len_;
  }

  [[nodiscard]] bool Tombstone() const {
    return value_[0];
  }

  [[nodiscard]] char *GetBuffer() const {
    return value_;
  }

  [[nodiscard]] size_t GetBufferLen() const {
    return len_ + 1;
  }

  [[nodiscard]] ElementView GetView() const {
    return ElementView(value_ + 1, len_);
  }

  bool operator==(const Tomblement &rhs) const {
    if (len_ != rhs.len_) return false;
    for (size_t i = 0; i < len_ + 1; i++) {
      if (value_[i] != rhs.value_[i]) return false;
    }
    return true;
  }

  bool operator<(const Tomblement &other) const {
    if (len_ != other.len_) return len_ < other.len_;
    for (size_t i = 0; i < len_ + 1; i++) {
      if (value_[i] != other.value_[i])
        return value_[i] < other.value_[i];
    }
    return false;
  }

 private:
  Tomblement() {};
  char *value_;
  size_t len_;
};

bool operator<(const ElementView &lhs, const ElementView &rhs);
bool operator<(const Element &lhs, const Element &rhs);
bool operator<(const ElementView &lhs, const Element &rhs);
bool operator<(const Element &lhs, const ElementView &rhs);
bool operator==(const ElementView &lhs, const ElementView &rhs);
bool operator==(const Element &lhs, const Element &rhs);
bool operator==(const Element &lhs, const ElementView &rhs);
bool operator==(const ElementView &lhs, const Element &rhs);




/*
class OwningElementWithTombstone {
 public:
  OwningElementWithTombstone(ElementView Element, bool tombstone)
      : Element(element), tombstone(tombstone) {}

  OwningElementWithTombstone(char *val, size_t val_len, bool tombstone)
      : Element(ElementView(val, val_len)), tombstone(tombstone) {}

  Element Element;
  bool tombstone;
};

 */

#endif  // PAPERLESS_ELEMENT_H
