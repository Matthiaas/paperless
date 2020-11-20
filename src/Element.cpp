//
// Created by matthias on 15.11.20.
//


#include <algorithm>
#include "Element.h"

bool operator==(const ElementView& lhs, const ElementView& rhs)  {
  if (lhs.Length() != rhs.Length() ) return false;
  for (size_t i = 0; i < lhs.Length() ; i++) {
    if (lhs.Value()[i] != rhs.Value()[i])
      return false;
  }
  return true;
}



// TODO: We have to sort lexicographically for mtbl library.
// Floris is checking if we can change the sorter of mtbl, so we can optimize this.
bool operator<(const ElementView& lhs, const ElementView& rhs) {
  int res = std::memcmp(lhs.Value(), rhs.Value(), std::min(lhs.Length(), rhs.Length()));
  if (res == 0) {
    return lhs.Length() < rhs.Length();
  }
  return res < 0;
// Old Version:
//  if (lhs.len_ != rhs.len_) return lhs.len_ < rhs.len_;
//  for (size_t i = 0; i < lhs.len_; i++) {
//    if (lhs.value_[i] != rhs.value_[i])
//      return lhs.value_[i] < rhs.value_[i];
//  }
//  return false;
}

bool operator<(const Element& lhs, const Element& rhs) {
  return lhs.el_ < rhs.el_;
}

bool operator<(const ElementView& lhs, const Element& rhs) {
  return lhs < rhs.el_;
}

bool operator<(const Element& lhs, const ElementView& rhs)  {
  return lhs.el_ < rhs;
}

bool operator==(const ElementView& lhs, const Element& rhs)  {
  return lhs == rhs.el_;
}

bool operator==(const Element& lhs, const ElementView& rhs)  {
  return lhs.el_ == rhs;
}

bool operator==(const Element& lhs, const Element& rhs) {
  return lhs.el_ == rhs.el_;
}