//
// Created by matthias on 15.11.20.
//


#include "Element.h"

bool operator==(const Element& lhs, const Element& rhs)  {
  if (lhs.Length() != rhs.Length() ) return false;
  for (size_t i = 0; i < lhs.Length() ; i++) {
    if (lhs.Value()[i] != rhs.Value()[i])
      return false;
  }
  return true;
}



bool operator<(const Element& lhs, const Element& rhs) {
  if (lhs.len_ != rhs.len_) return lhs.len_ < rhs.len_;
  for (size_t i = 0; i < lhs.len_; i++) {
    if (lhs.value_[i] != rhs.value_[i])
      return lhs.value_[i] < rhs.value_[i];
  }
  return false;
}

bool operator<(const OwningElement& lhs, const OwningElement& rhs) {
  return lhs.el_ < rhs.el_;
}

bool operator<(const Element& lhs, const OwningElement& rhs) {
  return lhs < rhs.el_;
}

bool operator<(const OwningElement& lhs, const Element& rhs)  {
  return lhs.el_ < rhs;
}

bool operator==(const Element& lhs, const OwningElement& rhs)  {
  return lhs == rhs.el_;
}

bool operator==(const OwningElement& lhs, const Element& rhs)  {
  return lhs.el_ == rhs;
}

bool operator==(const OwningElement& lhs, const OwningElement& rhs) {
  return lhs.el_ == rhs.el_;
}