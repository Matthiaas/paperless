//
// Created by matthias on 15.11.20.
//


#include "Element.h"

bool operator==(const NonOwningElement& lhs, const NonOwningElement& rhs)  {
  if (lhs.Length() != rhs.Length() ) return false;
  for (size_t i = 0; i < lhs.Length() ; i++) {
    if (lhs.Value()[i] != rhs.Value()[i])
      return false;
  }
  return true;
}



bool operator<(const NonOwningElement& lhs, const NonOwningElement& rhs) {
  if (lhs.len != rhs.len) return lhs.len < rhs.len;
  for (size_t i = 0; i < lhs.len; i++) {
    if (lhs.value[i] != rhs.value[i])
      return lhs.value[i] < rhs.value[i];
  }
  return false;
}

bool operator<(const Element& lhs, const Element& rhs) {
  return lhs.el_ < rhs.el_;
}

bool operator<(const NonOwningElement& lhs, const Element& rhs) {
  return lhs < rhs.el_;
}

bool operator<(const Element& lhs, const NonOwningElement& rhs)  {
  return lhs.el_ < rhs;
}

bool operator==(const NonOwningElement& lhs, const Element& rhs)  {
  return lhs == rhs.el_;
}

bool operator==(const Element& lhs, const NonOwningElement& rhs)  {
  return lhs.el_ == rhs;
}

bool operator==(const Element& lhs, const Element& rhs) {
  return lhs.el_ == rhs.el_;
}