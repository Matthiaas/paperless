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
  char* value;
  const size_t len;


  static Element copyElementContent(Element e) {
    Element res(static_cast<char *>(std::malloc(e.len)), e.len);
    std::memcpy(res.value, e.value, e.len);
    return res;
  }
};





#endif //PAPERLESS_ELEMENT_H
