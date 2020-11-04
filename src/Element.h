//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_ELEMENT_H
#define PAPERLESS_ELEMENT_H

#include <cstddef>

// We might want to rename that.
class Element {
public:
  Element(char* v, size_t len) : value(v), len(len) {}
private:
  const char* value;
  const size_t len;
};


#endif //PAPERLESS_ELEMENT_H
