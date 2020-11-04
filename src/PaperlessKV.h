//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_PAPERLESSKV_H
#define PAPERLESS_PAPERLESSKV_H


#include <functional>
#include <cstddef>
#include "Element.h"

class PaperlessKV {
public:
  explicit PaperlessKV(int id);
  PaperlessKV(int id, std::function<uint64_t(char*,size_t)>);

  PaperlessKV(const PaperlessKV&) = delete;
  PaperlessKV(PaperlessKV&&);

  void put(char* key, size_t key_len, char* value, size_t value_len);
  void get(char* key, size_t key_len);
  void deleteKey(char* key, size_t key_len);

  void put(Element key, Element value);
  void get(Element key);
  void deleteKey(Element key);
};


#endif //PAPERLESS_PAPERLESSKV_H
