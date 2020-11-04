//
// Created by matthias on 04.11.20.
//

#include "PaperlessKV.h"

void PaperlessKV::put(char *key, size_t key_len, char *value, size_t value_len) {
  put(Element(key, key_len), Element(value, value_len));
}

void PaperlessKV::get(char *key, size_t key_len) {
  get(Element(key, key_len));
}

void PaperlessKV::deleteKey(char *key, size_t key_len) {
  deleteKey(Element(key, key_len));
}
