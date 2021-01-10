#include "c_interface.h"

#include "KVWrapper.h"
#include "../PaperlessKV.h"
#include <iostream>

// This is a minimalistic interface used for creating Java bindings
// for YCSB benchmark.

// Don't forget that this file can wrap both paperless and papyrus
// based on setting PAPERLESS_BENCHMARK or PAPYRUS_BENCHMARK

void kv_init() {
  KV::Init(0, nullptr, "aux_db_name");
  KV::SetMode(PaperlessKV::Consistency_t::SEQUENTIAL, PaperlessKV::Mode_t::READANDWRITE);
}

void kv_put(const char* key, size_t key_len, const char* value, size_t value_len) {
//  for (int i = 0; i < key_len; ++i) std::cout << key[i];
//  std::cout << std::endl;
  KV::Put(key, key_len, value, value_len);
}

void kv_get(const char* key, size_t key_len, char* value, size_t value_len) {
//  value[0]=value[1]=value[2] = 0;
//  for (int i = 0; i < key_len; ++i) std::cout << key[i];
//  std::cout << std::endl;
  KV::Get(key, key_len, value, value_len);
//  std::cout << value[0] << value[1] << value[2] << std::endl;
}

void kv_finalize() {
  KV::Finalize();
  MPI_Finalize();
}
