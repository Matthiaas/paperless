#include "c_interface.h"

#include "KVWrapper.h"
#include "../PaperlessKV.h"

// This is a minimalistic interface used for creating Java bindings
// for YCSB benchmark.

// Don't forget that this file can wrap both paperless and papyrus
// based on setting PAPERLESS_BENCHMARK or PAPYRUS_BENCHMARK

void kv_init() {
  KV::Init(0, nullptr, "aux_db_name");
  KV::SetMode(PaperlessKV::Consistency_t::SEQUENTIAL, PaperlessKV::Mode_t::READANDWRITE);
}

void kv_put(const char* key, size_t key_len, const char* value, size_t value_len) {
  KV::Put(key, key_len, value, value_len);
}

void kv_get(const char* key, size_t key_len, char* value, size_t value_len) {
  KV::Get(key, key_len, value, value_len);
}

void kv_finalize() {
  KV::Finalize();
}