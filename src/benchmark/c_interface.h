#ifndef C_INTERFACE_H
#define C_INTERFACE_H

#include "KVWrapper.h"
#include "../PaperlessKV.h"

// This is a minimalistic interface used for creating Java bindings
// for YCSB benchmark.

// Don't forget that this file can wrap both paperless and papyrus
// based on setting PAPERLESS_BENCHMARK or PAPYRUS_BENCHMARK

void kv_init();
void kv_put(const char* key, size_t key_len, const char* value, size_t value_len);
void kv_get(const char* key, size_t key_len, char* value, size_t value_len);
void kv_finalize();

#endif // C_INTERFACE_H