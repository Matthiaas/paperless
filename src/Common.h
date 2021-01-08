//
// Created by fooris on 13.12.20.
//

#ifndef PAPERLESS_SRC_COMMON_H_
#define PAPERLESS_SRC_COMMON_H_

#include <cstdlib>
#include <cstring>

namespace PAPERLESS {
constexpr size_t kStride = 64;

static inline __attribute__((always_inline))
constexpr size_t padLength(size_t len) {
#ifdef VECTORIZE
  if (len % kStride == 0) {
    return len;
  }
  return (len & ~(kStride-1)) + kStride;
#else
  return len;
#endif
}

static inline __attribute__((always_inline))
void *malloc(size_t length) {
#ifdef VECTORIZE
  size_t len = padLength(length);
  void *p = std::aligned_alloc(kStride, len);
  std::memset(p, 0, len);
  return p;
#else
  return std::malloc(length);
#endif
}
}

#endif //PAPERLESS_SRC_COMMON_H_
