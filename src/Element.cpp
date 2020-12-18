//
// Created by matthias on 15.11.20.
//

#ifdef VECTORIZE
#include <immintrin.h>
#endif

#include <algorithm>
#include <cstdint>

#include "Element.h"

bool operator==(const ElementView &lhs, const ElementView &rhs) {
  if (lhs.Length() != rhs.Length()) return false;
  for (size_t i = 0; i < lhs.Length(); i++) {
    if (lhs.Value()[i] != rhs.Value()[i]) return false;
  }
  return true;
}

static inline __attribute__((always_inline)) uint64_t first_zero(
    uint64_t input) {
  uint64_t result;
  asm volatile(
      "not %1\n"
      "bsr %1, %0\n"
      : "=r"(result)
      : "r"(input));
  return result;
}

bool operator<(const ElementView &lhs, const ElementView &rhs) {
#ifdef VECTORIZE
  size_t len = PAPERLESS::roundToStrideLength(std::min(lhs.len_, rhs.len_));
  for (size_t index = 0; index < len; index += PAPERLESS::kStride) {
    const __m256i pa_l1 = _mm256_load_si256(reinterpret_cast<const __m256i *>(lhs.Value() + index));
    const __m256i pa_r1 = _mm256_load_si256(reinterpret_cast<const __m256i *>(rhs.Value() + index));
    const __m256i pa_l2 = _mm256_load_si256(reinterpret_cast<const __m256i *>(lhs.Value() + index + 32));
    const __m256i pa_r2 = _mm256_load_si256(reinterpret_cast<const __m256i *>(rhs.Value() + index + 32));

    const __m256i vcmp1 = _mm256_cmpeq_epi8(pa_l1, pa_r1);
    const int mask1 = _mm256_movemask_epi8(vcmp1);
    if (mask1 != -1) {
      unsigned first_diff = __builtin_ffs(~mask1) - 1;
      return lhs.Value()[first_diff] < rhs.Value()[first_diff];
    }

    const __m256i vcmp2 = _mm256_cmpeq_epi8(pa_l2, pa_r2);
    const int mask2 = _mm256_movemask_epi8(vcmp2);
    if (mask2 != -1) {
      unsigned first_diff = __builtin_ffs(~mask2) - 1 + 32;
      return lhs.Value()[first_diff] < rhs.Value()[first_diff];
    }
  }
  return lhs.len_ < rhs.len_;
#else
  int res = std::memcmp(lhs.Value(), rhs.Value(),
                        std::min(lhs.Length(), rhs.Length()));
  if (res == 0) {
    return lhs.Length() < rhs.Length();
  }
  return res < 0;
#endif
}

bool operator<(const Element &lhs, const Element &rhs) {
  return lhs.el_ < rhs.el_;
}

bool operator<(const ElementView &lhs, const Element &rhs) {
  return lhs < rhs.el_;
}

bool operator<(const Element &lhs, const ElementView &rhs) {
  return lhs.el_ < rhs;
}

bool operator==(const ElementView &lhs, const Element &rhs) {
  return lhs == rhs.el_;
}

bool operator==(const Element &lhs, const ElementView &rhs) {
  return lhs.el_ == rhs;
}

bool operator==(const Element &lhs, const Element &rhs) {
  return lhs.el_ == rhs.el_;
}