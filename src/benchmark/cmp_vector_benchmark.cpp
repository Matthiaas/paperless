#include <x86intrin.h>

#include <algorithm>
#include <iostream>
#include <random>
#include <vector>

#include "../Element.h"
#include "helper.h"
#include "timer.h"

#define NUM_COMPARES 100000

Element generateRandomElement(size_t length) {
  Element e{length};
  rand_str(length, e.Value());
  return e;
}

//#pragma GCC push_options
////#pragma GCC optimize ("O0")
// std::vector<std::tuple<size_t, bool, uint64_t>> measure(
//    const std::vector<std::tuple<size_t, bool, ElementView, ElementView>>
//        &pairs) {
//  std::vector<std::tuple<size_t, bool, uint64_t>> measurements{};
//  bool thingy = true;
//  for (auto &[len, equal, lhs, rhs] : pairs) {
//    uint64_t start = __builtin_ia32_rdtsc();
//    _mm_lfence();
//    _mm_mfence();
//    bool res = lhs < rhs;
//    _mm_lfence();
//    _mm_mfence();
//    measurements.emplace_back(len, equal, __builtin_ia32_rdtsc() - start);
//    thingy = thingy != res;
//  }
//  return measurements;
//}
//#pragma GCC pop_options

int main() {
#ifdef VECTORIZE
  std::string bench_name = "vector";
#else
  std::string bench_name = "memcmp";
#endif

  std::vector<Element> elements{};
  std::vector<std::tuple<size_t, bool, ElementView, ElementView>> pairs{};
  pairs.reserve(NUM_COMPARES);

  std::cerr << "Setting up...\n";
  // Setup phase.
  //  for (size_t l = 16; l < 1 * 128; l += 16 ) {
  for (size_t l = 16; l < 4 * 4096; l += 2048) {
    Element e = generateRandomElement(l);
    Element ce = Element::copyElementContent(e);

    for (int i = 0; i < NUM_COMPARES; i++) {
      pairs.emplace_back(l, true, e.GetView(), ce.GetView());
    }
    elements.emplace_back(std::move(e));
    elements.emplace_back(std::move(ce));
  }

  //  auto&[size, equal, view1, view2] = pairs.back();
  //  std::cout << view1.Value() << "\n" << view2.Value() << "\n";

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(pairs.begin(), pairs.end(), g);
  std::cerr << "Start Measurements\n";

  //  auto measurements = measure(pairs);
  std::vector<std::tuple<size_t, bool, uint64_t>> measurements{};
  bool thingy = true;
  for (auto &[len, equal, lhs, rhs] : pairs) {
    uint64_t start = __builtin_ia32_rdtsc();
    _mm_lfence();
    _mm_mfence();
    bool res = lhs < rhs;
    _mm_lfence();
    _mm_mfence();
    measurements.emplace_back(len, equal, __builtin_ia32_rdtsc() - start);
    thingy = thingy != res;
  }

  std::cout << "bench_name,key_len,equal,cycles\n";
  for (auto &[key_len, equal, cycles] : measurements) {
    std::cout << bench_name << "," << key_len << "," << equal << "," << cycles
              << "\n";
  }
}
