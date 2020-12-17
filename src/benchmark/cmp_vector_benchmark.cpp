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

int main() {
#ifdef VECTORIZE
  std::string bench_name = "vector";
#else
  std::string bench_name = "memcmp";
#endif

  std::vector<std::tuple<size_t, bool, ElementView, ElementView>> pairs{};
  std::vector<std::tuple<size_t, bool, uint64_t>> measurements{};
  pairs.reserve(NUM_COMPARES);

  std::cerr << "Setting up...\n";
  // Setup phase.
  for (size_t l = 1; l < 4096u; l += 1024) {
    Element e = generateRandomElement(l);
    Element ce = Element::copyElementContent(e);
    Element de = Element::copyElementContent(e);
    de.Value()[0]++;

    for (int i = 0; i < NUM_COMPARES; i++) {
      bool equal = i % 2;
      if (equal) {
        pairs.emplace_back(l, equal, e.GetView(), ce.GetView());
      } else {
        pairs.emplace_back(l, equal, e.GetView(), de.GetView());
      }
    }
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(pairs.begin(), pairs.end(), g);
  std::cerr << "Start Measurements\n";


  for (auto &[len, equal, lhs, rhs] : pairs) {
    auto start = _rdtsc();
    _mm_lfence();
    _mm_mfence();
    bool res = lhs < rhs;
    _mm_lfence();
    _mm_mfence();
    measurements.emplace_back(len, equal && (!res), _rdtsc() - start);
  }


  std::cout << "bench_name" << ", "<< "key_len" << ", " << "equal" << ", " << "cycles" << "\n";
  for (auto &[key_len, equal, cycles] : measurements) {
    std::cout << bench_name << ", " << key_len << ", " << equal << ", " << cycles << "\n";
  }
}