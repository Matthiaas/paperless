#include <x86intrin.h>
#include <iostream>
#include <vector>

#include "../Element.h"

#include "timer.h"
#include "helper.h"

#define NUM_COMPARES 100000

Element generateRandomElement() {
  size_t length = rand() % 4096;
  Element e{length};
  rand_str(length, e.Value());
  return e;
}

int main() {
  std::vector<std::pair<Element, Element>> pairs{};
  std::vector<std::pair<uint64_t, bool>> measurements{};
  pairs.reserve(NUM_COMPARES);
  measurements.reserve(NUM_COMPARES / 2);

  std::cerr << "Setting up...\n";
  // Setup phase.
  for (int i = 0; i < NUM_COMPARES; i++) {
    if (i % 2) {
      pairs.emplace_back(generateRandomElement(), generateRandomElement());
    } else {
      Element e = generateRandomElement();
      Element ce = Element::copyElementContent(e);
      pairs.emplace_back(std::move(ce), std::move(e));
    }
  }

  std::cerr << "Start Measurements\n";

  auto start = _rdtsc();
  for (auto &[a, b] : pairs) {
    _mm_lfence();
    _mm_mfence();
    bool res = a < b;
    _mm_lfence();
    _mm_mfence();
    measurements.emplace_back(_rdtsc() - start, res);
  }

  std::cout << "Total wall time: " << _wtime() - start;

  for (auto &[time, r] : measurements) {
    std::cout << time << "\n";
  }
  std::cout << "------\n";
  std::sort(measurements.begin(), measurements.end());
  std::cout << "median: " << measurements[measurements.size()/2].first << "\n";
}