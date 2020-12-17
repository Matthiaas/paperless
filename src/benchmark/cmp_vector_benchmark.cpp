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
  std::vector<std::tuple<size_t, bool, Element, Element>> pairs{};
  std::vector<std::tuple<size_t, bool, double>> measurements{};
  pairs.reserve(NUM_COMPARES);
  measurements.reserve(NUM_COMPARES / 2);

  std::cerr << "Setting up...\n";
  // Setup phase.
  for (size_t l = 1; l < 4096u; l += 16) {
    for (int i = 0; i < NUM_COMPARES; i++) {
      Element e = generateRandomElement(l);
      Element ce = Element::copyElementContent(e);
      bool equal = i % 2;
      if (!equal) {
        e.Value()[0]++;
      }
      pairs.emplace_back(l, equal, std::move(ce), std::move(e));
    }
  }

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(pairs.begin(), pairs.end(), g);
  std::cerr << "Start Measurements\n";


  for (auto &[len, equal, lhs, rhs] : pairs) {
    auto start = _wtime();
    _mm_lfence();
    _mm_mfence();
    bool res = lhs < rhs;
    _mm_lfence();
    _mm_mfence();
    measurements.emplace_back(len, equal & (!res), _wtime() - start);
  }

  for (auto &[len, equal, time] : measurements) {
    std::cout << len << ", " << equal << ", " << time << "\n";
  }
}