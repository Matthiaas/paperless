//
//
//

#include "BloomFilter.h"

#include <array>
#include <cmath>
#include <fstream>

#include "smhasher/MurmurHash3.h"


std::array<uint64_t, 2> hash(const char *data, size_t len) {
  std::array<uint64_t,2> hashValues{};
  MurmurHash3_x64_128(data, len, 0, hashValues.data());
  return hashValues;
}

inline uint64_t nthHash(uint8_t n,
                       uint64_t hashA,
                       uint64_t hashB,
                       uint64_t filterSize) {
  return (hashA + n * hashB) % filterSize;
}

void BloomFilter::insert(const ElementView &e) {
  auto hashes = hash(e.Value(), e.Length());
  for (int n = 0; n < num_hashes_; n++) {
    uint64_t index = nthHash(n, hashes[0], hashes[1], bits_.size());
    bits_[index] = true;
  }
}

bool BloomFilter::contains(const ElementView &e) const {
  auto hashes = hash(e.Value(), e.Length());
  for (int n = 0; n < num_hashes_; n++) {
    if (!bits_[nthHash(n, hashes[0], hashes[1], bits_.size())]) {
      return false;
    }
  }
  return true;
}

void BloomFilter::DumptoFile(const std::string& path) {
  std::ofstream file {path, std::ios::out | std::ios::binary};
  file << num_hashes_;
  std::copy(bits_.begin(), bits_.end(), std::ostreambuf_iterator<char>(file));
}
