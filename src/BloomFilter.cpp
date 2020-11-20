//
//
//

#include "BloomFilter.h"

#include <array>
#include <cmath>
#include <iostream>

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

void BloomFilter::DumpToFile(const std::string& path) {
  std::ofstream file {path, std::ios::out | std::ios::binary};
  uint8_t nh = num_hashes_;
  size_t size = bits_.size();
  file.write(reinterpret_cast<const char *>(&nh), sizeof nh);
  file.write(reinterpret_cast<const char *>(&size), sizeof size);
  std::copy(bits_.begin(), bits_.end(), std::ostreambuf_iterator<char>(file));
}

BloomFilter BloomFilter::CreateFromFile(const std::string &path) {
  BloomFilter ret;
  std::ifstream file{path, std::ios::in | std::ios::binary};

  size_t size;
  file.read(reinterpret_cast<char *>(&ret.num_hashes_), sizeof ret.num_hashes_);
  file.read(reinterpret_cast<char *>(&size), sizeof size);
  ret.bits_ = std::vector<bool>{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};

  if (ret.bits_.size() != size) {
    std::cerr << "ERROR: failed to read file (TODO please handle)\n";
  }
  return ret;
}

// ReadOnlyBloomFilterOnDisk
bool ReadOnlyBloomFilterOnDisk::contains(const ElementView &e) const {
  auto hashes = hash(e.Value(), e.Length());
  for (int n = 0; n < num_hashes_; n++) {
    uint64_t index = nthHash(n, hashes[0], hashes[1], size_);

    file_.seekg(kbaseFileOffset_ + index, std::ios::beg);
    if (file_.get() == 0) {
      return false;
    }
  }
  return true;
}
