//
// https://github.com/Matthiaas/RankAndSelectBasedQuotientFilter/
//

#ifndef PAPERLESS_SRC_BLOOMFILTER_H_
#define PAPERLESS_SRC_BLOOMFILTER_H_


#include <cstdint>
#include <vector>
#include <cmath>
#include <string>
#include <fstream>

#include "Element.h"

// FIXME: Add tests.
class BloomFilter {
 public:

  BloomFilter(size_t num_elements, double fp_rate) {
    double m = num_elements * std::log(fp_rate) / std::log(0.6185);
    num_hashes_ = std::pow(0.6185, m / num_elements);
    bits_ = std::vector<bool>(m);
  }

// TODO: Read Bloomfilter from File.
//  BloomFilter(const std::string& path) {
//    std::ifstream file{path, std::ios::in | std::ios::binary};
//    file >> num_hashes_;
//    bits_ = std::vector<bool>{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
//  }

  BloomFilter(const BloomFilter &other) = default;
  BloomFilter(BloomFilter &&other) = default;

  void insert(const Element &e);
  [[nodiscard]] bool contains(const Element &e) const;


  [[nodiscard]] uint8_t GetNumHashes() const {
    return num_hashes_;
  }

  [[nodiscard]] size_t GetSize() const {
    return bits_.size();
  }

  void DumptoFile(const std::string& path);


 private:
  uint8_t num_hashes_{};
  std::vector<bool> bits_;

};

#endif //PAPERLESS_SRC_BLOOMFILTER_H_
