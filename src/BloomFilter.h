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
  BloomFilter(const BloomFilter &other) = default;
  BloomFilter(BloomFilter &&other)  noexcept = default;

  BloomFilter(size_t num_elements, double fp_rate) {
    double m = num_elements * std::log(fp_rate) / std::log(0.6185);
    num_hashes_ = std::ceil(std::pow(0.6185, m / num_elements));
    bits_ = std::vector<bool>(m);
  }



  void insert(const ElementView &e);
  [[nodiscard]] bool contains(const ElementView &e) const;


  [[nodiscard]] uint8_t GetNumHashes() const {
    return num_hashes_;
  }

  [[nodiscard]] size_t GetSize() const {
    return bits_.size();
  }

  void DumpToFile(const std::string& path);

  static BloomFilter CreateFromFile(const std::string& path);


 private:
  uint8_t num_hashes_{};
  std::vector<bool> bits_;

  BloomFilter() = default;

};

class ReadOnlyBloomFilterOnDisk {
 public:
  explicit ReadOnlyBloomFilterOnDisk(const std::string& path)
    : file_(path, std::ifstream::binary) {
    file_.read(reinterpret_cast<char *>(&num_hashes_), sizeof num_hashes_);
    file_.read(reinterpret_cast<char *>(&size_), sizeof size_);
  }

  [[nodiscard]] bool contains(const ElementView &e) const;

  [[nodiscard]] uint8_t GetNumHashes() const {
    return num_hashes_;
  }

  [[nodiscard]] size_t GetSize() const {
    return size_;
  }

 private:
  uint8_t num_hashes_{};
  size_t size_{};
  mutable std::ifstream file_;

  static constexpr uint64_t kbaseFileOffset_ = sizeof(num_hashes_) + sizeof(size_);
};

#endif //PAPERLESS_SRC_BLOOMFILTER_H_
