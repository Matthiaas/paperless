//
// StorageManager.h - Interface for storing MemoryTables as SSTables.
//

#ifndef PAPERLESS_STORAGEMANAGER_H
#define PAPERLESS_STORAGEMANAGER_H


#include <filesystem>
#include <fcntl.h>
#include <queue>
#include <utility>

#include "BloomFilter.h"
#include "Element.h"
#include "RBTreeMemoryTable.h"
#include "mtbl.h"

class StorageManager {

 public:
  explicit StorageManager(const std::filesystem::path &dir,
                          size_t cache_size = 500)
      : StorageManager(dir / "sstables",
                       dir / "filters",
                       cache_size) {}

  explicit StorageManager(const std::string &sstable_dir_path,
                          const std::string &filter_dir_path,
                          size_t cache_size)
      : sstable_dir_path_(sstable_dir_path),
        filter_dir_path_(filter_dir_path),
        cache_size_(cache_size) {
    writer_options_ = mtbl_writer_options_init();
    reader_options_ = mtbl_reader_options_init();

    std::filesystem::create_directories(filter_dir_path_);
    std::filesystem::create_directories(sstable_dir_path_);

    total_num_filters_ = HighestFileIndexInDir(sstable_dir_path_);

    // Restore Cache.
//    uint64_t first = (total_num_filters_ > cache_size_) ? (total_num_filters_ - cache_size_) : 1;
//    for (uint64_t fi = first; fi <= total_num_filters_; fi++) {
//      auto filter = BloomFilter::CreateFromFile(filter_dir_path_ / std::to_string(fi));
//      int fd = open((sstable_dir_path_ / std::to_string(fi)).c_str(), O_RDONLY);
//      filter_cache.emplace_front(fi, filter, fd);
//    }

  }

  ~StorageManager() {
    mtbl_writer_options_destroy(&writer_options_);
    mtbl_reader_options_destroy(&reader_options_);
  };

  void flushToDisk(const RBTreeMemoryTable &);
  QueryResult readFromDisk(const ElementView &key);
  std::pair<QueryStatus, size_t> readFromDisk(const ElementView &key,
                                              const ElementView &buff);

  void deleteDisk() {
    std::filesystem::remove_all(sstable_dir_path_);
    std::filesystem::remove_all(filter_dir_path_);
  }

 private:
  // File indices start at 1.
  uint64_t total_num_filters_;
  std::filesystem::path sstable_dir_path_;
  std::filesystem::path filter_dir_path_;
  mtbl_writer_options *writer_options_;
  mtbl_reader_options *reader_options_;

  size_t cache_size_;

  // file_index, filter, fd
  std::deque<std::tuple<uint64_t, BloomFilter, int>> filter_cache;
  static constexpr double filter_fp_rate_ = 0.01;

  QueryResult ReadSSTable(const std::string &file_path, const ElementView &key);
  QueryResult ReadSSTable(int fd, const ElementView &key);

  static uint64_t HighestFileIndexInDir(const std::filesystem::path &dir_path) {
    uint64_t file_index = 0;
    for (const auto &dirEntry : std::filesystem::recursive_directory_iterator(dir_path)) {
      uint64_t c = std::stoll(dirEntry.path().filename());
      file_index = std::max(file_index, c);
    }
    return file_index;
  }
  inline uint64_t GetFirstUncachedFilterIndex() { return total_num_filters_ - filter_cache.size(); }
  inline bool AllFiltersCached() { return total_num_filters_ <= filter_cache.size(); }
};

#endif //PAPERLESS_STORAGEMANAGER_H
