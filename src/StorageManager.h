//
// StorageManager.h - Interface for storing MemoryTables as SSTables.
//

#ifndef PAPERLESS_STORAGEMANAGER_H
#define PAPERLESS_STORAGEMANAGER_H

#include <filesystem>
#include <utility>

#include "BloomFilter.h"
#include "Element.h"
#include "RBTreeMemoryTable.h"
#include "mtbl.h"

class StorageManager {

 public:
  explicit StorageManager(const std::filesystem::path &dir,
                          size_t cache_size = 100)
      : StorageManager(dir / "sstables",
                       dir / "filters",
                       cache_size) {}

  explicit StorageManager(const std::string& sstable_dir_path, const std::string& filter_dir_path)
  : sstable_dir_path_(sstable_dir_path), filter_dir_path_(filter_dir_path){
    writer_options_ = mtbl_writer_options_init();
    reader_options_ = mtbl_reader_options_init();
    cur_file_index_ = 0;

    std::filesystem::create_directory(filter_dir_path_);
    if (!std::filesystem::create_directory(sstable_dir_path_)) {
      for (const auto &dirEntry : std::filesystem::recursive_directory_iterator(sstable_dir_path_)) {
        uint64_t c = std::stoll(dirEntry.path().filename());
        cur_file_index_ = std::max(cur_file_index_, c);
      }
    }
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
  uint64_t cur_file_index_;
  std::filesystem::path sstable_dir_path_;
  std::filesystem::path filter_dir_path_;
  mtbl_writer_options *writer_options_;
  mtbl_reader_options *reader_options_;

  size_t cache_size_;

  std::map<uint64_t, BloomFilter, std::greater<>> filters;
  static constexpr double filter_fp_rate_ = 0.01;
};

  QueryResult ReadSSTable(const std::string &file_path, const ElementView &key);
};

#endif //PAPERLESS_STORAGEMANAGER_H
