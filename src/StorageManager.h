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
  explicit StorageManager(const std::string& dir_path)
    : StorageManager(dir_path, dir_path + "_filter"){}

  explicit StorageManager(const std::string& sstable_dir_path, const std::string& filter_dir_path)
  : sstable_dir_path_(sstable_dir_path), filter_dir_path_(filter_dir_path){
    writer_options_ = mtbl_writer_options_init();
    reader_options_ = mtbl_reader_options_init();
    cur_file_index_ = 0;
    for (const auto &dirEntry : std::filesystem::recursive_directory_iterator(sstable_dir_path_)) {
      uint64_t c = std::stoll(dirEntry.path().filename());
      cur_file_index_ = std::max(cur_file_index_, c);
    }
  }

  ~StorageManager(){
    mtbl_writer_options_destroy(&writer_options_);
    mtbl_reader_options_destroy(&reader_options_);
  };

  void flushToDisk(const RBTreeMemoryTable&);
  QueryResult readFromDisk(const ElementView &key);

 private:
  uint64_t cur_file_index_;
  std::filesystem::path sstable_dir_path_;
  std::filesystem::path filter_dir_path_;
  mtbl_writer_options *writer_options_;
  mtbl_reader_options *reader_options_;

  std::map<uint64_t, BloomFilter, std::greater<>> filters;
  static constexpr double filter_fp_rate_ = 0.01;
};


#endif //PAPERLESS_STORAGEMANAGER_H
