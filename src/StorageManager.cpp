//
// StorageManager.cpp
//

#include "StorageManager.h"

#include <mtbl.h>

#include <iostream>

#include "BloomFilter.h"

// FIXME: Untested
void StorageManager::flushToDisk(const RBTreeMemoryTable &mem_table) {
  ++cur_file_index_;
  auto file_path = sstable_dir_path_ / std::to_string(cur_file_index_);

  auto mtbl_writer = mtbl_writer_init(file_path.c_str(), writer_options_);
  BloomFilter filter = BloomFilter{mem_table.size(), filter_fp_rate_};

  for (const auto&[key, value] : mem_table) {
    auto key_ptr = reinterpret_cast<const uint8_t *>(key.Value());
    auto value_ptr = reinterpret_cast<const uint8_t *>(value.GetBuffer());

    auto res = mtbl_writer_add(mtbl_writer, key_ptr, key.Length(), value_ptr, value.GetBufferLen());
    if (res == mtbl_res_failure) {
      // FIXME: handle error!
      std::cerr << "ERROR: " << __FILE__ << " " << __LINE__ << ": " << "mtbl_writer_add failed (FIXME: handle ERROR & replace this with Logging)\n";
      break;
    }
    filter.insert(key.GetView());
  }
  filters.emplace(cur_file_index_, std::move(filter));
  mtbl_writer_destroy(&mtbl_writer);
//  filter.DumptoFile(filter_dir_path_ + "/" + file_name);
}

QueryResult StorageManager::readFromDisk(const ElementView &key) {
  for (auto&[file_index, filter] : filters) {
    if (filter.contains(key)) {
      auto file_path = sstable_dir_path_ / std::to_string(file_index);

      auto reader = mtbl_reader_init(file_path.c_str(), reader_options_);
      auto source = mtbl_reader_source(reader);
      auto iter = mtbl_source_get(source, reinterpret_cast<const uint8_t *>(key.Value()), key.Length());

      const uint8_t *res_ptr = nullptr;
      size_t res_len;
      const uint8_t *key_ptr= nullptr;
      size_t key_len;
      auto res = mtbl_iter_next(iter, &key_ptr, &key_len, &res_ptr, &res_len);

      if (res == mtbl_res_success) {
        // I do not like the fact, that there are 3 different sets of calls to destroy mtbl stuff.
        // But I am trying to avoid an expensive copy, and the destroy methods have to be called after the copy.
        if (res_len == 0 || res_ptr[0]) {
          mtbl_iter_destroy(&iter);
          mtbl_reader_destroy(&reader);
          return QueryStatus::DELETED;
        }
        Element result {(char *) res_ptr + 1, res_len - 1};
        mtbl_iter_destroy(&iter);
        mtbl_reader_destroy(&reader);
        return result;
      }
      mtbl_iter_destroy(&iter);
      mtbl_reader_destroy(&reader);
    }
  }
  return QueryStatus::NOT_FOUND;
}
