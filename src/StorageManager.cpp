//
// StorageManager.cpp
//

#include "StorageManager.h"

#include <fcntl.h>
#include <mtbl.h>
#include <unistd.h>

#include <iostream>

#include "BloomFilter.h"

void StorageManager::flushToDisk(const RBTreeMemoryTable &mem_table) {
  ++total_num_filters_;
  auto sstable_path = sstable_dir_path_ / std::to_string(total_num_filters_);
  auto filter_path = filter_dir_path_ / std::to_string(total_num_filters_);

  auto mtbl_writer = mtbl_writer_init(sstable_path.c_str(), writer_options_);
  BloomFilter filter = BloomFilter{mem_table.ByteSize(), filter_fp_rate_};

  for (const auto &[key, value] : mem_table) {
    auto key_ptr = reinterpret_cast<const uint8_t *>(key.Value());
    auto value_ptr = reinterpret_cast<const uint8_t *>(value.GetBuffer());

    auto res = mtbl_writer_add(mtbl_writer, key_ptr, key.Length(), value_ptr,
                               value.GetBufferLen());
    if (res == mtbl_res_failure) {
      // FIXME: handle error!
      std::cerr << "ERROR: " << __FILE__ << " " << __LINE__ << ": "
                << "mtbl_writer_add failed (FIXME: handle ERROR & replace this "
                   "with Logging)\n";
      break;
    }
    filter.insert(key.GetView());
  }
  mtbl_writer_destroy(&mtbl_writer);

  // Pop last filter and write to disk.
  if (filter_cache.size() > cache_size_) {
    auto [file_index, filter, fd] = filter_cache.back();
    close(fd);
    filter_cache.pop_back();
  }

  filter.DumpToFile(filter_path);
  int fd = open(sstable_path.c_str(), O_RDONLY);
  filter_cache.emplace_front(total_num_filters_, std::move(filter), fd);
}

QueryResult StorageManager::readFromDisk(const ElementView &key) {
  for (auto &[file_index, filter, fd] : filter_cache) {
    if (filter.contains(key)) {
      auto file_path = sstable_dir_path_ / std::to_string(file_index);
      if (auto result = ReadSSTable(fd, key);
          result != QueryStatus::NOT_FOUND) {
        return result;
      }
    }
  }

  // Check BloomFilters on Disk.
  if (AllFiltersCached()) {
    return QueryStatus::NOT_FOUND;
  }

  for (auto file_index = GetFirstUncachedFilterIndex(); file_index > 0; file_index--) {
    auto filter_dir_entry = filter_dir_path_ / std::to_string(file_index);
    auto filter = ReadOnlyBloomFilterOnDisk(filter_dir_path_);
    if (filter.contains(key)) {
      auto file_path = sstable_dir_path_ / filter_dir_entry.stem();
      if (auto result = ReadSSTable(file_path, key);
          result != QueryStatus::NOT_FOUND) {
        return result;
      }
    }
  }
  return QueryStatus::NOT_FOUND;
}

/*
 * Read from disk into user provided buffer.
 * WARNING: This adds an additional copy.
 */
std::pair<QueryStatus, size_t> StorageManager::readFromDisk(
    const ElementView &key, const ElementView &buff) {
  auto qr = readFromDisk(key);
  if (qr == QueryStatus::NOT_FOUND || qr == QueryStatus::DELETED) {
    return {qr.Status(), 0};
  } else {
    if (qr->Length() > buff.Length()) {
      return {QueryStatus::BUFFER_TOO_SMALL, qr->Length()};
    } else {
      std::memcpy(buff.Value(), qr->Value(), qr->Length());
      return {QueryStatus::FOUND, qr->Length()};
    }
  }
}

QueryResult StorageManager::ReadSSTable(const std::string &file_path,
                                        const ElementView &key) {
  int fd = open(file_path.c_str(), O_RDONLY);
  auto res = ReadSSTable(fd, key);
  close(fd);
  return res;
}

QueryResult StorageManager::ReadSSTable(int fd, const ElementView &key) {
  auto reader = mtbl_reader_init_fd(fd, reader_options_);
  auto source = mtbl_reader_source(reader);
  auto iter = mtbl_source_get(
      source, reinterpret_cast<const uint8_t *>(key.Value()), key.Length());

  const uint8_t *res_ptr = nullptr;
  size_t res_len;
  const uint8_t *key_ptr = nullptr;
  size_t key_len;
  auto res = mtbl_iter_next(iter, &key_ptr, &key_len, &res_ptr, &res_len);

  if (res == mtbl_res_success) {
    if (res_len == 0 || res_ptr[0]) {
      mtbl_iter_destroy(&iter);
      mtbl_reader_destroy(&reader);
      return QueryStatus::DELETED;
    }
    Element result{(char *)res_ptr + 1, res_len - 1};
    mtbl_iter_destroy(&iter);
    mtbl_reader_destroy(&reader);
    return result;
  }
  mtbl_iter_destroy(&iter);
  mtbl_reader_destroy(&reader);

  return QueryStatus::NOT_FOUND;
}
