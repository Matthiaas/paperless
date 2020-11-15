//
// StorageManager.cpp
//

#include "StorageManager.h"
#include <mtbl.h>
#include <iostream>

// FIXME: Untested
void StorageManager::flushToDisk(const RBTreeMemoryTable &mem_table) {
  std::string file_name {"asdf"};
  std::string file_path = dir_path_ + "/" + file_name;

  auto mtbl_writer = mtbl_writer_init(file_path.c_str(), writer_options_);

    for (const auto& pair : mem_table) {
    // TODO: Handle tombstone bits!
    auto res = mtbl_writer_add(mtbl_writer,
                    reinterpret_cast<const uint8_t *>(pair.first.value), pair.first.len,
                    reinterpret_cast<const uint8_t *>(pair.second.GetBuffer()), pair.second.GetBufferLen());
    if (res == mtbl_res_failure) {
      // FIXME: handle error!
      std::cerr << "ERROR: " << __FILE__ << " " << __LINE__ << ": " << "mtbl_writer_add failed (FIXME: handle ERROR & replace this with Logging)\n";
      break;
    }
  }

  // TODO: bloom filter

  mtbl_writer_destroy(&mtbl_writer);
}

QueryResult StorageManager::readFromDisk(Element key) {
  return QueryStatus::NOT_FOUND;
}
