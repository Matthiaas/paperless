//
// StorageManager.h - Interface for storing MemoryTables as SSTables.
//

#ifndef PAPERLESS_STORAGEMANAGER_H
#define PAPERLESS_STORAGEMANAGER_H

#include <utility>

#include "MemoryTable.h"
#include "Element.h"
#include "mtbl.h"

class StorageManager {
 private:
  std::string dir_path_;
  mtbl_writer_options *writer_options_;

public:
  explicit StorageManager(std::string dir_path) : dir_path_(std::move(dir_path)){
    writer_options_ = mtbl_writer_options_init();
  }


  ~StorageManager(){
    mtbl_writer_options_destroy(&writer_options_);
  };
  void flushToDisk(const MemoryTable&);
  QueryResult readFromDisk(Element);
};


#endif //PAPERLESS_STORAGEMANAGER_H
