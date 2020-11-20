#include <iostream>
#include "src/StorageManager.h"

int main() {
  StorageManager storage_manager{"/tmp/paperless"};
  RBTreeMemoryTable table1{};
  RBTreeMemoryTable table2{};

  char key[] = "key thingy";
  char value[] = "value thingy";

  table1.put({key, sizeof key}, {value, sizeof value});
  storage_manager.flushToDisk(table1);

//  table2.put({key, sizeof key}, Tomblement::getATombstone());
//  storage_manager.flushToDisk(table2);

  auto res = storage_manager.readFromDisk({key, sizeof key});

  if (res.hasValue()) {
    std::cout << res.Value().Value() << std::endl;
  } else {
    std::cout << res.Status() << std::endl;
  }
  return 0;
}