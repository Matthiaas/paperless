//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_STORAGEMANAGER_H
#define PAPERLESS_STORAGEMANAGER_H

#include "MemoryTable.h"
#include "Element.h"

class StorageManager {
  void flushToDisk(const MemoryTable);
  void readFromDisk(Element);
};


#endif //PAPERLESS_STORAGEMANAGER_H
