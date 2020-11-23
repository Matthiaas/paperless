//
// Created by matthias on 23.11.20.
//

#ifndef PAPERLESS_PAPERLESSKVFRIEND_H
#define PAPERLESS_PAPERLESSKVFRIEND_H

#include "../PaperlessKV.h"



class PaperLessKVFriend {
 public:
  PaperLessKVFriend(PaperlessKV* paper) : paper_(paper) {

  }

  LRUCache& getLocalCache() {
    return paper_->local_cache_;
  }

  LRUCache& getRemoteCache() {
    return paper_->remote_cache_;
  }

  PaperlessKV::RBTreeMemoryManager& getLocalManager() {
    return paper_->local_;
  }

  PaperlessKV::RBTreeMemoryManager& getRemoteManager() {
    return paper_->remote_;
  }

  StorageManager& getStorageManger() {
    return paper_->storage_manager_;
  }

  PaperlessKV* paper_;
};

#endif  // PAPERLESS_PAPERLESSKVFRIEND_H
