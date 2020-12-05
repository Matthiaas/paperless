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

  LRUTreeCache& getLocalCache() {
    return paper_->local_cache_;
  }

  LRUTreeCache& getRemoteCache() {
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

  inline void WriteIntToBuff(char* ptr, unsigned int x) {
    paper_->WriteIntToBuff(ptr,x);
  }

  inline unsigned int ReadIntFromBuff(char* ptr) {
    return paper_->ReadIntFromBuff(ptr);
  }

  PaperlessKV* paper_;
};

#endif  // PAPERLESS_PAPERLESSKVFRIEND_H
