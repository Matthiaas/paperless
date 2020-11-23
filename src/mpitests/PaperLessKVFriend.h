//
// Created by matthias on 23.11.20.
//

#ifndef PAPERLESS_PAPERLESSKVFRIEND_H
#define PAPERLESS_PAPERLESSKVFRIEND_H

#include "../PaperlessKV.h"



class PaperLessKVFriend {
 public:
  PaperLessKVFriend(std::string id)
    : paper(id, MPI_COMM_WORLD, 7, PaperlessKV::RELAXED){
  }

  LRUCache& getLocalCache() {
    return paper.local_cache_;
  }

  LRUCache& getRemoteCache() {
    return paper.remote_cache_;
  }

  PaperlessKV::RBTreeMemoryManager& getLocalManager() {
    return paper.local_;
  }

  PaperlessKV::RBTreeMemoryManager& getRemoteManager() {
    return paper.remote_;
  }

  PaperlessKV paper;
};

#endif  // PAPERLESS_PAPERLESSKVFRIEND_H
