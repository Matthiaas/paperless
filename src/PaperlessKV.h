//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_PAPERLESSKV_H
#define PAPERLESS_PAPERLESSKV_H

#include <mpi.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <string>
#include <thread>

#include "Element.h"
#include "ListQueue.h"
#include "MemoryTableManager.h"
#include "RBTreeMemoryTable.h"
#include "StorageManager.h"
#include "Types.h"

class PaperlessKV {
 public:
  enum Consistency { SEQUENTIAL, RELAXED };

  explicit PaperlessKV(std::string id, MPI_Comm comm, Consistency);
  PaperlessKV(std::string id, MPI_Comm comm, HashFunction, Consistency);

  PaperlessKV(const PaperlessKV&) = delete;
  PaperlessKV(PaperlessKV&&);

  ~PaperlessKV();

  void put(const char* key, size_t key_len,const char* value, size_t value_len);
  QueryResult get(const char* key, size_t key_len);
  void deleteKey(char* key, size_t key_len);



 private:

  using MemTable = RBTreeMemoryTable;
  using MemQueue = ListQueue<MemTable>;
  using RBTreeMemoryManager =
      MemoryTableManager<MemTable, MemQueue>;

  void put(const Element& key, Tomblement value);
  QueryResult get(const Element& key);
  void deleteKey(const Element& key);

  void compact();
  void dispatch();
  void respond_get();
  void respond_put();



  void sendKey(const Element& key, int target, int tag);
  void sendValue(const QueryResult& key, int target, int tag);
  int receiveKey(const Element& buff, int source, int tag, MPI_Status status);
  int receiveValue(const Element& buff, int source, int tag, MPI_Status status);
  OwningElement receiveKey(int source, int tag, MPI_Status* status);
  QueryResult receiveValue(int source, int tag, MPI_Status* status);


  QueryResult localGet(const Element& key, Hash hash);
  QueryResult remoteGetRelaxed(const Element& key, Hash hash);
  QueryResult remoteGetValue(const Element& key, Hash hash);

  std::string id_;

  Consistency consistency_;
  HashFunction hash_function_;

  RBTreeMemoryManager local_;
  RBTreeMemoryManager remote_;

  RBTreeMemoryTable local_cache_;
  RBTreeMemoryTable remote_cache_;

  std::thread compactor_;
  std::thread dispatcher_;
  std::thread get_responder_;
  std::thread put_responder_;

  StorageManager storage_manager_;

  std::atomic<bool> shutdown_;

  int rank_;
  int rank_size_;
  MPI_Comm comm;

  class Tagger {
   public:
    Tagger(int min, int max)
      : min_get_key(min), max_get_key(max) {
    }
    int getNextTag() {
      return  min_get_key + (cur_get_key++ % max_get_key);
    }
   private:
    const int min_get_key;
    const int max_get_key;
    std::atomic<int> cur_get_key;
  };

  Tagger get_value_tagger;

};

#endif  // PAPERLESS_PAPERLESSKV_H
