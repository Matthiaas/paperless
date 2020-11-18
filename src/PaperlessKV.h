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

#define SYNC_TAG 0
#define  KEY_TAG 1
#define VALUE_TAG 2
#define  KEY_PUT_TAG 3
#define VALUE_PUT_TAG 4

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

  void Fence();

 private:

  using MemTable = RBTreeMemoryTable;
  using MemQueue = ListQueue<MemTable>;
  using RBTreeMemoryManager =
      MemoryTableManager<MemTable, MemQueue>;

  void Put(const Element& key, Tomblement&& value);
  QueryResult Get(const Element& key);
  void deleteKey(const Element& key);

  void Compact();
  void Dispatch();
  void RespondGet();
  void RespondPut();



  void SendKey(const Element& key, int target, int tag);
  void SendValue(const QueryResult& key, int target, int tag);
  int receiveKey(const Element& buff, int source, int tag, MPI_Status status);
  int receiveValue(const Element& buff, int source, int tag, MPI_Status status);
  OwningElement ReceiveKey(int source, int tag, MPI_Status* status);
  QueryResult ReceiveValue(int source, int tag, MPI_Status* status);


  QueryResult LocalGet(const Element& key, Hash hash);
  QueryResult RemoteGetRelaxed(const Element& key, Hash hash);
  QueryResult RemoteGetValue(const Element& key, Hash hash);
  void RemotePut(const Element& key, Hash hash, const Element& value);

  std::string id_;

  Consistency consistency_;
  HashFunction hash_function_;

  RBTreeMemoryManager local_;
  RBTreeMemoryManager remote_;

  RBTreeMemoryTable local_cache_;
  RBTreeMemoryTable remote_cache_;

  StorageManager storage_manager_;

  std::atomic<bool> shutdown_;

  int rank_;
  int rank_size_;
  MPI_Comm comm_;

  std::thread compactor_;
  std::thread dispatcher_;
  std::thread get_responder_;
  std::thread put_responder_;



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
    std::atomic<int> cur_get_key = 0;
  };

  Tagger get_value_tagger;

  volatile int fence_calls_received;
  std::mutex fence_mutex;
  std::condition_variable fence_wait;

};

#endif  // PAPERLESS_PAPERLESSKV_H
