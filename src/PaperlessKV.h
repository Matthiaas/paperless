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
#include "LRUCache.h"
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

#define MAX_MTABLE_SIZE 10000
#define MAX_CACHE_SIZE 1000

class PaperlessKV {
 public:
  enum Consistency { SEQUENTIAL, RELAXED };
  enum Mode { READANDWRITE, READONLY};

  PaperlessKV(std::string id, MPI_Comm comm, uint32_t hash_seed, Consistency c = Consistency::RELAXED, Mode m =READANDWRITE);
  PaperlessKV(std::string id, MPI_Comm comm, HashFunction, Consistency c = Consistency::RELAXED, Mode m =READANDWRITE);

  PaperlessKV(const PaperlessKV&) = delete;
  PaperlessKV(PaperlessKV&&) = delete;

  ~PaperlessKV();

  void put(const char* key, size_t key_len,const char* value, size_t value_len);
  QueryResult get(const char* key, size_t key_len);
  void deleteKey(char* key, size_t key_len);

  // Must be called in every rank.
  // Unblocks when all ranks processed all gets and puts.
  void Fence();

  // Same as fence but it allows to change the Consistency
  void FenceAndChangeOptions(Consistency c, Mode m);
  void FenceAndCheckPoint();

 private:

  using MemTable = RBTreeMemoryTable;
  using MemQueue = ListQueue<MemTable>;
  using RBTreeMemoryManager =
      MemoryTableManager<MemTable, MemQueue>;

  void Put(const ElementView& key, Tomblement&& value);
  QueryResult Get(const ElementView& key);
  void deleteKey(const ElementView& key);

  // Called by the Compactor Thread to call the StorageManager with the data
  // given by the MemoryTableManager
  void Compact();
  // Called by the Dispatcher Thread to send data to its Owner rank
  // given by the MemoryTableManager. This is only required in RELAXED mode.
  void Dispatch();
  // Responds to remote get requests and send value back to the requesting rank.
  void RespondGet();
  // Works through remote put requests to store them locally.
  // TODO: Should this actually respond?
  void RespondPut();

  // Must to be called in every rank.
  // Calls MPI_Barrier and unblocks when current rank is not precessing
  // local puts from a remote rank.
  void Sync();

  // Sends a Key to specified target with specified tag.
  void SendKey(const ElementView& key, int target, int tag);
  // Sends a QueryResult (StatusOr) to specified target with specified tag.
  void SendValue(const QueryResult& key, int target, int tag);

  Element ReceiveKey(int source, int tag, MPI_Status* status);
  QueryResult ReceiveValue(int source, int tag, MPI_Status* status);

  // TODO: Implement this.
  int receiveKey(const ElementView& buff, int source, int tag, MPI_Status status);
  int receiveValue(const ElementView& buff, int source, int tag, MPI_Status status);


  // Performs a LocalGet operation, given that the key is owned by this rank.
  QueryResult LocalGet(const ElementView& key, Hash hash);
  // Gets the remote value but relaxed it migh check caches and returns outdated
  // values.
  QueryResult RemoteGetRelaxed(const ElementView& key, Hash hash);
  // Get a remote value immediately (used for SEQUENTIAL consistency).
  QueryResult RemoteGetValue(const ElementView& key, Hash hash);
  // Puts a single value to a remote rank immediately.
  void RemotePut(const ElementView& key, Hash hash, const ElementView& value);

  std::string id_;

  Consistency consistency_;
  Mode mode_;
  HashFunction hash_function_;

  RBTreeMemoryManager local_;
  RBTreeMemoryManager remote_;

  LRUCache local_cache_;
  LRUCache remote_cache_;

  StorageManager storage_manager_;

  std::atomic<bool> shutdown_;

  int rank_;
  int rank_size_;
  const MPI_Comm comm_;

  std::thread compactor_;
  std::thread dispatcher_;
  std::thread get_responder_;
  std::thread put_responder_;

  /*
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
  */

  volatile int fence_calls_received;
  std::mutex fence_mutex;
  std::condition_variable fence_wait;

  // For testing:
  friend class PaperLessKVFriend;

};

#endif  // PAPERLESS_PAPERLESSKV_H
