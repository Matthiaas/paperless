//
// Created by matthias on 04.11.20.
//

#ifndef PAPERLESS_PAPERLESSKV_H
#define PAPERLESS_PAPERLESSKV_H

#include <mpi.h>

#include "Responder.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <string>
#include <thread>

#include "Element.h"
#include "LRUHashCache.h"
#include "LRUTreeCache.h"
#include "ListQueue.h"
#include "MemoryTableManager.h"
#include "RBTreeMemoryTable.h"


#include "RemoteOperator.h"
#include "StorageManager.h"
#include "Types.h"

#define SYNC_TAG 0
#define  KEY_TAG 1
#define VALUE_TAG 2
#define  KEY_PUT_TAG 3
#define VALUE_PUT_TAG 4

class PaperlessKV {
 public:
  enum Consistency_t { SEQUENTIAL, RELAXED };
  enum Mode_t { READANDWRITE, READONLY};

  class Options {
   public:
    // Default values are set here.
    friend class PaperlessKV;
    size_t max_local_memtable_size = 1000000;
    size_t max_remote_memtable_size = 1000000;
    size_t max_local_cache_size = 1000000;
    size_t max_remote_cache_size = 1000000;
    std::string strorage_location;

    Consistency_t consistency = RELAXED;
    Mode_t mode = READANDWRITE;
   public:
    bool dispatch_data_in_chunks = true;

    Options MaxLocalMemTableSize(size_t s) {
      Options res = *this;
      res.max_local_memtable_size = s;
      return res;
    }
    Options MaxRemoteMemTableSize(size_t s) {
      Options res = *this;
      res.max_remote_memtable_size = s;
      return res;
    }
    Options MaxLocalCacheSize(size_t s) {
      Options res = *this;
      res.max_local_cache_size = s;
      return res;
    }
    Options MaxRemoteCacheSize(size_t s) {
      Options res = *this;
      res.max_remote_cache_size = s;
      return res;
    }
    Options Consistency(Consistency_t c) {
      Options res = *this;
      res.consistency = c;
      return res;
    }
    Options Mode(Mode_t m) {
      Options res = *this;
      res.mode = m;
      return res;
    }
    Options DispatchInChunks(size_t d) {
      Options res = *this;
      res.dispatch_data_in_chunks = static_cast<bool>(d);
      return res;
    }
    Options StorageLocation(std::string sl) {
      Options res = *this;
      res.strorage_location = sl;
      return res;
    }
  };

  PaperlessKV(std::string id, MPI_Comm comm, uint32_t hash_seed,
              Options options);
  PaperlessKV(std::string id, MPI_Comm comm, HashFunction, Options options);

  PaperlessKV(const PaperlessKV&) = delete;
  PaperlessKV(PaperlessKV&&) = delete;

  ~PaperlessKV();

  void put(const char* key, size_t key_len,const char* value, size_t value_len);
  QueryResult get(const char* key, size_t key_len);
  // If found, copies the value into the user-provided buffer and returns OK,
  // value length. If the buffer is too small, returns BUFFER_TOO_SMALL and
  // value length.
  std::pair<QueryStatus, size_t> get(const char* key, size_t key_len,
                                     char* value_buff, size_t value_buff_len);
  void DeleteKey(const char* key, size_t key_len);

  // Must be called in every rank.
  // Unblocks when all ranks processed all gets and puts.
  void Fence();

  // Same as fence but it allows to change the Consistency
  void FenceAndChangeOptions(Consistency_t c, Mode_t m);
  void FenceAndCheckPoint();

  void Shutdown();

  Owner GetOwner(const char* key, size_t key_len) {
    return hash_function_(key, key_len) % rank_size_;
  }

 private:

  using MemTable = RBTreeMemoryTable;
  using MemQueue = ListQueue<MemTable>;
  using RBTreeMemoryManager =
      MemoryTableManager<MemTable, MemQueue>;

  void Put(const ElementView& key, Tomblement&& value);
  QueryResult Get(const ElementView& key);
  void DeleteKey(const ElementView& key);

  // Called by the Compactor Thread to call the StorageManager with the data
  // given by the MemoryTableManager
  void Compact();
  // Called by the Dispatcher Thread to send data to its Owner rank
  // given by the MemoryTableManager. This is only required in RELAXED mode.
  void Dispatch();
  void Respond();

  // Must to be called in every rank.
  // Calls MPI_Barrier and unblocks when current rank is not precessing
  // local puts from a remote rank.
  void Sync();

  // Performs a LocalGet operation, given that the key is owned by this rank.
  QueryResult LocalGet(const ElementView& key, Hash hash);
  std::pair<QueryStatus, size_t> LocalGet(
      const ElementView& key, const ElementView& buff, Hash hash);
  // Gets the remote value but relaxed it might check caches and returns
  // outdated values.
  QueryResult RemoteGetRelaxed(const ElementView& key, Hash hash);
  std::pair<QueryStatus, size_t> RemoteGetRelaxed(
      const ElementView& key, const ElementView& v_buff, Hash hash);


  std::string id_;

  Consistency_t consistency_;
  Mode_t mode_;
  HashFunction hash_function_;

  RBTreeMemoryManager local_;
  RBTreeMemoryManager remote_;

  LRUTreeCache local_cache_;
  LRUTreeCache remote_cache_;

  //LRUHashCache local_cache_;
  //LRUHashCache remote_cache_;

  StorageManager storage_manager_;

  std::atomic<bool> shutdown_;

  int rank_;
  int rank_size_;
  const MPI_Comm comm_;

  Responder responder_;
  RemoteOperator remoteOperator_;

  std::thread compactor_;
  std::thread dispatcher_;
  std::thread responder_task;



  /*

  */



  // Settings:
  bool dispatch_data_in_chunks_;

  // Helpers:

  void WriteIntToBuff(char* ptr, unsigned int x) {
    ptr[0] = static_cast<char>(x & 0x000000ff);
    ptr[1] = static_cast<char>((x & 0x0000ff00) >> 8);
    ptr[2] = static_cast<char>((x & 0x00ff0000) >> 16);
    ptr[3] =static_cast<char>( (x & 0xff000000) >> 24);
  }

  unsigned int ReadIntFromBuff(char* ptr) {
    unsigned int x = 0;
    x = static_cast<unsigned int>(ptr[0]) &  0x000000ff;
    x += (static_cast<unsigned int>(ptr[1]) << 8) & 0x0000ff00;
    x += (static_cast<unsigned int>(ptr[2]) << 16) & 0x00ff0000;
    x += (static_cast<unsigned int>(ptr[3]) << 24) & 0xff000000;
    return x;
  }

  // For testing:
  friend class PaperLessKVFriend;
  friend class Responder;

};

#endif  // PAPERLESS_PAPERLESSKV_H
