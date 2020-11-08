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
#include "MemoryTableManager.h"
#include "StorageManager.h"
#include "Types.h"

class DUMMY {
 public:
  class Handler {
   public:
    void clear();
    MemoryTable get();
  };
};

class PaperlessKV {
 public:
  enum Consistency { SEQUENTIAL, RELAXED };

  explicit PaperlessKV(std::string id, MPI_Comm comm, Consistency);
  PaperlessKV(std::string id, MPI_Comm comm, HashFunction, Consistency);

  PaperlessKV(const PaperlessKV&) = delete;
  PaperlessKV(PaperlessKV&&);

  ~PaperlessKV();

  void put(char* key, size_t key_len, char* value, size_t value_len);
  void get(char* key, size_t key_len);
  void deleteKey(char* key, size_t key_len);

  void put(Element key, Element value);
  std::optional<Element> get(Element key);
  void deleteKey(Element key);

 private:

  void compact();
  void dispatch();
  void respond();

  std::optional<Element>  localGet(Element key, Hash hash);

  std::optional<Element> remoteGetRelaxed(Element key, Hash hash);
  std::optional<Element> remoteGetNow(Element key, Hash hash);

  std::string id_;

  Consistency consistency;
  HashFunction hash_function_;

  MemoryTableManager<DUMMY> local_;
  MemoryTableManager<DUMMY> remote_;

  MemoryTable local_cache_;
  MemoryTable remote_cache_;

  std::thread compactor_;
  std::thread dispatcher_;
  std::thread responder_;

  StorageManager storage_manager_;

  std::atomic<bool> shutdown_;

  int rank_;
  int rank_size_;
  MPI_Comm comm;


  class Tagger {
   public:
    Tagger(int min, int max)
      : max_get_key(max), min_get_key(min) {
    }
    int getNextTag() {
      return  min_get_key + (cur_get_key++ % max_get_key);
    }
   private:
    const int max_get_key;
    const int min_get_key;
    std::atomic<int> cur_get_key;
  };

  Tagger get_value_tagger;



};

#endif  // PAPERLESS_PAPERLESSKV_H
