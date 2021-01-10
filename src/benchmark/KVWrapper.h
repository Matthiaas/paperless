#ifndef PAPERLESS_KVWRAPPER_H
#define PAPERLESS_KVWRAPPER_H

#include <cstddef>
#include <cstdlib>
#include <chrono>
#include <new>
#include <utility>
#include <string>

#include "../Common.h"
#include "helper.h"

int update_ratio;
int rank;
size_t keylen;
size_t vallen;
size_t count;
char* key_set;


char* get_key(int idx) {
  return key_set + (idx * PAPERLESS::padLength(keylen + 1));
}

void generate_key_set() {
  key_set = (char*) PAPERLESS::malloc(PAPERLESS::padLength(keylen + 1) * count);
  for (size_t i = 0; i < count; i++) {
    rand_str(keylen, get_key(i));
  }
}



#ifdef PAPERLESS_BENCHMARK

#include "../PaperlessKV.h"
#include "OptionReader.h"
#include <mpi.h>

namespace KV {
std::unique_ptr<PaperlessKV> paper;

int found = 0;

inline void WaitForGetComplete() {}
inline void SetQueryAmount(size_t l) {}

inline void Init(int argc, char** argv, std::string name) {
  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

  auto options = ReadOptionsFromEnvVariables()
      .Consistency(PaperlessKV::RELAXED)
      .Mode(PaperlessKV::READANDWRITE);
  paper = std::make_unique<PaperlessKV>(name, MPI_COMM_WORLD, 1, options);
}

inline void Fence() {
  paper->Fence();
}

inline void Checkpoint() {
  paper->FenceAndCheckPoint();
}

inline void SetMode(PaperlessKV::Consistency_t c, PaperlessKV::Mode_t  m) {
  paper->FenceAndChangeOptions(c, m);
}

inline void Put(const char* key, size_t key_len,
                const char* value, size_t value_len) {
  paper->put(key, key_len, value, value_len);
}

inline void Get(const char* key, size_t key_len,
                char* value, size_t value_len) {
  found += paper->get(key, key_len, value, value_len).first == FOUND;
}

inline void Finalize() {
  //std::cout << "found: " << found << std::endl;
  paper->Shutdown();
  std::error_code errorCode; 
  if (std::filesystem::remove_all(ReadOptionsFromEnvVariables().strorage_location , errorCode)) { 
    std::cout << errorCode.message() << std::endl; 
  }

}

inline int GetOwner(const char* key, size_t key_len) {
  return paper->GetOwner(key, key_len);
}
}

#elif defined I_PAPERLESS_BENCHMARK

#include "../PaperlessKV.h"
#include "OptionReader.h"
#include <mpi.h>

namespace KV {
  std::unique_ptr<PaperlessKV> paper;
  std::vector<FutureQueryInfo> futures;

  inline void Init(int argc, char** argv, std::string name) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);


    auto options = ReadOptionsFromEnvVariables()
        .Consistency(PaperlessKV::RELAXED)
        .Mode(PaperlessKV::READANDWRITE);
    paper = std::make_unique<PaperlessKV>(name, MPI_COMM_WORLD, 1, options);
  }

  inline void SetQueryAmount(size_t l) {
    futures.clear();
    futures.reserve(l);
  }

  inline void Fence() {
    paper->Fence();
  }

  inline void Checkpoint() {
    paper->FenceAndCheckPoint();
  }

  inline void SetMode(PaperlessKV::Consistency_t c, PaperlessKV::Mode_t  m) {
    paper->FenceAndChangeOptions(c, m);
  }

  inline void Put(const char* key, size_t key_len,
                  const char* value, size_t value_len) {
    paper->put(key, key_len, value, value_len);
  }

  inline void Get(const char* key, size_t key_len,
                  char* value, size_t value_len) {
    futures.push_back(paper->IGet(key, key_len, value, value_len));
  }

  inline void WaitForGetComplete() {
    for(auto& f : futures) {
      f.Get();
    }
  }

  inline void Finalize() {
    paper->Shutdown();
    std::error_code errorCode; 
    if (std::filesystem::remove_all(ReadOptionsFromEnvVariables().strorage_location , errorCode)) { 
      std::cout << errorCode.message() << std::endl; 
    }
  }

  inline int GetOwner(const char* key, size_t key_len) {
    return paper->GetOwner(key, key_len);
  }
}


#elif defined PAPYRUS_BENCHMARK

#include <mpi.h>
#include <string>
#include <iostream>
#include "../PaperlessKV.h"
#include "OptionReader.h"
#include <smhasher/MurmurHash3.h>
#include "../../papyrus/include/papyrus/kv.h"
#include "../../papyrus/include/papyrus/mpi.h"

namespace KV {
  inline void WaitForGetComplete() {}
  inline void SetQueryAmount(size_t l) {}

  // Copypaste of deafult Papyrus hash function.
  int papyruskv_hash_fn(const char* key, size_t keylen, size_t nranks) {

    uint32_t res;
    MurmurHash3_x86_32(key, keylen, 1, &res);
    return static_cast<Hash>(res) % nranks;
  }

  int db;
  int rank, size;

  inline void Init(int argc, char** argv, std::string name) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::string storage_directory = "$STORAGE_LOCATION";
    papyruskv_init(&argc, &argv, storage_directory.c_str());

    papyruskv_option_t opt;
    opt.hash = papyruskv_hash_fn;
    opt.keylen = 0;
    opt.vallen = 0;

    int ret = papyruskv_open(name.c_str(), PAPYRUSKV_CREATE | PAPYRUSKV_RELAXED | PAPYRUSKV_RDWR, &opt, &db);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
  }

  inline void Fence() {
    int ret = papyruskv_barrier(db, PAPYRUSKV_MEMTABLE);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
  }

  inline void Checkpoint() {
    int ret = papyruskv_barrier(db, PAPYRUSKV_SSTABLE);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
  }

  inline void SetMode(PaperlessKV::Consistency_t c, PaperlessKV::Mode_t  m) {
    if(m == PaperlessKV::READONLY) {
      int ret = papyruskv_protect(db, PAPYRUSKV_RDONLY);
      if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    } else if(m == PaperlessKV::READANDWRITE) {
      int ret = papyruskv_protect(db, PAPYRUSKV_RDWR);
      if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    }

    if(c == PaperlessKV::SEQUENTIAL) {
      int ret = papyruskv_consistency(db, PAPYRUSKV_SEQUENTIAL);
      if (ret != PAPYRUSKV_OK) printf("[%s:%d] FAILED:ret[%d]\n", __FILE__, __LINE__, ret);
    } else if(c == PaperlessKV::RELAXED) {
      int ret = papyruskv_consistency(db, PAPYRUSKV_SEQUENTIAL);
      if (ret != PAPYRUSKV_OK) printf("[%s:%d] FAILED:ret[%d]\n", __FILE__, __LINE__, ret);
    }

    Fence();
  }

  inline void Put(const char* key, size_t key_len,
                  const char* value, size_t value_len) {
    int ret = papyruskv_put(db, key, key_len, value, value_len);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
  }

  inline void Get(const char* key, size_t key_len,
                  char* value, size_t value_len) {
    int ret = papyruskv_get(db, key, key_len, &value, &value_len);
    if (ret != PAPYRUSKV_OK) {
      printf("ERROR [%s:%d] [%d/%d] key[%s]\n", __FILE__, __LINE__, rank, size, key);
    }
  }
  inline void Finalize() {
    int ret = papyruskv_close(db);
    if (ret != PAPYRUSKV_OK) printf("[%s:%d] ret[%d]\n", __FILE__, __LINE__, ret);
    papyruskv_finalize();
    std::error_code errorCode; 
    if (std::filesystem::remove_all(ReadOptionsFromEnvVariables().strorage_location , errorCode)) { 
      std::cout << errorCode.message() << std::endl; 
    }
  }

  inline int GetOwner(const char* key, size_t key_len) {
    return papyruskv_hash_fn(key, key_len, size);
  }
}

#endif

namespace TimedKV {
using namespace std::chrono;

inline long Init(int argc, char** argv, std::string name) {
  auto start = high_resolution_clock::now();
  KV::Init(argc, argv, name);
  auto end = high_resolution_clock::now();
  return (duration_cast<nanoseconds>(end-start)).count();
}

inline long Fence() {
  auto start = high_resolution_clock::now();
  KV::Fence();
  auto end = high_resolution_clock::now();
  return (duration_cast<nanoseconds>(end-start)).count();
}

inline long Checkpoint() {
  auto start = high_resolution_clock::now();
  KV::Checkpoint();
  auto end = high_resolution_clock::now();
  return (duration_cast<nanoseconds>(end-start)).count();
}

inline long SetMode(PaperlessKV::Consistency_t c, PaperlessKV::Mode_t  m) {
  auto start = high_resolution_clock::now();
  KV::SetMode(c, m);
  auto end = high_resolution_clock::now();
  return (duration_cast<nanoseconds>(end-start)).count();
}

inline std::pair<long,int> Put(const char* key, size_t key_len,
                               const char* value, size_t value_len) {
  auto start = high_resolution_clock::now();
  KV::Put(key, key_len, value, value_len);
  auto end = high_resolution_clock::now();
  return std::make_pair((duration_cast<nanoseconds>(end-start)).count(),
                        KV::GetOwner(key, key_len));
}

inline std::pair<long,int> Get(const char* key, size_t key_len,
                               char* value, size_t value_len) {
  auto start = high_resolution_clock::now();
  KV::Get(key, key_len, value, value_len);
  auto end = high_resolution_clock::now();
  return std::make_pair((duration_cast<nanoseconds>(end-start)).count(),
                        KV::GetOwner(key, key_len));
}

inline long Finalize() {
  auto start = high_resolution_clock::now();
  KV::Finalize();
  auto end = high_resolution_clock::now();
  return (duration_cast<nanoseconds>(end-start)).count();
}
}

#endif  // PAPERLESS_KVWRAPPER_H
