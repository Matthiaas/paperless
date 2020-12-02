#include "MtmThreadedLoad.h"
#include "../ListQueue.h"
#include "../RBTreeMemoryTable.h"

#include <iostream>

using MemTable = RBTreeMemoryTable;
using MemMutexQueue = MutexedListQueue<MemTable>;
using MtmMutex =
    MemoryTableManager<MemTable, MemMutexQueue>;
using MemSharedMutexQueue = ListQueue<MemTable>;
using MtmSharedMutex =
    MemoryTableManager<MemTable, MemSharedMutexQueue>;

int main(int argc, char** argv) {
  MtmThreadedLoad<MtmMutex> bench_mutex;
  MtmThreadedLoad<MtmSharedMutex> bench_shared_mutex;

  Result mutex_res, shared_res;
  bench_mutex.Execute(argc, argv, &mutex_res);
  bench_shared_mutex.Execute(argc, argv, &shared_res);
  std::cout << "w_lock: " << mutex_res.producers_done << " " << mutex_res.consumers_done << std::endl;
  std::cout << "rw_lock: " << shared_res.producers_done << " " << shared_res.consumers_done << std::endl;
  return 0;
} 