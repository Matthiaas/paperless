// https://code.ornl.gov/eck/papyrus/-/blob/master/kv/apps/sc17/workload.cpp

#include "../MemoryTable.h"
#include "../MemoryTableManager.h"
#include "../ListQueue.h"
#include "../Status.h"
#include "../RBTreeMemoryTable.h"
#include <stdlib.h>
#include <stdio.h>
#include <random>
#include <thread>

#include "timer.h"

#define KILO    (1024UL)
#define MEGA    (1024 * KILO)
#define GIGA    (1024 * MEGA)


using MemTable = RBTreeMemoryTable;
using MemQueue = ListQueue<MemTable>;
using RBTreeMemoryManager =
    MemoryTableManager<MemTable, MemQueue>;

// Creates `keys_count` random keys on initialization and
// returns i-th one using `GetKey(i)` method.
class SharedRandomKeys {
public:
  SharedRandomKeys(size_t len, size_t keys_count, int seed) 
    : len_(len), keys_count_(keys_count), generator_(seed) {
    generate_key_set_();
  }

  size_t GetKeyLength() const {
    return len_;
  }

  size_t GetKeysCount() const {
    return keys_count_;
  }

  char* GetKey(int idx) const {
    return key_set_ + (idx * (len_ + 1));
  }

private:
  const size_t len_, keys_count_;
  char *key_set_;
  std::mt19937 generator_;
  
  void rand_str(size_t len, char* str) {
    static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int l = (int) (sizeof(charset) -1);
    std::uniform_int_distribution<int> distr(0, l-1);
    for (size_t i = 0; i < len - 1; i++) {
      int key = distr(generator_);
      str[i] = charset[key];
    }
    str[len - 1] = 0;
  }

  void generate_key_set_() {
    key_set_ = new char[(len_ + 1) * keys_count_];
    for (size_t i = 0; i < keys_count_; i++) {
      rand_str(len_, GetKey(i));
    }
  }
};

struct Producer {
  std::unique_ptr<std::thread> thread;

  Producer(RBTreeMemoryManager *mtm, SharedRandomKeys *keys, size_t vallen,
    size_t count, size_t update_ratio, int seed) {
    thread = std::make_unique<std::thread>(
      &Producer::Produce, mtm, keys, vallen, count, update_ratio, seed);
  }

  static void Produce(RBTreeMemoryManager *mtm, SharedRandomKeys *keys,
    size_t vallen, size_t count, size_t update_ratio, int seed) {
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> rand_key_idx(0, keys->GetKeysCount() - 1);

    std::string value("*", vallen);
    for (size_t i = 0; i < count; ++i) {
      ElementView key_elem(keys->GetKey(rand_key_idx(generator)), keys->GetKeyLength());
      if (i%100 < update_ratio) {
        Tomblement value_tombl(&value[0], value.size());
        mtm->Put(key_elem, std::move(value_tombl), /*hash=*/0, /*owner=*/0);
      } else {
        mtm->Get(key_elem, /*hash=*/0, /*owner=*/0);
      }
    }
  }
};

struct Consumer {
  std::unique_ptr<std::thread> thread;

  Consumer(RBTreeMemoryManager *mtm) {
    thread = std::make_unique<std::thread>(&Consumer::Consume, mtm);
  }

  static void Consume(RBTreeMemoryManager *mtm) {
    while (true) {
      auto chunk = mtm->GetChunk();
      if (chunk.IsPoisonPill()) {
        chunk.Clear();
        break;
      }
      chunk.Clear();
    }
  }
};


int main(int argc, char** argv) {
  size_t keylen;
  size_t vallen;
  size_t count;
  int update_ratio;
  int memtable_size_kb;
  int num_producer_threads;
  int num_consumer_threads;
  int seed;

  if (argc < 5) {
    printf("[%s:%d] usage: %s keylen vallen count update_ratio[0:100] membtable_size_kb num_producer_threads num_consumer_threads seed\n", __FILE__, __LINE__, argv[0]);
    return 0;
  }

  keylen = atol(argv[1]);
  vallen = atol(argv[2]);
  count = atol(argv[3]);
  update_ratio = atoi(argv[4]);
  memtable_size_kb = atoi(argv[5]);
  num_producer_threads = atoi(argv[6]);
  num_consumer_threads = atoi(argv[7]);
  seed = atoi(argv[8]);
  srand(seed);

  SharedRandomKeys keys(keylen, count * num_producer_threads, seed);
  RBTreeMemoryManager mtm(num_consumer_threads, memtable_size_kb);

  double cb = (keylen + vallen) * count;
  double total = (keylen + vallen) * count * num_producer_threads;
  printf("[%s:%d] update_ratio[%d%%] keylen[%lu] vallen[%lu] count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB num_threads[%d] seed[%d] total count[%lu] size[%0.lf]KB [%lf]MB [%lf]GB\n", __FILE__, __LINE__, update_ratio, keylen, vallen, count, cb / KILO, cb / MEGA, cb / GIGA, num_producer_threads, seed, count * num_producer_threads, total / KILO, total / MEGA, total / GIGA);

  _w(0);
  // Create consumer threads.
  std::vector<Consumer> consumers;
  for (int i = 0; i < num_consumer_threads; ++i) consumers.emplace_back(&mtm);

  // Create threads inserting elements.
  std::vector<Producer> producers;
  // SharedRandomKeys *keys, size_t vallen, size_t count, size_t update_ratio, int seed)
  for (int i = 0; i < num_producer_threads; ++i) {
    producers.emplace_back(&mtm, &keys, vallen, count, update_ratio, /*seed=*/rand());
  }

  for (auto &c : producers) {
    if (c.thread->joinable()) {
      c.thread->join();
    }
  }

  _w(1);
  mtm.Shutdown();
  for (auto &c : consumers) {
    if (c.thread->joinable()) {
      c.thread->join();
    }
  }
  
  printf("Time elapsed: %lf", _ww(0, 1));
  return 0;
}