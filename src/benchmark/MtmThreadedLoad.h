#ifndef PAPERLESS_MTMTHREADEDLOAD_H
#define PAPERLESS_MTMTHREADEDLOAD_H

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <thread>

#include "../ListQueue.h"
#include "../MemoryTable.h"
#include "../MemoryTableManager.h"
#include "../RBTreeMemoryTable.h"
#include "../Status.h"
#include "timer.h"

#define KILO (1024UL)
#define MEGA (1024 * KILO)
#define GIGA (1024 * MEGA)

// Creates `keys_count` random keys on initialization and
// returns i-th one using `GetKey(i)` method.
class SharedRandomKeys {
 public:
  SharedRandomKeys(size_t len, size_t keys_count, int seed)
      : len_(len),
        keys_count_(keys_count),
        entry_len_(PAPERLESS::padLength(len)),
        generator_(seed) {
    generate_key_set_();
  }

  size_t GetKeyLength() const { return len_; }

  size_t GetKeysCount() const { return keys_count_; }

  char *GetKey(int idx) const {
    return key_set_ + (idx * entry_len_);
  }

 private:
  const size_t len_, keys_count_;
  const size_t entry_len_;
  char *key_set_;
  std::mt19937 generator_;

  void rand_str(size_t len, char *str) {
    static char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int l = (int)(sizeof(charset) - 1);
    std::uniform_int_distribution<int> distr(0, l - 1);
    for (size_t i = 0; i < len - 1; i++) {
      int key = distr(generator_);
      str[i] = charset[key];
    }
    str[len - 1] = 0;
  }

  void generate_key_set_() {
    key_set_ = (char *) (PAPERLESS::malloc(entry_len_ * keys_count_));
    for (size_t i = 0; i < keys_count_; i++) {
      rand_str(len_, GetKey(i));
    }
  }
};

template <typename MTM>
struct Producer {
  std::unique_ptr<std::thread> thread;

  Producer(MTM *mtm, SharedRandomKeys *keys, size_t vallen, size_t count,
           size_t update_ratio, int seed) {
    thread = std::make_unique<std::thread>(&Producer::Produce, mtm, keys,
                                           vallen, count, update_ratio, seed);
  }

  static void Produce(MTM *mtm, SharedRandomKeys *keys, size_t vallen,
                      size_t count, size_t update_ratio, int seed) {
    std::mt19937 generator(seed);
    std::uniform_int_distribution<int> rand_key_idx(0,
                                                    keys->GetKeysCount() - 1);

    std::string value("*", vallen);
    for (size_t i = 0; i < count; ++i) {
      ElementView key_elem(keys->GetKey(rand_key_idx(generator)),
                           keys->GetKeyLength());
      if (i % 100 < update_ratio) {
        Tomblement value_tombl(&value[0], value.size());
        mtm->Put(key_elem, std::move(value_tombl), /*hash=*/0, /*owner=*/0);
      } else {
        mtm->Get(key_elem, /*hash=*/0, /*owner=*/0);
      }
    }
  }
};

template <typename MTM>
struct Consumer {
  std::unique_ptr<std::thread> thread;

  Consumer(MTM *mtm) {
    thread = std::make_unique<std::thread>(&Consumer::Consume, mtm);
  }

  static void Consume(MTM *mtm) {
    using std::chrono_literals::operator""ms;
    while (true) {
      auto chunk = mtm->GetChunk();
      if (chunk.IsPoisonPill()) {
        chunk.Clear();
        break;
      }
      std::this_thread::sleep_for(100ms);
      chunk.Clear();
    }
  }
};

struct Result {
  double producers_done;
  double consumers_done;
};

template <typename MTM>
class MtmThreadedLoad {
 private:
 public:
  // Returns time elapsed.
  void Execute(int argc, char **argv, Result *result) {
    size_t keylen;
    size_t vallen;
    size_t count;
    int update_ratio;
    int memtable_size_kb;
    int num_producer_threads;
    int num_consumer_threads;
    int seed;

    if (argc < 7) {
      printf(
          "[%s:%d] usage: %s keylen vallen count update_ratio[0:100] "
          "membtable_size_kb num_producer_threads num_consumer_threads seed\n",
          __FILE__, __LINE__, argv[0]);
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
    MTM mtm(num_consumer_threads, memtable_size_kb);
    _w(0);
    // Create consumer threads.
    std::vector<Consumer<MTM>> consumers;
    for (int i = 0; i < num_consumer_threads; ++i) consumers.emplace_back(&mtm);

    // Create threads inserting elements.
    std::vector<Producer<MTM>> producers;
    // SharedRandomKeys *keys, size_t vallen, size_t count, size_t update_ratio,
    // int seed)
    for (int i = 0; i < num_producer_threads; ++i) {
      producers.emplace_back(&mtm, &keys, vallen, count, update_ratio,
                             /*seed=*/rand());
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
    _w(2);

    result->producers_done = _ww(0, 1);
    result->consumers_done = _ww(0, 2);
  }
};

#endif  // PAPERLESS_MTMTHREADEDLOAD_H
