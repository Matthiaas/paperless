#include <memory>
#include <thread>
#include <iostream>
#include <set>
#include <string>

#include <catch.hpp>

#include "../Status.h"
#include "../MemoryTableManager.h"
#include "../ListQueue.h"
#include "../RBTreeMemoryTable.h"

using MemTable = RBTreeMemoryTable;
using MemQueue = ListQueue<MemTable>;
using RBTreeMemoryManager =
    MemoryTableManager<MemTable, MemQueue>;

char kKeyBytes[] = "key";
char kValueBytes[] = "value";
const Element kSampleValue{kValueBytes, 5};
const ElementView kSampleKey{kKeyBytes, 3};

TEST_CASE("MemoryTableManager Simple Get") {
  RBTreeMemoryManager manager(/*consumer_count_=*/0, /*max_mtable_size=*/10);
  Tomblement val{kValueBytes, 5};
  manager.Put(kSampleKey, std::move(val), /*hash=*/1, /*owner=*/0);
  const auto result = manager.Get(kSampleKey, /*hash=*/1, /*owner=*/0);
  CHECK(kSampleValue == result.Value());
}

TEST_CASE("MemoryTableManager Simple Dequeue") {
  // Setting max_mtable_size=0 forces memory tables to have a single element.
  RBTreeMemoryManager manager(/*consumer_count_=*/0, /*max_mtable_size=*/0);
  Tomblement val{kValueBytes, 5};
  // Enqueues memory table.
  manager.Put(kSampleKey, std::move(val), /*hash=*/1, /*owner=*/0);
  // Dequeue memory table.
  auto chunk = manager.GetChunk();
  const auto result_direct = chunk.Get()->get(kSampleKey);
  const auto result_dequeued = manager.Get(kSampleKey, /*hash=*/1, /*owner=*/0);
  chunk.Clear();
  QueryResult result_cleared = manager.Get(kSampleKey, /*hash=*/1, /*owner=*/0);

  CHECK(kSampleValue == result_direct.Value());
  CHECK(kSampleValue == result_dequeued.Value());
  CHECK(result_cleared == QueryStatus::NOT_FOUND);
}

std::string CreateIthKey(int i) {
  return "key" + std::to_string(i);
}

std::string CreateIthValue(int i) {
  return "value" + std::to_string(i);
}

struct Consumer {
  std::unique_ptr<std::vector<Element>> keys;
  std::unique_ptr<std::vector<Tomblement>> values;
  std::unique_ptr<std::thread> thread;

  Consumer(RBTreeMemoryManager *manager) {
    keys = std::make_unique<std::vector<Element>>();
    values = std::make_unique<std::vector<Tomblement>>();
    thread = std::make_unique<std::thread>(&Consumer::Consume, manager, keys.get(), values.get());
  }

  static void Consume(RBTreeMemoryManager *manager, std::vector<Element> *keys, std::vector<Tomblement> *values) {
    while (true) {
      auto chunk = manager->GetChunk();
      if (chunk.IsPoisonPill()) {
        chunk.Clear();
        break;
      }
      for (auto &kv : *chunk.Get()) {
        keys->emplace_back(Element::Element(kv.first));
        values->emplace_back(kv.second.Clone());
      }
      chunk.Clear();
    }
  }
};

struct Producer {
  const int start;
  const int count;
  std::unique_ptr<std::thread> thread;

  Producer(RBTreeMemoryManager *manager, int start, int count) : start(start), count(count) {
    thread = std::make_unique<std::thread>(&Producer::Produce, manager, start, count);
  }

  static void Produce(RBTreeMemoryManager *manager, int start, int count) {
    for (int i = start; i < start + count; ++i) {
      std::string key = CreateIthKey(i);
      std::string value = CreateIthValue(i);
      ElementView key_elem(&key[0], key.size());
      Tomblement value_tombl(&value[0], value.size());
      manager->Put(key_elem, std::move(value_tombl), /*hash=*/0, /*owner=*/0);
    }
  }
};

TEST_CASE("MemoryTableManager Threaded Put/Dequeue") {
  const int consumer_count = 10;
  MemoryTableManager<RBTreeMemoryTable, ListQueue<RBTreeMemoryTable>> 
    manager(consumer_count, /*max_mtable_size=*/0);
  // Create consumer threads.
  const int producer_count = 10;
  const int elems_per_thread = 10;
  std::vector<Consumer> consumers;
  for (int i = 0; i < consumer_count; ++i) consumers.emplace_back(&manager);

  // Create threads inserting elements.
  std::vector<Producer> producers;
  for (int i = 0; i < producer_count; ++i) {
    producers.emplace_back(&manager, i * elems_per_thread, elems_per_thread);
  }

  for (auto &c : producers) {
    if (c.thread->joinable()) {
      c.thread->join();
    }
  }

  manager.Shutdown();
  for (auto &c : consumers) {
    if (c.thread->joinable()) {
      c.thread->join();
    }
  }
  
  std::set<Element> expected_keys, actual_keys;
  for (int i = 0; i < consumer_count; ++i) {
    for (int j = i * elems_per_thread; j < i * elems_per_thread + elems_per_thread; ++j) {
      std::string key = CreateIthKey(j);
      expected_keys.insert(Element(&key[0], key.size()));
    }
  }
  for (auto &c : consumers) {
    for (auto &k : *c.keys) {
      actual_keys.insert(std::move(k));
    }
  }  
  CHECK(manager.Size() == 0);
  CHECK(expected_keys == actual_keys);
}
