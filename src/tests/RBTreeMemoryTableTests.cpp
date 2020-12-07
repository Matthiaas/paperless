#include <catch.hpp>
#include <thread>

#include "../RBTreeMemoryTable.h"
#include "TestUtils.h"

TEST_CASE("RBTreeMemoryTable: Put with View & Get Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  ElementView n_key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(n_key, std::move(val));
  const auto result = memTable.get(n_key);
  CHECK(result.hasValue());
  bool a = val_expected == (*result);
  CHECK(a);
}

TEST_CASE("RBTreeMemoryTable: Put with Key Move & Get Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  ElementView key{key_bytes, 3};
  Element keyToMove{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};

  memTable.put(std::move(keyToMove), {value_bytes, 5});
  const auto result = memTable.get(key);
  CHECK(result.hasValue());
  CHECK(val_expected == (*result));
}

TEST_CASE("RBTreeMemoryTable: Get & Put Overwrite Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  ElementView key{key_bytes, 3};

  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, std::move(val));
  const auto result = memTable.get(key);
  CHECK(result.hasValue());
  CHECK(val_expected == (*result));

  char value_overwrite_bytes[] = "overwrite";
  Element val_overwrite_expected{value_overwrite_bytes, 9};
  Tomblement val_overwrite{value_overwrite_bytes, 9};

  memTable.put(key, std::move(val_overwrite));
  const auto result_overwrite = memTable.get(key);
  CHECK(result_overwrite.hasValue());
  CHECK(val_overwrite_expected == (*result_overwrite));
}

TEST_CASE("RBTreeMemoryTable: Put & Get with user-provided buffer") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  size_t value_len = 5;
  Element val_expected{value_bytes, value_len};
  Tomblement val{value_bytes, value_len};

  memTable.put(key, std::move(val));

  SECTION("correct buffer") {
    size_t buffer_len = value_len;
    char *buffer[buffer_len];
    const auto result =
        memTable.get(key, reinterpret_cast<char *>(buffer), buffer_len);
    CHECK(result.first == QueryStatus::FOUND);
    CHECK(result.second == value_len);
    CHECK(memcmp(buffer, value_bytes, value_len) == 0);
  }

  SECTION("too short buffer") {
    size_t buffer_len = 3;
    char *buffer[buffer_len];
    const auto result =
        memTable.get(key, reinterpret_cast<char *>(buffer), buffer_len);
    CHECK(result.first == QueryStatus::BUFFER_TOO_SMALL);
    CHECK(result.second == value_len);
  }

  SECTION("not found") {
    size_t buffer_len = 3;
    char *buffer[buffer_len];
    const auto result = memTable.get(
        {"nonexistent", 11}, reinterpret_cast<char *>(buffer), buffer_len);
    CHECK(result.first == QueryStatus::NOT_FOUND);
    CHECK(result.second == 0);
  }
}

TEST_CASE("RBTreeMemoryTable: Put, Delete & Get Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, std::move(val));
  memTable.put(key, Tomblement::getATombstone());
  const auto result = memTable.get(key);
  CHECK(result.Status() == QueryStatus::DELETED);
}

TEST_CASE("RBTreeMemoryTable: Size Test") {
  RBTreeMemoryTable memTable;

  CHECK(memTable.ByteSize() == 0);

  char key_bytes[] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, std::move(val));
  CHECK(memTable.ByteSize() == key.Length() + val_expected.Length() + 1);

  char value_overwrite_bytes[] = "overwrite";
  Element val_overwrite_expected{value_overwrite_bytes, 9};
  Tomblement val_overwrite{value_overwrite_bytes, 9};

  memTable.put(key, std::move(val_overwrite));
  CHECK(memTable.ByteSize() ==
        key.Length() + val_overwrite_expected.Length() + 1);

  char another_key_bytes[] = "another_key";
  ElementView another_key{another_key_bytes, 11};
  char another_value_bytes[] = "another_value";
  Element another_value_expected{another_value_bytes, 13};
  Tomblement another_value{another_value_bytes, 13};
  memTable.put(another_key, std::move(another_value));

  CHECK(memTable.ByteSize() ==
        another_key.Length() + another_value_expected.Length() + 1 +
            key.Length() + val_overwrite_expected.Length() + 1);
}

template <int N>
struct Reader {
  std::unique_ptr<std::vector<Element>> keys;
  QueryResult readResults[N];
  std::unique_ptr<std::thread> thread;

  Reader(RBTreeMemoryTable *memoryTable, int start, int count) {
    keys = std::make_unique<std::vector<Element>>();
    thread = std::make_unique<std::thread>(
        &Reader::Read, memoryTable, keys.get(), readResults, start, count);
  }

  static void Read(RBTreeMemoryTable *memoryTable, std::vector<Element> *keys,
                   QueryResult values[N], int start, int count) {
    for (int i = start * count; i < (start + 1) * count; i++) {
      const std::string key = CreateIthKey(i);
      keys->emplace_back(&key[0], key.size());
      ElementView e(&key[0], key.size());
      QueryResult result = memoryTable->get(e);
      values[i - start * count] = std::move(result);
    }
  }
};

struct Writer {
  const int start;
  const int count;
  std::unique_ptr<std::thread> thread;

  Writer(RBTreeMemoryTable *memoryTable, int start, int count)
      : start(start), count(count) {
    thread = std::make_unique<std::thread>(&Writer::Write, memoryTable, start,
                                           count);
  }

  static void Write(RBTreeMemoryTable *memoryTable, int start, int count) {
    for (int i = start; i < start + count; ++i) {
      std::string key = CreateIthKey(i);
      std::string value = CreateIthValue(i);
      Element key_elem(&key[0], key.size());
      Tomblement value_tombl(&value[0], value.size());
      memoryTable->put(std::move(key_elem), std::move(value_tombl));
    }
  }
};

TEST_CASE("RBTreeMemoryTable: Threaded Put/Get") {
  const int thread_count = 100;
  const int elems_per_thread = 10;
  RBTreeMemoryTable memoryTable;

  std::vector<Reader<elems_per_thread>> readers;
  std::vector<Writer> writers;
  for (int i = 0; i < thread_count; ++i) {
    writers.emplace_back(&memoryTable, i * elems_per_thread, elems_per_thread);
  }
  for (int i = 0; i < thread_count; ++i) {
    readers.emplace_back(&memoryTable, i * elems_per_thread, elems_per_thread);
  }

  for (auto &t : writers) {
    if (t.thread->joinable()) {
      t.thread->join();
    }
  }
  for (auto &t : readers) {
    if (t.thread->joinable()) {
      t.thread->join();
    }
  }

  std::map<Element, Tomblement> map;
  // Check everything got properly inserted.
  for (int i = 0; i < thread_count * elems_per_thread; i++) {
    std::string key = CreateIthKey(i);
    std::string value = CreateIthValue(i);
    Element key_elem(&key[0], key.size());
    Tomblement value_tombl(&value[0], value.size());
    Element value_elem(&value[0], value.size());
    map.insert(std::make_pair(std::move(key_elem), std::move(value_tombl)));

    ElementView key_elem_view(&key[0], key.size());
    auto result = memoryTable.get(key_elem_view);

    CHECK(result.hasValue());
    CHECK(result.Value() == value_elem.GetView());
  }

  // Check that all readers got either NOT_FOUND or the correct value.
  for (int i = 0; i < thread_count; i++) {
    CHECK(readers[i].keys->size() == elems_per_thread);
    for (int j = 0; j < elems_per_thread; j++) {
      auto &result = readers[i].readResults[j];
      auto &key = readers[i].keys->at(j);
      bool not_found =
          !result.hasValue() && result.Status() == QueryStatus::NOT_FOUND;
      bool correct_value =
          result.hasValue() && (result.Value() == map.at(key).GetView());
      CHECK((not_found || correct_value));
    }
  }
}