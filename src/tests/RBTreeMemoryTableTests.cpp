#include <catch.hpp>

#include "../RBTreeMemoryTable.h"

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
    char* buffer[buffer_len];
    const auto result =
        memTable.get(key, reinterpret_cast<char*>(buffer), buffer_len);
    CHECK(result.first == QueryStatus::FOUND);
    CHECK(result.second == value_len);
    CHECK(memcmp(buffer, value_bytes, value_len) == 0);
  }

  SECTION("too short buffer") {
    size_t buffer_len = 3;
    char* buffer[buffer_len];
    const auto result =
        memTable.get(key, reinterpret_cast<char*>(buffer), buffer_len);
    CHECK(result.first == QueryStatus::BUFFER_TOO_SMALL);
    CHECK(result.second == value_len);
  }

  SECTION("not found") {
    size_t buffer_len = 3;
    char* buffer[buffer_len];
    const auto result = memTable.get(
        {"nonexistent", 11}, reinterpret_cast<char*>(buffer), buffer_len);
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

  CHECK(memTable.size() == 0);

  char key_bytes[] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, std::move(val));
  CHECK(memTable.size() == key.Length() + val_expected.Length() + 1);

  char value_overwrite_bytes[] = "overwrite";
  Element val_overwrite_expected{value_overwrite_bytes, 9};
  Tomblement val_overwrite{value_overwrite_bytes, 9};

  memTable.put(key, std::move(val_overwrite));
  CHECK(memTable.size() == key.Length() + val_overwrite_expected.Length() + 1);

  char another_key_bytes[] = "another_key";
  ElementView another_key{another_key_bytes, 11};
  char another_value_bytes[] = "another_value";
  Element another_value_expected{another_value_bytes, 13};
  Tomblement another_value{another_value_bytes, 13};
  memTable.put(another_key, std::move(another_value));

  CHECK(memTable.size() ==
        another_key.Length() + another_value_expected.Length() + 1 +
            key.Length() + val_overwrite_expected.Length() + 1);
}