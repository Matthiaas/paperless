#include <catch.hpp>

#include "../LRUHashCache.h"

TEST_CASE("LRUHashCache: Put with View & Get Test", "[LRUHashTest]") {
  LRUHashCache memTable(1000,3);
  alignas(PAPERLESS::kStride) char key_bytes[] = "key";
  ElementView n_key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};


  memTable.put(n_key, 0, Element::copyElementContent(val_expected));
  const auto result = memTable.get(n_key, 0);
  CHECK(result->hasValue());
  bool a = val_expected == (**result);
  CHECK(a);
}


TEST_CASE("LRUHashCache: Put with Key Move & Get Test", "[LRUHashTest]") {
  LRUHashCache memTable(1000,3);
  alignas(PAPERLESS::kStride) char key_bytes[64] = "key";
  ElementView key{key_bytes, 3};
  Element keyToMove{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};

  memTable.put(std::move(keyToMove), 0,  Element::copyElementContent(val_expected));
  const auto result = memTable.get(key, 0);
  CHECK(result->hasValue());
  CHECK(val_expected == (**result));
}

TEST_CASE("LRUHashCache: Get & Put Overwrite Test", "[LRUHashTest]") {
  LRUHashCache memTable(1000,3);
  alignas(PAPERLESS::kStride) char key_bytes[64] = "key";
  ElementView key{key_bytes, 3};

  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, 0, Element::copyElementContent(val_expected));
  const auto result = memTable.get(key, 0);
  CHECK(result.has_value());
  CHECK(val_expected == (**result));

  char value_overwrite_bytes[] = "overwrite";
  Element val_overwrite_expected{value_overwrite_bytes, 9};
  Tomblement val_overwrite{value_overwrite_bytes, 9};

  memTable.put(key, 0, Element::copyElementContent(val_overwrite_expected));
  const auto result_overwrite = memTable.get(key, 0);
  CHECK(result_overwrite.has_value());
  CHECK(val_overwrite_expected == (**result_overwrite));
}

/*
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
*/

TEST_CASE("LRUHashCache: Put, Delete & Get Test", "[LRUHashTest]") {
  LRUHashCache memTable(1000,3);
  alignas(PAPERLESS::kStride) char key_bytes[64] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, 0, Element::copyElementContent(val_expected));
  memTable.put(key, 0, QueryStatus::DELETED);
  const auto result = memTable.get(key, 0);
  CHECK(result->Status() == QueryStatus::DELETED);
}

TEST_CASE("LRUHashCache: Old Elements Get Removed Test", "[LRUHashTest]") {
  LRUHashCache memTable(25,3);

  CHECK(memTable.size() == 0);

  alignas(PAPERLESS::kStride) char key_bytes[64] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, 0, Element::copyElementContent(val_expected));
  const auto res =memTable.get(key, 0);
  CHECK(res.has_value());
  CHECK(val_expected == (**res));

  alignas(PAPERLESS::kStride) char another_key_bytes[64] = "another_key";
  ElementView another_key{another_key_bytes, 11};
  char another_value_bytes[] = "another_value";
  Element another_value_expected{another_value_bytes, 13};
  memTable.put(another_key, 0, Element::copyElementContent(another_value_expected));

  const auto res2 =memTable.get(key, 0);
  CHECK(!res2.has_value());


  const auto re3 =memTable.get(another_key, 0);
  CHECK(re3.has_value());
  CHECK(another_value_expected == (**re3));
}



TEST_CASE("LRUHashCache: Size Test", "[LRUHashTest]") {
  LRUHashCache memTable(1000,3);

  CHECK(memTable.size() == 0);

  alignas(PAPERLESS::kStride) char key_bytes[64] = "key";
  ElementView key{key_bytes, 3};
  char value_bytes[] = "value";
  Element val_expected{value_bytes, 5};

  memTable.put(key, 0, Element::copyElementContent(val_expected));
  CHECK(memTable.size() == key.Length() + val_expected.Length() );

  char value_overwrite_bytes[] = "overwrite";
  Element val_overwrite_expected{value_overwrite_bytes, 9};


  memTable.put(key, 0, Element::copyElementContent(val_overwrite_expected));
  CHECK(memTable.size() == key.Length() + val_overwrite_expected.Length() );

  alignas(PAPERLESS::kStride) char another_key_bytes[64] = "another_key";
  ElementView another_key{another_key_bytes, 11};
  char another_value_bytes[] = "another_value";
  Element another_value_expected{another_value_bytes, 13};
  memTable.put(another_key, 0, Element::copyElementContent(another_value_expected));

  CHECK(memTable.size() ==
        another_key.Length() + another_value_expected.Length()  +
        key.Length() + val_overwrite_expected.Length() );
}

