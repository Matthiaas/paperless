
#include <catch.hpp>

#include "../StorageManager.h"

#define ELEMENTVIEW(e) ElementView{e, sizeof(e)}
#define TOMBLEMENT(e) Tomblement{e, sizeof(e)}

alignas(PAPERLESS::kStride) char key1[PAPERLESS::kStride] = "1";
alignas(PAPERLESS::kStride) char key2[PAPERLESS::kStride] = "22";
alignas(PAPERLESS::kStride) char key3[PAPERLESS::kStride] = "333";
alignas(PAPERLESS::kStride) char key4[PAPERLESS::kStride] = "4444";
alignas(PAPERLESS::kStride) char key5[PAPERLESS::kStride] = "55555";

alignas(PAPERLESS::kStride) char value1[PAPERLESS::kStride] = "one";
alignas(PAPERLESS::kStride) char value2[PAPERLESS::kStride] = "two";
alignas(PAPERLESS::kStride) char value3[PAPERLESS::kStride] = "three";
alignas(PAPERLESS::kStride) char value4[PAPERLESS::kStride] = "four";
alignas(PAPERLESS::kStride) char value5[PAPERLESS::kStride] = "five";

// TODO(westermann): Shitty test for now, make better one later.
TEST_CASE("general StorageManager test") {
  std::string dir = "/tmp/StorageManagerTest/";
  StorageManager storage_manager {dir};

  RBTreeMemoryTable mtable1{};
  mtable1.put(ELEMENTVIEW(key1), TOMBLEMENT(value1));
  mtable1.put(ELEMENTVIEW(key2), TOMBLEMENT(value2));
  mtable1.put(ELEMENTVIEW(key3), TOMBLEMENT(value3));
  mtable1.put(ELEMENTVIEW(key4), TOMBLEMENT(value4));

  RBTreeMemoryTable mtable2{};
  mtable2.put(ELEMENTVIEW(key2), Tomblement::getATombstone());

  storage_manager.flushToDisk(mtable1);

  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key1)).Value() == ELEMENTVIEW(value1));
  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key2)).Value() == ELEMENTVIEW(value2));
  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key5)) == QueryStatus::NOT_FOUND);

  storage_manager.flushToDisk(mtable2);

  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key1)).Value() == ELEMENTVIEW(value1));
  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key2)) == QueryStatus::DELETED);
  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key5)) == QueryStatus::NOT_FOUND);

  storage_manager.deleteDisk();
}

TEST_CASE("Check with on-disk bloomfilter") {
  std::string dir = "/tmp/StorageManagerTest/";
  StorageManager storage_manager {dir, 1};

  RBTreeMemoryTable mtable1{};
  mtable1.put(ELEMENTVIEW(key1), TOMBLEMENT(value1));
  mtable1.put(ELEMENTVIEW(key2), TOMBLEMENT(value2));

  RBTreeMemoryTable mtable2{};
  mtable2.put(ELEMENTVIEW(key2), TOMBLEMENT(value2));

  storage_manager.flushToDisk(mtable1);

  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key1)).Value() == ELEMENTVIEW(value1));

  storage_manager.deleteDisk();
}

TEST_CASE("Reopening StorageManager preserves data") {
  std::string dir = "/tmp/StorageManagerTest/";
  StorageManager storage_manager {dir};

  RBTreeMemoryTable mtable1{};
  mtable1.put(ELEMENTVIEW(key1), TOMBLEMENT(value1));
  storage_manager.flushToDisk(mtable1);
  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key1)).Value() == ELEMENTVIEW(value1));

  StorageManager new_storage_manager {dir};
  REQUIRE(new_storage_manager.readFromDisk(ELEMENTVIEW(key1)).Value() == ELEMENTVIEW(value1));

  storage_manager.deleteDisk();
}