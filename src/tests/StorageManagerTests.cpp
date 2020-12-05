
#include <catch.hpp>
#include "../StorageManager.h"

#define ELEMENTVIEW(e) ElementView{e, sizeof(e)}
#define TOMBLEMENT(e) Tomblement{e, sizeof(e)}

char key1[2] = "1";
char key2[3] = "22";
char key3[4] = "333";
char key4[5] = "4444";
char key5[6] = "55555";

char value1[10] = "one";
char value2[10] = "two";
char value3[10] = "three";
char value4[10] = "four";
char value5[10] = "five";

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
  StorageManager storage_manager {dir, 0};

  RBTreeMemoryTable mtable1{};
  mtable1.put(ELEMENTVIEW(key1), TOMBLEMENT(value1));

  RBTreeMemoryTable mtable2{};
  mtable1.put(ELEMENTVIEW(key2), TOMBLEMENT(value2));

  storage_manager.flushToDisk(mtable1);

  REQUIRE(storage_manager.readFromDisk(ELEMENTVIEW(key1)).Value() == ELEMENTVIEW(value1));

  storage_manager.deleteDisk();
}
