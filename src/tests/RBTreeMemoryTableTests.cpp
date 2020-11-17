#include <catch.hpp>

#include "../RBTreeMemoryTable.h"

TEST_CASE("Get & Put Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  Element n_key {key_bytes, 3};
  char value_bytes[] = "value";
  OwningElement val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(n_key, std::move(val));
  const auto result = memTable.get(n_key);
  CHECK(result.hasValue());
  bool a = val_expected == (*result);
  CHECK(a);
}


TEST_CASE("Override Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  Element n_key {key_bytes, 3};
  char value_bytes1[] = "value1";
  char value_bytes2[] = "value2";
  OwningElement val_expected1{value_bytes1, 6};
  OwningElement val_expected2{value_bytes2, 6};
  Tomblement val1{value_bytes1, 6};
  Tomblement val2{value_bytes2, 6};

  memTable.put(n_key, std::move(val1));
  const auto result = memTable.get(n_key);
  CHECK(result.hasValue());
  CHECK(val_expected1 == (*result));

  memTable.put(n_key, std::move(val2));
  const auto result2 = memTable.get(n_key);
  CHECK(result.hasValue());
  CHECK(val_expected2 == (*result));
}