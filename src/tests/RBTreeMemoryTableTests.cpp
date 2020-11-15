#include <catch.hpp>

#include "../RBTreeMemoryTable.h"

TEST_CASE("Get & Put Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  NonOwningElement n_key {key_bytes, 3};
  Element key{key_bytes, 3};
  char value_bytes[] = "value";
  Tomblement val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(std::move(key), std::move(val));
  const auto result = memTable.get(n_key);
  CHECK(result.hasValue());
  bool a = val_expected == (*result);
  CHECK(a);
}
