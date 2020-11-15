#include <catch.hpp>

#include "../RBTreeMemoryTable.h"

TEST_CASE("Get & Put Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  const Element key{key_bytes, 3};
  char value_bytes[] = "value";
  const Element val{value_bytes, 5};

  memTable.put(key, val, false);
  const auto result = memTable.get(key);
  CHECK(result.hasValue());
  CHECK(val == result.Value());
}
