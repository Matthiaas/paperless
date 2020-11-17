#include <catch.hpp>

#include "../RBTreeMemoryTable.h"

TEST_CASE("RBTreeMemoryTable: Get & Put Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  Element n_key{key_bytes, 3};
  char value_bytes[] = "value";
  OwningElement val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(n_key, std::move(val));
  const auto result = memTable.get(n_key);
  CHECK(result.hasValue());
  bool a = val_expected == (*result);
  CHECK(a);
}

TEST_CASE("RBTreeMemoryTable: Get & Put Overwrite Test") {
  RBTreeMemoryTable memTable;
  char key_bytes[] = "key";
  Element key{key_bytes, 3};

  char value_bytes[] = "value";
  OwningElement val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, std::move(val));
  const auto result = memTable.get(key);
  CHECK(result.hasValue());
  CHECK(val_expected == (*result));

  char value_overwrite_bytes[] = "overwrite";
  OwningElement val_overwrite_expected{value_overwrite_bytes, 9};
  Tomblement val_overwrite{value_overwrite_bytes, 9};

  memTable.put(key, std::move(val_overwrite));
  const auto result_overwrite = memTable.get(key);
  CHECK(result_overwrite.hasValue());
  CHECK(val_overwrite_expected == (*result_overwrite));
}

TEST_CASE("RBTreeMemoryTable: Size Test") {
  RBTreeMemoryTable memTable;

  CHECK(memTable.size() == 0);

  char key_bytes[] = "key";
  Element key{key_bytes, 3};
  char value_bytes[] = "value";
  OwningElement val_expected{value_bytes, 5};
  Tomblement val{value_bytes, 5};

  memTable.put(key, std::move(val));
  CHECK(memTable.size() == key.Length() + val_expected.Length() + 1);

  char value_overwrite_bytes[] = "overwrite";
  OwningElement val_overwrite_expected{value_overwrite_bytes, 9};
  Tomblement val_overwrite{value_overwrite_bytes, 9};

  memTable.put(key, std::move(val_overwrite));
  CHECK(memTable.size() == key.Length() + val_overwrite_expected.Length() + 1);

  char another_key_bytes[] = "another_key";
  Element another_key{another_key_bytes, 11};
  char another_value_bytes[] = "another_value";
  OwningElement another_value_expected{another_value_bytes, 13};
  Tomblement another_value{another_value_bytes, 13};
  memTable.put(another_key, std::move(another_value));

  CHECK(memTable.size() ==
        another_key.Length() + another_value_expected.Length() + 1 +
            key.Length() + val_overwrite_expected.Length() + 1);
}