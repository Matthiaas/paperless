#include <memory>

#include <catch.hpp>

#include "../Status.h"
#include "../MemoryTableManager.h"
#include "../ListWriteBuffer.h"
#include "../RBTreeMemoryTable.h"

char key_bytes[] = "key";
const NonOwningElement key{key_bytes, 3};
char value_bytes[] = "value";
const Element val_expected{value_bytes, 5};


TEST_CASE("MemoryTableManager Get") {
  MemoryTableManager<RBTreeMemoryTable, ListWriteBuffer<RBTreeMemoryTable>>
    manager(/*rank_size=*/1, /*max_mtable_size=*/10);
  Tomblement val{value_bytes, 5};
  manager.put(key, std::move(val), /*hash=*/1, /*owner=*/0);
  const auto result = manager.get(key, /*hash=*/1, /*owner=*/0);
  CHECK(val_expected == result.Value());
}

TEST_CASE("MemoryTableManager Dequeue") {
  MemoryTableManager<RBTreeMemoryTable, ListWriteBuffer<RBTreeMemoryTable>> 
    manager(/*rank_size=*/1, /*max_mtable_size=*/0);
  Tomblement val{value_bytes, 5};
  // Enqueues memory table.
  manager.put(key, std::move(val), /*hash=*/1, /*owner=*/0);
  // Dequeue memory table.
  auto chunk = manager.getChunk();
  const auto result_direct = chunk.get()->get(key);
  const auto result_dequeued = manager.get(key, /*hash=*/1, /*owner=*/0);
  chunk.clear();
  QueryResult result_cleared = manager.get(key, /*hash=*/1, /*owner=*/0);

  CHECK(val_expected == result_direct.Value());
  CHECK(val_expected == result_dequeued.Value());
  CHECK(result_cleared == QueryStatus::NOT_FOUND);
}
