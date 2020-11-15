#include <catch.hpp>

#include "../Status.h"
#include "../MemoryTableManager.h"
#include "../ListWriteBuffer.h"
#include "../RBTreeMemoryTable.h"

char key_bytes[] = "key";
const Element key{key_bytes, 3};
char value_bytes[] = "value";
const Element val{value_bytes, 5};


//TEST_CASE("MemoryTableManager Get") {
//  MemoryTableManager<RBTreeMemoryTable, ListWriteBuffer<RBTreeMemoryTable>>
//    manager(/*rank_size=*/1, /*max_mtable_size=*/10);
//  manager.put(key, val, /*hash=*/1, /*owner=*/0);
//  const auto result = manager.get(key, /*hash=*/1, /*owner=*/0);
//  CHECK(val == result.Value());
//}



// TODO: Should work once StatusOr is fixed.
TEST_CASE("MemoryTableManager Dequeue") {
  MemoryTableManager<RBTreeMemoryTable, ListWriteBuffer<RBTreeMemoryTable>> 
    manager(/*rank_size=*/1, /*max_mtable_size=*/0);
  // Enqueues memory table.
  // manager.put(key, val, /*hash=*/1, /*owner=*/0, false);
  // Dequeue memory table.
  // auto chunk = manager.getChunk();
  // const auto result_direct = chunk.get()->get(key);
  // const auto result_dequeued = manager.get(key, /*hash=*/1, /*owner=*/0);
  // chunk.clear();
  // QueryResult result_cleared = manager.get(key, /*hash=*/1, /*owner=*/0);

  // CHECK(val == result_direct.Value());
  // CHECK(val == result_dequeued.Value());
  // CHECK(result_cleared == QueryStatus::NOT_FOUND);
}
