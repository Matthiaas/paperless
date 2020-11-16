#include <catch.hpp>

#include "../Element.h"
#include "../Status.h"
#include "../ListQueue.h"
#include "../RBTreeMemoryTable.h"

TEST_CASE("Let's see if it compiles") {
  ListQueue<RBTreeMemoryTable> buffer;
  buffer.Enqueue(std::make_unique<RBTreeMemoryTable>());
  auto chunk = buffer.Dequeue();
  chunk.Get();
  chunk.Clear();
}
