
#include <catch.hpp>
#include "../BloomFilter.h"

TEST_CASE("Construct new BloomFilter") {
  BloomFilter filter{5, 0.05};
  REQUIRE(filter.GetNumHashes() == 4);
  REQUIRE(filter.GetSize() == 32);
}