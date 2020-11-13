#include <catch.hpp>

#include "../Status.h"

// TODO: Fixme.
TEST_CASE("StatusOr HasValue") {
  StatusOr<int, QueryStatus> res = QueryStatus::NOT_FOUND;
  CHECK(res.hasValue() == false);
}
