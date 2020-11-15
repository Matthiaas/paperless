#include <catch.hpp>

#include "../Status.h"

TEST_CASE("StatusOr HasNoValue") {
  StatusOr<int, QueryStatus> res = QueryStatus::NOT_FOUND;
  CHECK(res.hasValue() == false);

  StatusOr<int, QueryStatus> res1(QueryStatus::NOT_FOUND);
  CHECK(res1.hasValue() == false);

  const auto status = QueryStatus::NOT_FOUND;
  StatusOr<int, QueryStatus> res2(status);
  CHECK(res2.hasValue() == false);
}


TEST_CASE("StatusOr HasValue") {
  StatusOr<int, QueryStatus> res = 1;
  CHECK(res.hasValue() == true);

  StatusOr<int, QueryStatus> res1(1);
  CHECK(res1.hasValue() == true);

  const auto status = 1;
  StatusOr<int, QueryStatus> res2(1);
  CHECK(res2.hasValue() == true);
}


TEST_CASE("StatusOr Comparison With Status And Type") {
  StatusOr<int, QueryStatus> res = 1;
  CHECK(res == 1);
  CHECK(res != 2);
  CHECK(res != QueryStatus::NOT_FOUND);
  CHECK(res != QueryStatus::DELETED);

  StatusOr<int, QueryStatus> res2 = QueryStatus::NOT_FOUND;
  CHECK(res2 != 1);
  CHECK(res2 != 2);
  CHECK(res2 == QueryStatus::NOT_FOUND);
  CHECK(res2 != QueryStatus::DELETED);
}

