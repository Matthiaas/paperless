#include <vector>

#include <catch.hpp>
#include "../Element.h"

TEST_CASE("Example Test Case") {
  OwningElement e{nullptr, 0};
  REQUIRE(e.Length() == 0);
  const auto result = OwningElement::copyElementContent(e);
  const OwningElement expected{nullptr, 0};
  CHECK(expected.Length() == result.Length());
}


TEST_CASE("Element Constructor Copys Data") {
  char bytes[] = "ThisIsATestString";
  size_t len = 17;
  OwningElement e(bytes, len);
  CHECK(e.Length() == len);
  CHECK(e.Value() != bytes);
  for (size_t i = 0; i < len; i++) {
    CHECK(bytes[i] == e.Value()[i]);
  }
}

TEST_CASE("Construct Element") {
  char bytes[] = "ThisIsATestString";
  size_t len = 17;
  Element e(bytes, len);
  CHECK(e.Length() == len);
  CHECK(e.Value() == bytes);
}

TEST_CASE("Compare OwningElement and Element") {
  char bytes[] = "ThisIsATestString";
  char bytes1[] = "ThisIsATestStrinh";
  size_t len = 17;
  Element e(bytes, len);
  OwningElement eo(bytes, len);
  Element e1(bytes1, len);
  OwningElement eo1(bytes1, len);
  CHECK(eo == e);
  CHECK(eo == e);
  CHECK(!(eo == e1));
  CHECK(!(eo == e1));
}
