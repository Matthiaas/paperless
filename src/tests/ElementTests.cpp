#include <vector>

#include <catch.hpp>
#include "../Element.h"

TEST_CASE("Example Test Case") {
  Element e{nullptr, 0};
  REQUIRE(e.Length() == 0);
  const auto result = Element::Element(e);
  const Element expected{nullptr, 0};
  CHECK(expected.Length() == result.Length());
}


TEST_CASE("ElementView Constructor Copys Data") {
  char bytes[] = "ThisIsATestString";
  size_t len = 17;
  Element e(bytes, len);
  CHECK(e.Length() == len);
  CHECK(e.Value() != bytes);
  for (size_t i = 0; i < len; i++) {
    CHECK(bytes[i] == e.Value()[i]);
  }
}

TEST_CASE("Construct ElementView") {
  char bytes[] = "ThisIsATestString";
  size_t len = 17;
  ElementView e(bytes, len);
  CHECK(e.Length() == len);
  CHECK(e.Value() == bytes);
}

TEST_CASE("Compare Element and ElementView") {
  char bytes[] = "ThisIsATestString";
  char bytes1[] = "ThisIsATestStrinh";
  size_t len = 17;
  ElementView e(bytes, len);
  Element eo(bytes, len);
  ElementView e1(bytes1, len);
  Element eo1(bytes1, len);
  CHECK(eo == e);
  CHECK(eo == e);
  CHECK(!(eo == e1));
  CHECK(!(eo == e1));
}
