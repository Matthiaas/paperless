#include <catch.hpp>
#include "../Element.h"

TEST_CASE("Example Test Case") {

    Element e{nullptr, 0};
    REQUIRE(e.Length() == 0);
    const auto result = Element::copyElementContent(e);
    const Element expected{nullptr, 0};
    CHECK(expected.Length() == result.Length());
}


TEST_CASE("Element Constructor Copys Data") {
  char bytes[] = "ThisIsATestString";
  int len = 17;
  Element e(bytes, len);
  CHECK(e.Length() == len);
  CHECK(e.Value() != bytes);
  for (size_t i = 0; i < len; i++) {
    CHECK(bytes[i] == e.Value()[i]);
  }
}

TEST_CASE("Construct NonOwningElement") {
  char bytes[] = "ThisIsATestString";
  int len = 17;
  NonOwningElement e(bytes, len);
  CHECK(e.Length() == len);
  CHECK(e.Value() == bytes);
}

TEST_CASE("Compare Owning and NonOwningElement") {
  char bytes[] = "ThisIsATestString";
  char bytes1[] = "ThisIsATestStrinh";
  int len = 17;
  NonOwningElement e(bytes, len);
  Element eo(bytes, len);
  NonOwningElement e1(bytes, len);
  Element eo1(bytes, len);
  CHECK(eo == e);
  CHECK(eo1 == e1);
  bool b = (eo == e1);
  CHECK(b);
  CHECK(!(eo == e1));
}