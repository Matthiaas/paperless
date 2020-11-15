#include <catch.hpp>
#include "../Element.h"

TEST_CASE("Example Test Case") {
    // TODO: This is just an example test case, this DOES NOT actually test the
    //  functionality.

    Element e{nullptr, 0};
    REQUIRE(e.Length() == 0);
    const auto result = Element::copyElementContent(e);
    const Element expected{nullptr, 0};
    CHECK(expected.Length() == result.Length());
}
