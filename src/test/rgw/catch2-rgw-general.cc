
#include <catch2/catch_all.hpp>
#define CATCH_CONFIG_MAIN

#include "common/reverse.h"

TEST_CASE("check support", "[common]")
{
 REQUIRE(true); 	// "it's working!"
}

SCENARIO("reverse_bits is reversible") {
 GIVEN("some input") {
     const uint32_t n = 0x00AAAAFF;

  WHEN("we reverse it twice") {
      const auto out = reverse_bits(reverse_bits(n));

   THEN("we should get back the original sequence") {
	REQUIRE(out == n);
   }
  }
 }
}

