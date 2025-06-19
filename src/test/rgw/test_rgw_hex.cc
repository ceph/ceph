
#include <catch2/catch_config.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#define CATCH_CONFIG_MAIN

#include "rgw/rgw_hex.h"

#include <ranges>
#include <cstring>

using std::end;
using std::begin;
using std::array;
using std::string;
using std::string_view;

TEST_CASE("hexdigit", "[rgw]") {

  SECTION("simple heuristic checks", "check known edge-cases") {
    CHECK(-EINVAL == hexdigit('\0'));

    CHECK(-EINVAL == hexdigit(static_cast<char>(255)));
    CHECK(-EINVAL == hexdigit(static_cast<char>(-1)));

    CHECK(0x0A == hexdigit('A'));
  }
}

TEST_CASE("buf_to_hex", "[rgw]") {

  constexpr auto in = "AabcdefghA";
  constexpr string_view out_expected { "41616263646566676841" };

  SECTION("C character array overload") {
    SECTION("empty input") {
      constexpr auto in_empty = "";
      const string_view out_expected_empty { "" };
      char out_data[] = {};

      buf_to_hex((const unsigned char* const)(in_empty), std::strlen(in_empty), out_data);

      CHECK(out_expected_empty == out_data);
    }

    SECTION("heuristic (known value)") {
      char out_data[1 + (3*sizeof(in))] = {};
  
      CAPTURE(out_data);
      buf_to_hex((const unsigned char* const)(in), std::strlen(in), out_data);
      CAPTURE(out_data);
  
      CHECK(out_expected == out_data);
    }
  }

  SECTION("std::array<> overload") {
    constexpr array<unsigned char, 1 + 10> in { "AabcdefghA" };
    constexpr array<unsigned char, 1 + out_expected.length()> out_array_expected = { "41616263646566676841" };

    CAPTURE(in);
    auto out_data = buf_to_hex(in);

    CAPTURE(out_array_expected);
    CAPTURE(out_array_expected.size());
    CAPTURE(out_data);
    CAPTURE(out_data.size());

    // buf_to_hex() forces the output size to be N*2+1, so we don't compare for /equality/:
    CHECK(std::lexicographical_compare(begin(out_array_expected), end(out_array_expected), 
                                       begin(out_data), end(out_data)));
  }
}

TEST_CASE("hex_to_buf", "[rgw]") {
  constexpr string_view in { "41616263646566676841" };
  constexpr string_view out_expected = "AabcdefghA";
   
  constexpr array<unsigned char, 1 + 10> in_array { "AabcdefghA" };
 
  char out_data[1 + out_expected.length()] = {};
  
  SECTION("heuristic (known value)") {
    int i = hex_to_buf(in.data(), out_data, in.length());
   
    CHECK(out_expected.length() == i);
    CHECK(out_expected == out_data);
  }
 
  SECTION("symmetry") {
   
    char out_data1[1 + (3*sizeof(in))] = {};
    
    auto out_data0 = buf_to_hex(in_array);
    
    unsigned i = hex_to_buf(out_data0.data(), out_data1, sizeof(out_data0));
    
    CHECK(in_array.size() == i);
    
    CHECK(std::equal(begin(in_array), end(in_array),
                     begin(out_data1), i + begin(out_data1)));
  }
}
