// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include "osd/OmapGetValues.h"

#include <catch2/catch_test_macros.hpp>

#include <map>
#include <string>

TEST_CASE("Native OMAP values survive borrowed input and request destruction", "[omap]")
{
  const std::string empty;
  std::map<std::string, ceph::bufferlist> retained;
  std::map<std::string, std::string> expected;
  {
    ceph::osd::OmapGetValues query(empty, empty, 1000);
    for (unsigned i = 0; i < 200; ++i) {
      std::string key = std::to_string(i) + std::string("\0key", 4);
      std::string value(i % 13 == 0 ? 0 : 1000 + i * 51, static_cast<char>(i));
      expected.emplace(key, value);
      query.append(key, value);
      key.assign("reused key");
      value.assign("reused value");
    }
    retained = std::move(query.values);
    query.reset();
    query.append("later", "another allocation");
  }
  REQUIRE(expected.size() == retained.size());
  for (const auto& [key, value] : expected) {
    REQUIRE(retained.at(key).to_str() == value);
  }
}

TEST_CASE("Native OMAP collection retains first-wins map decoding", "[omap]")
{
  const std::string empty;
  ceph::osd::OmapGetValues query(empty, empty, 10);
  query.append("", "");
  query.append("", "replacement");
  query.append(std::string("\0\xFF", 2), std::string("\0value", 6));
  query.append(std::string("\0\xFF", 2), "replacement");
  REQUIRE(2 == query.values.size());
  CHECK(query.values.at("").length() == 0);
  CHECK(query.values.at(std::string("\0\xFF", 2)).to_str() == std::string("\0value", 6));
}

TEST_CASE("A retried native OMAP read discards its failed attempt", "[omap]")
{
  const std::string empty;
  ceph::osd::OmapGetValues query(empty, empty, 10);
  query.append("old", "failed attempt");
  query.more = true;
  query.reset();
  query.append("new", "successful attempt");
  REQUIRE(1 == query.values.size());
  CHECK_FALSE(query.more);
  CHECK(query.values.at("new").to_str() == "successful attempt");
}
