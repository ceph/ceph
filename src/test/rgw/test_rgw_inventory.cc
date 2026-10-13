// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <catch2/catch_all.hpp>

#include "rgw/rgw_inventory.h"

using namespace rgw::inventory;

namespace {

Configuration cfg(const std::string& versions, const std::string& freq)
{
  Configuration c;
  c.id = "inv";
  c.included_object_versions = versions;
  c.schedule.frequency = freq;
  return c;
}

constexpr time_t kDay = 86400;
constexpr time_t kJan1_2026 = 1767225600;  // Thursday 00:00 UTC

} // anonymous namespace

TEST_CASE("Current versions with no optional fields selects nothing extra",
          "[inventory][fields]")
{
  const auto s = field_selection_from_config(cfg("Current", "Daily"));
  CHECK_FALSE(s.version_id);
  CHECK_FALSE(s.is_latest);
  CHECK_FALSE(s.is_delete_marker);
  CHECK_FALSE(s.size);
  CHECK_FALSE(s.last_modified);
  CHECK_FALSE(s.etag);
  CHECK_FALSE(s.storage_class);
}

TEST_CASE("All versions adds the version columns", "[inventory][fields]")
{
  const auto s = field_selection_from_config(cfg("All", "Daily"));
  CHECK(s.version_id);
  CHECK(s.is_latest);
  CHECK(s.is_delete_marker);
}

TEST_CASE("Optional fields map to columns", "[inventory][fields]")
{
  auto c = cfg("Current", "Daily");
  c.optional_fields = {"Size", "ETag", "BucketKeyStatus"};
  const auto s = field_selection_from_config(c);
  CHECK(s.size);
  CHECK(s.etag);
  CHECK(s.bucket_key_status);
  CHECK_FALSE(s.storage_class);
  CHECK_FALSE(s.last_modified);
}

TEST_CASE("Daily is always due", "[inventory][schedule]")
{
  for (int i = 0; i < 7; ++i) {
    CHECK(is_due(cfg("All", "Daily"), "t:b", kJan1_2026 + i * kDay));
  }
}

TEST_CASE("Weekly is due exactly once per week", "[inventory][schedule]")
{
  for (const std::string bucket : {"t:a", "t:photos", ":logs", "x:y"}) {
    int due = 0;
    for (int i = 0; i < 7; ++i) {
      due += is_due(cfg("All", "Weekly"), bucket, kJan1_2026 + i * kDay) ? 1 : 0;
    }
    CHECK(due == 1);
  }
}

TEST_CASE("Weekly day assignment is stable across calls", "[inventory][schedule]")
{
  for (int i = 0; i < 7; ++i) {
    const time_t t = kJan1_2026 + i * kDay;
    CHECK(is_due(cfg("All", "Weekly"), "t:photos", t) ==
          is_due(cfg("All", "Weekly"), "t:photos", t));
  }
}

TEST_CASE("Unknown frequency is never due", "[inventory][schedule]")
{
  CHECK_FALSE(is_due(cfg("All", "Hourly"), "t:b", kJan1_2026));
}
