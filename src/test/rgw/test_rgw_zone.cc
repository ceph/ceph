// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_zone.h"

#include "common/ceph_json.h"
#include "include/encoding.h"

#include <cstring>
#include <list>
#include <optional>
#include <string>

#include <gtest/gtest.h>

namespace {

const std::string local_id = "74506436-cfb6-4105-8a8c-edd0c62632b7";
const std::string remote_id = "70271812-eb5c-472a-9050-16e289e78941";

RGWZoneGroup make_zonegroup(const std::string& id, const std::string& name,
                            std::list<std::string> endpoints)
{
  RGWZoneGroup zonegroup{id, name};
  zonegroup.endpoints = std::move(endpoints);
  return zonegroup;
}

RGWZone& add_master_zone(RGWZoneGroup& zonegroup, const std::string& zone_id,
                         std::list<std::string> endpoints)
{
  zonegroup.master_zone = zone_id;
  RGWZone& zone = zonegroup.zones[zone_id];
  zone.id = zone_id;
  zone.endpoints = std::move(endpoints);
  return zone;
}

RGWPeriod make_period(std::initializer_list<RGWZoneGroup> zonegroups)
{
  RGWPeriod period{"period-id"};
  for (const auto& zonegroup : zonegroups) {
    period.period_map.zonegroups[zonegroup.id] = zonegroup;
  }
  return period;
}

// pre-admin_endpoints on-wire format (struct version 6)
void encode_zonegroup_v6(const RGWZoneGroup& zg, bufferlist& bl)
{
  ENCODE_START(6, 1, bl);
  encode(zg.name, bl);
  encode(zg.api_name, bl);
  encode(zg.is_master, bl);
  encode(zg.endpoints, bl);
  encode(zg.master_zone, bl);
  encode(zg.zones, bl);
  encode(zg.placement_targets, bl);
  encode(zg.default_placement, bl);
  encode(zg.hostnames, bl);
  encode(zg.hostnames_s3website, bl);
  {
    ENCODE_START(1, 1, bl);
    encode(zg.id, bl);
    encode(zg.name, bl);
    ENCODE_FINISH(bl);
  }
  encode(zg.realm_id, bl);
  encode(zg.sync_policy, bl);
  encode(zg.enabled_features, bl);
  ENCODE_FINISH(bl);
}

} // anonymous namespace

TEST(ZonegroupEndpoint, PrefersZonegroupEndpoint)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://zg:80"});
  add_master_zone(zonegroup, "zone-id", {"http://zone:80"});

  EXPECT_EQ("http://zg:80", rgw::get_zonegroup_endpoint(zonegroup));
}

TEST(ZonegroupEndpoint, FallsBackToMasterZoneEndpoint)
{
  auto zonegroup = make_zonegroup(local_id, "local", {});
  add_master_zone(zonegroup, "zone-id", {"http://zone:80"});

  EXPECT_EQ("http://zone:80", rgw::get_zonegroup_endpoint(zonegroup));
}

TEST(ZonegroupEndpoint, EmptyWithoutAnyEndpoint)
{
  auto zonegroup = make_zonegroup(local_id, "local", {});
  add_master_zone(zonegroup, "zone-id", {});

  EXPECT_EQ("", rgw::get_zonegroup_endpoint(zonegroup));
}

TEST(ZonegroupSystemEndpoint, PrefersAdminEndpoints)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://public:80"});
  const auto& zone = add_master_zone(zonegroup, "zone-id", {"http://zone:80"});
  zonegroup.admin_endpoints = {"http://admin:80"};

  const std::list<std::string> expected{"http://admin:80"};
  EXPECT_EQ(expected, rgw::get_zonegroup_system_endpoints(
                          zonegroup, zonegroup.endpoints));
  EXPECT_EQ(expected, rgw::get_zonegroup_system_endpoints(
                          zonegroup, zone.endpoints));
  EXPECT_FALSE(rgw::uses_legacy_system_endpoints(zonegroup));
}

TEST(ZonegroupSystemEndpoint, AdminEndpointsDoNotChangeRedirect)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://public:80"});
  zonegroup.admin_endpoints = {"http://admin:80"};

  EXPECT_EQ("http://public:80", rgw::get_zonegroup_endpoint(zonegroup));
}

// period push and radosgw-admin used only the zonegroup endpoints
TEST(ZonegroupSystemEndpoint, LegacyZonegroupEndpoints)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://public:80"});
  add_master_zone(zonegroup, "zone-id", {"http://zone:80"});

  const std::list<std::string> expected{"http://public:80"};
  EXPECT_EQ(expected, rgw::get_zonegroup_system_endpoints(
                          zonegroup, zonegroup.endpoints));
  EXPECT_TRUE(rgw::uses_legacy_system_endpoints(zonegroup));
}

TEST(ZonegroupSystemEndpoint, LegacyZonegroupEndpointsHaveNoZoneFallback)
{
  auto zonegroup = make_zonegroup(local_id, "local", {});
  add_master_zone(zonegroup, "zone-id", {"http://zone:80"});

  EXPECT_TRUE(rgw::get_zonegroup_system_endpoints(
                  zonegroup, zonegroup.endpoints).empty());
}

// zonegroup connections in RGWSI_Zone used the master zone endpoints
TEST(ZonegroupSystemEndpoint, LegacyMasterZoneEndpoints)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://public:80"});
  const auto& zone = add_master_zone(zonegroup, "zone-id", {"http://zone:80"});

  const std::list<std::string> expected{"http://zone:80"};
  EXPECT_EQ(expected, rgw::get_zonegroup_system_endpoints(
                          zonegroup, zone.endpoints));
}

TEST(ZonegroupEncode, AdminEndpointsRoundtrip)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://public:80"});
  zonegroup.admin_endpoints = {"http://admin:80"};

  bufferlist bl;
  encode(zonegroup, bl);

  RGWZoneGroup decoded;
  auto iter = bl.cbegin();
  decode(decoded, iter);

  EXPECT_EQ(zonegroup.admin_endpoints, decoded.admin_endpoints);
  EXPECT_EQ(zonegroup.endpoints, decoded.endpoints);
}

TEST(ZonegroupEncode, V6DecodeLeavesAdminEndpointsEmpty)
{
  auto zonegroup = make_zonegroup(local_id, "local", {"http://public:80"});

  bufferlist bl;
  encode_zonegroup_v6(zonegroup, bl);

  RGWZoneGroup decoded;
  auto iter = bl.cbegin();
  decode(decoded, iter);

  EXPECT_TRUE(decoded.admin_endpoints.empty());
  EXPECT_EQ(zonegroup.endpoints, decoded.endpoints);
}

TEST(ZonegroupJson, AdminEndpointsOptional)
{
  const char *json =
      R"({"id":"zg-id","name":"zg","is_master":true,"endpoints":["http://public:80"]})";
  JSONParser parser;
  ASSERT_TRUE(parser.parse(json, strlen(json)));

  RGWZoneGroup zonegroup;
  zonegroup.decode_json(&parser);

  EXPECT_TRUE(zonegroup.admin_endpoints.empty());
  ASSERT_EQ(1u, zonegroup.endpoints.size());
  EXPECT_EQ("http://public:80", zonegroup.endpoints.front());
}

TEST(ZonegroupJson, AdminEndpointsFromJson)
{
  const char *json =
      R"({"id":"zg-id","name":"zg","is_master":true,"endpoints":["http://public:80"],)"
      R"("admin_endpoints":["http://admin:80"]})";
  JSONParser parser;
  ASSERT_TRUE(parser.parse(json, strlen(json)));

  RGWZoneGroup zonegroup;
  zonegroup.decode_json(&parser);

  ASSERT_EQ(1u, zonegroup.admin_endpoints.size());
  EXPECT_EQ("http://admin:80", zonegroup.admin_endpoints.front());
}

TEST(FindZonegroupById, ReturnsLocalZonegroup)
{
  const auto local = make_zonegroup(local_id, "local", {"http://local:80"});
  const std::optional<RGWPeriod> no_period;

  EXPECT_EQ(&local, rgw::find_zonegroup_by_id(local, no_period, local_id));
}

TEST(FindZonegroupById, ReturnsLocalZonegroupForEmptyIdOnMaster)
{
  // buckets created before zonegroups existed have no zonegroup id, and
  // RGWZoneGroup::equals() treats those as local on the master zonegroup
  auto local = make_zonegroup(local_id, "local", {"http://local:80"});
  local.is_master = true;
  const std::optional<RGWPeriod> no_period;

  EXPECT_EQ(&local, rgw::find_zonegroup_by_id(local, no_period, ""));
}

TEST(FindZonegroupById, ReturnsNullWithoutPeriod)
{
  const auto local = make_zonegroup(local_id, "local", {"http://local:80"});
  const std::optional<RGWPeriod> no_period;

  EXPECT_EQ(nullptr, rgw::find_zonegroup_by_id(local, no_period, remote_id));
}

TEST(FindZonegroupById, ReturnsNullForUnknownId)
{
  const auto local = make_zonegroup(local_id, "local", {"http://local:80"});
  const std::optional<RGWPeriod> period = make_period({local});

  EXPECT_EQ(nullptr, rgw::find_zonegroup_by_id(local, period, remote_id));
}

TEST(FindZonegroupById, ReturnsRemoteZonegroupFromPeriod)
{
  const auto local = make_zonegroup(local_id, "local", {"http://local:80"});
  const auto remote = make_zonegroup(remote_id, "remote", {"http://remote:80"});
  const std::optional<RGWPeriod> period = make_period({local, remote});

  const RGWZoneGroup* found =
      rgw::find_zonegroup_by_id(local, period, remote_id);
  ASSERT_NE(nullptr, found);
  EXPECT_EQ(remote_id, found->id);
  EXPECT_EQ("remote", found->name);
}

// a request for a bucket in another zonegroup has to be redirected to that
// zonegroup's endpoint. redirecting to the local endpoint sends the client
// back to the gateway it just used, which loops forever
TEST(FindZonegroupById, RedirectTargetIsNotTheLocalEndpoint)
{
  const auto local = make_zonegroup(local_id, "local", {"http://local:80"});
  const auto remote = make_zonegroup(remote_id, "remote", {"http://remote:80"});
  const std::optional<RGWPeriod> period = make_period({local, remote});

  const RGWZoneGroup* bucket_zonegroup =
      rgw::find_zonegroup_by_id(local, period, remote_id);
  ASSERT_NE(nullptr, bucket_zonegroup);

  const std::string endpoint = rgw::get_zonegroup_endpoint(*bucket_zonegroup);
  EXPECT_EQ("http://remote:80", endpoint);
  EXPECT_NE(rgw::get_zonegroup_endpoint(local), endpoint);
}
