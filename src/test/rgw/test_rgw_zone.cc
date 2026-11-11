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
#include "driver/rados/rgw_sal_rados.h"

#include <list>
#include <string>
#include <vector>
#include <iterator>
#include <optional>

#include <gtest/gtest.h>

namespace {

const std::string local_id = "74506436-cfb6-4105-8a8c-edd0c62632b7";
const std::string remote_id = "70271812-eb5c-472a-9050-16e289e78941";

RGWZoneGroup make_zonegroup(const std::string& id, const std::string& name,
                            std::vector<std::string> endpoints)
{
  RGWZoneGroup zonegroup{id, name};
  zonegroup.endpoints = std::move(endpoints);
  return zonegroup;
}

void add_master_zone(RGWZoneGroup& zonegroup, const std::string& zone_id,
                     std::vector<std::string> endpoints)
{
  zonegroup.master_zone = zone_id;
  RGWZone& zone = zonegroup.zones[zone_id];
  zone.id = zone_id;
  zone.endpoints = std::move(endpoints);
}

RGWPeriod make_period(std::initializer_list<RGWZoneGroup> zonegroups)
{
  RGWPeriod period{"period-id"};
  for (const auto& zonegroup : zonegroups) {
    period.period_map.zonegroups[zonegroup.id] = zonegroup;
  }
  return period;
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

TEST(ZoneEncoding, StringSequencesRemainCompatible)
{
  const std::list<std::string> legacy {"s3.example", "objects.example"};
  const std::vector<std::string> contiguous(std::begin(legacy), std::end(legacy));
  bufferlist legacy_encoding;
  bufferlist contiguous_encoding;

  encode(legacy, legacy_encoding);
  encode(contiguous, contiguous_encoding);

  ASSERT_TRUE(legacy_encoding.contents_equal(contiguous_encoding));

  auto encoded = std::cbegin(legacy_encoding);
  std::vector<std::string> decoded;
  decode(decoded, encoded);

  EXPECT_EQ(contiguous, decoded);
  EXPECT_EQ(0, encoded.get_remaining());
}

TEST(ZonegroupEncoding, HostnamesRoundTripWithFollowingFields)
{
  RGWZoneGroup expected {"zonegroup-id", "zonegroup-name"};
  expected.hostnames = {"s3.example", "objects.example"};
  expected.hostnames_s3website = {"web.example"};
  expected.realm_id = "realm-id";
  expected.enabled_features.insert("resharding");
  bufferlist encoded;
  encode(expected, encoded);

  RGWZoneGroup decoded;
  auto input = std::cbegin(encoded);
  decode(decoded, input);

  EXPECT_EQ(expected.hostnames, decoded.hostnames);
  EXPECT_EQ(expected.hostnames_s3website, decoded.hostnames_s3website);
  EXPECT_EQ(expected.realm_id, decoded.realm_id);
  EXPECT_EQ(expected.enabled_features, decoded.enabled_features);
  EXPECT_EQ(0, input.get_remaining());
}

TEST(ZoneEndpointEncoding, RoundTripsZoneAndZonegroupFields)
{
  RGWZoneGroup expected {"zonegroup-id", "zonegroup-name"};
  expected.endpoints = {"https://zonegroup.example"};
  expected.realm_id = "realm-id";
  expected.enabled_features.insert("resharding");

  auto& zone = expected.zones[rgw_zone_id {"zone-id"}];
  zone.id = "zone-id";
  zone.name = "zone-name";
  zone.endpoints = {"https://zone.example"};
  zone.redirect_zone = "redirect-zone";
  zone.supported_features.insert("compress-encrypted");

  bufferlist encoded;
  encode(expected, encoded);

  RGWZoneGroup decoded;
  auto input = std::cbegin(encoded);
  decode(decoded, input);

  EXPECT_EQ(expected.endpoints, decoded.endpoints);
  EXPECT_EQ(expected.realm_id, decoded.realm_id);
  EXPECT_EQ(expected.enabled_features, decoded.enabled_features);

  const auto& decoded_zone = decoded.zones.at(rgw_zone_id {"zone-id"});
  EXPECT_EQ(zone.endpoints, decoded_zone.endpoints);
  EXPECT_EQ(zone.redirect_zone, decoded_zone.redirect_zone);
  EXPECT_EQ(zone.supported_features, decoded_zone.supported_features);
  EXPECT_EQ(0, input.get_remaining());
}

TEST(SALZonegroup, ExposesHostnamesAndZoneIds)
{
  RGWZoneGroup config {"zonegroup-id", "zonegroup-name"};
  config.hostnames = {"s3.example", "objects.example"};
  config.hostnames_s3website = {"web.example"};
  config.zones[rgw_zone_id {"zone-a"}].id = "zone-a";
  config.zones[rgw_zone_id {"zone-b"}].id = "zone-b";
  rgw::sal::RadosZoneGroup zonegroup {nullptr, config};

  EXPECT_EQ((std::vector<std::string> {"s3.example", "objects.example"}),
            zonegroup.get_hostnames());
  EXPECT_EQ((std::vector<std::string> {"web.example"}),
            zonegroup.get_s3website_hostnames());
  EXPECT_EQ((std::vector<std::string> {"zone-a", "zone-b"}),
            zonegroup.list_zones());
}
