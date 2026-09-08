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

void add_master_zone(RGWZoneGroup& zonegroup, const std::string& zone_id,
                     std::list<std::string> endpoints)
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
