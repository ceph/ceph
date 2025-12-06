// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "messages/MMgrDigest.h"
#include "messages/MPGStats.h"
#include "test/mgr/TestMgr.h"
#include "mgr/ClusterState.h"
#include "osd/osd_types.h"
#include "gtest/gtest.h"
#include "mon/PGMap.h"

#include <memory>

TEST(ClusterState, Construct)
{
  MonClient* mon_client = nullptr;
  Objecter* objecter = nullptr;
  MgrMap mgr_map;
  ClusterState cs(mon_client, objecter, mgr_map);

  SUCCEED();
}

TEST(ClusterState, Setters)
{
  MonClient* mon_client = nullptr;
  Objecter* objecter = nullptr;
  MgrMap mgr_map;
  ClusterState cs(mon_client, objecter, mgr_map);
  FSMap fs_map;
  ServiceMap service_map;

  cs.set_objecter(objecter);
  cs.set_fsmap(fs_map);
  cs.set_mgr_map(mgr_map);
  cs.set_service_map(service_map);

  SUCCEED();
}

TEST(ClusterState, LoadDigest)
{
  MonClient* mon_client = nullptr;
  Objecter* objecter = nullptr;
  MgrMap mgr_map;
  ClusterState cs(mon_client, objecter, mgr_map);
  auto digest = ceph::make_message<MMgrDigest>();

  digest->health_json.append("health");
  digest->mon_status_json.append("status");
  cs.load_digest(digest.get());

  SUCCEED();
}
