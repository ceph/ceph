// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mon/ConfigMap.h"

#include <iostream>
#include <string>
#include "crush/CrushWrapper.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "gtest/gtest.h"


TEST(ConfigMap, parse_key)
{
  ConfigMap cm;
  {
    std::string name, who;
    cm.parse_key("global/foo", &name, &who);
    ASSERT_EQ("foo", name);
    ASSERT_EQ("global", who);
  }
  {
    std::string name, who;
    cm.parse_key("mon/foo", &name, &who);
    ASSERT_EQ("foo", name);
    ASSERT_EQ("mon", who);
  }
  {
    std::string name, who;
    cm.parse_key("mon.a/foo", &name, &who);
    ASSERT_EQ("foo", name);
    ASSERT_EQ("mon.a", who);
  }
  {
    std::string name, who;
    cm.parse_key("mon.a/mgr/foo", &name, &who);
    ASSERT_EQ("mgr/foo", name);
    ASSERT_EQ("mon.a", who);
  }
  {
    std::string name, who;
    cm.parse_key("mon.a/a=b/foo", &name, &who);
    ASSERT_EQ("foo", name);
    ASSERT_EQ("mon.a/a=b", who);
  }
  {
    std::string name, who;
    cm.parse_key("mon.a/a=b/c=d/foo", &name, &who);
    ASSERT_EQ("foo", name);
    ASSERT_EQ("mon.a/a=b/c=d", who);
  }
}

TEST(ConfigMap, add_option)
{
  ConfigMap cm;
  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  int r;

  r = cm.add_option(
    cct.get(), "foo", "global", "fooval",
    [&](const std::string& name) {
      return nullptr;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(1, cm.global.options.size());

  r = cm.add_option(
    cct.get(), "foo", "mon", "fooval",
    [&](const std::string& name) {
      return nullptr;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(1, cm.by_type.size());
  ASSERT_EQ(1, cm.by_type["mon"].options.size());
  
  r = cm.add_option(
    cct.get(), "foo", "mon.a", "fooval",
    [&](const std::string& name) {
      return nullptr;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(1, cm.by_id.size());
  ASSERT_EQ(1, cm.by_id["mon.a"].options.size());
}


TEST(ConfigMap, result_sections)
{
  ConfigMap cm;
  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  auto crush = new CrushWrapper;
  crush->finalize();

  int r;

  r = cm.add_option(
    cct.get(), "foo", "global", "g",
    [&](const std::string& name) {
      return nullptr;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(1, cm.global.options.size());

  r = cm.add_option(
    cct.get(), "foo", "mon", "m",
    [&](const std::string& name) {
      return nullptr;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(1, cm.by_type.size());
  ASSERT_EQ(1, cm.by_type["mon"].options.size());

  r = cm.add_option(
    cct.get(), "foo", "mon.a", "a",
    [&](const std::string& name) {
      return nullptr;
    });
  ASSERT_EQ(0, r);
  ASSERT_EQ(1, cm.by_id.size());
  ASSERT_EQ(1, cm.by_id["mon.a"].options.size());

  EntityName n;
  n.set(CEPH_ENTITY_TYPE_MON, "a");
  auto c = cm.generate_entity_map(
    n, {}, crush, "none", nullptr);
  ASSERT_EQ(1, c.size());
  ASSERT_EQ("a", c["foo"]);

  n.set(CEPH_ENTITY_TYPE_MON, "b");
  c = cm.generate_entity_map(
    n, {}, crush, "none", nullptr);
  ASSERT_EQ(1, c.size());
  ASSERT_EQ("m", c["foo"]);

  n.set(CEPH_ENTITY_TYPE_MDS, "c");
  c = cm.generate_entity_map(
    n, {}, crush, "none", nullptr);
  ASSERT_EQ(1, c.size());
  ASSERT_EQ("g", c["foo"]);
}

