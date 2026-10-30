// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/MgrStandby.h"

TEST_F(MgrStandbyTestHelper, BasicSetup) {
  ASSERT_NE(cct, nullptr);
  ASSERT_NE(py_registry, nullptr);
  ASSERT_NE(clog, nullptr);
  ASSERT_NE(mc, nullptr);
  ASSERT_NE(objecter, nullptr);
  ASSERT_NE(messenger, nullptr);
  ASSERT_NE(cs, nullptr);
  ASSERT_NE(cs->test_get_objecter(), nullptr);
  ASSERT_EQ(mgr_map.epoch, 0);
  ASSERT_GE(osd_map.get_epoch(), 0);
}
