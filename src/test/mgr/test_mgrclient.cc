// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/MgrClient.h"
#include "mon/MonMap.h"

TEST_F(TestMgr, MgrClientBasicSetup) {
  ASSERT_NE(mc, nullptr);
  ASSERT_NE(objecter, nullptr);
  ASSERT_NE(messenger, nullptr);

  MonMap monmap;
  MgrClient client(cct.get(), messenger.get(), &monmap);
  ASSERT_FALSE(client.is_initialized());

  client.init();
  ASSERT_TRUE(client.is_initialized());

  client.shutdown();
}
