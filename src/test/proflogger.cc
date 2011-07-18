// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_context.h"
#include "common/ProfLogger.h"

#include "test/unit.h"

class ProfLoggerCollectionTest
{
public:
  ProfLoggerCollectionTest(ProfLoggerCollection *coll)
    : m_coll(coll)
  {
  }
  bool init(const std::string &uri) {
    Mutex::Locker lock(m_coll->m_lock);
    if (m_coll->m_thread != NULL) {
      return false;
    }
    return m_coll->init(uri);
  }
  bool shutdown() {
    Mutex::Locker lock(m_coll->m_lock);
    m_coll->shutdown();
    return (m_coll->m_thread == NULL);
  }
private:
  ProfLoggerCollection *m_coll;
};

TEST(ProfLogger, SetupTeardown) {
  ProfLoggerCollectionTest plct(g_ceph_context->GetProfLoggerCollection());
  int ret = plct.shutdown();
  ASSERT_EQ(ret, true);
}
