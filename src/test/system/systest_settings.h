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

#ifndef CEPH_SYSTEM_TEST_SETTINGS_H
#define CEPH_SYSTEM_TEST_SETTINGS_H

/* Singleton with settings grabbed from environment variables */
class SysTestSettings
{
public:
  static SysTestSettings& inst();
  bool use_threads() const;
private:
  static SysTestSettings* m_inst;
  SysTestSettings();
  ~SysTestSettings();

  bool m_use_threads;
};

#endif
