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

#include "systest_settings.h"

#include <pthread.h>
#include <sstream>
#include <stdlib.h>

pthread_mutex_t g_system_test_settings_lock = PTHREAD_MUTEX_INITIALIZER;

SysTestSettings& SysTestSettings::
inst()
{
  pthread_mutex_lock(&g_system_test_settings_lock);
  if (!m_inst)
    m_inst = new SysTestSettings();
  pthread_mutex_unlock(&g_system_test_settings_lock);
  return *m_inst;
}

bool SysTestSettings::
use_threads() const
{
  return m_use_threads;
}

std::string SysTestSettings::
get_log_name(const std::string &suffix) const
{
  if (m_log_file_base.empty())
    return "";
  std::ostringstream oss;
  oss << m_log_file_base << "." << suffix;
  return oss.str();
}

SysTestSettings* SysTestSettings::
m_inst = NULL;

SysTestSettings::
SysTestSettings()
{
  m_use_threads = !!getenv("USE_THREADS");
  const char *lfb = getenv("LOG_FILE_BASE");
  if (lfb)
    m_log_file_base.assign(lfb);
}

SysTestSettings::
~SysTestSettings()
{
}
