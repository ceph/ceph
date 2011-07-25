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

#include "common/config_obs.h"

#include <string>

class AdminSocket;
class CephContext;

#define CEPH_ADMIN_SOCK_VERSION 1U

class AdminSocketConfigObs : public md_config_obs_t
{
public:
  AdminSocketConfigObs(CephContext *cct);
  ~AdminSocketConfigObs();
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const md_config_t *conf,
			  const std::set <std::string> &changed);
private:
  AdminSocketConfigObs(const AdminSocketConfigObs& rhs);
  AdminSocketConfigObs& operator=(const AdminSocketConfigObs &rhs);
  bool init(const std::string &path);
  void shutdown();

  CephContext *m_cct;
  AdminSocket* m_thread;
  std::string m_path;
  int m_shutdown_fd;

  friend class AdminSocket;
  friend class AdminSocketTest;
};
