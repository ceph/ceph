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
#include "common/Thread.h"

#include <string>
#include "include/buffer.h"

class AdminSocket;
class CephContext;

#define CEPH_ADMIN_SOCK_VERSION 1U

class AdminSocket : public Thread, public md_config_obs_t
{
public:
  AdminSocket(CephContext *cct);
  virtual ~AdminSocket();

  // md_config_obs_t
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const md_config_t *conf,
				  const std::set <std::string> &changed);

private:
  AdminSocket(const AdminSocket& rhs);
  AdminSocket& operator=(const AdminSocket &rhs);

  bool init(const std::string &path);
  void shutdown();

  std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr);
  std::string bind_and_listen(const std::string &sock_path, int *fd);

  void *entry();
  bool do_accept();

  bool handle_version_request(int connection_fd);
  bool handle_json_request(int connection_fd, bool schema);

  CephContext *m_cct;
  std::string m_path;
  int m_sock_fd;
  int m_shutdown_rd_fd;
  int m_shutdown_wr_fd;

  friend class AdminSocketTest;
};


