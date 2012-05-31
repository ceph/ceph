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

#ifndef CEPH_COMMON_ADMIN_SOCKET_H
#define CEPH_COMMON_ADMIN_SOCKET_H

#include "common/Thread.h"
#include "common/Mutex.h"

#include <string>
#include <map>
#include "include/buffer.h"

class AdminSocket;
class CephContext;

#define CEPH_ADMIN_SOCK_VERSION "2"

class AdminSocketHook {
public:
  virtual bool call(std::string command, bufferlist& out) = 0;
  virtual ~AdminSocketHook() {};
};

class AdminSocket : public Thread
{
public:
  AdminSocket(CephContext *cct);
  virtual ~AdminSocket();

  /**
   * register an admin socket command
   *
   * @param command command string
   * @param hook implementaiton
   * @param help help text.  if empty, command will not be included in 'help' output.
   *
   * @return 0 for success, -EEXIST if command already registered.
   */
  int register_command(std::string command, AdminSocketHook *hook, std::string help);

  /**
   * unregister an admin socket command
   *
   * @param command command string
   * @return 0 on succest, -ENOENT if command dne.
   */
  int unregister_command(std::string command);

  bool init(const std::string &path);
  
private:
  AdminSocket(const AdminSocket& rhs);
  AdminSocket& operator=(const AdminSocket &rhs);

  void shutdown();

  std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr);
  std::string bind_and_listen(const std::string &sock_path, int *fd);

  void *entry();
  bool do_accept();

  CephContext *m_cct;
  std::string m_path;
  int m_sock_fd;
  int m_shutdown_rd_fd;
  int m_shutdown_wr_fd;

  Mutex m_lock;    // protects m_hooks, m_help
  AdminSocketHook *m_version_hook, *m_help_hook;

  std::map<std::string,AdminSocketHook*> m_hooks;
  std::map<std::string,std::string> m_help;

  friend class AdminSocketTest;
  friend class HelpHook;
};

#endif
