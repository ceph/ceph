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

#include <condition_variable>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "include/buffer.h"
#include "common/cmdparse.h"

class AdminSocket;
class CephContext;

using namespace std::literals;

inline constexpr auto CEPH_ADMIN_SOCK_VERSION = "2"sv;

class AdminSocketHook {
public:
  virtual bool call(std::string_view command, const cmdmap_t& cmdmap,
		    std::string_view format, ceph::buffer::list& out) = 0;
  virtual ~AdminSocketHook() {}
};

class AdminSocket
{
public:
  AdminSocket(CephContext *cct);
  ~AdminSocket();

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator =(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&) = delete;
  AdminSocket& operator =(AdminSocket&&) = delete;

  /**
   * register an admin socket command
   *
   * The command is registered under a command string.  Incoming
   * commands are split by space and matched against the longest
   * registered command.  For example, if 'foo' and 'foo bar' are
   * registered, and an incoming command is 'foo bar baz', it is
   * matched with 'foo bar', while 'foo fud' will match 'foo'.
   *
   * The entire incoming command string is passed to the registered
   * hook.
   *
   * @param command command string
   * @param cmddesc command syntax descriptor
   * @param hook implementation
   * @param help help text.  if empty, command will not be included in 'help' output.
   *
   * @return 0 for success, -EEXIST if command already registered.
   */
  int register_command(std::string_view command,
		       std::string_view cmddesc,
		       AdminSocketHook *hook,
		       std::string_view help);

  /**
   * unregister an admin socket command.
   *
   * If a command is currently in progress, this will block until it
   * is done.  For that reason, you must not hold any locks required
   * by your hook while you call this.
   *
   * @param command command string
   * @return 0 on succest, -ENOENT if command dne.
   */
  int unregister_command(std::string_view command);

  /*
   * unregister all commands belong to hook.
   */
  void unregister_commands(const AdminSocketHook *hook);

  bool init(const std::string& path);

  void chown(uid_t uid, gid_t gid);
  void chmod(mode_t mode);

private:

  void shutdown();

  std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr);
  std::string destroy_shutdown_pipe();
  std::string bind_and_listen(const std::string &sock_path, int *fd);

  std::thread th;
  void entry() noexcept;
  bool do_accept();
  bool validate(const std::string& command,
		const cmdmap_t& cmdmap,
		ceph::buffer::list& out) const;

  CephContext *m_cct;
  std::string m_path;
  int m_sock_fd = -1;
  int m_shutdown_rd_fd = -1;
  int m_shutdown_wr_fd = -1;

  bool in_hook = false;
  std::condition_variable in_hook_cond;
  std::mutex lock;  // protects `hooks`
  std::unique_ptr<AdminSocketHook> version_hook;
  std::unique_ptr<AdminSocketHook> help_hook;
  std::unique_ptr<AdminSocketHook> getdescs_hook;

  struct hook_info {
    AdminSocketHook* hook;
    std::string desc;
    std::string help;

    hook_info(AdminSocketHook* hook, std::string_view desc,
	      std::string_view help)
      : hook(hook), desc(desc), help(help) {}
  };

  std::map<std::string, hook_info, std::less<>> hooks;

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
};

#endif
