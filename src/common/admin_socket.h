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

#if defined(WITH_SEASTAR) && !defined(WITH_ALIEN)
#include "crimson/admin/admin_socket.h"
#else

#include <condition_variable>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "include/buffer.h"
#include "include/common_fwd.h"
#include "common/ref.h"
#include "common/cmdparse.h"

class MCommand;
class MMonCommand;

inline constexpr auto CEPH_ADMIN_SOCK_VERSION = std::string_view("2");

class AdminSocketHook {
public:
  /**
   * @brief
   * Handler for admin socket commands, synchronous version
   *
   * Executes action associated with admin command and returns byte-stream output @c out.
   * There is no restriction on output. Each handler defines output semantics.
   * Typically output is textual representation of some ceph's internals.
   * Handler should use provided formatter @c f if structuralized output is being produced.
   *
   * @param command[in] String matching constant part of cmddesc in @ref AdminSocket::register_command
   * @param cmdmap[in]  Parameters extracted from argument part of cmddesc in @ref AdminSocket::register_command
   * @param f[in]       Formatter created according to requestor preference, used by `ceph --format`
   * @param errss[out]  Error stream, should contain details in case of execution failure
   * @param out[out]    Produced output
   *
   * @retval 0 Success, errss is ignored and does not contribute to output
   * @retval <0 Error code, errss is prepended to @c out
   *
   * @note If @c out is empty, then admin socket will try to flush @c f to out.
   */
  virtual int call(
    std::string_view command,
    const cmdmap_t& cmdmap,
    const ceph::buffer::list& inbl,
    ceph::Formatter *f,
    std::ostream& errss,
    ceph::buffer::list& out) = 0;

  /**
   * @brief
   * Handler for admin socket commands, asynchronous version
   *
   * Executes action associated with admin command and prepares byte-stream response.
   * When processing is done @c on_finish must be called.
   * There is no restriction on output. Each handler defines own output semantics.
   * Typically output is textual representation of some ceph's internals.
   * Input @c inbl can be passed, see ceph --in-file.
   * Handler should use provided formatter @c f if structuralized output is being produced.
   * on_finish handler has following parameters:
   * - result code of handler (same as @ref AdminSocketHook::call)
   * - error message, text
   * - output
   *
   * @param[in] command String matching constant part of cmddesc in @ref AdminSocket::register_command
   * @param[in] cmdmap  Parameters extracted from argument part of cmddesc in @ref AdminSocket::register_command
   * @param[in] f       Formatter created according to requestor preference, used by `ceph --format`
   * @param[in] inbl    Input content for handler
   * @param[in] on_finish Function to call when processing is done
   *
   * @note If @c out is empty, then admin socket will try to flush @c f to out.
   */
  virtual void call_async(
    std::string_view command,
    const cmdmap_t& cmdmap,
    ceph::Formatter *f,
    const ceph::buffer::list& inbl,
    std::function<void(int,const std::string&,ceph::buffer::list&)> on_finish) {
    // by default, call the synchronous handler and then finish
    ceph::buffer::list out;
    std::ostringstream errss;
    int r = call(command, cmdmap, inbl, f, errss, out);
    on_finish(r, errss.str(), out);
  }
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
  int register_command(std::string_view cmddesc,
		       AdminSocketHook *hook,
		       std::string_view help);

  /*
   * unregister all commands belong to hook.
   */
  void unregister_commands(const AdminSocketHook *hook);

  bool init(const std::string& path);

  void chown(uid_t uid, gid_t gid);
  void chmod(mode_t mode);

  /// execute (async)
  void execute_command(
    const std::vector<std::string>& cmd,
    const ceph::buffer::list& inbl,
    std::function<void(int,const std::string&,ceph::buffer::list&)> on_fin);

  /// execute (blocking)
  int execute_command(
    const std::vector<std::string>& cmd,
    const ceph::buffer::list& inbl,
    std::ostream& errss,
    ceph::buffer::list *outbl);

  void queue_tell_command(ceph::cref_t<MCommand> m);
  void queue_tell_command(ceph::cref_t<MMonCommand> m); // for compat

private:

  void shutdown();
  void wakeup();

  std::string create_wakeup_pipe(int *pipe_rd, int *pipe_wr);
  std::string destroy_wakeup_pipe();
  std::string bind_and_listen(const std::string &sock_path, int *fd);

  std::thread th;
  void entry() noexcept;
  void do_accept();
  void do_tell_queue();

  CephContext *m_cct;
  std::string m_path;
  int m_sock_fd = -1;
  int m_wakeup_rd_fd = -1;
  int m_wakeup_wr_fd = -1;
  bool m_shutdown = false;

  bool in_hook = false;
  std::condition_variable in_hook_cond;
  std::mutex lock;  // protects `hooks`
  std::unique_ptr<AdminSocketHook> version_hook;
  std::unique_ptr<AdminSocketHook> help_hook;
  std::unique_ptr<AdminSocketHook> getdescs_hook;
  std::unique_ptr<AdminSocketHook> raise_hook;

  std::mutex tell_lock;
  std::list<ceph::cref_t<MCommand>> tell_queue;
  std::list<ceph::cref_t<MMonCommand>> tell_legacy_queue;

  struct hook_info {
    AdminSocketHook* hook;
    std::string desc;
    std::string help;

    hook_info(AdminSocketHook* hook, std::string_view desc,
	      std::string_view help)
      : hook(hook), desc(desc), help(help) {}
  };

  /// find the first hook which matches the given prefix and cmdmap
  std::pair<int, AdminSocketHook*> find_matched_hook(
    std::string& prefix,
    const cmdmap_t& cmdmap);

  std::multimap<std::string, hook_info, std::less<>> hooks;

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
};

#endif
#endif
