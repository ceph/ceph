// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once

/*!
  A Crimson-wise version of the src/common/admin_socket.h

  Running on a single core:
  - the hooks database is only manipulated on that main core. The same applies to the hook-servers.

  (was:
    hook servers running on other cores dispatch the register/unregister requests to that main core).
    Incoming requests arriving on the admin socket are only received on that specific core. The actual
    operation is delegated to the relevant core if needed.
  )
 */
#include <string>
#include <string_view>
#include <map>
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/core/gate.hh"
#include "seastar/core/iostream.hh"
#include "common/cmdparse.h"

class AdminSocket;
class CephContext;

using namespace std::literals;

inline constexpr auto CEPH_ADMIN_SOCK_VERSION = "2"sv;

/*!
  A specific hook must implement exactly one of the two interfaces:
  (1) call(command, cmdmap, format, out)
  or
  (2) exec_command(formatter, command, cmdmap, format, out)

  The default implementation of (1) above calls exec_command() after handling most
  of the boiler-plate choirs:
  - setting up the formmater, with an appropiate 'section' already opened;
  - handling possible failures (exceptions or future_exceptions) returned by (2)
  - flushing the output to the outgoing bufferlist.
*/
class AdminSocketHook {
public:
  /*!
      \retval 'false' for hook execution errors
  */
  virtual seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
		                     std::string_view format, ceph::buffer::list& out) const;

  virtual ~AdminSocketHook() {}

protected:
  virtual seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const {
    return seastar::now();
  }

  /*!
    (customization point) the high-level section of the output is an array
    (affects the JSON formatting)
   */
  virtual bool format_as_array() const {
    return false;
  }

  /*!
    customization point (as some commands expect non-standard response header)
   */
  virtual std::string section_name(std::string_view command) const {
    return std::string{command};
  }
};

/*!
  The details of a single API in a server's hooks block
*/
struct AsokServiceDef {
  const std::string command;   //!< the sequence of words that should be used
  const std::string cmddesc;   //!< the command syntax
  const AdminSocketHook* hook;
  const std::string help;      //!< help message
};

class AdminHooksIter; // gates-controlled iterator over all server blocks

// a ref-count owner of the AdminSocket, used to guarantee its existence until all server-blocks are unregistered
using AdminSocketRef = seastar::lw_shared_ptr<AdminSocket>;

using AsokRegistrationRes = std::optional<AdminSocketRef>;  // holding the server alive until after our unregistration

class AdminSocket : public seastar::enable_lw_shared_from_this<AdminSocket> {
public:
  AdminSocket(CephContext* cct);
  ~AdminSocket();

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator =(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&) = delete;
  AdminSocket& operator =(AdminSocket&&) = delete;

  using hook_server_tag = const void*;

  seastar::future<> init(const std::string& path);

  /*!
   * register an admin socket hooks server
   *
   * The server registers a set of APIs under a common hook_server_tag.
   *
   * The commands block registered by a specific server have a common
   * seastar::gate, used when the server wishes to remove its block's
   * registration.
   *
   * Commands (APIs) are registered under a command string. Incoming
   * commands are split by spaces and matched against the longest
   * registered command. For example, if 'foo' and 'foo bar' are
   * registered, and an incoming command is 'foo bar baz', it is
   * matched with 'foo bar', while 'foo fud' will match 'foo'.
   *
   * The entire incoming command string is passed to the registered
   * hook.
   *
   * \param server_tag  a tag identifying the server registering the hook
   * \param apis_served a vector of the commands served by this server. Each
   *        command registration includes its identifying command string, the
   *        expected call syntax, and some help text.
   *        A note re the help text: if empty, command will not be included in 'help' output.
   *
   * \retval a shared ptr to the asok server itself, or nullopt if
   *         a block with same tag is already registered.
   */

  seastar::future<AsokRegistrationRes>
  register_server(hook_server_tag  server_tag, const std::vector<AsokServiceDef>& apis_served);


  // no single-command unregistration, as en-bulk per server unregistration is the pref method.
  // I will consider adding specific API for those cases where the client needs to disable
  // one specific service. It will be clearly named, to mark the fact that it would not replace
  // deregistration. Something like disable_command(). The point against adding it: will have to
  // make the server-block objects mutable.

  /*!
     unregister all hooks registered by this hooks-server
   */
  seastar::future<> unregister_server(hook_server_tag server_tag);

  seastar::future<> unregister_server(hook_server_tag server_tag, AdminSocketRef&& server_ref);

private:

  class parsed_command_t;
  // and two shorthands:
  using Maybe_parsed =  std::optional<AdminSocket::parsed_command_t>;
  using Future_parsed = seastar::future<Maybe_parsed>;

  /*!
    server_registration() is called by register_server() after acquiring the
    table lock.
  */
  AsokRegistrationRes server_registration(hook_server_tag  server_tag,
                                          const std::vector<AsokServiceDef>& apis_served);

  /*!
    Registering the APIs that are served directly by the admin_socket server.
  */
  seastar::future<AsokRegistrationRes> internal_hooks();

  seastar::future<> init_async(const std::string& path);

  seastar::future<> handle_client(seastar::input_stream<char>& inp, seastar::output_stream<char>& out);

  seastar::future<> execute_line(std::string cmdline, seastar::output_stream<char>& out);

  bool validate_command(const parsed_command_t& parsed,
                        const std::string& command_text,
                        ceph::buffer::list& out) const;

  CephContext* m_cct;
  bool do_die{false};  // RRR when is the OSD expected to kill the asok interface?

  // Note: seems like multiple Context objects are created when calling vstart, and that
  // translates to multiple AdminSocket objects being created. But only the "real" one
  // (the OSD's) is actually initialized by a call to 'init()'.
  // Thus, we should try to discourage the "other" objects from registering hooks.

  std::unique_ptr<AdminSocketHook> version_hook;
  std::unique_ptr<AdminSocketHook> git_ver_hook;
  std::unique_ptr<AdminSocketHook> the0_hook;
  std::unique_ptr<AdminSocketHook> help_hook;
  std::unique_ptr<AdminSocketHook> getdescs_hook;
  std::unique_ptr<AdminSocketHook> test_throw_hook;  // for dev unit-tests

  struct parsed_command_t {
    std::string            m_cmd;
    cmdmap_t               m_parameters;
    std::string            m_format;
    const AsokServiceDef*  m_api;
    const AdminSocketHook* m_hook;
    ::seastar::gate*       m_gate;      //!< the gate of the server-block where the command was located
    /*!
        the length of the whole command-sequence under the 'prefix' header
     */
    std::size_t            m_cmd_seq_len;
  };

  /*!
    parse the incoming command line into the sequence of words that identifies the API,
    and into its arguments.
    Locate the command string in the registered blocks.
  */
  seastar::future<std::optional<AdminSocket::parsed_command_t>> parse_cmd(const std::string command_text);

  struct server_block {
    server_block(const std::vector<AsokServiceDef>& hooks)
      : m_hooks{hooks}
    {}
    const std::vector<AsokServiceDef>& m_hooks;
    mutable ::seastar::gate m_gate;
  };

  // \todo possible improvement: cache all available commands, from all servers, in one vector.
  //  Recreate the list every register/unreg request.

  /*!
    The servers table is protected by a rw-lock, to be acquired exclusively only
    when registering or removing a server.
    Note that the lock is *not* held when executing a specific hook. As the map
    keeps stable item addresses, we do not worry about a specific API block
    disappearing from under our feet (each individual block is protected from removal
    by a seastar::gate). Still, we must guarantee the ability to safely perform
    map lookups.
   */
  seastar::shared_mutex servers_tbl_rwlock;
  std::map<hook_server_tag, server_block> servers;

  struct GateAndHook {
    ::seastar::gate* m_gate;
    const AsokServiceDef* api;
  };

  /*!
    locate_command() will search all servers' control blocks. If found, the
    relevant gate is entered. Returns the AsokServiceDef, and the "activated" gate.
   */
  AdminSocket::GateAndHook locate_command(std::string_view cmd);

  /*!
    Find the longest subset of words in 'match' that is a registered API (in any of the
    servers' control blocks).
    The search is conducted with the servers table RW-lock held.
    If a matching API is found, the
    relevant gate is entered. Returns the AsokServiceDef, and the "activated" gate.

    Note: uses an async seastar::thread to wait for servers_tbl_rwlock. The returned
    future is guaranteed to be available.
   */
  seastar::future<AdminSocket::GateAndHook> locate_subcmd(std::string match);

public:
  /*!
    iterator support
   */
  AdminHooksIter begin();
  AdminHooksIter end();

  using ServersListIt = std::map<hook_server_tag, server_block>::iterator;
  using ServerApiIt =  std::vector<AsokServiceDef>::const_iterator;

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
  friend class AdminHooksIter;
};


/*!
  An iterator over all registered APIs. Each server-block is locked (via its own seastar::gate) before
  iterating over its entries.
*/
struct AdminHooksIter : public std::iterator<std::output_iterator_tag, AsokServiceDef> {
public:
   explicit AdminHooksIter(AdminSocket& master, bool end_flag=false);

  ~AdminHooksIter();

  const AsokServiceDef* operator*() const {
    return &(*m_siter);
  }

  /*!
    The (in)equality test is only used to compare to 'end'.
   */
  bool operator!=(const AdminHooksIter & other) const { return m_in_gate != other.m_in_gate; }

  AdminHooksIter& operator++();

private:
  AdminSocket&                 m_master;
  bool                         m_in_gate{false};
  AdminSocket::ServersListIt   m_miter;
  AdminSocket::ServerApiIt     m_siter;

  friend class AdminSocket;
};
