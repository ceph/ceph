// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
  A Crimson-wise version of the src/common/admin_socket.h

  Note: assumed to be running on a single core.
*/
#include <map>
#include <string>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/net/api.hh>

#include "common/cmdparse.h"

using namespace std::literals;

namespace crimson::admin {

class AdminSocket;

/**
 * A specific hook must implement exactly one of the two interfaces:
 * (1) call(command, cmdmap, format, out)
 * or
 * (2) exec_command(formatter, command, cmdmap, format, out)
 *
 * The default implementation of (1) above calls exec_command() after handling
 * most of the boiler-plate choirs:
 * - setting up the formatter, with an appropiate 'section' already opened;
 * - handling possible failures (exceptions or future_exceptions) returned
 *   by (2)
 * - flushing the output to the outgoing bufferlist.
 */
class AdminSocketHook {
 public:
  AdminSocketHook(std::string_view prefix,
		  std::string_view desc,
		  std::string_view help) :
    prefix{prefix}, desc{desc}, help{help}
  {}
  /**
   * \retval 'false' for hook execution errors
   */
  virtual seastar::future<ceph::bufferlist>
  call(std::string_view command,
       std::string_view format,
       const cmdmap_t& cmdmap) const = 0;
  virtual ~AdminSocketHook() {}
  const std::string_view prefix;
  const std::string_view desc;
  const std::string_view help;
};

class AdminSocket : public seastar::enable_lw_shared_from_this<AdminSocket> {
 public:
  AdminSocket() = default;
  ~AdminSocket() = default;

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator=(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&) = delete;
  AdminSocket& operator=(AdminSocket&&) = delete;

  using hook_server_tag = const void*;

  /**
   *  create the async Seastar thread that handles asok commands arriving
   *  over the socket.
   */
  seastar::future<> start(const std::string& path);

  seastar::future<> stop();

  /**
   * register an admin socket hooks server
   *
   * The server registers a set of APIs under a common hook_server_tag.
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
   *
   * A note regarding the help text: if empty, command will not be
   * included in 'help' output.
   *
   * \retval a shared ptr to the asok server itself, or nullopt if
   *         a block with same tag is already registered.
   */
  seastar::future<> register_command(std::unique_ptr<AdminSocketHook>&& hook);

 private:
  /**
   * the result of analyzing an incoming command, and locating it in
   * the registered APIs collection.
   */
  struct parsed_command_t {
    cmdmap_t parameters;
    std::string format;
    const AdminSocketHook& hook;
  };
  // and the shorthand:
  using maybe_parsed_t = std::optional<AdminSocket::parsed_command_t>;

  /**
   * Registering the APIs that are served directly by the admin_socket server.
   */
  seastar::future<> register_admin_hooks();

  seastar::future<> handle_client(seastar::input_stream<char>& inp,
                                  seastar::output_stream<char>& out);

  seastar::future<> execute_line(std::string cmdline,
                                 seastar::output_stream<char>& out);

  seastar::future<> finalize_response(seastar::output_stream<char>& out,
                                      ceph::bufferlist&& msgs);

  bool validate_command(const parsed_command_t& parsed,
                        const std::string& command_text,
                        ceph::bufferlist& out) const;

  /**
   * Non-owning ptr to the UNIX-domain "server-socket".
   * Named here to allow a call to abort_accept().
   */
  seastar::api_v2::server_socket* m_server_sock{ nullptr };

  /**
   * stopping incoming ASOK requests at shutdown
   */
  seastar::gate arrivals_gate;

  /**
   *  parse the incoming command line into the sequence of words that identifies
   *  the API, and into its arguments. Locate the command string in the
   *  registered blocks.
   */
  maybe_parsed_t parse_cmd(std::string command_text, bufferlist& out);

  /**
   *  The servers table is protected by a rw-lock, to be acquired exclusively
   *  only when registering or removing a server.
   *  The lock is locked-shared when executing any hook.
   */
  mutable seastar::shared_mutex servers_tbl_rwlock;
  using hooks_t = std::map<std::string_view, std::unique_ptr<AdminSocketHook>>;
  hooks_t hooks;

 public:
  /**
   * iterator support
   */
  hooks_t::const_iterator begin() const {
    return hooks.cbegin();
  }
  hooks_t::const_iterator end() const {
    return hooks.cend();
  }

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
};

}  // namespace crimson::admin
