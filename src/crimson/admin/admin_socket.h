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
#include "include/buffer.h"
#include "crimson/net/Fwd.h"

using namespace std::literals;

class MCommand;

namespace crimson::admin {

class AdminSocket;

struct tell_result_t {
  int ret = 0;
  std::string err;
  ceph::bufferlist out;
  tell_result_t() = default;
  tell_result_t(int ret, std::string&& err);
  tell_result_t(int ret, std::string&& err, ceph::bufferlist&& out);
  /**
   * create a \c tell_result_t indicating the successful completion
   * of command
   *
   * \param formatter the content of formatter will be flushed to the
   *        output buffer
   */
  tell_result_t(std::unique_ptr<Formatter> formatter);
};

/**
 * An abstract class to be inherited by implementations of asock hooks
 */
class AdminSocketHook {
 public:
  AdminSocketHook(std::string_view prefix,
		  std::string_view desc,
		  std::string_view help) :
    prefix{prefix}, desc{desc}, help{help}
  {}
  /**
   * handle command defined by cmdmap
   *
   * \param cmdmap dictionary holding the named parameters
   * \param format the expected format of the output
   * \param input the binary input of the command
   * \pre \c cmdmap should be validated with \c desc
   * \retval an instance of \c tell_result_t
   * \note a negative \c ret should be set to indicate that the hook fails to
   *       fulfill the command either because of an invalid input or other
   *       failures. in that case, a brief reason of the failure should
   *       noted in \c err in the returned value
   */
  virtual seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
					      std::string_view format,
					      ceph::bufferlist&& input) const = 0;
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

  /**
   *  create the async Seastar thread that handles asok commands arriving
   *  over the socket.
   */
  seastar::future<> start(const std::string& path);

  seastar::future<> stop();

  /**
   * register an admin socket hook
   *
   * Commands (APIs) are registered under a command string. Incoming
   * commands are split by spaces and matched against the longest
   * registered command. For example, if 'foo' and 'foo bar' are
   * registered, and an incoming command is 'foo bar baz', it is
   * matched with 'foo bar', while 'foo fud' will match 'foo'.
   *
   * \param hook a hook which includes its identifying command string, the
   *        expected call syntax, and some help text.
   *
   * A note regarding the help text: if empty, command will not be
   * included in 'help' output.
   */
  void register_command(std::unique_ptr<AdminSocketHook>&& hook);

  /**
   * Registering the APIs that are served directly by the admin_socket server.
   */
  void register_admin_commands();
  /**
   * handle a command message by replying an MCommandReply with the same tid
   *
   * \param conn connection over which the incoming command message is received
   * \param m message carrying the command vector and optional input buffer
   */
  seastar::future<> handle_command(crimson::net::ConnectionRef conn,
				   boost::intrusive_ptr<MCommand> m);

private:
  /**
   * the result of analyzing an incoming command, and locating it in
   * the registered APIs collection.
   */
  struct parsed_command_t {
    cmdmap_t params;
    std::string format;
    const AdminSocketHook& hook;
  };
  // and the shorthand:
  seastar::future<> handle_client(seastar::input_stream<char>& inp,
                                  seastar::output_stream<char>& out);

  seastar::future<> execute_line(std::string cmdline,
                                 seastar::output_stream<char>& out);

  seastar::future<> finalize_response(seastar::output_stream<char>& out,
                                      ceph::bufferlist&& msgs);

  seastar::future<tell_result_t> execute_command(const std::vector<std::string>& cmd,
						 ceph::bufferlist&& buf);

  std::optional<seastar::future<>> task;
  std::optional<seastar::server_socket> server_sock;
  std::optional<seastar::connected_socket> connected_sock;

  /**
   * stopping incoming ASOK requests at shutdown
   */
  seastar::gate stop_gate;

  /**
   * parse the incoming command vector, find a registered hook by looking up by
   * its prefix, perform sanity checks on the parsed parameters with the hook's
   * command description
   *
   * \param cmd a vector of string which presents a command
   * \retval on success, a \c parsed_command_t is returned, tell_result_t with
   *         detailed error messages is returned otherwise
   */
  std::variant<parsed_command_t, tell_result_t>
  parse_cmd(const std::vector<std::string>& cmd);

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
};

}  // namespace crimson::admin
