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

#include "common/version.h"
#include "crimson/net/Socket.h"
// activate for some inline unit-testing: #define UNIT_TEST_OSD_ASOK
#include "crimson/admin/admin_socket.h"
#include "crimson/common/log.h"
//#include "seastar/testing/test_case.hh"
#include "seastar/net/api.hh"
#include "seastar/net/inet_address.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/thread.hh"
//#include "seastar/util/log.hh"
#include "seastar/util/std-compat.hh"
#include <boost/algorithm/string.hpp>

/*!
  A Crimson-wise version of the admin socket - implementation file

  \todo handle the unlinking of the admin socket. Note that 'cleanup_files' at-exit functionality is not yet
        implemented in Crimson
*/

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

#ifdef UNIT_TEST_OSD_ASOK
namespace asok_unit_testing {
  seastar::future<> utest_run_1(AdminSocket* asok);
}
#endif

/*!
  Hooks table - iterator support

  Note: Each server-block is locked (via its gate member) before
        iterating over its entries.
 */

AdminHooksIter AdminSocket::begin() {
  AdminHooksIter it{*this};
  it.m_miter->second.m_gate.enter();
  it.m_in_gate = true;
  return it;
}

AdminHooksIter AdminSocket::end() {
  AdminHooksIter it{*this, true};
  return it;
}

/*!
  note that 'end-flag' is an optional parameter, and is only used internally (to
  signal the end() state).
 */
AdminHooksIter::AdminHooksIter(AdminSocket& master, bool end_flag)
    : m_master{master}
{
  if (end_flag) {
    // create the equivalent of servers.end();
    m_miter = m_master.servers.end();
  } else {
    m_miter = m_master.servers.begin();
    m_siter = m_miter->second.m_hooks.begin();
  }
}

AdminHooksIter& AdminHooksIter::operator++()
{
  ++m_siter;
  if (m_siter == m_miter->second.m_hooks.end()) {
    // move to the next server-block
    m_miter->second.m_gate.leave();
    m_in_gate = false;

    while (true) {
      m_miter++;
      if (m_miter == m_master.servers.end())
        return *this;

      try {
        m_miter->second.m_gate.enter(); // will throw if gate was already closed
        m_siter = m_miter->second.m_hooks.begin();
        m_in_gate = true;
        return *this;
      } catch (...) {
        // this server-block is being torn-down, and cannot be used
        continue;
      }
    }
  }
  return *this;
}

AdminHooksIter::~AdminHooksIter()
{
  if (m_in_gate) {
    m_miter->second.m_gate.leave();
  }
}

/*!
  the defaut implementation of the hook API

  Note that we never throw or return a failed future.
 */
seastar::future<bool> AdminSocketHook::call(std::string_view command, const cmdmap_t& cmdmap,
                                            std::string_view format, ceph::bufferlist& out) const
{
  unique_ptr<Formatter> f{Formatter::create(format, "json-pretty"sv, "json-pretty"s)};

  // customize the output section, as required by a couple of hook APIs:
  std::string section{section_name(command)};
  boost::replace_all(section, " ", "_");
  if (format_as_array()) {
    f->open_array_section(section.c_str());
  } else {
    f->open_object_section(section.c_str());
  }

  bool bres{false};

  /*
      call the command-specific hook.
      A note re error handling:
        - will be modified to use the new 'erroretor'. For now:
        - exec_command() may throw or return an exceptional future. We return a message that starts
          with "error" on both failure scenarios.
   */
  return seastar::do_with(std::move(f), std::move(bres), [this, &command, &cmdmap, &format, &out](unique_ptr<Formatter>& ftr, bool& br) {

    return seastar::futurize_apply([this, &command, &cmdmap, &format, &out, f=ftr.get()] {
      return exec_command(f, command, cmdmap, format, out);
    }).
      // get us to a resolved state:
      then_wrapped([&ftr,&command,&br](seastar::future<> res) -> seastar::future<bool> {

      //  we now have a ready future (or a failure)
      if (!res.failed()) {
        br = true;
      }
      res.ignore_ready_future();
      return seastar::make_ready_future<bool>(br);
    }).then([this, &ftr, &out](auto res) -> seastar::future<bool> {
      ftr->close_section();
      ftr->enable_line_break();
      ftr->flush(out);
      return seastar::make_ready_future<bool>(res);
    });
  });
}


AdminSocket::AdminSocket(CephContext *cct)
  : m_cct(cct)
{}

AdminSocket::~AdminSocket()
{}

/*!
  Note re context: running in the core running the asok.
  And no futurization until required to support multiple cores.
*/
AsokRegistrationRes AdminSocket::server_registration(AdminSocket::hook_server_tag server_tag,
                                 const std::vector<AsokServiceDef>& apis_served)
{
  auto ne = servers.try_emplace(
                                server_tag,
                                apis_served);

  //  is this server tag already registered?
  if (!ne.second) {
    return std::nullopt;
  }

  logger().info("{}: {} server registration (tag: {})", __func__, (int)getpid(), (uint64_t)(server_tag));
  return this->shared_from_this();
}

seastar::future<AsokRegistrationRes>
AdminSocket::register_server(hook_server_tag  server_tag, const std::vector<AsokServiceDef>& apis_served)
{
  return seastar::with_lock(servers_tbl_rwlock, [this, server_tag, &apis_served]() {
    return server_registration(server_tag, apis_served);
  });
}


/*!
  Called by the server implementing the hook. The gate will be closed, and the function
  will block until all execution of commands within the gate are done.
 */
seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag)
{
  logger().debug("{}: {} server un-reg (tag: {}) ({} prev cnt: {})",
                __func__, (int)getpid(), (uint64_t)(server_tag), (uint64_t)(this), servers.size());

  //  locate the server registration
  return seastar::with_shared(servers_tbl_rwlock, [this, server_tag]() {

    auto srv_itr = servers.find(server_tag);
    return srv_itr;

  }).then([this, server_tag](auto srv_itr) {

    if (srv_itr == servers.end()) {
      logger().warn("{}: unregistering a non-existing registration (tag: {})", __func__, (uint64_t)(server_tag));
      // list what servers *are* there (for debugging). Should be under the lock
      //for (auto& [k, d] : servers) {
      //  logger().debug("---> server: {} {}", (uint64_t)(k), d.m_hooks.front().command);
      //}
      return seastar::now();

    } else {

      // note: usually we would have liked to maintain the shared lock, and just upgrade it here
      // to an exclusive one. But there is no chance of anyone else removing our specific server-block
      // before we re-acquire the lock

      return (srv_itr->second.m_gate.close()).then([this, srv_itr]() {
        return with_lock(servers_tbl_rwlock, [this, srv_itr]() {
          servers.erase(srv_itr);
          return seastar::now();
        });
      });
    }
  }).finally([server_tag]() {
    logger().info("{}: done removing server block {}", __func__, (uint64_t)(server_tag));
  });
}

seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag, AdminSocketRef&& server_ref)
{
  // reducing the ref-count on us (the asok server) by discarding server_ref:
  return seastar::do_with(std::move(server_ref), [this, server_tag](auto& srv) {
    return unregister_server(server_tag).finally([this,server_tag,&srv]() {
      logger().debug("{}: {} - {}", __func__, (uint64_t)(server_tag), srv->servers.size());
    });
  });
}

AdminSocket::GateAndHook AdminSocket::locate_command(std::string_view cmd)
{
  for (auto& [tag, srv] : servers) {
    // "lock" the server's control block before searching for the command string
    try {
      srv.m_gate.enter();
    } catch (...) {
      // probable error: gate is already closed
      continue;
    }
    for (auto& api : srv.m_hooks) {
      if (api.command == cmd) {
        logger().info("{}: located {} w/ server {}", __func__, cmd, tag);
        // note that the gate was entered!
        return AdminSocket::GateAndHook{&srv.m_gate, &api};
      }
    }
    // not registered by this server. Close this server's gate.
    srv.m_gate.leave();
  }

  return AdminSocket::GateAndHook{nullptr, nullptr};
}

seastar::future<AdminSocket::GateAndHook> AdminSocket::locate_subcmd(std::string match)
{
  return seastar::with_shared(servers_tbl_rwlock, [this, match]() mutable {

    AdminSocket::GateAndHook gh;

    while (match.size()) {
      gh = locate_command(match);

      if (gh.api) {
        // found a match
        break;
      }

      // drop right-most word
      size_t pos = match.rfind(' ');
      if (pos == std::string::npos) {
        match.clear();  // we fail
        break;
      } else {
        match.resize(pos);
      }
    }

    return gh;
  });
}

seastar::future<std::optional<AdminSocket::parsed_command_t>> AdminSocket::parse_cmd(const std::string command_text)
{
  /*
    preliminaries:
      - create the formatter specified by the command_text parameters
      - locate the "op-code" string (the 'prefix' segment)
      - prepare for command parameters extraction via cmdmap_t
  */
  cmdmap_t cmdmap;
  vector<string> cmdvec;
  stringstream errss;

  //  note that cmdmap_from_json() may throw on syntax issues
  cmdvec.push_back(command_text); // as cmdmap_from_json() likes the input in this format
  try {
    if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
      logger().error("{}: incoming command error: {}", __func__, errss.str()); // RRR verify errrss contents
      return seastar::make_ready_future<Maybe_parsed>(Maybe_parsed{std::nullopt});
    }
  } catch ( ... ) {
    logger().error("{}: incoming command syntax: {}", __func__, command_text);
    return seastar::make_ready_future<Maybe_parsed>(Maybe_parsed{std::nullopt});
  }

  string format;
  string match;
  std::size_t full_command_seq;
  try {
    cmd_getval(m_cct, cmdmap, "format", format);
    cmd_getval(m_cct, cmdmap, "prefix", match);
    full_command_seq = match.length(); // the full sequence, before we start chipping away the end
  } catch (const bad_cmd_get& e) {
    return seastar::make_ready_future<Maybe_parsed>(Maybe_parsed{std::nullopt});
  }

  if (!match.length()) {
    // no command identified
    return seastar::make_ready_future<Maybe_parsed>(Maybe_parsed{std::nullopt});
  }

  if (format != "json"s && format != "json-pretty"s &&
      format != "xml"s && format != "xml-pretty"s) {
    format = "json-pretty"s;
  }

  /*
      match the incoming op-code to one of the registered APIs (and lock the block containing that API from
          de-registration)
  */
  return seastar::do_with(std::move(cmdmap), std::move(format), std::move(match),
                         [this, full_command_seq](auto&& cmdmap, auto&& format, auto&& match) {
    return locate_subcmd(match).then_wrapped([this, &match, &cmdmap, format, full_command_seq](auto&& fgh) {

      if (fgh.failed()) {
        return seastar::make_ready_future<Maybe_parsed>(Maybe_parsed{std::nullopt});
      } else {
        AdminSocket::GateAndHook gh = fgh.get0();
        if (!gh.api)
           return seastar::make_ready_future<Maybe_parsed>(Maybe_parsed{std::nullopt});
        else
          return seastar::make_ready_future<Maybe_parsed>(
                  parsed_command_t{match, cmdmap, format, gh.api, gh.api->hook, gh.m_gate, full_command_seq});
      }
    });
  });
}

bool AdminSocket::validate_command(const parsed_command_t& parsed,
                                   const std::string& command_text,
                                   ceph::buffer::list& out) const
{
  // did we receive any arguments apart from the command word(s)?
  //logger().debug("ct:{} {} origl:{} pmc:{} pmcl:{}", command_text, command_text.length(), parsed.m_cmd_seq_len, parsed.m_cmd, parsed.m_cmd.length());
  if (parsed.m_cmd_seq_len == parsed.m_cmd.length())
    return true;

  logger().debug("{}: validating {} against:{}", __func__, command_text, parsed.m_api->cmddesc);

  stringstream os; // for possible validation error messages
  try {
    // validate_cmd throws on some syntax errors
    if (validate_cmd(m_cct, parsed.m_api->cmddesc, parsed.m_parameters, os)) {
      return true;
    }
  } catch( std::exception& e ) {
    logger().error("{}: validation failure ({} : {}) {}", __func__, command_text, parsed.m_cmd, e.what());
  }

  logger().error("{}: validation failure (incoming:{}) {}", __func__, command_text, os.str());
  out.append(os);
  return false;
}

seastar::future<> AdminSocket::execute_line(std::string cmdline, seastar::output_stream<char>& out)
{
  return parse_cmd(cmdline).then([&out, cmdline, this](std::optional<AdminSocket::parsed_command_t> parsed) {

    if (parsed.has_value()) {

      return seastar::do_with(std::move(parsed), ::ceph::bufferlist(),
                            [&out, cmdline, this, gatep = parsed->m_gate] (auto&& parsed, auto&& out_buf) {

      return ((parsed->m_api && validate_command(*parsed, cmdline, out_buf)) ?

              parsed->m_hook->call(parsed->m_cmd, parsed->m_parameters, (*parsed).m_format, out_buf) :
              seastar::make_ready_future<bool>(false) /* failed args syntax validation */
        ).
        then_wrapped([&out, &out_buf, gatep](auto&& fut) -> seastar::future<bool> {

          gatep->leave();

          if (fut.failed()) {
            // add 'failed' to the contents of out_buf? not what happens in the old code
            return seastar::make_ready_future<bool>(false);
          } else {
            return std::move(fut);
          }
        }).then([&out, &out_buf, gatep](auto call_res) {

          string outbuf_cont = out_buf.to_str();
          uint32_t response_length = htonl(outbuf_cont.length());
          logger().info("asok response length: {}", outbuf_cont.length());

          return out.write((char*)&response_length, sizeof(uint32_t)).then([&out, outbuf_cont]() {

            if (outbuf_cont.empty())
              return out.write("    ");
            else
              return out.write(outbuf_cont.c_str());
          });
        });
      });

    } else {

      // no command op-code identified
      std::string no_opcode = "error: no command identified in incoming message ("s + cmdline + ")"s;
      uint32_t response_length = htonl(no_opcode.length());
      logger().info("asok f len: {}", no_opcode.length());

      return out.write((char*)&response_length, sizeof(uint32_t)).
        then([&out, no_opcode, response_length]() {
          if (response_length)
            return out.write(no_opcode.c_str());
          else
            return seastar::now();
        });
    }
  });
}

seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>& inp, seastar::output_stream<char>& out)
{
  //  RRR \todo safe read
  //  RRR \todo handle old protocol (see original code) - is still needed?

  return inp.read().
    then( [&out, this](auto full_cmd) {

      seastar::sstring cmd_line{full_cmd.begin(), full_cmd.end()};
      logger().debug("{}: incoming asok string: {}\n", __func__, cmd_line);
      return execute_line(cmd_line, out);

    }).then([&out]() { return out.flush(); }).
    finally([&out]() { return out.close(); }).
    then([&inp,&out]() {
      return inp.close();
    }).handle_exception([](auto ep) {
      logger().debug("exception on {}: {}", __func__, ep);
      return seastar::make_ready_future<>();
    }).discard_result();
}

seastar::future<> AdminSocket::init(const std::string& path)
{
#ifdef UNIT_TEST_OSD_ASOK
  // in-line unit-testing:
  std::ignore = seastar::async([this]() {
     seastar::sleep(3s).wait();
     asok_unit_testing::utest_run_1(this).wait();
  });
#endif

  return seastar::async([this, path] {
    auto serverfut = init_async(path);
  });
}


seastar::future<> AdminSocket::init_async(const std::string& path)
{
  internal_hooks().get(); // we are executing in an async thread, thus OK to wait()

  logger().debug("{}: asok socket path={}", __func__, path);
  auto sock_path = seastar::socket_address{seastar::unix_domain_addr{path}};

  return seastar::do_with(seastar::engine().listen(sock_path), [this](seastar::server_socket& lstn) {

    return seastar::do_until([this](){ return do_die; }, [&lstn,this]() {
      return lstn.accept().
        then([this](seastar::accept_result from_accept) {

          seastar::connected_socket cn = std::move(from_accept.connection);

          return do_with(std::move(cn.input()), std::move(cn.output()), std::move(cn),
                        [this](auto& inp, auto& out, auto& cn) {

            return handle_client(inp, out).finally([this, &inp, &out] {
              ; // left for debugging: std::cerr << "finally "  << std::endl;
            });
          });
      });
    });
  });
}

// ///////////////////////////////////////
// the internal hooks
// ///////////////////////////////////////

/*!
    The response to the '0' command is not formatted, neither as JSON or otherwise.
*/
class The0Hook : public AdminSocketHook {
public:
  seastar::future<bool> call(std::string_view command, const cmdmap_t&,
	    std::string_view format, bufferlist& out) const override {
    out.append(CEPH_ADMIN_SOCK_VERSION);
    return seastar::make_ready_future<bool>(true);
  }
};

class VersionHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit VersionHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, [[maybe_unused]] std::string_view command,
                                 [[maybe_unused]] const cmdmap_t& cmdmap,
	                         [[maybe_unused]] std::string_view format, [[maybe_unused]] bufferlist& out) const final
  {
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("release", ceph_release_to_str());
    f->dump_string("release_type", ceph_release_type());
    return seastar::now();
  }
};

/*!
  Note that the git_version command is expected to return a 'version' JSON segment.
*/
class GitVersionHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit GitVersionHook(AdminSocket* as) : m_as{as} {}

  std::string section_name(std::string_view command) const final {
    return "version"s;
  }

  /*seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final
  {
    f->dump_string("git_version", git_version_to_str());
    return seastar::now();
  }*/

  // slowing the response down for debugging.
  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final {
    return seastar::sleep(1s).then([this, f]() {
      f->dump_string("git_version", git_version_to_str());
      return seastar::now();
    });
  }
};

class HelpHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit HelpHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final {

    return seastar::with_shared(m_as->servers_tbl_rwlock, [this,f]() {
      for (const auto& hk_info : *m_as) {
        if (hk_info->help.length())
	  f->dump_string(hk_info->command.c_str(), hk_info->help);
      }
      return seastar::now();
    });
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as{as}
  {}

  std::string section_name(std::string_view command) const final {
    return "command_descriptions"s;
  }

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
                                 std::string_view format, bufferlist& out) const final
  {
    return seastar::with_shared(m_as->servers_tbl_rwlock, [this,f]() {
      int cmdnum = 0;

      for (const auto& hk_info : *m_as) {
        ostringstream secname;
        secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
        dump_cmd_and_help_to_json(f,
                                  CEPH_FEATURES_ALL,
                                  secname.str().c_str(),
                                  hk_info->cmddesc,
                                  hk_info->help);
        cmdnum++;
      }
      return seastar::now();
    });
  }
};

class TestThrowHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit TestThrowHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
                                 std::string_view format, bufferlist& out) const final {
    if (command == "fthrowAs")
      return seastar::make_exception_future<>(std::system_error{1, std::system_category()});

    return seastar::sleep(3s).then([this]() {
      throw(std::invalid_argument("As::TestThrowHook"));
      return seastar::now();
    });
  }
};

/// the hooks that are served directly by the admin_socket server
seastar::future<AsokRegistrationRes> AdminSocket::internal_hooks()
{
  version_hook = std::make_unique<VersionHook>(this);
  git_ver_hook = std::make_unique<GitVersionHook>(this);
  the0_hook = std::make_unique<The0Hook>();
  help_hook = std::make_unique<HelpHook>(this);
  getdescs_hook = std::make_unique<GetdescsHook>(this);
  test_throw_hook = std::make_unique<TestThrowHook>(this);

  static const std::vector<AsokServiceDef> internal_hooks_tbl{
      AsokServiceDef{"0",            "0",                    the0_hook.get(),        ""}
    , AsokServiceDef{"version",      "version",              version_hook.get(),     "get ceph version"}
    , AsokServiceDef{"git_version",  "git_version",          git_ver_hook.get(),     "get git sha1"}
    , AsokServiceDef{"help",         "help",                 help_hook.get(),        "list available commands"}
    , AsokServiceDef{"get_command_descriptions", "get_command_descriptions",
                                                             getdescs_hook.get(),    "list available commands"}
    , AsokServiceDef{"throwAs",      "throwAs",              test_throw_hook.get(),  ""}   // dev
    , AsokServiceDef{"fthrowAs",     "fthrowAs",             test_throw_hook.get(),  ""}   // dev
  };

  // server_registration() returns a shared pointer to the AdminSocket server, i.e. to us. As we
  // already have shared ownership of this object, we do not need it. RRR verify
  return register_server(AdminSocket::hook_server_tag{this}, internal_hooks_tbl);
}

#ifdef UNIT_TEST_OSD_ASOK
// ///////////////////////////////////////////
// unit-testing servers-block map manipulation
// ///////////////////////////////////////////

/*
  run multiple seastar threads that register/remove/search server blocks, testing
  the locks that protect them.
*/

#include <random>
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace seastar;

namespace asok_unit_testing {

class UTestHook : public AdminSocketHook {
public:
  explicit UTestHook() {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final
  {
    return seastar::now();
  }
};

static const UTestHook test_hook;

static const std::vector<AsokServiceDef> test_hooks{
      AsokServiceDef{"3",            "3",                    &test_hook,        "3"}
    , AsokServiceDef{"4",            "4",                    &test_hook,        "4"}
    , AsokServiceDef{"5",            "5",                    &test_hook,        "5"}
    , AsokServiceDef{"6",            "6",                    &test_hook,        "6"}
    , AsokServiceDef{"7",            "7",                    &test_hook,        "7"}
};


static const std::vector<AsokServiceDef> test_hooks10{
      AsokServiceDef{"13",            "13",                    &test_hook,        "13"}
    , AsokServiceDef{"14",            "14",                    &test_hook,        "14"}
    , AsokServiceDef{"15",            "15",                    &test_hook,        "15"}
    , AsokServiceDef{"16",            "16",                    &test_hook,        "16"}
    , AsokServiceDef{"17",            "17",                    &test_hook,        "17"}
};


struct test_reg_st {

  AdminSocket*                       asok;
  AdminSocket::hook_server_tag       tag;
  milliseconds                       period;
  gate&                              g;
  const std::vector<AsokServiceDef>& hks;
  std::random_device                 rd;
  std::default_random_engine         generator{rd()};
  AdminSocketRef                     current_reg; // holds the shared ownership

  test_reg_st(AdminSocket* ask, AdminSocket::hook_server_tag tag, milliseconds period, gate& gt, const std::vector<AsokServiceDef>& hks) :
    asok{ask},
    tag{tag},
    period{period},
    g{gt},
    hks{hks}
  {
    // start an async thread to reg/unreg
    std::ignore = seastar::async([this, hks] {
      auto loop_fut = loop(hks);
    });
  }

  // sleep, register, or fail w/ exception
  seastar::future<> delayed_reg(milliseconds dly) {
    return seastar::sleep(dly).
      then([this, dly]() {

        return with_gate(g, [this, dly]() {
          return asok->register_server(tag, hks);
        }).then([this](AsokRegistrationRes r) {
          current_reg = *r;
          return seastar::now();
        });
      });
  }

  // sleep, un-register, or fail w/ exception
  seastar::future<> delayed_unreg(milliseconds dly) {
    return seastar::sleep(dly).
      then([this]() {

        // must not check gate here! return with_gate(g, [this]() {
        return asok->unregister_server(tag, std::move(current_reg));
        // });
      });
  }

  seastar::future<> loop(const std::vector<AsokServiceDef>& hks) {
    std::uniform_int_distribution<int> dist(80, 120);
    auto act_period = milliseconds{period.count() * dist(generator) / 100};

    ceph::get_logger(ceph_subsys_osd).warn("{} starting", (uint64_t)(tag));
    return seastar::keep_doing([this, act_period, hks]() {

      return delayed_reg(act_period).then([this, act_period]() {
        return delayed_unreg(act_period);
      });
    }).handle_exception([this](std::exception_ptr eptr) {
      return seastar::now();
    }).finally([this]() {
      ceph::get_logger(ceph_subsys_osd).warn("{} done", (uint64_t)(tag));
      return seastar::now();
    });
  }
};

struct test_lookup_st {

  AdminSocket*                       asok;
  string                             cmd;
  milliseconds                       period;
  milliseconds                       delaying_inside;
  gate&                              g;
  std::random_device                 rd;
  std::default_random_engine         generator{rd()};
  std::uniform_int_distribution<int> dist{80, 120};


  test_lookup_st(AdminSocket* ask, string cmd, milliseconds period, milliseconds delaying_inside, gate& gt) :
    asok{ask},
    cmd{cmd},
    period{period},
    delaying_inside{delaying_inside},
    g{gt}
  {
    // start an async thread to reg/unreg
    std::ignore = seastar::async([this] {
      auto loop_fut = loop();
    });
  }

  seastar::future<> loop() {
    std::uniform_int_distribution<int> dist(80, 120);
    ceph::get_logger(ceph_subsys_osd).warn("utest-lookup {} starting", cmd);

    return seastar::keep_doing([this, dist]() mutable {
      milliseconds act_period = milliseconds{period.count() * dist(generator) / 100};
      milliseconds ins_period = milliseconds{delaying_inside.count() * dist(generator) / 100};

      return seastar::futurize_apply( [this, act_period, ins_period]() {
        return with_gate(g, [this, act_period, ins_period]()  {

          //
          //  look for the command, waste some time, then free the server-block
          //
          return seastar::async([this, act_period, ins_period]() mutable {

            bool fnd{false};

            for (const auto& hk : *asok) {

              if (hk->command == cmd) {
                fnd = true;
                // wait here for a while, with the iterator still holding the gate
                seastar::sleep(ins_period).get();
                break;
              }
            }
            if (!fnd) {
              std::cerr << __func__ << "(): " << cmd << " not found\n";
            }
            seastar::sleep(act_period).get();
          });
        });
      }).finally([this] {
        return seastar::now();
      });
    }).handle_exception([this](auto e) {
      // gate is closed
      ceph::get_logger(ceph_subsys_osd).warn("utest-lookup {} done", cmd);
      return seastar::now();
    });
  }
};

seastar::future<> utest_run_1(AdminSocket* asok)
{
  constexpr seconds run_time{3};
  seastar::gate gt;

  return seastar::do_with(std::move(gt), [run_time, asok](auto& gt) {

    test_reg_st* ta1 = new test_reg_st{asok, AdminSocket::hook_server_tag{(void*)(0x17)}, 200ms, gt, test_hooks  };
    test_reg_st ta2{asok, AdminSocket::hook_server_tag{(void*)(0x27)}, 150ms, gt, test_hooks10};

    test_lookup_st* tlk1 = new test_lookup_st{asok, "0",  55ms, 95ms, gt};
    test_lookup_st tlk2{asok, "help", 210ms, 90ms, gt};

    return seastar::sleep(run_time).
     then([&gt]() { return gt.close();}).
     then([&gt](){ return seastar::sleep(2000ms); }).
     then([ta1, &ta2, tlk1, &tlk2]() {
       delete ta1;
       delete tlk1;
       return seastar::now();
     });
  }).then_wrapped([](auto&& x) {
    try {
      x.get();
      return seastar::now();
    } catch (... ) {
      return seastar::now();
    }
  });
}

}
#endif
