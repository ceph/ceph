// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/admin/admin_socket.h"

#include <fmt/format.h>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/std-compat.hh>

#include "common/version.h"
#include "crimson/common/log.h"
#include "crimson/net/Socket.h"

/**
 *  A Crimson-wise version of the admin socket - implementation file
 *
 *  \todo handle the unlinking of the admin socket. Note that 'cleanup_files'
 *  at-exit functionality is not yet implemented in Crimson.
 */

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::admin {

// Hooks table - iterator support

AdminHooksIter AdminSocket::begin() const
{
  AdminHooksIter it{ *this };
  return it;
}

AdminHooksIter AdminSocket::end() const
{
  AdminHooksIter it{ *this, true };
  return it;
}

/**
 *  note that 'end-flag' is an optional parameter, and is only used internally
 * (to signal the end() state).
 */
AdminHooksIter::AdminHooksIter(const AdminSocket& master, bool end_flag)
    : m_master{ master }, m_end_marker{ end_flag }
{
  if (end_flag) {
    // create the equivalent of servers.end();
    m_miter = m_master.servers.cend();
  } else {
    m_miter = m_master.servers.cbegin();
    m_siter = m_miter->second.m_hooks.begin();
  }
}

AdminHooksIter& AdminHooksIter::operator++()
{
  if (!m_end_marker) {
    ++m_siter;
    if (m_siter == m_miter->second.m_hooks.end()) {
      // move to the next server-block
      m_miter++;
      if (m_miter == m_master.servers.end()) {
        m_end_marker = true;
        return *this;
      }

      m_siter = m_miter->second.m_hooks.begin();
    }
  }
  return *this;
}

seastar::future<AsokRegistrationRes> AdminSocket::register_server(
  hook_server_tag server_tag, const std::vector<AsokServiceDef>& apis_served)
{
  return seastar::with_lock(
    servers_tbl_rwlock,
    [this, server_tag, &apis_served]() -> AsokRegistrationRes {
      auto ne = servers.try_emplace(server_tag, apis_served);
      //  was this server tag already registered?
      if (!ne.second) {
        return {};
      }
      logger().info("register_server(): pid:{} ^:{} (tag: {})", (int)getpid(),
		    (uint64_t)(this), (uint64_t)(server_tag));
      return this->shared_from_this();
    });
}

seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag)
{
  logger().debug("{}: pid:{} server tag: {})", __func__, (int)getpid(),
                 (uint64_t)(server_tag));

  return seastar::with_lock(servers_tbl_rwlock, [this, server_tag] {
    if (auto erased = servers.erase(server_tag); erased == 0) {
      logger().warn("unregister_server(): unregistering a "
		    "non-existing registration (tag: {})",
		    (uint64_t)(server_tag));
    }
  });
}

seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag,
                                                 AdminSocketRef&& server_ref)
{
  return unregister_server(server_tag).then([ref=std::move(server_ref)] {
    // reducing the ref-count on us (the ASOK server) by discarding server_ref:
  }).finally([server_tag] {
    logger().debug("unregister_server: done {}", (uint64_t)(server_tag));
  });
}

AdminSocket::maybe_service_def_t AdminSocket::locate_subcmd(
  std::string match) const
{
  while (!match.empty()) {
    // try locating this sub-sequence of the incoming command
    for (auto& [tag, srv] : servers) {
      for (auto& api : srv.m_hooks) {
        if (api.command == match) {
          logger().debug("{}: located {} w/ server {}", __func__, match, tag);
          return &api;
        }
      }
    }
    // drop right-most word
    size_t pos = match.rfind(' ');
    if (pos == match.npos) {
      match.clear();  // we fail
      return {};
    } else {
      match.resize(pos);
    }
  }
  return {};
}

/*
 * Note: parse_cmd() is executed with servers_tbl_rwlock held as shared
 */
AdminSocket::maybe_parsed_t AdminSocket::parse_cmd(std::string cmd,
						   ceph::bufferlist& out)
{
  // preliminaries:
  //   - create the formatter specified by the cmd parameters
  //   - locate the "op-code" string (the 'prefix' segment)
  //   - prepare for command parameters extraction via cmdmap_t
  cmdmap_t cmdmap;

  try {
    stringstream errss;
    //  note that cmdmap_from_json() may throw on syntax issues
    if (!cmdmap_from_json({cmd}, &cmdmap, errss)) {
      logger().error("{}: incoming command error: {}", __func__, errss.str());
      out.append("error:"s);
      out.append(errss.str());
      return maybe_parsed_t{ std::nullopt };
    }
  } catch (std::runtime_error& e) {
    logger().error("{}: incoming command syntax: {}", __func__, cmd);
    out.append("error: command syntax"s);
    return maybe_parsed_t{ std::nullopt };
  }

  string format;
  string match;
  std::size_t full_command_seq;  // the full sequence, before we start chipping
                                 // away the end
  try {
    cmd_getval(cmdmap, "format", format);
    cmd_getval(cmdmap, "prefix", match);
    full_command_seq = match.length();
  } catch (const bad_cmd_get& e) {
    logger().error("{}: invalid syntax: {}", __func__, cmd);
    out.append("error: command syntax: missing 'prefix'"s);
    return maybe_parsed_t{ std::nullopt };
  }

  if (match.empty()) {
    // no command identified
    out.append("error: no command identified"s);
    return maybe_parsed_t{ std::nullopt };
  }

  // match the incoming op-code to one of the registered APIs
  auto parsed_cmd = locate_subcmd(match);
  if (!parsed_cmd.has_value()) {
    return maybe_parsed_t{ std::nullopt };
  }
  return parsed_command_t{ match,
                           cmdmap,
                           format,
                           parsed_cmd.value(),
                           parsed_cmd.value()->hook,
                           full_command_seq };
}

/*
 * Note: validate_command() is executed with servers_tbl_rwlock held as shared
 */
bool AdminSocket::validate_command(const parsed_command_t& parsed,
                                   const std::string& command_text,
                                   ceph::bufferlist& out) const
{
  // did we receive any arguments apart from the command word(s)?
  if (parsed.cmd_seq_len == parsed.cmd.length())
    return true;

  logger().info("{}: validating {} against:{}", __func__, command_text,
                parsed.api->cmddesc);

  stringstream os;  // for possible validation error messages
  try {
    // validate_cmd throws on some syntax errors
    if (validate_cmd(nullptr, parsed.api->cmddesc, parsed.parameters, os)) {
      return true;
    }
  } catch (std::exception& e) {
    logger().error("{}: validation failure ({} : {}) {}", __func__,
                   command_text, parsed.cmd, e.what());
  }

  os << "error: command validation failure ";
  logger().error("{}: validation failure (incoming:{}) {}", __func__,
                 command_text, os.str());
  out.append(os);
  return false;
}

seastar::future<> AdminSocket::finalize_response(
  seastar::output_stream<char>& out, ceph::bufferlist&& msgs)
{
  string outbuf_cont = msgs.to_str();
  if (outbuf_cont.empty()) {
    outbuf_cont = " {} ";
  }
  uint32_t response_length = htonl(outbuf_cont.length());
  logger().info("asok response length: {}", outbuf_cont.length());

  return out.write((char*)&response_length, sizeof(uint32_t))
    .then([&out, outbuf_cont] { return out.write(outbuf_cont.c_str()); });
}

seastar::future<> AdminSocket::execute_line(std::string cmdline,
                                            seastar::output_stream<char>& out)
{
  return seastar::with_shared(servers_tbl_rwlock,
			      [this, cmdline, &out]() mutable {
    ceph::bufferlist err;
    auto parsed = parse_cmd(cmdline, err);
    if (!parsed.has_value() ||
	!validate_command(*parsed, cmdline, err)) {
      return finalize_response(out, std::move(err));
    }
    return parsed->hook->call(parsed->cmd,
			      parsed->format,
			      parsed->parameters).then(
      [this, &out](auto result) {
	// add 'failed' to the contents of out_buf? not what
	// happens in the old code
        return finalize_response(out, std::move(result));
      });
  });
}

// an input_stream consumer that reads buffer into a std::string up to the first
// '\0' which indicates the end of command
struct line_consumer {
  using tmp_buf = seastar::temporary_buffer<char>;
  using consumption_result_type =
    typename seastar::input_stream<char>::consumption_result_type;

  seastar::future<consumption_result_type> operator()(tmp_buf&& buf) {
    size_t consumed = 0;
    for (auto c : buf) {
      consumed++;
      if (c == '\0' || c == '\n') {
	buf.trim_front(consumed);
	return seastar::make_ready_future<consumption_result_type>(
	  consumption_result_type::stop_consuming_type(std::move(buf)));
      } else {
	line.push_back(c);
      }
    }
    return seastar::make_ready_future<consumption_result_type>(
      seastar::continue_consuming{});
  }
  std::string line;
};

seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>& in,
                                             seastar::output_stream<char>& out)
{
  auto consumer = seastar::make_shared<line_consumer>();
  return in.consume(*consumer).then([consumer, &out, this] {
    logger().debug("AdminSocket::handle_client: incoming asok string: {}",
                   consumer->line);
    return execute_line(consumer->line, out);
  }).then([&out] {
    return out.flush();
  }).finally([&out] {
    return out.close();
  }).then([&in] {
    return in.close();
  }).handle_exception([](auto ep) {
    logger().debug("exception on {}: {}", __func__, ep);
    return seastar::make_ready_future<>();
  }).discard_result();
}

/**
 *  \brief continuously run a block of code "within a gate"
 *
 *  Neither gate closure, nor a failure of the code block we are running, will
 *  cause an exception to be thrown by safe_action_gate_func()
 */
template <typename AsyncAction>
seastar::future<> do_until_gate(seastar::gate& gt, AsyncAction action)
{
  auto stop_cond = [&gt] { return gt.is_closed(); };
  auto safe_action{ [act = std::move(action), &gt]() mutable {
    if (gt.is_closed())
      return seastar::make_ready_future<>();
    return with_gate(gt, [act = std::move(act)] {
      return act().handle_exception([](auto e) {});
    });
  } };

  return seastar::do_until(stop_cond, std::move(safe_action)).discard_result();
}

seastar::future<> AdminSocket::start(const std::string& path)
{
  if (path.empty()) {
    logger().error(
      "{}: Admin Socket socket path missing from the configuration", __func__);
    return seastar::now();
  }

  logger().debug("{}: asok socket path={}", __func__, path);
  auto sock_path = seastar::socket_address{ seastar::unix_domain_addr{ path } };

  std::ignore = register_admin_hooks().then([this, sock_path](
                                              AsokRegistrationRes reg_res) {
    return seastar::do_with(
      seastar::engine().listen(sock_path),
      [this](seastar::server_socket& lstn) {
        m_server_sock = &lstn;  // used for 'abort_accept()'
        return do_until_gate(arrivals_gate, [&lstn, this] {
          return lstn.accept().then(
            [this](seastar::accept_result from_accept) {
              seastar::connected_socket cn =
		std::move(from_accept.connection);
	      return do_with(cn.input(), cn.output(), std::move(cn),
                [this](auto& inp, auto& out, auto& cn) {
		  return handle_client(inp, out).finally([] {
		    ;  // left for debugging
                  });
		});
	    });
          }).then([] {
            logger().debug("AdminSocket::init(): admin-sock thread terminated");
            return seastar::now();
          });
      });
  });

  return seastar::make_ready_future<>();
}

seastar::future<> AdminSocket::stop()
{
  if (m_server_sock && !arrivals_gate.is_closed()) {
    // note that we check 'is_closed()' as if already closed - the server-sock
    // may have already been discarded
    m_server_sock->abort_accept();
    m_server_sock = nullptr;
  }

  return seastar::futurize_apply([this] { return arrivals_gate.close(); })
    .then_wrapped([](seastar::future<> res) {
      if (res.failed()) {
        std::ignore = res.handle_exception([](std::exception_ptr eptr) {
          return seastar::make_ready_future<>();
        });
      }
      return seastar::make_ready_future<>();
    }).handle_exception(
      [](std::exception_ptr eptr) { return seastar::make_ready_future<>();
    }).finally([] { return seastar::make_ready_future<>(); });
}

/////////////////////////////////////////
// the internal hooks
/////////////////////////////////////////

class VersionHook final : public AdminSocketHook {
 public:
  seastar::future<bufferlist> call(std::string_view,
				   std::string_view format,
				   const cmdmap_t&) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("version");
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("release", ceph_release_to_str());
    f->dump_string("release_type", ceph_release_type());
    f->close_section();
    bufferlist out;
    f->flush(out);
    return seastar::make_ready_future<bufferlist>(std::move(out));
  }
};

/**
  Note that the git_version command is expected to return a 'version' JSON
  segment.
*/
class GitVersionHook final : public AdminSocketHook {
 public:
  seastar::future<bufferlist> call(std::string_view command,
				   std::string_view format,
				   const cmdmap_t&) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("version");
    f->dump_string("git_version", git_version_to_str());
    f->close_section();
    ceph::bufferlist out;
    f->flush(out);
    return seastar::make_ready_future<bufferlist>(std::move(out));
  }
};

class HelpHook final : public AdminSocketHook {
  const AdminSocket& m_as;

 public:
  explicit HelpHook(const AdminSocket& as) : m_as{as} {}

  seastar::future<bufferlist> call(std::string_view command,
				   std::string_view format,
				   const cmdmap_t& cmdmap) const final
  {
    return seastar::with_shared(m_as.servers_tbl_rwlock,
				[this, format] {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
      f->open_object_section("help");
      for (const auto& hook : m_as) {
        if (!hook->help.empty()) {
          f->dump_string(hook->command.c_str(), hook->help);
	}
      }
      f->close_section();
      ceph::bufferlist out;
      f->flush(out);
      return seastar::make_ready_future<bufferlist>(std::move(out));
    });
  }
};

class GetdescsHook final : public AdminSocketHook {
  const AdminSocket& m_as;

 public:
  explicit GetdescsHook(const AdminSocket& as) : m_as{ as } {}

  seastar::future<bufferlist> call(std::string_view command,
				   std::string_view format,
				   const cmdmap_t& cmdmap) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    return seastar::with_shared(m_as.servers_tbl_rwlock, [this, f=move(f)] {
      int cmdnum = 0;
      f->open_object_section("command_descriptions");
      for (const auto& hook : m_as) {
	auto secname = fmt::format("cmd {:>03}", cmdnum);
        dump_cmd_and_help_to_json(f.get(), CEPH_FEATURES_ALL, secname,
                                  hook->cmddesc, hook->help);
        cmdnum++;
      }
      f->close_section();
      ceph::bufferlist out;
      f->flush(out);
      return seastar::make_ready_future<bufferlist>(std::move(out));
    });
  }
};

/// the hooks that are served directly by the admin_socket server
seastar::future<AsokRegistrationRes> AdminSocket::register_admin_hooks()
{
  version_hook = std::make_unique<VersionHook>();
  git_ver_hook = std::make_unique<GitVersionHook>();
  help_hook = std::make_unique<HelpHook>(*this);
  getdescs_hook = std::make_unique<GetdescsHook>(*this);

  // clang-format off
  static const std::vector<AsokServiceDef> internal_hooks_tbl{
      AsokServiceDef{"version",      "version",              version_hook.get(),     "get ceph version"}
    , AsokServiceDef{"git_version",  "git_version",          git_ver_hook.get(),     "get git sha1"}
    , AsokServiceDef{"help",         "help",                 help_hook.get(),        "list available commands"}
    , AsokServiceDef{"get_command_descriptions", "get_command_descriptions",
                                                             getdescs_hook.get(),    "list available commands"}
  };
  // clang-format on

  // server_registration() returns a shared pointer to the AdminSocket
  // server, i.e. to us. As we already have shared ownership of this object,
  // we do not need it.
  return register_server(AdminSocket::hook_server_tag{ this },
                         internal_hooks_tbl);
}

}  // namespace crimson::admin
