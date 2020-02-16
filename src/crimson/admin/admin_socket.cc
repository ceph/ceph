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

seastar::future<>
AdminSocket::register_command(std::unique_ptr<AdminSocketHook>&& hook)
{
  return seastar::with_lock(servers_tbl_rwlock,
			    [this, hook = std::move(hook)]() mutable {
    auto prefix = hook->prefix;
    auto [it, added] = hooks.emplace(prefix, std::move(hook));
    //  was this server tag already registered?
    assert(added);
    if (added) {
      logger().info("register_command(): {})", it->first);
    }
    return seastar::now();
  });
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
  string prefix;
  try {
    cmd_getval(cmdmap, "format", format);
    cmd_getval(cmdmap, "prefix", prefix);
  } catch (const bad_cmd_get& e) {
    logger().error("{}: invalid syntax: {}", __func__, cmd);
    out.append("error: command syntax: missing 'prefix'"s);
    return maybe_parsed_t{ std::nullopt };
  }

  if (prefix.empty()) {
    // no command identified
    out.append("error: no command identified"s);
    return maybe_parsed_t{ std::nullopt };
  }

  // match the incoming op-code to one of the registered APIs
  if (auto found = hooks.find(prefix); found != hooks.end()) {
    return parsed_command_t{ cmdmap, format, *found->second };
  } else {
    return maybe_parsed_t{ std::nullopt };
  }
}

/*
 * Note: validate_command() is executed with servers_tbl_rwlock held as shared
 */
bool AdminSocket::validate_command(const parsed_command_t& parsed,
                                   const std::string& command_text,
                                   ceph::bufferlist& out) const
{
  logger().info("{}: validating {} against:{}", __func__, command_text,
                parsed.hook.desc);

  stringstream os;  // for possible validation error messages
  if (validate_cmd(nullptr, std::string{parsed.hook.desc}, parsed.parameters, os)) {
    return true;
  } else {
    os << "error: command validation failure ";
    logger().error("{}: validation failure (incoming:{}) {}", __func__,
		   command_text, os.str());
    out.append(os);
    return false;
  }
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
    return parsed->hook.call(parsed->hook.prefix,
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

seastar::future<> AdminSocket::start(const std::string& path)
{
  if (path.empty()) {
    logger().error(
      "{}: Admin Socket socket path missing from the configuration", __func__);
    return seastar::now();
  }

  logger().debug("{}: asok socket path={}", __func__, path);
  auto sock_path = seastar::socket_address{ seastar::unix_domain_addr{ path } };
  server_sock = seastar::engine().listen(sock_path);
  // listen in background
  std::ignore = seastar::do_until(
    [this] { return stop_gate.is_closed(); },
    [this] {
      return seastar::with_gate(stop_gate, [this] {
        assert(!connected_sock.has_value());
        return server_sock->accept().then([this](seastar::accept_result acc) {
          connected_sock = std::move(acc.connection);
          return seastar::do_with(connected_sock->input(),
                                  connected_sock->output(),
            [this](auto& input, auto& output) mutable {
            return handle_client(input, output);
          }).finally([this] {
            assert(connected_sock.has_value());
            connected_sock.reset();
          });
        }).handle_exception([this](auto ep) {
          if (!stop_gate.is_closed()) {
            logger().error("AdminSocket: terminated: {}", ep);
          }
        });
      });
    }).then([] {
      logger().debug("AdminSocket::init(): admin-sock thread terminated");
      return seastar::now();
    });

  return seastar::make_ready_future<>();
}

seastar::future<> AdminSocket::stop()
{
  if (!server_sock) {
    return seastar::now();
  }
  server_sock->abort_accept();
  server_sock.reset();
  if (connected_sock) {
    connected_sock->shutdown_input();
    connected_sock->shutdown_output();
    connected_sock.reset();
  }
  return stop_gate.close();
}

/////////////////////////////////////////
// the internal hooks
/////////////////////////////////////////

class VersionHook final : public AdminSocketHook {
 public:
  VersionHook()
    : AdminSocketHook{"version", "version", "get ceph version"}
  {}
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
  GitVersionHook()
    : AdminSocketHook{"git_version", "git_version", "get git sha1"}
  {}
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
  explicit HelpHook(const AdminSocket& as) :
    AdminSocketHook{"help", "help", "list available commands"},
    m_as{as}
  {}

  seastar::future<bufferlist> call(std::string_view command,
				   std::string_view format,
				   const cmdmap_t& cmdmap) const final
  {
    return seastar::with_shared(m_as.servers_tbl_rwlock,
				[this, format] {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
      f->open_object_section("help");
      for (const auto& [prefix, hook] : m_as) {
        if (!hook->help.empty()) {
          f->dump_string(prefix.data(), hook->help);
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
  explicit GetdescsHook(const AdminSocket& as) :
    AdminSocketHook{"get_command_descriptions",
		    "get_command_descriptions",
		    "list available commands"},
    m_as{ as } {}

  seastar::future<bufferlist> call(std::string_view command,
				   std::string_view format,
				   const cmdmap_t& cmdmap) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    return seastar::with_shared(m_as.servers_tbl_rwlock, [this, f=move(f)] {
      int cmdnum = 0;
      f->open_object_section("command_descriptions");
      for (const auto& [prefix, hook] : m_as) {
	auto secname = fmt::format("cmd {:>03}", cmdnum);
        dump_cmd_and_help_to_json(f.get(), CEPH_FEATURES_ALL, secname,
                                  std::string{hook->desc},
				  std::string{hook->help});
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
seastar::future<> AdminSocket::register_admin_commands()
{
  return seastar::when_all_succeed(
    register_command(std::make_unique<VersionHook>()),
    register_command(std::make_unique<GitVersionHook>()),
    register_command(std::make_unique<HelpHook>(*this)),
    register_command(std::make_unique<GetdescsHook>(*this)));
}

}  // namespace crimson::admin
