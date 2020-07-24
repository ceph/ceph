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
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "crimson/common/log.h"
#include "crimson/net/Socket.h"

using namespace crimson::common;
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

tell_result_t::tell_result_t(int ret, std::string&& err)
  : ret{ret}, err(std::move(err))
{}

tell_result_t::tell_result_t(int ret, std::string&& err, ceph::bufferlist&& out)
  : ret{ret}, err(std::move(err)), out(std::move(out))
{}

tell_result_t::tell_result_t(Formatter* formatter)
{
  formatter->flush(out);
}

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
auto AdminSocket::parse_cmd(const std::vector<std::string>& cmd)
  -> std::variant<parsed_command_t, tell_result_t>
{
  // preliminaries:
  //   - create the formatter specified by the cmd parameters
  //   - locate the "op-code" string (the 'prefix' segment)
  //   - prepare for command parameters extraction via cmdmap_t
  cmdmap_t cmdmap;
  ceph::bufferlist out;

  try {
    stringstream errss;
    //  note that cmdmap_from_json() may throw on syntax issues
    if (!cmdmap_from_json(cmd, &cmdmap, errss)) {
      logger().error("{}: incoming command error: {}", __func__, errss.str());
      out.append("error:"s);
      out.append(errss.str());
      return tell_result_t{-EINVAL, "invalid json", std::move(out)};
    }
  } catch (const std::runtime_error& e) {
    logger().error("{}: incoming command syntax: {}", __func__, cmd);
    out.append(string{e.what()});
    return tell_result_t{-EINVAL, "invalid json", std::move(out)};
  }

  string format;
  string prefix;
  try {
    cmd_getval(cmdmap, "format", format);
    cmd_getval(cmdmap, "prefix", prefix);
  } catch (const bad_cmd_get& e) {
    logger().error("{}: invalid syntax: {}", __func__, cmd);
    out.append(string{e.what()});
    return tell_result_t{-EINVAL, "invalid json", std::move(out)};
  }

  // match the incoming op-code to one of the registered APIs
  if (auto found = hooks.find(prefix); found != hooks.end()) {
    return parsed_command_t{ cmdmap, format, *found->second };
  } else {
    return tell_result_t{-EINVAL,
                         fmt::format("unknown command '{}'", prefix),
                         std::move(out)};
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


seastar::future<> AdminSocket::handle_command(crimson::net::Connection* conn,
					      boost::intrusive_ptr<MCommand> m)
{
  return execute_command(m->cmd, std::move(m->get_data())).then(
    [conn, tid=m->get_tid()](auto result) {
    auto [ret, err, out] = std::move(result);
    auto reply = make_message<MCommandReply>(ret, err);
    reply->set_tid(tid);
    reply->set_data(out);
    return conn->send(reply);
  });
}

seastar::future<> AdminSocket::execute_line(std::string cmdline,
                                            seastar::output_stream<char>& out)
{
  return execute_command({cmdline}, {}).then([&out, this](auto result) {
     auto [ret, stderr, stdout] = std::move(result);
     if (ret < 0) {
       stdout.append(fmt::format("ERROR: {}\n", cpp_strerror(ret)));
       stdout.append(stderr);
     }
     return finalize_response(out, std::move(stdout));
  });
}

auto AdminSocket::execute_command(const std::vector<std::string>& cmd,
				  ceph::bufferlist&& buf)
  -> seastar::future<tell_result_t>
{
  return seastar::with_shared(servers_tbl_rwlock,
			      [cmd, buf=std::move(buf), this]() mutable {
    auto maybe_parsed = parse_cmd(cmd);
    if (auto parsed = std::get_if<parsed_command_t>(&maybe_parsed); parsed) {
      stringstream os;
      string desc{parsed->hook.desc};
      if (!validate_cmd(nullptr, desc, parsed->params, os)) {
	logger().error("AdminSocket::execute_command: "
		       "failed to validate '{}': {}", cmd, os.str());
	ceph::bufferlist out;
	out.append(os);
	return seastar::make_ready_future<tell_result_t>(
          tell_result_t{-EINVAL, "invalid command json", std::move(out)});
      }
      return parsed->hook.call(parsed->params, parsed->format, std::move(buf));
    } else {
      auto& result = std::get<tell_result_t>(maybe_parsed);
      return seastar::make_ready_future<tell_result_t>(std::move(result));
    }
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
      if (c == '\0') {
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
  });
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
  try {
    server_sock = seastar::engine().listen(sock_path);
  } catch (const std::system_error& e) {
    logger().error("{}: unable to listen({}): {}", __func__, path, e.what());
    server_sock.reset();
    return seastar::make_ready_future<>();
  }
  // listen in background
  task = seastar::do_until(
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
    }).finally([path] {
      return seastar::remove_file(path);
    });
  return seastar::make_ready_future<>();
}

seastar::future<> AdminSocket::stop()
{
  if (!server_sock) {
    return seastar::now();
  }
  server_sock->abort_accept();
  if (connected_sock) {
    connected_sock->shutdown_input();
    connected_sock->shutdown_output();
  }
  return stop_gate.close().then([this] {
    assert(task.has_value());
    return task->then([] {
      logger().info("AdminSocket: stopped");
      return seastar::now();
    });
  });
}

/////////////////////////////////////////
// the internal hooks
/////////////////////////////////////////

class VersionHook final : public AdminSocketHook {
 public:
  VersionHook()
    : AdminSocketHook{"version", "version", "get ceph version"}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&&) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("version");
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("release", ceph_release_to_str());
    f->dump_string("release_type", ceph_release_type());
    f->close_section();
    return seastar::make_ready_future<tell_result_t>(f.get());
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
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&&) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("version");
    f->dump_string("git_version", git_version_to_str());
    f->close_section();
    return seastar::make_ready_future<tell_result_t>(f.get());
  }
};

class HelpHook final : public AdminSocketHook {
  const AdminSocket& m_as;

 public:
  explicit HelpHook(const AdminSocket& as) :
    AdminSocketHook{"help", "help", "list available commands"},
    m_as{as}
  {}

  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&&) const final
  {
    return seastar::with_shared(m_as.servers_tbl_rwlock,
				[format, this] {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
      f->open_object_section("help");
      for (const auto& [prefix, hook] : m_as) {
        if (!hook->help.empty()) {
          f->dump_string(prefix.data(), hook->help);
	}
      }
      f->close_section();
      return seastar::make_ready_future<tell_result_t>(f.get());
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

  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
				      std::string_view format,
				      ceph::bufferlist&&) const final
  {
    return seastar::with_shared(m_as.servers_tbl_rwlock, [format, this] {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
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
      return seastar::make_ready_future<tell_result_t>(f.get());
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
