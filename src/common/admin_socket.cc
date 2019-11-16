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
#include <poll.h>
#include <sys/un.h>

#include "common/admin_socket.h"
#include "common/admin_socket_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/Thread.h"
#include "common/version.h"


// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "include/sock_compat.h"

#define dout_subsys ceph_subsys_asok
#undef dout_prefix
#define dout_prefix *_dout << "asok(" << (void*)m_cct << ") "


using std::ostringstream;

/*
 * UNIX domain sockets created by an application persist even after that
 * application closes, unless they're explicitly unlinked. This is because the
 * directory containing the socket keeps a reference to the socket.
 *
 * This code makes things a little nicer by unlinking those dead sockets when
 * the application exits normally.
 */

template<typename F, typename... Args>
inline int retry_sys_call(F f, Args... args) {
  int r;
  do {
    r = f(args...);
  } while (r < 0 && errno == EINTR);
  return r;
};


static std::mutex cleanup_lock;
static std::vector<std::string> cleanup_files;
static bool cleanup_atexit = false;

static void remove_cleanup_file(std::string_view file) {
  std::unique_lock l(cleanup_lock);

  if (auto i = std::find(cleanup_files.cbegin(), cleanup_files.cend(), file);
      i != cleanup_files.cend()) {
    retry_sys_call(::unlink, i->c_str());
    cleanup_files.erase(i);
  }
}

void remove_all_cleanup_files() {
  std::unique_lock l(cleanup_lock);
  for (const auto& s : cleanup_files) {
    retry_sys_call(::unlink, s.c_str());
  }
  cleanup_files.clear();
}

static void add_cleanup_file(std::string file) {
  std::unique_lock l(cleanup_lock);
  cleanup_files.push_back(std::move(file));
  if (!cleanup_atexit) {
    atexit(remove_all_cleanup_files);
    cleanup_atexit = true;
  }
}

AdminSocket::AdminSocket(CephContext *cct)
  : m_cct(cct)
{}

AdminSocket::~AdminSocket()
{
  shutdown();
}

/*
 * This thread listens on the UNIX domain socket for incoming connections.
 * It only handles one connection at a time at the moment. All I/O is nonblocking,
 * so that we can implement sensible timeouts. [TODO: make all I/O nonblocking]
 *
 * This thread also listens to m_shutdown_rd_fd. If there is any data sent to this
 * pipe, the thread terminates itself gracefully, allowing the
 * AdminSocketConfigObs class to join() it.
 */

std::string AdminSocket::create_shutdown_pipe(int *pipe_rd, int *pipe_wr)
{
  int pipefd[2];
  if (pipe_cloexec(pipefd) < 0) {
    int e = errno;
    ostringstream oss;
    oss << "AdminSocket::create_shutdown_pipe error: " << cpp_strerror(e);
    return oss.str();
  }
  
  *pipe_rd = pipefd[0];
  *pipe_wr = pipefd[1];
  return "";
}

std::string AdminSocket::destroy_shutdown_pipe()
{
  // Send a byte to the shutdown pipe that the thread is listening to
  char buf[1] = { 0x0 };
  int ret = safe_write(m_shutdown_wr_fd, buf, sizeof(buf));

  // Close write end
  retry_sys_call(::close, m_shutdown_wr_fd);
  m_shutdown_wr_fd = -1;

  if (ret != 0) {
    ostringstream oss;
    oss << "AdminSocket::destroy_shutdown_pipe error: failed to write"
      "to thread shutdown pipe: error " << ret;
    return oss.str();
  }

  th.join();

  // Close read end. Doing this before join() blocks the listenter and prevents
  // joining.
  retry_sys_call(::close, m_shutdown_rd_fd);
  m_shutdown_rd_fd = -1;

  return "";
}

std::string AdminSocket::bind_and_listen(const std::string &sock_path, int *fd)
{
  ldout(m_cct, 5) << "bind_and_listen " << sock_path << dendl;

  struct sockaddr_un address;
  if (sock_path.size() > sizeof(address.sun_path) - 1) {
    ostringstream oss;
    oss << "AdminSocket::bind_and_listen: "
	<< "The UNIX domain socket path " << sock_path << " is too long! The "
	<< "maximum length on this system is "
	<< (sizeof(address.sun_path) - 1);
    return oss.str();
  }
  int sock_fd = socket_cloexec(PF_UNIX, SOCK_STREAM, 0);
  if (sock_fd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "AdminSocket::bind_and_listen: "
	<< "failed to create socket: " << cpp_strerror(err);
    return oss.str();
  }
  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, sizeof(address.sun_path),
	   "%s", sock_path.c_str());
  if (::bind(sock_fd, (struct sockaddr*)&address,
	   sizeof(struct sockaddr_un)) != 0) {
    int err = errno;
    if (err == EADDRINUSE) {
      AdminSocketClient client(sock_path);
      bool ok;
      client.ping(&ok);
      if (ok) {
	ldout(m_cct, 20) << "socket " << sock_path << " is in use" << dendl;
	err = EEXIST;
      } else {
	ldout(m_cct, 20) << "unlink stale file " << sock_path << dendl;
	retry_sys_call(::unlink, sock_path.c_str());
	if (::bind(sock_fd, (struct sockaddr*)&address,
		 sizeof(struct sockaddr_un)) == 0) {
	  err = 0;
	} else {
	  err = errno;
	}
      }
    }
    if (err != 0) {
      ostringstream oss;
      oss << "AdminSocket::bind_and_listen: "
	  << "failed to bind the UNIX domain socket to '" << sock_path
	  << "': " << cpp_strerror(err);
      close(sock_fd);
      return oss.str();
    }
  }
  if (listen(sock_fd, 5) != 0) {
    int err = errno;
    ostringstream oss;
    oss << "AdminSocket::bind_and_listen: "
	  << "failed to listen to socket: " << cpp_strerror(err);
    close(sock_fd);
    retry_sys_call(::unlink, sock_path.c_str());
    return oss.str();
  }
  *fd = sock_fd;
  return "";
}

void AdminSocket::entry() noexcept
{
  ldout(m_cct, 5) << "entry start" << dendl;
  while (true) {
    struct pollfd fds[2];
    memset(fds, 0, sizeof(fds));
    fds[0].fd = m_sock_fd;
    fds[0].events = POLLIN | POLLRDBAND;
    fds[1].fd = m_shutdown_rd_fd;
    fds[1].events = POLLIN | POLLRDBAND;

    int ret = poll(fds, 2, -1);
    if (ret < 0) {
      int err = errno;
      if (err == EINTR) {
	continue;
      }
      lderr(m_cct) << "AdminSocket: poll(2) error: '"
		   << cpp_strerror(err) << dendl;
      return;
    }

    if (fds[0].revents & POLLIN) {
      // Send out some data
      do_accept();
    }
    if (fds[1].revents & POLLIN) {
      // Parent wants us to shut down
      return;
    }
  }
  ldout(m_cct, 5) << "entry exit" << dendl;
}

void AdminSocket::chown(uid_t uid, gid_t gid)
{
  if (m_sock_fd >= 0) {
    int r = ::chown(m_path.c_str(), uid, gid);
    if (r < 0) {
      r = -errno;
      lderr(m_cct) << "AdminSocket: failed to chown socket: "
		   << cpp_strerror(r) << dendl;
    }
  }
}

void AdminSocket::chmod(mode_t mode)
{
  if (m_sock_fd >= 0) {
    int r = ::chmod(m_path.c_str(), mode);
    if (r < 0) {
      r = -errno;
      lderr(m_cct) << "AdminSocket: failed to chmod socket: "
                   << cpp_strerror(r) << dendl;
    }
  }
}

bool AdminSocket::do_accept()
{
  struct sockaddr_un address;
  socklen_t address_length = sizeof(address);
  ldout(m_cct, 30) << "AdminSocket: calling accept" << dendl;
  int connection_fd = accept_cloexec(m_sock_fd, (struct sockaddr*) &address,
			     &address_length);
  if (connection_fd < 0) {
    int err = errno;
    lderr(m_cct) << "AdminSocket: do_accept error: '"
			   << cpp_strerror(err) << dendl;
    return false;
  }
  ldout(m_cct, 30) << "AdminSocket: finished accept" << dendl;

  char cmd[1024];
  unsigned pos = 0;
  string c;
  while (1) {
    int ret = safe_read(connection_fd, &cmd[pos], 1);
    if (ret <= 0) {
      if (ret < 0) {
        lderr(m_cct) << "AdminSocket: error reading request code: "
		     << cpp_strerror(ret) << dendl;
      }
      retry_sys_call(::close, connection_fd);
      return false;
    }
    if (cmd[0] == '\0') {
      // old protocol: __be32
      if (pos == 3 && cmd[0] == '\0') {
	switch (cmd[3]) {
	case 0:
	  c = "0";
	  break;
	case 1:
	  c = "perfcounters_dump";
	  break;
	case 2:
	  c = "perfcounters_schema";
	  break;
	default:
	  c = "foo";
	  break;
	}
	//wrap command with new protocol
	c = "{\"prefix\": \"" + c + "\"}";
	break;
      }
    } else {
      // new protocol: null or \n terminated string
      if (cmd[pos] == '\n' || cmd[pos] == '\0') {
	cmd[pos] = '\0';
	c = cmd;
	break;
      }
    }
    if (++pos >= sizeof(cmd)) {
      lderr(m_cct) << "AdminSocket: error reading request too long" << dendl;
      retry_sys_call(::close, connection_fd);
      return false;
    }
  }

  bool rval;
  bufferlist out;
  rval = execute_command(c, out);
  if (rval) {
    uint32_t len = htonl(out.length());
    int ret = safe_write(connection_fd, &len, sizeof(len));
    if (ret < 0) {
      lderr(m_cct) << "AdminSocket: error writing response length "
          << cpp_strerror(ret) << dendl;
      rval = false;
    } else {
      if (out.write_fd(connection_fd) >= 0)
        rval = true;
    }
  }

  retry_sys_call(::close, connection_fd);
  return rval;
}

int AdminSocket::execute_command(const std::string& cmd, ceph::bufferlist& out)
{
  cmdmap_t cmdmap;
  string format;
  vector<string> cmdvec;
  stringstream errss;
  cmdvec.push_back(cmd);
  if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
    ldout(m_cct, 0) << "AdminSocket: " << errss.str() << dendl;
    return false;
  }
  string match;
  try {
    cmd_getval(m_cct, cmdmap, "format", format);
    cmd_getval(m_cct, cmdmap, "prefix", match);
  } catch (const bad_cmd_get& e) {
    return false;
  }
  if (format != "json" && format != "json-pretty" &&
      format != "xml" && format != "xml-pretty")
    format = "json-pretty";

  std::unique_lock l(lock);
  decltype(hooks)::iterator p;
  while (match.size()) {
    p = hooks.find(match);
    if (p != hooks.cend())
      break;

    // drop right-most word
    size_t pos = match.rfind(' ');
    if (pos == std::string::npos) {
      match.clear();  // we fail
      break;
    } else {
      match.resize(pos);
    }
  }

  if (p == hooks.cend()) {
    lderr(m_cct) << "AdminSocket: request '" << cmd << "' not defined" << dendl;
    return false;
  }
  string args;
  if (match != cmd) {
    args = cmd.substr(match.length() + 1);
  }

  // Drop lock to avoid cycles in cases where the hook takes
  // the same lock that was held during calls to register/unregister,
  // and set in_hook to allow unregister to wait for us before
  // removing this hook.
  in_hook = true;
  auto match_hook = p->second.hook;
  l.unlock();
  bool success = (validate(match, cmdmap, out) &&
      match_hook->call(match, cmdmap, format, out));
  l.lock();
  in_hook = false;
  in_hook_cond.notify_all();
  if (!success) {
    ldout(m_cct, 0) << "AdminSocket: request '" << match << "' args '" << args
        << "' to " << match_hook << " failed" << dendl;
    out.append("failed");
  } else {
    ldout(m_cct, 5) << "AdminSocket: request '" << match << "' '" << args
        << "' to " << match_hook
        << " returned " << out.length() << " bytes" << dendl;
  }
  return true;
}



bool AdminSocket::validate(const std::string& command,
			   const cmdmap_t& cmdmap,
			   bufferlist& out) const
{
  stringstream os;
  if (validate_cmd(m_cct, hooks.at(command).desc, cmdmap, os)) {
    return true;
  } else {
    out.append(os);
    return false;
  }
}

int AdminSocket::register_command(std::string_view command,
				  std::string_view cmddesc,
				  AdminSocketHook *hook,
				  std::string_view help)
{
  int ret;
  std::unique_lock l(lock);
  auto i = hooks.find(command);
  if (i != hooks.cend()) {
    ldout(m_cct, 5) << "register_command " << command << " hook " << hook
		    << " EEXIST" << dendl;
    ret = -EEXIST;
  } else {
    ldout(m_cct, 5) << "register_command " << command << " hook " << hook
		    << dendl;
    hooks.emplace_hint(i,
		       std::piecewise_construct,
		       std::forward_as_tuple(command),
		       std::forward_as_tuple(hook, cmddesc, help));
    ret = 0;
  }
  return ret;
}

int AdminSocket::unregister_command(std::string_view command)
{
  int ret;
  std::unique_lock l(lock);
  auto i = hooks.find(command);
  if (i != hooks.cend()) {
    ldout(m_cct, 5) << "unregister_command " << command << dendl;

    // If we are currently processing a command, wait for it to
    // complete in case it referenced the hook that we are
    // unregistering.
    in_hook_cond.wait(l, [this]() { return !in_hook; });

    hooks.erase(i);


    ret = 0;
  } else {
    ldout(m_cct, 5) << "unregister_command " << command << " ENOENT" << dendl;
    ret = -ENOENT;
  }
  return ret;
}

void AdminSocket::unregister_commands(const AdminSocketHook *hook)
{
  std::unique_lock l(lock);
  auto i = hooks.begin();
  while (i != hooks.end()) {
    if (i->second.hook == hook) {
      ldout(m_cct, 5) << __func__ << " " << i->first << dendl;

      // If we are currently processing a command, wait for it to
      // complete in case it referenced the hook that we are
      // unregistering.
      in_hook_cond.wait(l, [this]() { return !in_hook; });
      hooks.erase(i++);
    } else {
      i++;
    }
  }
}

class VersionHook : public AdminSocketHook {
public:
  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) override {
    if (command == "0"sv) {
      out.append(CEPH_ADMIN_SOCK_VERSION);
    } else {
      JSONFormatter jf;
      jf.open_object_section("version");
      if (command == "version") {
	jf.dump_string("version", ceph_version_to_str());
	jf.dump_string("release", ceph_release_name(ceph_release()));
	jf.dump_string("release_type", ceph_release_type());
      } else if (command == "git_version") {
	jf.dump_string("git_version", git_version_to_str());
      }
      ostringstream ss;
      jf.close_section();
      jf.enable_line_break();
      jf.flush(ss);
      out.append(ss.str());
    }
    return true;
  }
};

class HelpHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit HelpHook(AdminSocket *as) : m_as(as) {}
  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format,
	    bufferlist& out) override {
    std::unique_ptr<Formatter> f(Formatter::create(format, "json-pretty"sv,
						   "json-pretty"sv));
    f->open_object_section("help");
    for (const auto& [command, info] : m_as->hooks) {
      if (info.help.length())
	f->dump_string(command.c_str(), info.help);
    }
    f->close_section();
    ostringstream ss;
    f->flush(ss);
    out.append(ss.str());
    return true;
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as(as) {}
  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) override {
    int cmdnum = 0;
    JSONFormatter jf;
    jf.open_object_section("command_descriptions");
    for (const auto& [command, info] : m_as->hooks) {
      // GCC 8 actually has [[maybe_unused]] on a structured binding
      // do what you'd expect. GCC 7 does not.
      (void)command;
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(&jf,
                                CEPH_FEATURES_ALL,
				secname.str().c_str(),
				info.desc,
				info.help);
      cmdnum++;
    }
    jf.close_section(); // command_descriptions
    jf.enable_line_break();
    ostringstream ss;
    jf.flush(ss);
    out.append(ss.str());
    return true;
  }
};

bool AdminSocket::init(const std::string& path)
{
  ldout(m_cct, 5) << "init " << path << dendl;

  /* Set up things for the new thread */
  std::string err;
  int pipe_rd = -1, pipe_wr = -1;
  err = create_shutdown_pipe(&pipe_rd, &pipe_wr);
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocketConfigObs::init: error: " << err << dendl;
    return false;
  }
  int sock_fd;
  err = bind_and_listen(path, &sock_fd);
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocketConfigObs::init: failed: " << err << dendl;
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }

  /* Create new thread */
  m_sock_fd = sock_fd;
  m_shutdown_rd_fd = pipe_rd;
  m_shutdown_wr_fd = pipe_wr;
  m_path = path;

  version_hook = std::make_unique<VersionHook>();
  register_command("0", "0", version_hook.get(), "");
  register_command("version", "version", version_hook.get(), "get ceph version");
  register_command("git_version", "git_version", version_hook.get(),
		   "get git sha1");
  help_hook = std::make_unique<HelpHook>(this);
  register_command("help", "help", help_hook.get(),
		   "list available commands");
  getdescs_hook = std::make_unique<GetdescsHook>(this);
  register_command("get_command_descriptions", "get_command_descriptions",
		   getdescs_hook.get(), "list available commands");

  th = make_named_thread("admin_socket", &AdminSocket::entry, this);
  add_cleanup_file(m_path.c_str());
  return true;
}

void AdminSocket::shutdown()
{
  // Under normal operation this is unlikely to occur.  However for some unit
  // tests, some object members are not initialized and so cannot be deleted
  // without fault.
  if (m_shutdown_wr_fd < 0)
    return;

  ldout(m_cct, 5) << "shutdown" << dendl;

  auto err = destroy_shutdown_pipe();
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocket::shutdown: error: " << err << dendl;
  }

  retry_sys_call(::close, m_sock_fd);

  unregister_commands(version_hook.get());
  version_hook.reset();

  unregister_command("help");
  help_hook.reset();

  unregister_command("get_command_descriptions");
  getdescs_hook.reset();

  remove_cleanup_file(m_path);
  m_path.clear();
}
