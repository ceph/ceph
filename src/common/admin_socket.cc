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
#include "common/ceph_mutex.h"

#ifndef WITH_SEASTAR
#include "common/Cond.h"
#endif

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "include/sock_compat.h"

#define dout_subsys ceph_subsys_asok
#undef dout_prefix
#define dout_prefix *_dout << "asok(" << (void*)m_cct << ") "

using namespace std::literals;

using std::ostringstream;
using std::string;
using std::stringstream;

using namespace TOPNSPC::common;

using ceph::bufferlist;
using ceph::cref_t;
using ceph::Formatter;


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
 * This thread also listens to m_wakeup_rd_fd. If there is any data sent to this
 * pipe, the thread wakes up.  If m_shutdown is set, the thread terminates
 * itself gracefully, allowing the AdminSocketConfigObs class to join() it.
 */

std::string AdminSocket::create_wakeup_pipe(int *pipe_rd, int *pipe_wr)
{
  int pipefd[2];
  if (pipe_cloexec(pipefd, O_NONBLOCK) < 0) {
    int e = errno;
    ostringstream oss;
    oss << "AdminSocket::create_wakeup_pipe error: " << cpp_strerror(e);
    return oss.str();
  }
  
  *pipe_rd = pipefd[0];
  *pipe_wr = pipefd[1];
  return "";
}

std::string AdminSocket::destroy_wakeup_pipe()
{
  // Send a byte to the wakeup pipe that the thread is listening to
  char buf[1] = { 0x0 };
  int ret = safe_write(m_wakeup_wr_fd, buf, sizeof(buf));

  // Close write end
  retry_sys_call(::close, m_wakeup_wr_fd);
  m_wakeup_wr_fd = -1;

  if (ret != 0) {
    ostringstream oss;
    oss << "AdminSocket::destroy_shutdown_pipe error: failed to write"
      "to thread shutdown pipe: error " << ret;
    return oss.str();
  }

  th.join();

  // Close read end. Doing this before join() blocks the listenter and prevents
  // joining.
  retry_sys_call(::close, m_wakeup_rd_fd);
  m_wakeup_rd_fd = -1;

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
  // FIPS zeroization audit 20191115: this memset is fine.
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
    // FIPS zeroization audit 20191115: this memset is fine.
    memset(fds, 0, sizeof(fds));
    fds[0].fd = m_sock_fd;
    fds[0].events = POLLIN | POLLRDBAND;
    fds[1].fd = m_wakeup_rd_fd;
    fds[1].events = POLLIN | POLLRDBAND;

    ldout(m_cct,20) << __func__ << " waiting" << dendl;
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
    ldout(m_cct,20) << __func__ << " awake" << dendl;

    if (fds[0].revents & POLLIN) {
      // Send out some data
      do_accept();
    }
    if (fds[1].revents & POLLIN) {
      // read off one byte
      char buf;
      auto s = ::read(m_wakeup_rd_fd, &buf, 1);
      if (s == -1) {
        int e = errno;
        ldout(m_cct, 5) << "AdminSocket: (ignoring) read(2) error: '"
		        << cpp_strerror(e) << dendl;
      }
      do_tell_queue();
    }
    if (m_shutdown) {
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

void AdminSocket::do_accept()
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
    return;
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
      return;
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
      return;
    }
  }

  std::vector<std::string> cmdvec = { c };
  bufferlist empty, out;
  ostringstream err;
  int rval = execute_command(cmdvec, empty /* inbl */, err, &out);

  // Unfortunately, the asok wire protocol does not let us pass an error code,
  // and many asok command implementations return helpful error strings.  So,
  // let's prepend an error string to the output if there is an error code.
  if (rval < 0) {
    ostringstream ss;
    ss << "ERROR: " << cpp_strerror(rval) << "\n";
    ss << err.str() << "\n";
    bufferlist o;
    o.append(ss.str());
    o.claim_append(out);
    out.claim_append(o);
  }
  uint32_t len = htonl(out.length());
  int ret = safe_write(connection_fd, &len, sizeof(len));
  if (ret < 0) {
    lderr(m_cct) << "AdminSocket: error writing response length "
		 << cpp_strerror(ret) << dendl;
  } else {
    if (out.write_fd(connection_fd) < 0) {
      lderr(m_cct) << "AdminSocket: error writing response payload "
		   << cpp_strerror(ret) << dendl;
    }
  }
  retry_sys_call(::close, connection_fd);
}

void AdminSocket::do_tell_queue()
{
  ldout(m_cct,10) << __func__ << dendl;
  std::list<cref_t<MCommand>> q;
  std::list<cref_t<MMonCommand>> lq;
  {
    std::lock_guard l(tell_lock);
    q.swap(tell_queue);
    lq.swap(tell_legacy_queue);
  }
  for (auto& m : q) {
    bufferlist outbl;
    execute_command(
      m->cmd,
      m->get_data(),
      [m](int r, const std::string& err, bufferlist& outbl) {
	auto reply = new MCommandReply(r, err);
	reply->set_tid(m->get_tid());
	reply->set_data(outbl);
#ifdef WITH_SEASTAR
#warning "fix message send with crimson"
#else
	m->get_connection()->send_message(reply);
#endif
      });
  }
  for (auto& m : lq) {
    bufferlist outbl;
    execute_command(
      m->cmd,
      m->get_data(),
      [m](int r, const std::string& err, bufferlist& outbl) {
	auto reply = new MMonCommandAck(m->cmd, r, err, 0);
	reply->set_tid(m->get_tid());
	reply->set_data(outbl);
#ifdef WITH_SEASTAR
#warning "fix message send with crimson"
#else
	m->get_connection()->send_message(reply);
#endif
      });
  }
}

int AdminSocket::execute_command(
  const std::vector<std::string>& cmd,
  const bufferlist& inbl,
  std::ostream& errss,
  bufferlist *outbl)
{
#ifdef WITH_SEASTAR
#warning "must implement admin socket blocking execute_command() for crimson"
  return -ENOSYS;
#else
  bool done = false;
  int rval = 0;
  ceph::mutex mylock = ceph::make_mutex("admin_socket::excute_command::mylock");
  ceph::condition_variable mycond;
  C_SafeCond fin(mylock, mycond, &done, &rval);
  execute_command(
    cmd,
    inbl,
    [&errss, outbl, &fin](int r, const std::string& err, bufferlist& out) {
      errss << err;
      *outbl = std::move(out);
      fin.finish(r);
    });
  {
    std::unique_lock l{mylock};
    mycond.wait(l, [&done] { return done;});
  }
  return rval;
#endif
}

void AdminSocket::execute_command(
  const std::vector<std::string>& cmdvec,
  const bufferlist& inbl,
  std::function<void(int,const std::string&,bufferlist&)> on_finish)
{
  cmdmap_t cmdmap;
  string format;
  stringstream errss;
  bufferlist empty;
  ldout(m_cct,10) << __func__ << " cmdvec='" << cmdvec << "'" << dendl;
  if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
    ldout(m_cct, 0) << "AdminSocket: " << errss.str() << dendl;
    return on_finish(-EINVAL, "invalid json", empty);
  }
  string prefix;
  try {
    cmd_getval(cmdmap, "format", format);
    cmd_getval(cmdmap, "prefix", prefix);
  } catch (const bad_cmd_get& e) {
    return on_finish(-EINVAL, "invalid json, missing format and/or prefix",
		     empty);
  }

  auto f = Formatter::create(format, "json-pretty", "json-pretty");

  std::unique_lock l(lock);
  decltype(hooks)::iterator p;
  p = hooks.find(prefix);
  if (p == hooks.cend()) {
    lderr(m_cct) << "AdminSocket: request '" << cmdvec
		 << "' not defined" << dendl;
    delete f;
    return on_finish(-EINVAL, "unknown command prefix "s + prefix, empty);
  }

  // make sure one of the registered commands with this prefix validates.
  while (!validate_cmd(m_cct, p->second.desc, cmdmap, errss)) {
    ++p;
    if (p->first != prefix) {
      delete f;
      return on_finish(-EINVAL, "invalid command json", empty);
    }
  }

  // Drop lock to avoid cycles in cases where the hook takes
  // the same lock that was held during calls to register/unregister,
  // and set in_hook to allow unregister to wait for us before
  // removing this hook.
  in_hook = true;
  auto hook = p->second.hook;
  l.unlock();
  hook->call_async(
    prefix, cmdmap, f, inbl,
    [f, on_finish](int r, const std::string& err, bufferlist& out) {
      // handle either existing output in bufferlist *or* via formatter
      if (r >= 0 && out.length() == 0) {
	f->flush(out);
      }
      delete f;
      on_finish(r, err, out);
    });

  l.lock();
  in_hook = false;
  in_hook_cond.notify_all();
}

void AdminSocket::queue_tell_command(cref_t<MCommand> m)
{
  ldout(m_cct,10) << __func__ << " " << *m << dendl;
  std::lock_guard l(tell_lock);
  tell_queue.push_back(std::move(m));
  wakeup();
}
void AdminSocket::queue_tell_command(cref_t<MMonCommand> m)
{
  ldout(m_cct,10) << __func__ << " " << *m << dendl;
  std::lock_guard l(tell_lock);
  tell_legacy_queue.push_back(std::move(m));
  wakeup();
}

int AdminSocket::register_command(std::string_view cmddesc,
				  AdminSocketHook *hook,
				  std::string_view help)
{
  int ret;
  std::unique_lock l(lock);
  string prefix = cmddesc_get_prefix(cmddesc);
  auto i = hooks.find(prefix);
  if (i != hooks.cend() &&
      i->second.desc == cmddesc) {
    ldout(m_cct, 5) << "register_command " << prefix
		    << " cmddesc " << cmddesc << " hook " << hook
		    << " EEXIST" << dendl;
    ret = -EEXIST;
  } else {
    ldout(m_cct, 5) << "register_command " << prefix << " hook " << hook
		    << dendl;
    hooks.emplace_hint(i,
		       std::piecewise_construct,
		       std::forward_as_tuple(prefix),
		       std::forward_as_tuple(hook, cmddesc, help));
    ret = 0;
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
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    if (command == "0"sv) {
      out.append(CEPH_ADMIN_SOCK_VERSION);
    } else {
      f->open_object_section("version");
      if (command == "version") {
	f->dump_string("version", ceph_version_to_str());
	f->dump_string("release", ceph_release_to_str());
	f->dump_string("release_type", ceph_release_type());
      } else if (command == "git_version") {
	f->dump_string("git_version", git_version_to_str());
      }
      ostringstream ss;
      f->close_section();
    }
    return 0;
  }
};

class HelpHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit HelpHook(AdminSocket *as) : m_as(as) {}
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    f->open_object_section("help");
    for (const auto& [command, info] : m_as->hooks) {
      if (info.help.length())
	f->dump_string(command.c_str(), info.help);
    }
    f->close_section();
    return 0;
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as(as) {}
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    int cmdnum = 0;
    f->open_object_section("command_descriptions");
    for (const auto& [command, info] : m_as->hooks) {
      // GCC 8 actually has [[maybe_unused]] on a structured binding
      // do what you'd expect. GCC 7 does not.
      (void)command;
      ostringstream secname;
      secname << "cmd" << std::setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(f,
                                CEPH_FEATURES_ALL,
				secname.str().c_str(),
				info.desc,
				info.help);
      cmdnum++;
    }
    f->close_section(); // command_descriptions
    return 0;
  }
};

bool AdminSocket::init(const std::string& path)
{
  ldout(m_cct, 5) << "init " << path << dendl;

  /* Set up things for the new thread */
  std::string err;
  int pipe_rd = -1, pipe_wr = -1;
  err = create_wakeup_pipe(&pipe_rd, &pipe_wr);
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
  m_wakeup_rd_fd = pipe_rd;
  m_wakeup_wr_fd = pipe_wr;
  m_path = path;

  version_hook = std::make_unique<VersionHook>();
  register_command("0", version_hook.get(), "");
  register_command("version", version_hook.get(), "get ceph version");
  register_command("git_version", version_hook.get(),
		   "get git sha1");
  help_hook = std::make_unique<HelpHook>(this);
  register_command("help", help_hook.get(),
		   "list available commands");
  getdescs_hook = std::make_unique<GetdescsHook>(this);
  register_command("get_command_descriptions",
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
  if (m_wakeup_wr_fd < 0)
    return;

  ldout(m_cct, 5) << "shutdown" << dendl;
  m_shutdown = true;

  auto err = destroy_wakeup_pipe();
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocket::shutdown: error: " << err << dendl;
  }

  retry_sys_call(::close, m_sock_fd);

  unregister_commands(version_hook.get());
  version_hook.reset();

  unregister_commands(help_hook.get());
  help_hook.reset();

  unregister_commands(getdescs_hook.get());
  getdescs_hook.reset();

  remove_cleanup_file(m_path);
  m_path.clear();
}

void AdminSocket::wakeup()
{
  // Send a byte to the wakeup pipe that the thread is listening to
  char buf[1] = { 0x0 };
  int r = safe_write(m_wakeup_wr_fd, buf, sizeof(buf));
  (void)r;
}
