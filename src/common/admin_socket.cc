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

#include "include/int_types.h"

#include "common/Thread.h"
#include "common/admin_socket.h"
#include "common/admin_socket_client.h"
#include "common/config.h"
#include "common/cmdparse.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/pipe.h"
#include "common/safe_io.h"
#include "common/version.h"
#include "common/Formatter.h"

#include <errno.h>
#include <fcntl.h>
#include <map>
#include <poll.h>
#include <set>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "include/compat.h"

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
static pthread_mutex_t cleanup_lock = PTHREAD_MUTEX_INITIALIZER;
static std::vector <const char*> cleanup_files;
static bool cleanup_atexit = false;

static void remove_cleanup_file(const char *file)
{
  pthread_mutex_lock(&cleanup_lock);
  VOID_TEMP_FAILURE_RETRY(unlink(file));
  for (std::vector <const char*>::iterator i = cleanup_files.begin();
       i != cleanup_files.end(); ++i) {
    if (strcmp(file, *i) == 0) {
      free((void*)*i);
      cleanup_files.erase(i);
      break;
    }
  }
  pthread_mutex_unlock(&cleanup_lock);
}

static void remove_all_cleanup_files()
{
  pthread_mutex_lock(&cleanup_lock);
  for (std::vector <const char*>::iterator i = cleanup_files.begin();
       i != cleanup_files.end(); ++i) {
    VOID_TEMP_FAILURE_RETRY(unlink(*i));
    free((void*)*i);
  }
  cleanup_files.clear();
  pthread_mutex_unlock(&cleanup_lock);
}

static void add_cleanup_file(const char *file)
{
  char *fname = strdup(file);
  if (!fname)
    return;
  pthread_mutex_lock(&cleanup_lock);
  cleanup_files.push_back(fname);
  if (!cleanup_atexit) {
    atexit(remove_all_cleanup_files);
    cleanup_atexit = true;
  }
  pthread_mutex_unlock(&cleanup_lock);
}


AdminSocket::AdminSocket(CephContext *cct)
  : m_cct(cct),
    m_sock_fd(-1),
    m_shutdown_rd_fd(-1),
    m_shutdown_wr_fd(-1),
    in_hook(false),
    m_lock("AdminSocket::m_lock"),
    m_version_hook(NULL),
    m_help_hook(NULL),
    m_getdescs_hook(NULL)
{
}

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

#define PFL_SUCCESS ((void*)(intptr_t)0)
#define PFL_FAIL ((void*)(intptr_t)1)

std::string AdminSocket::create_shutdown_pipe(int *pipe_rd, int *pipe_wr)
{
  int pipefd[2];
  int ret = pipe_cloexec(pipefd);
  if (ret < 0) {
    ostringstream oss;
    oss << "AdminSocket::create_shutdown_pipe error: " << cpp_strerror(ret);
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
  VOID_TEMP_FAILURE_RETRY(close(m_shutdown_wr_fd));
  m_shutdown_wr_fd = -1;

  if (ret != 0) {
    ostringstream oss;
    oss << "AdminSocket::destroy_shutdown_pipe error: failed to write"
      "to thread shutdown pipe: error " << ret;
    return oss.str();
  }

  join();

  // Close read end. Doing this before join() blocks the listenter and prevents
  // joining.
  VOID_TEMP_FAILURE_RETRY(close(m_shutdown_rd_fd));
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
  int sock_fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if (sock_fd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "AdminSocket::bind_and_listen: "
	<< "failed to create socket: " << cpp_strerror(err);
    return oss.str();
  }
  int r = fcntl(sock_fd, F_SETFD, FD_CLOEXEC);
  if (r < 0) {
    r = errno;
    VOID_TEMP_FAILURE_RETRY(::close(sock_fd));
    ostringstream oss;
    oss << "AdminSocket::bind_and_listen: failed to fcntl on socket: " << cpp_strerror(r);
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
	VOID_TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
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
    VOID_TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
    return oss.str();
  }
  *fd = sock_fd;
  return "";
}

void* AdminSocket::entry()
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
      return PFL_FAIL;
    }

    if (fds[0].revents & POLLIN) {
      // Send out some data
      do_accept();
    }
    if (fds[1].revents & POLLIN) {
      // Parent wants us to shut down
      return PFL_SUCCESS;
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

bool AdminSocket::do_accept()
{
  struct sockaddr_un address;
  socklen_t address_length = sizeof(address);
  ldout(m_cct, 30) << "AdminSocket: calling accept" << dendl;
  int connection_fd = accept(m_sock_fd, (struct sockaddr*) &address,
			     &address_length);
  ldout(m_cct, 30) << "AdminSocket: finished accept" << dendl;
  if (connection_fd < 0) {
    int err = errno;
    lderr(m_cct) << "AdminSocket: do_accept error: '"
			   << cpp_strerror(err) << dendl;
    return false;
  }

  char cmd[1024];
  int pos = 0;
  string c;
  while (1) {
    int ret = safe_read(connection_fd, &cmd[pos], 1);
    if (ret <= 0) {
      lderr(m_cct) << "AdminSocket: error reading request code: "
		   << cpp_strerror(ret) << dendl;
      close(connection_fd);
      return false;
    }
    //ldout(m_cct, 0) << "AdminSocket read byte " << (int)cmd[pos] << " pos " << pos << dendl;
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
    pos++;
  }

  bool rval = false;

  map<string, cmd_vartype> cmdmap;
  string format;
  vector<string> cmdvec;
  stringstream errss;
  cmdvec.push_back(cmd);
  if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
    ldout(m_cct, 0) << "AdminSocket: " << errss.rdbuf() << dendl;
    return false;
  }
  cmd_getval(m_cct, cmdmap, "format", format);
  if (format != "json" && format != "json-pretty" &&
      format != "xml" && format != "xml-pretty")
    format = "json-pretty";
  cmd_getval(m_cct, cmdmap, "prefix", c);

  m_lock.Lock();
  map<string,AdminSocketHook*>::iterator p;
  string match = c;
  while (match.size()) {
    p = m_hooks.find(match);
    if (p != m_hooks.end())
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

  bufferlist out;
  if (p == m_hooks.end()) {
    lderr(m_cct) << "AdminSocket: request '" << c << "' not defined" << dendl;
  } else {
    string args;
    if (match != c) {
      args = c.substr(match.length() + 1);
    }

    // Drop lock to avoid cycles in cases where the hook takes
    // the same lock that was held during calls to register/unregister,
    // and set in_hook to allow unregister to wait for us before
    // removing this hook.
    in_hook = true;
    auto match_hook = p->second;
    m_lock.Unlock();
    bool success = match_hook->call(match, cmdmap, format, out);
    m_lock.Lock();
    in_hook = false;
    in_hook_cond.Signal();

    if (!success) {
      ldout(m_cct, 0) << "AdminSocket: request '" << match << "' args '" << args
		      << "' to " << p->second << " failed" << dendl;
      out.append("failed");
    } else {
      ldout(m_cct, 5) << "AdminSocket: request '" << match << "' '" << args
		       << "' to " << p->second
		       << " returned " << out.length() << " bytes" << dendl;
    }
    uint32_t len = htonl(out.length());
    int ret = safe_write(connection_fd, &len, sizeof(len));
    if (ret < 0) {
      lderr(m_cct) << "AdminSocket: error writing response length "
		   << cpp_strerror(ret) << dendl;
    } else {
      if (out.write_fd(connection_fd) >= 0)
	rval = true;
    }
  }
  m_lock.Unlock();

  VOID_TEMP_FAILURE_RETRY(close(connection_fd));
  return rval;
}

int AdminSocket::register_command(std::string command, std::string cmddesc, AdminSocketHook *hook, std::string help)
{
  int ret;
  m_lock.Lock();
  if (m_hooks.count(command)) {
    ldout(m_cct, 5) << "register_command " << command << " hook " << hook << " EEXIST" << dendl;
    ret = -EEXIST;
  } else {
    ldout(m_cct, 5) << "register_command " << command << " hook " << hook << dendl;
    m_hooks[command] = hook;
    m_descs[command] = cmddesc;
    m_help[command] = help;
    ret = 0;
  }  
  m_lock.Unlock();
  return ret;
}

int AdminSocket::unregister_command(std::string command)
{
  int ret;
  m_lock.Lock();
  if (m_hooks.count(command)) {
    ldout(m_cct, 5) << "unregister_command " << command << dendl;
    m_hooks.erase(command);
    m_descs.erase(command);
    m_help.erase(command);

    // If we are currently processing a command, wait for it to
    // complete in case it referenced the hook that we are
    // unregistering.
    if (in_hook) {
      in_hook_cond.Wait(m_lock);
    }

    ret = 0;
  } else {
    ldout(m_cct, 5) << "unregister_command " << command << " ENOENT" << dendl;
    ret = -ENOENT;
  }  
  m_lock.Unlock();
  return ret;
}

class VersionHook : public AdminSocketHook {
public:
  virtual bool call(std::string command, cmdmap_t &cmdmap, std::string format,
		    bufferlist& out) {
    if (command == "0") {
      out.append(CEPH_ADMIN_SOCK_VERSION);
    } else {
      JSONFormatter jf;
      jf.open_object_section("version");
      if (command == "version")
	jf.dump_string("version", ceph_version_to_str());
      else if (command == "git_version")
	jf.dump_string("git_version", git_version_to_str());
      ostringstream ss;
      jf.close_section();
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
  bool call(string command, cmdmap_t &cmdmap, string format, bufferlist& out) {
    Formatter *f = Formatter::create(format, "json-pretty", "json-pretty");
    f->open_object_section("help");
    for (map<string,string>::iterator p = m_as->m_help.begin();
	 p != m_as->m_help.end();
	 ++p) {
      if (p->second.length())
	f->dump_string(p->first.c_str(), p->second);
    }
    f->close_section();
    ostringstream ss;
    f->flush(ss);
    out.append(ss.str());
    delete f;
    return true;
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as(as) {}
  bool call(string command, cmdmap_t &cmdmap, string format, bufferlist& out) {
    int cmdnum = 0;
    JSONFormatter jf(false);
    jf.open_object_section("command_descriptions");
    for (map<string,string>::iterator p = m_as->m_descs.begin();
	 p != m_as->m_descs.end();
	 ++p) {
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(&jf,
				secname.str().c_str(),
				p->second.c_str(),
				m_as->m_help[p->first]);
      cmdnum++;
    }
    jf.close_section(); // command_descriptions
    ostringstream ss;
    jf.flush(ss);
    out.append(ss.str());
    return true;
  }
};

bool AdminSocket::init(const std::string &path)
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

  m_version_hook = new VersionHook;
  register_command("0", "0", m_version_hook, "");
  register_command("version", "version", m_version_hook, "get ceph version");
  register_command("git_version", "git_version", m_version_hook, "get git sha1");
  m_help_hook = new HelpHook(this);
  register_command("help", "help", m_help_hook, "list available commands");
  m_getdescs_hook = new GetdescsHook(this);
  register_command("get_command_descriptions", "get_command_descriptions",
		   m_getdescs_hook, "list available commands");

  create("admin_socket");
  add_cleanup_file(m_path.c_str());
  return true;
}

void AdminSocket::shutdown()
{
  std::string err;

  // Under normal operation this is unlikely to occur.  However for some unit
  // tests, some object members are not initialized and so cannot be deleted
  // without fault.
  if (m_shutdown_wr_fd < 0)
    return;

  ldout(m_cct, 5) << "shutdown" << dendl;

  err = destroy_shutdown_pipe();
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocket::shutdown: error: " << err << dendl;
  }

  VOID_TEMP_FAILURE_RETRY(close(m_sock_fd));

  unregister_command("version");
  unregister_command("git_version");
  unregister_command("0");
  delete m_version_hook;

  unregister_command("help");
  delete m_help_hook;

  unregister_command("get_command_descriptions");
  delete m_getdescs_hook;

  remove_cleanup_file(m_path.c_str());
  m_path.clear();
}
