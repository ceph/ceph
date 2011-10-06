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

#include "common/Thread.h"
#include "common/admin_socket.h"
#include "common/config.h"
#include "common/config_obs.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/pipe.h"
#include "common/safe_io.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
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
  TEMP_FAILURE_RETRY(unlink(file));
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
    TEMP_FAILURE_RETRY(unlink(*i));
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

/*
 * This thread listens on the UNIX domain socket for incoming connections.
 * It only handles one connection at a time at the moment. All I/O is nonblocking,
 * so that we can implement sensible timeouts. [TODO: make all I/O nonblocking]
 *
 * This thread also listens to m_shutdown_fd. If there is any data sent to this
 * pipe, the thread terminates itself gracefully, allowing the
 * AdminSocketConfigObs class to join() it.
 */

#define PFL_SUCCESS ((void*)(intptr_t)0)
#define PFL_FAIL ((void*)(intptr_t)1)

class AdminSocket : public Thread
{
public:
  static std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr)
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

  static std::string bind_and_listen(const std::string &sock_path, int *fd)
  {
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
    fcntl(sock_fd, F_SETFD, FD_CLOEXEC);
    memset(&address, 0, sizeof(struct sockaddr_un));
    address.sun_family = AF_UNIX;
    snprintf(address.sun_path, sizeof(address.sun_path),
	     "%s", sock_path.c_str());
    if (bind(sock_fd, (struct sockaddr*)&address,
	       sizeof(struct sockaddr_un)) != 0) {
      int err = errno;
      if (err == EADDRINUSE) {
	// The old UNIX domain socket must still be there.
	// Let's unlink it and try again.
	TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
	if (bind(sock_fd, (struct sockaddr*)&address,
		   sizeof(struct sockaddr_un)) == 0) {
	  err = 0;
	}
	else {
	  err = errno;
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
      TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
      return oss.str();
    }
    *fd = sock_fd;
    return "";
  }

  AdminSocket(int sock_fd, int shutdown_fd, AdminSocketConfigObs *parent)
    : m_sock_fd(sock_fd),
      m_shutdown_fd(shutdown_fd),
      m_parent(parent)
  {
  }

  virtual ~AdminSocket()
  {
    if (m_sock_fd != -1)
      close(m_sock_fd);
    if (m_shutdown_fd != -1)
      close(m_shutdown_fd);
  }

  virtual void* entry()
  {
    while (true) {
      struct pollfd fds[2];
      memset(fds, 0, sizeof(fds));
      fds[0].fd = m_sock_fd;
      fds[0].events = POLLIN | POLLRDBAND;
      fds[1].fd = m_shutdown_fd;
      fds[1].events = POLLIN | POLLRDBAND;

      int ret = poll(fds, 2, -1);
      if (ret < 0) {
	int err = errno;
	if (err == EINTR) {
	  continue;
	}
	lderr(m_parent->m_cct) << "AdminSocket: poll(2) error: '"
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
  }


private:
  bool do_accept()
  {
    int ret;
    struct sockaddr_un address;
    socklen_t address_length = sizeof(address);
    ldout(m_parent->m_cct, 30) << "AdminSocket: calling accept" << dendl;
    int connection_fd = accept(m_sock_fd, (struct sockaddr*) &address,
				   &address_length);
    ldout(m_parent->m_cct, 30) << "AdminSocket: finished accept" << dendl;
    if (connection_fd < 0) {
      int err = errno;
      lderr(m_parent->m_cct) << "AdminSocket: do_accept error: '"
	  << cpp_strerror(err) << dendl;
      return false;
    }

    uint32_t request, request_raw;
    ret = safe_read(connection_fd, &request_raw, sizeof(request_raw));
    if (ret < 0) {
      lderr(m_parent->m_cct) << "AdminSocket: error reading request code: "
	  << cpp_strerror(ret) << dendl;
      close(connection_fd);
      return false;
    }
    request = ntohl(request_raw);
    switch (request) {
      case 0:
	/* version request */
	ret = handle_version_request(connection_fd);
	break;
      case 1:
	/* data request */
	ret = handle_json_request(connection_fd, false);
	break;
      case 2:
	/* schema request */
	ret = handle_json_request(connection_fd, true);
	break;
      default:
	lderr(m_parent->m_cct) << "AdminSocket: unknown request "
	    << "code " << request << dendl;
	ret = false;
	break;
    }
    TEMP_FAILURE_RETRY(close(connection_fd));
    return ret;
  }

  bool handle_version_request(int connection_fd)
  {
    uint32_t version_raw = htonl(CEPH_ADMIN_SOCK_VERSION);
    int ret = safe_write(connection_fd, &version_raw, sizeof(version_raw));
    if (ret < 0) {
      lderr(m_parent->m_cct) << "AdminSocket: error writing version_raw: "
	  << cpp_strerror(ret) << dendl;
      return false;
    }
    return true;
  }

  bool handle_json_request(int connection_fd, bool schema)
  {
    std::vector<char> buffer;
    buffer.reserve(512);

    PerfCountersCollection *coll = m_parent->m_cct->GetPerfCountersCollection();
    if (coll) {
      coll->write_json_to_buf(buffer, schema);
    }

    uint32_t len = htonl(buffer.size());
    int ret = safe_write(connection_fd, &len, sizeof(len));
    if (ret < 0) {
      lderr(m_parent->m_cct) << "AdminSocket: error writing message size: "
	  << cpp_strerror(ret) << dendl;
      return false;
    }
    ret = safe_write(connection_fd, &buffer[0], buffer.size());
    if (ret < 0) {
      lderr(m_parent->m_cct) << "AdminSocket: error writing message: "
	  << cpp_strerror(ret) << dendl;
      return false;
    }
    ldout(m_parent->m_cct, 30) << "AdminSocket: handle_json_request succeeded."
	 << dendl;
    return true;
  }

  AdminSocket(AdminSocket &rhs);
  const AdminSocket &operator=(const AdminSocket &rhs);

  int m_sock_fd;
  int m_shutdown_fd;
  AdminSocketConfigObs *m_parent;
};

/*
 * The AdminSocketConfigObs receives callbacks from the configuration
 * management system. It will create the AdminSocket thread when the
 * appropriate configuration is set.
 */
AdminSocketConfigObs::
AdminSocketConfigObs(CephContext *cct)
  : m_cct(cct),
    m_thread(NULL),
    m_shutdown_fd(-1)
{
}

AdminSocketConfigObs::
~AdminSocketConfigObs()
{
  shutdown();
}

const char** AdminSocketConfigObs::
get_tracked_conf_keys() const
{
  static const char *KEYS[] = { "admin_socket",
	  "internal_safe_to_start_threads",
	  NULL
  };
  return KEYS;
}

void AdminSocketConfigObs::
handle_conf_change(const md_config_t *conf,
		   const std::set <std::string> &changed)
{
  if (!conf->internal_safe_to_start_threads) {
    // We can't do anything until it's safe to start threads.
    return;
  }
  shutdown();
  if (conf->admin_socket.empty()) {
    // The admin socket is disabled.
    return;
  }
  if (!init(conf->admin_socket)) {
    lderr(m_cct) << "AdminSocketConfigObs: failed to start AdminSocket" << dendl;
  }
}

bool AdminSocketConfigObs::
init(const std::string &path)
{
  /* Set up things for the new thread */
  std::string err;
  int pipe_rd = -1, pipe_wr = -1;
  err = AdminSocket::create_shutdown_pipe(&pipe_rd, &pipe_wr);
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocketConfigObs::init: error: " << err << dendl;
    return false;
  }
  int sock_fd;
  err = AdminSocket::bind_and_listen(path, &sock_fd);
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocketConfigObs::init: failed: " << err << dendl;
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }

  /* Create new thread */
  m_thread = new (std::nothrow) AdminSocket(sock_fd, pipe_rd, this);
  if (!m_thread) {
    lderr(m_cct) << "AdminSocketConfigObs::init: failed: " << err << dendl;
    TEMP_FAILURE_RETRY(unlink(path.c_str()));
    close(sock_fd);
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }
  m_path = path;
  m_thread->create();
  m_shutdown_fd = pipe_wr;
  add_cleanup_file(m_path.c_str());
  return true;
}

void AdminSocketConfigObs::
shutdown()
{
  if (!m_thread)
    return;
  // Send a byte to the shutdown pipe that the thread is listening to
  char buf[1] = { 0x0 };
  int ret = safe_write(m_shutdown_fd, buf, sizeof(buf));
  TEMP_FAILURE_RETRY(close(m_shutdown_fd));
  m_shutdown_fd = -1;

  if (ret == 0) {
    // Join and delete the thread
    m_thread->join();
    delete m_thread;
  }
  else {
    lderr(m_cct) << "AdminSocketConfigObs::shutdown: failed to write "
	    "to thread shutdown pipe: error " << ret << dendl;
  }
  m_thread = NULL;
  remove_cleanup_file(m_path.c_str());
  m_path.clear();
}
