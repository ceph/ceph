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
#include <unistd.h>

#include "common/OutputDataSocket.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/safe_io.h"
#include "include/compat.h"
#include "include/sock_compat.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

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


OutputDataSocket::OutputDataSocket(CephContext *cct, uint64_t _backlog)
  : m_cct(cct),
    data_max_backlog(_backlog),
    m_sock_fd(-1),
    m_shutdown_rd_fd(-1),
    m_shutdown_wr_fd(-1),
    going_down(false),
    data_size(0),
    skipped(0)
{
}

OutputDataSocket::~OutputDataSocket()
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
 * OutputDataSocketConfigObs class to join() it.
 */

#define PFL_SUCCESS ((void*)(intptr_t)0)
#define PFL_FAIL ((void*)(intptr_t)1)

std::string OutputDataSocket::create_shutdown_pipe(int *pipe_rd, int *pipe_wr)
{
  int pipefd[2];
  if (pipe_cloexec(pipefd, 0) < 0) {
    int e = errno;
    ostringstream oss;
    oss << "OutputDataSocket::create_shutdown_pipe error: " << cpp_strerror(e);
    return oss.str();
  }
  
  *pipe_rd = pipefd[0];
  *pipe_wr = pipefd[1];
  return "";
}

std::string OutputDataSocket::bind_and_listen(const std::string &sock_path, int *fd)
{
  ldout(m_cct, 5) << "bind_and_listen " << sock_path << dendl;

  struct sockaddr_un address;
  if (sock_path.size() > sizeof(address.sun_path) - 1) {
    ostringstream oss;
    oss << "OutputDataSocket::bind_and_listen: "
	<< "The UNIX domain socket path " << sock_path << " is too long! The "
	<< "maximum length on this system is "
	<< (sizeof(address.sun_path) - 1);
    return oss.str();
  }
  int sock_fd = socket_cloexec(PF_UNIX, SOCK_STREAM, 0);
  if (sock_fd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "OutputDataSocket::bind_and_listen: "
	<< "failed to create socket: " << cpp_strerror(err);
    return oss.str();
  }
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, sizeof(address.sun_path),
	   "%s", sock_path.c_str());
  if (::bind(sock_fd, (struct sockaddr*)&address,
	   sizeof(struct sockaddr_un)) != 0) {
    int err = errno;
    if (err == EADDRINUSE) {
      // The old UNIX domain socket must still be there.
      // Let's unlink it and try again.
      VOID_TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
      if (::bind(sock_fd, (struct sockaddr*)&address,
	       sizeof(struct sockaddr_un)) == 0) {
	err = 0;
      }
      else {
	err = errno;
      }
    }
    if (err != 0) {
      ostringstream oss;
      oss << "OutputDataSocket::bind_and_listen: "
	  << "failed to bind the UNIX domain socket to '" << sock_path
	  << "': " << cpp_strerror(err);
      close(sock_fd);
      return oss.str();
    }
  }
  if (listen(sock_fd, 5) != 0) {
    int err = errno;
    ostringstream oss;
    oss << "OutputDataSocket::bind_and_listen: "
	  << "failed to listen to socket: " << cpp_strerror(err);
    close(sock_fd);
    VOID_TEMP_FAILURE_RETRY(unlink(sock_path.c_str()));
    return oss.str();
  }
  *fd = sock_fd;
  return "";
}

void* OutputDataSocket::entry()
{
  ldout(m_cct, 5) << "entry start" << dendl;
  while (true) {
    struct pollfd fds[2];
    // FIPS zeroization audit 20191115: this memset is not security related.
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
      lderr(m_cct) << "OutputDataSocket: poll(2) error: '"
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

  return PFL_SUCCESS; // unreachable
}


bool OutputDataSocket::do_accept()
{
  struct sockaddr_un address;
  socklen_t address_length = sizeof(address);
  ldout(m_cct, 30) << "OutputDataSocket: calling accept" << dendl;
  int connection_fd = accept_cloexec(m_sock_fd, (struct sockaddr*) &address,
			     &address_length);
  if (connection_fd < 0) {
    int err = errno;
    lderr(m_cct) << "OutputDataSocket: do_accept error: '"
			   << cpp_strerror(err) << dendl;
    return false;
  }
  ldout(m_cct, 30) << "OutputDataSocket: finished accept" << dendl;

  handle_connection(connection_fd);
  close_connection(connection_fd);

  return 0;
}

void OutputDataSocket::handle_connection(int fd)
{
  ceph::buffer::list bl;

  m_lock.lock();
  init_connection(bl);
  m_lock.unlock();

  if (bl.length()) {
    /* need to special case the connection init buffer output, as it needs
     * to be dumped before any data, including older data that was sent
     * before the connection was established, or before we identified
     * older connection was broken
     */
    int ret = safe_write(fd, bl.c_str(), bl.length());
    if (ret < 0) {
      return;
    }
  }

  int ret = dump_data(fd);
  if (ret < 0)
    return;

  do {
    {
      std::unique_lock l(m_lock);
      if (!going_down) {
	cond.wait(l);
      }
      if (going_down) {
	break;
      }
    }
    ret = dump_data(fd);
  } while (ret >= 0);
}

int OutputDataSocket::dump_data(int fd)
{
  m_lock.lock();
  auto l = std::move(data);
  data.clear();
  data_size = 0;
  m_lock.unlock();

  for (auto iter = l.begin(); iter != l.end(); ++iter) {
    ceph::buffer::list& bl = *iter;
    int ret = safe_write(fd, bl.c_str(), bl.length());
    if (ret >= 0) {
      ret = safe_write(fd, delim.c_str(), delim.length());
    }
    if (ret < 0) {
      std::scoped_lock lock(m_lock);
      for (; iter != l.end(); ++iter) {
        ceph::buffer::list& bl = *iter;
	data.push_back(bl);
	data_size += bl.length();
      }
      return ret;
    }
  }

  return 0;
}

void OutputDataSocket::close_connection(int fd)
{
  VOID_TEMP_FAILURE_RETRY(close(fd));
}

bool OutputDataSocket::init(const std::string &path)
{
  ldout(m_cct, 5) << "init " << path << dendl;

  /* Set up things for the new thread */
  std::string err;
  int pipe_rd = -1, pipe_wr = -1;
  err = create_shutdown_pipe(&pipe_rd, &pipe_wr);
  if (!err.empty()) {
    lderr(m_cct) << "OutputDataSocketConfigObs::init: error: " << err << dendl;
    return false;
  }
  int sock_fd;
  err = bind_and_listen(path, &sock_fd);
  if (!err.empty()) {
    lderr(m_cct) << "OutputDataSocketConfigObs::init: failed: " << err << dendl;
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }

  /* Create new thread */
  m_sock_fd = sock_fd;
  m_shutdown_rd_fd = pipe_rd;
  m_shutdown_wr_fd = pipe_wr;
  m_path = path;
  create("out_data_socket");
  add_cleanup_file(m_path.c_str());
  return true;
}

void OutputDataSocket::shutdown()
{
  m_lock.lock();
  going_down = true;
  cond.notify_all();
  m_lock.unlock();

  if (m_shutdown_wr_fd < 0)
    return;

  ldout(m_cct, 5) << "shutdown" << dendl;

  // Send a byte to the shutdown pipe that the thread is listening to
  char buf[1] = { 0x0 };
  int ret = safe_write(m_shutdown_wr_fd, buf, sizeof(buf));
  VOID_TEMP_FAILURE_RETRY(close(m_shutdown_wr_fd));
  m_shutdown_wr_fd = -1;

  if (ret == 0) {
    join();
  } else {
    lderr(m_cct) << "OutputDataSocket::shutdown: failed to write "
      "to thread shutdown pipe: error " << ret << dendl;
  }

  remove_cleanup_file(m_path.c_str());
  m_path.clear();
}

void OutputDataSocket::append_output(ceph::buffer::list& bl)
{
  std::lock_guard l(m_lock);

  if (data_size + bl.length() > data_max_backlog) {
    if (skipped % 100 == 0) {
      ldout(m_cct, 0) << "dropping data output, max backlog reached (skipped=="
		      << skipped << ")"
		      << dendl;
      skipped = 1;
    } else
      ++skipped;

    cond.notify_all();
    return;
  }

  data.push_back(bl);
  data_size += bl.length();
  cond.notify_all();
}
