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

#include "common/admin_socket.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/admin_socket_client.h"
#include "include/compat.h"

#include <boost/filesystem/path.hpp>

#include <sys/un.h>
#include <sys/wait.h>

using std::ostringstream;

const char* get_rand_socket_path()
{
  static char *g_socket_path = NULL;

  if (g_socket_path == NULL) {
    char buf[512];
    const char *tdir = getenv("TMPDIR");
    if (tdir == NULL) {
      tdir = "/tmp";
    }
    snprintf(buf, sizeof(((struct sockaddr_un*)0)->sun_path),
	     "%s/perfcounters_test_socket.%ld.%ld",
	     tdir, (long int)getpid(), time(NULL));
    g_socket_path = (char*)strdup(buf);
  }
  return g_socket_path;
}

static std::string do_connect(int fd, const std::string &path)
{
  struct sockaddr_un address;
  if (path.size() > sizeof(address.sun_path) - 1) {
    ostringstream oss;
    oss << "AdminSocket::: "
	<< "The UNIX domain socket path " << path << " is too long! The "
	<< "maximum length on this system is "
	<< (sizeof(address.sun_path) - 1);
    return oss.str();
  }
  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, sizeof(address.sun_path),
	   "%s", path.c_str());
  if (::connect(fd, (struct sockaddr*)&address, sizeof address) == -1) {
    int err = errno;
    ostringstream oss;
    oss << "connect(" << fd << ") failed: " << cpp_strerror(err);
    return oss.str();
  }

  return "";
}

static std::string asok_connect(const std::string &sock_path, int *fd)
{
  int socket_fd = ::socket(PF_UNIX, SOCK_STREAM, 0);
  if(socket_fd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "asok_connect: socket(PF_UNIX, SOCK_STREAM, 0) failed: " << cpp_strerror(err);
    return oss.str();
  }
  if (::fcntl(socket_fd, F_SETFD, FD_CLOEXEC) == -1) {
    int err = errno;
    VOID_TEMP_FAILURE_RETRY(::close(socket_fd));
    ostringstream oss;
    oss << "asok_connect: failed to fcntl on socket: " << cpp_strerror(err);
    return oss.str();
  }

  if (sock_path.size() < sizeof(((struct sockaddr_un*)0)->sun_path)) {
    const auto &status = do_connect(socket_fd, sock_path);
    if (status.size()) {
      VOID_TEMP_FAILURE_RETRY(::close(socket_fd));
      return status;
    }
  } else {
    int p[2];
    if (::pipe(p) == -1) {
      int err = errno;
      VOID_TEMP_FAILURE_RETRY(::close(socket_fd));
      ostringstream oss;
      oss << "asok_connect: failed to create pipe: " << cpp_strerror(err);
      return oss.str();
    }
    if (::fcntl(p[0], F_SETFD, FD_CLOEXEC) == -1 || ::fcntl(p[1], F_SETFD, FD_CLOEXEC) == -1) {
      int err = errno;
      VOID_TEMP_FAILURE_RETRY(::close(socket_fd));
      VOID_TEMP_FAILURE_RETRY(::close(p[0]));
      VOID_TEMP_FAILURE_RETRY(::close(p[1]));
      ostringstream oss;
      oss << "asok_connect: failed to fcntl on socket: " << cpp_strerror(err);
      return oss.str();
    }

    pid_t child = ::fork();
    if (child == 0) {
      VOID_TEMP_FAILURE_RETRY(::close(p[0]));
      boost::filesystem::path path(sock_path);
      auto dirname = path.parent_path();
      auto basename = path.filename();
      if (::chdir(dirname.c_str()) == -1) {
        int err = errno;
        ostringstream oss;
        oss << "asok_connect: failed to chdir to socket dir '" << dirname << "': " << cpp_strerror(err);
        const auto &s = oss.str();
        auto _ = safe_write(p[1], s.c_str(), s.size());
        (void)_;
        ::_exit(EXIT_FAILURE);
      }
      const auto &status = do_connect(socket_fd, basename.string());
      if (status.size() > 0) {
        auto _ = safe_write(p[1], status.c_str(), status.size());
        (void)_;
        ::_exit(EXIT_FAILURE);
      }
      ::_exit(EXIT_SUCCESS);
    } else if (child > 0) {
      int status;
      VOID_TEMP_FAILURE_RETRY(::close(p[1]));
      int result = TEMP_FAILURE_RETRY(::waitpid(child, &status, 0));
      if (result == -1) {
        int err = errno;
        VOID_TEMP_FAILURE_RETRY(::close(socket_fd));
        VOID_TEMP_FAILURE_RETRY(::close(p[0]));
        VOID_TEMP_FAILURE_RETRY(::unlink(sock_path.c_str()));
        ostringstream oss;
        oss << "bind_and_listen: failed to wait for helper process: " << cpp_strerror(err);
        return oss.str();
      }
      if (status) {
        char buf[4096];
        ssize_t r = safe_read(p[0], buf, sizeof buf);
        if (r > 0) {
          VOID_TEMP_FAILURE_RETRY(::close(socket_fd));
          VOID_TEMP_FAILURE_RETRY(::close(p[0]));
          VOID_TEMP_FAILURE_RETRY(::unlink(sock_path.c_str()));
          return std::string(buf, (size_t)r);
        }
      }
      VOID_TEMP_FAILURE_RETRY(::close(p[0]));
    } else {
      VOID_TEMP_FAILURE_RETRY(::close(p[0]));
      VOID_TEMP_FAILURE_RETRY(::close(p[1]));
      const auto &status = do_connect(socket_fd, sock_path);
      if (status.size()) {
        return status;
      }
    }
  }

  struct timeval timer;
  timer.tv_sec = 5;
  timer.tv_usec = 0;
  if (::setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timer, sizeof(timer))) {
    int err = errno;
    ostringstream oss;
    oss << "setsockopt(" << socket_fd << ", SO_RCVTIMEO) failed: "
	<< cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }
  timer.tv_sec = 5;
  timer.tv_usec = 0;
  if (::setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timer, sizeof(timer))) {
    int err = errno;
    ostringstream oss;
    oss << "setsockopt(" << socket_fd << ", SO_SNDTIMEO) failed: "
	<< cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }

  *fd = socket_fd;
  return "";
}

static std::string asok_request(int socket_fd, std::string request)
{
  ssize_t res = safe_write(socket_fd, request.c_str(), request.length() + 1);
  if (res < 0) {
    int err = res;
    ostringstream oss;
    oss << "safe_write(" << socket_fd << ") failed to write request code: "
	<< cpp_strerror(err);
    return oss.str();
  }
  return "";
}

AdminSocketClient::
AdminSocketClient(const std::string &path)
  : m_path(path)
{
}

std::string AdminSocketClient::ping(bool *ok)
{
  std::string version;
  std::string result = do_request("{\"prefix\":\"0\"}", &version);
  *ok = result == "" && version.length() == 1;
  return result;
}

std::string AdminSocketClient::do_request(std::string request, std::string *result)
{
  int socket_fd = 0, res;
  std::string buffer;
  uint32_t message_size_raw, message_size;

  std::string err = asok_connect(m_path, &socket_fd);
  if (!err.empty()) {
    goto out;
  }
  err = asok_request(socket_fd, request);
  if (!err.empty()) {
    goto done;
  }
  res = safe_read_exact(socket_fd, &message_size_raw,
				sizeof(message_size_raw));
  if (res < 0) {
    int e = res;
    ostringstream oss;
    oss << "safe_read(" << socket_fd << ") failed to read message size: "
	<< cpp_strerror(e);
    err = oss.str();
    goto done;
  }
  message_size = ntohl(message_size_raw);
  buffer.resize(message_size, 0);
  res = safe_read_exact(socket_fd, &buffer[0], message_size);
  if (res < 0) {
    int e = res;
    ostringstream oss;
    oss << "safe_read(" << socket_fd << ") failed: " << cpp_strerror(e);
    err = oss.str();
    goto done;
  }
  //printf("MESSAGE FROM SERVER: %s\n", buffer.c_str());
  std::swap(*result, buffer);
done:
  close(socket_fd);
 out:
  return err;
}
