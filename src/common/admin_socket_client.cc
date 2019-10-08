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

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "common/admin_socket.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/admin_socket_client.h"

#include "include/compat.h"
#include "include/sock_compat.h"

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

static std::string asok_connect(const std::string &path, int *fd)
{
  int socket_fd = socket_cloexec(PF_UNIX, SOCK_STREAM, 0);
  if(socket_fd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "socket(PF_UNIX, SOCK_STREAM, 0) failed: " << cpp_strerror(err);
    return oss.str();
  }

  struct sockaddr_un address;
  // FIPS zeroization audit 20191115: this memset is fine.
  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, sizeof(address.sun_path), "%s", path.c_str());

  if (::connect(socket_fd, (struct sockaddr *) &address, 
	sizeof(struct sockaddr_un)) != 0) {
    int err = errno;
    ostringstream oss;
    oss << "connect(" << socket_fd << ") failed: " << cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }

  struct timeval timer;
  timer.tv_sec = 10;
  timer.tv_usec = 0;
  if (::setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timer, sizeof(timer))) {
    int err = errno;
    ostringstream oss;
    oss << "setsockopt(" << socket_fd << ", SO_RCVTIMEO) failed: "
	<< cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }
  timer.tv_sec = 10;
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
