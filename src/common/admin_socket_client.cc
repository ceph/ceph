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
#include "common/ceph_context.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/admin_socket_client.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <map>
#include <poll.h>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#include <vector>

using std::ostringstream;

/* Helper class used to time out the client after a certain amount of time
 * passes.
 */
class Alarm
{
public:
  Alarm(int s) {
    alarm(s);
  }
  ~Alarm() {
    alarm(0);
  }
};

AdminSocketClient::
AdminSocketClient(const std::string &path)
  : m_path(path)
{
}

std::string AdminSocketClient::
get_message(std::string *message)
{
  Alarm my_alarm(300);

  int socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if(socket_fd < 0) {
    int err = errno;
    ostringstream oss;
    oss << "socket(PF_UNIX, SOCK_STREAM, 0) failed: " << cpp_strerror(err);
    return oss.str();
  }

  struct sockaddr_un address;
  memset(&address, 0, sizeof(struct sockaddr_un));
  address.sun_family = AF_UNIX;
  snprintf(address.sun_path, sizeof(address.sun_path), "%s", m_path.c_str());

  if (connect(socket_fd, (struct sockaddr *) &address, 
	sizeof(struct sockaddr_un)) != 0) {
    int err = errno;
    ostringstream oss;
    oss << "connect(" << socket_fd << ") failed: " << cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }

  std::vector<uint8_t> vec(65536, 0);
  uint8_t *buffer = &vec[0];

  uint32_t request = htonl(0x1);
  ssize_t res = safe_write(socket_fd, &request, sizeof(request));
  if (res < 0) {
    int err = res;
    ostringstream oss;
    oss << "safe_write(" << socket_fd << ") failed to write request code: "
	<< cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }

  uint32_t message_size_raw;
  res = safe_read_exact(socket_fd, &message_size_raw,
				sizeof(message_size_raw));
  if (res < 0) {
    int err = res;
    ostringstream oss;
    oss << "safe_read(" << socket_fd << ") failed to read message size: "
	<< cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }
  uint32_t message_size = ntohl(message_size_raw);
  res = safe_read_exact(socket_fd, buffer, message_size);
  if (res < 0) {
    int err = res;
    ostringstream oss;
    oss << "safe_read(" << socket_fd << ") failed: " << cpp_strerror(err);
    close(socket_fd);
    return oss.str();
  }

  //printf("MESSAGE FROM SERVER: %s\n", buffer);
  message->assign((const char*)buffer);
  close(socket_fd);
  return "";
}
