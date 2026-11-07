// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

// Test helpers for the runtime minimum-part-size override
// (admin commands get-min-part-size / set-min-part-size).

#include "constants.hpp"
#include "service_impl.hpp"

#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace kvrgw {
namespace testutil {

// Sends one line to the admin socket and returns the reply (without '\n').
inline std::string admin_cmd(const std::string &socket_path, const std::string &line)
{
  const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return "ERROR socket";
  }
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  socket_path.copy(addr.sun_path, sizeof(addr.sun_path) - 1);
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(fd);
    return "ERROR connect";
  }
  const std::string out = line + "\n";
  (void)!write(fd, out.data(), out.size());
  std::string reply;
  char buf[256];
  ssize_t n;
  while ((n = read(fd, buf, sizeof(buf))) > 0) {
    reply.append(buf, static_cast<size_t>(n));
    if (reply.find('\n') != std::string::npos) {
      break;
    }
  }
  close(fd);
  while (!reply.empty() && (reply.back() == '\n' || reply.back() == '\r')) {
    reply.pop_back();
  }
  return reply;
}

// Sets the minimum non-last part size on construction and restores the
// default on every exit path, so a failing test cannot leave the small value.
class MinPartSizeGuard {
 public:
  MinPartSizeGuard(KvRgwServiceImpl &svc, uint64_t bytes) : svc_(svc)
  {
    svc_.min_part_size().store(bytes, std::memory_order_relaxed);
  }
  ~MinPartSizeGuard()
  {
    svc_.min_part_size().store(kMinPartSizeBytes, std::memory_order_relaxed);
  }
  MinPartSizeGuard(const MinPartSizeGuard &) = delete;
  MinPartSizeGuard &operator=(const MinPartSizeGuard &) = delete;

 private:
  KvRgwServiceImpl &svc_;
};

}  // namespace testutil
}  // namespace kvrgw
