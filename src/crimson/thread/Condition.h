// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/reactor.hh>
#include <sys/eventfd.h>

namespace crimson::thread {

/// a synchronization primitive can be used to block a seastar thread, until
/// another thread notifies it.
class Condition {
  seastar::file_desc file_desc;
  int fd;
  seastar::pollable_fd_state fd_state;
  eventfd_t event = 0;
public:
  Condition()
    : file_desc{seastar::file_desc::eventfd(0, 0)},
      fd(file_desc.get()),
      fd_state{std::move(file_desc)}
  {}
  seastar::future<> wait() {
    return seastar::engine().read_some(fd_state, &event, sizeof(event))
      .then([](size_t) {
	  return seastar::now();
	});
  }
  void notify() {
    eventfd_t result = 1;
    ::eventfd_write(fd, result);
  }
};

} // namespace crimson::thread
