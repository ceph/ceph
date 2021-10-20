// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <stdio.h>
#include <signal.h>
#include <thread>

#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/thread.hh>

struct SeastarRunner {
  seastar::app_template app;
  seastar::file_desc begin_fd;
  std::unique_ptr<seastar::readable_eventfd> on_end;

  std::thread thread;

  SeastarRunner() :
    begin_fd{seastar::file_desc::eventfd(0, 0)} {}

  ~SeastarRunner() {}

  void init(int argc, char **argv)
  {
    thread = std::thread([argc, argv, this] { reactor(argc, argv); });
    eventfd_t result = 0;
    if (int r = ::eventfd_read(begin_fd.get(), &result); r < 0) {
      std::cerr << "unable to eventfd_read():" << errno << std::endl;
      throw std::runtime_error("Cannot start seastar");
    }
  }
  
  void stop()
  {
    run([this] {
      on_end->write_side().signal(1);
      return seastar::now();
    });
    thread.join();
  }

  void reactor(int argc, char **argv)
  {
    app.run(argc, argv, [this] {
      on_end.reset(new seastar::readable_eventfd);
      return seastar::now().then([this] {
	::eventfd_write(begin_fd.get(), 1);
	  return seastar::now();
      }).then([this] {
	return on_end->wait().then([](size_t){});
      }).handle_exception([](auto ep) {
	std::cerr << "Error: " << ep << std::endl;
      }).finally([this] {
	on_end.reset();
      });
    });
  }

  template <typename Func>
  void run(Func &&func) {
    auto fut = seastar::alien::submit_to(app.alien(), 0,
					 std::forward<Func>(func));
    fut.get();
  }
};


