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

#include "test/crimson/ctest_utils.h"

struct SeastarRunner {
  static constexpr eventfd_t APP_RUNNING = 1;
  static constexpr eventfd_t APP_NOT_RUN = 2;

  seastar::app_template app;
  seastar::file_desc begin_fd;
  std::unique_ptr<seastar::readable_eventfd> on_end;

  std::thread thread;

  bool begin_signaled = false;

  SeastarRunner() :
    app{get_smp_opts_from_ctest()}, begin_fd{seastar::file_desc::eventfd(0, 0)} {}

  ~SeastarRunner() {}

  bool is_running() const {
    return !!on_end;
  }

  int init(int argc, char **argv)
  {
    thread = std::thread([argc, argv, this] { reactor(argc, argv); });
    eventfd_t result;
    if (int r = ::eventfd_read(begin_fd.get(), &result); r < 0) {
      std::cerr << "unable to eventfd_read():" << errno << std::endl;
      return r;
    }
    assert(begin_signaled == true);
    if (result == APP_RUNNING) {
      assert(is_running());
      return 0;
    } else {
      assert(result == APP_NOT_RUN);
      assert(!is_running());
      return 1;
    }
  }
  
  void stop()
  {
    if (is_running()) {
      run([this] {
        on_end->write_side().signal(1);
        return seastar::now();
      });
    }
    thread.join();
  }

  void reactor(int argc, char **argv)
  {
    auto ret = app.run(argc, argv, [this] {
      on_end.reset(new seastar::readable_eventfd);
      return seastar::now().then([this] {
	begin_signaled = true;
	[[maybe_unused]] auto r = ::eventfd_write(begin_fd.get(), APP_RUNNING);
	assert(r == 0);
	return seastar::now();
      }).then([this] {
	return on_end->wait().then([](size_t){});
      }).handle_exception([](auto ep) {
	std::cerr << "Error: " << ep << std::endl;
      }).finally([this] {
	on_end.reset();
      });
    });
    if (ret != 0) {
      std::cerr << "Seastar app returns " << ret << std::endl;
    }
    if (!begin_signaled) {
      begin_signaled = true;
      ::eventfd_write(begin_fd.get(), APP_NOT_RUN);
    }
  }

  template <typename Func>
  void run(Func &&func) {
    assert(is_running());
    auto fut = seastar::alien::submit_to(app.alien(), 0,
					 std::forward<Func>(func));
    fut.get();
  }
};


