// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>

#include <boost/asio/io_context.hpp>

#include "rgw_frontend.h"
#define REQUEST_TIMEOUT 65000

#define BACKOFF_MAX_ROUND 60
#define BACKOFF_MAX_WAIT 1000 // ms

class RGWAsioBackoff {
  int cur_wait;
  int max_msecs;
  int try_cnt;

  void update_wait_time();
public:
  explicit RGWAsioBackoff(int _max_msecs = BACKOFF_MAX_WAIT) :
                          cur_wait(1), max_msecs(_max_msecs), try_cnt(0) {}

  void backoff_sleep();
  void reset() {
    cur_wait = 1;
    try_cnt = 0;
  }

  bool is_backoff_done() const { return (try_cnt >= BACKOFF_MAX_ROUND); }
};

class RGWAsioFrontend : public RGWFrontend {
  class Impl;
  std::unique_ptr<Impl> impl;
public:
  RGWAsioFrontend(RGWProcessEnv& env, RGWFrontendConfig* conf,
		  rgw::dmclock::SchedulerCtx& sched_ctx,
		  boost::asio::io_context& io_context);
  ~RGWAsioFrontend() override;

  int init() override;
  int run() override;
  void stop() override;
  void join() override;

  void pause_for_new_config() override;
  void unpause_with_new_config() override;
};
