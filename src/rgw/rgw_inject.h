// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <chrono>
#include <string_view>
#include <thread>

#include <boost/asio/basic_waitable_timer.hpp>

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/dout.h"

// for testing: wait at a named point when rgw_inject_delay_pattern names
// it and rgw_inject_delay_sec is positive. A request coroutine waits on a
// timer, so the frontend keeps serving other requests meanwhile.
inline void rgw_inject_delay(const DoutPrefixProvider* dpp, optional_yield y,
                             std::string_view point)
{
  CephContext* cct = dpp->get_cct();
  const double delay_sec = cct->_conf->rgw_inject_delay_sec;
  if (delay_sec <= 0 ||
      std::string_view(cct->_conf->rgw_inject_delay_pattern) != point) {
    return;
  }
  ldpp_dout(dpp, 0) << "injecting delay of " << delay_sec << "s at " << point
                    << dendl;
  const auto dur = std::chrono::duration_cast<ceph::timespan>(
      std::chrono::duration<double>(delay_sec));
  if (y) {
    auto& yield = y.get_yield_context();
    boost::asio::basic_waitable_timer<ceph::coarse_mono_clock> timer(
        yield.get_executor(), dur);
    boost::system::error_code ec;
    timer.async_wait(yield[ec]);
  } else {
    std::this_thread::sleep_for(dur);
  }
}
