// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_dmclock_queue.h"

namespace rgw::dmclock {

void PriorityQueue::cancel()
{
  queue.remove_by_req_filter([] (RequestRef&& request) {
      auto c = static_cast<Completion*>(request.release());
      Completion::dispatch(std::unique_ptr<Completion>{c},
                           boost::asio::error::operation_aborted,
                           PhaseType::priority);
      return true;
    });
  timer.cancel();
}

void PriorityQueue::cancel(const client_id& client)
{
  queue.remove_by_client(client, false, [] (RequestRef&& request) {
      auto c = static_cast<Completion*>(request.release());
      Completion::dispatch(std::unique_ptr<Completion>{c},
                           boost::asio::error::operation_aborted,
                           PhaseType::priority);
    });
  schedule(crimson::dmclock::TimeZero);
}

void PriorityQueue::schedule(const Time& time)
{
  timer.expires_at(Clock::from_double(time));
  timer.async_wait([this] (boost::system::error_code ec) {
      // process requests unless the wait was canceled. note that a canceled
      // wait may execute after this PriorityQueue destructs
      if (ec != boost::asio::error::operation_aborted) {
        process(get_time());
      }
    });
}

void PriorityQueue::process(const Time& now)
{
  // must run in the executor. we should only invoke completion handlers if the
  // executor is running
  assert(get_executor().running_in_this_thread());

  for (;;) {
    auto pull = queue.pull_request(now);

    if (pull.is_none()) {
      // no pending requests, cancel the timer
      timer.cancel();
      return;
    }
    if (pull.is_future()) {
      // update the timer based on the future time
      schedule(pull.getTime());
      return;
    }

    // complete the request
    auto& r = pull.get_retn();
    auto result = r.phase;
    auto c = static_cast<Completion*>(r.request.release());
    Completion::post(std::unique_ptr<Completion>{c},
                     boost::system::error_code{}, result);
  }
}

} // namespace rgw::dmclock
