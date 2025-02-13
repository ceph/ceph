// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "reshard_writer.h"
#include <boost/asio/execution.hpp>
#include <boost/asio/query.hpp>

namespace rgwrados::reshard {

BaseWriter::BaseWriter(const executor_type& ex, uint64_t max_aio)
  : svc(boost::asio::use_service<service_type>(
          boost::asio::query(ex, boost::asio::execution::context))),
    ex(ex), max_aio(max_aio)
{
  // register for service_shutdown() notifications
  svc.add(*this);
}

BaseWriter::~BaseWriter()
{
  svc.remove(*this);
}

void BaseWriter::drain(boost::asio::yield_context yield)
{
  if (outstanding > 0) {
    Waiter waiter;
    drain_waiters.push_back(waiter);
    waiter.async_wait(yield); // may rethrow error from previous completion
  } else if (error) {
    // make sure errors get reported to the caller
    throw boost::system::system_error(error);
  }
}

void BaseWriter::on_complete(boost::system::error_code ec)
{
  --outstanding;

  constexpr auto complete_all = [] (boost::intrusive::list<Waiter> waiters,
                                    boost::system::error_code ec) {
      while (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.complete(ec);
      }
    };

  if (ec) {
    if (!error) {
      error = ec;
    }
    // fail all waiting calls to write()
    complete_all(std::move(write_waiters), ec);
  } else if (!write_waiters.empty()) {
    // wake one waiting call to write()
    Waiter& waiter = write_waiters.front();
    write_waiters.pop_front();
    waiter.complete(ec);
  }

  if (outstanding == 0) {
    // wake all waiting calls to drain()
    complete_all(std::move(drain_waiters), error);
  }
}

void BaseWriter::service_shutdown()
{
  // release any outstanding completion handlers
  constexpr auto shutdown = [] (boost::intrusive::list<Waiter> waiters) {
      while (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.shutdown();
      }
    };
  shutdown(std::move(write_waiters));
  shutdown(std::move(drain_waiters));
}

} // namespace rgwrados::reshard
