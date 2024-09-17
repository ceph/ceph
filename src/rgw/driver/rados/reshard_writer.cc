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
#include "librados/librados_asio.h"
#include "cls/rgw/cls_rgw_client.h"

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

  constexpr auto complete_all = [] (boost::intrusive::list<Waiter>& waiters,
                                    boost::system::error_code ec) {
      while (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.complete(ec);
      }
    };
  constexpr auto complete_one = [] (boost::intrusive::list<Waiter>& waiters,
                                    boost::system::error_code ec) {
      if (!waiters.empty()) {
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
    complete_all(write_waiters, ec);
  } else {
    // wake one waiting call to write()
    complete_one(write_waiters, {});
  }

  if (outstanding == 0) {
    // wake all waiting calls to drain()
    complete_all(drain_waiters, error);
  }
}

void BaseWriter::service_shutdown()
{
  // release any outstanding completion handlers
  constexpr auto shutdown = [] (boost::intrusive::list<Waiter>& waiters) {
      while (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.shutdown();
      }
    };
  shutdown(write_waiters);
  shutdown(drain_waiters);
}


PutBatch::PutBatch(boost::asio::io_context& ctx,
                   librados::IoCtx ioctx,
                   std::string object,
                   size_t batch_size)
  : ctx(ctx),
    ioctx(std::move(ioctx)),
    object(std::move(object)),
    batch_size(batch_size)
{
  entries.reserve(batch_size); // preallocate the batch
}

[[nodiscard]] bool PutBatch::empty() const
{
  return entries.empty();
}

[[nodiscard]] bool PutBatch::add(rgw_cls_bi_entry entry,
                                 std::optional<RGWObjCategory> category,
                                 rgw_bucket_category_stats entry_stats)
{
  entries.push_back(std::move(entry));

  if (category) {
    stats[*category] += entry_stats;
  }

  return entries.size() >= batch_size;
}

void PutBatch::flush(Completion completion)
{
  librados::ObjectWriteOperation op;

  // issue a separate bi_put() call for each entry
  for (auto& entry : entries) {
    cls_rgw_bi_put(op, std::move(entry));
  }
  entries.clear();

  constexpr bool absolute = false; // add to existing stats
  cls_rgw_bucket_update_stats(op, absolute, stats);
  stats.clear();

  constexpr int flags = 0;
  constexpr jspan_context* trace = nullptr;
  librados::async_operate(ctx, ioctx, object, &op, flags,
                          trace, std::move(completion));
}

PutEntriesBatch::PutEntriesBatch(boost::asio::io_context& ctx,
                                 librados::IoCtx ioctx,
                                 std::string object,
                                 size_t batch_size,
                                 bool check_existing)
  : ctx(ctx),
    ioctx(std::move(ioctx)),
    object(std::move(object)),
    batch_size(batch_size),
    check_existing(check_existing)
{
  entries.reserve(batch_size); // preallocate the batch
}

[[nodiscard]] bool PutEntriesBatch::empty() const
{
  return entries.empty();
}

[[nodiscard]] bool PutEntriesBatch::add(rgw_cls_bi_entry entry,
                                        std::optional<RGWObjCategory> category,
                                        rgw_bucket_category_stats stats)
{
  entries.push_back(std::move(entry));
  // bi_put_entries() handles stats on the server side
  std::ignore = category;
  std::ignore = stats;

  return entries.size() >= batch_size;
}

void PutEntriesBatch::flush(Completion completion)
{
  librados::ObjectWriteOperation op;

  cls_rgw_bi_put_entries(op, std::move(entries), check_existing);
  entries.clear();

  constexpr int flags = 0;
  constexpr jspan_context* trace = nullptr;
  librados::async_operate(ctx, ioctx, object, &op, flags,
                          trace, std::move(completion));
}

} // namespace rgwrados::reshard
