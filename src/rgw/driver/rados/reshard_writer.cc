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
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/execution.hpp>
#include <boost/asio/query.hpp>
#include "neorados/cls/rgw.h"

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

auto BaseWriter::drain()
  -> boost::asio::awaitable<void>
{
  if (outstanding > 0) {
    Waiter waiter;
    drain_waiters.push_back(waiter);
    co_await waiter.get(); // may rethrow error from previous completion
  } else if (error) {
    // make sure errors get reported to the caller
    throw boost::system::system_error(error);
  }
}

void BaseWriter::on_complete(boost::system::error_code ec)
{
  --outstanding;

  constexpr auto complete_all = [] (boost::intrusive::list<Waiter>& waiters,
                                    std::exception_ptr eptr) {
      while (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.complete(eptr);
      }
    };
  constexpr auto complete_one = [] (boost::intrusive::list<Waiter>& waiters,
                                    std::exception_ptr eptr) {
      if (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.complete(eptr);
      }
    };

  if (ec) {
    if (!error) {
      error = ec;
    }
    // fail all waiting calls to write()
    auto eptr = std::make_exception_ptr(boost::system::system_error{ec});
    complete_all(write_waiters, eptr);
  } else {
    // wake one waiting call to write()
    complete_one(write_waiters, nullptr);
  }

  if (outstanding == 0) {
    // wake all waiting calls to drain()
    std::exception_ptr eptr;
    if (error) {
      eptr = std::make_exception_ptr(boost::system::system_error{error});
    }
    complete_all(drain_waiters, eptr);
  }
}

void BaseWriter::service_shutdown()
{
  // release any outstanding completion handlers
  constexpr auto shutdown = [] (auto& waiters) {
      while (!waiters.empty()) {
        Waiter& waiter = waiters.front();
        waiters.pop_front();
        waiter.shutdown();
      }
    };
  shutdown(write_waiters);
  shutdown(drain_waiters);
}


PutBatch::PutBatch(neorados::RADOS& rados,
                   neorados::IOContext ioctx,
                   neorados::Object object,
                   size_t batch_size)
  : rados(rados),
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
  neorados::WriteOp op;

  // issue a separate bi_put() call for each entry
  for (auto& entry : entries) {
    op.exec(neorados::cls::rgw::bi_put(std::move(entry)));
  }
  entries.clear();

  op.exec(neorados::cls::rgw::bi_add_stats(
          std::make_move_iterator(stats.begin()),
          std::make_move_iterator(stats.end())));
  stats.clear();

  rados.execute(object, ioctx, std::move(op), std::move(completion));
}

PutEntriesBatch::PutEntriesBatch(neorados::RADOS& rados,
                                 neorados::IOContext ioctx,
                                 neorados::Object object,
                                 size_t batch_size,
                                 bool check_existing)
  : rados(rados),
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
  neorados::WriteOp op;

  op.exec(neorados::cls::rgw::bi_put_entries(
          std::make_move_iterator(entries.begin()),
          std::make_move_iterator(entries.end()),
          check_existing));
  entries.clear();

  rados.execute(object, ioctx, std::move(op), std::move(completion));
}

} // namespace rgwrados::reshard
