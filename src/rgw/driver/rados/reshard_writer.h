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

#pragma once

#include <optional>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/system.hpp>

#include <neorados/RADOS.hpp>

#include "cls/rgw/cls_rgw_types.h"
#include "common/async/co_waiter.h"
#include "common/async/service.h"

namespace rgwrados::reshard {

// base class for Writer that contains everything that doesn't
// depend on the Batch template type
class BaseWriter : public ceph::async::service_list_base_hook {
 public:
  using executor_type = boost::asio::any_io_executor;
  executor_type get_executor() const noexcept { return ex; }

  // wait for all outstanding flush completions
  auto drain()
    -> boost::asio::awaitable<void>;

  void service_shutdown();

 protected:
  BaseWriter(const executor_type& ex, uint64_t max_aio);
  ~BaseWriter();

  using service_type = ceph::async::service<BaseWriter>;
  service_type& svc;
  executor_type ex;

  const uint64_t max_aio;
  uint64_t outstanding = 0;
  boost::system::error_code error;

  struct Waiter : boost::intrusive::list_base_hook<>,
                  ceph::async::co_waiter<void, executor_type> {};
  boost::intrusive::list<Waiter> write_waiters;
  boost::intrusive::list<Waiter> drain_waiters;

  friend struct Completion;
  void on_complete(boost::system::error_code ec);
};

// handler that notifies the writer on completion of a flushed batch
struct Completion {
  BaseWriter& writer;

  using executor_type = BaseWriter::executor_type;
  boost::asio::executor_work_guard<executor_type> work =
      make_work_guard(writer.get_executor());

  executor_type get_executor() const { return work.get_executor(); }

  void operator()(boost::system::error_code ec)
  {
    work.reset();
    writer.on_complete(ec);
  }
};

// example Batch type for Writer
struct BatchArchetype final {
  // return true if there are entries to flush
  bool empty() const;
  // append an entry to the batch and return whether a flush is necessary
  bool add(rgw_cls_bi_entry entry,
           std::optional<RGWObjCategory> category,
           rgw_bucket_category_stats stats);
  // flush the batch to storage, calling the given handler upon completion
  void flush(Completion completion);
};

// Writes index entries in batches to a given target shard object
template <typename Batch>
class Writer : public BaseWriter {
 public:
  Writer(const executor_type& ex, uint64_t max_aio, Batch& batch)
    : BaseWriter(ex, max_aio), batch(batch) {}

  // add the given entry to its batch. if the batch is full, send an
  // async write request to flush it in the background. write() only
  // suspends once we reach the limit of outstanding write requests to
  // avoid buffering additional entries. this is important to bound
  // the overall memory usage of index entries
  auto write(rgw_cls_bi_entry entry,
             std::optional<RGWObjCategory> category,
             rgw_bucket_category_stats stats)
    -> boost::asio::awaitable<void>
  {
    if (error) {
      // fail new writes once we're in the error state
      throw boost::system::system_error(error);
    }

    while (outstanding >= max_aio) {
      Waiter waiter;
      write_waiters.push_back(waiter);
      co_await waiter.get(); // may rethrow error from previous completion
    }

    const bool full = batch.add(entry, category, std::move(stats));
    if (full) {
      ++outstanding;
      batch.flush(Completion{*this});
    }
  }

  // if there's an incomplete batch, flush it
  void flush()
  {
    if (!batch.empty()) {
      ++outstanding;
      batch.flush(Completion{*this});
    }
  }

  // wait for all outstanding flush completions
  using BaseWriter::drain;

 private:
  Batch& batch;
};

// a target shard batch that must be flushed with several bi_put() calls
class PutBatch {
 public:
  PutBatch(neorados::RADOS& rados,
           neorados::IOContext ioctx,
           neorados::Object object,
           size_t batch_size);

  [[nodiscard]] bool empty() const;
  [[nodiscard]] bool add(rgw_cls_bi_entry entry,
                         std::optional<RGWObjCategory> category,
                         rgw_bucket_category_stats stats);
  void flush(Completion completion);

 private:
  neorados::RADOS& rados;
  neorados::IOContext ioctx;
  neorados::Object object;
  const size_t batch_size;
  std::vector<rgw_cls_bi_entry> entries;
  using category_stats_map = boost::container::flat_map<
      RGWObjCategory, rgw_bucket_category_stats>;
  category_stats_map stats;
};

// a target shard batch that can be flushed with one bi_put_entries() call
class PutEntriesBatch {
 public:
  PutEntriesBatch(neorados::RADOS& rados,
                  neorados::IOContext ioctx,
                  neorados::Object object,
                  size_t batch_size,
                  bool check_existing);

  [[nodiscard]] bool empty() const;
  [[nodiscard]] bool add(rgw_cls_bi_entry entry,
                         std::optional<RGWObjCategory> category,
                         rgw_bucket_category_stats stats);
  void flush(Completion completion);

 private:
  neorados::RADOS& rados;
  neorados::IOContext ioctx;
  neorados::Object object;
  const size_t batch_size;
  const bool check_existing;
  std::vector<rgw_cls_bi_entry> entries;
};

} // namespace rgwrados::reshard
