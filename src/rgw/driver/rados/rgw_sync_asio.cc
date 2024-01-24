// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sync_asio.h"

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "neorados/cls/log.h"

#include "rgw_neorados.h"
#include "rgw_sync_common.h"

using ceph::real_clock;
using neorados::WriteOp;

namespace ncl = neorados::cls::log;

namespace rgw::sync {
asio::awaitable<std::unique_ptr<ErrorLogger>> create_error_logger(
  const DoutPrefixProvider* dpp,
  sal::RadosStore* store) {
  auto ioc =
    co_await neorados::init_iocontext(
      dpp, store->get_neorados(),
      store->get_siteconfig().get_zone_params().log_pool,
      neorados::create, neorados::mostly_omap);

  auto p = new ErrorLogger(store, std::move(ioc));
  co_return p;
}

asio::awaitable<void> ErrorLogger::log_error(const DoutPrefixProvider* dpp,
					     std::string source_zone,
					     std::string section,
					     std::string name,
					     uint32_t error_code,
					     std::string message) {
  error_info info(std::move(source_zone), error_code, std::move(message));
  buffer::list bl;
  encode(info, bl);
  ncl::entry entry{real_clock::now(), std::move(section), std::move(name),
                   std::move(bl)};
  co_await store->get_neorados().execute(
    next_oid(), ioc, WriteOp{}.exec(ncl::add(std::move(entry))),
    asio::use_awaitable);
}

asio::awaitable<void> Backoff::backoff() {
  update_wait_time();
  asio::steady_timer t(co_await asio::this_coro::executor, cur_wait);
  co_return co_await t.async_wait(asio::use_awaitable);
}
}
