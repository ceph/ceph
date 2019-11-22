// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <initializer_list>
#include <optional>
#include <thread>
#include <tuple>
#include <string_view>
#include <vector>

#include <sys/param.h>

#include <unistd.h>

#include <boost/system/system_error.hpp>

#include <fmt/format.h>

#include "include/RADOS/RADOS.hpp"

#include "include/scope_guard.h"

#include "common/async/context_pool.h"
#include "common/ceph_time.h"
#include "common/ceph_argparse.h"
#include "common/async/waiter.h"

#include "global/global_init.h"

std::string_view hostname() {
  static char hostname[MAXHOSTNAMELEN] = { 0 };
  static size_t len = 0;
  if (!len) {
    auto r = gethostname(hostname, sizeof(hostname));
    if (r != 0) {
      throw boost::system::system_error(
        errno, boost::system::system_category());
    }
    len = std::strlen(hostname);
  }
  return {hostname, len};
}

std::string temp_pool_name(const std::string_view prefix)
{
  using namespace std::chrono;
  static std::uint64_t num = 1;
  return fmt::format(
    "{}-{}-{}-{}-{}",
    prefix,
    hostname(),
    getpid(),
    duration_cast<milliseconds>(ceph::coarse_real_clock::now()
                                .time_since_epoch()).count(),
    num++);
}

boost::system::error_code noisy_list(RADOS::RADOS& r,
                                     int64_t p) {
    ceph::async::waiter<boost::system::error_code> w;
    RADOS::Cursor next;
    std::vector<RADOS::Entry> v;
    auto b = RADOS::Cursor::begin();
    auto e = RADOS::Cursor::end();

    std::cout << "begin = " << b.to_str() << std::endl;
    std::cout << "end = " << e.to_str() << std::endl;
    r.enumerate_objects(p, b, e, 1000, {}, &v, &next, w,
                        RADOS::all_nspaces);
    auto ec = w.wait();
    if (ec) {
      std::cerr << "RADOS::enumerate_objects: " << ec << std::endl;
      return ec;
    }

    std::cout << "Got " << v.size() << " entries." << std::endl;

    std::cout << "next cursor = " << next.to_str() << std::endl;
    std::cout << "next == end: " << (next == e) << std::endl;
    std::cout << "Returned Objects: ";
    std::cout << "[";
    auto o = v.cbegin();
    while (o != v.cend()) {
      std::cout << *o;
      if (++o != v.cend())
        std::cout << " ";
    }
    std::cout << "]" << std::endl;
    return {};
}

boost::system::error_code create_several(RADOS::RADOS& r,
                                         const RADOS::IOContext& i,
                                         std::initializer_list<std::string> l) {
  for (const auto& o : l) {
    ceph::async::waiter<boost::system::error_code> w;
    RADOS::WriteOp op;
    std::cout << "Creating " << o << std::endl;
    ceph::bufferlist bl;
    bl.append("My bologna has no name.");
    op.write_full(std::move(bl));
    r.execute(o, i, std::move(op), w);
    auto ec = w.wait();
    if (ec) {
      std::cerr << "RADOS::execute: " << ec << std::endl;
      return ec;
    }
  }
  return {};
}

int main(int argc, char** argv)
{
  using namespace std::literals;

  std::vector<const char*> args;
  argv_to_vec(argc, const_cast<const char**>(argv), args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  ceph::async::io_context_pool p(1);
  auto r = RADOS::RADOS::make_with_cct(cct.get(), p,
				       boost::asio::use_future).get();

  auto pool_name = temp_pool_name("ceph_test_RADOS_list_pool");

  {
    ceph::async::waiter<boost::system::error_code> w;
    r.create_pool(pool_name, std::nullopt, w);
    auto ec = w.wait();
    if (ec) {
      std::cerr << "RADOS::create_pool: " << ec << std::endl;
      return 1;
    }
  }

  auto pd = make_scope_guard(
    [&pool_name, &r]() {
      ceph::async::waiter<boost::system::error_code> w;
      r.delete_pool(pool_name, w);
      auto ec = w.wait();
      if (ec)
        std::cerr << "RADOS::delete_pool: " << ec << std::endl;
    });

  std::int64_t pool;

  {
    ceph::async::waiter<boost::system::error_code, std::int64_t> w;
    r.lookup_pool(pool_name, w);
    boost::system::error_code ec;
    std::tie(ec, pool) = w.wait();
    if (ec) {
      std::cerr << "RADOS::lookup_pool: " << ec << std::endl;
      return 1;
    }
  }

  RADOS::IOContext i(pool);

  if (noisy_list(r, pool)) {
    return 1;
  }

  if (create_several(r, i, {"meow", "woof", "squeak"})) {
    return 1;
  }

  std::this_thread::sleep_for(5s);

  if (noisy_list(r, pool)) {
    return 1;
  }

  return 0;
}
