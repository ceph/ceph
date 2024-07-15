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

#include "include/neorados/RADOS.hpp"

#include "include/scope_guard.h"

#include "common/async/context_pool.h"
#include "common/ceph_time.h"
#include "common/ceph_argparse.h"
#include "common/async/blocked_completion.h"

#include "global/global_init.h"

#include "test/neorados/common_tests.h"


namespace ba = boost::asio;
namespace bs = boost::system;
namespace ca = ceph::async;
namespace R = neorados;

std::string_view hostname() {
  static char hostname[MAXHOSTNAMELEN] = { 0 };
  static size_t len = 0;
  if (!len) {
    auto r = gethostname(hostname, sizeof(hostname));
    if (r != 0) {
      throw bs::system_error(
        errno, bs::system_category());
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

bs::error_code noisy_list(R::RADOS& r, int64_t p)
{
  auto b = R::Cursor::begin();
  auto e = R::Cursor::end();

  std::cout << "begin = " << b.to_str() << std::endl;
  std::cout << "end = " << e.to_str() << std::endl;
  try {
    auto [v, next] = r.enumerate_objects({p, R::all_nspaces}, b, e, 1000, {},
					 ca::use_blocked);

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
  } catch (const bs::system_error& e) {
    std::cerr << "RADOS::enumerate_objects: " << e.what() << std::endl;
    return e.code();
  }
  return {};
}

bs::error_code create_several(R::RADOS& r, const R::IOContext& i,
			      std::initializer_list<std::string> l)
{
  for (const auto& o : l) try {
      R::WriteOp op;
      std::cout << "Creating " << o << std::endl;
      ceph::bufferlist bl;
      bl.append("My bologna has no name.");
      op.write_full(std::move(bl));
      r.execute(o, i, std::move(op), ca::use_blocked);
    } catch (const bs::system_error& e) {
      std::cerr << "RADOS::execute: " << e.what() << std::endl;
      return e.code();
    }
  return {};
}

int main(int argc, char** argv)
{
  using namespace std::literals;

  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  try {
    ca::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p, ca::use_blocked);

    auto pool_name = get_temp_pool_name("ceph_test_RADOS_list_pool"sv);
    r.create_pool(pool_name, std::nullopt, ca::use_blocked);
    auto pd = make_scope_guard(
    [&pool_name, &r]() {
      r.delete_pool(pool_name, ca::use_blocked);
    });
    auto pool = r.lookup_pool(pool_name, ca::use_blocked);
    R::IOContext i(pool);

    if (noisy_list(r, pool)) {
      return 1;
    }
    if (create_several(r, i, {"meow", "woof", "squeak"})) {
      return 1;
    }
    if (noisy_list(r, pool)) {
      return 1;
    }

  } catch (const bs::system_error& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
