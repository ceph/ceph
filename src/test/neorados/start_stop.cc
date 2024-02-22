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

#include <thread>
#include <vector>

#include <boost/asio/use_future.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"

namespace R = neorados;


int main(int argc, char** argv)
{
  using namespace std::literals;

  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(30s);
  }
  std::this_thread::sleep_for(30s);
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(30s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(1s);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(500ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(500ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(50ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(50ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(50ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5ms);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5us);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5us);
  }
  {
    ceph::async::io_context_pool p(1);
    auto r = R::RADOS::make_with_cct(cct.get(), p,
					 boost::asio::use_future).get();
    std::this_thread::sleep_for(5us);
  }
  return 0;
}
