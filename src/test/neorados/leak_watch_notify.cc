// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Contributors to the Ceph Project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <optional>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "common_tests.h"

namespace asio = boost::asio;

using boost::system::error_code;

// Intentionally get a watch and don't unwatch it to make sure we call
// `linger_cancel` and don't leak.

int main()
{    
  asio::io_context c;
  std::string_view oid = "obj";

  std::optional<neorados::RADOS> rados;
  neorados::IOContext pool;
  neorados::RADOS::Builder{}.build(c, [&](error_code ec, neorados::RADOS r_) {
    rados = std::move(r_);
    create_pool(*rados, get_temp_pool_name(),
		[&](error_code ec, int64_t poolid) {
		  pool.set_pool(poolid);
		  rados->watch(oid, pool, [&](error_code, std::uint64_t cookie) {
		    std::cout << "Got watch: " << cookie << std::endl;
		  });
		});
  });
  c.run();
}
