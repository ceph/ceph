// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <string>
#include <string_view>

#include "include/neorados/RADOS.hpp"

std::string get_temp_pool_name(std::string_view prefix = {});

template<typename CompletionToken>
auto create_pool(neorados::RADOS& r, std::string pname,
		 CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken,
				void(boost::system::error_code,
				     std::int64_t)> init(token);
  r.create_pool(pname, std::nullopt,
		[&r, pname = std::string(pname),
		 h = std::move(init.completion_handler)]
		(boost::system::error_code ec) mutable {
		  r.lookup_pool(
		    pname,
		    [h = std::move(h)]
		    (boost::system::error_code ec, std::int64_t pool) mutable {
		      std::move(h)(ec, pool);
		    });
		});
  return init.result.get();
}
