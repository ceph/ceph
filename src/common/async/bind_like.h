// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/asio/associated_allocator.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/bind_executor.hpp>

#include "common/async/bind_allocator.h"

namespace ceph::async {
template<typename Executor, typename Allocator, typename Completion>
auto bind_ea(const Executor& executor, const Allocator& allocator,
	     Completion&& completion) {
  return bind_allocator(allocator,
			boost::asio::bind_executor(
			  executor,
			  std::forward<Completion>(completion)));
}


// Bind `Completion` to the executor and allocator of `Proto`
template<typename Proto, typename Completion>
auto bind_like(const Proto& proto, Completion&& completion) {
  return bind_ea(boost::asio::get_associated_executor(proto),
		 boost::asio::get_associated_allocator(proto),
		 std::forward<Completion>(completion));
}
}
