// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2017 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_FUTURE_HPP
#define CEPH_FUTURE_HPP

// using boost::future instead of std::future because it supports continuations
// with future::then(), which is part of the 'Technical Specification for C++
// Extensions for Concurrency':
//   http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4107.html
#define BOOST_THREAD_VERSION 4
#define BOOST_THREAD_PROVIDES_EXECUTORS

#include <boost/thread/future.hpp>

namespace ceph {

using boost::launch;
using boost::promise;
using boost::future;
using boost::shared_future;
using boost::packaged_task;
using boost::make_ready_future;
using boost::make_exceptional_future;
using boost::when_all;
using boost::when_any;

} // namespace ceph

#endif // CEPH_FUTURE_HPP
