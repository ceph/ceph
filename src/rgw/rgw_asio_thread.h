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
 * Foundation.  See file COPYING.
 *
 */

#pragma once

class DoutPrefixProvider;

/// indicates whether the current thread is in boost::asio::io_context::run(),
/// used to log warnings if synchronous librados calls are made
extern thread_local bool is_asio_thread;

/// call when an operation will block the calling thread due to an empty
/// optional_yield. a warning is logged when is_asio_thread is true
void maybe_warn_about_blocking(const DoutPrefixProvider* dpp);
