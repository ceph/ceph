// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/*
 * This header file contains types required to integrate boost coroutines into
 * the OSD backend.
 *
 * - yield_token_t (pull_type): Is used by a coroutine to suspend execution (yield)
 * while waiting for an I/O operation to complete.
 * - resume_token_t (push_type): Is used by the completion callback to
 */

#pragma once
#include <boost/coroutine2/all.hpp>

using yield_token_t = boost::coroutines2::coroutine<void>::pull_type;
using resume_token_t = boost::coroutines2::coroutine<void>::push_type;

struct CoroHandles {
  yield_token_t& yield;
  resume_token_t& resume;
};