// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <fmt/format.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "ECCommon.h"

template <>
struct fmt::formatter<ECCommon::read_request_t> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<ECCommon::read_result_t> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<ECCommon::ReadOp> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<ECCommon::RMWPipeline::Op> : fmt::ostream_formatter {};
