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

#ifndef ECBMSGTYPES_FMT_H
#define ECBMSGTYPES_FMT_H

#include <fmt/format.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "ECMsgTypes.h"

template <> struct fmt::formatter<ECSubWrite> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECSubWriteReply> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECSubRead> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECSubReadReply> : fmt::ostream_formatter {};

#endif
