// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_LOGFMT_H
#define CEPH_LOGFMT_H

#include <fmt/format.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "LogEntry.h"
#include "include/utime_fmt.h"
#include "msg/msg_fmt.h"

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<clog_type> : fmt::ostream_formatter {};
#endif

template <> struct fmt::formatter<EntityName> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const EntityName& e, FormatContext& ctx) const {
    return formatter<std::string_view>::format(e.to_str(), ctx);
  }
};

template <> struct fmt::formatter<LogEntry> : fmt::formatter<std::string_view> {
  auto format(const LogEntry& e, format_context& ctx) const -> format_context::iterator;
};

#endif
