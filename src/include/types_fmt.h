// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */

#include <fmt/core.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include <include/types.h>

namespace fmt {

template <> struct formatter<shard_id_t> : ostream_formatter {};
} // namespace fmt
