// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some neorados types
 */

#include <fmt/core.h>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include <include/neorados/RADOS.hpp>

template <> struct fmt::formatter<neorados::Object> : fmt::ostream_formatter {};
