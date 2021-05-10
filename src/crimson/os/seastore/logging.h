// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"

#define LOGGER crimson::get_logger(ceph_subsys_seastore)
#define LOG_PREFIX(x) constexpr auto FNAME = #x

#define LOG(LEVEL, MSG, ...) LOGGER . LEVEL("{}: " MSG, FNAME __VA_OPT__(,) __VA_ARGS__)
#define LOGT(LEVEL, MSG, t, ...) LOGGER . LEVEL("{}({}): " MSG, FNAME, (void*)&t __VA_OPT__(,) __VA_ARGS__)

#define TRACE(...) LOG(trace, __VA_ARGS__)
#define TRACET(...) LOGT(trace, __VA_ARGS__)

#define DEBUG(...) LOG(debug, __VA_ARGS__)
#define DEBUGT(...) LOGT(debug, __VA_ARGS__)

#define WARN(...) LOG(warn, __VA_ARGS__)
#define WARNT(...) LOGT(warn, __VA_ARGS__)

#define ERROR(...) LOG(error, __VA_ARGS__)
#define ERRORT(...) LOGT(error, __VA_ARGS__)
