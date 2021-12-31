// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <fmt/format.h>

#include "crimson/common/log.h"

#define LOGGER crimson::get_logger(ceph_subsys_seastore)
#define LOG_PREFIX(x) constexpr auto FNAME = #x

#ifdef NDEBUG

#define LOG(level_, MSG, ...) \
  LOGGER.log(level_, "{}: " MSG, FNAME , ##__VA_ARGS__)
#define LOGT(level_, MSG, t, ...) \
  LOGGER.log(level_, "{}({}): " MSG, FNAME, (void*)&t , ##__VA_ARGS__)

#else

// do compile-time format string validation
using namespace fmt::literals;
template<seastar::log_level lv>
void _LOG(seastar::logger& logger, std::string_view info) {
  logger.log(lv, info.data());
}

#define LOG(level_, MSG, ...) \
  _LOG<level_>(LOGGER, "{}: " MSG ## _format(FNAME , ##__VA_ARGS__))
#define LOGT(level_, MSG, t_, ...) \
  _LOG<level_>(LOGGER, "{}({}): " MSG ## _format(FNAME, (void*)&t_ , ##__VA_ARGS__))

#endif

#define TRACE(...) LOG(seastar::log_level::trace, __VA_ARGS__)
#define TRACET(...) LOGT(seastar::log_level::trace, __VA_ARGS__)

#define DEBUG(...) LOG(seastar::log_level::debug, __VA_ARGS__)
#define DEBUGT(...) LOGT(seastar::log_level::debug, __VA_ARGS__)

#define INFO(...) LOG(seastar::log_level::info, __VA_ARGS__)
#define INFOT(...) LOGT(seastar::log_level::info, __VA_ARGS__)

#define WARN(...) LOG(seastar::log_level::warn, __VA_ARGS__)
#define WARNT(...) LOGT(seastar::log_level::warn, __VA_ARGS__)

#define ERROR(...) LOG(seastar::log_level::error, __VA_ARGS__)
#define ERRORT(...) LOGT(seastar::log_level::error, __VA_ARGS__)
