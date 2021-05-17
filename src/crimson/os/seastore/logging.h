// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <fmt/format.h>

#include "crimson/common/log.h"

#define LOGGER crimson::get_logger(ceph_subsys_seastore)
#define LOG_PREFIX(x) constexpr auto FNAME = #x

#ifdef NDEBUG

#define LOG(LEVEL, MSG, ...) LOGGER . LEVEL("{}: " MSG, FNAME __VA_OPT__(,) __VA_ARGS__)
#define LOGT(LEVEL, MSG, t, ...) LOGGER . LEVEL("{}({}): " MSG, FNAME, (void*)&t __VA_OPT__(,) __VA_ARGS__)

#define TRACE(...) LOG(trace, __VA_ARGS__)
#define TRACET(...) LOGT(trace, __VA_ARGS__)

#define DEBUG(...) LOG(debug, __VA_ARGS__)
#define DEBUGT(...) LOGT(debug, __VA_ARGS__)

#define INFO(...) LOG(info, __VA_ARGS__)
#define INFOT(...) LOGT(info, __VA_ARGS__)

#define WARN(...) LOG(warn, __VA_ARGS__)
#define WARNT(...) LOGT(warn, __VA_ARGS__)

#define ERROR(...) LOG(error, __VA_ARGS__)
#define ERRORT(...) LOGT(error, __VA_ARGS__)

#else
// do compile-time format string validation
using namespace fmt::literals;
template<seastar::log_level lv>
void LOG(std::string_view info) {
  crimson::get_logger(ceph_subsys_seastore).log(lv, info);
}
#define TRACE(MSG_, ...) LOG<seastar::log_level::trace>("{}: " MSG_ ## _format(FNAME __VA_OPT__(,) __VA_ARGS__))
#define TRACET(MSG_, t_, ...) LOG<seastar::log_level::trace>("{}({}): " MSG_ ## _format(FNAME, (void*)&t_ __VA_OPT__(,) __VA_ARGS__))

#define DEBUG(MSG_, ...) LOG<seastar::log_level::debug>("{}: " MSG_ ## _format(FNAME __VA_OPT__(,) __VA_ARGS__))
#define DEBUGT(MSG_, t_, ...) LOG<seastar::log_level::debug>("{}({}): " MSG_ ## _format(FNAME, (void*)&t_ __VA_OPT__(,) __VA_ARGS__))

#define INFO(MSG_, ...) LOG<seastar::log_level::info>("{}: " MSG_ ## _format(FNAME __VA_OPT__(,) __VA_ARGS__))
#define INFOT(MSG_, t_, ...) LOG<seastar::log_level::info>("{}({}): " MSG_ ## _format(FNAME, (void*)&t_ __VA_OPT__(,) __VA_ARGS__))

#define WARN(MSG_, ...) LOG<seastar::log_level::warn>("{}: " MSG_ ## _format(FNAME __VA_OPT__(,) __VA_ARGS__))
#define WARNT(MSG_, t_, ...) LOG<seastar::log_level::warn>("{}({}): " MSG_ ## _format(FNAME, (void*)&t_ __VA_OPT__(,) __VA_ARGS__))

#define ERROR(MSG_, ...) LOG<seastar::log_level::error>("{}: " MSG_ ## _format(FNAME __VA_OPT__(,) __VA_ARGS__))
#define ERRORT(MSG_, t_, ...) LOG<seastar::log_level::error>("{}({}): " MSG_ ## _format(FNAME, (void*)&t_ __VA_OPT__(,) __VA_ARGS__))
#endif
