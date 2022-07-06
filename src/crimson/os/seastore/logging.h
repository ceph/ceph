// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <fmt/format.h>

#include "crimson/common/log.h"

#define SET_SUBSYS(subname_) static constexpr auto SOURCE_SUBSYS = ceph_subsys_##subname_
#define LOCAL_LOGGER crimson::get_logger(SOURCE_SUBSYS)
#define LOGGER(subname_) crimson::get_logger(ceph_subsys_##subname_)
#define LOG_PREFIX(x) constexpr auto FNAME = #x

#define LOG(level_, MSG, ...) \
  LOCAL_LOGGER.log(level_, "{}: " MSG, FNAME , ##__VA_ARGS__)
#define LOGT(level_, MSG, t, ...) \
  LOCAL_LOGGER.log(level_, "{} {}: " MSG, (void*)&t, FNAME , ##__VA_ARGS__)
#define SUBLOG(subname_, level_, MSG, ...) \
  LOGGER(subname_).log(level_, "{}: " MSG, FNAME , ##__VA_ARGS__)
#define SUBLOGT(subname_, level_, MSG, t, ...) \
  LOGGER(subname_).log(level_, "{} {}: " MSG, (void*)&t, FNAME , ##__VA_ARGS__)

#define TRACE(...) LOG(seastar::log_level::trace, __VA_ARGS__)
#define TRACET(...) LOGT(seastar::log_level::trace, __VA_ARGS__)
#define SUBTRACE(subname_, ...) SUBLOG(subname_, seastar::log_level::trace, __VA_ARGS__)
#define SUBTRACET(subname_, ...) SUBLOGT(subname_, seastar::log_level::trace, __VA_ARGS__)

#define DEBUG(...) LOG(seastar::log_level::debug, __VA_ARGS__)
#define DEBUGT(...) LOGT(seastar::log_level::debug, __VA_ARGS__)
#define SUBDEBUG(subname_, ...) SUBLOG(subname_, seastar::log_level::debug, __VA_ARGS__)
#define SUBDEBUGT(subname_, ...) SUBLOGT(subname_, seastar::log_level::debug, __VA_ARGS__)

#define INFO(...) LOG(seastar::log_level::info, __VA_ARGS__)
#define INFOT(...) LOGT(seastar::log_level::info, __VA_ARGS__)
#define SUBINFO(subname_, ...) SUBLOG(subname_, seastar::log_level::info, __VA_ARGS__)
#define SUBINFOT(subname_, ...) SUBLOGT(subname_, seastar::log_level::info, __VA_ARGS__)

#define WARN(...) LOG(seastar::log_level::warn, __VA_ARGS__)
#define WARNT(...) LOGT(seastar::log_level::warn, __VA_ARGS__)
#define SUBWARN(subname_, ...) SUBLOG(subname_, seastar::log_level::warn, __VA_ARGS__)
#define SUBWARNT(subname_, ...) SUBLOGT(subname_, seastar::log_level::warn, __VA_ARGS__)

#define ERROR(...) LOG(seastar::log_level::error, __VA_ARGS__)
#define ERRORT(...) LOGT(seastar::log_level::error, __VA_ARGS__)
#define SUBERROR(subname_, ...) SUBLOG(subname_, seastar::log_level::error, __VA_ARGS__)
#define SUBERRORT(subname_, ...) SUBLOGT(subname_, seastar::log_level::error, __VA_ARGS__)
