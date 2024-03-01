// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <fmt/format.h>
#include <seastar/util/log.hh>

#include "common/subsys_types.h"

namespace crimson {
seastar::logger& get_logger(int subsys);
static inline seastar::log_level to_log_level(int level) {
  if (level < 0) {
    return seastar::log_level::error;
  } else if (level < 1) {
    return seastar::log_level::warn;
  } else if (level <= 5) {
    return seastar::log_level::info;
  } else if (level <= 20) {
    return seastar::log_level::debug;
  } else {
    return seastar::log_level::trace;
  }
}
}

/* Logging convenience macros
 *
 * The intention here is to standardize prefixing log lines with the function name
 * and a context prefix (like the operator<< for the PG).  Place
 *
 * SET_SUBSYS(osd);
 *
 * at the top of the file to declare the log lines within the file as being (in this case)
 * in the osd subsys.  At the beginning of each method/function, add
 *
 * LOG_PREFIX(Class::method_name)
 *
 * to set the FNAME symbol to Class::method_name.  In order to use the log macros
 * within lambdas, capture FNAME by value.
 *
 * Log lines can then be declared using the appropriate macro below.
 */

#define SET_SUBSYS(subname_) static constexpr auto SOURCE_SUBSYS = ceph_subsys_##subname_
#define LOCAL_LOGGER crimson::get_logger(SOURCE_SUBSYS)
#define LOGGER(subname_) crimson::get_logger(ceph_subsys_##subname_)
#define LOG_PREFIX(x) constexpr auto FNAME = #x

#define LOG(level_, MSG, ...) \
  LOCAL_LOGGER.log(level_, "{}: " MSG, FNAME , ##__VA_ARGS__)
#define SUBLOG(subname_, level_, MSG, ...) \
  LOGGER(subname_).log(level_, "{}: " MSG, FNAME , ##__VA_ARGS__)
#define LOGI(level_, MSG, ...) \
  LOCAL_LOGGER.log(level_, "{} {}: " MSG, \
    interruptor::get_interrupt_cond(), FNAME , ##__VA_ARGS__)
#define SUBLOGI(subname_, level_, MSG, ...) \
  LOGGER(subname_).log(level_, "{} {}: " MSG, \
    interruptor::get_interrupt_cond(), FNAME , ##__VA_ARGS__)

#define TRACE(...) LOG(seastar::log_level::trace, __VA_ARGS__)
#define TRACEI(...) LOGI(seastar::log_level::trace, __VA_ARGS__)
#define SUBTRACE(subname_, ...) SUBLOG(subname_, seastar::log_level::trace, __VA_ARGS__)
#define SUBTRACEI(subname_, ...) SUBLOGI(subname_, seastar::log_level::trace, __VA_ARGS__)

#define DEBUG(...) LOG(seastar::log_level::debug, __VA_ARGS__)
#define DEBUGI(...) LOGI(seastar::log_level::debug, __VA_ARGS__)
#define SUBDEBUG(subname_, ...) SUBLOG(subname_, seastar::log_level::debug, __VA_ARGS__)
#define SUBDEBUGI(subname_, ...) SUBLOGI(subname_, seastar::log_level::debug, __VA_ARGS__)

#define INFO(...) LOG(seastar::log_level::info, __VA_ARGS__)
#define INFOI(...) LOGI(seastar::log_level::info, __VA_ARGS__)
#define SUBINFO(subname_, ...) SUBLOG(subname_, seastar::log_level::info, __VA_ARGS__)
#define SUBINFOI(subname_, ...) SUBLOGI(subname_, seastar::log_level::info, __VA_ARGS__)

#define WARN(...) LOG(seastar::log_level::warn, __VA_ARGS__)
#define WARNI(...) LOGI(seastar::log_level::warn, __VA_ARGS__)
#define SUBWARN(subname_, ...) SUBLOG(subname_, seastar::log_level::warn, __VA_ARGS__)
#define SUBWARNI(subname_, ...) SUBLOGI(subname_, seastar::log_level::warn, __VA_ARGS__)

#define ERROR(...) LOG(seastar::log_level::error, __VA_ARGS__)
#define ERRORI(...) LOGI(seastar::log_level::error, __VA_ARGS__)
#define SUBERROR(subname_, ...) SUBLOG(subname_, seastar::log_level::error, __VA_ARGS__)
#define SUBERRORI(subname_, ...) SUBLOGI(subname_, seastar::log_level::error, __VA_ARGS__)

// *DPP macros are intended to take DoutPrefixProvider implementations, but anything with
// an operator<< will work as a prefix

#define SUBLOGDPP(subname_, level_, MSG, dpp, ...) \
  LOGGER(subname_).log(level_, "{} {}: " MSG, dpp, FNAME , ##__VA_ARGS__)
#define SUBLOGDPPI(subname_, level_, MSG, dpp, ...) \
  LOGGER(subname_).log(level_, "{} {}: " MSG, \
  interruptor::get_interrupt_cond(), dpp, FNAME , ##__VA_ARGS__)
#define SUBTRACEDPP(subname_, ...) SUBLOGDPP(subname_, seastar::log_level::trace, __VA_ARGS__)
#define SUBTRACEDPPI(subname_, ...) SUBLOGDPPI(subname_, seastar::log_level::trace, __VA_ARGS__)
#define SUBDEBUGDPP(subname_, ...) SUBLOGDPP(subname_, seastar::log_level::debug, __VA_ARGS__)
#define SUBDEBUGDPPI(subname_, ...) SUBLOGDPPI(subname_, seastar::log_level::debug, __VA_ARGS__)
#define SUBINFODPP(subname_, ...) SUBLOGDPP(subname_, seastar::log_level::info, __VA_ARGS__)
#define SUBINFODPPI(subname_, ...) SUBLOGDPPI(subname_, seastar::log_level::info, __VA_ARGS__)
#define SUBWARNDPP(subname_, ...) SUBLOGDPP(subname_, seastar::log_level::warn, __VA_ARGS__)
#define SUBWARNDPPI(subname_, ...) SUBLOGDPPI(subname_, seastar::log_level::warn, __VA_ARGS__)
#define SUBERRORDPP(subname_, ...) SUBLOGDPP(subname_, seastar::log_level::error, __VA_ARGS__)
#define SUBERRORDPPI(subname_, ...) SUBLOGDPPI(subname_, seastar::log_level::error, __VA_ARGS__)

#define LOGDPP(level_, MSG, dpp, ...) \
  LOCAL_LOGGER.log(level_, "{} {}: " MSG, dpp, FNAME , ##__VA_ARGS__)
#define LOGDPPI(level_, MSG, dpp, ...) \
  LOCAL_LOGGER.log(level_, "{} {}: " MSG, \
  interruptor::get_interrupt_cond(), dpp, FNAME , ##__VA_ARGS__)
#define TRACEDPP(...) LOGDPP(seastar::log_level::trace, __VA_ARGS__)
#define TRACEDPPI(...) LOGDPPI(seastar::log_level::trace, __VA_ARGS__)
#define DEBUGDPP(...) LOGDPP(seastar::log_level::debug, __VA_ARGS__)
#define DEBUGDPPI(...) LOGDPPI(seastar::log_level::debug, __VA_ARGS__)
#define INFODPP(...) LOGDPP(seastar::log_level::info, __VA_ARGS__)
#define INFODPPI(...) LOGDPPI(seastar::log_level::info, __VA_ARGS__)
#define WARNDPP(...) LOGDPP(seastar::log_level::warn, __VA_ARGS__)
#define WARNDPPI(...) LOGDPPI(seastar::log_level::warn, __VA_ARGS__)
#define ERRORDPP(...) LOGDPP(seastar::log_level::error, __VA_ARGS__)
#define ERRORDPPI(...) LOGDPPI(seastar::log_level::error, __VA_ARGS__)
