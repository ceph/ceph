// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <fmt/format.h>

#include "crimson/common/log.h"

#define LOGT(level_, MSG, t, ...) \
  LOCAL_LOGGER.log(level_, "{} trans.{} {}: " MSG, (void*)&t, \
    (t).get_trans_id(), FNAME , ##__VA_ARGS__)
#define SUBLOGT(subname_, level_, MSG, t, ...) \
  LOGGER(subname_).log(level_, "{} trans.{} {}: " MSG, (void*)&t, \
    (t).get_trans_id(), FNAME , ##__VA_ARGS__)

#define TRACET(...) LOGT(seastar::log_level::trace, __VA_ARGS__)
#define SUBTRACET(subname_, ...) SUBLOGT(subname_, seastar::log_level::trace, __VA_ARGS__)

#define DEBUGT(...) LOGT(seastar::log_level::debug, __VA_ARGS__)
#define SUBDEBUGT(subname_, ...) SUBLOGT(subname_, seastar::log_level::debug, __VA_ARGS__)

#define INFOT(...) LOGT(seastar::log_level::info, __VA_ARGS__)
#define SUBINFOT(subname_, ...) SUBLOGT(subname_, seastar::log_level::info, __VA_ARGS__)

#define WARNT(...) LOGT(seastar::log_level::warn, __VA_ARGS__)
#define SUBWARNT(subname_, ...) SUBLOGT(subname_, seastar::log_level::warn, __VA_ARGS__)

#define ERRORT(...) LOGT(seastar::log_level::error, __VA_ARGS__)
#define SUBERRORT(subname_, ...) SUBLOGT(subname_, seastar::log_level::error, __VA_ARGS__)
