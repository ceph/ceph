// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */
#include "common/fmt_common.h"
#include "include/types.h"


static inline auto format_as(shard_id_t sid)
{
  return (int)sid.id;
}
