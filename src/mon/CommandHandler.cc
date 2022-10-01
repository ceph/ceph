// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Ltd
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "CommandHandler.h"

#include "common/strtol.h"
#include "include/ceph_assert.h"

#include <ostream>
#include <string>
#include <string_view>

int CommandHandler::parse_bool(std::string_view str, bool* result, std::ostream& ss)
{
  ceph_assert(result != nullptr);

  std::string interr;
  int64_t n = strict_strtoll(str.data(), 10, &interr);

  if (str == "false" || str == "no"
      || (interr.length() == 0 && n == 0)) {
    *result = false;
    return 0;
  } else if (str == "true" || str == "yes"
      || (interr.length() == 0 && n == 1)) {
    *result = true;
    return 0;
  } else {
    ss << "value must be false|no|0 or true|yes|1";
    return -EINVAL;
  }
}
