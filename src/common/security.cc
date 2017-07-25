// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <string>
#include <vector>

#include <boost/regex.hpp>

#include "common/security.h"

const std::string ceph::security::mask(const std::string &candidate) {
  boost::regex reg(
      "(\"key\": \"dm-crypt\\/osd\\/.*\\/luks\", \"val\": \")(.*)(\")");
  if (boost::regex_search(candidate, reg))
    return boost::regex_replace(candidate, reg, "$1REDACTED$3");
  else
    return candidate;
}

const std::vector<std::string> ceph::security::mask(const std::vector<std::string> &candidate) {
  std::vector<std::string> ret;
  ret.reserve(candidate.size());
  std::for_each(
      candidate.begin(), candidate.end(),
      [&](std::string mask_target) { ret.push_back(ceph::security::mask(mask_target)); });
  return ret;
}
