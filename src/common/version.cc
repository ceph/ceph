// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/version.h"

#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <string>
#include <iterator>


#include "ceph_ver.h"
#include "common/ceph_strings.h"

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

const char *ceph_version_to_str()
{
  char* debug_version_for_testing = getenv("ceph_debug_version_for_testing");
  if (debug_version_for_testing) {
    return debug_version_for_testing;
  } else {
    return CEPH_GIT_NICE_VER;
  }
}

const char *ceph_release_to_str(void)
{
  return ceph_release_name(CEPH_RELEASE);
}

const char *git_version_to_str(void)
{
  return STRINGIFY(CEPH_GIT_VER);
}

static std::string read_vendor_release_file()
{
  auto filename = "/etc/ceph_version";
  std::ifstream file(filename);

  if(!file.is_open()){
    return "";
  }

  std::string content;
  try {
    content.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
  } catch (const std::exception &e) {
    return "";
  }
  if (!content.empty()) {
    while (!content.empty() && (content.back() == '\n' || content.back() == '\r')) {
      content.pop_back();
    }
    return std::string(" release ") + content;
  }

  return "";

}

std::string const pretty_version_to_str(void)
{
  std::ostringstream oss;
  oss << "ceph version " << CEPH_GIT_NICE_VER
      << " (" << STRINGIFY(CEPH_GIT_VER) << ") "
      << ceph_release_name(CEPH_RELEASE)
      << " (" << CEPH_RELEASE_TYPE << " - "
      << CEPH_BUILD_TYPE << ")"
#ifdef WITH_CRIMSON
      << " (crimson)"
#endif
      << read_vendor_release_file()
      ;
  return oss.str();
}

const char *ceph_release_type(void)
{
  return CEPH_RELEASE_TYPE;
}
