// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_COMMON_VERSION_H
#define CEPH_COMMON_VERSION_H

#include <string>

// Return a string describing the Ceph version
const char *ceph_version_to_str(void);

// Return a string describing the git version
const char *git_version_to_str(void);

// Return a formatted string describing the ceph and git versions
std::string const pretty_version_to_str(void);

#endif
