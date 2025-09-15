// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_NVMEOFGWBEACONCONSTANTS_H
#define CEPH_NVMEOFGWBEACONCONSTANTS_H

// This header contains version constants used across multiple files
// to avoid duplication and maintain consistency.

// Beacon version constants
#define BEACON_VERSION_LEGACY 1             // Legacy beacon format (no diff support)
#define BEACON_VERSION_ENHANCED 2           // Enhanced beacon format (with diff support)

#endif /* CEPH_NVMEOFGWBEACONCONSTANTS_H */
