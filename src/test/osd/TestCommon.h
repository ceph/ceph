// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstddef>
#include <string>
#include "test/osd/PGBackendTestFixture.h"

/**
 * WriteReadParam - parameter structure for write-then-read parameterized tests.
 *
 * Shared between test files to avoid ODR violations if both translation units
 * are ever linked together, and to eliminate code duplication.
 */
struct WriteReadParam {
  size_t size;
  char fill;
  std::string label;
};

/**
 * BackendConfig - parameterizes the backend type for unified tests.
 *
 * Each configuration defines a pool type (EC or REPLICATED) plus
 * EC-specific settings.  The test fixture uses this to configure
 * PGBackendTestFixture before SetUp().
 */
struct BackendConfig {
  PGBackendTestFixture::PoolType pool_type;
  // EC-specific (ignored for REPLICATED)
  std::string ec_plugin;     // e.g. "isa", "jerasure", "mock"
  std::string ec_technique;  // e.g. "reed_sol_van"
  bool ec_optimizations;     // FLAG_EC_OPTIMIZATIONS on the pool
  // Label for test naming
  std::string label;
};

/**
 * BackendWriteReadParam - combined parameter for backend + write/read size tests.
 *
 * Used for two-level parameterization: backend configuration × data sizes.
 */
struct BackendWriteReadParam {
  BackendConfig backend;
  WriteReadParam write_read;
};

