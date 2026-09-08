/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "test/catch2_compat.h"

#include <cstdio>
#include <exception>

int main(int argc, char *argv[])
try
{
  return ceph::test::run_catch2(argc, argv);
} catch (const std::exception& e) {
  std::fprintf(stderr, "Catch2 test runner failed: %s\n", e.what());
  return 1;
}
