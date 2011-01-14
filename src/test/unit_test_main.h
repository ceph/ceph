// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/common_init.h"

#include "gtest/gtest.h"

/* The main function for Ceph unit tests */
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, const_cast<const char**>(argv), args);
  env_to_vec(args);
  common_set_defaults(false);
  common_init(args, argv[0], false);
  set_no_logging();

  return RUN_ALL_TESTS();
}
