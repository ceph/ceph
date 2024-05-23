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

#ifndef CEPH_ADMIN_SOCKET_OUTPUT_TESTS_H
#define CEPH_ADMIN_SOCKET_OUTPUT_TESTS_H

// Test function declarations, definitions in admin_socket_output_tests.cc

// Example test function

/*
bool test_config_get_admin_socket(std::string& output);
*/

bool test_dump_pgstate_history(std::string& output);

#endif // CEPH_ADMIN_SOCKET_OUTPUT_TESTS_H
