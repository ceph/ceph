// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <string>
#include "include/common_fwd.h"

namespace rgw::account {

/// generate a randomized account id in a specific format
std::string generate_id(CephContext* cct);

/// validate that an account id matches the generated format
bool validate_id(std::string_view id, std::string* err_msg = nullptr);

/// check an account name for any invalid characters
bool validate_name(std::string_view name, std::string* err_msg = nullptr);

} // namespace rgw::account
