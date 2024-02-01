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

#include <optional>
#include <string_view>
#include "common/ceph_context.h"

namespace rgw::IAM {

struct Policy;

/// Return a managed policy by ARN.
auto get_managed_policy(CephContext* cct, std::string_view arn)
    -> std::optional<Policy>;

} // namespace rgw::IAM
