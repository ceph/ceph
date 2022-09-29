// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <memory>
#include "rgw_sal_config.h"

namespace rgw::dbstore {

// ConfigStore factory
auto create_config_store(const DoutPrefixProvider* dpp, const std::string& uri)
  -> std::unique_ptr<sal::ConfigStore>;

} // namespace rgw::dbstore
