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

#include "driver/immutable_config/store.h"

namespace rgw::sal {

/// Create an immutable ConfigStore by parsing the zonegroup and zone from the
/// given json filename.
auto create_json_config_store(const DoutPrefixProvider* dpp,
                              const std::string& filename)
    -> std::unique_ptr<ConfigStore>;

} // namespace rgw::sal
