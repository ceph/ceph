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

#include <stdexcept>

#include <fmt/format.h>

#include "store.h"
#ifdef SQLITE_ENABLED
#include "sqlite.h"
#endif

namespace rgw::dbstore {

auto create_config_store(const DoutPrefixProvider* dpp, const std::string& uri)
  -> std::unique_ptr<sal::ConfigStore>
{
#ifdef SQLITE_ENABLED
  if (uri.starts_with("file:")) {
    return config::create_sqlite_store(dpp, uri);
  }
#endif
  throw std::runtime_error(fmt::format("unrecognized URI {}", uri));
}

} // namespace rgw::dbstore
