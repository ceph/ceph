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

#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "impl.h"
#include "store.h"

namespace rgw::rados {

RadosConfigStore::RadosConfigStore(std::unique_ptr<ConfigImpl> impl)
  : impl(std::move(impl))
{
}

RadosConfigStore::~RadosConfigStore() = default;


auto create_config_store(const DoutPrefixProvider* dpp)
    -> std::unique_ptr<RadosConfigStore>
{
  auto impl = std::make_unique<ConfigImpl>(dpp->get_cct()->_conf);

  // initialize a Rados client
  int r = impl->rados.init_with_context(dpp->get_cct());
  if (r < 0) {
    ldpp_dout(dpp, -1) << "Rados client initialization failed with "
        << cpp_strerror(-r) << dendl;
    return nullptr;
  }
  r = impl->rados.connect();
  if (r < 0) {
    ldpp_dout(dpp, -1) << "Rados client connection failed with "
        << cpp_strerror(-r) << dendl;
    return nullptr;
  }

  return std::make_unique<RadosConfigStore>(std::move(impl));
}

} // namespace rgw::rados
