// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "driver/rados/config/store.h"
#include "driver/json_config/store.h"
#include "rgw_d3n_datacache.h"

#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#include "driver/dbstore/config/store.h"
#endif

#define dout_subsys ceph_subsys_rgw

auto DriverManager::create_config_store(const DoutPrefixProvider* dpp,
                                        std::string_view type)
  -> std::unique_ptr<rgw::sal::ConfigStore>
{
  try {
    if (type == "rados") {
      return rgw::rados::create_config_store(dpp);
#ifdef WITH_RADOSGW_DBSTORE
    } else if (type == "dbstore") {
      const auto uri = g_conf().get_val<std::string>("dbstore_config_uri");
      return rgw::dbstore::create_config_store(dpp, uri);
#endif
    } else if (type == "json") {
      auto filename = g_conf().get_val<std::string>("rgw_json_config");
      return rgw::sal::create_json_config_store(dpp, filename);
    } else {
      ldpp_dout(dpp, -1) << "ERROR: unrecognized config store type '"
          << type << "'" << dendl;
      return nullptr;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << "ERROR: failed to initialize config store '"
        << type << "': " << e.what() << dendl;
  }
  return nullptr;
}

