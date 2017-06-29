// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include "rgw_client_io.h"
#include "rgw_crypt.h"
#include "rgw_crypt_sanitize.h"
#define dout_subsys ceph_subsys_rgw

namespace rgw {
namespace io {

void BasicClient::init(CephContext *cct) {
  init_env(cct);

  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    const auto& env_map = get_env().get_map();

    for (const auto& iter: env_map) {
      rgw::crypt_sanitize::env x{iter.first, iter.second};
      ldout(cct, 20) << iter.first << "=" << (x) << dendl;
    }
  }
}

} /* namespace io */
} /* namespace rgw */
