// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/restore.h"

#include <iostream>
#include <optional>
#include <string>

#include "common/errno.h"
#include "rgw_basic_types.h"
#include "rgw_restore.h"
#include "rgw_sal.h"

using namespace rgw_admin;
using namespace std;

int rgw_admin_restore(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      RGWStreamFlusher& stream_flusher,
                      const rgw_admin_restore_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& object = *opts.object;
  auto& restore_status_filter = *opts.restore_status_filter;

  rgw::restore::RestoreEntry entry;
  entry.bucket = rgw_bucket {tenant, bucket_name};

  std::string err_msg;
  int ret = 0;
  if (command == OPT::RESTORE_STATUS) {
    entry.obj_key = rgw_obj_key {object};
    ret = driver->get_rgwrestore()->status(dpp, entry, err_msg,
                                           stream_flusher, null_yield);
  } else if (command == OPT::RESTORE_LIST) {
    ret = driver->get_rgwrestore()->list(dpp, entry, restore_status_filter,
                                         err_msg, stream_flusher, null_yield);
  }

  if (ret < 0) {
    cerr << err_msg << std::endl;
    return -ret;
  }
  return 0;
}
