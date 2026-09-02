// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/bucket_resync_encrypted_multipart.h"

#include <iostream>
#include <memory>
#include <string>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "driver/rados/rgw_sal_rados.h"
#include "radosgw-admin/bucket.h"
#include "rgw_sal.h"
#include "services/svc_zone.h"

using namespace rgw_admin;
using namespace std;

int rgw_admin_bucket_resync_encrypted_multipart(
    const DoutPrefixProvider* dpp,
    rgw::sal::Driver* driver,
    ceph::Formatter* formatter,
    RGWStreamFlusher& stream_flusher,
    std::unique_ptr<rgw::sal::Bucket>& bucket,
    const rgw_admin_bucket_resync_encrypted_multipart_options& opts)
{
  auto& command = opts.command;
  auto& tenant = opts.tenant;
  auto& bucket_name = opts.bucket_name;
  auto& bucket_id = opts.bucket_id;
  auto& marker = opts.marker;
  bool yes_i_really_mean_it = opts.yes_i_really_mean_it;

  if (command != OPT::BUCKET_RESYNC_ENCRYPTED_MULTIPART) {
    return EINVAL;
  }

  // repair logic for replication of encrypted multipart uploads:
  // https://tracker.ceph.com/issues/46062
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
  if (ret < 0) {
    return -ret;
  }

  auto rados_driver = dynamic_cast<rgw::sal::RadosStore*>(driver);
  if (!rados_driver) {
    cerr << "ERROR: this command can only work when the cluster "
        "has a RADOS backing store." << std::endl;
    return EPERM;
  }

  // fail if recovery wouldn't generate replication log entries
  if (!rados_driver->svc()->zone->need_to_log_data() && !yes_i_really_mean_it) {
    cerr << "This command is only necessary for replicated buckets." << std::endl;
    cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return EPERM;
  }

  formatter->open_object_section("modified");
  encode_json("bucket", bucket->get_name(), formatter);
  encode_json("bucket_id", bucket->get_bucket_id(), formatter);

  ret = rados_driver->getRados()->bucket_resync_encrypted_multipart(
      dpp, null_yield, rados_driver, bucket->get_info(),
      marker, stream_flusher);
  if (ret < 0) {
    return -ret;
  }
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}
