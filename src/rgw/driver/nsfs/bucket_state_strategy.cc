// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include "bucket_state_strategy.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

int RgwBucketStateStrategy::load(const DoutPrefixProvider* dpp, int dir_fd,
				 const std::string& bucket_name,
				 Attrs& attrs, RGWBucketInfo& info) const
{
  auto i = attrs.find(BUCKET_INFO_KEY);
  if (i == attrs.end()) {
    return -ENOENT;
  }

  /* erased whether or not it decodes.  It is not object metadata and
   * surfacing it would put it in the bucket's S3 attribute set, which
   * is true of a corrupt one too. */
  bufferlist bl = i->second;
  attrs.erase(i);

  try {
    auto p = bl.cbegin();
    decode(info, p);
  } catch (buffer::error& err) {
    /* present and unreadable, which is not the same as absent:  the
     * bucket has stored state and we cannot see it */
    ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket_name << " has a "
      << BUCKET_INFO_KEY << " attribute that does not decode" << dendl;
    return -EBADMSG;
  }

  return 0;
}

}}} // namespace rgw::sal::nsfs
