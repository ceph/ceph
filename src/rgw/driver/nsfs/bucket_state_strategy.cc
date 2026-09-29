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

#include <fcntl.h>
#include <unistd.h>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/scope_guard.h"

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

namespace {

/* Their record whole, in one read.  No iostreams and no incremental
 * parse:  a bucket record is small, and a cap keeps a corrupt or
 * hostile file from being read into memory whole. */
constexpr size_t NB_RECORD_MAX = 1u << 20;

int slurp(const DoutPrefixProvider* dpp, const std::string& path,
	  std::string& out)
{
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return -errno;
  }
  auto close_fd = make_scope_guard([fd] { ::close(fd); });

  out.clear();
  char buf[8192];
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      return -errno;
    }
    if (n == 0) {
      break;
    }
    if ((out.size() + n) > NB_RECORD_MAX) {
      ldpp_dout(dpp, 0) << "ERROR: " << path << " exceeds "
	<< NB_RECORD_MAX << " bytes" << dendl;
      return -EFBIG;
    }
    out.append(buf, n);
  }
  return 0;
}

} // namespace

int NooBaaBucketStateStrategy::load(const DoutPrefixProvider* dpp, int dir_fd,
				    const std::string& bucket_name,
				    Attrs& attrs, RGWBucketInfo& info) const
{
  if (config_root.empty()) {
    return -ENOENT;
  }

  const std::string path =
    config_root + "/buckets/" + bucket_name + ".json";

  std::string doc;
  int ret = slurp(dpp, path, doc);
  if (ret == -ENOENT) {
    /* no record:  this bucket has no state in their store, which is
     * the absent case and not a failure */
    return -ENOENT;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: reading " << path << ": "
      << cpp_strerror(-ret) << dendl;
    return ret;
  }

  JSONParser p;
  if (!p.parse(doc.data(), doc.size())) {
    ldpp_dout(dpp, 0) << "ERROR: " << path << " is not JSON" << dendl;
    return -EBADMSG;
  }

  /* Versioning.  Their enum is the whole of it:  DISABLED, SUSPENDED,
   * ENABLED.  RGW keeps a suspended bucket versioned and marks it
   * suspended beside that, because it still holds versions. */
  std::string versioning;
  JSONDecoder::decode_json("versioning", versioning, &p);
  if (versioning == "ENABLED") {
    info.flags |= BUCKET_VERSIONED;
  } else if (versioning == "SUSPENDED") {
    info.flags |= BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED;
  } else if (!versioning.empty() && (versioning != "DISABLED")) {
    ldpp_dout(dpp, 0) << "ERROR: " << path << " has versioning \""
      << versioning << "\", which is none of theirs" << dendl;
    return -EBADMSG;
  }

  std::string created;
  JSONDecoder::decode_json("creation_date", created, &p);
  if (!created.empty()) {
    ceph::real_time t;
    if (parse_time(created.c_str(), &t) == 0) {
      info.creation_time = t;
    } else {
      /* not fatal:  a creation date we cannot read costs a listing the
       * right timestamp, and nothing else.  Versioning is the reason
       * this reader exists. */
      ldpp_dout(dpp, 4) << "nsfs: " << path << " has an unparsable "
	<< "creation_date \"" << created << "\"" << dendl;
    }
  }

  ldpp_dout(dpp, 10) << "nsfs: bucket " << bucket_name
    << " took versioning from " << path << ": "
    << (versioning.empty() ? "unset" : versioning) << dendl;

  return 0;
}

}}} // namespace rgw::sal::nsfs
