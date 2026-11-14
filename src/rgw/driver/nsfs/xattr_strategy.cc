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

#include "xattr_strategy.h"
#include "bucket_profile.h"


#include <cerrno>

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* The prefixes, in one place.
 *
 * RGW's own attributes arrive named user.rgw.* and are re-prefixed rather
 * than stored as they come, so that this driver's namespace and a
 * generic one cannot collide on a tree something else also writes.
 *
 * The ':' is what keeps NooBaa out (DECISIONS.md 2026-11-05).  NooBaa
 * stores user metadata as "user." and the rest of the x-amz-meta-
 * header's name (`s3_utils.js:119`, `namespace_fs.js:299`), and a header
 * name is an HTTP token, which has no ':'.  So no NooBaa object carries
 * a name under this prefix, and `x-amz-meta-nsfs.rgw.etag` is the user
 * metadata it was written as, not our ETag. */
static const std::string NSFS_XATTR_PREFIX = "user.nsfs:";
static const std::string NSFS_RGW_XATTR_PREFIX = "user.nsfs:rgw.";
static const std::string RGW_ATTR_PFX = "user.rgw.";

std::string PrefixedXattrStrategy::disk_name(const std::string& key) const
{
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return fmt::format("{}{}", NSFS_RGW_XATTR_PREFIX,
		       std::string_view(key).substr(RGW_ATTR_PFX.size()));
  }
  return fmt::format("{}{}", NSFS_XATTR_PREFIX, key);
}

bool PrefixedXattrStrategy::parse_disk_name(const std::string& disk,
				       std::string& key) const
{
  /* a marker, which is no attribute's */
  if (disk.starts_with(MARKER_XATTR_PREFIX)) {
    return false;
  }
  if (disk.compare(0, NSFS_RGW_XATTR_PREFIX.size(),
		   NSFS_RGW_XATTR_PREFIX) == 0) {
    key = fmt::format("{}{}", RGW_ATTR_PFX,
		      std::string_view(disk).substr(NSFS_RGW_XATTR_PREFIX.size()));
    return true;
  }
  if (disk.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = disk.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }
  return false;
}

int PrefixedXattrStrategy::object_owner(const Attrs& attrs,
				   ACLOwner& owner) const
{
  auto i = attrs.find(RGW_ATTR_ACL);
  if (i == attrs.end()) {
  }
  RGWAccessControlPolicy policy;
  try {
    auto bp = i->second.cbegin();
    policy.decode(bp);
  } catch (const buffer::error&) {
    return -EIO;
  }
  owner = policy.get_owner();
  return 0;
}

bool PrefixedXattrStrategy::counted_string_value(const std::string& key) const
{

  /* nsfs's, written on NooBaa's tree because it has nowhere else to go;
   * a marker is no attribute's */
  if (disk.starts_with(MARKER_XATTR_PREFIX)) {
    return false;
  }

  /* What is left under user. is user metadata, `user.nsfs.*` included:
   * a client's key that begins "nsfs." */

}}} // namespace rgw::sal::nsfs
