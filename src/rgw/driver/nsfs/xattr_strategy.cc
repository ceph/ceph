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

#include <cerrno>

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* The prefixes, in one place.
 *
 * RGW's own attributes arrive named user.rgw.* and are re-prefixed rather
 * than stored as they come, so that this driver's namespace and a
 * generic one cannot collide on a tree something else also writes. */
static const std::string NSFS_XATTR_PREFIX = "user.nsfs.";
static const std::string NSFS_RGW_XATTR_PREFIX = "user.nsfs.rgw.";
static const std::string RGW_ATTR_PFX = "user.rgw.";

std::string RGWXattrStrategy::disk_name(const std::string& key) const
{
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return NSFS_RGW_XATTR_PREFIX + key.substr(RGW_ATTR_PFX.size());
  }
  return NSFS_XATTR_PREFIX + key;
}

bool RGWXattrStrategy::parse_disk_name(const std::string& disk,
				       std::string& key) const
{
  if (disk.compare(0, NSFS_RGW_XATTR_PREFIX.size(),
		   NSFS_RGW_XATTR_PREFIX) == 0) {
    key = RGW_ATTR_PFX + disk.substr(NSFS_RGW_XATTR_PREFIX.size());
    return true;
  }
  if (disk.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = disk.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }
  return false;
}

int RGWXattrStrategy::object_owner(const Attrs& attrs,
				   const struct statx* stx,
				   ACLOwner& owner) const
{
  /* recorded, and separate from the file's uid.  Nothing keeps the two
   * consistent, which is why a file created outside this gateway has no
   * answer here at all. */
  auto i = attrs.find(RGW_ATTR_ACL);
  if (i == attrs.end()) {
    return -EINVAL;
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

/* The attributes RGW writes as counted strings, from the five writers
 * catalogued in docs/RGW_COUNTED_STRING_ATTRS.md.  Everything else --
 * the encoded ACL, object_type, bucket_info, the multipart blobs -- is
 * ceph-encoded and must not be touched. */
bool RGWXattrStrategy::counted_string_value(const std::string& key) const
{
  /* x-amz-meta-*, from rgw_get_request_metadata() and the librgw
   * setxattr path */
  if (key.compare(0, sizeof(RGW_ATTR_META_PREFIX) - 1,
		  RGW_ATTR_META_PREFIX) == 0) {
    return true;
  }

  /* s->generic_attrs, written by rgw_op.cc:3861;  the table is
   * generic_attrs[] at rgw_rest.cc:122 */
  return key == RGW_ATTR_CONTENT_TYPE ||
         key == RGW_ATTR_CONTENT_LANG ||
         key == RGW_ATTR_EXPIRES ||
         key == RGW_ATTR_CACHE_CONTROL ||
         key == RGW_ATTR_CONTENT_DISP ||
         key == RGW_ATTR_CONTENT_ENC ||
         key == RGW_ATTR_X_ROBOTS_TAG;

  /* NOT RGW_ATTR_ETAG.  Only the opaque-etag path (rgw_op.cc:9000)
   * writes it counted;  the ordinary MD5 path does not, and nsfs stores
   * it bare since 15386ec4a26.  Listing it made the strip a no-op and
   * the restore a corruption -- HEAD returned an etag with a NUL inside
   * the quotes. */
}

const char* RGWXattrStrategy::bucket_info_key() const
{
  return "bucket_info";
}

}}} // namespace rgw::sal::nsfs
