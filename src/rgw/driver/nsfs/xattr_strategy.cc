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

std::string PrefixedXattrStrategy::disk_name(const std::string& key) const
{
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return NSFS_RGW_XATTR_PREFIX + key.substr(RGW_ATTR_PFX.size());
  }
  return NSFS_XATTR_PREFIX + key;
}

bool PrefixedXattrStrategy::parse_disk_name(const std::string& disk,
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

int PrefixedXattrStrategy::object_owner(const Attrs& attrs,
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
bool PrefixedXattrStrategy::counted_string_value(const std::string& key) const
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

const char* PrefixedXattrStrategy::bucket_info_key() const
{
  return "bucket_info";
}

/* --- NooBaaXattrStrategy ---------------------------------------------- */

static const std::string NB_USER_PREFIX = "user.";
static const std::string NB_INTERNAL_PREFIX = "user.noobaa.";
static const std::string NB_CONTENT_MD5 = "user.content_md5";

/* RGW's logical key on the left, theirs on the right.  One to one;  the
 * rows which are not are named in the header and deferred. */
struct NBKeyMap {
  const char* logical;
  const char* disk;
};

static const NBKeyMap NB_KEYS[] = {
  { RGW_ATTR_ETAG,            "user.content_md5" },
  { RGW_ATTR_CONTENT_TYPE,    "user.noobaa.content_type" },
  { RGW_ATTR_CONTENT_ENC,     "user.noobaa.content_encoding" },
  { "version_id",             "user.noobaa.version_id" },
  { "delete_marker",          "user.noobaa.delete_marker" },
  { "non_current_timestamp",  "user.noobaa.non_current_timestamp" },
};

/* Three answers, not two.
 *
 * A key they keep goes to their name.  A key of ours with no
 * counterpart -- bucket_info, rename intent -- goes under user.nsfs.,
 * where it is inert to them;  the convergence report already records
 * that we write bucket_info on a base bucket for exactly that reason.
 * Only an S3 attribute they deliberately do not keep is dropped, and
 * that is the ACL:  the empty name says "nowhere", and a write path
 * which sees one skips the attribute rather than inventing a place for
 * it.
 *
 * Returning the key unchanged would be the wrong third answer -- it
 * would write under a name neither format owns, which is how two
 * gateways come to disagree about what an object carries. */
std::string NooBaaXattrStrategy::disk_name(const std::string& key) const
{
  for (const auto& m : NB_KEYS) {
    if (key == m.logical) {
      return m.disk;
    }
  }

  /* user metadata is theirs key for key:  to_fs_xattr() prefixes with
   * "user." and nothing else */
  if (key.compare(0, std::strlen(RGW_ATTR_META_PREFIX),
		  RGW_ATTR_META_PREFIX) == 0) {
    return NB_USER_PREFIX + key.substr(std::strlen(RGW_ATTR_META_PREFIX));
  }

  /* nowhere:  they store no ACL, and their own gateway accepts the
   * request and keeps nothing (`s3_put_object_acl.js`) */
  if (key == RGW_ATTR_ACL) {
    return std::string();
  }

  /* ours, and inert to them */
  return NSFS_XATTR_PREFIX + key;
}

bool NooBaaXattrStrategy::parse_disk_name(const std::string& disk,
					  std::string& key) const
{
  for (const auto& m : NB_KEYS) {
    if (disk == m.disk) {
      key = m.logical;
      return true;
    }
  }

  /* ours, written on their tree because it has nowhere else to go */
  if (disk.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = disk.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }

  /* Everything else under user.noobaa. is theirs and structured --
   * tags, the object-lock family, the part record -- and this format
   * does not yet map any of it.  Claimed as unmapped rather than
   * surfaced:  handing a caller one of their plain strings under an RGW
   * key would feed a NooBaa value to a ceph decoder. */
  if (disk.compare(0, NB_INTERNAL_PREFIX.size(), NB_INTERNAL_PREFIX) == 0) {
    return false;
  }

  /* what is left under user. is user metadata */
  if (disk.compare(0, NB_USER_PREFIX.size(), NB_USER_PREFIX) == 0) {
    key = std::string(RGW_ATTR_META_PREFIX) +
	  disk.substr(NB_USER_PREFIX.size());
    return true;
  }

  return false;
}

/* From the inode.  They record no owner anywhere:  the write happened
 * under the account's identity, so the file's uid is the answer, and it
 * is the only answer a natively created file has. */
int NooBaaXattrStrategy::object_owner(const Attrs& attrs,
				      const struct statx* stx,
				      ACLOwner& owner) const
{
  if (!stx || !(stx->stx_mask & STATX_UID)) {
    return -EINVAL;
  }
  owner.id = rgw_user(std::to_string(stx->stx_uid));
  return 0;
}

/* Never.  The terminator is an RGW-side accident that our own writers
 * append;  nothing in their tree carries one, and restoring a byte they
 * never wrote would corrupt the value.  See
 * docs/RGW_COUNTED_STRING_ATTRS.md. */
bool NooBaaXattrStrategy::counted_string_value(const std::string& key) const
{
  return false;
}

const char* NooBaaXattrStrategy::bucket_info_key() const
{
  return "bucket_info";
}

}}} // namespace rgw::sal::nsfs
