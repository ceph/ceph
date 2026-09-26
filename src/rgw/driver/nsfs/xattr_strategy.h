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

#pragma once

#include <string>

#include <linux/stat.h>

#include "rgw_acl.h"
#include "rgw_sal.h"

namespace rgw { namespace sal { namespace nsfs {

/* Where an object's metadata lives on disk, and what it is called.
 *
 * Unlike FSStrategy and MPUStrategy this one deals in RGW types, because
 * one of its questions -- who owns this object -- is an RGW question whose
 * answer is not necessarily an attribute at all.
 *
 * Selected by the bucket's recorded format, not by probing:  a tree
 * written one way cannot be read the other, so this is a property of the
 * data rather than of the machine it is running on. */
class XattrStrategy {
public:
  virtual ~XattrStrategy() = default;

  /* the on-disk xattr name for a logical attribute key, and the inverse.
   * parse_disk_name() returns false for a name this format does not own,
   * which is how a foreign attribute -- another gateway's, or a user's --
   * is left alone rather than surfaced as object metadata. */
  virtual std::string disk_name(const std::string& key) const = 0;
  virtual bool parse_disk_name(const std::string& disk,
			       std::string& key) const = 0;

  /* Who owns this object.
   *
   * A question, not "read this attribute", and that is the whole point of
   * having this method.  nsfs records ownership in an ACL attribute and
   * the file's uid is incidental;  NooBaa records no owner at all, and the
   * file's uid *is* the answer, because the write happened under the
   * account's identity.  A caller which reads a key has already assumed
   * the first of those -- which is why a file created by a native GPFS
   * client currently lists as unknown/unknown.
   *
   * stx may be null when the caller has attributes but no stat;  a format
   * which needs the inode says so by failing. */
  virtual int object_owner(const Attrs& attrs, const struct statx* stx,
			   ACLOwner& owner) const = 0;

  /* Does RGW store this attribute's value as a counted string, with its
   * NUL terminator included?
   *
   * `rgw_get_request_metadata` (`rgw_op.h:2479`) and four other writers
   * append `size() + 1`.  On rados the byte is private.  On a filesystem
   * it is on-disk format, and it makes our metadata differ from NooBaa's
   * for the same object by a byte nobody meant to write.
   *
   * The driver strips it on the way to disk and restores it on the way
   * back, so the tree is clean and every RGW consumer sees what it sees
   * on rados.  A compensation, not a fix -- see
   * docs/RGW_COUNTED_STRING_ATTRS.md.  If upstream drops the terminator,
   * this predicate and `attr_on_disk()` go with it.
   *
   * It asks about the KEY because the value cannot say:  a ceph-encoded
   * blob may legitimately end in a zero byte, so "strip whatever ends in
   * NUL" would corrupt an ACL or a bucket_info.  And only attributes
   * that are counted on *every* path that writes them may be listed --
   * etag is not, which is why it is absent. */
  virtual bool counted_string_value(const std::string& key) const = 0;

  /* The attribute holding this bucket's encoded RGWBucketInfo.
   *
   * Both formats answer the same way, deliberately:  a tree we write is
   * readable by us and carries owner, versioning and placement which
   * NooBaa's own config store holds elsewhere and which we do not write.
   * It is a method so that the decision is stated once and in one place,
   * not because the two differ.  See ACCOUNT_METADATA.md and the
   * decomposition plan 3.1 -- our noobaa format is a fork of NooBaa's, and
   * this attribute is where that shows. */
  virtual const char* bucket_info_key() const = 0;

  virtual const char* name() const = 0;
};

/* NOT COVERED HERE:  the FORMAT OF A VALUE.
 *
 * disk_name()/parse_disk_name() rename;  the bytes pass through and the
 * caller decodes them with RGW's decoder.  That is right where the value
 * is opaque on both sides -- user metadata is just bytes -- and wrong
 * wherever it is structured.  RGW_ATTR_ACL is an encoded
 * RGWAccessControlPolicy;  NooBaa has no such attribute and its xattrs are
 * plain strings under its own conventions, so a rename-only mapping would
 * hand a NooBaa value to a ceph decoder.
 *
 * It is not only a cross-format problem.  nsfs_apply_non_md5_etag() writes
 * user.nsfs.rgw.etag as mtime-<b36>-ino-<b36> rather than an MD5 digest
 * when rgw_non_md5_etag is set:  one key, two value formats, chosen by a
 * config var at the write site rather than by the format.
 *
 * object_owner() is the shape that handles this -- a question, with the
 * decode kept inside the strategy.  etag, content type, version id and
 * delete marker want the same treatment, and it is deferred deliberately
 * rather than overlooked:  specifying them needs an inventory of what
 * NooBaa stores and how, which does not exist yet.  See the decomposition
 * plan, S5. */

/* nsfs's own layout:  logical keys under user.nsfs., with RGW's own
 * user.rgw. attributes re-prefixed to user.nsfs.rgw. so that the two
 * namespaces cannot collide, and ownership decoded from RGW_ATTR_ACL. */
class RGWXattrStrategy : public XattrStrategy {
public:
  std::string disk_name(const std::string& key) const override;
  bool parse_disk_name(const std::string& disk,
		       std::string& key) const override;

  int object_owner(const Attrs& attrs, const struct statx* stx,
		   ACLOwner& owner) const override;

  bool counted_string_value(const std::string& key) const override;

  const char* bucket_info_key() const override;

  const char* name() const override { return "rgw"; }
};

}}} // namespace rgw::sal::nsfs
