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

/* for xattr_map_t:  the on-disk view, full names to raw values, which
 * the widened pair below deals in */
#include "fs_strategy.h"

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

  /* The same pair, widened from one name to a set of whole attributes.
   *
   * disk_name()/parse_disk_name() rename a key and let the bytes
   * through, which is all a format needs when one logical attribute is
   * one attribute on disk.  Where it is not, they can say nothing:
   * object tags are one ceph-encoded RGWObjTags for us and one
   * attribute per tag for NooBaa, so the mapping is one-to-many in
   * both directions and the value has to be built rather than copied.
   *
   * These have to be the mechanism rather than a question like
   * object_owner(), because the consumer is not ours.  rgw_op.cc reads
   * and writes tags through the attribute map -- `attrs.find(
   * RGW_ATTR_TAGS)` in six places, modify_obj_attrs() for
   * PutObjectTagging, delete_obj_attrs() for the delete -- and that is
   * generic code shared with every driver.  A method on this interface
   * would never be called.  So RGW_ATTR_TAGS must appear in the map,
   * assembled from what is on disk, and fan back out when written.
   *
   * Both take the on-disk NAMES and an fd, not a map of values.  The
   * caller has the names already -- one flistxattr it was doing
   * anyway -- and reading every value to hand over would buy an
   * fgetxattr per object for attributes no format wants;  on our own
   * trees the unclaimed set is `security.selinux` and nothing else.
   * So each format reads what it claims and nothing else, and a
   * format that claims nothing issues no syscall at all.
   *
   * parse_disk_attrs() runs after the per-name pass and is given the
   * names that pass could not place.  It adds the logical attributes
   * they encode.
   *
   * disk_attrs() translates one logical attribute the other way.  It
   * is given the names currently on the file because a one-to-many
   * write has to remove what it supersedes, and which names those are
   * is only knowable from the disk -- the tag keys are IN the names.
   * Returns false for a key it does not handle, which is the signal to
   * fall back to disk_name(). */
  virtual void parse_disk_attrs(const DoutPrefixProvider* dpp, int fd,
				const std::vector<std::string>& unclaimed,
				Attrs& out) const {}

  virtual bool disk_attrs(const std::string& key, const bufferlist& val,
			  const std::vector<std::string>& current,
			  xattr_map_t& to_write,
			  std::vector<std::string>& to_remove) const {
    return false;
  }

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
 * namespaces cannot collide, and ownership decoded from RGW_ATTR_ACL.
 *
 * Named for the prefixing, which is what distinguishes it:  the NooBaa
 * format keeps plain names and declines to claim foreign ones.  Not
 * "RGW", which is the prefix on half the classes in this tree and so
 * cannot carry a distinction from NooBaa at all. */
class PrefixedXattrStrategy : public XattrStrategy {
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

/* NooBaa's layout, which is what the base profile is.
 *
 *   user.<k>                          user metadata, key for key
 *   user.content_md5                  the etag
 *   user.noobaa.content_type          content type
 *   user.noobaa.content_encoding      content encoding
 *   user.noobaa.version_id            version id
 *   user.noobaa.delete_marker         delete marker
 *   user.noobaa.non_current_timestamp when it stopped being current
 *
 * Read from noobaa-core at 68ca22d33, `namespace_fs.js`:  the constant
 * block at the head of the file, and `to_fs_xattr()` for the user
 * metadata mapping.
 *
 * Ownership comes from the inode.  They record no owner anywhere, and
 * the file's uid is the answer because the write happened under the
 * account's identity -- which is also what stops a natively created
 * file listing as unknown/unknown.
 *
 * NOT MAPPED, DELIBERATELY:
 *
 * S3 ACLs.  They store none, so there is nowhere for RGW_ATTR_ACL to
 * go.  The write is accepted and not stored, which is what their own
 * gateway does -- `s3_put_object_acl.js` says so:  "we only handle
 * canned acl, the rest is deprecated in favor of bucket policy.
 * however we do not fail the request because there are still clients
 * that call it."  Refusing would make base stricter than the format it
 * emulates and break the clients that comment protects.  Losing an ACL
 * in *translation* is a different matter and is the export manifest's
 * job;  a base bucket never held one.
 *
 * Object tags and the object-lock family.  Not renames:  ours is one
 * encoded blob for every tag and two encoded attributes for the lock,
 * theirs is user.noobaa.tag.<k> per tag and three plain attributes.
 * Changing the shape of a value is what disk_name() and
 * parse_disk_name() cannot express, and it is the value-format work S5
 * names separately.  Deferred, not overlooked. */
class NooBaaXattrStrategy : public XattrStrategy {
public:
  std::string disk_name(const std::string& key) const override;
  bool parse_disk_name(const std::string& disk,
		       std::string& key) const override;

  /* object tags, which are one attribute per tag in their format and
   * one encoded RGWObjTags in ours */
  void parse_disk_attrs(const DoutPrefixProvider* dpp, int fd,
			const std::vector<std::string>& unclaimed,
			Attrs& out) const override;
  bool disk_attrs(const std::string& key, const bufferlist& val,
		  const std::vector<std::string>& current,
		  xattr_map_t& to_write,
		  std::vector<std::string>& to_remove) const override;

  int object_owner(const Attrs& attrs, const struct statx* stx,
		   ACLOwner& owner) const override;

  bool counted_string_value(const std::string& key) const override;

  const char* bucket_info_key() const override;

  const char* name() const override { return "noobaa"; }
};

}}} // namespace rgw::sal::nsfs
