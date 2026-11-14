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
 * user.nsfs:rgw.etag as mtime-<b36>-ino-<b36> rather than an MD5 digest
 * when rgw_non_md5_etag is set:  one key, two value formats, chosen by a
 * config var at the write site rather than by the format.
 *
 * object_owner() is the shape that handles this -- a question, with the
 * decode kept inside the strategy.  etag, content type, version id and
 * delete marker want the same treatment, and it is deferred deliberately
 * rather than overlooked:  specifying them needs an inventory of what
 * NooBaa stores and how, which does not exist yet.  See the decomposition
 * plan, S5. */

/* nsfs's own layout:  logical keys under user.nsfs:, with RGW's own
 * user.rgw. attributes re-prefixed to user.nsfs:rgw. so that the two
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


  const char* name() const override { return "rgw"; }
};


}}} // namespace rgw::sal::nsfs
