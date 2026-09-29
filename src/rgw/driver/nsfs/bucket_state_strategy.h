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

#include "rgw_common.h"
#include "rgw_sal.h"

namespace rgw { namespace sal { namespace nsfs {

/* The attribute a bucket's encoded RGWBucketInfo is written to.
 *
 * A constant and not a strategy question, because every profile writes
 * it:  RGW operates on bucket state in its own form in all three, and a
 * base bucket carries one attribute NooBaa does not read.  Reading is
 * the part that varies, which is what the interface below is for. */
inline constexpr const char* BUCKET_INFO_KEY = "bucket_info";

/* Where a bucket's own state comes from.
 *
 * Not an attribute name.  That was `XattrStrategy::bucket_info_key()`,
 * and it could not express NooBaa, whose bucket state is a JSON file
 * outside the data tree entirely -- no value of a `const char*` names a
 * file in another directory.  Both implementations of it returned the
 * same string, which is what a vestigial abstraction looks like.
 *
 * Reading only.  Writing is uniform across the profiles, so a `store()`
 * here would be a virtual with one implementation.
 *
 * The caller sees RGW types.  A format whose state is a dozen JSON
 * fields has to place them:  some become members of RGWBucketInfo, and
 * some -- bucket policy, lifecycle, CORS, encryption -- have to appear
 * in the attribute map, because `rgw_op.cc` reaches into it directly and
 * a question method on this interface would never be called.  That is
 * the same constraint that forced `parse_disk_attrs()` for object tags,
 * one level up. */
class BucketStateStrategy {
public:
  virtual ~BucketStateStrategy() = default;

  /* Fill `info`, and add to `attrs` whatever this bucket's stored state
   * says that RGW reads from the attribute map.
   *
   * `attrs` is in and out:  it arrives holding what was read from the
   * bucket directory, and an implementation both consumes from it --
   * erasing what is its own and not object metadata -- and adds to it.
   *
   * `dir_fd` is the bucket directory, for a format that keeps state
   * beside the data;  `bucket_name` is for one that keeps it elsewhere.
   *
   * THREE OUTCOMES, and they are not two:
   *
   *   0          state was read
   *   -ENOENT    there is none, which is legitimate;  the bucket has no
   *              stored state and the caller's defaults stand
   *   -EBADMSG   state is there and could not be read
   *   other      the store itself could not be reached
   *
   * -EBADMSG is its own code rather than a generic failure because
   * three callers have to tell it apart:  `load_bucket()` fails the
   * bucket on it, `list_buckets()` skips that one bucket rather than
   * denying the whole account a listing, and `get_bucket_profile()`
   * ignores it, because a bucket's profile comes from its marker and an
   * operator diagnosing unreadable state needs that endpoint most.
   *
   * The third is the one that matters.  A bucket whose object-lock
   * record will not parse must not be served as though it had none,
   * because unset is permissive for lock, policy, public access block
   * and encryption alike.  Distinguishing absent from unreadable is
   * what lets the caller fail closed on the second without refusing the
   * first. */
  virtual int load(const DoutPrefixProvider* dpp, int dir_fd,
		   const std::string& bucket_name,
		   Attrs& attrs, RGWBucketInfo& info) const = 0;

  virtual const char* name() const = 0;
};

/* Our own:  one attribute on the bucket directory holding an encoded
 * RGWBucketInfo, which is what every profile writes and what shared and
 * strong also read. */
class RgwBucketStateStrategy : public BucketStateStrategy {
public:
  int load(const DoutPrefixProvider* dpp, int dir_fd,
	   const std::string& bucket_name,
	   Attrs& attrs, RGWBucketInfo& info) const override;

  const char* name() const override { return "rgw"; }
};

/* NooBaa's:  one JSON record per bucket in a store outside the tree.
 *
 * `<config_root>/buckets/<name>.json`.  This is the reader the interface
 * exists for -- the thing `bucket_info_key()` structurally could not
 * name.
 *
 * WHERE THE ADDRESS COMES FROM.  `rgw_nsfs_noobaa_config_root`, which
 * names the *resolved* directory.  NooBaa starts at `/etc/noobaa.conf.d`,
 * which is the configuration directory itself unless it holds a
 * `config_dir_redirect` file naming another one (`config.js`,
 * `_get_config_root()`;  their `Configuration.md`).  We hold the resolved
 * address rather than repeating that lookup, because an upgrade that
 * removes their package removes the well-known path -- and in an
 * un-redirected deployment removes the directory itself, since the RPM
 * created it.  A bucket's retention settings must not depend on a step
 * of the cutover that nobody thinks of as data.
 *
 * WE READ ONE SUBDIRECTORY OF IT.  `buckets/`, and nothing else.  Of the
 * eight entries a configuration directory holds, `system.json`,
 * `config.json` and `certificates/` are their service's own and mean
 * nothing here;  `identities/`, `accounts_by_name/`, `access_keys/` and
 * `master_keys.json` are an account import's business and not the
 * request path's.  We are not a NooBaa, and most of what they keep
 * there describes their deployment rather than the data.
 *
 * WE NEVER WRITE IT.  Not a preference:  Madhu, 2026-09-29 -- NooBaa
 * owns the shared root entirely and nothing populates it from outside.
 *
 * WHAT IS MAPPED, AND WHAT IS NOT.  Versioning and the creation date,
 * which is what a base bucket loses today and what silently costs
 * versions:  a bucket that reads as unversioned takes a PUT in place
 * instead of demoting the current version into `.versions/`.
 *
 * `owner_account` is read and not applied.  It is an account id in
 * their store, and turning one into an `rgw_owner` needs the account
 * import;  a base bucket's owner is already whatever load_bucket()
 * seeds.  `path` likewise -- placement is a path and the fileset is
 * observed, not stored (ACCOUNT_METADATA.md 8.1), but nothing consumes
 * it here yet.  Their optional features -- policy, lifecycle, CORS,
 * encryption, website, public access block, object lock, tags -- are
 * each a shape translation into the attribute map and are the step
 * after this one. */
class NooBaaBucketStateStrategy : public BucketStateStrategy {
  std::string config_root;

public:
  explicit NooBaaBucketStateStrategy(std::string root)
    : config_root(std::move(root)) {}

  int load(const DoutPrefixProvider* dpp, int dir_fd,
	   const std::string& bucket_name,
	   Attrs& attrs, RGWBucketInfo& info) const override;

  const char* name() const override { return "noobaa"; }
};

}}} // namespace rgw::sal::nsfs
