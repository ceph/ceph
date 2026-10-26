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

/* NooBaa's per-bucket `force_md5_etag`, carried and not enforced.
 *
 * NooBaa's field name, so the provenance of the value is readable on
 * disk;  the XattrStrategy supplies the prefix, making it
 * `user.nsfs.force_md5_etag` in the nsfs format.  The value is NooBaa's
 * literal -- `true` or `false` -- and the attribute is absent when
 * NooBaa's record did not mention it, which keeps the tri-state
 * NooBaa's cascade depends on.  See `ACCOUNT_IMPORT.md` §6.1a. */
#define RGW_NSFS_ATTR_FORCE_MD5_ETAG "force_md5_etag"


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
   * what lets the caller fail closed on the second and still serve the
   * first. */
  virtual int load(const DoutPrefixProvider* dpp, int dir_fd,
		   const std::string& bucket_name,
		   Attrs& attrs, RGWBucketInfo& info) const = 0;

  virtual const char* name() const = 0;
};

/* The nsfs one:  one attribute on the bucket directory holding an encoded
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
 * `_get_config_root()`;  NooBaa's `Configuration.md`).  nsfs holds the
 * resolved address rather than repeating that lookup, because an upgrade that
 * removes NooBaa's package removes the well-known path -- and in an
 * un-redirected deployment removes the directory itself, since the RPM
 * created it.  A bucket's retention settings must not depend on a step
 * of the cutover that nobody thinks of as data.
 *
 * WE READ ONE SUBDIRECTORY OF IT.  `buckets/`, and nothing else.  Of the
 * eight entries a configuration directory holds, `system.json`,
 * `config.json` and `certificates/` are NooBaa's service's own and mean
 * nothing here;  `identities/`, `accounts_by_name/`, `access_keys/` and
 * `master_keys.json` are an account import's business and not the
 * request path's.  nsfs is not a NooBaa, and most of what NooBaa keeps
 * there describes NooBaa's deployment rather than the data.
 *
 * WE NEVER WRITE IT.  Not a preference:  Madhu, 2026-09-29 -- NooBaa
 * owns the shared root entirely and nothing populates it from outside.
 *
 * WHAT IS MAPPED, AND WHAT IS NOT.  Versioning and the creation date,
 * which is what a base bucket loses today and what silently costs
 * versions:  a bucket that reads as unversioned takes a PUT in place
 * instead of demoting the current version into `.versions/`.
 *
 * `owner_account` is an account id in NooBaa's store, applied through
 * the account import's record of it (IdentityDB::get_owner_by_noobaa_id).
 * Before the identities are imported there is no such record, and the
 * owner stays whatever load_bucket() seeded.  `path` likewise -- placement is a path and the fileset is
 * observed, not stored (ACCOUNT_METADATA.md 8.1), but nothing consumes
 * it here yet.  NooBaa's optional features -- policy, lifecycle, CORS,
 * encryption, website, public access block, object lock, tags -- are
 * each a shape translation into the attribute map and are the step
 * after this one. */
class IdentityDB;

class NooBaaBucketStateStrategy : public BucketStateStrategy {
  std::string config_root;
  const IdentityDB* identity_db;

public:
  NooBaaBucketStateStrategy(std::string root, const IdentityDB* idb)
    : config_root(std::move(root)), identity_db(idb) {}

  int load(const DoutPrefixProvider* dpp, int dir_fd,
	   const std::string& bucket_name,
	   Attrs& attrs, RGWBucketInfo& info) const override;

  /* P5.  Where NooBaa's record says the bucket's storage is.
   *
   * `path` is required on every NooBaa bucket record, so a real
   * deployment's buckets are scattered and unreachable without this --
   * and in `serve` mode nothing is written, so there is no index row
   * to read instead.  That is what makes serve mode mean anything for
   * a deployment we have not touched.
   *
   * **This is the existing consumer returning one more field, not a
   * new one.**  `archive/NSFS_BUCKET_PLACEMENT_PLAN.md` §7 withdrew a
   * resolution branch that would read NooBaa's store on the request path,
   * because the plan must not add a second consumer of it.  Serve mode
   * already reads this record on every request for a bucket's
   * configuration (`NSFS_ADOPTION.md` §7), so taking `path` from the
   * same place adds no access -- which is why the method lives here
   * rather than in the resolver.
   *
   * **Reading it never adopts.**  Resolution is read-only;  adoption
   * happens because the mode says so, or by explicit command.
   *
   * Returns -ENOENT when there is no record, and 0 with `out` empty
   * when the record names no path. */
  int recorded_path(const DoutPrefixProvider* dpp,
		    const std::string& bucket_name,
		    std::string& out) const;

  /* The `_id` NooBaa's record of `bucket_name` carries.  -ENOENT when
   * there is no record. */
  int recorded_id(const DoutPrefixProvider* dpp,
		  const std::string& bucket_name, std::string& out) const;

  /* The names NooBaa's store has a bucket record for, sorted:  every
   * `buckets/<name>.json`, without reading any of them.  Empty with no
   * store. */
  int list_records(const DoutPrefixProvider* dpp,
		   std::vector<std::string>& names) const;

  const char* name() const override { return "noobaa"; }
};

}}} // namespace rgw::sal::nsfs
