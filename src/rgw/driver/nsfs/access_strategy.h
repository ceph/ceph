// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * nsfs:  what to do when the filesystem refuses an operation S3
 * authorised.
 */

#pragma once

namespace rgw { namespace sal { namespace nsfs {

/* What a refused operation was touching.
 *
 * The axis has different answers for different things, which is why
 * this is a parameter and not one switch:  nobody wants the gateway
 * overwriting another identity's bytes, and a deployment may well
 * want an S3 grant to reach that object's tags. */
enum class AccessClass {
  /* the object's bytes */
  data,
  /* the gateway's own records *about* an object -- its tags and its
   * ACL.  Invisible to an NFS or SMB client, and NooBaa stores no
   * ACLs at all, so there is no "the filesystem should govern this"
   * story behind them. */
  object_attrs,
};

/* And what to do about it. */
enum class Refused {
  /* both S3 and the filesystem must allow.  Cross-identity sharing
   * is arranged in the filesystem, with a supplementary group */
  refuse,
  /* S3 decides:  the gateway performs the operation under a raise,
   * having already authorised it */
  as_gateway_raised,
};

/* The access model this gateway runs.
 *
 * Separate from the *format* strategies, which select whose
 * attribute names and staging layout are on disk.  This selects
 * authority, and the two do not co-vary:  an adopted NooBaa-format
 * bucket could want cross-account policy, and an extended bucket
 * could want strict parity.
 *
 * A third position exists in the tree and is deliberately not
 * expressed here yet:  lifecycle acts as the **bucket owner**
 * (DECISIONS.md §6.3), which is neither the requester nor the
 * gateway.  When it is brought onto this axis it belongs as another
 * `Refused` value rather than as another branch at its call site --
 * see inflight/access-model-axis.md, which catalogues the positions
 * found so far. */
class AccessStrategy {
public:
  virtual ~AccessStrategy() = default;

  /* What to do when the filesystem refuses this class of operation
   * to an identity S3 authorised. */
  virtual Refused refused(AccessClass what) const = 0;

  /* For the `fs features` map.  A test cannot infer the model from
   * any request -- every model answers alike wherever S3 and POSIX
   * agree, which is nearly always -- so the gateway names it. */
  virtual const char* name() const = 0;
};

/* Both must allow.  The default, and NooBaa's model:  they perform
 * every operation as the requesting account, and their IAM users and
 * roles carry their account's uid, so S3 and POSIX cannot disagree
 * inside an account. */
class PosixStrictAccess final : public AccessStrategy {
public:
  Refused refused(AccessClass) const override { return Refused::refuse; }
  const char* name() const override { return "posix-strict"; }
};

/* S3 decides for an object's metadata, and never for its bytes.
 *
 * `rgw_nsfs_cross_account_permission`.  The data answer is not an
 * oversight and not a default to be relaxed later:  it is the line
 * the axis is drawn on, and it is enforced here rather than at the
 * call sites, so a reader of this class knows the whole of it. */
class S3MetadataAccess final : public AccessStrategy {
public:
  Refused refused(AccessClass what) const override {
    return (what == AccessClass::object_attrs) ? Refused::as_gateway_raised
					       : Refused::refuse;
  }
  const char* name() const override { return "s3-metadata"; }
};

/* Data on S3's word -- **wrong on purpose, and reachable only from a
 * driver hint.**
 *
 * No configuration selects this.  It exists so that the test which
 * asserts the attribute/byte line can watch itself fail:  a test for
 * an invariant that has only ever seen a correct system proves
 * nothing, and the guard this project wrote for exactly this line
 * once never ran at all, because it keyed on a flag the real path
 * did not set (2026-10-16).
 *
 * Selecting it makes the gateway serve another identity's bytes on
 * an S3 grant alone, which is the mistake a future change would
 * make.  The hint restores the configured model afterwards. */
class DataOnS3sWordAccess final : public AccessStrategy {
public:
  Refused refused(AccessClass) const override {
    return Refused::as_gateway_raised;
  }
  const char* name() const override { return "data-on-s3s-word"; }
};

} } } // namespace rgw::sal::nsfs
