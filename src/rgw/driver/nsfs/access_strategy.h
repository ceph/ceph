// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * nsfs:  what to do when the filesystem denies an operation S3
 * authorised.
 */

#pragma once

namespace rgw { namespace sal { namespace nsfs {

/* What a denied operation was touching.
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
 * Three values, by which access path is native:  `posix-strict` (the
 * filesystem;  a side-loading NFS or SMB client is co-equal, so POSIX
 * permissions must stay truthful), `s3-metadata` (S3 for the
 * gateway's records *about* objects), `s3-native` (S3;  the
 * filesystem is storage).
 *
 * A further position exists in the tree and is deliberately not
 * expressed here yet:  lifecycle acts as the **bucket owner**
 * (DECISIONS.md §6.3), which is neither the requester nor the
 * gateway.  When it is brought onto this axis it belongs as another
 * `Refused` value rather than as another branch at its call site --
 * see inflight/access-model-axis.md, which catalogues the positions
 * found so far. */
class AccessStrategy {
public:
  virtual ~AccessStrategy() = default;

  /* What to do when the filesystem denies this class of operation
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

/* S3 decides for an object's metadata, and not for its bytes.
 *
 * The middle value.  It is enforced here rather than at the call
 * sites, so a reader of this class knows the whole of it.
 *
 * The data answer is this value's, not the axis's.  `s3-native`
 * below answers data differently and is a supported model;  an
 * earlier version of this comment called the data line "the line the
 * axis is drawn on", which overstated a description of what was
 * built as a principle. */
class S3MetadataAccess final : public AccessStrategy {
public:
  Refused refused(AccessClass what) const override {
    return (what == AccessClass::object_attrs) ? Refused::as_gateway_raised
					       : Refused::refuse;
  }
  const char* name() const override { return "s3-metadata"; }
};

/* S3 is the native access model:  S3 decides, for data as well as
 * metadata.
 *
 * The filesystem is storage, and a side-loading NFS or SMB client is
 * secondary -- so POSIX permissions no longer describe who can reach
 * an object's bytes.  A deployment choosing this accepts that;  one
 * that cannot should run `posix-strict`, which is Spectrum Scale's
 * default.
 *
 * This is what `rgw::sal::POSIXDriver` already does.  `rgw_sal_posix.cc`
 * has no `FSIdentity`, no `run_as` and no identity plumbing:  every
 * request runs as the gateway and S3 is the only authority.  So nsfs
 * here and the posix driver should score alike on the same suite, and
 * a difference means one of them is wrong.
 *
 * The invariant test takes its second polarity from here:
 * `posix-strict` denies the bytes and `s3-native` serves them, so it
 * watches itself fail without a source edit and a rebuild.  That was
 * the only thing this strategy did when it was written (2026-10-16),
 * reachable from the driver hint alone;  making it selectable was a
 * rename and a configuration change, not new behaviour. */
class S3NativeAccess final : public AccessStrategy {
public:
  Refused refused(AccessClass) const override {
    return Refused::as_gateway_raised;
  }
  const char* name() const override { return "s3-native"; }
};

} } } // namespace rgw::sal::nsfs
