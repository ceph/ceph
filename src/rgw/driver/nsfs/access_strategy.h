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
  /* the directories on the way to an object, which the walk opens
   * before the object's own open.  A path is namespace, which the
   * gateway reads whatever the model (the listing rule), so permitting
   * it lets the walk go on as the gateway holding read-search;  the
   * content is still decided at the object, by its own class. */
  path,
};

/* The access model this gateway runs.
 *
 * Separate from the *format* strategies, which select whose
 * attribute names and staging layout are on disk.  This selects
 * authority, and the two do not co-vary:  an adopted NooBaa-format
 * bucket could want cross-account policy, and an extended bucket
 * could want strict parity.
 *
 * Two values, by which access path is native:  `posix-strict` (the
 * filesystem;  a side-loading NFS or SMB client is co-equal, so POSIX
 * permissions must stay truthful) and `s3-native` (S3;  the filesystem
 * is storage).  The classes are finer than the two values need, so a
 * model that draws the line between them can be added without
 * reclassifying a call site.
 *
 * It answers one question:  whether the gateway performs an operation
 * itself.  A further position exists in the tree and is a different
 * question:  lifecycle acts as the **bucket owner** (DECISIONS.md
 * §6.3), which is neither the requester nor the gateway -- *who*
 * performs it, not *whether* the gateway does.  See
 * inflight/access-model-axis.md, which catalogues the positions found
 * so far. */
class AccessStrategy {
public:
  virtual ~AccessStrategy() = default;

  /* Whether the model permits the gateway to perform this class of
   * operation itself, once S3 has authorised it and the filesystem has
   * denied the requester.  Where it does not, the denial stands, and
   * cross-identity sharing is arranged in the filesystem, with a
   * supplementary group. */
  virtual bool permits(AccessClass what) const = 0;

  /* For the `fs features` map.  A test cannot infer the model from
   * any request -- every model answers alike wherever S3 and POSIX
   * agree, which is nearly always -- so the gateway names it. */
  virtual const char* name() const = 0;
};

/* Both must allow.  The default, and NooBaa's model:  they perform
 * every operation as the requesting account, and their IAM users and
 * roles carry their account's uid, so S3 and POSIX cannot disagree
 * inside an account.  NooBaa walks as the requester, too. */
class PosixStrictAccess final : public AccessStrategy {
public:
  bool permits(AccessClass) const override { return false; }
  const char* name() const override { return "posix-strict"; }
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
  bool permits(AccessClass) const override { return true; }
  const char* name() const override { return "s3-native"; }
};

} } } // namespace rgw::sal::nsfs
