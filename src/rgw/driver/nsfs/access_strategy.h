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
 * The classes follow one criterion:  S3 may decide what a side-loading
 * client -- anyone reaching the bucket's storage other than through
 * RGW, an NFS or SMB export, a local process -- cannot observe, and the
 * filesystem decides what that client can (GLOSSARY.md, "access
 * model").  A caller names the class of what it does;  nothing infers
 * it. */
enum class AccessClass {
  /* what a side-loading client can observe:  an object's bytes, and
   * every entry in the tree, so creating, linking, renaming or
   * removing a file or a directory is data */
  data,
  /* the gateway's own records *about* an object -- its tags and its
   * ACL.  Invisible to a side-loading client, and NooBaa stores no
   * ACLs at all. */
  object_attrs,
  /* the directories on the way to an object, which the walk opens
   * before the object's own open.  A path shows nothing a listing does
   * not, and listings are complete under every model, so permitting it
   * lets the walk go on as the gateway holding read-search;  the
   * object is still decided by its own class. */
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
 * Under impersonation the authenticated user's POSIX identity can carry two
 * things:  authority, whether an operation may happen, and
 * attribution, whose its result is on disk.  A model says which the
 * identity keeps.  `posix-strict` keeps both;  `s3-native` keeps
 * attribution and gives authority to S3.  The classes are finer than
 * the two models need, so a model that draws the line between them
 * can be added without reclassifying a call site.
 *
 * It answers one question:  whether the gateway performs an operation
 * itself, with a raise, once S3 has authorised it and the filesystem
 * has refused the authenticated user.  A further position exists in
 * the tree and is a different question:  lifecycle acts as the
 * **bucket owner** (DECISIONS.md §6.3), which is neither the
 * authenticated user nor the gateway -- *who* performs it, not
 * *whether* the gateway does.  See inflight/access-model-axis.md,
 * which catalogues the positions found so far. */
class AccessStrategy {
public:
  virtual ~AccessStrategy() = default;

  /* Whether the model permits the gateway to perform this class of
   * operation itself, once S3 has authorised it and the filesystem has
   * denied the authenticated user.  Where it does not, the denial stands, and
   * cross-identity sharing is arranged in the filesystem, with a
   * supplementary group. */
  virtual bool permits(AccessClass what) const = 0;

  /* For the `fs features` map.  A test cannot infer the model from
   * any request -- every model answers alike wherever S3 and POSIX
   * agree, which is nearly always -- so the gateway names it. */
  virtual const char* name() const = 0;
};

/* The identity carries authority and attribution:  both S3 and POSIX
 * must allow, so POSIX permissions remain an accurate account of who
 * can reach what, and a side-loading client has the same standing as
 * an S3 client.
 *
 * The default, and NooBaa's model:  they perform every operation as
 * the requesting account, and their IAM users and roles carry their
 * account's uid, so S3 and POSIX cannot disagree inside an account.
 * NooBaa walks as the request's account, too.  It is the model under which an
 * in-place upgrade of a NooBaa installation keeps its semantics. */
class PosixStrictAccess final : public AccessStrategy {
public:
  bool permits(AccessClass) const override { return false; }
  const char* name() const override { return "posix-strict"; }
};

/* The identity carries attribution only, and S3 has authority:  IAM,
 * bucket policy and ACLs decide, as on every other RGW backend.  Where
 * S3 granted what the identity cannot do, the gateway performs it as
 * S3's agent, with a raise, and what that creates is attributed as the
 * authenticated user's own would be:  an object the user's, a directory
 * its parent's owner's (DECISIONS.md 2026-10-20).
 *
 * The filesystem is storage that records whose things are.  A
 * side-loading client still answers to POSIX, so it sees what S3 did
 * and whose it is, but its permissions do not bound S3.  A deployment
 * that needs them to should run `posix-strict`, the default.
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
