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

#include <linux/capability.h>
#include <sys/resource.h>
#include <sys/sysmacros.h>

#include "rgw_sal_nsfs.h"
#include "rgw_rest_user.h"
#include "driver/posix/sync_policy.h"
#include "rgw_rest_driver_hint.h"
#include "common/iso_8601.h"
#include "common/version.h"
#include "rgw_rest_nsfs_profile.h"
#include "rgw_rest_nsfs_abandoned.h"
#include "rgw_rest_nsfs_bucket.h"
#include "rgw_rest_nsfs_identity.h"
#include "common/code_environment.h"

#include "impersonate.h"
#include "rgw_pubsub_push.h"
#include "rgw_pubsub.h"
#include "rgw_object_ownership.h"
#include "rgw_s3_filter.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstdint>
#include <optional>
#include "rgw_mime.h"
#include "rgw_multi.h"
#include "include/scope_guard.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/async/spawn_throttle.h"
#include "common/errno.h"
#include "rgw_lc.h"

/* I6:  the import the adoption mode runs.  These are the only place
 * the request path reaches the adopt code, and it reaches it once, at
 * startup. */
#include "adopt/noobaa_config_store.h"
#include "adopt/noobaa_master_keys.h"
#include "adopt/noobaa_import_plan.h"
#include "adopt/noobaa_import.h"
#include "adopt/noobaa_schema.h"
#include "adopt/noobaa_bucket_import.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

template <typename T>
static bool decode_raw_attr(rgw::sal::Attrs& attrs, const char* name, T& val) {
  auto it = attrs.find(name);
  if (it == attrs.end()) {
    return false;
  }
  bufferlist bl = it->second;
  try {
    auto it = bl.cbegin();
    decode(val, it);
  } catch (buffer::error&) {
    return false;
  }
  return true;
}

template <typename T>
static void encode_attr(rgw::sal::Attrs& attrs, const char* name, const T& val) {
  bufferlist bl;
  encode(val, bl);
  attrs[name] = std::move(bl);
}

namespace rgw { namespace sal {

using namespace nsfs;

const int64_t READ_SIZE = 128 * 1024;
// required alignment for O_DIRECT reads/writes (rgw_nsfs_direct_io)
const int64_t DIRECT_IO_ALIGN = 4096;

/* The on-disk spelling of an attribute, and which attributes are nsfs's
 * to read at all, belong to XattrStrategy (xattr_strategy.h). */

/* Where a bucket's own state comes from belongs to BucketStateStrategy
 * (bucket_state_strategy.h);  the attribute it is written to is the
 * constant nsfs::BUCKET_INFO_KEY, because writing is uniform. */
#define RGW_NSFS_ATTR_OBJECT_TYPE "object_type"
#define RGW_NSFS_ATTR_MULTIPART_PART_COUNT "multipart_part_count"
#define RGW_NSFS_ATTR_MULTIPART_PART_SIZES "multipart_part_sizes"

#define RGW_NSFS_ATTR_VERSION_ID "version_id"
#define RGW_NSFS_ATTR_DELETE_MARKER "delete_marker"
#define RGW_NSFS_ATTR_NON_CURRENT_TS "non_current_timestamp"

static const std::string HIDDEN_VERSIONS_PATH{nsfs::VERSIONS_DIR};
static const std::string HIDDEN_SHADOW_PATH{nsfs::SHADOW_DIR};
static const std::string NULL_VERSION_ID = "null";

static constexpr int NSFS_VERSION_RETRIES = 5;
static const std::string VERSIONS_LOCKFILE = ".versions/.lock";

/* version locking is now handled by FSStrategy::version_lock(),
 * which returns an RAII VersionLockHandle — OFD on POSIX,
 * LWE cluster-wide right on GPFS. */

const std::string mp_ns = "multipart";
/* Object naming -- the file name for a key, its inverse, the bucket
 * directory and the directory-object sentinel -- belongs to PathStrategy
 * (path_strategy.h). */

/* The multipart staging layout -- where an in-flight upload's parts live
 * and what they are called -- now belongs to MPUStrategy (mpu_strategy.h),
 * because it is the part of nsfs that has to differ from NooBaa's and,
 * separately, has to differ on a filesystem which cannot share extents. */
static const std::string VERSIONS_LOCK_NAME{nsfs::VERSIONS_LOCK};

/* Names this driver creates on disk which are not objects.  Listing
 * paths suppress exactly these;  every other dot-prefixed name is an
 * ordinary object.  Hiding all of them is a client convention -- ls
 * filters, readdir(3) does not -- and a server which drops them from
 * the listing leaves a namespace a client cannot see or empty. */
/* the carried aggregate, or an empty set for an FSEnt built without a
 * parent.  Held by a caller across its loop. */
static const nsfs::ReservedNames& reserved_or_none(
    const nsfs::ReservedNames* rn)
{
  static const nsfs::ReservedNames none{};
  return rn ? *rn : none;
}

/* Matched against the driver's assembled set rather than asked of any
 * strategy:  this runs per directory entry. */
/* Does any component of a key name something the driver reserves?
 *
 * `is_reserved_name()` below tests one directory entry.  A key is a
 * path, so a collision can be at any component:  `.folder` names the
 * sentinel, `foo/.folder` names the sentinel inside `foo`, and
 * `.versions/x` puts an object where version entries live.
 *
 * Used to reject such a write rather than accept it.  The listing skips
 * reserved names, so accepting one produces an object that is on disk
 * and in no listing -- the client is told the write succeeded and can
 * never find it again. */
static bool key_hits_reserved_name(std::string_view key,
				   const nsfs::ReservedNames& rn);

static bool is_reserved_name(std::string_view name,
			     const nsfs::ReservedNames& rn)
{
  for (const auto& e : rn.exact) {
    if (name == e) {
      return true;
    }
  }
  for (const auto& p : rn.prefixes) {
    if (name.starts_with(p)) {
      return true;
    }
  }
  for (const auto& p : rn.staging_prefixes) {
    if (name.starts_with(p)) {
      return true;
    }
  }
  return false;
}

/* Does an on-disk entry count as *content*, for deciding whether a bucket
 * or directory is empty?
 *
 * Driver scaffolding does not:  the driver creates and removes it and a
 * client cannot address it.  Everything else does -- including two kinds
 * of write that are in flight rather than at rest:
 *
 *   - an unpublished shadow (.shadow/<leaf>)
 *   - an incomplete multipart upload (.multipart_*)
 *
 * S3 rejects the delete of a bucket holding an incomplete multipart upload
 * even though the bucket lists as empty, and a shadow is the NFS-side
 * equivalent of one.  Treating either as ignorable does not merely permit
 * the delete -- delete_directory() then recurses into it, destroying a
 * write the client believes is still in progress.  The client can see and
 * abort an upload (ListMultipartUploads) and can close or discard a write,
 * so rejecting the delete costs nothing it cannot resolve.
 *
 * .versions/ and .shadow/ are containers:  scaffolding when they hold
 * nothing of their own, content when they do.  .folder is content -- it
 * *is* an object.  Bucket directories carry no .folder;  their info is an
 * xattr.
 *
 * NB this deliberately diverges from the posix driver, which exempts
 * ".multipart" in POSIXBucket::check_empty() (b46ecc1d4f2, "rgw/posix:
 * Account for incomplete uploads during deletes") and again as `is_mp` in
 * its delete_directory().  nsfs inherited both and now aligns with S3
 * instead.  If the posix line wants the same, the exemption there should
 * become an abort rather than a pass. */
static bool dir_has_content(int parent_fd, const char* dname,
			    std::string_view ignore = {})
{
  int fd = ::openat(parent_fd, dname, O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
  if (fd < 0) {
    return false;
  }
  DIR* d = ::fdopendir(fd);
  if (!d) {
    ::close(fd);
    return false;
  }
  bool found = false;
  struct dirent* de;
  while ((de = ::readdir(d)) != nullptr) {
    std::string_view n = de->d_name;
    if ((n == ".") || (n == "..")) {
      continue;
    }
    if (!ignore.empty() && (n == ignore)) {
      continue;
    }
    found = true;
    break;
  }
  ::closedir(d);
  return found;
}

static bool key_hits_reserved_name(std::string_view key,
				   const nsfs::ReservedNames& rn)
{
  size_t pos = 0;
  while (pos <= key.size()) {
    const size_t slash = key.find('/', pos);
    const std::string_view comp = (slash == std::string_view::npos)
	? key.substr(pos)
	: key.substr(pos, slash - pos);
    /* An empty component is a doubled or trailing slash, not a name */
    if (!comp.empty() && is_reserved_name(comp, rn)) {
      return true;
    }
    if (slash == std::string_view::npos) {
      break;
    }
    pos = slash + 1;
  }
  return false;
}

static bool entry_is_content(int parent_fd, std::string_view name,
			     const nsfs::ReservedNames& rn)
{
  /* a link is not an object, so it does not keep its bucket */
  struct statx stx;
  if ((nsfs::statx(parent_fd, std::string(name).c_str(), AT_SYMLINK_NOFOLLOW,
		   STATX_MODE, &stx) == 0) && S_ISLNK(stx.stx_mode)) {
    return false;
  }
  if (name == HIDDEN_VERSIONS_PATH) {
    /* version entries are user data;  the lock file is nsfs's */
    return dir_has_content(parent_fd, std::string(name).c_str(),
			   VERSIONS_LOCK_NAME);
  }
  if (name == HIDDEN_SHADOW_PATH) {
    return dir_has_content(parent_fd, std::string(name).c_str());
  }
  for (const auto& e : rn.content_exact) {
    if (name == e) {
      return true; /* the sentinel of a directory object */
    }
  }
  for (const auto& p : rn.staging_prefixes) {
    if (name.starts_with(p)) {
      /* An upload in progress does not keep its bucket, as RADOS's
       * check_bucket_empty() counts only the objects' namespace:
       * deleting the bucket aborts it (NSFSBucket::abort_multiparts()). */
      return false;
    }
  }
  /* remaining reserved names are scaffolding */
  return !is_reserved_name(name, rn);
}

/* See posix driver comment — object ownership is now read from the
 * standard RGW_ATTR_ACL attribute, matching rados. The former
 * NSFSOwner xattr could not represent account-owned objects. */

/* The storage class a listing row should report.
 *
 * From the object's own attribute, which is where the write path puts
 * a requested class.  Absent means STANDARD -- RGW stores no
 * attribute for the default -- and that is what the absence means
 * rather than a fallback.
 *
 * A literal STANDARD used to be assigned at twelve sites.  An object
 * written with a non-default class therefore had it on disk, returned
 * it from HEAD (rgw_rest.cc maps the attribute to the
 * X-Amz-Storage-Class header), and listed as STANDARD:  the same shape
 * as the demoted-version, multipart and copy entries, where the row
 * carried less than the object.
 *
 * Not a counted string -- see counted_string_value() -- so the bytes
 * are the value. */
static std::string listed_storage_class(const Attrs& attrs)
{
  auto i = attrs.find(RGW_ATTR_STORAGE_CLASS);
  if ((i == attrs.end()) || (i->second.length() == 0)) {
    return RGW_STORAGE_CLASS_STANDARD;
  }
  return i->second.to_str();
}

static std::string to_base36(uint64_t v)
{
  if (v == 0) return "0";
  const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  std::string result;
  while (v > 0) {
    result.insert(result.begin(), digits[v % 36]);
    v /= 36;
  }
  return result;
}

static bool from_base36(const std::string& s, uint64_t& out)
{
  out = 0;
  for (char c : s) {
    uint64_t d;
    if (c >= '0' && c <= '9') {
      d = c - '0';
    } else if (c >= 'a' && c <= 'z') {
      d = 10 + (c - 'a');
    } else {
      return false;
    }
    out = out * 36 + d;
  }
  return true;
}

static uint64_t statx_mtime_ns(const struct statx& stx)
{
  return (uint64_t)stx.stx_mtime.tv_sec * 1000000000ULL
       + stx.stx_mtime.tv_nsec;
}

static std::string nsfs_version_id_from_statx(const struct statx& stx)
{
  return "mtime-" + to_base36(statx_mtime_ns(stx))
       + "-ino-" + to_base36(stx.stx_ino);
}

struct nsfs_version_info {
  uint64_t mtime_ns{0};
  uint64_t ino{0};
};

static bool nsfs_parse_version_id(const std::string& version_id,
                                  nsfs_version_info& info)
{
  if (version_id == NULL_VERSION_ID) {
    info = {};
    return true;
  }
  static const std::string mtime_pfx = "mtime-";
  static const std::string ino_pfx = "-ino-";
  if (version_id.compare(0, mtime_pfx.size(), mtime_pfx) != 0) {
    return false;
  }
  auto ino_pos = version_id.find(ino_pfx, mtime_pfx.size());
  if (ino_pos == std::string::npos) {
    return false;
  }
  std::string mtime_str = version_id.substr(mtime_pfx.size(),
                                            ino_pos - mtime_pfx.size());
  std::string ino_str = version_id.substr(ino_pos + ino_pfx.size());
  return from_base36(mtime_str, info.mtime_ns) &&
         from_base36(ino_str, info.ino);
}

/* A version's id is the one recorded on it when it was written.
 *
 * Both gateways compute an id once, at write, from the new file's stat,
 * and record it;  NooBaa reports the record (`namespace_fs.js`,
 * _get_version_id_by_xattr), and so does this.  Reading it rather than
 * recomputing it keeps the id a client was given:  NooBaa's loses the
 * low bits of the mtime, so a recomputed one differs, and any id
 * derived from a stat changes when the mtime or the inode does.
 *
 * No record means the null version, in both gateways:  the object was
 * written while versioning was disabled, or placed outside S3
 * (DECISIONS.md 2026-10-13).
 *
 * `alt` is the fallback strategy, so an object the conversion has not
 * reached is read under NooBaa's name. */
static std::string recorded_version_id(int fd, const nsfs::XattrStrategy* xs,
				       const nsfs::XattrStrategy* alt = nullptr)
{
  for (const nsfs::XattrStrategy* s : {xs, alt}) {
    if (!s) {
      continue;
    }
    char buf[256];
    const std::string name = s->disk_name(RGW_NSFS_ATTR_VERSION_ID);
    const ssize_t len = nsfs::fgetxattr(fd, name.c_str(), buf, sizeof(buf));
    if (len > 0) {
      return std::string(buf, len);
    }
  }
  return NULL_VERSION_ID;
}

static bool is_null_version_fd(int fd, const nsfs::XattrStrategy* xs,
			       const nsfs::XattrStrategy* alt = nullptr)
{
  return recorded_version_id(fd, xs, alt) == NULL_VERSION_ID;
}

/* The object's own digest, from its etag xattr, or empty if it carries
 * none.  A listing must prefer this over synthesize_etag():  the token is
 * what an object gets when nothing ever hashed it, and reporting it for an
 * object that *was* hashed makes LIST disagree with HEAD.  The token also
 * contains dashes, which S3 SDKs read as "multipart, do not validate", so
 * the disagreement silently disables the client's integrity check rather
 * than merely looking untidy. */
static std::string etag_from_fd(int fd, const nsfs::XattrStrategy* xs)
{
  char buf[128];
  std::string xattr = xs->disk_name(RGW_ATTR_ETAG);
  ssize_t len = nsfs::fgetxattr(fd, xattr.c_str(), buf, sizeof(buf));
  if (len <= 0) {
    return {};
  }
  return std::string(buf, len);
}

/* The object's recorded owner, from its ACL xattr.
 *
 * XattrStrategy::object_owner() reads an attribute map, so this builds
 * the single entry it looks at.  The ACL is ceph-encoded rather than a
 * counted string, so the bytes on disk go into the map unchanged.
 *
 * Sized from the attribute rather than from a fixed buffer:  a policy
 * with many grants would otherwise be truncated, and a truncated
 * encoding fails to decode, which would look like an object with no
 * owner rather than like an error.
 */
static int owner_from_fd(int fd, const nsfs::XattrStrategy* xs,
			 ACLOwner& owner)
{
  Attrs attrs;
  const std::string xattr = xs->disk_name(RGW_ATTR_ACL);
  if (!xattr.empty()) {
    ssize_t len = nsfs::fgetxattr(fd, xattr.c_str(), nullptr, 0);
    if (len > 0) {
      bufferlist bl;
      bl.push_back(buffer::create(len));
      len = nsfs::fgetxattr(fd, xattr.c_str(), bl.c_str(), len);
      if (len > 0) {
	bl.splice(len, bl.length() - len);
	attrs[RGW_ATTR_ACL] = std::move(bl);
      }
    }
  }
  return xs->object_owner(attrs, owner);
}

/* A listing entry's owner (DECISIONS.md 2026-10-11).
 *
 * `ret` and `recorded` are what XattrStrategy::object_owner() returned.
 * An owner the object records is the entry's;  an object that records
 * none is its bucket's;  a recorded owner that cannot be read is
 * `unknown`, the one case that still is. */
static void set_dir_entry_owner(rgw_bucket_dir_entry& bde, int ret,
				const ACLOwner& recorded,
				const ACLOwner& bucket_owner)
{
  if ((ret < 0) && (ret != -ENODATA)) {
    bde.meta.owner = "unknown";
    bde.meta.owner_display_name = "unknown";
    return;
  }
  const ACLOwner& o = (ret >= 0) ? recorded : bucket_owner;
  bde.meta.owner = to_string(o.id);
  bde.meta.owner_display_name = o.display_name;
}

/* version entry name within .versions/: <leaf>_<version_id> */
static inline std::string nsfs_ver_entry(const std::string& leaf,
                                         const std::string& vid)
{
  return leaf + "_" + vid;
}

static std::string synthesize_etag(const struct statx& stx)
{
  return nsfs_version_id_from_statx(stx);
}

/* rgw_non_md5_etag on NSFS: use the same mtime-ino string as version ids
 * (filesystem inode from this file's stat), not the generic req-id form.
 *
 * Two functions rather than one with a `fd < 0` sentinel.  The value
 * goes to two places and not every caller wants both:  the multipart
 * part writer only needs it in the map, because its own write_attrs()
 * persists that map afterwards, while the atomic writer's map has
 * already been written by the time it gets here, so for it the
 * attribute write below is the only thing that reaches disk.  One
 * function returning "applied" for both could not tell them apart, and
 * a caller that wanted disk and got the map would be the defect this
 * pair exists to prevent.
 *
 * Returns true when the option is on and `attrs` now carries the etag.
 * Nothing reaches disk here. */
static bool nsfs_non_md5_etag_into_attrs(const DoutPrefixProvider* dpp,
                                         const struct statx& stx,
                                         Attrs& attrs)
{
  if (!dpp->get_cct()->_conf->rgw_non_md5_etag) {
    return false;
  }
  bufferlist bl;
  bl.append(nsfs_version_id_from_statx(stx));
  attrs[RGW_ATTR_ETAG] = std::move(bl);
  return true;
}

/* Record that same etag on the object.
 *
 * The attribute is `id`'s.  A `user.*` attribute needs write
 * permission tested against the *calling* thread rather than against
 * whoever opened the descriptor, so the gateway writing through the
 * identity's fd fails on a 0660 file it does not own.  `id` is
 * undefaulted so a new caller cannot become the gateway by leaving it
 * out.
 *
 * A closed descriptor is -EBADF and not a skip:  every caller of this
 * is one whose attribute map will not be written again. */
static int nsfs_record_non_md5_etag(const DoutPrefixProvider* dpp, int fd,
                                    const struct statx& stx,
                                    const nsfs::XattrStrategy* xs,
                                    const nsfs::FSIdentity& id,
                                    bool inject_as_gateway,
                                    optional_yield y)
{
  if (fd < 0) {
    return -EBADF;
  }
  const std::string etag = nsfs_version_id_from_statx(stx);
  const std::string xattr = xs->disk_name(RGW_ATTR_ETAG);
  if (unlikely(inject_as_gateway)) {
    /* inject-etag-as-gateway:  the defect exactly -- the calling
     * thread writes and nobody looks at what came back */
    ::fsetxattr(fd, xattr.c_str(), etag.c_str(), etag.size(), 0);
    return 0;
  }
  return nsfs::with_identity(dpp, id, [&]() -> int {
    if (::fsetxattr(fd, xattr.c_str(), etag.c_str(), etag.size(), 0) < 0) {
      return -errno;
    }
    return 0;
  }, y);
}

/* The uid and gid of an open descriptor.
 *
 * Conversion wants them twice:  for the owner it records on an object
 * that never had one, and for the files it creates. */
static int statx_ids(int fd, struct statx& stx)
{
  if (nsfs::statx(fd, "", AT_EMPTY_PATH, STATX_UID | STATX_GID, &stx) < 0) {
    return -errno;
  }
  if ((stx.stx_mask & (STATX_UID | STATX_GID))
      != (STATX_UID | STATX_GID)) {
    return -EOPNOTSUPP;
  }
  return 0;
}

/* Conversion creates nothing new.  Every file it writes re-expresses
 * something that is already there, so it takes that thing's uid and
 * gid -- the sentinel from the directory it replaces, an upload's
 * files from NooBaa's.
 *
 * Not the gateway's, which would leave a converted directory object
 * unreadable to the account that owns the data.  And not an
 * account's:  a sideloaded file may belong to a uid no account maps
 * to, and there would be nobody to be.
 *
 * A no-op when the source is already the gateway's, which is every file on a
 * tree this gateway wrote, and which needs no capability.  Otherwise
 * it needs CAP_CHOWN -- a fourth beyond the three the daemon is given
 * -- and without it conversion fails rather than leaving behind a
 * file whose owner is wrong.
 *
 * **Call this after the mode is set, never before.**  A file's mode is
 * fixed by the open that creates it, while the gateway still owns it;
 * chowning first would leave us modifying a file owned by somebody
 * else, which needs CAP_FOWNER as well.  One capability rather than
 * two, and only because of the order. */
static int inherit_ids(const DoutPrefixProvider* dpp, int fd,
		       const struct statx& src, bool no_chown,
		       std::string_view what)
{
  if ((src.stx_uid == ::geteuid()) && (src.stx_gid == ::getegid())) {
    return 0;
  }
  if (no_chown) {
    ldpp_dout(dpp, 0) << "ERROR: not creating " << what
      << " for uid " << src.stx_uid << ":" << src.stx_gid
      << ":  chown is unavailable (injected)" << dendl;
    return -EPERM;
  }
  if (::fchown(fd, src.stx_uid, src.stx_gid) < 0) {
    int ret = -errno;
    ldpp_dout(dpp, 0) << "ERROR: cannot give " << what << " the uid "
      << src.stx_uid << ":" << src.stx_gid << " of what it re-expresses: "
      << cpp_strerror(-ret)
      << (ret == -EPERM
	  ? ";  the daemon needs CAP_CHOWN to convert a tree it does not own"
	  : "")
      << dendl;
    return ret;
  }
  return 0;
}


/* LMDB comparator for versioned bucket listing.
 * Key format: name \0 instance.
 * Sort: name ascending, then instance descending (newest version first).
 * Instance is "mtime-{base36}-ino-{base36}" — we extract and compare
 * the mtime numerically for correct ordering regardless of base36 width. */
static int nsfs_lmdb_cmp(const MDB_val *a, const MDB_val *b)
{
  std::string_view sa(static_cast<const char*>(a->mv_data), a->mv_size);
  std::string_view sb(static_cast<const char*>(b->mv_data), b->mv_size);

  auto sep_a = sa.find('\0');
  auto sep_b = sb.find('\0');
  std::string_view name_a = sa.substr(0, sep_a);
  std::string_view name_b = sb.substr(0, sep_b);

  int cmp = name_a.compare(name_b);
  if (cmp != 0) {
    return cmp;
  }

  /* same name — compare instances in reverse (newest first) */
  std::string_view inst_a = (sep_a != std::string_view::npos)
    ? sa.substr(sep_a + 1) : std::string_view{};
  std::string_view inst_b = (sep_b != std::string_view::npos)
    ? sb.substr(sep_b + 1) : std::string_view{};

  /* empty instance (non-versioned) sorts before any version */
  if (inst_a.empty() && inst_b.empty()) { return 0; }
  if (inst_a.empty()) { return -1; }
  if (inst_b.empty()) { return 1; }

  /* extract mtime from "mtime-{base36}-ino-{base36}" */
  auto extract_mtime = [](std::string_view inst) -> uint64_t {
    static constexpr std::string_view pfx = "mtime-";
    static constexpr std::string_view sep = "-ino-";
    if (inst.substr(0, pfx.size()) != pfx) {
      return 0;
    }
    auto ino_pos = inst.find(sep, pfx.size());
    if (ino_pos == std::string_view::npos) {
      return 0;
    }
    uint64_t val = 0;
    for (size_t i = pfx.size(); i < ino_pos; ++i) {
      char c = inst[i];
      uint64_t d;
      if (c >= '0' && c <= '9') { d = c - '0'; }
      else if (c >= 'a' && c <= 'z') { d = 10 + (c - 'a'); }
      else { return 0; }
      val = val * 36 + d;
    }
    return val;
  };

  uint64_t mt_a = extract_mtime(inst_a);
  uint64_t mt_b = extract_mtime(inst_b);

  /* descending: larger mtime sorts first */
  if (mt_a > mt_b) { return -1; }
  if (mt_a < mt_b) { return 1; }

  /* same mtime — compare full instance for determinism */
  return inst_b.compare(inst_a);
}

/*
 * Promote the newest non-delete-marker version from .versions/ to the
 * current path.  Uses CAS (safe_link) to avoid races with concurrent
 * PUT or DELETE: if a new current appears between the scan and the link
 * (a concurrent writer won), the link fails with EEXIST and we skip.
 * Retries on CAS mismatch (the candidate was moved by another thread).
 */
/* What the promote's last rung raises, and nothing else raises.
 *
 * `CAP_DAC_OVERRIDE` for the bucket directory the link lands in and
 * for the promoted file's attribute;  `CAP_FOWNER` for
 * protected_hardlinks' check on the source inode
 * (`inode_owner_or_capable()` in `may_linkat()`) and for a sticky
 * `.versions` entry.  Both are in the gateway's permitted set
 * already, so this names a site and not a new capability. */
static constexpr uint64_t PROMOTE_RESIDUE_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE) | nsfs::cap_bit(CAP_FOWNER);

/* The identity an object records for itself, resolved to credentials.
 *
 * The third rung of the promote's ladder (n plan §5).  A uid can be
 * carried by several identity rows -- every user of an account runs
 * as the account -- and where their group lists differ the uid cannot
 * say which applies.  A recorded owner can:  it is a name, and a name
 * is the identity table's primary key, so this is the ordinary keyed
 * resolve with no rule of its own.
 *
 * The read runs as the file's own owner, because the gateway cannot
 * open an identity's `0660` file -- which is the same reason the
 * promote needs this at all.  `inout` is written only on success.
 *
 * An object that records no owner returns false rather than an error.
 * NooBaa records none (`object_owner()` returns -ENODATA and the
 * caller supplies the bucket's), and neither does a file written over
 * NFS or SMB, so this rung is empty in an adopted tree and in a
 * sideloaded one.  Conversion synthesises an ACL from the bucket owner
 * for every object and version that lacked one, so a converted tree
 * has it.
 */
static bool credentials_from_recorded_owner(const DoutPrefixProvider* dpp,
					    int dir_fd,
					    const std::string& name,
					    const nsfs::XattrStrategy* xs,
					    const nsfs::IdentityDB& db,
					    nsfs::Credentials& inout,
					    optional_yield y)
{
  ACLOwner recorded;
  const nsfs::Credentials reader = inout;	/* the file's own owner */
  auto read_owner = [&]() -> int {
    int fd = ::openat(dir_fd, name.c_str(), O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
      return -errno;
    }
    auto close_fd = make_scope_guard([fd] { ::close(fd); });
    return owner_from_fd(fd, xs, recorded);
  };

  const int ret = nsfs::impersonation_enabled()
    ? nsfs::run_as(dpp, reader, read_owner, y) : read_owner();
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "credentials_from_recorded_owner: " << name
      << " records no usable owner: " << cpp_strerror(-ret) << dendl;
    return false;
  }

  nsfs::Credentials named;
  if (nsfs::resolve_credentials(dpp, db, recorded.id, named, y) < 0) {
    return false;
  }
  /* the groups of the identity the object names, on the uid the file
   * carries:  the uid is what protected_hardlinks asks about, and it
   * is the file's */
  inout.groups = std::move(named.groups);
  ldpp_dout(dpp, 10) << "credentials_from_recorded_owner: " << name
    << " names " << recorded.id << ", " << inout.groups.size()
    << " groups" << dendl;
  return true;
}

/* What the gateway raises to perform, in another identity's key
 * directory or `.versions`, what the authenticated user was denied
 * (with_identity_or_raise()):  DAC_OVERRIDE for the directory's mode,
 * FOWNER for a sticky `.versions` and for an attribute on another
 * identity's file. */
static constexpr uint64_t RAISED_KEYDIR_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE) | nsfs::cap_bit(CAP_FOWNER);

/* What a raised create raises, and then what gives the new entry its
 * owner:  the authenticated user for an object, the parent's owner for a
 * directory (DECISIONS.md 2026-10-20). */
static constexpr uint64_t RAISED_CREATE_CAPS = nsfs::cap_bit(CAP_DAC_OVERRIDE);
static constexpr uint64_t RAISED_CHOWN_CAPS = nsfs::cap_bit(CAP_CHOWN);

/* Give what a raised create made to `uid` and `gid`:  `name` in `dirfd`,
 * or `dirfd` itself where `name` is empty. */
static int chown_raised(const DoutPrefixProvider* dpp, int dirfd,
			const char* name, uid_t uid, gid_t gid)
{
  const int flags = AT_SYMLINK_NOFOLLOW | (*name ? 0 : AT_EMPTY_PATH);
  const int ret = nsfs::with_capabilities(dpp, RAISED_CHOWN_CAPS,
					  [&]() -> int {
    return (::fchownat(dirfd, name, uid, gid, flags) < 0)
	? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not give " << name << " to uid "
      << uid << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

/* A raised directory create:  the directory is its parent's owner's. */
static int chown_raised_dir(const DoutPrefixProvider* dpp, int parent_fd,
			    const char* name)
{
  struct statx pstx;
  const int ret = statx_ids(parent_fd, pstx);
  if (ret < 0) {
    return ret;
  }
  return chown_raised(dpp, parent_fd, name, pstx.stx_uid, pstx.stx_gid);
}

/* One call as `id`, or as the gateway where `id` is inactive -- the
 * calls inside a key's directory and its `.versions`, which are
 * their creator's (call-site table §11).  Denied, the gateway performs it
 * where `model` permits (with_identity_or_raise());  a null `model`
 * lets the denial stand.  Each returns 0 or a descriptor, or -errno;
 * `dpp` may be null only when `id` is inactive. */
static int openat_as(const DoutPrefixProvider* dpp, const nsfs::FSIdentity& id,
		     const nsfs::AccessStrategy* model,
		     int dirfd, const char* name, int flags)
{
  return nsfs::open_as_or_raise(dpp, id, model,
				nsfs::AccessClass::data,
				RAISED_KEYDIR_CAPS,
				dirfd, name, flags, 0);
}

static int unlinkat_as(const DoutPrefixProvider* dpp,
		       const nsfs::FSIdentity& id,
		       const nsfs::AccessStrategy* model,
		       int dirfd, const char* name)
{
  return nsfs::with_identity_or_raise(dpp, id, model,
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS,
				      [&]() -> int {
    return (::unlinkat(dirfd, name, 0) < 0) ? -errno : 0;
  }, null_yield);
}

/* Whether `name` is in `dirfd`, looked for as `id`:  1 or 0, or -errno.
 * A denial is not absence -- read as absence, a create would publish
 * over the object it could not see. */
static int exists_as(const DoutPrefixProvider* dpp, const nsfs::FSIdentity& id,
		     const nsfs::AccessStrategy* model,
		     int dirfd, const char* name)
{
  return nsfs::with_identity_or_raise(dpp, id, model,
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS,
				      [&]() -> int {
    if (::faccessat(dirfd, name, F_OK, AT_EACCESS) == 0) {
      return 1;
    }
    return (errno == ENOENT) ? 0 : -errno;
  }, null_yield);
}

/* A symbolic link is not an object (DECISIONS.md 2026-10-23), so a write or a
 * delete that lands on a name a link holds unlinks the link, never its
 * target.  Returns 0 whether or not there was a link, or -errno. */
static int unlink_if_link(const DoutPrefixProvider* dpp,
			  const nsfs::FSIdentity& id,
			  const nsfs::AccessStrategy* model,
			  int dirfd, const char* name)
{
  struct statx stx;
  if ((nsfs::statx(dirfd, name, AT_SYMLINK_NOFOLLOW, STATX_MODE, &stx) != 0) ||
      !S_ISLNK(stx.stx_mode)) {
    return 0;
  }
  const int ret = unlinkat_as(dpp, id, model, dirfd, name);
  if ((ret < 0) && (ret != -ENOENT)) {
    return ret;
  }
  ldpp_dout(dpp, 10) << "nsfs: " << name
    << " is a symbolic link, which is not an object;  unlinked" << dendl;
  return 0;
}

/* The rule:  a name in a bucket's tree holds either an object (a file)
 * or a directory, never both, so the keys `p` and `p/`, or `p` and any
 * key under `p/`, cannot exist together.  The one written first holds
 * the name until it is deleted, and a write of the other returns 409
 * KeyPathConflict (DECISIONS.md 2026-10-24).
 *
 * This function enforces the half where `p` is a directory and the
 * write is to `p`;  resolve_key_path() enforces the half where `p` is a
 * file and the write is under it.  409 rather than 500, because the
 * write cannot succeed until the client deletes what holds the name,
 * and a 500 tells the client to retry. */
static int refuse_directory_name(const DoutPrefixProvider* dpp,
				 int dirfd, const char* name)
{
  struct statx stx;
  if ((nsfs::statx(dirfd, name, AT_SYMLINK_NOFOLLOW, STATX_MODE, &stx) != 0) ||
      !S_ISDIR(stx.stx_mode)) {
    return 0;
  }
  ldpp_dout(dpp, 4) << "nsfs: rejecting key " << name
    << ":  a directory holds its name" << dendl;
  return -ERR_KEY_PATH_CONFLICT;
}

static int renameat_as(const DoutPrefixProvider* dpp,
		       const nsfs::FSIdentity& id,
		       const nsfs::AccessStrategy* model, int olddirfd,
		       const char* oldname, int newdirfd, const char* newname)
{
  return nsfs::with_identity_or_raise(dpp, id, model,
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS,
				      [&]() -> int {
    return (::renameat(olddirfd, oldname, newdirfd, newname) < 0)
	? -errno : 0;
  }, null_yield);
}

/* What promote_version() promoted, for a caller that has to describe the
 * new current version.  Both fields are free here:  the version id is the
 * suffix of the .versions/ entry name, and the etag is read from the
 * descriptor the success path already opens to strip the non-current
 * timestamp.  Reporting them saves the caller an openat() and an
 * fgetxattr() it would otherwise repeat. */
struct PromoteResult {
  bool promoted{false};
  std::string version_id;
  std::string etag;
};

/* 0 when the current name ends up in the state it should be in --
 * promoted here, held by a writer who got there first, or nothing to
 * promote -- and -errno when a promote was needed and did not happen.
 * A promote that fails leaves the version entry in .versions/, so the
 * bucket is never empty and DeleteBucket answers 409 forever;  that
 * went unreported while this returned void. */
/* `scanner` opens and lists `.versions`, which is the key directory's
 * and so its creator's;  the publish runs as the promoted version's
 * own owner, below.  Called outside any with_identity():  run_as()
 * refuses one identity on a thread acting as another (H1). */
static int promote_version(int parent_fd, const std::string& leaf,
                           const DoutPrefixProvider* dpp,
                           FSStrategy* fs_strategy,
                           const nsfs::XattrStrategy* xs,
                           const nsfs::IdentityDB* idb,
                           const nsfs::FSIdentity& scanner,
                           const nsfs::AccessStrategy* model,
                           optional_yield y,
                           PromoteResult* out = nullptr)
{
  ldpp_dout(dpp, 10) << "promote_version: enter leaf=" << leaf
    << " parent_fd=" << parent_fd << dendl;
  for (int attempt = 0; attempt < NSFS_VERSION_RETRIES; ++attempt) {
    /* bail if a current version already exists */
    struct statx cur_stx;
    if (nsfs::statx(parent_fd, leaf.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_INO, &cur_stx) == 0) {
      ldpp_dout(dpp, 10) << "promote_version: " << leaf
        << " current already exists (ino=" << cur_stx.stx_ino
        << "), bail" << dendl;
      return 0;
    }

    int vfd = openat_as(dpp, scanner, model, parent_fd,
                        HIDDEN_VERSIONS_PATH.c_str(), O_RDONLY | O_DIRECTORY);
    if (vfd < 0) {
      int err = -vfd;
      ldpp_dout(dpp, err == ENOENT ? 10 : 0) << "promote_version: " << leaf
        << " .versions/ not opened (errno=" << err << ")" << dendl;
      /* no .versions/ is nothing to promote;  anything else is */
      return err == ENOENT ? 0 : -err;
    }

    /* find newest version entry for this key (including delete markers) */
    uint64_t max_mtime = 0;
    std::string max_name;
    DIR* vdir = fdopendir(dup(vfd));
    if (vdir) {
      struct dirent* de;
      while ((de = readdir(vdir)) != nullptr) {
        /* no dot filter:  the key match below rejects ".", ".." and
         * .lock, and filtering on the dot would hide the versions of
         * an object whose own name begins with one */
        std::string vn(de->d_name);
        if (vn.size() <= leaf.size() + 1 ||
            vn.compare(0, leaf.size(), leaf) != 0 ||
            vn[leaf.size()] != '_') {
          continue;
        }
        std::string vid = vn.substr(leaf.size() + 1);
        nsfs_version_info vi;
        if (!nsfs_parse_version_id(vid, vi)) {
          continue;
        }
        uint64_t entry_mtime;
        if (vid == NULL_VERSION_ID) {
          struct statx nstx;
          if (nsfs::statx(vfd, de->d_name, AT_SYMLINK_NOFOLLOW,
                    STATX_MTIME, &nstx) < 0) {
            continue;
          }
          entry_mtime = statx_mtime_ns(nstx);
        } else {
          entry_mtime = vi.mtime_ns;
        }
        if (entry_mtime > max_mtime || max_name.empty()) {
          max_mtime = entry_mtime;
          max_name = vn;
        }
      }
      closedir(vdir);
    }

    if (max_name.empty()) {
      ldpp_dout(dpp, 10) << "promote_version: " << leaf
        << " no versions to promote" << dendl;
      ::close(vfd);
      return 0;
    }
    ldpp_dout(dpp, 10) << "promote_version: " << leaf
      << " candidate=" << max_name << dendl;

    struct statx cand_stx;
    if (nsfs::statx(vfd, max_name.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_ALL, &cand_stx) < 0) {
      int err = errno;
      ::close(vfd);
      if (err == ENOENT) {
        /* the entry went while the scan was running, so another
         * thread is working this key.  Re-scan rather than report:
         * either its promote took the current name, which the next
         * attempt's first check sees, or another entry is the newest
         * now. */
        struct timespec ts = {0, 100000 + (rand() % 500000)};
        nanosleep(&ts, nullptr);
        continue;
      }
      ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
        << " cannot stat candidate " << max_name << ": "
        << cpp_strerror(err) << dendl;
      return -err;
    }

    /* As the owner of the version being promoted.
     *
     * Each of the three steps below fails for the gateway against
     * a version an identity wrote:  fs.protected_hardlinks permits a
     * link only to the target's owner, a sticky .versions/ entry is
     * the file owner's to unlink, and the timestamp comes off the
     * owner's file.  Nobody configures a promote, so neither the
     * authenticated user nor the bucket owner has a claim to perform it, and
     * either fails the same way unless it happens to be the
     * writer.  The file says who wrote it, and a hard link preserves
     * the inode's owner, so publishing it under the current name
     * grants nobody access they did not already have
     * (`RGW_NSFS_VADE_MECUM.md` §12, "The raise sites").
     *
     * The groups come from the identity row that carries this uid,
     * because the bucket directory is reachable by group for everyone
     * but its owner and `stat()` cannot see a group list (§5.3).  No
     * row, or rows that disagree, leaves the file owner's own rights,
     * which is what this did before and is reported where it fails
     * rather than passing silently.
     *
     * run_as() decides whether a switch is needed, because it is the
     * only thing that knows who this thread already is;  a geteuid()
     * test here would match by accident on a thread already acting as
     * somebody, as it did in the part eviction. */
    nsfs::Credentials cand_owner;
    cand_owner.uid = cand_stx.stx_uid;
    cand_owner.gid = cand_stx.stx_gid;
    /* whether an identity record names this file's owner at all.  It
     * decides the last rung:  a denial of an identity we could name
     * is the tree's decision about that identity and is reported, while
     * a denial with no identity to name is what the raise is for. */
    bool named = false;
    if (idb) {
      const int cret = nsfs::complete_file_credentials(dpp, *idb, cand_owner);
      named = (cret == 0);
      if (cret == -ENODATA) {
	/* rung 3:  several rows carry the uid and disagree, so ask the
	 * object who it belongs to */
	named = credentials_from_recorded_owner(dpp, vfd, max_name, xs,
						*idb, cand_owner, y);
      }
    }

    SafeResult sr = SafeResult::ERROR;
    auto publish = [&]() -> int {
      /* CAS link: .versions/candidate → current path */
      sr = fs_strategy->safe_link(dpp, vfd, max_name,
                                  parent_fd, leaf,
                                  statx_mtime_ns(cand_stx),
                                  cand_stx.stx_ino, null_yield);
      ldpp_dout(dpp, 10) << "promote_version: safe_link " << max_name
        << " → " << leaf << " result=" << (int)sr << dendl;
      if (sr != SafeResult::OK) {
        return 0;
      }
      /* unlink the .versions/ entry.  The inode is under both names
       * until this returns, so a failure here lists the object
       * twice, once current and once not. */
      if (::unlinkat(vfd, max_name.c_str(), 0) < 0) {
        ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
          << " promoted, but " << max_name << " stays in "
          << HIDDEN_VERSIONS_PATH << ": " << cpp_strerror(errno)
          << ";  the object lists twice" << dendl;
      }
      /* remove non_current_timestamp on the promoted file */
      int pfd = ::openat(parent_fd, leaf.c_str(), O_RDONLY | O_NOFOLLOW);
      if (pfd < 0) {
        ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
          << " promoted, but it cannot be opened: " << cpp_strerror(errno)
          << ";  it keeps its non-current timestamp" << dendl;
        return 0;
      }
      std::string ts_x = xs->disk_name(RGW_NSFS_ATTR_NON_CURRENT_TS);
      if (::fremovexattr(pfd, ts_x.c_str()) < 0 && errno != ENODATA) {
        ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
          << " promoted, but its non-current timestamp stays: "
          << cpp_strerror(errno) << dendl;
      }
      if (out) {
        out->etag = etag_from_fd(pfd, xs);
      }
      ::close(pfd);
      return 0;
    };

    const int aret = nsfs::impersonation_enabled()
      ? nsfs::run_as(dpp, cand_owner, publish, y) : publish();
    if (aret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
        << " cannot run as uid " << cand_owner.uid << ", the owner of "
        << max_name << ": " << cpp_strerror(-aret) << dendl;
      ::close(vfd);
      return aret;
    }

    /* The ladder's last rung.  No identity record names this file's
     * owner -- a version written over NFS or SMB, where there is no
     * row to complete from and no recorded owner to read -- so there
     * is no identity whose rights the denial describes.  Raise, as
     * the gateway and not as anybody:  CAP_DAC_OVERRIDE for the
     * bucket directory and the promoted file's attribute,
     * CAP_FOWNER for protected_hardlinks' check on the source inode
     * and for the sticky `.versions` entry.
     *
     * After the denial and never before it, so a tree whose bucket
     * directories need no group never pays for this.  `SafeResult`
     * cannot yet say *denied* rather than *failed* -- that is phase
     * D's third piece -- so an I/O error reaches here too and is
     * retried once pointlessly, which costs a syscall and reports
     * the same thing twice. */
    if (((sr == SafeResult::ERROR) || (sr == SafeResult::DENIED)) &&
        !named) {
      ldpp_dout(dpp, 4) << "promote_version: " << leaf << " failed as uid "
        << cand_owner.uid << ", which no identity record names;  retrying "
        "as the gateway with DAC_OVERRIDE and FOWNER raised" << dendl;
      const int rret = nsfs::with_capabilities(dpp, PROMOTE_RESIDUE_CAPS,
                                               publish);
      if (rret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
          << " could not be published with capabilities raised: "
          << cpp_strerror(-rret) << dendl;
      }
    }

    if (sr == SafeResult::OK) {
      if (out) {
        out->promoted = true;
        out->version_id = max_name.substr(leaf.size() + 1);
      }
      ::close(vfd);
      return 0;
    }

    ::close(vfd);
    if ((sr == SafeResult::ERROR) || (sr == SafeResult::DENIED)) {
      /* The link failed, and a retry fails the same way --
       * the strategy logged the errno.  What an operator needs is the
       * consequence, which no other line states. */
      ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
        << " not promoted from " << max_name << " as uid "
        << cand_owner.uid << ";  the entry stays in "
        << HIDDEN_VERSIONS_PATH
        << ", so the bucket is not empty and will not delete" << dendl;
      return -EIO;
    }
    /* MISMATCH — candidate changed, retry */
    struct timespec ts = {0, 100000 + (rand() % 500000)};
    nanosleep(&ts, nullptr);
  }

  ldpp_dout(dpp, 0) << "ERROR: promote_version: " << leaf
    << " not promoted after " << NSFS_VERSION_RETRIES
    << " attempts;  the newest version stays in " << HIDDEN_VERSIONS_PATH
    << ", so the bucket is not empty and will not delete" << dendl;
  return -EAGAIN;
}

/* `.versions` beside `parent_fd`, made if it is missing.
 *
 * As `id`, at NooBaa's mode, where `id` is active:  it is the key
 * directory's, and so its creator's (call-site table §11.1).  The
 * gateway's, at the same mode, otherwise.  `dpp` may be null only
 * when `id` is inactive. */
static int ensure_versions_dir(const DoutPrefixProvider* dpp, int parent_fd,
			       const nsfs::FSIdentity& id,
			       const nsfs::AccessStrategy* model)
{
  if (id.active()) {
    const mode_t mode = nsfs::dir_create_mode();
    bool raised = false;
    bool made = false;
    const int ret = nsfs::with_identity_or_raise(dpp, id, model,
						 nsfs::AccessClass::data,
						 RAISED_CREATE_CAPS,
						 [&]() -> int {
      if (::mkdirat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(), mode) < 0) {
	return (errno == EEXIST) ? 0 : -errno;
      }
      made = true;
      return nsfs::apply_create_mode_at(dpp, parent_fd,
					HIDDEN_VERSIONS_PATH.c_str(), mode);
    }, null_yield, &raised);
    if ((ret < 0) || !(raised && made)) {
      return ret;
    }
    return chown_raised_dir(dpp, parent_fd, HIDDEN_VERSIONS_PATH.c_str());
  }
  const mode_t mode = nsfs::dir_create_mode();
  int ret = ::mkdirat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(), mode);
  if (ret < 0) {
    return (errno == EEXIST) ? 0 : -errno;
  }
  return nsfs::apply_create_mode_at(nullptr, parent_fd,
				    HIDDEN_VERSIONS_PATH.c_str(), mode);
}

/* `.versions` beside `parent_fd`, opened as `id` where it is active.
 * A descriptor, or -errno;  `dpp` as ensure_versions_dir(). */
static int open_versions_dir(const DoutPrefixProvider* dpp, int parent_fd,
			     const nsfs::FSIdentity& id,
			     const nsfs::AccessStrategy* model)
{
  int ret = ensure_versions_dir(dpp, parent_fd, id, model);
  if (ret < 0) {
    return ret;
  }
  if (id.active()) {
    return openat_as(dpp, id, model, parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
		     O_RDONLY | O_DIRECTORY);
  }
  int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
                     O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
  if (vfd < 0) {
    return -errno;
  }
  return vfd;
}

/* ---- rename support -------------------------------------------------- */

/* The .versions/ entries belonging to one leaf.
 *
 * The suffix parse matches the listing path (see FSEnt::fill_cache):  an
 * entry is "<leaf>_null" or "<leaf>_mtime-...".  Requiring the separator to
 * fall exactly at leaf.size() is what stops "photo.jpg" collecting the
 * versions of "photo.jpg.bak". */
struct LeafVersion {
  std::string entry;   /* <leaf>_<version-id> */
  std::string ver_id;
};

static int list_leaf_versions(int parent_fd, const std::string& leaf,
			      std::vector<LeafVersion>& out)
{
  /* the gateway's listing, in a `.versions` that may be an
   * identity's:  read-search at the call */
  if (nsfs::impersonation_enabled()) {
    (void) nsfs::hold_thread_capabilities(nullptr);
  }
  int vfd = ::openat(parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
		     O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
  if (vfd < 0) {
    /* no .versions/ at all just means no history */
    return (errno == ENOENT) ? 0 : -errno;
  }
  DIR* vdir = ::fdopendir(vfd);
  if (! vdir) {
    int err = errno;
    ::close(vfd);
    return -err;
  }
  struct dirent* de;
  while ((de = ::readdir(vdir)) != nullptr) {
    std::string vname(de->d_name);
    if (vname.size() <= leaf.size() || vname.compare(0, leaf.size(), leaf) != 0
	|| vname[leaf.size()] != '_') {
      continue;
    }
    std::string vid = vname.substr(leaf.size() + 1);
    if (vid != NULL_VERSION_ID && vid.compare(0, 6, "mtime-") != 0) {
      continue;
    }
    out.push_back(LeafVersion{vname, vid});
  }
  ::closedir(vdir); /* closes vfd */
  return 0;
}

/* Intent record.
 *
 * One xattr on the source .versions/ directory, written before the first
 * move and cleared after the leaf lands.  It carries *what the move was*,
 * never how far it got:  the leaf's location is the commit record, so
 * recovery derives direction from the filesystem rather than from a progress
 * counter that could go stale.  Two xattr operations for the whole move,
 * whatever the version count.  See RENAME_DESIGN.md.
 *
 * The slice flag is part of "what the move was" and has to be recorded:
 * with it, history left at the source is rubbish to be removed;  without it,
 * history left at the source has to follow the leaf.  The same on-disk state
 * means opposite things depending on it. */
struct RenameIntent {
  std::string leaf;
  std::string tenant;
  std::string bucket;
  std::string key;
  bool slice{false};
};

static std::string rename_intent_xattr(const nsfs::XattrStrategy* xs)
{
  return xs->disk_name("rename_intent");
}

static int write_rename_intent(const DoutPrefixProvider* dpp,
			       const nsfs::XattrStrategy* xs,
			       int src_parent_fd, const nsfs::FSIdentity& id,
			       const nsfs::AccessStrategy* model,
			       const std::string& leaf,
			       const std::string& dst_tenant,
			       const std::string& dst_bucket,
			       const std::string& dst_key,
			       bool slice)
{
  int vfd = open_versions_dir(dpp, src_parent_fd, id, model);
  if (vfd < 0) {
    return vfd;
  }
  /* leaf \0 tenant \0 bucket \0 key \0 flags */
  std::string rec = leaf;
  rec.push_back('\0');
  rec += dst_tenant;
  rec.push_back('\0');
  rec += dst_bucket;
  rec.push_back('\0');
  rec += dst_key;
  rec.push_back('\0');
  if (slice) {
    rec.push_back('s');
  }
  std::string name = rename_intent_xattr(xs);
  /* as `id`:  the `.versions` the record lives on is its creator's */
  const int err = nsfs::with_identity_or_raise(dpp, id, model,
						 nsfs::AccessClass::data,
						 RAISED_KEYDIR_CAPS,
						 [&]() -> int {
    return (::fsetxattr(vfd, name.c_str(), rec.data(), rec.size(), 0) < 0)
	? -errno : 0;
  }, null_yield);
  ::close(vfd);
  return err;
}

static bool parse_rename_intent(const std::string& rec, RenameIntent& out)
{
  std::vector<std::string> f;
  size_t pos = 0;
  for (;;) {
    size_t nul = rec.find('\0', pos);
    if (nul == std::string::npos) {
      f.push_back(rec.substr(pos));
      break;
    }
    f.push_back(rec.substr(pos, nul - pos));
    pos = nul + 1;
  }
  if (f.size() < 4) {
    return false;
  }
  out.leaf = f[0];
  out.tenant = f[1];
  out.bucket = f[2];
  out.key = f[3];
  out.slice = (f.size() > 4) && (f[4].find('s') != std::string::npos);
  return ! out.leaf.empty() && ! out.bucket.empty() && ! out.key.empty();
}

static void clear_rename_intent(const DoutPrefixProvider* dpp,
				const nsfs::XattrStrategy* xs,
				int src_parent_fd, const nsfs::FSIdentity& id,
				const nsfs::AccessStrategy* model)
{
  /* the gateway's open, in a `.versions` that may be an identity's:
   * read-search at the call */
  if (! id.active() && nsfs::impersonation_enabled()) {
    (void) nsfs::hold_thread_capabilities(dpp);
  }
  int vfd = openat_as(dpp, id, model, src_parent_fd,
		      HIDDEN_VERSIONS_PATH.c_str(), O_RDONLY | O_DIRECTORY);
  if (vfd < 0) {
    return;
  }
  std::string name = rename_intent_xattr(xs);
  const int cret = nsfs::with_identity_or_raise(dpp, id, model,
						nsfs::AccessClass::data,
						RAISED_KEYDIR_CAPS,
						[&]() -> int {
    return (::fremovexattr(vfd, name.c_str()) < 0) ? -errno : 0;
  }, null_yield);
  ::close(vfd);
  if ((cret < 0) && (cret != -ENODATA)) {
    ldpp_dout(dpp, 0) << "ERROR: could not clear a rename record:  "
      << cpp_strerror(-cret) << ";  the next write to the key goes "
      "through recovery" << dendl;
  }
}

/* takes the .versions/ descriptor, for callers which already have one open */
static bool rename_intent_present_at(const nsfs::XattrStrategy* xs,
				     int vfd, std::string& rec)
{
  char buf[1024];
  std::string name = rename_intent_xattr(xs);
  ssize_t len = nsfs::fgetxattr(vfd, name.c_str(), buf, sizeof(buf));
  if (len <= 0) {
    return false;
  }
  rec.assign(buf, len);
  return true;
}

static bool rename_intent_present(const nsfs::XattrStrategy* xs,
				  int src_parent_fd, std::string& rec)
{
  /* the gateway's listing, in a `.versions` that may be an
   * identity's:  read-search at the call */
  if (nsfs::impersonation_enabled()) {
    (void) nsfs::hold_thread_capabilities(nullptr);
  }
  int vfd = ::openat(src_parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
		     O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
  if (vfd < 0) {
    return false;
  }
  bool present = rename_intent_present_at(xs, vfd, rec);
  ::close(vfd);
  return present;
}

/* Raise site 5 (n plan §5):  rename recovery moves version entries
 * another identity wrote, between `.versions` directories that are
 * their writers' at 0770, under whichever request takes the version
 * lock next -- so it is the gateway's and not that request's.
 * `CAP_DAC_OVERRIDE`, taken only after the gateway's own call is
 * denied, so a gateway on its own directories never raises. */
static constexpr uint64_t RENAME_RECOVERY_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE);

static int recovery_write(const DoutPrefixProvider* dpp,
			  const std::function<int()>& fn)
{
  const int ret = fn();
  if ((ret != -EACCES) && (ret != -EPERM)) {
    return ret;
  }
  return nsfs::with_capabilities(dpp, RENAME_RECOVERY_CAPS, fn);
}

/* Finish or undo an interrupted move.
 *
 * The record says what the move was;  the filesystem says how far it got.
 * The leaf rename is the commit point, so the leaf's location decides the
 * direction:  still at the source and the move never committed, so anything
 * already at the destination comes back;  at the destination and it did
 * commit, so whatever history is still behind follows it.  Nothing is
 * inferred from a progress count, which is why the record does not keep one
 * and why it cannot go stale.
 *
 * Present at both keys, or at neither, is not a state this sequence can
 * produce -- renameat() is atomic and the version lock excludes the other
 * mutations -- so it means something outside this code has been at the
 * directory, most likely a client recreating the source key after the crash.
 * Recovery stops and leaves the record rather than guess;  destroying real
 * data is worse than staying stuck.
 *
 * The caller must hold the source version lock.
 */
static int recover_rename_intent(const nsfs::XattrStrategy* xs,
				 const DoutPrefixProvider* dpp,
				 NSFSDriver* driver,
				 const std::string& src_bucket_name,
				 int src_parent_fd, const RenameIntent& in)
{
  const bool at_src = (::faccessat(src_parent_fd, in.leaf.c_str(), F_OK,
				   AT_SYMLINK_NOFOLLOW | AT_EACCESS) == 0);

  /* The destination may be gone entirely -- a bucket deleted since the
   * crash, or intermediate directories never created.  Create nothing while
   * recovering:  resolve_path() with create_dirs would manufacture the very
   * evidence we are reading. */
  std::unique_ptr<rgw::sal::Bucket> dbucket;
  std::vector<std::unique_ptr<nsfs::Directory>> dchain;
  nsfs::Directory* dst_dir{nullptr};
  std::string dst_leaf;
  int dst_parent_fd = -1;

  int ret = driver->load_bucket_internal(dpp, rgw_bucket(in.tenant, in.bucket),
					 &dbucket, null_yield);
  if (ret == 0) {
    auto* db = static_cast<NSFSBucket*>(dbucket.get());
    ret = nsfs::resolve_path(dpp, db->get_dir(), in.key,
			     /*create_dirs=*/false, driver->ctx(),
			     dchain, dst_dir, dst_leaf,
			     /* recovery is the gateway's:  raise site 5 */
			     nsfs::FSIdentity::none(), nullptr);
    if (ret == 0 && dst_dir) {
      dst_parent_fd = dst_dir->get_fd();
      if (dst_parent_fd < 0) {
	(void) dst_dir->open(dpp);
	dst_parent_fd = dst_dir->get_fd();
      }
    }
  }

  const bool at_dst = (dst_parent_fd >= 0)
    && (::faccessat(dst_parent_fd, dst_leaf.c_str(), F_OK,
		    AT_SYMLINK_NOFOLLOW | AT_EACCESS) == 0);

  if (at_src == at_dst) {
    ldpp_dout(dpp, 0) << "ERROR: rename recovery: " << in.leaf << " -> "
      << in.bucket << "/" << in.key << " is present at "
      << (at_src ? "both keys" : "neither key")
      << ";  leaving the intent record for manual attention" << dendl;
    return -EBUSY;
  }

  int src_vfd = ::openat(src_parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
			 O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
  if (src_vfd < 0) {
    /* the record we just read lives on that directory */
    return -errno;
  }

  size_t n = 0;

  if (at_src) {
    /* never committed:  bring back whatever went across */
    std::vector<LeafVersion> stranded;
    if (dst_parent_fd >= 0) {
      ret = list_leaf_versions(dst_parent_fd, dst_leaf, stranded);
      if (ret < 0) {
	::close(src_vfd);
	return ret;
      }
    }
    int dst_vfd = -1;
    if (! stranded.empty()) {
      dst_vfd = ::openat(dst_parent_fd, HIDDEN_VERSIONS_PATH.c_str(),
			 O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
      if (dst_vfd < 0) {
	ret = -errno;
	::close(src_vfd);
	return ret;
      }
    }
    for (const auto& v : stranded) {
      std::string sname = in.leaf + "_" + v.ver_id;
      ret = recovery_write(dpp, [&]() -> int {
	return (::renameat(dst_vfd, v.entry.c_str(), src_vfd,
			   sname.c_str()) < 0) ? -errno : 0;
      });
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: rename recovery: returning " << v.entry
	  << " failed: " << cpp_strerror(-ret) << ";  the intent record stays "
	  << "and " << in.leaf << " needs manual attention" << dendl;
	::close(dst_vfd);
	::close(src_vfd);
	return ret;
      }
      ++n;
    }
    if (dst_vfd >= 0) {
      ::close(dst_vfd);
    }
    ldpp_dout(dpp, 1) << "rename recovery: " << in.leaf
      << " never committed;  returned " << n << " version(s) from "
      << in.bucket << "/" << in.key << dendl;
  } else {
    /* committed:  the history that did not make it follows the leaf, or, if
     * the move was sliced, was meant to be dropped */
    std::vector<LeafVersion> behind;
    ret = list_leaf_versions(src_parent_fd, in.leaf, behind);
    if (ret < 0) {
      ::close(src_vfd);
      return ret;
    }
    int dst_vfd = -1;
    if (! behind.empty() && ! in.slice) {
      dst_vfd = open_versions_dir(dpp, dst_parent_fd,
				  nsfs::FSIdentity::none(), nullptr);
      if (dst_vfd < 0) {
	::close(src_vfd);
	return dst_vfd;
      }
    }
    for (const auto& v : behind) {
      if (in.slice) {
	ret = recovery_write(dpp, [&]() -> int {
	  return (::unlinkat(src_vfd, v.entry.c_str(), 0) < 0) ? -errno : 0;
	});
	if (ret < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: rename recovery: sliced version "
	    << v.entry << " could not be removed: " << cpp_strerror(-ret)
	    << dendl;
	  continue;
	}
      } else {
	std::string dname = dst_leaf + "_" + v.ver_id;
	ret = recovery_write(dpp, [&]() -> int {
	  return (::renameat(src_vfd, v.entry.c_str(), dst_vfd,
			     dname.c_str()) < 0) ? -errno : 0;
	});
	if (ret < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: rename recovery: forwarding " << v.entry
	    << " failed: " << cpp_strerror(-ret) << ";  the intent record "
	    << "stays and " << in.leaf << " needs manual attention" << dendl;
	  ::close(dst_vfd);
	  ::close(src_vfd);
	  return ret;
	}
      }
      ++n;
    }
    if (dst_vfd >= 0) {
      ::close(dst_vfd);
    }
    ldpp_dout(dpp, 1) << "rename recovery: " << in.leaf << " committed to "
      << in.bucket << "/" << in.key << ";  " << (in.slice ? "dropped " :
      "forwarded ") << n << " version(s)" << dendl;
  }

  /* the record is on a directory its writer owns, so its removal is
   * raised with the moves:  left behind, it would send the next write
   * back through recovery and keep every rename out of here at -EBUSY */
  const std::string name = rename_intent_xattr(xs);
  const int cret = recovery_write(dpp, [&]() -> int {
    return (::fremovexattr(src_vfd, name.c_str()) < 0) ? -errno : 0;
  });
  ::close(src_vfd);
  if ((cret < 0) && (cret != -ENODATA)) {
    ldpp_dout(dpp, 0) << "ERROR: rename recovery: " << in.leaf
      << " committed, but its record could not be removed:  "
      << cpp_strerror(-cret) << ";  renames of it return -EBUSY" << dendl;
  }

  /* The cache cannot describe what recovery just changed:  a rekey expresses
   * a whole move, and this was half of one.  Drop the filled flag on both
   * sides and let the next listing rebuild from the store. */
  auto* bcache = driver->get_bucket_cache();
  if (bcache) {
    (void) bcache->invalidate_bucket(dpp, src_bucket_name);
    if (dbucket) {
      (void) bcache->invalidate_bucket(dpp, in.bucket);
    }
  }
  return 0;
}

/* The version lockfile, `id`'s where `id` is active
 * (DECISIONS 2026-10-15:  a mutex, not a permission).  `dpp` as
 * ensure_versions_dir(). */
static int open_versions_lockfile(const DoutPrefixProvider* dpp,
				  int parent_fd, const nsfs::FSIdentity& id,
				  const nsfs::AccessStrategy* model)
{
  int ret = ensure_versions_dir(dpp, parent_fd, id, model);
  if (ret < 0) {
    return ret;
  }
  if (id.active()) {
    bool raised = false;
    const int fd = nsfs::open_as_or_raise(dpp, id, model,
					  nsfs::AccessClass::data,
					  RAISED_CREATE_CAPS,
					  parent_fd, VERSIONS_LOCKFILE.c_str(),
					  O_RDWR | O_CREAT,
					  nsfs::file_create_mode(), &raised);
    if (fd < 0) {
      return fd;
    }
    struct statx lstx;
    if (! raised) {
      (void) nsfs::apply_create_mode(dpp, fd, nsfs::file_create_mode(),
				     null_yield, id);
    } else if ((statx_ids(fd, lstx) == 0) && (lstx.stx_uid == ::geteuid())) {
      /* the gateway made it:  its directory's owner's, as `.versions` is */
      struct statx pstx;
      (void) nsfs::apply_create_mode(dpp, fd, nsfs::file_create_mode(),
				     null_yield);
      if (statx_ids(parent_fd, pstx) == 0) {
	(void) chown_raised(dpp, fd, "", pstx.stx_uid, pstx.stx_gid);
      }
    }
    return fd;
  }
  int fd = ::openat(parent_fd, VERSIONS_LOCKFILE.c_str(),
                    O_RDWR | O_NOFOLLOW | O_CREAT, nsfs::file_create_mode());
  if (fd < 0) {
    return -errno;
  }
  /* in the tree like any object, so the same mode */
  (void) nsfs::apply_create_mode(nullptr, fd, nsfs::file_create_mode(), null_yield);
  return fd;
}

/* Take a directory's version lock, recovering an interrupted move out of it
 * first.
 *
 * Every mutation of a directory's version history comes through the version
 * lock, so this is where a half-finished rename gets noticed:  the next write
 * to *any* object in that directory repairs it.  The intent record lives on
 * the source, and the destination's stranded versions are reachable only from
 * that record, so the source side is the only side that can find them -- and
 * this is the lock recovery needs anyway.
 *
 * Read paths deliberately do not repair.  Directory::fill_cache() notices the
 * record and says so in the log, but does not take this lock:  doing so would
 * make a listing mutate and would introduce a bucket-cache-entry-lock ->
 * version-lock ordering that does not exist today.  The cost is that a
 * listing can show the delta until the next write to that directory.
 *
 * By default a failed recovery does not fail the caller.  The record may well
 * describe a different object, and failing every write to a directory
 * because one object's move is stuck is worse than the delta.  Callers which
 * cannot tolerate it -- rename(), which would overwrite the record and lose
 * the only pointer to the stranded versions -- pass recovery_ret and check
 * it.
 */
static std::unique_ptr<VersionLockHandle>
version_lock_recovering(const DoutPrefixProvider* dpp, NSFSDriver* driver,
			const nsfs::XattrStrategy* xs,
			const std::string& bucket_name, int parent_fd,
			const nsfs::FSIdentity& id,
			int* recovery_ret = nullptr)
{
  auto vlock = driver->get_fs_strategy()->version_lock(
    dpp, open_versions_lockfile(dpp, parent_fd, id,
				driver->get_access_strategy()));
  if (recovery_ret) {
    *recovery_ret = 0;
  }

  std::string rec;
  if (! rename_intent_present(xs, parent_fd, rec)) {
    return vlock;
  }

  RenameIntent in;
  if (! parse_rename_intent(rec, in)) {
    ldpp_dout(dpp, 0) << "ERROR: .versions/ carries an unparseable "
      << "rename_intent record (" << rec.size() << " bytes);  leaving it "
      << "rather than guessing which move it described" << dendl;
    if (recovery_ret) {
      *recovery_ret = -EBUSY;
    }
    return vlock;
  }

  ldpp_dout(dpp, 1) << "an earlier move of " << in.leaf << " out of this "
    << "directory did not complete;  recovering it" << dendl;
  int ret = recover_rename_intent(xs, dpp, driver,
				  bucket_name, parent_fd, in);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: recovering the interrupted move of "
      << in.leaf << " failed: " << cpp_strerror(-ret) << ";  the record "
      << "stays and the next write to this directory will retry" << dendl;
    if (recovery_ret) {
      *recovery_ret = ret;
    }
  }
  return vlock;
}

struct DemoteResult {
  bool did_demote{false};
  std::string demoted_ver_id;

  /* The demoted version's own facts, captured where the demotion
   * happens.
   *
   * A caller that has already replaced the object cannot read them
   * afterwards:  by then the inode under the leaf name is the *new*
   * version.  That is why four of the five demoted cache entries were
   * built from a key and almost nothing else -- the facts were never
   * carried out of here, and only the delete path, which still holds
   * the pre-delete object, could fill them in for itself.
   *
   * See fill_demoted_entry(). */
  struct statx stx{};
  std::string etag;
  /* what object_owner() returned for the demoted version */
  ACLOwner owner;
  int owner_ret{-ENODATA};
};

/*
 * Demote the current version of an object to .versions/.
 *
 * Caller must already hold (or not need) the version lock.
 * parent_fd: open fd to the bucket directory
 * leaf: object name within that directory
 * ver_enabled: whether versioning is enabled (vs suspended)
 * fs_strategy: for safe_link
 *
 * Returns 0 on success (including "nothing to demote"), negative on error.
 */
/* `id` is who the demote links as, and it is not defaulted:  the link
 * moves the current object, which belongs to whoever wrote it, and
 * fs.protected_hardlinks(5) denies a link to a file the linker
 * neither owns nor can write.  As the gateway that is every object an
 * identity wrote -- a 500 on an account overwriting its own object
 * (PUNCHLIST, run 2026-10-16).  As the authenticated user it succeeds
 * exactly where that user may write the object, which is the rule the
 * rest of the tree follows.  A caller with no authenticated user says
 * so by passing {}. */
static int demote_current_version(const DoutPrefixProvider* dpp,
				  FSStrategy* fs_strategy,
				  const nsfs::XattrStrategy* xs,
				  int parent_fd,
				  const std::string& leaf,
				  bool ver_enabled,
				  DemoteResult& result,
				  const nsfs::FSIdentity& id,
				  const nsfs::AccessStrategy* model,
				  optional_yield y,
				  const nsfs::XattrStrategy* alt = nullptr)
{
  ldpp_dout(dpp, 10) << "nsfs: demote enter leaf=" << leaf
    << " enabled=" << ver_enabled << " as="
    << (id.active() ? to_string(id.key) : std::string("gateway")) << dendl;
  int vfd = open_versions_dir(dpp, parent_fd, id, model);
  if (vfd < 0) {
    ldpp_dout(dpp, 10) << "nsfs: demote " << leaf
      << " has no .versions: " << cpp_strerror(-vfd) << dendl;
    return vfd;
  }
  auto close_vfd = make_scope_guard([vfd] { ::close(vfd); });

  if (!ver_enabled) {
    std::string null_name = nsfs_ver_entry(leaf, NULL_VERSION_ID);
    const int uret = unlinkat_as(dpp, id, model, vfd, null_name.c_str());
    if ((uret < 0) && (uret != -ENOENT)) {
      ldpp_dout(dpp, 4) << "nsfs: demote " << leaf << ":  unlinking "
	<< null_name << ": " << cpp_strerror(-uret) << dendl;
      return uret;
    }
  }

  struct statx cur_stx;
  if (nsfs::statx(parent_fd, leaf.c_str(),
	    AT_SYMLINK_NOFOLLOW, STATX_ALL, &cur_stx) != 0) {
    return 0;
  }
  if (S_ISLNK(cur_stx.stx_mode)) {
    /* not an object, so not a version:  nothing is demoted */
    return unlink_if_link(dpp, id, model, parent_fd, leaf.c_str());
  }
  if (S_ISDIR(cur_stx.stx_mode)) {
    /* the key's name holds a directory, so the write is refused by the
     * rule at refuse_directory_name().  Checked here because link(2)
     * returns EPERM for a directory, which would be reported as a
     * permission denial (403). */
    return refuse_directory_name(dpp, parent_fd, leaf.c_str());
  }

  /* Demoted under the id it was recorded with, so a client holding it
   * still finds it.  An unreadable file keeps the derived id, which is
   * what it would be given if it could be read and had none recorded
   * as a version. */
  std::string cur_ver_id = nsfs_version_id_from_statx(cur_stx);
  {
    int chk_fd = openat_as(dpp, id, model, parent_fd, leaf.c_str(), O_RDONLY);
    if (chk_fd >= 0) {
      cur_ver_id = recorded_version_id(chk_fd, xs, alt);
      ::close(chk_fd);
    }
  }
  const bool cur_is_null = (cur_ver_id == NULL_VERSION_ID);

  if (!ver_enabled && cur_is_null) {
    return 0;
  }

  std::string ver_name = nsfs_ver_entry(leaf, cur_ver_id);

  /* the link inside the wrapper, so a denial can be the gateway's to
   * perform:  as `id` on the helper, then raised */
  SafeResult sr = SafeResult::ERROR;
  (void) nsfs::with_identity_or_raise(dpp, id, model,
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS,
				      [&]() -> int {
    sr = fs_strategy->safe_link(
      dpp, parent_fd, leaf,
      vfd, ver_name,
      statx_mtime_ns(cur_stx), cur_stx.stx_ino, null_yield,
      nsfs::FSIdentity::none());
    return (sr == SafeResult::DENIED) ? -EACCES : 0;
  }, y);
  ldpp_dout(dpp, 10) << "nsfs: demote " << leaf << " -> " << ver_name
    << " result=" << (int)sr << dendl;

  if (sr == SafeResult::OK) {
    int demoted_fd = openat_as(dpp, id, model, vfd, ver_name.c_str(), O_RDONLY);
    if (demoted_fd >= 0) {
      /* when it stopped being current (D5):  on the demoted file, the
       * writer's, and a `user.*` write is checked against the caller */
      const auto now_ms = std::to_string(
	std::chrono::duration_cast<std::chrono::milliseconds>(
	  std::chrono::system_clock::now().time_since_epoch()).count());
      const std::string ts_x =
	xs->disk_name(RGW_NSFS_ATTR_NON_CURRENT_TS);
      const int tret = nsfs::with_identity_or_raise(dpp, id, model,
						    nsfs::AccessClass::data,
						    RAISED_KEYDIR_CAPS,
						    [&]() -> int {
	return (::fsetxattr(demoted_fd, ts_x.c_str(), now_ms.c_str(),
			    now_ms.size(), 0) < 0) ? -errno : 0;
      }, y);
      if (tret < 0) {
	ldpp_dout(dpp, 4) << "nsfs: demote " << leaf
	  << ":  its non-current timestamp was not written: "
	  << cpp_strerror(-tret) << dendl;
      }
      /* While the fd that can return them is open.  cur_stx was taken
       * on the leaf before the link and safe_link moves no bytes, so
       * it describes this same inode. */
      result.etag = etag_from_fd(demoted_fd, xs);
      result.owner_ret = owner_from_fd(demoted_fd, xs, result.owner);
      ::close(demoted_fd);
    }
    result.stx = cur_stx;
    result.did_demote = true;
    result.demoted_ver_id = cur_ver_id;
  } else if (sr == SafeResult::MISMATCH && cur_is_null) {
    /* null version already demoted by a concurrent writer */
  } else if (sr == SafeResult::DENIED) {
    /* the filesystem denied this identity, which S3 reports as a
     * 403:  the request passed the bucket's policy and not the
     * tree's permissions, and saying 500 would name the gateway for
     * the tree's answer */
    ldpp_dout(dpp, 4) << "version demote failed for " << leaf
      << ":  the filesystem denied the writer" << dendl;
    return -EACCES;
  } else if (sr != SafeResult::OK) {
    ldpp_dout(dpp, 0) << "ERROR: version demote failed for "
      << leaf << " sr=" << (int)sr << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  return 0;
}

/* `.shadow` beside `parent_fd`, made as `.versions` is (n plan D4a):
 * as `id` at NooBaa's mode where it is active, raised and given to the
 * key directory's owner where the model permits;  the gateway's
 * otherwise.  `dpp` may be null only when `id` is inactive. */
static int ensure_shadow_dir(const DoutPrefixProvider* dpp, int parent_fd,
			     const nsfs::FSIdentity& id,
			     const nsfs::AccessStrategy* model)
{
  const mode_t mode = nsfs::dir_create_mode();
  if (id.active()) {
    bool raised = false;
    bool made = false;
    const int ret = nsfs::with_identity_or_raise(dpp, id, model,
						 nsfs::AccessClass::data,
						 RAISED_CREATE_CAPS,
						 [&]() -> int {
      if (::mkdirat(parent_fd, HIDDEN_SHADOW_PATH.c_str(), mode) < 0) {
	return (errno == EEXIST) ? 0 : -errno;
      }
      made = true;
      return nsfs::apply_create_mode_at(dpp, parent_fd,
					HIDDEN_SHADOW_PATH.c_str(), mode);
    }, null_yield, &raised);
    if ((ret < 0) || !(raised && made)) {
      return ret;
    }
    return chown_raised_dir(dpp, parent_fd, HIDDEN_SHADOW_PATH.c_str());
  }
  int ret = ::mkdirat(parent_fd, HIDDEN_SHADOW_PATH.c_str(), mode);
  if (ret < 0) {
    return (errno == EEXIST) ? 0 : -errno;
  }
  return nsfs::apply_create_mode_at(nullptr, parent_fd,
				    HIDDEN_SHADOW_PATH.c_str(), mode);
}

static int open_shadow_dir(const DoutPrefixProvider* dpp, int parent_fd,
			   const nsfs::FSIdentity& id,
			   const nsfs::AccessStrategy* model)
{
  int ret = ensure_shadow_dir(dpp, parent_fd, id, model);
  if (ret < 0) {
    return ret;
  }
  return openat_as(dpp, id, model, parent_fd, HIDDEN_SHADOW_PATH.c_str(),
		   O_RDONLY | O_DIRECTORY);
}

/* An object FSIO made raised is the identity's, as a raised copy's
 * destination is (clone_file_by_copy()). */
static int chown_raised_object(const DoutPrefixProvider* dpp, int fd,
			       const nsfs::FSIdentity& id)
{
  return chown_raised(dpp, fd, "", id.cred.uid, id.cred.gid);
}

static int copy_xattrs_fd(const DoutPrefixProvider* dpp, int src_fd, int dst_fd)
{
  char namebuf[64 * 1024];
  ssize_t buflen = ::flistxattr(src_fd, namebuf, sizeof(namebuf));
  if (buflen < 0) {
    return -errno;
  }

  char valbuf[64 * 1024];
  const char* p = namebuf;
  while (buflen > 0) {
    ssize_t keylen = strlen(p) + 1;
    ssize_t vallen = nsfs::fgetxattr(src_fd, p, valbuf, sizeof(valbuf));
    if (vallen < 0) {
      if (errno == ENODATA) {
	buflen -= keylen;
	p += keylen;
	continue;
      }
      return -errno;
    }
    if (::fsetxattr(dst_fd, p, valbuf, vallen, 0) < 0) {
      return -errno;
    }
    buflen -= keylen;
    p += keylen;
  }
  return 0;
}

namespace nsfs {

int resolve_key_path(const DoutPrefixProvider* dpp,
                     Directory* root,
                     const rgw_obj_key& key,
                     const std::string& key_path,
                     const ReservedNames& rn,
                     bool internal_bucket,
                     bool create_dirs,
                     CephContext* cct,
                     std::vector<std::unique_ptr<Directory>>& dir_chain,
                     Directory*& leaf_dir,
                     std::string& leaf_name,
                     const FSIdentity& id,
                     const AccessStrategy* model)
{
  if (!internal_bucket && key.ns.empty() &&
      key_hits_reserved_name(key.name, rn)) {
    if (dpp) {
      ldpp_dout(dpp, 4) << "nsfs: rejecting key " << key.name
	<< ":  a component names driver scaffolding" << dendl;
    }
    return create_dirs ? -ERR_INVALID_OBJECT_NAME : -ENOENT;
  }
  const int ret = resolve_path(dpp, root, key_path, create_dirs, cct,
			       dir_chain, leaf_dir, leaf_name, id, model);
  if (create_dirs && (ret == -ENOTDIR)) {
    /* A write under a file:  `foo/bar` is an object, so `foo/bar/xyzzy`
     * cannot be made, by the rule at refuse_directory_name().  Both
     * keys are legal S3, and AWS and RADOS hold both, because their
     * keys are opaque strings.
     *
     * The sentinel does not help:  it names a key ending in '/', so
     * moving the file there would rename the object rather than
     * re-represent it.  NooBaa has the same constraint and returns
     * 404, which says the object is absent when a conflicting one is
     * present. */
    if (dpp) {
      ldpp_dout(dpp, 4) << "nsfs: rejecting key " << key.name
	<< ":  a component of it is an existing object" << dendl;
    }
    return -ERR_KEY_PATH_CONFLICT;
  }
  return ret;
}

int resolve_path(const DoutPrefixProvider* dpp,
                 Directory* root,
                 const std::string& key_path,
                 bool create_dirs,
                 CephContext* cct,
                 std::vector<std::unique_ptr<Directory>>& dir_chain,
                 Directory*& leaf_dir,
                 std::string& leaf_name,
                 const FSIdentity& id,
                 const AccessStrategy* model)
{
  leaf_dir = root;
  leaf_name = key_path;

  size_t pos = 0;
  size_t slash;
  while ((slash = key_path.find('/', pos)) != std::string::npos) {
    std::string component = key_path.substr(pos, slash - pos);
    if (component.empty()) {
      pos = slash + 1;
      continue;
    }

    auto dir = std::make_unique<Directory>(component, leaf_dir, cct);
    dir->set_identity(id);
    dir->set_access_model(model);
    int ret = dir->open(dpp);
    if (ret < 0) {
      if (!create_dirs || ret != -ENOENT) {
        return ret;
      }
      ret = dir->create(dpp, null_yield);
      if (ret < 0) {
        return ret;
      }
      ret = dir->open(dpp);
      if (ret < 0) {
        return ret;
      }
    }

    leaf_dir = dir.get();
    dir_chain.push_back(std::move(dir));
    pos = slash + 1;
  }

  leaf_name = key_path.substr(pos);
  return 0;
}

} // namespace nsfs

static inline std::string gen_rand_instance_name()
{
  enum { OBJ_INSTANCE_LEN = 32 };
  char buf[OBJ_INSTANCE_LEN + 1];

#if 0
  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, OBJ_INSTANCE_LEN);
#else
  static uint64_t last_id = UINT64_MAX;
  snprintf(buf, OBJ_INSTANCE_LEN, "%lx", last_id);
  last_id--;
#endif

  return buf;
}

static inline bool get_attr(Attrs& attrs, const char* name, bufferlist& bl)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return false;
  }

  bl = iter->second;
  return true;
}

/* Returns TRUE on success, like get_attr() above -- not an errno.
 * Four callers once tested it as an int:  one inverted the sense and
 * discarded every part's metadata read from disk, and three wrote
 * `if (ret < 0)`, which a bool never satisfies, so their failure
 * handling was dead code and a failed decode read as success with a
 * default-constructed result. */
template <typename F>
[[nodiscard]] static bool decode_attr(Attrs &attrs, const char *name, F &f) {
  bufferlist bl;
  if (!get_attr(attrs, name, bl)) {
    return false;
  }
  try {
    auto bufit = bl.cbegin();
    decode(f, bufit);
  } catch (buffer::error &err) {
    return false;
  }

  return true;
}


/* Ownership is XattrStrategy::object_owner() -- the owner an object
 * records, if any -- and, where none is recorded, the bucket's
 * (set_dir_entry_owner(), NSFSBucket::default_object_owner()). */

static inline ceph::real_time from_statx_timestamp(const struct statx_timestamp& xts)
{
  struct timespec ts{xts.tv_sec, xts.tv_nsec};
  return ceph::real_clock::from_timespec(ts);
}

/* The cache entry for a version a write has just demoted.
 *
 * One function because every caller of demote_current_version() needs
 * this entry and each of them built a different subset of it:  the PUT
 * path set the key and the flags and nothing else, so on a warm cache
 * every non-current version listed as zero-length with no etag and no
 * owner.  The bytes were never at risk -- a versioned GET reads the
 * file -- but the listing said otherwise.
 *
 * It hid because a listing that misses runs Directory::fill_cache(),
 * which reads each entry's xattrs and fills everything in.  A bucket
 * listed for the first time after its writes therefore looks correct,
 * and so does every test that creates a bucket, writes, and lists
 * once.  The same mechanism hid the FLAG_CURRENT half of this bug
 * until it was fixed in NSFSFSIOObject::publish(), whose comment
 * describes it.
 */
static void fill_demoted_entry(const std::string& obj_name,
			       const DemoteResult& d,
			       const ACLOwner& bucket_owner,
			       rgw_bucket_dir_entry& bde)
{
  bde.key.name = obj_name;
  bde.key.instance = d.demoted_ver_id;
  bde.ver.pool = 1;
  bde.ver.epoch = 1;
  bde.exists = true;
  bde.meta.category = RGWObjCategory::Main;
  bde.meta.size = d.stx.stx_size;
  bde.meta.accounted_size = d.stx.stx_size;
  bde.meta.mtime = from_statx_timestamp(d.stx.stx_mtime);
  bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
  /* its own digest where it has one;  see etag_from_fd() for why the
   * synthesized token must not stand in for a real one */
  bde.meta.etag = d.etag.empty() ? synthesize_etag(d.stx) : d.etag;
  /* FLAG_VER without FLAG_CURRENT is what demotion means, and clearing
   * that flag is why the entry is rewritten at all */
  bde.flags = rgw_bucket_dir_entry::FLAG_VER;
  set_dir_entry_owner(bde, d.owner_ret, d.owner, bucket_owner);
}

static inline int copy_dir_fd(int old_fd)
{
  return openat(old_fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
}

/* Read an object's attributes, through one format or through two.
 *
 * `alt` is the fallback strategy and is null everywhere but
 * a bucket mid-upgrade -- see NSFSBucket::xattr_fallback().  It is
 * asked only about a name the bucket's own format declines, so a tree
 * with nothing foreign left runs the loop it ran before:  no extra
 * syscall, and one more string comparison for each name neither format
 * owns, which in practice is `security.selinux`.
 *
 * The nsfs format first.  An object rewritten during the conversion
 * carries nsfs names and must be read as nsfs's even while the bucket
 * still holds objects that are not.
 *
 * Names starting with `skip` are not read at all:  NooBaaDirectoryObject
 * reads a directory that may also carry the bucket's own state.
 *
 * `unknown`, if given, receives every `user.noobaa.*` name the reviewed
 * releases of NooBaa do not write (X4's third layer, DECISIONS.md
 * 2026-10-26).  Reported rather than refused here, because listing reads
 * through this too, and one object must not end a listing;  the callers
 * that hand out or convert an object refuse. */
static int get_x_attrs(optional_yield y, const DoutPrefixProvider* dpp, int fd,
		       Attrs& attrs, const std::string& display,
		       const nsfs::XattrStrategy* xs,
		       const nsfs::XattrStrategy* alt = nullptr,
		       const nsfs::FSIdentity& id = {},
		       std::string_view skip = {},
		       std::vector<std::string>* unknown = nullptr)
{
  /* The whole read as the identity:  each value needs read
   * permission on the object.  Nested with_identity() calls below are harmless -- a thread
   * already running as the identity makes the call directly. */
  return nsfs::with_identity(dpp, id, [&]() -> int {
  /* large enough for every attribute an object carries in practice;  the
   * biggest is the encoded ACL at a couple of hundred bytes */
  enum { INLINE_VALUE_MAX = 1024 };
  char namebuf[64 * 1024]; // Max list size supported on linux
  ssize_t buflen;
  int ret;

  buflen = flistxattr(fd, namebuf, sizeof(namebuf));
  if (buflen < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not list attributes for " << display << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  std::vector<std::string> unclaimed;
  char *keyptr = namebuf;
  while (buflen > 0) {
    std::string value;
    ssize_t vallen, keylen;
    char* vp;

    keylen = strlen(keyptr) + 1;
    std::string xattr_name(keyptr);
    std::string key;

    if (!skip.empty() && xattr_name.starts_with(skip)) {
      buflen -= keylen;
      keyptr += keylen;
      continue;
    }

    const nsfs::XattrStrategy* owner = xs;
    if (!xs->parse_disk_name(xattr_name, key)) {
      if (!alt || !alt->parse_disk_name(xattr_name, key)) {
	/* Neither format places this name.  Kept rather than
	 * forgotten:  a one-to-many attribute -- object tags, which
	 * are one per tag in NooBaa's format -- cannot be placed a name at
	 * a time, and parse_disk_attrs() is given these to make what
	 * they encode.  The value is not read here;  the format reads
	 * the ones it claims. */
	if (unknown && !nsfs::noobaa_schema::object_attribute_known(xattr_name)) {
	  unknown->push_back(xattr_name);
	}
	unclaimed.emplace_back(std::move(xattr_name));
	buflen -= keylen;
	keyptr += keylen;
	continue;
      }
      owner = alt;
    }

    /* Read straight into a stack buffer.  Sizing the value first with a
     * nullptr call doubles the syscalls on this path, and every attribute
     * an object actually carries fits well inside INLINE_VALUE_MAX -- the
     * largest is the encoded ACL, a couple of hundred bytes.  A value that
     * does not fit falls back to the size-then-read pair. */
    char valbuf[INLINE_VALUE_MAX];
    vallen = nsfs::fgetxattr(fd, keyptr, valbuf, sizeof(valbuf));
    vp = valbuf;

    if (vallen < 0 && errno == ERANGE) {
      vallen = nsfs::fgetxattr(fd, keyptr, nullptr, 0);
      if (vallen >= 0) {
	value.resize(vallen);
	vp = value.data();
	vallen = nsfs::fgetxattr(fd, keyptr, vp, vallen);
      }
    }

    if (vallen < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << keyptr << " for " << display << ": " << cpp_strerror(ret) << dendl;
      return -ret;
    }

    bufferlist bl;
    bl.append(vp, vallen);
    /* `owner` and not `xs`:  whether a value is a counted string is the
     * format's, and NooBaa's are not.  Asking the wrong one appends a
     * NUL the NooBaa value never had. */
    if (owner->counted_string_value(key)) {
      /* Restore the terminator RGW writes and attr_on_disk() strips.
       *
       * If the value already ends in one, some write path skipped
       * attr_on_disk() and we are about to double it.  That is a bug in
       * this driver, not in the tree, so say so loudly rather than hand
       * a NUL to a client. */
      if (vallen > 0 && vp[vallen - 1] == '\0') {
	ldpp_dout(dpp, 0) << "ERROR: attribute " << key << " on " << display
	  << " is already NUL-terminated on disk;  some write path skipped "
	  << "attr_on_disk().  Not restoring." << dendl;
      } else {
	bl.append('\0');
      }
    }
    attrs.emplace(std::move(key), std::move(bl)); /* key and bl are r-value refs */

    buflen -= keylen;
    keyptr += keylen;
  }

  /* What the per-name pass could not place.
   *
   * The nsfs format first here too, and for the same reason the read
   * asks that way round:  an attribute a converting object carries in
   * the nsfs format is the later word.  In practice the nsfs format
   * claims nothing at this stage -- every nsfs logical attribute is one
   * attribute on disk -- so this costs a virtual call and no syscall on
   * an nsfs tree. */
  if (!unclaimed.empty()) {
    xs->parse_disk_attrs(dpp, fd, unclaimed, attrs);
    if (alt) {
      alt->parse_disk_attrs(dpp, fd, unclaimed, attrs);
    }
  }

  return 0;
  }, y);
}

/* The on-disk form of an attribute value.
 *
 * RGW stores several string attributes as counted strings, terminator
 * included;  on rados that byte is private, here it is on-disk format.
 * Strip it going out.  get_x_attrs() restores it coming back, so every
 * RGW consumer sees what it would see on rados.
 *
 * EVERY write of an attribute value must come through here.  The driver
 * has twenty raw fsetxattr() sites;  they all ask the strategy for the
 * name already, and they must all ask it about the value too.  Missing
 * one does not lose the byte, it *doubles* it -- the read side restores
 * unconditionally -- and a doubled terminator reaches the client.  That
 * is not hypothetical:  it happened to multipart on the first attempt.
 *
 * See XattrStrategy::counted_string_value() and
 * docs/RGW_COUNTED_STRING_ATTRS.md. */
static std::string attr_on_disk(const nsfs::XattrStrategy* xs,
				const std::string& key,
				const bufferlist& value)
{
  std::string v = value.to_str();
  if (!v.empty() && v.back() == '\0' && xs->counted_string_value(key)) {
    v.pop_back();
  }
  return v;
}

static int write_x_attr(const DoutPrefixProvider* dpp, optional_yield y, int fd,
			const std::string& key, bufferlist& value,
			const std::string& display,
			const nsfs::XattrStrategy* xs,
			const nsfs::FSIdentity& id)
{
  int ret;
  std::string attrname;

  attrname = xs->disk_name(key);
  if (attrname.empty()) {
    /* nowhere to keep it in this format;  accepted and not stored, as
     * NooBaa's own gateway does with an ACL */
    return 0;
  }

  const std::string v = attr_on_disk(xs, key, value);
  /* as the object's identity:  a `user.*` write is checked against the
   * calling thread */
  ret = nsfs::with_identity(dpp, id, [&]() -> int {
    return (::fsetxattr(fd, attrname.c_str(), v.data(), v.size(), 0) < 0)
	? -errno : 0;
  }, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not write attribute " << attrname << " for " << display << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

static int remove_x_attr(const DoutPrefixProvider *dpp, optional_yield y,
                         int fd, const std::string &key,
                         const std::string &display,
                         const nsfs::XattrStrategy* xs,
                         const nsfs::XattrStrategy* alt = nullptr,
                         nsfs::FSStrategy* fs = nullptr,
                         const nsfs::FSIdentity& id = {})
{
  int ret;

  /* Through the fallback as well, as a write is.  Mid-upgrade an
   * object the conversion has not reached carries the key in NooBaa's
   * spelling, which the fallback reads, so removing the key removes
   * every spelling either format gives it.  A spelling the file lacks
   * is not a failure:  it carries one or the other. */
  if (alt) {
    std::vector<std::string> current;
    if (fs) {
      nsfs::xattr_map_t raw;
      ret = fs->get_xattrs(dpp, fd, raw, y, id);
      if (ret < 0) {
	return ret;
      }
      current.reserve(raw.size());
      for (auto& [n, _] : raw) {
	current.emplace_back(n);
      }
    }
    std::vector<std::string> names;
    for (const auto* s : {xs, alt}) {
      nsfs::xattr_map_t none_to_write;
      bufferlist none;
      if (fs && s->disk_attrs(key, none, current, none_to_write, names)) {
	continue;
      }
      std::string n{s->disk_name(key)};
      if (!n.empty()) {
	names.push_back(std::move(n));
      }
    }
    if (fs) {
      return fs->remove_xattrs(dpp, fd, names, y, id);
    }
    for (const auto& n : names) {
      /* no strategy here, so the identity is applied directly */
      const int rret = nsfs::with_identity(dpp, id, [&]() -> int {
	if ((fremovexattr(fd, n.c_str()) < 0) && (errno != ENODATA)) {
	  return -errno;
	}
	return 0;
      }, y);
      if (rret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: could not remove attribute " << n
	  << " for " << display << ": " << cpp_strerror(-rret) << dendl;
	return rret;
      }
    }
    return 0;
  }

  /* A key one format spreads over several names cannot be removed by
   * naming one.  Object tags are one attribute per tag in NooBaa's format,
   * so DeleteObjectTagging has to remove all of them -- asked of the
   * format the same way a write is, with nothing to write. */
  if (fs) {
    nsfs::xattr_map_t raw;
    if (fs->get_xattrs(dpp, fd, raw, y, id) >= 0) {
      std::vector<std::string> current;
      current.reserve(raw.size());
      for (auto& [n, _] : raw) {
	current.emplace_back(n);
      }
      nsfs::xattr_map_t none_to_write;
      std::vector<std::string> spread;
      bufferlist none;
      if (xs->disk_attrs(key, none, current, none_to_write, spread)) {
	if (spread.empty()) {
	  /* nothing of this key is on the file;  removing what is not
	   * there is not a failure, and NooBaa's own DeleteObjectTagging
	   * swallows it too (`namespace_fs.js:2794`) */
	  return 0;
	}
	return fs->remove_xattrs(dpp, fd, spread, y, id);
      }
    }
  }

  std::string attrname{xs->disk_name(key)};
  if (attrname.empty()) {
    /* this format keeps the attribute nowhere, so there is nothing to
     * remove and nothing went wrong */
    return 0;
  }

  /* Through the strategy where there is one, so this carries the
   * identity like every other branch.  This is the branch a format that
   * keeps the key under a single name takes -- which is the nsfs
   * format for object tags -- and it removed the attribute as the
   * gateway, so DeleteObjectTagging answered 403 against an
   * identity's own object. */
  if (fs) {
    std::vector<std::string> one{attrname};
    return fs->remove_xattrs(dpp, fd, one, y, id);
  }

  ret = nsfs::with_identity(dpp, id, [&]() -> int {
    if (fremovexattr(fd, attrname.c_str()) < 0) {
      return -errno;
    }
    return 0;
  }, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not remove attribute " << attrname
      << " for " << display << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

/* A denial is the caller's to handle -- raise site 3 and the access
 * model retry it raised, and a refusal that stands is reported where
 * it stands -- so it is not an error here (efe009fbcad, for site 1). */
static int removal_log_level(int err)
{
  return ((err == EACCES) || (err == EPERM)) ? 10 : 0;
}

static const char* removal_log_prefix(int err)
{
  return removal_log_level(err) ? "" : "ERROR: ";
}

static int delete_directory(int parent_fd, const char* dname, bool delete_children,
		     const DoutPrefixProvider* dpp,
		     const nsfs::ReservedNames& rn);

/* Everything in the directory open on `dir_fd`, which this takes, and
 * not the directory itself:  delete_directory() is this and the
 * directory's removal from its parent.  Apart because a bucket in the
 * base path has its contents removed by the authenticated user and its
 * entry by the gateway (NSFSBucket::remove()). */
static int delete_directory_entries(int dir_fd, const char* dname,
				    bool delete_children,
				    const DoutPrefixProvider* dpp,
				    const nsfs::ReservedNames& rn)
{
  int ret;
  DIR *dir;
  struct dirent *entry;

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    ret = errno;
    ldpp_dout(dpp, removal_log_level(ret)) << removal_log_prefix(ret)
                      << "could not open directory " << dname
                      << " for listing: " << cpp_strerror(ret) << dendl;
    ::close(dir_fd);
    return -ret;
  }

  errno = 0;
  while ((entry = readdir(dir)) != NULL) {
    struct statx stx;

    if ((entry->d_name[0] == '.' && entry->d_name[1] == '\0') ||
        (entry->d_name[0] == '.' && entry->d_name[1] == '.' &&
         entry->d_name[2] == '\0')) {
      /* Skip . and .. */
      errno = 0;
      continue;
    }

    /* scaffolding is removed with the directory;  content blocks the
     * removal unless the caller asked to recurse.  See entry_is_content. */
    if (entry_is_content(dir_fd, entry->d_name, rn) && !delete_children) {
      closedir(dir);
      return -ENOTEMPTY;
    }

    ret = nsfs::statx(dir_fd, entry->d_name, AT_SYMLINK_NOFOLLOW, STATX_ALL,
		      &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, removal_log_level(ret)) << removal_log_prefix(ret)
                        << "could not stat object " << entry->d_name
                        << ": " << cpp_strerror(ret) << dendl;
      closedir(dir);
      return -ret;
    }

    if (S_ISDIR(stx.stx_mode)) {
      /* Recurse */
      ret = delete_directory(dir_fd, entry->d_name, true, dpp, rn);
      if (ret < 0) {
        closedir(dir);
        return ret;
      }

      continue;
    }

    /* Otherwise, unlink */
    ret = unlinkat(dir_fd, entry->d_name, 0);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, removal_log_level(ret)) << removal_log_prefix(ret)
                        << "could not remove file " << entry->d_name
                        << ": " << cpp_strerror(ret) << dendl;
      closedir(dir);
      return -ret;
    }
  }
  closedir(dir);
  return 0;
}

static int delete_directory(int parent_fd, const char* dname, bool delete_children,
		     const DoutPrefixProvider* dpp,
		     const nsfs::ReservedNames& rn)
{
  int ret;
  int dir_fd = openat(parent_fd, dname, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (dir_fd < 0) {
    dir_fd = errno;
    ldpp_dout(dpp, removal_log_level(dir_fd)) << removal_log_prefix(dir_fd)
                      << "could not open subdir " << dname << ": "
                      << cpp_strerror(dir_fd) << dendl;
    return -dir_fd;
  }
  ret = delete_directory_entries(dir_fd, dname, delete_children, dpp, rn);
  if (ret < 0) {
    return ret;
  }

  ret = unlinkat(parent_fd, dname, AT_REMOVEDIR);
  if (ret < 0) {
    ret = errno;
    if (errno != ENOENT) {
      ldpp_dout(dpp, removal_log_level(ret)) << removal_log_prefix(ret)
	<< "could not remove directory " << dname << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  return 0;
}

namespace nsfs {

FSEnt::FSEnt(std::string _name, Directory* _parent, CephContext* _ctx, FSStrategy* _strat)
  : fname(_name), parent(_parent), ctx(_ctx),
    fs_strategy(_strat ? _strat : (_parent ? _parent->fs_strategy : nullptr)),
    mpu_strategy(_parent ? _parent->mpu_strategy : nullptr),
    xattr_strategy(_parent ? _parent->xattr_strategy : nullptr),
    path_strategy(_parent ? _parent->path_strategy : nullptr),
    xattr_fallback(_parent ? _parent->xattr_fallback : nullptr),
    path_fallback(_parent ? _parent->path_fallback : nullptr),
    reserved_names(_parent ? _parent->reserved_names : nullptr)
{}

FSEnt::FSEnt(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx, FSStrategy* _strat)
  : fname(_name), parent(_parent), exist(true), stx(_stx), stat_done(true), ctx(_ctx),
    fs_strategy(_strat ? _strat : (_parent ? _parent->fs_strategy : nullptr)),
    mpu_strategy(_parent ? _parent->mpu_strategy : nullptr),
    xattr_strategy(_parent ? _parent->xattr_strategy : nullptr),
    path_strategy(_parent ? _parent->path_strategy : nullptr),
    xattr_fallback(_parent ? _parent->xattr_fallback : nullptr),
    path_fallback(_parent ? _parent->path_fallback : nullptr),
    reserved_names(_parent ? _parent->reserved_names : nullptr)
{}

int FSEnt::stat(const DoutPrefixProvider* dpp, bool force)
{
  if (force) {
    stat_done = false;
  }

  if (stat_done) {
    return 0;
  }

  /* AT_FDCWD with no parent, which is how `Directory::open()` has
   * always resolved one:  a parentless entry's `fname` is an absolute
   * path.  The base path is such an entry and so is a bucket served
   * from a stored path (P2), and this dereferenced the null. */
  int ret = nsfs::statx(parent ? parent->get_fd() : AT_FDCWD, fname.c_str(),
		  AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    exist = false;
    return -ret;
  }

  exist = true;
  stat_done = true;
  return 0;
}

/* A descriptor for this entry, opened as the gateway, owned by the
 * caller.  See the header for why it is not the entry's own.
 *
 * `parent->get_fd()` is right here where it is not right for an
 * NSFSObject resolving a nested key:  by the time an entry exists,
 * its chain has been walked and its parent is the directory it
 * actually sits in. */
int FSEnt::open_scratch_as_gateway(const DoutPrefixProvider* dpp,
				   bool read_only)
{
  if (!parent) {
    return -EINVAL;
  }
  int pfd = parent->get_fd();
  if (pfd < 0) {
    const int pret = parent->open(dpp);
    if (pret < 0) {
      return pret;
    }
    pfd = parent->get_fd();
  }
  const int flags = (read_only ? O_RDONLY : O_RDWR) | O_NOFOLLOW;
  const int fd = ::openat(pfd, get_name().c_str(), flags);
  if (fd < 0) {
    return -errno;
  }
  return fd;
}

int FSEnt::write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs,
		       AttrWriteMode mode, const std::vector<std::string>* rmattrs,
		       int use_fd)
{
  int ret = 0;
  if (use_fd < 0) {
    ret = open(dpp);
    if (ret < 0) {
      return ret;
    }
  }
  /* the caller's descriptor carries no identity:  it opened it as
   * the gateway and holds the raise around this call */
  const int afd = (use_fd >= 0) ? use_fd : get_fd();
  const FSIdentity& aid = (use_fd >= 0) ? FSIdentity::none() : identity;

  need_fsync = true;

  /* this entry's own names, where it has them;  see own_xattr_strategy */
  const XattrStrategy* xs = own_xattr_strategy ? own_xattr_strategy
					       : xattr_strategy;
  const XattrStrategy* xf = own_xattr_strategy ? nullptr : xattr_fallback;

  /* An empty value means "leave this attr alone", matching
   * RGWRados::set_attrs().  MERGE only:  under REPLACE_ALL a name absent
   * from the write set is pruned, so skipping it here would delete the
   * attr rather than preserve it. */
  const bool skip_empty = (mode == AttrWriteMode::MERGE);

  if (fs_strategy) {
    nsfs::xattr_map_t to_write;
    std::vector<std::string> to_remove;

    /* The names already on the file, for the formats that need them.
     *
     * A one-to-many attribute -- object tags, one per tag in NooBaa's
     * format -- has to remove what it supersedes, and which names those
     * are is only knowable from the disk, because the tag keys are in
     * the names.  Read once and lazily:  nothing asks for it unless a
     * format's disk_attrs() does, so an nsfs tree pays nothing. */
    std::vector<std::string> current;
    bool have_current = false;
    auto disk_names = [&]() -> const std::vector<std::string>& {
      if (!have_current) {
	have_current = true;
	nsfs::xattr_map_t raw;
	if (fs_strategy->get_xattrs(dpp, afd, raw, y, aid) >= 0) {
	  current.reserve(raw.size());
	  for (auto& [n, _] : raw) {
	    current.emplace_back(n);
	  }
	}
      }
      return current;
    };

    /* An empty disk name means this format keeps the attribute
     * nowhere -- an S3 ACL on a NooBaa tree, where NooBaa's own gateway
     * also accepts the request and stores nothing.  Skipped rather
     * than written under a name neither format owns. */
    auto place = [&](const std::string& key, const bufferlist& bl) {
      /* the widened pair first:  a key one format spreads over
       * several names cannot be placed by disk_name(), which returns
       * one.  False means this format does not spread this key, which
       * is every key for us today. */
      if (xs->disk_attrs(key, bl, disk_names(), to_write,
				     to_remove)) {
	return;
      }
      std::string dname = xs->disk_name(key);
      if (dname.empty()) {
	return;
      }
      to_write.try_emplace(std::move(dname),
			   attr_on_disk(xs, key, bl));
    };

    if (extra_attrs) {
      for (auto& [key, bl] : *extra_attrs) {
        if (skip_empty && !bl.length()) {
          continue;
        }
        place(key, bl);
      }
    }
    for (auto& [key, bl] : attrs) {
      if (skip_empty && !bl.length()) {
        continue;
      }
      place(key, bl);
    }

    if (mode == AttrWriteMode::REPLACE_ALL) {
      nsfs::xattr_map_t old_raw;
      fs_strategy->get_xattrs(dpp, afd, old_raw, y, aid);

      for (auto& [disk_name, _] : old_raw) {
        /* Facts about the bucket that no attribute write owns.  Each
         * parses as an nsfs name -- they are under user.nsfs. -- so
         * without this they would be pruned by the next REPLACE_ALL:
         * a bucket would lose its profile a moment after being marked,
         * or forget that its directory predates it and delete a tree
         * it was only lent.
         *
         * The converting marker is here for completeness.  A bucket
         * write cannot land mid-conversion today, because BucketGate
         * holds mutations off, so it has never been reachable -- which
         * is a reason to list it rather than to leave it out. */
        if ((disk_name == nsfs::EXTENSIONS_XATTR) ||
            (disk_name == nsfs::CONVERTING_XATTR) ||
            (disk_name == nsfs::OWNS_STORAGE_XATTR)) {
          continue;
        }
        /* Through the fallback as well.  A superseded NooBaa attribute
         * then resolves to the key about to be written and
         * lands in to_remove, which is how a converting object stops
         * carrying both spellings of the same fact. */
        std::string logical;
        if (!xs->parse_disk_name(disk_name, logical) &&
            (!xf ||
             !xf->parse_disk_name(disk_name, logical))) {
          continue;
        }
        if (to_write.find(disk_name) == to_write.end()) {
          to_remove.push_back(disk_name);
        }
      }
    } else if (rmattrs) {
      for (auto& key : *rmattrs) {
        /* The same translation, with nothing to write.  A key one
         * format spreads over several names is removed by naming
         * none of them:  disk_attrs() supersedes whatever is on disk
         * and is not in to_write, which with an empty value is all of
         * it.  DeleteObjectTagging arrives here, and without this it
         * removed the one nsfs name and left NooBaa's tags. */
        bufferlist none;
        /* and through the fallback, for the spelling an object the
         * conversion has not reached still carries */
        for (const auto* s : {xs, xf}) {
          if (!s) {
            continue;
          }
          if (s->disk_attrs(key, none, disk_names(), to_write,
                            to_remove)) {
            continue;
          }
          std::string disk_name{s->disk_name(key)};
          if (!disk_name.empty() &&
              (to_write.find(disk_name) == to_write.end())) {
            to_remove.push_back(std::move(disk_name));
          }
        }
      }
    }

    /* One logical key, one spelling.
     *
     * MERGE touches only the keys it was given, which is the object
     * contract and must stay that way -- so the wholesale prune above
     * is not available here.  What is needed is narrower:  a key we
     * are writing may already be on this file under the other
     * format's name, and leaving it there gives the object two
     * spellings of one fact.  The read would keep finding nsfs's, so
     * nothing would look wrong, and the conversion would never finish
     * because the foreign attribute never goes away.
     *
     * Only where a fallback exists, which is only mid-upgrade.  The
     * design record had this landing in the REPLACE_ALL prune with no
     * new logic;  that was wrong, because objects do not write that
     * way. */
    if (xf) {
      for (auto& [disk_name, _] : to_write) {
        std::string logical;
        if (!xs->parse_disk_name(disk_name, logical)) {
          continue;
        }
        std::string foreign = xf->disk_name(logical);
        if (!foreign.empty() && (foreign != disk_name) &&
            (to_write.find(foreign) == to_write.end())) {
          to_remove.push_back(std::move(foreign));
        }
      }
    }

    /* The driver never writes NooBaa's format (DECISIONS.md 2026-10-21).
     * The gate keeps a client's write off a bucket in it, and this keeps
     * every other writer off, below the gate.  Removals pass:  they are
     * how a conversion leaves nothing of NooBaa's behind. */
    if (!to_write.empty() && !xs->writes()) {
      ldpp_dout(dpp, 0) << "ERROR: rejecting a write of " << to_write.size()
	<< " attribute(s) to " << get_name() << " in the " << xs->name()
	<< " format:  the driver never writes it" << dendl;
      return -ERR_INVALID_BUCKET_STATE;
    }

    if (!to_remove.empty()) {
      ret = fs_strategy->remove_xattrs(dpp, afd, to_remove, y, aid);
      if (ret < 0) {
        return ret;
      }
    }

    return fs_strategy->set_xattrs(dpp, afd, to_write, y, aid);
  }

  /* per-attr syscalls;  first the same rule as the strategy branch
   * above, before anything is removed */
  if (!xs->writes()) {
    auto writing = [&](const Attrs& m) {
      return std::any_of(m.begin(), m.end(),
			 [](const auto& kv) { return kv.second.length() != 0; });
    };
    if (writing(attrs) || (extra_attrs && writing(*extra_attrs))) {
      ldpp_dout(dpp, 0) << "ERROR: rejecting a write to " << get_name()
	<< " in the " << xs->name() << " format:  the driver never writes it"
	<< dendl;
      return -ERR_INVALID_BUCKET_STATE;
    }
  }

  if (mode == AttrWriteMode::REPLACE_ALL) {
    Attrs old_attrs;
    ret = get_x_attrs(y, dpp, afd, old_attrs, get_name(), xs,
		    xf, aid);
    if (ret >= 0) {
      for (auto& it : old_attrs) {
        if (attrs.find(it.first) == attrs.end() &&
            (!extra_attrs || extra_attrs->find(it.first) == extra_attrs->end())) {
          remove_x_attr(dpp, y, afd, it.first, get_name(), xs, xf,
			nullptr, aid);
        }
      }
    }
  } else if (rmattrs) {
    for (auto& key : *rmattrs) {
      if (attrs.find(key) == attrs.end() &&
          (!extra_attrs || extra_attrs->find(key) == extra_attrs->end())) {
        remove_x_attr(dpp, y, afd, key, get_name(), xs, xf,
		      nullptr, aid);
      }
    }
  }

  if (extra_attrs) {
    for (auto &it : *extra_attrs) {
      if (skip_empty && !it.second.length()) {
        continue;
      }
      ret = write_x_attr(dpp, y, fd, it.first, it.second, get_name(),
			 xs, aid);
      if (ret < 0) {
        return ret;
      }
    }
  }

  for (auto& it : attrs) {
    if (skip_empty && !it.second.length()) {
      continue;
    }
    ret = write_x_attr(dpp, y, fd, it.first, it.second, get_name(),
		       xs, aid);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

int FSEnt::read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs,
		      int use_fd, std::vector<std::string>* unknown)
{
  if (use_fd < 0) {
    const int ret = open(dpp);
    if (ret < 0) {
      return ret;
    }
  }
  const int afd = (use_fd >= 0) ? use_fd : get_fd();
  const FSIdentity& aid = (use_fd >= 0) ? FSIdentity::none() : identity;

  /* this entry's own names, where it has them;  see own_xattr_strategy */
  if (own_xattr_strategy) {
    return get_x_attrs(y, dpp, afd, attrs, get_name(),
		       own_xattr_strategy, nullptr, aid, {}, unknown);
  }
  return get_x_attrs(y, dpp, afd, attrs, get_name(), xattr_strategy,
		    xattr_fallback, aid, {}, unknown);
}

int FSEnt::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const ACLOwner& bucket_owner, const std::string& path_prefix)
{
  rgw_bucket_dir_entry bde{};

  rgw_obj_key key = path_strategy->key_from_name(path_prefix + get_name());

  int ret = make_dir_entry(dpp, y, key, flags, bucket_owner, bde);
  if (ret < 0) {
    return ret;
  }
  return cb(dpp, bde);
}

int FSEnt::make_dir_entry(const DoutPrefixProvider *dpp, optional_yield y,
			  const rgw_obj_key& in_key, uint32_t flags,
			  const ACLOwner& bucket_owner,
			  rgw_bucket_dir_entry& bde)
{
  rgw_obj_key key = in_key;
  if (parent->get_type() == ObjectType::MULTIPART) {
    key.ns = mp_ns;
  }
  key.get_index_key(&bde.key);
  bde.ver.pool = 1;
  bde.ver.epoch = 1;

  switch (parent->get_type().type) {
    case ObjectType::MULTIPART:
    case ObjectType::DIRECTORY:
      bde.exists = true;
      break;
    case ObjectType::UNKNOWN:
    case ObjectType::FILE:
      return -EINVAL;
  }

  Attrs attrs;
  int ret = open(dpp);
  if (ret < 0)
    return ret;

  /* through the entry, which knows where its attributes are:  a
   * NooBaaDirectoryObject's are on its directory */
  ret = read_attrs(dpp, y, attrs);
  if (ret < 0)
    return ret;

  ACLOwner acl_owner;
  ret = xattr_strategy->object_owner(attrs, acl_owner);
  set_dir_entry_owner(bde, ret, acl_owner, bucket_owner);
  bde.meta.category = RGWObjCategory::Main;
  bde.meta.size = stx.stx_size;
  bde.meta.accounted_size = stx.stx_size;
  bde.meta.mtime = from_statx_timestamp(stx.stx_mtime);
  bde.meta.storage_class = listed_storage_class(attrs);
  bde.meta.appendable = true;
  bufferlist etag_bl;
  if (rgw::sal::get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
    bde.meta.etag = etag_bl.to_str();
  } else {
    bde.meta.etag = synthesize_etag(stx);
  }

  if (flags & FLAG_LIST_VERSIONS) {
    bde.key.instance = recorded_version_id(get_fd(), xattr_strategy,
					   xattr_fallback);
    bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                rgw_bucket_dir_entry::FLAG_CURRENT;
    std::string dm_xattr = xattr_strategy->disk_name(RGW_NSFS_ATTR_DELETE_MARKER);
    char dm_buf[8];
    ssize_t dm_len = nsfs::fgetxattr(get_fd(), dm_xattr.c_str(),
                                 dm_buf, sizeof(dm_buf));
    if (dm_len > 0) {
      bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
    }
  }

  return 0;
}

int File::create(const DoutPrefixProvider *dpp, optional_yield y,
		   bool* existed, bool temp_file)
{
  int flags, ret;
  std::string path;
  if(temp_file) {
    flags = O_TMPFILE | O_RDWR;
    path = ".";
  } else {
    flags = O_CREAT | O_RDWR;
    path = get_name();
  }

  direct_io = ctx->_conf.get_val<bool>("rgw_nsfs_direct_io");
  if (direct_io) {
    flags |= O_DIRECT;
  }

  /* NooBaa's object mode, 0666 under `rgw_nsfs_umask`, so 0660 by
   * default (DECISIONS.md 2026-10-11).  Asked for here, and restored
   * below where the process umask takes some of it -- every path that
   * creates a file needs it, not only those that write, so it is not
   * left to the write path.
   *
   * Under a personality the open goes through the ring, because
   * that is where the kernel decides access -- and under
   * impersonation it is also what gives the new file the right
   * owner.  With none, the plain syscall, unchanged. */
  const mode_t mode = nsfs::file_create_mode();
  bool raised = false;
  if (identity.active()) {
    ret = nsfs::open_as_or_raise(dpp, identity, access_model,
				 nsfs::AccessClass::data,
				 RAISED_CREATE_CAPS, parent->get_fd(),
				 path.c_str(), flags | O_NOFOLLOW, mode,
				 &raised);
  } else {
    ret = openat(parent->get_fd(), path.c_str(), flags | O_NOFOLLOW, mode);
    if (ret < 0) {
      ret = -errno;
    }
  }
  if (ret < 0) {
    ret = -ret;
    if (ret == EEXIST) {
      return 0;
    }
    ldpp_dout(dpp, 0) << "ERROR: could not create object " << get_name()
		      << " (flags 0x" << std::hex << flags << std::dec
		      << ", as uid " << identity.cred.uid << "): "
                      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  fd = ret;
  need_fsync = true;

  /* Made by the gateway, so its mode is the gateway's to set, and then
   * the object is the authenticated user's (DECISIONS.md 2026-10-20).
   * A raised O_CREAT that found the file there made nothing, and
   * leaves it. */
  struct statx cstx;
  const bool made_raised = raised && (statx_ids(fd, cstx) == 0) &&
    (cstx.stx_uid == ::geteuid());
  if (raised && !made_raised) {
    return 0;
  }
  ret = nsfs::apply_create_mode(dpp, fd, mode, y,
				made_raised ? FSIdentity::none() : identity);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not set the mode on " << get_name()
		      << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (made_raised) {
    return chown_raised(dpp, fd, "", identity.cred.uid, identity.cred.gid);
  }
  return 0;
}

int File::open(const DoutPrefixProvider* dpp)
{
  if (fd >= 0) {
    return 0;
  }

  direct_io = ctx->_conf.get_val<bool>("rgw_nsfs_direct_io");
  bool ro = read_only;
  if (nsfs::impersonation_enabled() && !identity.active()) {
    /* No identity, so this is the gateway opening an object it does
     * not own -- reading an ACL to authorise a request, or an xattr
     * to compose a listing row.  It holds CAP_DAC_READ_SEARCH and
     * nothing else by deliberate choice, so it must ask for a read
     * and stop there.  The data paths deny the request rather than
     * arrive here without an identity;  see NSFSReadOp::iterate(). */
    ro = true;
    /* and holding it is this thread's own business now:  no thread
     * starts with a capability effective, so the one that reads
     * without an identity takes it here, once */
    (void) nsfs::hold_thread_capabilities(dpp);
  }
  /* O_NOFOLLOW:  a File stands only for a regular file (get_ent()
   * and stat() refuse anything else), so a link here is the entry
   * swapped between the stat and this open -- refused rather than
   * followed, as the gateway with read-search would follow it to
   * anything it can read. */
  int flags = (ro ? O_RDONLY : O_RDWR) | O_NOFOLLOW |
	      (direct_io ? O_DIRECT : 0);

  int ret;
  if (identity.active()) {
    ret = open_as(dpp, identity, parent->get_fd(), fname.c_str(), flags,
		  S_IRWXU);
  } else {
    ret = openat(parent->get_fd(), fname.c_str(), flags, S_IRWXU);
    if (ret < 0) {
      ret = -errno;
    }
  }
  /* POSIX decides access here and nowhere later, so this line is
   * what explains a permission outcome:  no identity under
   * impersonation means the open ran as the gateway. */
  ldpp_dout(dpp, 20) << "nsfs: open " << get_name()
    << (ro ? " O_RDONLY" : " O_RDWR")
    << " as " << (identity.active() ? to_string(identity.key) : "the gateway")
    << " -> " << ret << dendl;
  if (ret == -ELOOP) {
    /* O_NOFOLLOW met a link swapped in after the stat:  not an object */
    ldpp_dout(dpp, 10) << "nsfs: " << get_name()
      << " is a symbolic link, which is not an object" << dendl;
    return -ENOENT;
  }
  if (ret < 0) {
    ret = -ret;
    ldpp_dout(dpp, 0) << "ERROR: could not open object " << get_name()
		      << " (flags 0x" << std::hex << flags << std::dec
		      << ", as uid " << identity.cred.uid
		      << ", read_only " << read_only << "): "
                      << cpp_strerror(ret) << dendl;
    return -ret;
    }

  fd = ret;

  if (!direct_io) {
    /* fadvise only tunes buffered-read readahead; O_DIRECT bypasses the
     * page cache entirely, so the hint would be a no-op syscall. */
    int read_fadvise = ctx->_conf.get_val<int64_t>("rgw_nsfs_read_fadvise");
    if (read_fadvise != POSIX_FADV_NORMAL) {
      ::posix_fadvise(fd, 0, 0, read_fadvise);
    }
  }

  return 0;
}

int File::close()
{
  if (fd < 0) {
    return 0;
  }

  if (need_fsync) {
    auto policy = rgw::posix::parse_sync_policy(
      ctx->_conf.get_val<std::string>("rgw_posix_sync_policy"));
    if (policy == rgw::posix::SyncPolicy::ALWAYS ||
        policy == rgw::posix::SyncPolicy::COMPLETE) {
      int ret = ::fdatasync(fd);
      if (ret < 0) {
        return ret;
      }
    }
    int write_fadvise = ctx->_conf.get_val<int64_t>("rgw_nsfs_write_fadvise");
    if (write_fadvise != POSIX_FADV_NORMAL) {
      ::posix_fadvise(fd, 0, 0, write_fadvise);
    }
    need_fsync = false;
  }

  int ret = ::close(fd);
  if(ret < 0) {
    return ret;
  }
  fd = -1;

  return 0;
}


int File::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = FSEnt::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  if (S_ISLNK(stx.stx_mode)) {
    /* Not an object, so not cached as one:  a publish replaces the link
     * and stats this entry again, and must see what it published. */
    ldpp_dout(dpp, 10) << "nsfs: " << get_name()
      << " is a symbolic link, which is not an object" << dendl;
    stat_done = false;
    exist = false;
    return -ENOENT;
  }
  if (!S_ISREG(stx.stx_mode)) {
    /* Not a file */
    ldpp_dout(dpp, 0) << "ERROR: " << get_name() << " is not a file" << dendl;
    return -EINVAL;
  }

  return 0;
}

int File::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  need_fsync = true;
  int64_t write_chunk_size =
    ctx->_conf.get_val<Option::size_t>("rgw_nsfs_write_chunk_size");
  int64_t left = bl.length();
  ssize_t ret;

  if (direct_io) {
    /* Never fcntl-off O_DIRECT on a shared fd; pad or RMW instead. */
    return posix_direct_write(fd, ofs, bl, dpp);
  }

  char* curp = bl.c_str();
  const bool positioned =
    ctx->_conf.get_val<uint64_t>("rgw_nsfs_put_iodepth") >= 2;
  int64_t woff = ofs;

  if (!positioned) {
    ret = lseek(fd, ofs, SEEK_SET);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
        << ofs << " :" << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  while (left > 0) {
    int64_t want = (write_chunk_size > 0) ? std::min(left, write_chunk_size) : left;
    if (positioned) {
      ret = ::pwrite(fd, curp, want, woff);
    } else {
      ret = ::write(fd, curp, want);
    }
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not write object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    curp += ret;
    woff += ret;
    left -= ret;
  }

  return 0;
}

int File::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y,
		      int use_fd)
{
  /* Open, or say why not.
   *
   * This read the member descriptor without checking it, so a
   * failed open earlier in the request left `fd` at -1 and the
   * pread that followed returned EBADF -- which reached the client
   * as a 500.  The filesystem had said EACCES, which is a 403:  a
   * permission the tree does not grant, reported as a gateway
   * failure.  Measured 2026-10-16, eight acl tests, reachable once
   * the authorisation pass stopped denying first. */
  if ((use_fd < 0) && (fd < 0)) {
    const int oret = open(dpp);
    if (oret < 0) {
      return oret;
    }
  }
  /* the caller's descriptor where it gave one */
  const int rfd = (use_fd >= 0) ? use_fd : fd;

  int64_t read_chunk_size =
    ctx->_conf.get_val<Option::size_t>("rgw_nsfs_read_chunk_size");
  if (read_chunk_size <= 0) {
    read_chunk_size = READ_SIZE;
  }
  int64_t len = std::min(left, read_chunk_size);
  ssize_t ret;

  if (direct_io) {
    /* O_DIRECT requires the read offset, length, and buffer address to be
     * aligned to the filesystem block size.  Round the requested window
     * out to DIRECT_IO_ALIGN and slice the exact bytes back out of the
     * aligned buffer, so callers can keep passing arbitrary ranges. */
    int64_t aligned_ofs = ofs & ~(DIRECT_IO_ALIGN - 1);
    int64_t front = ofs - aligned_ofs;
    int64_t aligned_len = ((front + len + DIRECT_IO_ALIGN - 1) /
                           DIRECT_IO_ALIGN) * DIRECT_IO_ALIGN;

    bufferptr bp(buffer::create_small_page_aligned(aligned_len));
    ret = ::pread(rfd, bp.c_str(), aligned_len, aligned_ofs);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not read object " << get_name() << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    int64_t got = std::min<int64_t>(std::max<int64_t>(ret - front, 0), len);
    if (got > 0) {
      bl.append(bp, front, got);
    }
    return got;
  }

  bufferptr bp(len);
  if (ctx->_conf.get_val<uint64_t>("rgw_nsfs_get_iodepth") >= 2) {
    ret = ::pread(rfd, bp.c_str(), len, ofs);
  } else {
    ret = lseek(rfd, ofs, SEEK_SET);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not seek object " << get_name() << " to "
                        << ofs << " :" << cpp_strerror(ret) << dendl;
      return -ret;
    }
    ret = ::read(rfd, bp.c_str(), len);
  }
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not read object " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  bl.append(bp, 0, ret);

  return ret;
}

/* Move this file's directory entry into dst_dir under dst_name.
 *
 * Directory::rename() exists beside this and is deliberately different:  it
 * uses RENAME_EXCHANGE when the destination is occupied, which is what the
 * shadow/publish flow wants.  An object rename wants POSIX replace
 * semantics, so this is a plain renameat(). */
int File::rename(const DoutPrefixProvider* dpp, optional_yield y,
		 Directory* dst_dir, const std::string& dst_name)
{
  if (! parent || ! dst_dir) {
    return -EINVAL;
  }
  int src_fd = parent->get_fd();
  if (src_fd < 0) {
    int ret = parent->open(dpp);
    if (ret < 0) {
      return ret;
    }
    src_fd = parent->get_fd();
  }
  int dst_fd = dst_dir->get_fd();
  if (dst_fd < 0) {
    int ret = dst_dir->open(dpp);
    if (ret < 0) {
      return ret;
    }
    dst_fd = dst_dir->get_fd();
  }
  if (src_fd < 0 || dst_fd < 0) {
    return -EIO;
  }

  /* as the entry's identity:  both directories are its */
  const int ret = nsfs::with_identity_or_raise(dpp, identity, access_model,
						 nsfs::AccessClass::data,
						 RAISED_KEYDIR_CAPS,
						 [&]() -> int {
    return (::renameat(src_fd, fname.c_str(), dst_fd, dst_name.c_str()) < 0)
	? -errno : 0;
  }, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: renameat " << fname << " -> " << dst_name
		      << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* keep the FSEnt consistent with where the file now is, as
   * Directory::rename() does -- a live ent pointing at a moved name is a
   * hazard for anything still holding it */
  parent = dst_dir;
  fname = dst_name;
  return 0;
}

int File::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      Directory* dst_dir, const std::string& dst_name,
                      const CopyHints& hints, bool src_as_gateway)
{
  /* remove any existing target */
  {
    std::unique_ptr<FSEnt> del;
    int ret = dst_dir->get_ent(dpp, y, dst_name, std::string(), del);
    if (ret == -ENOENT) {
      ret = unlink_if_link(dpp, dst_dir->get_identity(),
			   dst_dir->get_access_model(), dst_dir->get_fd(),
			   dst_name.c_str());
      if (ret < 0) {
	return ret;
      }
    } else if (ret >= 0) {
      /* a copy onto a directory is refused by the rule at
       * refuse_directory_name();  removing the directory would delete
       * every key under it */
      if (del->get_type() != ObjectType::FILE) {
	ldpp_dout(dpp, 4) << "nsfs: rejecting copy to " << dst_name
			  << ":  a directory holds its name" << dendl;
	return -ERR_KEY_PATH_CONFLICT;
      }
      /* as the authenticated user, whom copy_object() gave this entry:  the
       * bucket directory is not the gateway's (raise site 4), and a
       * leaf directory resolve_key_path() did not make carries no
       * identity of its own */
      del->set_identity(identity);
      del->set_access_model(access_model);
      ret = del->remove(dpp, y, /*delete_children=*/true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not remove dest " << dst_name
                          << dendl;
        return ret;
      }
    }
  }

  int src_dir_fd = parent->get_fd();
  if (src_dir_fd < 0) {
    parent->open(dpp);
    src_dir_fd = parent->get_fd();
  }
  int dst_dir_fd = dst_dir->get_fd();
  if (dst_dir_fd < 0) {
    dst_dir->open(dpp);
    dst_dir_fd = dst_dir->get_fd();
  }

  /* as this entry's identity:  it reads the source and
   * owns the copy -- except that the access model may let the gateway
   * read a source the filesystem denies them, which the caller has
   * resolved */
  return fs_strategy->clone_file(dpp, src_dir_fd, get_name(),
                                 dst_dir_fd, dst_name, /*excl=*/y, hints,
                                 false, identity, src_as_gateway);
}

int File::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  if (!exists()) {
    return 0;
  }

  int parent_fd = parent->get_fd();
  /* as the entry's identity:  its directory is the identity's */
  int ret = nsfs::with_identity_or_raise(dpp, identity, access_model,
					   nsfs::AccessClass::data,
					   RAISED_KEYDIR_CAPS, [&]() -> int {
    return (::unlinkat(parent_fd, fname.c_str(), 0) < 0) ? -errno : 0;
  }, y);
  if ((ret < 0) && (ret != -ENOENT)) {
    ldpp_dout(dpp, 0) << "ERROR: could not remove object " << get_name()
		      << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (fs_strategy) {
    fs_strategy->cleanup_clone(dpp, parent_fd, fname);
  }

  return 0;
}

int File::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y,
			 std::string temp_fname, bool excl, bool as_gateway)
{
  if (fd < 0) {
    return 0;
  }

  /* As the gateway only where the caller says the access model allows
   * it (NSFSObject::link_temp_file), which raises CAP_FOWNER for it:
   * the publish links the identity's O_TMPFILE into the directory,
   * and fs.protected_hardlinks allows that only to the inode's owner.
   * Narrow on purpose -- the open() and stat() below must stay the
   * identity's, because they cache a descriptor on the entry and a
   * gateway-opened one outliving this call is the defect
   * with_cross_account_retry() records. */
  const FSIdentity& who = as_gateway ? FSIdentity::none() : identity;
  int ret = excl
    ? fs_strategy->link_temp_file_excl(fd, parent->get_fd(), get_name(),
				       dpp, y, who)
    : fs_strategy->link_temp_file(fd, parent->get_fd(), get_name(),
				  dpp, y, who);
  if (ret < 0) {
    return ret;
  }

  /* note that open() and stat() return already sign-reversed result codes */
  ret = open(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: NSFSAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = stat(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: NSFSAtomicWriter failed closing file" << dendl;
    return ret;
  }

  return 0;
}

bool Directory::file_exists(std::string& name)
{
  struct statx nstx;
  int ret = nsfs::statx(fd, name.c_str(), AT_SYMLINK_NOFOLLOW, STATX_ALL,
			&nstx);

  return (ret >= 0);
}

int Directory::create(const DoutPrefixProvider* dpp, optional_yield y,
			 bool* existed, bool temp_file)
{
  if (temp_file) {
    ldpp_dout(dpp, 0) << "ERROR: cannot create directory with temp_file " << get_name() << dendl;
    return -EINVAL;
  }

  /* A key directory made by a walk is its creator's, at NooBaa's
   * mode (resolve_path()).  mkdir(2) masks the mode with the umask, so
   * the mode is then set outright, as the directory's new owner. */
  if (identity.active()) {
    const int at = parent ? parent->get_fd() : AT_FDCWD;
    const mode_t mode = nsfs::dir_create_mode();
    bool raised = false;
    const int ret = nsfs::with_identity_or_raise(dpp, identity, access_model,
						 nsfs::AccessClass::data,
						 RAISED_CREATE_CAPS,
						 [&]() -> int {
      if (::mkdirat(at, fname.c_str(), mode) < 0) {
	return -errno;
      }
      return nsfs::apply_create_mode_at(dpp, at, fname.c_str(), mode);
    }, y, &raised);
    if (ret == -EEXIST) {
      if (existed != nullptr) {
	*existed = true;
      }
      return 0;
    }
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not create directory " << get_name()
			<< " as uid " << identity.cred.uid << ": "
			<< cpp_strerror(-ret) << dendl;
      return ret;
    }
    if (raised) {
      return chown_raised_dir(dpp, at, fname.c_str());
    }
    return ret;
  }

  /* As the gateway, at NooBaa's mode.  A client's bucket directory is
   * then given to its owner (NSFSBucket::create(), raise site 4), so it
   * is the account's at 0770 with no sticky bit, as NooBaa's is
   * (DECISIONS.md 2026-10-15). */
  const mode_t dir_mode = nsfs::dir_create_mode();
  /* A bucket at a stored path has no parent entry, and its fname is the
   * whole path:  AT_FDCWD, as Directory::open() resolves one. */
  const int at = parent ? parent->get_fd() : AT_FDCWD;
  int ret = mkdirat(at, fname.c_str(), dir_mode);
  const bool made = (ret == 0);
  if (ret < 0) {
    ret = -errno;
  }
  if (ret < 0) {
    ret = -ret;			/* both branches yield -errno */
    if (ret != EEXIST) {
      if (dpp)
	ldpp_dout(dpp, 0) << "ERROR: could not create bucket " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (existed != nullptr) {
      *existed = true;
    }
  }

  /* mkdir(2) masks the mode with the process umask, so the mode is set
   * outright.  Only on a directory this call made:  on EEXIST the
   * directory is already somebody else's -- an adopted NooBaa bucket,
   * or one an operator placed -- and a create that found it there does
   * not change it. */
  if (made) {
    ret = nsfs::apply_create_mode_at(dpp, at, fname.c_str(), dir_mode);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not set the mode on " << get_name()
	<< ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return 0;
}

int Directory::open(const DoutPrefixProvider* dpp)
{
  if (fd >= 0) {
    return 0;
  }

  int pfd{AT_FDCWD};
  if (parent)
    pfd = parent->get_fd();

  /* As the walk's identity first.  Denied, the walk goes on as the
   * gateway only where the access model permits the path class;
   * under posix-strict the denial stands, as NooBaa's does. */
  if (identity.active()) {
    const int r = nsfs::open_as(dpp, identity, pfd, fname.c_str(),
				O_RDONLY | O_DIRECTORY | O_NOFOLLOW, 0);
    if (r >= 0) {
      fd = r;
      return 0;
    }
    if (!(access_model && access_model->permits(nsfs::AccessClass::path) &&
	  ((r == -EACCES) || (r == -EPERM)))) {
      ldpp_dout(dpp, 0) << "ERROR: could not open dir " << get_name()
			<< " as uid " << identity.cred.uid << ": "
			<< cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 10) << "nsfs: walk denied at " << get_name()
		       << " as uid " << identity.cred.uid
		       << ";  going on as the gateway" << dendl;
  }

  /* The gateway searching a tree it may not own -- the same
   * CAP_DAC_READ_SEARCH the no-identity object open takes, and taken
   * the same way, by the thread, once. */
  if (nsfs::impersonation_enabled()) {
    (void) nsfs::hold_thread_capabilities(dpp);
  }

  int ret = openat(pfd, fname.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not open dir " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }

  fd = ret;

  return 0;
}

int Directory::close()
{
  if (fd < 0) {
    return 0;
  }

  ::close(fd);
  fd = -1;

  return 0;
}

int Directory::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = FSEnt::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  if (!S_ISDIR(stx.stx_mode)) {
    /* Not a directory */
    ldpp_dout(dpp, 0) << "ERROR: " << get_name() << " is not a directory" << dendl;
    return -EINVAL;
  }

  return 0;
}

int Directory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  /* AT_FDCWD with no parent, as FSEnt::stat() has it:  a parentless
   * directory's name is an absolute path, which is how a bucket served
   * from a stored path is built. */
  const int at = parent ? parent->get_fd() : AT_FDCWD;
  /* A key directory pruned after a delete is its creator's.  A
   * removal with its children is a bucket's, which is the gateway's
   * (raise site 3). */
  if (identity.active() && !delete_children) {
    return nsfs::with_identity_or_raise(dpp, identity, access_model,
						nsfs::AccessClass::data,
						RAISED_KEYDIR_CAPS,
						[&]() -> int {
      return delete_directory(at, fname.c_str(), false, dpp,
			      reserved_or_none(reserved_names));
    }, y);
  }
  return delete_directory(at, fname.c_str(), delete_children, dpp,
			  reserved_or_none(reserved_names));
}

int Directory::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		     optional_yield y)
{
  return -EINVAL;
}

int Directory::read(int64_t ofs, int64_t left, bufferlist &bl,
                    const DoutPrefixProvider *dpp, optional_yield y,
                    int use_fd)
{
  return -EINVAL;
}

int Directory::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y,
                              std::string temp_fname, bool excl, bool)
{
  return -EINVAL;
}

template <typename F>
int Directory::for_each(const DoutPrefixProvider* dpp, const F& func)
{
  DIR* dir;
  struct dirent* entry;
  int ret;

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  /* fdopendir() takes ownership of its fd and closedir() closes it.
   * Callbacks (get_ent/fill_cache/statx/openat) also use Directory::fd, so
   * iterate on a dup'd descriptor and leave this->fd alone.  Using the same
   * fd for readdir and openat corrupts the directory stream and can skip
   * entries (e.g. multipart parts → InvalidPart on complete). */
  int dir_fd = ::dup(fd);
  if (dir_fd < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not dup dir fd " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    ret = errno;
    ::close(dir_fd);
    ldpp_dout(dpp, 0) << "ERROR: could not open dir " << get_name() << " for listing: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  rewinddir(dir);

  ret = 0;
  while ((entry = readdir(dir)) != NULL) {
    std::string_view vname(entry->d_name);

    if (vname == "." || vname == "..")
      continue;

    int r = func(entry->d_name);
    if (r < 0) {
      ret = r;
      break;
    }
  }

  if (ret == -EAGAIN) {
    /* Limit reached */
    ret = 0;
  }

  closedir(dir); /* closes dir_fd only; Directory::fd remains valid */
  return ret;
}

int Directory::rename(const DoutPrefixProvider* dpp, optional_yield y, Directory* dst_dir, std::string dst_name)
{
  int flags = 0;
  int ret;
  std::string src_name = fname;
  int parent_fd = parent->get_fd();

  if (dst_dir->file_exists(dst_name)) {
    flags = RENAME_EXCHANGE;
  }
  // swap
  ret = renameat2(parent_fd, src_name.c_str(), dst_dir->get_fd(), dst_name.c_str(), flags);
  if(ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: renameat2 for shadow object could not finish: "
	<< cpp_strerror(ret) << dendl;
    return -ret;
  }

  /* Parent of this dir is now dest dir */
  parent = dst_dir;
  /* Name has changed */
  fname = dst_name;

  // Delete old one (could be file or directory)
  struct statx stx;
  ret = nsfs::statx(parent_fd, src_name.c_str(), AT_SYMLINK_NOFOLLOW,
		  STATX_ALL, &stx);
  if (ret < 0) {
    ret = errno;
    if (ret == ENOENT) {
      return 0;
    }
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                  << cpp_strerror(ret) << dendl;
    return -ret;
  }

  if (S_ISREG(stx.stx_mode)) {
    ret = unlinkat(parent_fd, src_name.c_str(), 0);
  } else if (S_ISDIR(stx.stx_mode)) {
    ret = delete_directory(parent_fd, src_name.c_str(), true, dpp,
			   reserved_or_none(reserved_names));
  }
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove old file " << get_name()
                      << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int Directory::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      Directory* dst_dir, const std::string& dst_name,
                      const CopyHints& hints, bool src_as_gateway)
{
  int ret;

  // Delete the target
  {
    std::unique_ptr<FSEnt> del;
    ret = dst_dir->get_ent(dpp, y, dst_name, std::string(), del);
    if (ret == -ENOENT) {
      ret = unlink_if_link(dpp, dst_dir->get_identity(),
			   dst_dir->get_access_model(), dst_dir->get_fd(),
			   dst_name.c_str());
      if (ret < 0) {
	return ret;
      }
    } else if (ret >= 0) {
      /* a copy onto a directory is refused;  see File::copy() */
      if (del->get_type() != ObjectType::FILE) {
	ldpp_dout(dpp, 4) << "nsfs: rejecting copy to " << dst_name
			  << ":  a directory holds its name" << dendl;
	return -ERR_KEY_PATH_CONFLICT;
      }
      /* as the authenticated user, whom copy_object() gave this entry:  the
       * bucket directory is not the gateway's (raise site 4), and a
       * leaf directory resolve_key_path() did not make carries no
       * identity of its own */
      del->set_identity(identity);
      del->set_access_model(access_model);
      ret = del->remove(dpp, y, /*delete_children=*/true);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not remove dest " << dst_name
                          << dendl;
        return ret;
      }
    }
  }

  ret = dst_dir->open(dpp);
  std::unique_ptr<Directory> dest = clone_dir();
  dest->parent = dst_dir;
  dest->fname = dst_name;

  ret = dest->create(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not create dest " << dest->get_name() << dendl;
    return ret;
  }

  Attrs attrs;
  ret = read_attrs(dpp, y, attrs);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not read attrs from " << get_name() << dendl;
    return ret;
  }
  ret = dest->write_attrs(dpp, y, attrs, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not write attrs to " << dest->get_name() << dendl;
    return ret;
  }

  const auto& rn = reserved_or_none(reserved_names);
  ret = for_each(dpp, [this, &dest, &dpp, &y, &rn, &hints,
		       src_as_gateway](const char* name) {
    std::unique_ptr<FSEnt> sobj;

    if (is_reserved_name(name, rn)) {
      return 0;
    }

    int r = this->get_ent(dpp, y, name, std::string(), sobj);
    if ((r == -ENOENT) || (r == -EINVAL)) {
      /* not an object, or gone since readdir:  nothing to copy */
      return 0;
    }
    if (r < 0)
      return r;
    return sobj->copy(dpp, y, dest.get(), name, hints, src_as_gateway);
  });

  return ret;
}

int Directory::get_ent(const DoutPrefixProvider *dpp, optional_yield y, const std::string &name, const std::string& instance, std::unique_ptr<FSEnt>& ent)
{
  struct statx nstx;
  std::unique_ptr<FSEnt> nent;

  int ret = open(dpp);
  if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not open directory " << name << dendl;
      return ret;
  }

  ret = nsfs::statx(get_fd(), name.c_str(),
                  AT_SYMLINK_NOFOLLOW, STATX_ALL, &nstx);
  if (ret < 0) {
      ret = errno;
      /* Absence is a result here, not a failure.  This is the "does
       * this object exist" path -- a HEAD of something that is not
       * there, a GET before a PUT, and get_fsio_handle() statting an
       * object it is about to create, which discards the result for
       * exactly that reason.  Logging it at 0 put an ERROR in the log
       * for every such request. */
      ldpp_dout(dpp, (ret == ENOENT) ? 15 : 0)
          << ((ret == ENOENT) ? "no object " : "ERROR: could not stat object ")
          << name << " in dir " << get_name() << " : " << cpp_strerror(ret)
          << dendl;
      return -ret;
  }
  if (S_ISREG(nstx.stx_mode)) {
    nent = std::make_unique<File>(name, this, nstx, ctx);
  } else if (S_ISDIR(nstx.stx_mode)) {
    /* A staging directory is told from an ordinary one by its name,
     * which MPUStrategy owns.  This used to open the directory and read
     * an `object_type` attribute -- an openat and a full attribute read
     * per directory entry, to cache a fact the name already gives.
     * The attribute was inherited from the posix driver, could not work
     * on a NooBaa-format tree, and already fell back to DIRECTORY when
     * absent, which is what a natively created directory has. */
    ObjectType type{mpu_strategy->is_staging_dir(name)
		      ? ObjectType::MULTIPART : ObjectType::DIRECTORY};

    switch (type.type) {
    case ObjectType::MULTIPART:
      nent = std::make_unique<MPDirectory>(name, this, nstx, ctx);
      break;
    case ObjectType::DIRECTORY:
      nent = std::make_unique<Directory>(name, this, nstx, ctx);
      break;
    default:
      ldpp_dout(dpp, 0) << "ERROR: invalid type " << type << dendl;
      return -EINVAL;
    }
  } else if (S_ISLNK(nstx.stx_mode)) {
    /* not an object:  no object is at this name */
    ldpp_dout(dpp, 10) << "nsfs: " << name << " in dir " << get_name()
      << " is a symbolic link, which is not an object" << dendl;
    return -ENOENT;
  } else {
    return -EINVAL;
  }

  ent.swap(nent);
  return 0;
}

bool Directory::directory_object(const DoutPrefixProvider* dpp,
				 PathStrategy::DirectoryObject& out)
{
  if (open(dpp) < 0) {
    return false;
  }
  if (path_strategy && path_strategy->directory_object(dpp, get_fd(), out)) {
    return true;
  }
  return path_fallback &&
	 path_fallback->directory_object(dpp, get_fd(), out);
}

NooBaaDirectoryObject::NooBaaDirectoryObject(
    const std::string& sentinel, Directory* dir,
    const PathStrategy::DirectoryObject& _dobj, CephContext* _ctx)
  : FSEnt(sentinel, dir, _ctx), dobj(_dobj)
{
  if (dobj.content_in_sentinel) {
    content = std::make_unique<File>(sentinel, dir, _ctx);
  }
}

NooBaaDirectoryObject::NooBaaDirectoryObject(const NooBaaDirectoryObject& o)
  : FSEnt(o), dobj(o.dobj)
{
  if (o.content) {
    content = o.content->clone();
  }
}

int NooBaaDirectoryObject::open(const DoutPrefixProvider* dpp)
{
  int ret = parent->open(dpp);
  if (ret < 0) {
    return ret;
  }
  if (!content) {
    return 0;
  }
  /* the access this entry was given, which the caller set on it and not
   * on the sentinel */
  content->set_identity(identity);
  content->set_read_only(read_only);
  ret = content->open(dpp);
  if (ret < 0) {
    return ret;
  }
  fd = content->get_fd();
  return 0;
}

int NooBaaDirectoryObject::close()
{
  if (content) {
    content->close();
  }
  fd = -1;
  return 0;
}

/* NooBaa's own choice of stat (`namespace_fs.js` read_object_md):  the
 * sentinel's when the object has content, the directory's when it is
 * empty, with the size from the marker either way.  Reported as a
 * regular file, which is what the object is to every caller. */
int NooBaaDirectoryObject::stat(const DoutPrefixProvider* dpp, bool force)
{
  if (force) {
    stat_done = false;
  }
  if (stat_done) {
    return 0;
  }

  /* The directory's stat afresh:  for the root object the directory is
   * the bucket's own, a long-lived entry whose stat was taken when the
   * bucket was loaded and is stale once anything at the top level has
   * changed. */
  int ret = content ? content->stat(dpp, force)
		    : parent->stat(dpp, /*force=*/true);
  if (ret < 0) {
    exist = false;
    return ret;
  }
  stx = content ? content->get_stx() : parent->get_stx();
  stx.stx_mode = (stx.stx_mode & ~S_IFMT) | S_IFREG;
  stx.stx_size = dobj.size;
  exist = true;
  stat_done = true;
  return 0;
}

int NooBaaDirectoryObject::read(int64_t ofs, int64_t left, bufferlist& bl,
				const DoutPrefixProvider* dpp, optional_yield y,
				int use_fd)
{
  if (!content) {
    return 0;
  }
  if (content->get_fd() < 0) {
    int ret = open(dpp);
    if (ret < 0) {
      return ret;
    }
  }
  return content->read(ofs, left, bl, dpp, y);
}

int NooBaaDirectoryObject::read_attrs(const DoutPrefixProvider* dpp,
				      optional_yield y, Attrs& attrs,
				      int use_fd, std::vector<std::string>* unknown)
{
  /* a sentinel's attributes live on its parent directory, which this
   * opens itself;  a caller's descriptor is for the object and is not
   * what this reads */
  (void) use_fd;
  int ret = parent->open(dpp);
  if (ret < 0) {
    return ret;
  }
  return get_x_attrs(y, dpp, parent->get_fd(), attrs, parent->get_name(),
		     xattr_strategy, xattr_fallback, identity, "user.nsfs.",
		     unknown);
}

int NooBaaDirectoryObject::copy(const DoutPrefixProvider* dpp, optional_yield y,
				Directory* dst_dir, const std::string& name,
				const CopyHints& hints, bool src_as_gateway)
{
  if (content) {
    return content->copy(dpp, y, dst_dir, name, hints, src_as_gateway);
  }

  /* An empty object copies to an empty file.  The destination's
   * attributes are written by the caller, as for any copy. */
  std::unique_ptr<FSEnt> del;
  int ret = dst_dir->get_ent(dpp, y, name, std::string(), del);
  if (ret == -ENOENT) {
    ret = unlink_if_link(dpp, dst_dir->get_identity(),
			 dst_dir->get_access_model(), dst_dir->get_fd(),
			 name.c_str());
    if (ret < 0) {
      return ret;
    }
  } else if (ret >= 0) {
    /* a copy onto a directory is refused;  see File::copy() */
    if (del->get_type() != ObjectType::FILE) {
      ldpp_dout(dpp, 4) << "nsfs: rejecting copy to " << name
			<< ":  a directory holds its name" << dendl;
      return -ERR_KEY_PATH_CONFLICT;
    }
    /* as the authenticated user;  see File::copy() */
    del->set_identity(identity);
    del->set_access_model(access_model);
    ret = del->remove(dpp, y, /*delete_children=*/true);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove dest " << name << dendl;
      return ret;
    }
  }
  File dst(name, dst_dir, ctx);
  return dst.create(dpp, y);
}

/* Changing such an object has to convert it -- move it into the nsfs
 * format and clear NooBaa's marker -- or the change is read back
 * through the marker as if it had not happened.  Not built yet, so a
 * change is rejected rather than half made. */
static int reject_noobaa_directory_object_change(const DoutPrefixProvider* dpp,
						 const std::string& dir)
{
  ldpp_dout(dpp, 1) << "nsfs: rejecting a change to the directory object at "
    << dir << ":  it is in NooBaa's format, and converting it on write "
    << "is not built" << dendl;
  return -EOPNOTSUPP;
}

int NooBaaDirectoryObject::create(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  bool* existed, bool temp_file)
{
  return reject_noobaa_directory_object_change(dpp, parent->get_name());
}

int NooBaaDirectoryObject::remove(const DoutPrefixProvider* dpp,
				  optional_yield y, bool delete_children)
{
  return reject_noobaa_directory_object_change(dpp, parent->get_name());
}

int NooBaaDirectoryObject::write(int64_t ofs, bufferlist& bl,
				 const DoutPrefixProvider* dpp, optional_yield y)
{
  return reject_noobaa_directory_object_change(dpp, parent->get_name());
}

int NooBaaDirectoryObject::write_attrs(const DoutPrefixProvider* dpp,
				       optional_yield y, Attrs& attrs,
				       Attrs* extra_attrs, AttrWriteMode mode,
				       const std::vector<std::string>* rmattrs,
				       int use_fd)
{
  return reject_noobaa_directory_object_change(dpp, parent->get_name());
}

int NooBaaDirectoryObject::link_temp_file(const DoutPrefixProvider* dpp,
					  optional_yield y,
					  std::string target_fname, bool excl,
					  bool)
{
  return reject_noobaa_directory_object_change(dpp, parent->get_name());
}

int Directory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                          fill_cache_cb_t &cb, uint32_t flags,
                          const ACLOwner& bucket_owner,
                          const std::string& path_prefix)
{
  const auto& rn = reserved_or_none(reserved_names);
  int ret = for_each(dpp, [this, &cb, &dpp, &y, &path_prefix, flags,
			   &bucket_owner, &rn](const char *name) {
    std::unique_ptr<FSEnt> ent;

    /* A directory-object sentinel is reserved but listed, as the
     * directory's object -- except at the bucket root.  The root's is
     * the root object, the key `/`, and NooBaa never lists it
     * (DECISIONS.md 2026-10-13). */
    if (is_reserved_name(name, rn) &&
	(!path_strategy->names_directory_object(name) || path_prefix.empty())) {
      return 0;
    }

    /* The sentinel of a directory NooBaa's format marks is that
     * object's data and not the object:  the walk one level up emitted
     * it, from the directory. */
    if (path_strategy->names_directory_object(name)) {
      PathStrategy::DirectoryObject dobj{};
      if (directory_object(dpp, dobj)) {
	return 0;
      }
    }

    int ret = get_ent(dpp, y, name, std::string(), ent);
    if ((ret == -ENOENT) || (ret == -EINVAL)) {
      /* Not an object -- a link, or not a file or a directory -- or
       * removed since readdir returned it.  Ending the walk here would
       * end the listing here, since the cache keeps what it has. */
      ldpp_dout(dpp, 10) << "nsfs: listing skips " << name << " in "
	<< get_name() << ": " << cpp_strerror(-ret) << dendl;
      return 0;
    }
    if (ret < 0)
      return ret;

    ent->stat(dpp);

    if (path_strategy->names_directory_object(name)) {
      /* The directory's object, emitted with key = path_prefix, which is
       * already "photos/" inside photos/.  Built by make_dir_entry(), as
       * every other object's row is:  a row built here by hand left out
       * the version identity, so in a versioned bucket a directory object
       * listed as no version at all. */
      rgw_bucket_dir_entry bde{};
      ret = ent->make_dir_entry(dpp, y, path_strategy->key_from_name(path_prefix),
				flags, bucket_owner, bde);
      if (ret < 0)
        return ret;

      return cb(dpp, bde);
    }

    if (ent->get_type() == ObjectType::DIRECTORY) {
      Directory* subdir = static_cast<Directory*>(ent.get());
      ret = subdir->open(dpp);
      if (ret < 0)
        return ret;

      /* A directory object in NooBaa's format, empty or not:  the
       * directory carries the marker and the object's attributes, so it
       * is emitted here, from the directory, and its sentinel is skipped
       * on the way in.
       *
       * One fgetxattr, on the descriptor just opened for the recursion,
       * and only where a strategy marks directories -- a base bucket, or
       * one mid-conversion.  The nsfs strategy answers without a
       * syscall. */
      PathStrategy::DirectoryObject dobj{};
      if (subdir->directory_object(dpp, dobj)) {
	NooBaaDirectoryObject dent(path_strategy->folder_object_name(),
				   subdir, dobj, ctx);
	rgw_bucket_dir_entry bde{};
	ret = dent.stat(dpp);
	if (ret == 0) {
	  ret = dent.make_dir_entry(
	      dpp, y, path_strategy->key_from_name(path_prefix + name + "/"),
	      flags, bucket_owner, bde);
	}
	if (ret < 0) {
	  /* marked as holding content with no sentinel to hold it:
	   * NooBaa cannot read it either */
	  ldpp_dout(dpp, 1) << "nsfs: not listing the directory object "
	    << path_prefix << name << "/:  " << cpp_strerror(-ret) << dendl;
	} else {
	  ret = cb(dpp, bde);
	  if (ret < 0) {
	    return ret;
	  }
	}
      }

      return subdir->fill_cache(dpp, y, cb, flags, bucket_owner,
                                path_prefix + name + "/");
    }

    ret = ent->fill_cache(dpp, y, cb, flags, bucket_owner, path_prefix);
    if (ret < 0)
      return ret;
    return 0;
  });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list directory " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return ret;
  }

  /* enumerate .versions/ for versioned buckets */
  if (flags & FSEnt::FLAG_LIST_VERSIONS) {
    /* the gateway's listing, in a `.versions` that may be an
     * identity's:  read-search at the call */
    if (nsfs::impersonation_enabled()) {
      (void) nsfs::hold_thread_capabilities(dpp);
    }
    int vfd = ::openat(get_fd(), HIDDEN_VERSIONS_PATH.c_str(),
                       O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
    if (vfd >= 0) {
      /* Notice an interrupted move;  do not repair it.  Repair needs the
       * version lock, and taking it here would put a mutation on the read
       * path and create a bucket-cache-entry-lock -> version-lock ordering
       * that exists nowhere else.  The next write to this directory recovers
       * it -- see version_lock_recovering().  Until then this listing is
       * where the delta shows, so it is worth saying so. */
      std::string irec;
      if (rename_intent_present_at(xattr_strategy, vfd, irec)) {
        RenameIntent in;
        if (parse_rename_intent(irec, in)) {
          ldpp_dout(dpp, 0) << "WARNING: listing " << get_name()
            << ": an interrupted move of " << in.leaf << " to " << in.bucket
            << "/" << in.key << " has not been recovered;  this listing may "
            << "report its history incompletely.  The next write to this "
            << "directory will repair it" << dendl;
        } else {
          ldpp_dout(dpp, 0) << "WARNING: listing " << get_name()
            << ": .versions/ carries an unparseable rename_intent record"
            << dendl;
        }
      }
      DIR* vdir = fdopendir(vfd);
      if (vdir) {
        struct dirent* de;
        while ((de = readdir(vdir)) != nullptr) {
          /* no dot filter:  the parse below rejects ".", ".." and
           * .lock, and filtering on the dot would hide the versions of
           * an object whose own name begins with one */
          std::string vname(de->d_name);
          /* parse "key_version_id" — find the last '_mtime-' or '_null' */
          std::string obj_name;
          std::string ver_id;
          auto null_pos = vname.rfind("_null");
          if (null_pos != std::string::npos &&
              null_pos + 5 == vname.size()) {
            obj_name = vname.substr(0, null_pos);
            ver_id = NULL_VERSION_ID;
          } else {
            auto mtime_pos = vname.rfind("_mtime-");
            if (mtime_pos != std::string::npos) {
              obj_name = vname.substr(0, mtime_pos);
              ver_id = vname.substr(mtime_pos + 1);
            } else {
              continue;
            }
          }

          struct statx vstx;
          if (nsfs::statx(vfd, de->d_name, AT_SYMLINK_NOFOLLOW,
                    STATX_ALL, &vstx) < 0) {
            continue;
          }

          /* a directory object's versions are its sentinel's, and list
           * under the directory's key, as its current entry does above;
           * the root's are the root object's, which is never listed */
          std::string full_key = path_prefix + obj_name;
          if (path_strategy->names_directory_object(obj_name)) {
            if (path_prefix.empty()) {
              continue;
            }
            full_key = path_prefix;
          }

          rgw_bucket_dir_entry bde{};
          /* obj_name came off a file name, so it has to go back through
           * the path strategy to become a key -- every other path fills
           * bde.key from get_index_key(), and a cache entry keyed the
           * other way is one the removal path cannot find.  The two
           * agreed only while both doubled a leading underscore. */
          rgw_obj_key vkey = path_strategy->key_from_name(full_key);
          vkey.get_index_key(&bde.key);
          bde.key.instance = ver_id;
          bde.ver.pool = 1;
          bde.ver.epoch = 1;
          bde.exists = true;
          bde.flags = rgw_bucket_dir_entry::FLAG_VER;

          /* check delete marker */
          int tfd = ::openat(vfd, de->d_name, O_RDONLY | O_NOFOLLOW);
          if (tfd >= 0) {
            char buf[8];
            std::string dm_xattr = xattr_strategy->disk_name(RGW_NSFS_ATTR_DELETE_MARKER);
            ssize_t xlen = nsfs::fgetxattr(tfd, dm_xattr.c_str(), buf,
                                           sizeof(buf));
            if (xlen > 0) {
              bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
              /* FLAG_CURRENT for orphaned DMs (no top-level file) is
               * resolved by a fixup pass over LMDB after fill_cache
               * completes — see bucket_cache.h fixup_current_flags() */
            }

            Attrs attrs;
            ret = get_x_attrs(y, dpp, tfd, attrs, vname, xattr_strategy,
			      xattr_fallback);

            ACLOwner acl_owner;
            set_dir_entry_owner(bde,
                                xattr_strategy->object_owner(attrs, acl_owner),
                                acl_owner, bucket_owner);

            bufferlist etag_bl;
            if (rgw::sal::get_attr(attrs, RGW_ATTR_ETAG, etag_bl)) {
              bde.meta.etag = etag_bl.to_str();
            }

            /* Set here, with the other attribute-derived fields,
             * because `attrs` does not outlive this block.  A version
             * entry we could not open keeps the default below. */
            bde.meta.storage_class = listed_storage_class(attrs);

            ::close(tfd);
          }

          bde.meta.category = RGWObjCategory::Main;
          bde.meta.size = vstx.stx_size;
          bde.meta.accounted_size = vstx.stx_size;
          bde.meta.mtime = from_statx_timestamp(vstx.stx_mtime);
          if (bde.meta.storage_class.empty()) {
            bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
          }

          if (bde.meta.etag.empty()) {
            bde.meta.etag = synthesize_etag(vstx);
          }

          ret = cb(dpp, bde);
          if (ret < 0) {
            break;
          }
        }
        closedir(vdir);
      } else {
        ::close(vfd);
      }
    }
  }

  return 0;
}

int MPDirectory::create(const DoutPrefixProvider* dpp, optional_yield y,
			 bool* existed, bool temp_file)
{
  std::string path;

  if(temp_file) {
    tmpname = path = "._tmpname_" +
           std::to_string(ceph::util::generate_random_number<uint64_t>());
  } else {
    path = get_name();
  }

  const mode_t dir_mode = nsfs::dir_create_mode();

  /* Staging is its uploader's, as NooBaa makes it
   * (`namespace_fs.js:1785`, DECISIONS.md 2026-10-23).  Where the
   * access model performs a denied create, the gateway gives the
   * directory to the uploader, as it gives a raised object to its
   * authenticated user. */
  if (identity.active()) {
    const int pfd = parent->get_fd();
    bool raised = false;
    const int ret = nsfs::with_identity_or_raise(dpp, identity, access_model,
						 nsfs::AccessClass::data,
						 RAISED_CREATE_CAPS,
						 [&]() -> int {
      if (::mkdirat(pfd, path.c_str(), dir_mode) < 0) {
	return -errno;
      }
      return nsfs::apply_create_mode_at(dpp, pfd, path.c_str(), dir_mode);
    }, y, &raised);
    if (ret == -EEXIST) {
      if (existed != nullptr) {
	*existed = true;
      }
      return 0;
    }
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not create staging " << get_name()
			<< " as uid " << identity.cred.uid << ": "
			<< cpp_strerror(-ret) << dendl;
      return ret;
    }
    if (raised) {
      return chown_raised(dpp, pfd, path.c_str(), identity.cred.uid,
			  identity.cred.gid);
    }
    return 0;
  }

  int ret = mkdirat(parent->get_fd(), path.c_str(), dir_mode);
  const bool made = (ret == 0);
  if (ret < 0) {
    ret = -errno;
  }
  if (ret < 0) {
    ret = -ret;			/* both branches yield -errno */
    if (ret != EEXIST) {
      if (dpp)
	ldpp_dout(dpp, 0) << "ERROR: could not create bucket " << get_name() << ": "
	  << cpp_strerror(ret) << dendl;
      return -ret;
    } else if (existed != nullptr) {
      *existed = true;
    }
  }

  /* see Directory::create:  the mode is set outright, and only on a
   * directory this call made */
  if (made) {
    ret = nsfs::apply_create_mode_at(dpp, parent->get_fd(), path.c_str(),
				     dir_mode);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not set the mode on " << get_name()
	<< ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  return 0;
}

int MPDirectory::read(int64_t ofs, int64_t left, bufferlist &bl,
                    const DoutPrefixProvider *dpp, optional_yield y,
		      int use_fd)
{
  std::string pname;
  for (auto part : parts) {
    if (ofs < part.second) {
      pname = part.first;
      break;
    }

    ofs -= part.second;
  }

  if (pname.empty()) {
    // ofs is past the end
    return 0;
  }

  if (!cur_read_part || cur_read_part->get_name() != pname) {
    cur_read_part = std::make_unique<File>(pname, this, ctx);
  }
  int ret = cur_read_part->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return cur_read_part->read(ofs, left, bl, dpp, y);
}

int MPDirectory::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y,
                                std::string temp_fname, bool excl, bool)
{
  if (tmpname.empty()) {
    return 0;
  }

  /* Temporarily change name to tmpname, so we can reuse rename() */
  std::string savename = fname;
  fname = tmpname;
  tmpname.clear();

  return rename(dpp, y, parent, savename);
}

int MPDirectory::remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children)
{
  return Directory::remove(dpp, y, /*delete_children=*/true);
}

int MPDirectory::stat(const DoutPrefixProvider* dpp, bool force)
{
  int ret = Directory::stat(dpp, force);
  if (ret < 0) {
    return ret;
  }

  uint64_t total_size{0};
  for_each(dpp, [this, &total_size, &dpp](const char *name) {
    int ret;
    struct statx stx;
    std::string sname = name;

    if (! mpu_strategy->part_number(sname)) {
      /* Skip non-parts */
      return 0;
    }

    ret = nsfs::statx(fd, name, AT_SYMLINK_NOFOLLOW, STATX_ALL, &stx);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": "
                        << cpp_strerror(ret) << dendl;
      return -ret;
    }

    if (!S_ISREG(stx.stx_mode)) {
      /* Skip non-files */
      return 0;
    }

    parts[name] = stx.stx_size;
    total_size += stx.stx_size;
    return 0;
  });

  stx.stx_size = total_size;

  return 0;
}


std::unique_ptr<File> MPDirectory::get_part_file(int partnum)
{
  std::string partname = mpu_strategy->part_name(partnum);
  rgw_obj_key part_key(partname);

  return std::make_unique<File>(partname, this, ctx);
}

int MPDirectory::fill_cache(const DoutPrefixProvider *dpp, optional_yield y,
                            fill_cache_cb_t &cb, uint32_t flags,
                            const ACLOwner& bucket_owner,
                            const std::string& path_prefix)
{
  int ret = FSEnt::fill_cache(dpp, y, cb, FSEnt::FLAG_NONE, bucket_owner,
			      path_prefix);
  if (ret < 0)
    return ret;

  return Directory::fill_cache(dpp, y, cb, FSEnt::FLAG_NONE, bucket_owner,
			       path_prefix);
}

} // namespace nsfs


bool NSFSZoneGroup::placement_target_exists(std::string& target) const {
  return !!group->placement_targets.count(target);
}

void NSFSZoneGroup::get_placement_target_names(std::set<std::string>& names) const {
  for (const auto& target : group->placement_targets) {
    names.emplace(target.second.name);
  }
}

ZoneGroup& NSFSZone::get_zonegroup() {
  return *zonegroup;
}

const RGWZoneParams& NSFSZone::get_rgw_params() {
  return *zone_params;
}

const std::string& NSFSZone::get_id() {
  return zone_params->get_id();
}

const std::string& NSFSZone::get_name() const {
  return zone_params->get_name();
}

bool NSFSZone::is_writeable() {
  return true;
}

bool NSFSZone::get_redirect_endpoint(std::string* endpoint) {
  return false;
}

const std::string& NSFSZone::get_current_period_id() {
  return current_period->get_id();
}

const RGWAccessKey& NSFSZone::get_system_key() {
  return zone_params->system_key;
}

const std::string& NSFSZone::get_realm_name() {
  return realm->get_name();
}

const std::string& NSFSZone::get_realm_id() {
  return realm->get_id();
}

RGWBucketSyncPolicyHandlerRef NSFSZone::get_sync_policy_handler() {
  return nullptr;
}

int NSFSLuaManager::get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script)
{
  return -ENOENT;
}

std::tuple<rgw::lua::LuaCodeType, int> NSFSLuaManager::get_script_or_bytecode(const DoutPrefixProvider* dpp, optional_yield y,
                                                                               const std::string& key)
{
  return std::make_tuple("", -ENOENT);
}

int NSFSLuaManager::put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script)
{
  return -ENOENT;
}

int NSFSLuaManager::del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key)
{
  return -ENOENT;
}

int NSFSLuaManager::add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
{
  return -ENOENT;
}

int NSFSLuaManager::remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
{
  return -ENOENT;
}

int NSFSLuaManager::list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages)
{
  return -ENOENT;
}

int NSFSLuaManager::reload_packages(const DoutPrefixProvider* dpp, optional_yield y)
{
  return -ENOENT;
}

/* Build every strategy implementation, the profiles which select among
 * them, and the union of names they reserve.
 *
 * Separate from initialize() so that a test can construct a driver
 * without the rest of it -- the configuration, the caches, the
 * frontends -- and still have a driver whose strategies are not null.
 * Factored rather than reproduced:  the selection is what S5 changes,
 * and a second copy of it drifts from the first.
 *
 * base_path must already be set;  the extents probe reads it.
 */
void NSFSDriver::init_strategies(const DoutPrefixProvider* dpp,
				 std::optional<bool> shares_extents)
{
  auto gpfs_lib = g_conf().get_val<std::string>("rgw_nsfs_gpfs_lib_path");
  bool gpfs_clone = g_conf().get_val<bool>("rgw_nsfs_gpfs_clone_files");
  bool gpfs_lwe = g_conf().get_val<bool>("rgw_nsfs_gpfs_lwe_locking");
  bool gpfs_batch = g_conf().get_val<bool>("rgw_nsfs_gpfs_batch_xattrs");
  fs_strategy = nsfs::GPFSStrategy::try_create(dpp, gpfs_lib,
					       fs_reference_path(),
                                               gpfs_clone, gpfs_lwe,
                                               gpfs_batch);
  if (!fs_strategy) {
    fs_strategy = std::make_unique<nsfs::POSIXStrategy>();
  }
  ldpp_dout(dpp, 1) << "nsfs: using " << fs_strategy->name()
    << " fs strategy" << dendl;
  /* Probed unless the caller says otherwise.  The strided layout, the
   * extent guard and the divert path are reached only where a copy is
   * real, which on a developer machine is nowhere -- so a caller which
   * means to exercise them has to be able to say so. */
  fs_strategy->set_shares_extents(
      shares_extents ? *shares_extents
		     : nsfs::probe_shares_extents(dpp, fs_reference_path()));

  /* Which staging layout.
   *
   * Assembling from per-part files is nearly free where the filesystem
   * shares extents and costs a copy of the whole object where it does
   * not -- GPFS returns EOPNOTSUPP at every granularity, and ext4 and
   * tmpfs have no reflink at all.  So writing each part straight to its
   * final offset is worth its complexity exactly where the copy is
   * real, and FSStrategy has just measured that.
   *
   * A capability question, not a vendor one:  link() is plain POSIX and
   * nothing here names a filesystem.  It chooses between OUR two
   * layouts and nothing else:  a base bucket stages as NooBaa does,
   * because base is NooBaa's format. */
  if (fs_strategy->shares_extents()) {
    mpu_strategy = std::make_unique<nsfs::PerPartMPUStrategy>();
  } else {
    mpu_strategy = std::make_unique<nsfs::StridedMPUStrategy>();
  }
  ldpp_dout(dpp, 1) << "nsfs: using " << mpu_strategy->name()
    << " multipart staging layout" << dendl;

  /* one implementation each;  the second of each is selected by the
   * bucket's recorded format and is not written yet */
  xattr_strategy = std::make_unique<nsfs::PrefixedXattrStrategy>();
  path_strategy = std::make_unique<nsfs::SentinelPathStrategy>();
  bucket_state_strategy = std::make_unique<nsfs::RgwBucketStateStrategy>();

  /* NooBaa's bucket records, if given the address of a store.
   * Unset means do not consult one, which is every deployment that is
   * not reading a NooBaa tree. */
  {
    const auto& nb_root =
      g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
    if (!nb_root.empty()) {
      nb_bucket_state_strategy =
	std::make_unique<nsfs::NooBaaBucketStateStrategy>(nb_root,
							  userDB.get());
      ldpp_dout(dpp, 1) << "nsfs: reading NooBaa bucket state from "
	<< nb_root << dendl;

      /* The mode, read beside the store it governs.  Read here and
       * not earlier because it means nothing without one. */
      const auto& m =
	g_conf().get_val<std::string>("rgw_nsfs_adoption_mode");
      /* in either mode:  the `adoption` hint can switch to automatic */
      adoption_wait_ms = g_conf().get_val<uint64_t>("rgw_nsfs_adoption_wait_ms");
      if (m == "automatic") {
	adoption_mode = AdoptionMode::Automatic;
      } else {
	adoption_mode = AdoptionMode::Command;
	if (!m.empty() && (m != "command")) {
	  /* The option is an enum, so this is a deployment that got
	   * past validation somehow;  the safe reading is the one that
	   * changes nothing on disk. */
	  ldpp_dout(dpp, 0) << "ERROR: rgw_nsfs_adoption_mode is \"" << m
	    << "\", which is neither command nor automatic;  adopting "
	    << "by command" << dendl;
	}
      }
      ldpp_dout(dpp, 1) << "nsfs: adoption mode " << m
	<< (adoption_mode == AdoptionMode::Command
	      ? ";  nothing is adopted until a command asks" : "")
	<< dendl;
    }
  }

  /* the access model, selected once:  what to do when the filesystem
   * denies an operation S3 authorised.  Named in the `fs features`
   * map, because no request reveals it. */
  const auto model =
    cct->_conf.get_val<std::string>("rgw_nsfs_access_model");
  if (model == "s3-native") {
    access_strategy = std::make_unique<nsfs::S3NativeAccess>();
  } else {
    access_strategy = std::make_unique<nsfs::PosixStrictAccess>();
  }
  access_strategy_live.store(access_strategy.get(),
			     std::memory_order_relaxed);
  ldpp_dout(dpp, 1) << "nsfs: access model " << access_strategy->name()
    << dendl;
  if (userDB) {
    ldpp_dout(dpp, 1) << "nsfs: user identity override "
      << (userDB->user_identity_override() ? "on" : "off")
      << ", role identity override "
      << (userDB->role_identity_override() ? "on" : "off") << dendl;
  }

  nb_xattr_strategy = std::make_unique<nsfs::NooBaaXattrStrategy>();
  nb_path_strategy = std::make_unique<nsfs::NooBaaPathStrategy>();
  nb_mpu_strategy = std::make_unique<nsfs::NooBaaMPUStrategy>();

  /* The format shared and strong use.  Its MPU member is left null:
   * per-part against strided is chosen from what the filesystem can
   * do, not from what the tree is. */
  rgw_meta_format.xattr_strategy = xattr_strategy.get();
  rgw_meta_format.path_strategy = path_strategy.get();
  rgw_meta_format.mpu_strategy = nullptr;
  rgw_meta_format.bucket_state_strategy = bucket_state_strategy.get();
  rgw_meta_format.fname = "rgw-meta";

  /* What base is:  NooBaa's format entire -- NooBaa's attribute names,
   * path conventions and staging layout.  Its MPU member is named
   * rather than left null, because NooBaa's staging layout is part of
   * the format and not a capability question;  a base bucket stages as
   * NooBaa does whatever the filesystem can do. */
  noobaa_format.xattr_strategy = nb_xattr_strategy.get();
  noobaa_format.path_strategy = nb_path_strategy.get();
  /* Bucket state is in the nsfs format in every profile:  NooBaa's
   * record is a JSON file in a store not read on the request path, so
   * a base bucket's state is the attribute nsfs writes beside the data,
   * the same one shared and strong carry.  A reader for NooBaa's store
   * is a separate piece of work and does not belong on the request
   * path. */
  noobaa_format.mpu_strategy = nb_mpu_strategy.get();
  noobaa_format.bucket_state_strategy = bucket_state_strategy.get();
  noobaa_format.fname = "noobaa";

  /* Three profiles, two formats.  Shared and strong differ in which
   * extensions the mask permits, not in how the tree is written, so
   * they name one format between them. */
  base_profile.extensions = nsfs::EXTENSIONS_NONE;
  base_profile.format = &noobaa_format;
  shared_profile.extensions = nsfs::EXTENSIONS_SHARED;
  shared_profile.format = &rgw_meta_format;
  extended_profile.extensions = nsfs::EXTENSIONS_STRONG;
  extended_profile.format = &rgw_meta_format;
  ldpp_dout(dpp, 1) << "nsfs: new buckets are marked with extension set "
    << new_bucket_extensions() << dendl;

  /* Every strategy names some scaffolding, and the listing paths test one
   * name at a time;  assemble the union once so that no strategy is
   * consulted per directory entry.  The mechanism's own temporaries come
   * from fs_strategy.h -- they are not a format's to choose. */
  {
    auto add = [this](const nsfs::ReservedNames& r) {
      auto& a = reserved_names;
      a.exact.insert(a.exact.end(), r.exact.begin(), r.exact.end());
      a.prefixes.insert(a.prefixes.end(), r.prefixes.begin(),
			r.prefixes.end());
      a.staging_prefixes.insert(a.staging_prefixes.end(),
				r.staging_prefixes.begin(),
				r.staging_prefixes.end());
      a.content_exact.insert(a.content_exact.end(),
			     r.content_exact.begin(), r.content_exact.end());
    };
    add(path_strategy->reserved_names());
    add(mpu_strategy->reserved_names());
    /* NooBaa's too, and unconditionally.
     *
     * The aggregate exists so that no strategy is consulted per
     * directory entry, which means it cannot depend on the bucket --
     * and a base bucket's scaffolding is NooBaa's.  Without this a
     * bucket in NooBaa's format listed `.noobaa-nsfs_<id>/...` as
     * objects, and an upgraded one would list whatever the conversion
     * left behind.  The cost is a few more comparisons per entry,
     * which is what an aggregate is for. */
    add(nb_path_strategy->reserved_names());
    add(nb_mpu_strategy->reserved_names());
    reserved_names.prefixes.emplace_back(nsfs::TMP_LINK_PREFIX);
    reserved_names.prefixes.emplace_back(nsfs::UNLINK_TMP_PREFIX);
    reserved_names.prefixes.emplace_back(nsfs::CLONE_PARENT_PREFIX);
  }

}

/* Build the multipart part cache.
 *
 * Sizing is the caller's, not the configuration's.  The sizes decide
 * whether eviction ever happens, and eviction is what runs the
 * stabilize callback below, so a caller which wants that path reachable
 * has to be able to ask for it -- the defaults are large enough that it
 * never fires.  What is shared here is the structure:  the policy, the
 * callback, and how they are wired.
 */
void NSFSDriver::init_multipart_cache(const DoutPrefixProvider* dpp,
				      uint64_t mp_max, uint64_t mp_lanes,
				      uint64_t mp_parts, uint64_t mp_max_parts,
				      file::listing::MultipartCachePolicy mp_policy)
{
    file::listing::stabilize_fn_t stabilize;
    if (mp_policy == file::listing::MultipartCachePolicy::writeback) {
      stabilize = [this](const file::listing::MultipartCacheKey& key,
	  const boost::container::flat_map<uint32_t,
	    file::listing::MultipartPartInfo>& parts) {
	/* Eviction is the only thing that writes these records under
	 * this policy, so whatever it cannot find is lost. */
	/* ctx() and not the cct member:  eviction can run from anywhere
	 * and a driver whose cct was never set -- the unit test's is one
	 * -- would dereference null here rather than log. */
	const DoutPrefix dp{ctx(), dout_subsys,
			    "nsfs multipart stabilize: "};

	/* Resolved as every lookup is, from the row or NooBaa's record:
	 * a bucket at a stored path is not under the base path, and the
	 * key carries the tenant the row is keyed by. */
	const auto [tenant, name] =
	  nsfs::mp_cache_bucket_split(key.bucket_name);
	auto bdir = placed_bucket_dir(&dp, name, tenant);
	if (!bdir || (bdir->open(&dp) < 0)) {
	  return;
	}
	const int bfd = bdir->get_fd();

	/* RGW's layout only.  NooBaa's staging is read and never written
	 * (DECISIONS.md 2026-10-21):  its part records are on disk where
	 * NooBaa wrote them, a part RGW takes into one of its uploads
	 * waits until adoption has carried the upload into RGW's layout,
	 * so nothing here has a record to persist into NooBaa's staging.
	 * An entry for an upload still there finds no RGW staging and
	 * writes nothing. */
	nsfs::MPUStrategy* layouts[1] = { mpu_strategy.get() };
	nsfs::XattrStrategy* xattrs[1] = { xattr_strategy.get() };

	for (int i = 0; i < 1; ++i) {
	  auto* mpu = layouts[i];
	  auto* xs = xattrs[i];
	  auto root = mpu->staging_root(&dp, bfd);
	  if (!root) {
	    continue;
	  }
	  std::string path = mpu->staging_dir_name(key.upload_meta);
	  if (*root != ".") {
	    path = *root + "/" + path;
	  }
	  int dir_fd = ::openat(bfd, path.c_str(), O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
	  if (dir_fd < 0) {
	    continue;
	  }
	  auto close_dir = make_scope_guard([dir_fd] { ::close(dir_fd); });

	  for (auto& [num, pi] : parts) {
	    auto fname = mpu->part_name(num);
	    /* As the part file's owner.  Under impersonation a part is
	     * written as its uploader, and this runs with no request in
	     * hand -- for parts filled from disk as well as added -- so
	     * the file itself says who wrote it.  Its owner may always
	     * open it for writing and set its attributes;  acting as
	     * that owner on that one file grants nothing it does not
	     * already have. */
	    nsfs::Credentials owner;
	    {
	      struct stat st;
	      if (nsfs::fstatat(dir_fd, fname.c_str(), &st,
			    AT_SYMLINK_NOFOLLOW) < 0) {
		continue;
	      }
	      owner.uid = st.st_uid;
	      owner.gid = st.st_gid;
	      /* and its groups from the row that carries the uid, where
	       * one does:  a stat cannot see a group list, and the same
	       * rule serves both file-derived identities */
	      if (userDB) {
		(void) nsfs::complete_file_credentials(&dp, *userDB, owner);
	      }
	    }
	    auto write_record = [&]() -> int {
	      int pfd = ::openat(dir_fd, fname.c_str(), O_RDWR | O_NOFOLLOW);
	      if (pfd < 0) {
		return -errno;
	      }
	      auto close_pfd = make_scope_guard([pfd] { ::close(pfd); });

	      NSFSUploadPartInfo upi;
	      upi.num = pi.num;
	      upi.size = pi.size;
	      upi.etag = pi.etag;
	      upi.mtime = pi.mtime;
	      upi.cksum = pi.cksum;
	      /* Where the bytes are, which the record is the only source
	       * for once part 1 has been linked to the shared file.
	       * Dropping these defaulted them to "its own file, offset
	       * zero, nothing stored", so assembly read a shared part
	       * out of the empty record file and the completed object
	       * lost its bytes -- with the length right, because `size`
	       * was copied and only the placement was not. */
	      upi.shared = pi.shared;
	      upi.offset = pi.offset;
	      upi.stored = pi.stored;

	      bufferlist bl;
	      encode(upi, bl);
	      std::string xn = xs->disk_name(RGW_NSFS_ATTR_MPUPLOAD);
	      std::string xv = attr_on_disk(xs, RGW_NSFS_ATTR_MPUPLOAD, bl);
	      if (::fsetxattr(pfd, xn.c_str(), xv.data(), xv.size(), 0) < 0) {
		/* reported by the caller, which also knows the uid */
		return -errno;
	      }

	      /* and whatever the layout records for itself:  the nsfs
	       * layout's is the attribute just written */
	      nsfs::MPUStrategy::PartRecord rec;
	      rec.size = upi.size;
	      rec.stored = upi.stored;
	      rec.offset = upi.offset;
	      rec.shared = upi.shared;
	      rec.etag = upi.etag;
	      rec.mtime = upi.mtime;
	      rec.cksum = upi.cksum;
	      (void) mpu->write_part_record(&dp, dir_fd, fname, rec);
	      return 0;
	    };
	    /* run_as() decides whether a switch is needed, because it is
	     * the only thing that knows who this thread already is.  An
	     * earlier `owner.uid == ::geteuid()` test here asked the
	     * wrong question:  eviction is reached from cache insert, so
	     * this can run on a thread already acting as somebody, and
	     * geteuid() then returns *that* identity rather than the
	     * gateway's -- matching by accident and skipping the switch.
	     *
	     * The result is not discarded.  A record that cannot be
	     * written is a part the upload will not see, so say so here:
	     * the inner failures report themselves, but a failure to
	     * assume the owner never reaches them. */
	    int wret;
	    if (!nsfs::impersonation_enabled()) {
	      wret = write_record();
	    } else {
	      wret = nsfs::run_as(&dp, owner, write_record, null_yield);
	    }
	    if (wret < 0) {
	      ldpp_dout(&dp, 0) << "ERROR: could not write the record for part "
		<< num << " of " << key.upload_meta << " as uid " << owner.uid
		<< ": " << cpp_strerror(-wret) << ";  it is lost" << dendl;
	    }
	  }
	  return;
	}

	ldpp_dout(&dp, 4) << "no staging directory for " << key.upload_meta
	  << " in bucket " << key.bucket_name << ";  its part records are "
	  << "not written" << dendl;
      };
    }

    multipart_cache.reset(
      new nsfs::MultipartCache(
	mp_max, mp_lanes, mp_parts, mp_max_parts,
	mp_policy, std::move(stabilize)));
}

int NSFSDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  int ret = -1;
  base_path_ = g_conf().get_val<std::string>("rgw_nsfs_base_path");

  ldpp_dout(dpp, 20) << "Initializing NSFS driver: " << base_path_ << dendl;

  /* Do not start in a mode we cannot honour.
   *
   * With impersonation on, a request whose credentials cannot be
   * applied must not fall back to the gateway's own -- the daemon
   * reaches more of the filesystem than any user it would be acting
   * for.  Without the capability that is every request, so the
   * failure belongs at startup, as one message, rather than as a
   * flood later.
   *
   * Wherever it is configured.  A tool or library that is not to
   * impersonate is not configured to;  see impersonation_enabled(). */
  if (nsfs::impersonation_enabled()) {
    std::string missing;
    const bool have = nsfs::have_credential_capabilities(&missing);

    if (! have) {
      ldpp_dout(dpp, -1) << "ERROR: rgw_nsfs_impersonate is set but this"
	" process lacks " << missing << " in its permitted set.  Grant"
	" them -- setcap cap_setuid,cap_setgid+p on the binary, or a"
	" systemd unit with AmbientCapabilities=CAP_SETUID CAP_SETGID --"
	" or unset rgw_nsfs_impersonate.  Note +p and not +ep:  nothing"
	" here starts effective." << dendl;
      return -EPERM;
    } else if (g_conf().get_val<bool>("rgw_nsfs_io_uring_sqpoll")) {
      /* An SQPOLL ring's kernel thread reads an SQE after it is
       * submitted, personality included, and a submission holds its
       * personality only until io_uring_submit() returns -- after
       * that the slot may be unregistered or reused, and the
       * operation fails or runs as whoever holds it now
       * (probes/persprobe.c, case S1). */
      ldpp_dout(dpp, -1) << "ERROR: rgw_nsfs_io_uring_sqpoll cannot be"
	" used with rgw_nsfs_impersonate:  the polling thread reads each"
	" submission's personality after it is released.  Unset one of"
	" them." << dendl;
      return -EINVAL;
    } else {
      /* Every thread that serves an impersonated request creates an
       * io_uring ring, because the open carries the personality on a
       * submission entry.  Ring memory is charged against
       * RLIMIT_MEMLOCK, so the budget is one limit against the
       * frontend's whole thread count -- and a thread that cannot
       * create a ring cannot register a personality, so its requests
       * fail rather than being served as the gateway.  Report the
       * limit here so the arithmetic is visible before it bites. */
      struct rlimit rl{};
      if (::getrlimit(RLIMIT_MEMLOCK, &rl) == 0) {
	ldpp_dout(dpp, 1) << "nsfs: impersonation enabled;  RLIMIT_MEMLOCK is "
	  << rl.rlim_cur << " bytes, and each serving thread creates an"
	  " io_uring ring charged against it" << dendl;
      } else {
	ldpp_dout(dpp, 1) << "nsfs: impersonation enabled" << dendl;
      }

      /* Nothing is lowered here, because nothing starts effective:
       * the capabilities are granted permitted-only and each is put
       * in the effective set at its own site -- CAP_DAC_READ_SEARCH
       * by the thread that reads with no identity, the other three
       * by with_capabilities() around a named call.  Lowering at
       * startup was tried and does not work:  the frontend's
       * io_context_pool threads are created before this runs, and
       * capabilities are per-thread, so they kept theirs. */
    }
  }

  nsfs::init_create_modes(dpp, cct);
  init_strategies(dpp);

  /* ordered listing cache */
  bucket_cache.reset(
    new BucketCache(
      this, fs_reference_path(),
      g_conf().get_val<std::string>("rgw_nsfs_database_root"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_max_buckets"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_lanes"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_partitions"),
      g_conf().get_val<int64_t>("rgw_nsfs_cache_lmdb_count"),
      g_conf().get_val<bool>("rgw_nsfs_inotify")));

  /* multipart upload cache */
  {
    auto mp_pol_str =
      g_conf().get_val<std::string>("rgw_posix_multipart_cache_policy");
    auto mp_policy = file::listing::MultipartCachePolicy::writethrough;
    if (mp_pol_str == "writeback") {
      mp_policy = file::listing::MultipartCachePolicy::writeback;
    } else if (mp_pol_str == "volatile") {
      mp_policy = file::listing::MultipartCachePolicy::volatile_;
    }
    init_multipart_cache(
      dpp,
      g_conf().get_val<uint64_t>("rgw_posix_multipart_cache_max"),
      g_conf().get_val<uint64_t>("rgw_posix_multipart_cache_lanes"),
      g_conf().get_val<uint64_t>("rgw_posix_multipart_cache_partitions"),
      g_conf().get_val<uint64_t>("rgw_posix_multipart_cache_max_parts"),
      mp_policy);
  }

  /* user info cache */
  user_cache.set_max_size(dpp, g_conf().get_val<uint64_t>("rgw_nsfs_cache_max_users"));

  ret = open_base_path(dpp);
  if (ret < 0) {
    return ret;
  }
  lc = new RGWLC();
  lc->initialize(cct, this);

  if (use_lc_thread) {
    ret = userDB->createLCTables(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Failed to create LC tables, ret=" << ret << dendl;
      return ret;
    }
    lc->start_processor();
  }

  /* The bound on an nsswitch-resolved identity's staleness.  Not in a
   * process with no background tasks:  radosgw-admin would wait at exit
   * for the sweep's first pass over every name. */
  if (background_tasks) {
    userDB->start_name_sweep();
  }

  quota_handler = RGWQuotaHandler::generate_handler(dpp, this, true);

  init_sync_policy(dpp);

  if (!RGWPubSubEndpoint::init_all(cct)) {
    ldpp_dout(dpp, 1) << "WARNING: failed to init notification endpoints" << dendl;
  }

  /* I6.  `automatic` adopts the store at startup;  `command` waits to
   * be asked (DECISIONS.md 2026-10-11).  Last, so everything it needs
   * -- the strategies, the root, the database -- is up.
   *
   * **A failed import does not fail startup.**  It reports to the log
   * and the gateway comes up, because the tree is still readable and
   * not starting would take away the one thing an operator can
   * use to see what went wrong.  Re-running is the recovery, and the
   * mode still says to, so a restart retries it. */
  if (int r = load_adoption_completion(dpp); r < 0) {
    return r;
  }
  if (adoption_complete()) {
    ldpp_dout(dpp, 1) << "nsfs: adoption completed at "
      << completion->completed_at << " from " << completion->config_root
      << ";  NooBaa's store is not read" << dendl;
  } else if (adoption_configured() &&
	     (adoption_mode == AdoptionMode::Automatic) &&
	     io_context && background_tasks) {
    /* A gateway that serves adopts while it serves.  The identities
     * are imported first and here, because every bucket's owner
     * resolves through them;  each bucket waits in the queue, and one a
     * request reaches goes ahead of the rest. */
    if (int r = start_adoption_queue(dpp); r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: adoption did not start: "
	<< cpp_strerror(-r) << ";  the deployment is readable and a "
	<< "restart will retry it" << dendl;
    }
  } else if (adoption_configured() &&
	     (adoption_mode == AdoptionMode::Automatic)) {
    if (int r = run_adoption(dpp, null_yield); r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: adoption did not finish: "
	<< cpp_strerror(-r) << ";  the deployment is readable and a "
	<< "restart will retry it" << dendl;
    }
  }

  ldpp_dout(dpp, 20) << "SUCCESS" << dendl;
  return 0;
}

int NSFSDriver::open_base_path(const DoutPrefixProvider* dpp)
{
  /* Made as a bucket directory is made, so its mode is the one the
   * gateway gives directories;  then held only as a descriptor, not as
   * a Directory, so that nothing can open a bucket relative to it. */
  {
    nsfs::Directory d(base_path_, nullptr, ctx(), fs_strategy.get());
    int ret = d.open(dpp);
    if (ret == -ENOTDIR) {
      ldpp_dout(dpp, 0) << " ERROR: base path (" << base_path_
	<< "): was not a directory." << dendl;
      return ret;
    }
    if (ret == -ENOENT) {
      ret = d.create(dpp, null_yield);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not create base path ("
	  << base_path_ << "): " << cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
  }
  base_fd = ::open(base_path_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (base_fd < 0) {
    const int ret = -errno;
    ldpp_dout(dpp, 0) << " ERROR: could not open base path (" << base_path_
      << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return 0;
}

void NSFSDriver::init_sync_policy(const DoutPrefixProvider* dpp)
{
  const auto& name = g_conf().get_val<std::string>("rgw_posix_sync_policy");
  if (rgw::posix::parse_sync_policy(name) != rgw::posix::SyncPolicy::RELAXED) {
    ldpp_dout(dpp, 1) << "sync policy: " << name << dendl;
    return;
  }
  const uint64_t interval_ms =
    g_conf().get_val<uint64_t>("rgw_posix_sync_interval_ms");
  sync_set = std::make_unique<nsfs::SyncFsSet>(interval_ms);
  /* the default location's filesystem, before any bucket is loaded */
  struct statx stx{};
  if (nsfs::statx(base_fd, "", AT_EMPTY_PATH, STATX_INO, &stx) == 0) {
    note_bucket_filesystem(dpp, stx, base_fd);
  }
  sync_set->start();
  ldpp_dout(dpp, 1) << "sync policy: relaxed (syncfs every " << interval_ms
    << "ms, on every filesystem that holds a bucket)" << dendl;
}

void NSFSDriver::note_bucket_filesystem(const DoutPrefixProvider* dpp,
					const struct statx& stx, int fd)
{
  if (!sync_set) {
    return;
  }
  const dev_t dev = makedev(stx.stx_dev_major, stx.stx_dev_minor);
  if (sync_set->add(dev, fd)) {
    ldpp_dout(dpp, 1) << "sync policy: relaxed now syncs filesystem "
      << stx.stx_dev_major << ":" << stx.stx_dev_minor << dendl;
  }
}

void NSFSDriver::shutdown()
{
  /* before the io_context is finished:  a worker parked on it would
   * keep it running */
  std::lock_guard l{adoption_queue_start_mtx};
  adoption_queue_shut = true;
  if (adoption_queue_owner) {
    adoption_queue_owner->shutdown();
  }
}

void NSFSDriver::finalize()
{
  if (sync_set) {
    sync_set->shutdown();
  }
  RGWPubSubEndpoint::shutdown_all();
  RGWQuotaHandler::free_handler(quota_handler);
}

std::unique_ptr<User> NSFSDriver::get_user(const rgw_user &u)
{
  return std::make_unique<NSFSUser>(this, u);
}

int NSFSDriver::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  {
    UserCacheEntry ce;
    if (user_cache.lookup_user_by_access_key(dpp, key, ce)) {
      auto u = new NSFSUser(this, ce.info);
      u->get_attrs() = ce.attrs;
      u->get_version_tracker() = ce.objv_tracker;
      user->reset(u);
      return 0;
    }
  }

  RGWUserInfo uinfo;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_user(dpp, std::string("access_key"), key, uinfo, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  User* u = new NSFSUser(this, uinfo);

  if (!u)
    return -ENOMEM;

  u->get_attrs() = std::move(attrs);
  u->get_version_tracker() = objv_tracker;
  user->reset(u);

  user_cache.insert_user(dpp, {uinfo, u->get_attrs(), objv_tracker});
  return 0;
}

int NSFSDriver::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{

  RGWUserInfo uinfo;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_user(dpp, std::string("email"), email, uinfo, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  User* u = new NSFSUser(this, uinfo);

  if (!u)
    return -ENOMEM;

  u->get_attrs() = std::move(attrs);
  u->get_version_tracker() = objv_tracker;
  user->reset(u);

  user_cache.insert_user(dpp, {uinfo, u->get_attrs(), objv_tracker});
  return 0;
}

int NSFSDriver::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  /* Swift keys and subusers are not supported by DBStore for now */
  return -ENOTSUP;
}

int NSFSDriver::load_account_by_id(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view id,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("account_id"), std::string(id), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int NSFSDriver::load_account_by_name(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::string_view name,
				 RGWAccountInfo& info,
				 Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("name"), std::string(name), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int NSFSDriver::load_account_by_email(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  std::string_view email,
				  RGWAccountInfo& info,
				  Attrs& attrs,
				  RGWObjVersionTracker& objv)
{
  RGWObjVersionTracker objv_tracker;

  int ret = userDB->get_account(dpp, std::string("email"), std::string(email), info, &attrs,
      &objv_tracker);

  if (ret < 0)
    return ret;

  objv = objv_tracker;
  return 0;
}

int NSFSDriver::store_account(const DoutPrefixProvider* dpp,
			  optional_yield y, bool exclusive,
			  const RGWAccountInfo& info,
			  const RGWAccountInfo* old_info,
			  const Attrs& attrs,
			  RGWObjVersionTracker& objv)
{
  int ret = userDB->store_account(dpp, info, exclusive, &attrs, &objv);

  if (ret < 0)
    return ret;

  return 0;
}

int NSFSDriver::delete_account(const DoutPrefixProvider* dpp,
			     optional_yield y,
			     const RGWAccountInfo& info,
			     RGWObjVersionTracker& objv)
{
  int ret = userDB->remove_account(dpp, info, &objv);

  if (ret < 0)
    return ret;

  return 0;
}



int NSFSDriver::load_owner_by_email(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view email,
				    rgw_owner& owner)
{
  RGWUserInfo uinfo;
  int ret = get_user_db()->get_user(dpp, "email", std::string{email},
				   uinfo, nullptr, nullptr);
  if (ret < 0) {
    return ret;
  }
  owner = std::move(uinfo.user_id);
  return 0;
}

std::unique_ptr<Object> NSFSDriver::get_object(const rgw_obj_key& k)
{
  return std::make_unique<NSFSObject>(this, k);
}

/* P2 and P5.  Where a bucket's storage is, resolved per lookup:
 *
 *     placement row for (name, tenant)?          ->  its Path
 *     NooBaa's record, adoption not complete?    ->  its path
 *     otherwise                                  ->  null:  nothing places it
 *
 * Only a placed directory is a bucket (`RGW_NSFS_VADE_MECUM.md` §1).  A
 * bucket nothing places is not looked for anywhere:  bucket_dir() gives
 * it a Directory with no path, which is -ENOENT when opened.
 *
 * **Bound per lookup, not cached.**  NooBaa's is read on every lookup --
 * `read_bucket_sdk_info()` builds the namespace resource around the
 * record each time, so changing the record changes where the next
 * request goes -- and storage is attachable to a running Scale cluster
 * at any time, so a design that binds a location once is wrong on the
 * platform this driver exists for.  That also disposes of whether a
 * path is "mutable":  binding late makes mutability a non-event.
 */
std::unique_ptr<nsfs::Directory>
NSFSDriver::placed_bucket_dir(const DoutPrefixProvider* dpp,
			      const std::string& name,
			      const std::string& tenant)
{
  if (!userDB) {
    return nullptr;
  }
  std::string path;

  nsfs::BucketPlacement bp;
  if (userDB->get_bucket_placement(dpp, name, tenant, bp) == 0) {
    /* A row is nsfs's and is authoritative:  it stops here rather than
     * falling through to NooBaa's record. */
    path = bp.path;
  } else {
    /* P5.  No row in the index, so ask NooBaa's record.
     *
     * `path` is required on every NooBaa bucket record, so a real
     * deployment's buckets are scattered -- and in `serve` mode
     * nothing is written, so there is no row to have.  Without this,
     * serve mode only ever works for buckets at the default location,
     * which is not what a deployment we have not touched looks like.
     *
     * The read is the one serve mode already does for a bucket's
     * configuration, through the component that owns it, so this adds
     * no consumer of NooBaa's store.
     *
     * **It never adopts.**  Resolving where a bucket lives is
     * read-only;  adoption happens because the mode says so, or by
     * explicit command. */
    if (!nb_bucket_state_strategy || reread_adoption_complete(dpp)) {
      return nullptr;
    }
    auto* nb_state = static_cast<nsfs::NooBaaBucketStateStrategy*>(
	nb_bucket_state_strategy.get());
    if (nb_state->recorded_path(dpp, name, path) < 0) {
      return nullptr;
    }
    if (path.empty()) {
      return nullptr;
    }
    /* NooBaa's path names the bucket's own directory, and a path nsfs
     * would not store is one it will not serve from either -- the
     * record is a store nsfs did not write (`RGW_NSFS_VADE_MECUM.md` §1). */
    if (!nsfs::placement_path_is_acceptable(path)) {
      ldpp_dout(dpp, 0) << "ERROR: bucket " << name << ": their record "
	<< "says " << path << ", which is not an absolute canonical path"
	<< dendl;
      return nullptr;
    }
  }

  ldpp_dout(dpp, 10) << "nsfs: bucket " << name << " is placed at "
    << path << dendl;
  return make_placed_dir(path);
}

std::unique_ptr<nsfs::Directory>
NSFSDriver::make_placed_dir(const std::string& path)
{
  /* No parent, and the whole path as the name -- which is how the
   * base path itself is built.  The FSStrategy has to be passed
   * explicitly for the same reason it is there:  a Directory built
   * without one leaves every File below it with a null strategy. */
  auto d = std::make_unique<nsfs::Directory>(path, nullptr, ctx(),
					     fs_strategy.get());
  /* Seeded exactly as the base path is.
   *
   * A child inherits its strategies from its parent, and this has no
   * parent -- so without this its xattr strategy is null and the first
   * `read_attrs()` faults.  The same four the root is given, because a
   * placed bucket is a root of its own as far as resolution is
   * concerned.
   *
   * Which format's strategies these are is the driver's default, the
   * same as the root's.  A placed bucket in NooBaa's format still has
   * its profile recognised per bucket, which is where the per-format
   * strategies come from;  this is only the seed. */
  d->set_mpu_strategy(mpu_strategy.get());
  d->set_xattr_strategy(xattr_strategy.get());
  d->set_path_strategy(path_strategy.get());
  d->set_reserved_names(&reserved_names);
  return d;
}

namespace {

bool is_role_principal(const rgw_owner& who)
{
  const auto* u = std::get_if<rgw_user>(&who);
  return u && (u->ns == rgw::sal::role_session_ns);
}

/* The identity row's key for `who`:  a role principal's is its role
 * key, every other owner's its rendering */
std::string identity_key_for(const rgw_owner& who)
{
  if (is_role_principal(who)) {
    return nsfs::role_identity_key(std::get<rgw_user>(who).id);
  }
  return to_string(who);
}

} // anonymous namespace

rgw_owner NSFSDriver::new_bucket_path_source(const DoutPrefixProvider* dpp,
					     const rgw_owner& owner,
					     const rgw_owner& authenticated_user,
					     optional_yield y)
{
  const auto* user = std::get_if<rgw_user>(&authenticated_user);
  const auto* acct = std::get_if<rgw_account_id>(&owner);
  if (!user || !acct) {
    return owner;
  }
  if (user->ns == rgw::sal::role_session_ns) {
    /* a role given an identity, under the role override, as a user's
     * own row under the user override */
    if (!userDB->role_identity_override()) {
      return owner;
    }
    std::unique_ptr<RGWRole> role = get_role(user->id);
    if ((role->load_by_id(dpp, y) < 0) || (role->get_account_id() != *acct)) {
      return owner;
    }
    nsfs::Identity row;
    if (userDB->get_identity(dpp, nsfs::role_identity_key(user->id), row) < 0) {
      return owner;
    }
    return authenticated_user;
  }
  if (!userDB->user_identity_override()) {
    return owner;
  }
  std::unique_ptr<User> u = get_user(*user);
  if ((u->load_user(dpp, y) < 0) || (u->get_info().account_id != *acct)) {
    return owner;
  }
  nsfs::Identity row;
  if (userDB->get_identity(dpp, to_string(authenticated_user), row) < 0) {
    return owner;
  }
  return authenticated_user;
}

int NSFSDriver::new_bucket_path(const DoutPrefixProvider* dpp,
				const rgw_owner& owner, const rgw_bucket& b,
				std::string& path, bool* owners_path)
{
  if (owners_path) {
    *owners_path = false;
  }
  /* NooBaa has no tenants, so only an untenanted name can be one of
   * its buckets. */
  if (b.tenant.empty() && nb_bucket_state_strategy &&
      !reread_adoption_complete(dpp)) {
    auto* nb_state = static_cast<nsfs::NooBaaBucketStateStrategy*>(
      nb_bucket_state_strategy.get());
    std::string recorded;
    if (nb_state->recorded_path(dpp, b.name, recorded) != -ENOENT) {
      ldpp_dout(dpp, 1) << "nsfs: not creating bucket " << b.name
	<< ":  a NooBaa bucket record holds the name" << dendl;
      return -EEXIST;
    }
  }

  const std::string dname =
    b.tenant.empty() ? b.name : (b.tenant + "$" + b.name);
  const std::string key = identity_key_for(owner);

  nsfs::Identity id;
  int ret = userDB->get_identity(dpp, key, id);
  if ((ret < 0) && (ret != -ENOENT)) {
    return ret;
  }
  if ((ret == 0) && !id.new_buckets_path.empty()) {
    std::string at = id.new_buckets_path;
    while ((at.size() > 1) && (at.back() == '/')) {
      at.pop_back();
    }
    path = at + "/" + dname;
    if (owners_path) {
      *owners_path = true;
    }
    return 0;
  }

  bool imported = false;
  ret = userDB->owner_was_imported(dpp, key, imported);
  if (ret < 0) {
    return ret;
  }
  if (imported) {
    ldpp_dout(dpp, 1) << "nsfs: " << key << " has no new_buckets_path, and "
      << "an account imported from NooBaa without one cannot create "
      << "buckets" << dendl;
    return -EACCES;
  }
  path = default_bucket_location() + "/" + dname;
  return 0;
}

/* The directory of a bucket nothing places:  no row, and no NooBaa
 * record.  It has no path, so opening it is -ENOENT and nothing on disk
 * is consulted -- a directory under the base path is not a bucket
 * because of where it sits (DECISIONS.md 2026-10-13).  The handle is
 * still a bucket, because RGW's CreateBucket creates through the one
 * load_bucket() returned, and create() places it. */
std::unique_ptr<nsfs::Directory>
NSFSDriver::bucket_dir(const DoutPrefixProvider* dpp, const std::string& name,
		       const std::string& tenant)
{
  if (auto placed = placed_bucket_dir(dpp, name, tenant)) {
    return placed;
  }
  return make_placed_dir(std::string{});
}

int NSFSDriver::custom_bucket_path(const DoutPrefixProvider* dpp,
				   const rgw_owner& owner, const RGWEnv* env,
				   std::optional<std::string>& path)
{
  path.reset();
  if (!env) {
    return 0;
  }
  /* the header as RGWEnv keeps it:  HTTP_, upper case, '-' as '_' */
  std::string var = "HTTP_";
  for (char c : g_conf().get_val<std::string>(
	 "rgw_nsfs_custom_bucket_path_header")) {
    var += (c == '-') ? '_' : static_cast<char>(std::toupper(
						   static_cast<unsigned char>(c)));
  }
  const char* asked = env->get(var.c_str());
  if (!asked || !*asked) {
    return 0;
  }

  std::string list;
  nsfs::Identity id;
  if ((userDB->get_identity(dpp, identity_key_for(owner), id) == 0) &&
      !id.custom_bucket_path_allowed_list.empty()) {
    list = id.custom_bucket_path_allowed_list;
  } else {
    /* the global fallback, which nothing validated when it was set */
    std::string bad;
    if (nsfs::normalize_allowed_list(
	  g_conf().get_val<std::string>("rgw_nsfs_custom_bucket_path_allowed_list"),
	  list, bad) < 0) {
      ldpp_dout(dpp, 0) << "ERROR: rgw_nsfs_custom_bucket_path_allowed_list "
	<< "entry \"" << bad << "\" is not an absolute canonical path;  "
	<< "denying custom bucket paths" << dendl;
      return -EACCES;
    }
  }
  if (list.empty() || !nsfs::allowed_list_admits(list, asked)) {
    ldpp_dout(dpp, 1) << "nsfs: custom bucket path " << asked << " denied:  "
      << (list.empty() ? "no allowlist" : "not in the allowlist " + list)
      << dendl;
    return -EACCES;
  }
  if (!nsfs::placement_path_is_acceptable(asked)) {
    ldpp_dout(dpp, 1) << "nsfs: custom bucket path " << asked << " rejected:  "
      << "not an absolute canonical path" << dendl;
    return -EINVAL;
  }
  path = asked;
  return 0;
}

int NSFSDriver::check_owner_access(const DoutPrefixProvider* dpp,
				   const rgw_owner& owner,
				   const std::string& path, int mode,
				   optional_yield y)
{
  /* Only when the gateway writes as identities:  without impersonation
   * an account's uid never touches the filesystem, and a gateway that
   * cannot assume one has nothing to check with. */
  if (!nsfs::impersonation_enabled()) {
    return 0;
  }
  nsfs::Credentials cred;
  /* a role principal resolves as its session does */
  int ret = is_role_principal(owner)
    ? get_credentials_for(dpp, owner, y, cred)
    : nsfs::resolve_credentials(dpp, *userDB, owner, cred, y);
  if ((ret == -ENOENT) && unresolved_as_gateway_injected(owner)) {
    return 0;
  }
  if (ret == -ENOENT) {
    /* credentials that do not resolve are denied, not checked as the gateway
     * (`RGW_NSFS_VADE_MECUM.md` §12, "No authenticated user means refuse") */
    ldpp_dout(dpp, 1) << "nsfs: " << to_string(owner) << " may not access "
      << path << ":  no credentials resolve for it" << dendl;
    return -EACCES;
  }
  if (ret < 0) {
    return ret;
  }
  /* The directory itself, not the path to it:  the gateway opens a
   * bucket's directory and serves everything below it from that handle,
   * so the directories above it are never the owner's to pass through
   * (DECISIONS.md 2026-10-15).  NooBaa resolves the whole path as the
   * account, because it serves by path. */
  const int fd = ::open(path.c_str(), O_PATH | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    ret = -errno;
  } else {
    ret = nsfs::run_as(dpp, cred, [fd, mode]() -> int {
	return (::faccessat(fd, "", mode, AT_EMPTY_PATH | AT_EACCESS) < 0)
	  ? -errno : 0;
      }, y);
    ::close(fd);
  }
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "nsfs: " << to_string(owner) << " (uid " << cred.uid
      << ") may not access " << path << ":  " << cpp_strerror(-ret) << dendl;
    return -EACCES;
  }
  return 0;
}

int NSFSDriver::check_owner_can_create_at(const DoutPrefixProvider* dpp,
					  const rgw_owner& owner,
					  const std::string& path,
					  optional_yield y)
{
  return check_owner_access(
    dpp, owner, std::filesystem::path(path).parent_path().string(),
    W_OK | X_OK, y);
}

/* Whether `path` carries any user.nsfs.* attribute:  1 if it does, 0 if
 * not, else a negative errno.  More attributes than fit counts as
 * carrying them, since a directory nothing wrote to has few. */
static int carries_nsfs_state(const std::string& path)
{
  char names[8192];
  const ssize_t len = ::listxattr(path.c_str(), names, sizeof(names));
  if (len < 0) {
    return (errno == ERANGE) ? 1 : -errno;
  }
  for (const char* q = names; q < names + len; q += std::strlen(q) + 1) {
    if (std::string_view(q).starts_with("user.nsfs.")) {
      return 1;
    }
  }
  return 0;
}

/* Until adoption is complete a NooBaa bucket not yet imported has no
 * row, so no row insert rejects a bucket at its directory or nested
 * with it;  its record's path is checked here instead, for creation
 * and placement alike.  Only the bucket records are read. */
int NSFSDriver::check_clear_of_noobaa_paths(const DoutPrefixProvider* dpp,
					    const std::string& name,
					    const std::string& path)
{
  if (!nb_bucket_state_strategy || reread_adoption_complete(dpp)) {
    return 0;
  }
  auto* nb_state = static_cast<nsfs::NooBaaBucketStateStrategy*>(
    nb_bucket_state_strategy.get());
  std::vector<std::string> records;
  int ret = nb_state->list_records(dpp, records);
  if (ret < 0) {
    return ret;
  }
  auto nested = [](std::string_view a, std::string_view b) {
    return (a == b) ||
      (a.starts_with(b) && (a[b.size()] == '/')) ||
      (b.starts_with(a) && (b[a.size()] == '/'));
  };
  for (const auto& rec : records) {
    std::string recorded;
    if (nb_state->recorded_path(dpp, rec, recorded) < 0) {
      continue;
    }
    /* NooBaa writes its paths with a trailing slash as often as not */
    while ((recorded.size() > 1) && (recorded.back() == '/')) {
      recorded.pop_back();
    }
    if (nested(path, recorded)) {
      ldpp_dout(dpp, 1) << "nsfs: bucket " << name << ":  " << path
	<< " is NooBaa bucket " << rec << "'s directory " << recorded
	<< ", or nested with it" << dendl;
      return -EEXIST;
    }
  }
  return 0;
}

int NSFSDriver::place_bucket(const DoutPrefixProvider* dpp, optional_yield y,
			     const std::string& name, const std::string& path,
			     const rgw_owner& owner,
			     const RGWZoneGroup& zonegroup,
			     std::string* profile)
{
  if (!nsfs::placement_path_is_acceptable(path)) {
    ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << " at "
      << path << ":  not an absolute canonical path" << dendl;
    return -EINVAL;
  }
  struct stat st;
  if (::stat(path.c_str(), &st) < 0) {
    int ret = -errno;
    ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << " at "
      << path << ":  " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (!S_ISDIR(st.st_mode)) {
    ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << " at "
      << path << ":  not a directory" << dendl;
    return -EINVAL;
  }

  /* the owner exists, as NooBaa's `bucket add` requires */
  ACLOwner acl_owner;
  acl_owner.id = owner;
  if (const auto* a = std::get_if<rgw_account_id>(&owner)) {
    RGWAccountInfo ainfo;
    Attrs aattrs;
    RGWObjVersionTracker objv;
    if (load_account_by_id(dpp, y, *a, ainfo, aattrs, objv) < 0) {
      ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << ":  "
	<< "no account " << *a << dendl;
      return -ERR_NO_SUCH_USER;
    }
    acl_owner.display_name = ainfo.name;
  } else {
    std::unique_ptr<User> user = get_user(std::get<rgw_user>(owner));
    if (user->load_user(dpp, y) < 0) {
      ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << ":  "
	<< "no user " << owner << dendl;
      return -ERR_NO_SUCH_USER;
    }
    /* A placed bucket's owner is an account, never a user (DECISIONS.md
     * 2026-10-15).  A user in no account has no place in NooBaa's model,
     * where every bucket belongs to an account, and a BucketOwnerEnforced
     * bucket would leave it unable to read its objects without a policy.
     * A user in an account is rejected as NooBaa's `bucket add` rejects
     * an IAM user:  the caller names the account. */
    const auto& account = user->get_info().account_id;
    ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << ":  "
      << owner << (account.empty() ? " belongs to no account"
		   : " is a user of account " + account + ";  name the account")
      << ", and a placed bucket's owner must be an account" << dendl;
    return -EINVAL;
  }

  /* NooBaa checks the owner can read and write the directory, as the
   * owner (`is_dir_accessible`);  here the kernel decides */
  int ret = check_owner_access(dpp, owner, path, R_OK | W_OK, y);
  if (ret < 0) {
    return ret;
  }

  /* The name is taken by a row, or by a NooBaa record until adoption
   * is complete -- the same rejection creation makes.  Nesting with
   * another bucket's directory is the row insert's. */
  if (nb_bucket_state_strategy && !reread_adoption_complete(dpp)) {
    auto* nb_state = static_cast<nsfs::NooBaaBucketStateStrategy*>(
      nb_bucket_state_strategy.get());
    std::string recorded;
    if (nb_state->recorded_path(dpp, name, recorded) != -ENOENT) {
      ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name
	<< ":  a NooBaa bucket record holds the name" << dendl;
      return -EEXIST;
    }
  }

  /* Never over an existing bucket.  A directory carrying nsfs state is
   * one, or was one;  and until adoption is complete a NooBaa record
   * not yet imported serves its directory with no row to stop it, so
   * its path is checked here as the rows are, nesting included. */
  ret = carries_nsfs_state(path);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: not placing bucket " << name << ":  "
      << "reading the attributes of " << path << ": "
      << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (ret > 0) {
    ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << ":  "
      << path << " carries nsfs state, so it is or was a bucket's" << dendl;
    return -EEXIST;
  }
  ret = check_clear_of_noobaa_paths(dpp, name, path);
  if (ret < 0) {
    return ret;
  }

  nsfs::BucketPlacement bp;
  bp.bucket_name = name;
  bp.path = path;
  bp.owner = to_string(owner);
  ret = userDB->insert_bucket_placement(dpp, bp);
  if (ret == -EADDRINUSE) {
    ldpp_dout(dpp, 1) << "nsfs: not placing bucket " << name << ":  "
      << path << " is another bucket's directory, or nested with one"
      << dendl;
    return -EEXIST;
  }
  if (ret < 0) {
    return ret;
  }

  /* The bucket an RGW CreateBucket would have made, over a directory
   * that was already there:  the configured profile, marked at once;
   * versioning off;  BucketOwnerEnforced;  its creation time the
   * directory's, which load_bucket() seeds;  and the directory left in
   * place when the bucket is deleted (DECISIONS.md 2026-10-14). */
  RGWBucketInfo seed;
  seed.bucket.name = name;
  seed.owner = owner;
  std::unique_ptr<Bucket> loaded = get_bucket(seed);
  auto* bucket = static_cast<NSFSBucket*>(loaded.get());
  bool marked = false;
  ret = bucket->load_bucket(dpp, y);
  if (ret == 0) {
    RGWBucketInfo& info = bucket->get_info();
    info.owner = owner;
    char buf[17];
    gen_rand_alphanumeric(ctx(), buf, sizeof(buf) - 1);
    buf[16] = '\0';
    info.bucket.marker = name + "." + buf;
    info.bucket.bucket_id = info.bucket.marker;
    /* the site's, as CreateBucket records it:  a bucket in another
     * zonegroup is redirected */
    info.zonegroup = zonegroup.get_id();
    info.placement_rule = zonegroup.default_placement;
    info.flags = 0;
    info.requester_pays = false;

    ret = bucket->mark_extensions(dpp, new_bucket_extensions());
    marked = true;
  }
  if (ret == 0) {
    Attrs attrs;
    RGWAccessControlPolicy acl;
    acl.create_default(acl_owner.id, acl_owner.display_name);
    bufferlist aclbl;
    acl.encode(aclbl);
    attrs[RGW_ATTR_ACL] = std::move(aclbl);
    rgw::s3::OwnershipControls oc;
    oc.object_ownership = rgw::s3::ObjectOwnership::BucketOwnerEnforced;
    bufferlist ocbl;
    encode(oc, ocbl);
    attrs[RGW_ATTR_OWNERSHIP_CONTROLS] = std::move(ocbl);
    ret = create_failure_injected() ? -EIO
				    : bucket->merge_and_store_attrs(dpp, attrs, y);
  }
  if (ret == 0) {
    ret = bucket->set_owns_storage(dpp, false);
  }
  if (ret < 0) {
    /* taken back whole:  what was written to the directory, then the
     * row, so the directory is as it was and the name is free */
    if (marked) {
      bucket->release_nsfs_state(dpp);
    }
    userDB->remove_bucket_placement(dpp, name, "");
    ldpp_dout(dpp, 0) << "ERROR: placing bucket " << name << " at " << path
      << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  if (profile) {
    *profile = bucket->profile_name();
  }
  ldpp_dout(dpp, 1) << "nsfs: bucket " << name << " placed at " << path
    << " for " << owner << ", profile " << bucket->profile_name() << dendl;
  return 0;
}

int NSFSDriver::load_bucket_internal(const DoutPrefixProvider* dpp,
				     const rgw_bucket& b,
				     std::unique_ptr<Bucket>* bucket,
				     optional_yield y)
{
  *bucket = std::make_unique<NSFSBucket>(this, bucket_dir(dpp, b.name,
							  b.tenant), b);
  return (*bucket)->load_bucket(dpp, y);
}

int NSFSDriver::store_admits_adoption(const DoutPrefixProvider* dpp,
				      std::string& why)
{
  const auto& root =
    g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
  nsfs::NooBaaConfigStore store(root);
  nsfs::NooBaaStoreContents contents;
  if (int ret = store.read(dpp, contents); ret < 0) {
    why = root + ":  " + cpp_strerror(-ret);
    return ret;
  }
  if (const auto skew = contents.version_skew(); !skew.empty()) {
    why = root + ":  " + skew.front();
    return -ENOTSUP;
  }
  return 0;
}

bool NSFSDriver::adoption_failed(const std::string& bucket)
{
  std::lock_guard l{adoption_failures_mtx};
  return adoption_failures.contains(bucket);
}

int NSFSDriver::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  int ret = load_bucket_internal(dpp, b, bucket, y);

  /* A bucket that cannot be adopted cannot be served (DECISIONS.md
   * 2026-10-29). */
  auto refuse = [&] {
    ldpp_dout(dpp, 4) << "nsfs: bucket " << b.name << " is not served:  its "
      << "adoption failed, and waits for an operator" << dendl;
    bucket->reset();
    return -ERR_INVALID_BUCKET_STATE;
  };
  if ((ret == 0) && adoption_failed(b.name)) {
    return refuse();
  }

  /* A request promotes the bucket it reaches, and waits for it up to the
   * quantum.  Only a request:  work with no coroutine is the driver's
   * own and must not wait on itself.  Adopted in time, the bucket is
   * loaded again as it now is;  failed, it is refused;  otherwise it is
   * served as found, and a write is refused retryably
   * (reject_unadopted()). */
  if ((ret == 0) && y &&
      awaits_adoption(dpp, *static_cast<NSFSBucket*>(bucket->get()))) {
    if (adoption_queue.load(std::memory_order_acquire)->promote(
	  b.name, std::chrono::milliseconds(adoption_wait_ms.load()), y) == 0) {
      ret = load_bucket_internal(dpp, b, bucket, y);
    } else if (adoption_failed(b.name)) {
      return refuse();
    }
  }
  return ret;
}

std::unique_ptr<Bucket> NSFSDriver::get_bucket(const RGWBucketInfo& i)
{
  /* Don't need to fetch the bucket info, use the provided one */
  return std::make_unique<NSFSBucket>(
    this, bucket_dir(nullptr, i.bucket.name, i.bucket.tenant), i);
}

std::string NSFSDriver::zone_unique_trans_id(const uint64_t unique_num)
{
  char buf[41]; /* 2 + 21 + 1 + 16 (timestamp can consume up to 16) + 1 */
  time_t timestamp = time(NULL);

  snprintf(buf, sizeof(buf), "tx%021llx-%010llx",
           (unsigned long long)unique_num,
           (unsigned long long)timestamp);

  return std::string(buf);
}

int NSFSDriver::get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zg)
{
  /* XXX: for now only one zonegroup supported */
  std::unique_ptr<RGWZoneGroup> rzg =
      std::make_unique<RGWZoneGroup>("default", "default");
  rzg->api_name = "default";
  rzg->is_master = true;
  ZoneGroup* group = new NSFSZoneGroup(this, std::move(rzg));
  if (!group)
    return -ENOMEM;

  zg->reset(group);
  return 0;
}

int NSFSDriver::list_all_zones(const DoutPrefixProvider* dpp,
			    std::list<std::string>& zone_ids)
{
  zone_ids.push_back(zone.get_id());
  return 0;
}

int NSFSDriver::cluster_stat(RGWClusterStat& stats)
{
  /* the "cluster" is the filesystem the namespace lives on.  ganesha
   * asks for this on every statfs, and a client uses it to decide
   * whether a write can fit, so leaving it unfilled is not harmless */
  struct statvfs vfs;
  if (::statvfs(fs_reference_path().c_str(), &vfs) < 0) {
    return -errno;
  }

  /* frsize is the fragment size blocks are counted in;  f_bsize is a
   * hint for i/o size and is not what f_blocks counts */
  uint64_t frsize = vfs.f_frsize ? vfs.f_frsize : vfs.f_bsize;

  stats.kb = (vfs.f_blocks * frsize) >> 10;
  stats.kb_avail = (vfs.f_bavail * frsize) >> 10;
  stats.kb_used = ((vfs.f_blocks - vfs.f_bfree) * frsize) >> 10;
  /* inodes in use is the closest thing to an object count here */
  stats.num_objects = (vfs.f_files >= vfs.f_ffree)
    ? (vfs.f_files - vfs.f_ffree) : 0;

  return 0;
}

std::unique_ptr<Lifecycle> NSFSDriver::get_lifecycle(void)
{
  return std::make_unique<NSFSLifecycle>(this);
}

std::unique_ptr<Writer> NSFSDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size,
				  const rgw_owner& authenticated_user)
{
  return nullptr;
}

std::unique_ptr<Writer> NSFSDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag,
				  const rgw_owner& authenticated_user)
{

  return std::make_unique<NSFSAtomicWriter>(dpp, y, _head_obj, this, owner, ptail_placement_rule, olh_epoch, unique_tag,
					    authenticated_user);
}

const std::string& NSFSDriver::get_compression_type(const rgw_placement_rule& rule) {
  return zone.get_rgw_params().get_compression_type(rule);
}

std::unique_ptr<Notification> NSFSDriver::get_notification(rgw::sal::Object* obj,
			      rgw::sal::Object* src_obj, struct req_state* s,
			      rgw::notify::EventType event_type, optional_yield y,
			      const std::string* object_name)
{
  return std::make_unique<FSNotification>(this, obj, src_obj, event_type, s);
}

std::unique_ptr<Notification> NSFSDriver::get_notification(
    const DoutPrefixProvider* dpp,
    rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    const rgw::notify::EventTypeList& event_types,
    rgw::sal::Bucket* _bucket,
    std::string& _user_id,
    std::string& _user_tenant,
    std::string& _req_id,
    optional_yield y) {
  return std::make_unique<FSNotification>(this, obj, src_obj, event_types,
      _bucket, _user_id, _user_tenant, _req_id);
}

// TODO: marker and other params
int NSFSDriver::list_buckets(const DoutPrefixProvider* dpp, const rgw_owner& owner,
			     const std::string& tenant, const std::string& marker,
			     const std::string& end_marker, uint64_t max,
			     bool need_stats, BucketList &result, optional_yield y)
{
  result.buckets.clear();
  result.next_marker.clear();
  if (max == 0) {
    return 0;
  }

  /* The caller's rows, in name order:  a bucket exists because a record
   * says so, and the row says whose it is, so no other owner's bucket is
   * opened (DECISIONS.md 2026-10-13).  A NooBaa bucket not yet imported
   * has no row and an owner no RGW owner matches, so it is not listed
   * until the import, as before. */
  const std::string key = to_string(owner);
  std::string after = marker;
  for (;;) {
    std::vector<nsfs::BucketPlacement> rows;
    int ret = userDB->list_bucket_placements_by_owner(
      dpp, key, tenant, after, end_marker, max - result.buckets.size(), rows);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not list buckets for " << owner
	<< ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    if (rows.empty()) {
      return 0;
    }
    for (const auto& row : rows) {
      after = row.bucket_name;
      std::unique_ptr<Bucket> bucket;
      ret = load_bucket_internal(dpp, rgw_bucket(row.tenant, row.bucket_name), &bucket,
				 y);
      if (ret < 0) {
	/* A bucket we cannot read is one bucket, not a failed listing.
	 * -ENOENT is a row whose directory is gone.
	 * ERR_NOT_IMPLEMENTED is an extension set this build does not
	 * implement -- written by a newer gateway, or restored from one.
	 * -EBADMSG is stored state that does not decode, which fails
	 * that bucket closed.  The account's other buckets are fine. */
	if ((ret == -ENOENT) || (ret == -ERR_NOT_IMPLEMENTED) ||
	    (ret == -EBADMSG)) {
	  ldpp_dout(dpp, 1) << "list_buckets: skipping " << row.bucket_name
	    << ": " << cpp_strerror(-ret) << dendl;
	  continue;
	}
	return ret;
      }
      /* The row's owner indexes the one bucket_info records;  they
       * disagree only if an owner was changed somewhere that did not
       * update the row, which is a defect to see, not to paper over. */
      if (bucket->get_owner() != owner) {
	ldpp_dout(dpp, 0) << "ERROR: bucket " << row.bucket_name
	  << " is listed for " << owner << " but owned by "
	  << bucket->get_owner() << ";  its placement row is stale" << dendl;
	continue;
      }
      RGWBucketEnt ent;
      ent.bucket = bucket->get_key();
      /* from the bucket just loaded, not from stx_btime:  the birth time
       * is optional and a filesystem which does not report it -- GPFS --
       * leaves it zeroed, and load_bucket() has already decided what
       * this bucket's creation time is. */
      ent.creation_time = bucket->get_creation_time();
      // TODO: ent.size and ent.count
      result.buckets.push_back(std::move(ent));
      if (result.buckets.size() == max) {
	result.next_marker = row.bucket_name;
	return 0;
      }
    }
  }
}

/* The credentials `who` runs as:  a user's own row, else its account's;
 * an account's -- a role session in one -- its row.
 *
 * A user's account is needed only here, and such a user is normally in
 * the user cache already, since authenticating it put it there.  A
 * user that will not load still resolves under its own key.
 * `account`, if given, receives the account a user resolved through;
 * `row`, if given, the key of the row that resolved, as
 * nsfs::resolve_credentials() gives it. */
static int resolve_credentials_for(const DoutPrefixProvider* dpp,
				   NSFSDriver* driver, const rgw_owner& who,
			     optional_yield y, rgw_account_id* account,
			     nsfs::Credentials& cred,
			     std::string* row = nullptr)
{
  const auto* user = std::get_if<rgw_user>(&who);
  if (! user) {
    const int ret = nsfs::resolve_credentials(dpp, *driver->get_identity_db(),
					      who, cred, y);
    if (row && ((ret == 0) || (ret == -EPERM))) {
      *row = to_string(who);
    }
    return ret;
  }
  /* a role session (rgw::sal::role_session_ns):  the role's account is
   * the role's to say, and fixed for its life */
  if (user->ns == rgw::sal::role_session_ns) {
    std::unique_ptr<RGWRole> role = driver->get_role(user->id);
    const int ret = role->load_by_id(dpp, y);
    if (ret < 0) {
      return ret;
    }
    if (account) {
      *account = role->get_account_id();
    }
    return nsfs::resolve_role_credentials(dpp, *driver->get_identity_db(),
					  user->id, role->get_account_id(),
					  cred, y, row);
  }
  rgw_account_id acct;
  std::unique_ptr<User> u = driver->get_user(*user);
  if (u->load_user(dpp, y) == 0) {
    acct = u->get_info().account_id;
  }
  if (account) {
    *account = acct;
  }
  return nsfs::resolve_credentials(dpp, *driver->get_identity_db(),
				   *user, acct, cred, y, row);
}

int NSFSDriver::get_credentials_for(const DoutPrefixProvider* dpp,
				    const rgw_owner& who, optional_yield y,
				    nsfs::Credentials& out, std::string* row)
{
  return resolve_credentials_for(dpp, this, who, y, nullptr, out, row);
}

/* Defined below, beside the operations that use it most. */
static int pin_personality_for(const DoutPrefixProvider* dpp,
			       NSFSDriver* driver, const rgw_owner& who,
			       nsfs::FSIdentity* ident, optional_yield y);
static const rgw_owner& acting_for(const rgw_owner& authenticated_user,
				   const rgw_owner& on_behalf_of);

int NSFSBucket::create(const DoutPrefixProvider* dpp,
			const CreateParams& params,
			optional_yield y,
			const rgw_owner& authenticated_user)
{
  info.owner = params.owner;


  if (params.marker.empty()) {
    char buf[17];
    gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
    buf[16] = '\0';
    info.bucket.marker = info.bucket.name + "." + buf;
    info.bucket.bucket_id = info.bucket.marker;
  } else {
    info.bucket.marker = params.marker;
    info.bucket.bucket_id = params.bucket_id;
  }

  info.zonegroup = params.zonegroup_id;
  info.placement_rule = params.placement_rule;
  info.swift_versioning = params.swift_ver_location.has_value();
  if (params.swift_ver_location) {
    info.swift_ver_location = *params.swift_ver_location;
  }
  if (params.obj_lock_enabled) {
    info.flags |= BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
  }
  info.requester_pays = false;
  if (params.creation_time) {
    info.creation_time = *params.creation_time;
  } else {
    info.creation_time = ceph::real_clock::now();
  }
  if (params.quota) {
    info.quota = *params.quota;
  }

  int ret = set_attrs(params.attrs);
  if (ret < 0) {
    return ret;
  }

  /* A client's bucket is placed the way NooBaa places one:  the record
   * first, carrying the path, then the directory, and the record taken
   * back if the directory cannot be made (`bucketspace_fs.js`
   * create_bucket and _create_uls).  The row is what makes the name
   * taken:  of two creations of one name, one insert succeeds. */
  if (!is_internal()) {
    nsfs::BucketPlacement bp;
    bp.bucket_name = get_name();
    bp.tenant = get_tenant();
    bp.owner = to_string(params.owner);
    /* the owner's new_buckets_path and allowlist, or under the I9
     * override the creating user's own;  the bucket is the owner's
     * either way */
    const rgw_owner path_source = driver->new_bucket_path_source(
      dpp, params.owner, authenticated_user, y);
    bool owners_path = false;
    ret = driver->new_bucket_path(dpp, path_source, get_key(), bp.path,
				  &owners_path);
    if (ret < 0) {
      return ret;
    }
    std::optional<std::string> custom;
    ret = driver->custom_bucket_path(dpp, path_source, params.env, custom);
    if (ret < 0) {
      return ret;
    }
    if (custom) {
      bp.path = *custom;
    }
    /* NooBaa's guard applies where NooBaa has it:  a path the account
     * chose.  The base path is the gateway's default, not the
     * account's. */
    if (owners_path || custom) {
      ret = driver->check_owner_can_create_at(dpp, path_source, bp.path, y);
      if (ret < 0) {
	return ret;
      }
    }
    ret = driver->check_clear_of_noobaa_paths(dpp, get_name(), bp.path);
    if (ret < 0) {
      return ret;
    }
    ret = driver->get_identity_db()->insert_bucket_placement(dpp, bp);
    if (ret == -EADDRINUSE) {
      ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << ":  "
	<< bp.path << " is another bucket's directory, or nested with one"
	<< dendl;
      return -EEXIST;
    }
    if (ret < 0) {
      return ret;
    }
    dir = driver->make_placed_dir(bp.path);
  }

  bool existed = false;
  ret = create(dpp, y, &existed, authenticated_user);
  if (ret < 0) {
    if (!is_internal()) {
      driver->get_identity_db()->remove_bucket_placement(dpp, get_name(),
							 get_tenant());
    }
    return ret;
  }
  if (!is_internal() && (dir->stat(dpp) == 0) && (dir->open(dpp) == 0)) {
    driver->note_bucket_filesystem(dpp, dir->get_stx(), dir->get_fd());
  }

  return 0;
}

int NSFSUser::load_user_from_cache_or_db(const DoutPrefixProvider* dpp, bool& cache_hit)
{
  cache_hit = false;
  UserCacheEntry ce;
  if (driver->get_user_cache().lookup_user_by_uid(dpp, this->get_id().id, ce)) {
    this->get_info() = std::move(ce.info);
    this->get_attrs() = std::move(ce.attrs);
    this->get_version_tracker() = std::move(ce.objv_tracker);
    cache_hit = true;
    return 0;
  }

  int ret = driver->get_user_db()->get_user(dpp, std::string("user_id"), this->get_id().id, this->get_info(), &(this->get_attrs()),
        &(this->get_version_tracker()));
  if (ret == 0) {
    driver->get_user_cache().insert_user(dpp, {this->get_info(), this->get_attrs(), this->get_version_tracker()});
  }
  return ret;
}

int NSFSUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool cache_hit;
  int ret = load_user_from_cache_or_db(dpp, cache_hit);
  if (cache_hit) {
    ldpp_dout(dpp, 21) << "UserCache: read_attrs: cache hit for uid=" << this->get_id().id << dendl;
  }
  return ret;
}

int NSFSUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y)
{
  auto attrs = this->get_attrs();
  for(auto& it : new_attrs) {
	attrs[it.first] = it.second;
  }
  this->get_attrs() = std::move(attrs);

  return store_user(dpp, y, false);
}

int NSFSUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  bool cache_hit;
  int ret = load_user_from_cache_or_db(dpp, cache_hit);
  if (cache_hit) {
    ldpp_dout(dpp, 21) << "UserCache: load_user: cache hit for uid=" << this->get_id().id << dendl;
  }
  return ret;
}

int NSFSUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
  /* I9:  a user stored for the first time in an account gets a copy of
   * its account's identity row, as NooBaa's CreateUser copies the
   * account's nsfs_account_config.  Whatever
   * rgw_nsfs_user_identity_override says:  it decides whether the row
   * is consulted, not whether it is written.  Asked before the store,
   * since afterwards every user exists;  `old_info` cannot say, as most
   * updates pass none. */
  bool created = false;
  if (!get_info().account_id.empty()) {
    RGWUserInfo existing;
    existing.user_id = get_id();
    created = (driver->get_user_db()->get_user(dpp, "user_id", get_id().id,
					       existing, nullptr, nullptr)
	       == -ENOENT);
  }
  int ret = driver->get_user_db()->store_user(dpp, this->get_info(), exclusive, &(this->get_attrs()), &(this->get_version_tracker()), old_info);
  if (ret == 0) {
    driver->get_user_cache().invalidate_user(dpp, this->get_id().id);
    driver->get_user_cache().insert_user(dpp, {this->get_info(), this->get_attrs(), this->get_version_tracker()});
  }
  if ((ret == 0) && created) {
    ret = copy_account_identity(dpp);
  }
  return ret;
}

/* The copy itself.  An account with no row gives none, so the user
 * follows its account if one is added later (DECISIONS.md 2026-10-27);
 * a row the user already has is left alone. */
int NSFSUser::copy_account_identity(const DoutPrefixProvider* dpp)
{
  auto* db = driver->get_identity_db();
  const std::string key = to_string(rgw_owner{get_id()});
  nsfs::Identity row;
  int ret = db->get_identity(dpp, key, row);
  if (ret != -ENOENT) {
    return ret;
  }
  ret = db->get_identity(dpp, to_string(rgw_owner{get_info().account_id}),
			 row);
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    return ret;
  }
  row.key = key;
  ret = db->put_identity(dpp, row);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: nsfs: user " << key << " was stored, but "
      "its copy of account " << get_info().account_id << "'s identity was "
      "not: " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int NSFSUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = driver->get_user_db()->remove_user(dpp, this->get_info(), &(this->get_version_tracker()));
  if (ret == 0) {
    driver->get_user_cache().invalidate_user(dpp, this->get_id().id);
  } else {
    ldpp_dout(dpp, 0) << "ERROR: failed to remove user uid=" << this->get_id().id << " ret=" << ret << dendl;
  }
  return ret;
}

int NSFSUser::list_groups(const DoutPrefixProvider* dpp, optional_yield y,
                          std::string_view marker, uint32_t max_items,
                          GroupList& listing)
{
  std::vector<RGWGroupInfo> groups;
  int ret = driver->get_user_db()->list_user_groups(dpp,
      get_id().id, std::string(marker), max_items + 1, groups);
  if (ret < 0) {
    return ret;
  }

  if (groups.size() > max_items) {
    listing.next_marker = groups[max_items].name;
    groups.resize(max_items);
  }
  listing.groups = std::move(groups);
  return 0;
}

int NSFSUser::verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider *dpp, optional_yield y)
{
  *verified = false;
  return 0;
}

std::unique_ptr<Object> NSFSBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<NSFSObject>(driver, k, this);
}

int NSFSObject::fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb)
{
  auto* b = static_cast<NSFSBucket*>(get_bucket());
  return ent->fill_cache(dpp, y, cb, FSEnt::FLAG_NONE,
			 b->default_object_owner(dpp, y));
}

int NSFSDriver::mint_listing_entry(const std::string &bname,
                                    rgw_bucket_dir_entry &bde) {
    std::unique_ptr<Bucket> b;
    std::unique_ptr<Object> obj;
    NSFSObject *pobj;
    int ret;

    ret = load_bucket(nullptr, rgw_bucket(std::string(), bname),
                      &b, null_yield);
    if (ret < 0)
      return ret;

    obj = b->get_object(path_strategy->key_from_name(bde.key.name));
    pobj = static_cast<NSFSObject *>(obj.get());

    if (!pobj->check_exists(nullptr)) {
      ret = errno;
      return -ret;
    }

    ret = pobj->get_obj_attrs(null_yield, nullptr);
    if (ret < 0)
      return ret;

    ret = pobj->fill_cache(nullptr, null_yield,
        [&bde](const DoutPrefixProvider *dpp, rgw_bucket_dir_entry &nbde) -> int {
	  bde = nbde;
	  return 0;
        });

    return ret;
}

std::unique_ptr<LuaManager> NSFSDriver::get_lua_manager(const std::string& luarocks_path)
{
  return std::make_unique<NSFSLuaManager>(this);
}

std::unique_ptr<RGWRole> NSFSDriver::get_role(std::string name,
    std::string tenant,
    rgw_account_id account_id,
    std::string path,
    std::string trust_policy,
    std::string description,
    std::string max_session_duration_str,
    std::multimap<std::string,std::string> tags)
{
  return std::make_unique<DBStoreRole>(
      get_user_db(), std::move(name), std::move(tenant),
      std::move(account_id), std::move(path), std::move(trust_policy),
      std::move(description), std::move(max_session_duration_str),
      std::move(tags));
}

std::unique_ptr<RGWRole> NSFSDriver::get_role(std::string id)
{
  return std::make_unique<DBStoreRole>(get_user_db(), std::move(id));
}

std::unique_ptr<RGWRole> NSFSDriver::get_role(const RGWRoleInfo& info)
{
  return std::make_unique<DBStoreRole>(get_user_db(), info);
}

int NSFSDriver::count_account_roles(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    uint32_t& count)
{
  return get_user_db()->count_account_roles(dpp,
      std::string(account_id), count);
}

int NSFSDriver::list_account_roles(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   std::string_view path_prefix,
				   std::string_view marker,
				   uint32_t max_items,
				   RoleList& listing)
{
  std::vector<RGWRoleInfo> roles;
  int ret = get_user_db()->list_roles(dpp, "account",
      "", std::string(account_id),
      std::string(path_prefix), std::string(marker),
      max_items + 1, roles);
  if (ret < 0) {
    return ret;
  }

  if (roles.size() > max_items) {
    listing.next_marker = roles[max_items].name;
    roles.resize(max_items);
  }
  listing.roles = std::move(roles);
  return 0;
}

int NSFSDriver::list_roles(const DoutPrefixProvider *dpp,
			   optional_yield y,
			   const std::string& tenant,
			   const std::string& path_prefix,
			   const std::string& marker,
			   uint32_t max_items,
			   RoleList& listing)
{
  std::vector<RGWRoleInfo> roles;
  int ret = get_user_db()->list_roles(dpp, "tenant",
      tenant, "",
      path_prefix, marker,
      max_items + 1, roles);
  if (ret < 0) {
    return ret;
  }

  if (roles.size() > max_items) {
    listing.next_marker = roles[max_items].name;
    roles.resize(max_items);
  }
  listing.roles = std::move(roles);
  return 0;
}

int NSFSDriver::load_account_user_by_name(const DoutPrefixProvider* dpp,
					  optional_yield y,
					  std::string_view account_id,
					  std::string_view tenant,
					  std::string_view username,
					  std::unique_ptr<User>* user)
{
  RGWUserInfo uinfo;
  int ret = get_user_db()->get_account_user_by_name(dpp,
      std::string(account_id), std::string(username), uinfo);
  if (ret < 0) {
    return ret;
  }
  if (user) {
    *user = get_user(uinfo.user_id);
    (*user)->get_info() = uinfo;
    ret = (*user)->load_user(dpp, y);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int NSFSDriver::count_account_users(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    uint32_t& count)
{
  return get_user_db()->count_account_users(dpp,
      std::string(account_id), count);
}

int NSFSDriver::list_account_users(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   std::string_view tenant,
				   std::string_view path_prefix,
				   std::string_view marker,
				   uint32_t max_items,
				   UserList& listing)
{
  std::vector<RGWUserInfo> users;
  int ret = get_user_db()->list_account_users(dpp,
      std::string(account_id), std::string(marker),
      max_items + 1, users);
  if (ret < 0) {
    return ret;
  }

  if (!path_prefix.empty()) {
    std::string pp(path_prefix);
    users.erase(
        std::remove_if(users.begin(), users.end(),
            [&pp](const RGWUserInfo& u) {
              return u.path.substr(0, pp.size()) != pp;
            }),
        users.end());
  }

  if (users.size() > max_items) {
    listing.next_marker = users[max_items].display_name;
    users.resize(max_items);
  }
  listing.users = std::move(users);
  return 0;
}

int NSFSDriver::load_group_by_id(const DoutPrefixProvider* dpp,
				 optional_yield y, std::string_view id,
				 RGWGroupInfo& info, Attrs& attrs,
				 RGWObjVersionTracker& objv)
{
  info.id = std::string(id);
  return get_user_db()->get_group(dpp, "group_id", info, attrs);
}

int NSFSDriver::load_group_by_name(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view account_id,
				   std::string_view name,
				   RGWGroupInfo& info, Attrs& attrs,
				   RGWObjVersionTracker& objv)
{
  info.account_id = std::string(account_id);
  info.name = std::string(name);
  return get_user_db()->get_group(dpp, "name", info, attrs);
}

int NSFSDriver::store_group(const DoutPrefixProvider* dpp, optional_yield y,
			    const RGWGroupInfo& info, const Attrs& attrs,
			    RGWObjVersionTracker& objv, bool exclusive,
			    const RGWGroupInfo* old_info)
{
  return get_user_db()->store_group(dpp, info, attrs, exclusive);
}

int NSFSDriver::remove_group(const DoutPrefixProvider* dpp, optional_yield y,
			     const RGWGroupInfo& info,
			     RGWObjVersionTracker& objv)
{
  return get_user_db()->remove_group(dpp, info);
}

int NSFSDriver::list_group_users(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::string_view tenant,
				 std::string_view id,
				 std::string_view marker,
				 uint32_t max_items,
				 UserList& listing)
{
  std::vector<std::string> user_ids;
  int ret = get_user_db()->list_group_users(dpp,
      std::string(id), std::string(marker), max_items + 1, user_ids);
  if (ret < 0) {
    return ret;
  }

  if (user_ids.size() > max_items) {
    listing.next_marker = user_ids[max_items];
    user_ids.resize(max_items);
  }

  for (auto& uid : user_ids) {
    RGWUserInfo uinfo;
    uinfo.user_id.id = uid;
    ret = get_user_db()->get_user(dpp, std::string("user_id"), uid,
                                  uinfo, nullptr, nullptr);
    if (ret < 0) {
      continue;
    }
    listing.users.push_back(std::move(uinfo));
  }
  return 0;
}

int NSFSDriver::count_account_groups(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view account_id,
				     uint32_t& count)
{
  return get_user_db()->count_account_groups(dpp,
      std::string(account_id), count);
}

int NSFSDriver::list_account_groups(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view path_prefix,
				    std::string_view marker,
				    uint32_t max_items,
				    GroupList& listing)
{
  std::vector<RGWGroupInfo> groups;
  int ret = get_user_db()->list_account_groups(dpp,
      std::string(account_id), std::string(path_prefix),
      std::string(marker), max_items + 1, groups);
  if (ret < 0) {
    return ret;
  }

  if (groups.size() > max_items) {
    listing.next_marker = groups[max_items].name;
    groups.resize(max_items);
  }
  listing.groups = std::move(groups);
  return 0;
}

int NSFSDriver::store_oidc_provider(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    const RGWOIDCProviderInfo& info,
				    bool exclusive,
				    RGWObjVersionTracker* objv_tracker)
{
  return get_user_db()->store_oidc_provider(dpp, info, exclusive);
}

int NSFSDriver::load_oidc_provider(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view tenant,
				   std::string_view url,
				   RGWOIDCProviderInfo& info,
				   RGWObjVersionTracker* objv_tracker)
{
  return get_user_db()->load_oidc_provider(dpp,
      std::string(tenant), std::string(url), info);
}

int NSFSDriver::delete_oidc_provider(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     std::string_view tenant,
				     std::string_view url)
{
  return get_user_db()->delete_oidc_provider(dpp,
      std::string(tenant), std::string(url));
}

int NSFSDriver::get_oidc_providers(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   std::string_view tenant,
				   std::vector<RGWOIDCProviderInfo>& providers)
{
  return get_user_db()->list_oidc_providers(dpp,
      std::string(tenant), providers);
}

/* --- Topic SAL methods --- */

int NSFSDriver::read_topic_v2(const std::string& topic_name,
			      const std::string& tenant,
			      rgw_pubsub_topic& topic,
			      RGWObjVersionTracker* objv_tracker,
			      optional_yield y,
			      const DoutPrefixProvider* dpp)
{
  obj_version objv;
  int ret = get_user_db()->load_topic(dpp, topic_name, tenant, topic, objv);
  if (!ret && objv_tracker) {
    objv_tracker->read_version = objv;
  }
  return ret;
}

int NSFSDriver::write_topic_v2(const rgw_pubsub_topic& topic, bool exclusive,
			       RGWObjVersionTracker& objv_tracker,
			       optional_yield y,
			       const DoutPrefixProvider* dpp)
{
  return get_user_db()->store_topic(dpp, topic, exclusive, objv_tracker.write_version);
}

int NSFSDriver::remove_topic_v2(const std::string& topic_name,
				const std::string& tenant,
				RGWObjVersionTracker& objv_tracker,
				optional_yield y,
				const DoutPrefixProvider* dpp)
{
  return get_user_db()->remove_topic(dpp, topic_name, tenant);
}

int NSFSDriver::update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
					    const std::string& bucket_key,
					    bool add_mapping,
					    optional_yield y,
					    const DoutPrefixProvider* dpp)
{
  if (add_mapping) {
    return get_user_db()->add_bucket_topic_mapping(dpp, topic.name, bucket_key);
  } else {
    return get_user_db()->remove_bucket_topic_mapping(dpp, topic.name, bucket_key);
  }
}

int NSFSDriver::get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
					 std::set<std::string>& bucket_keys,
					 optional_yield y,
					 const DoutPrefixProvider* dpp)
{
  return get_user_db()->get_bucket_topic_mapping(dpp, topic.name, bucket_keys);
}

int NSFSDriver::remove_bucket_mapping_from_topics(
    const rgw_pubsub_bucket_topics& bucket_topics,
    const std::string& bucket_key,
    optional_yield y,
    const DoutPrefixProvider* dpp)
{
  return get_user_db()->remove_bucket_from_topic_mappings(dpp, bucket_key);
}

int NSFSDriver::list_account_topics(const DoutPrefixProvider* dpp,
				    optional_yield y,
				    std::string_view account_id,
				    std::string_view marker,
				    uint32_t max_items,
				    TopicList& listing)
{
  rgw_owner owner = rgw_account_id(std::string(account_id));
  std::vector<rgw_pubsub_topic> topics;
  int ret = get_user_db()->list_topics(dpp, "owner", owner,
      std::string(marker), max_items, topics);
  if (ret) {
    return ret;
  }
  for (auto& t : topics) {
    listing.topics.push_back(std::move(t.name));
  }
  if (!listing.topics.empty()) {
    listing.next_marker = listing.topics.back();
  }
  return 0;
}

/* In an anonymous namespace because the posix driver, which nsfs was
 * forked from, defines an rgw::sal::meta_list_handle of its own with
 * more members.  Two definitions of one name are one type to the
 * linker, which keeps one inline constructor for both:  posix's then
 * wrote its extra members past the end of this smaller handle. */
namespace {
struct meta_list_handle {
  std::string marker;
  std::string section;

  meta_list_handle(const std::string& _section, const std::string& _marker) {
    marker = _marker;
    section = _section;
  }
};
} // namespace

int NSFSDriver::meta_list_keys_init(const DoutPrefixProvider *dpp,
                                     const std::string& section,
                                     const std::string& marker, void** phandle)
{
  /* A section this driver does not list is rejected here, as RADOS's
   * metadata manager rejects one it has no handler for;  an empty
   * listing would read as "there are none". */
  if ((section != "user") && (section != "bucket") &&
      (section != "account") && (section != "topic")) {
    ldpp_dout(dpp, 1) << "nsfs: no metadata section " << section << dendl;
    return -ENOENT;
  }
  meta_list_handle* stuff = new meta_list_handle(section, marker);
  *phandle = (void *)stuff;
  return 0;
  }

int NSFSDriver::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle,
                                     int max, std::list<std::string>& keys,
                                     bool* truncated)
{
  meta_list_handle *h = static_cast<meta_list_handle *>(handle);
  *truncated = false;
  int ret;
  keys.clear();
  if (h->section == "user") {
    ret = get_user_db()->list_users(dpp, h->marker, max, keys, truncated);
    if (ret < 0) {
      return ret;
    }
    if (keys.size() > 0) {
      h->marker = *keys.rbegin();
      if (std::cmp_equal(keys.size(),max)) {
        *truncated = true;
      }
    }
  } else if (h->section == "bucket") {
    /* Every bucket a record places:  each row, and each NooBaa record
     * not yet imported, which has no row (DECISIONS.md 2026-10-13).
     * A row and a record of one name are one bucket, the row's.  No
     * owner filter, since this is the operator's listing.  Keys are
     * RGW's, `<tenant>/<name>` for a tenant's bucket. */
    std::vector<nsfs::BucketPlacement> rows;
    ret = userDB->list_bucket_placements(dpp, "", UINT64_MAX, rows);
    if (ret < 0) {
      return ret;
    }
    std::set<std::string> all;
    std::set<std::string> untenanted;
    for (const auto& r : rows) {
      all.insert(r.tenant.empty() ? r.bucket_name
				  : (r.tenant + "/" + r.bucket_name));
      if (r.tenant.empty()) {
	untenanted.insert(r.bucket_name);
      }
    }
    if (nb_bucket_state_strategy && !reread_adoption_complete(dpp)) {
      std::vector<std::string> records;
      ret = static_cast<nsfs::NooBaaBucketStateStrategy*>(
	nb_bucket_state_strategy.get())->list_records(dpp, records);
      if (ret < 0) {
	return ret;
      }
      for (const auto& n : records) {
	if (!untenanted.contains(n)) {
	  all.insert(n);
	}
      }
    }
    for (auto it = all.upper_bound(h->marker); it != all.end(); ++it) {
      keys.push_back(*it);
      h->marker = *it;
      if (std::cmp_equal(keys.size(), max)) {
	*truncated = (std::next(it) != all.end());
	break;
      }
    }
  } else if ((h->section == "account") || (h->section == "topic")) {
    /* one more than asked for, so a full page says whether another
     * follows */
    const uint64_t ask = static_cast<uint64_t>(max) + 1;
    ret = (h->section == "account")
      ? userDB->list_account_ids(dpp, h->marker, ask, keys)
      : userDB->list_topic_keys(dpp, h->marker, ask, keys);
    if (ret < 0) {
      return ret;
    }
    if (std::cmp_greater(keys.size(), max)) {
      keys.pop_back();
      *truncated = true;
    }
    if (!keys.empty()) {
      h->marker = keys.back();
    }
  }
  return 0;
}

void NSFSDriver::meta_list_keys_complete(void* handle)
{
  if (handle) {
    meta_list_handle *h = static_cast<meta_list_handle *>(handle);
    delete h;
  }
  return;
}

MDB_cmp_func* NSFSBucket::lmdb_cmp()
{
  return nsfs_lmdb_cmp;
}

int NSFSBucket::fill_cache(const DoutPrefixProvider* dpp, optional_yield y,
                            fill_cache_cb_t& cb)
{
  uint32_t flags = nsfs::FSEnt::FLAG_NONE;
  if (get_info().versioned()) {
    flags |= nsfs::FSEnt::FLAG_LIST_VERSIONS;
  }
  int ret = dir->fill_cache(dpp, y, cb, flags, default_object_owner(dpp, y));
  if (ret < 0) {
    return ret;
  }
  if (get_info().versioned()) {
    ret |= file::listing::FILL_CACHE_FLAG_FIXUP_CURRENT;
  }
  return ret;
}

const ACLOwner& NSFSBucket::default_object_owner(const DoutPrefixProvider* dpp,
						 optional_yield y)
{
  if (default_owner) {
    return *default_owner;
  }
  /* mint_listing_entry() reaches here with no prefix provider */
  NoDoutPrefix ndp(driver->ctx(), dout_subsys);
  const DoutPrefixProvider* p = dpp ? dpp : &ndp;

  ACLOwner o;
  o.id = get_info().owner;
  if (const auto* a = std::get_if<rgw_account_id>(&o.id)) {
    RGWAccountInfo info;
    Attrs attrs;
    RGWObjVersionTracker objv;
    if (driver->load_account_by_id(p, y, *a, info, attrs, objv) == 0) {
      o.display_name = info.name;
    }
  } else if (const auto* u = std::get_if<rgw_user>(&o.id); u && !u->empty()) {
    std::unique_ptr<User> user = driver->get_user(*u);
    if (user->load_user(p, y) == 0) {
      o.display_name = user->get_display_name();
    }
  }
  default_owner = std::move(o);
  return *default_owner;
}

int NSFSBucket::list(const DoutPrefixProvider* dpp, ListParams& params,
		    int max, ListResults& results, optional_yield y)
{
  /* multipart namespace: incomplete uploads live in staging directories
   * that aren't in the LMDB cache — delegate to list_multiparts() and
   * format results using RADOS naming conventions so the LC processor
   * can parse them via rgw_obj_key::parse_index_key().
   *
   * Skip this when we ARE a shadow (staging) bucket (ns == mp_ns) —
   * list_parts() uses shadow->list() to enumerate part files inside
   * the staging directory, which must fall through to the normal
   * LMDB/directory listing below. */
  if (params.ns == mp_ns && ns != mp_ns) {
    std::vector<std::unique_ptr<MultipartUpload>> uploads;
    /* The marker is the meta of the last upload returned, and this
     * function advances it in place.  That is the contract
     * RadosBucket::list follows, and RGWLC::handle_multipart_expiration
     * depends on it:  its loop repeats while is_truncated and never
     * sets a marker itself, so a driver which leaves the marker alone
     * makes the sweep re-read the same uploads until the interval
     * budget runs out, aborting nothing and reaching no later
     * bucket. */
    std::string marker = params.marker.name;
    int ret = list_multiparts(dpp, params.prefix, marker, params.delim,
			      max, uploads, nullptr,
			      &results.is_truncated, y,
			      rgw::sal::no_authenticated_user);
    if (ret < 0)
      return ret;
    for (auto& upload : uploads) {
      rgw_bucket_dir_entry bde{};
      bde.key.name = fmt::format("_{}_{}",
				 mp_ns,
				 upload->get_meta());
      bde.meta.mtime = upload->get_mtime();
      bde.meta.category = RGWObjCategory::MultiMeta;
      bde.exists = true;
      results.objs.push_back(std::move(bde));
    }
    if (!uploads.empty()) {
      results.next_marker = rgw_obj_key(uploads.back()->get_meta());
      params.marker = results.next_marker;
    }
    return 0;
  }

int count{0};
bool in_prefix{false};
// Names in the cache are in OID format
rgw_obj_key marker_key(params.marker);
params.marker = marker_key.get_oid();
{
  rgw_obj_key key(params.prefix);
  params.prefix = key.name;
}
if (max <= 0) {
    return 0;
  }

  //params.list_versions
  int ret = driver->get_bucket_cache()->list_bucket(
    dpp, y, this, params.marker.name, [&](const rgw_bucket_dir_entry& bde) -> bool
      {
	std::string ns;
	// bde.key can be encoded with the namespace.  Decode it here
	rgw_obj_key bde_key{bde.key};
	if (!params.list_versions && !bde.is_visible()) {
	  return true;
	}
        if (bde_key.ns != params.ns) {
          // Namespace must match
          return true;
        }
        if (!marker_key.empty() && marker_key == bde_key.name) {
	  // Skip marker
	  return true;
	}
	if (!params.prefix.empty()) {
	  // We have a prefix, only match
          if (!bde_key.name.starts_with(params.prefix)) {
            // Prefix doesn't match; skip
	    if (in_prefix) {
              return false;
            }
            return true;
          }
	  // Prefix matches
	  if (params.delim.empty()) {
	    // No delimiter, add matches
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
          }
          auto delim_pos = bde_key.name.find(params.delim, params.prefix.size());
          if (delim_pos == std::string_view::npos) {
	    // Straight prefix match
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
	  }
          results.next_marker =
              bde_key.name.substr(0, delim_pos + params.delim.length());
          if (!results.common_prefixes.contains(results.next_marker.name)) {
            results.common_prefixes[results.next_marker.name] = true;
            count++; // Count will be checked when we exit prefix
            if (in_prefix) {
              // We've hit the next prefix entry.  Check count
              if (count >= max) {
                results.is_truncated = true;
                // Time to stop
                return false;
	      }
            }
          }
          in_prefix = true;
          return true;
        }
        if (!params.delim.empty()) {
	  // Delimiter, but no prefix
	  auto delim_pos = bde_key.name.find(params.delim) ;
          if (delim_pos == std::string_view::npos) {
	    // Delimiter doesn't match, insert
            results.next_marker.set(bde.key);
            results.objs.push_back(bde);
	    count++;
	    if (count >= max) {
              results.is_truncated = true;
	      return false;
	    }
	    return true;
          }
          std::string prefix_key =
              bde_key.name.substr(0, delim_pos + params.delim.length());
          if (!marker_key.empty() && marker_key == prefix_key) {
            // Skip marker
            return true;
          }
	  std::string decoded_key;
	  rgw_obj_key::parse_index_key(prefix_key, &decoded_key, &ns);
          if (!results.common_prefixes.contains(decoded_key)) {
	    if (in_prefix) {
	      // New prefix, check the count
	      count++;
              if (count >= max) {
                results.is_truncated = true;
                return false;
              }
            }
	    in_prefix = true;
            results.common_prefixes[decoded_key] = true;
	    // Fallthrough
          }
	  results.next_marker.name = decoded_key;
	  return true;
        }

        results.next_marker.set(bde.key);
        results.objs.push_back(bde);
        count++;
        if (count >= max) {
          results.is_truncated = true;
          return false;
        }
        return true;
    });

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not list bucket " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    results.objs.clear();
    return ret;
  }

  return 0;
}

int NSFSBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
					Attrs& new_attrs, optional_yield y)
{
  return merge_and_store_attrs(dpp, new_attrs, y, MutationPurpose::client);
}

int NSFSBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
				      Attrs& new_attrs, optional_yield y,
				      MutationPurpose purpose)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, this, purpose);
  if (mg.result() < 0) {
    return mg.result();
  }
  for (auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }

  return write_attrs(dpp, y);
}

/* What a bucket delete raises to remove its directory, and nothing
 * else (raise site 3).
 *
 * `CAP_DAC_OVERRIDE` for the scaffolding under the bucket directory --
 * `.versions` and the `.lock` files in it -- which an identity creates
 * under impersonation at its own 0770, and which the gateway removes
 * with the bucket.  NooBaa removes the same as its process
 * (bucketspace_fs.js:424).  Raised here rather than in
 * delete_directory(), which rename, conversion and multipart
 * completion share (§5.2).  In the gateway's permitted set already,
 * so this names a site and not a new capability.  Taken only after the
 * removal is denied:  a gateway on its own directories never raises,
 * so one started without the capability still deletes its buckets.
 *
 * And `CAP_FOWNER` for the directory itself where its parent is sticky
 * -- a bucket placed under `/tmp` -- since the directory is its
 * creator's (raise site 4) and the sticky bit lets only the owner of
 * the entry or of the parent remove it;  DAC_OVERRIDE does not reach
 * that check.  NooBaa removes it as the account, its owner. */
static constexpr uint64_t BUCKET_DELETE_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE) | nsfs::cap_bit(CAP_FOWNER);

/* Whether a bucket directory's parent is the base path, the gateway's
 * own directory.  Who acts on the bucket directory's entry follows who
 * had to be able to make it (DECISIONS.md 2026-10-24):  in the base
 * path the gateway, which makes the directory for its creator (raise
 * site 4);  under a location the account owns -- its
 * new_buckets_path, or a custom path -- the authenticated user, as
 * NooBaa makes and removes it as the account. */
static bool parent_is_base_path(const std::string& base,
				const std::string& dir_path)
{
  auto trimmed = [](std::string p) {
    while ((p.size() > 1) && (p.back() == '/')) {
      p.pop_back();
    }
    return std::filesystem::path(p).lexically_normal();
  };
  return trimmed(dir_path).parent_path() == trimmed(base);
}

/* What a bucket-state write raises, and nothing else raises (raise
 * site 1):  `CAP_DAC_OVERRIDE` for an attribute on a bucket directory
 * the gateway does not own, and `CAP_FOWNER` for put_info()'s explicit
 * mtime on one, which DAC_OVERRIDE does not grant (DECISIONS.md 2026-10-16,
 * measured).  Taken only after the call is denied:  a gateway on its
 * own directories never raises, so one started without the capability
 * still serves them. */
static constexpr uint64_t BUCKET_STATE_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE) | nsfs::cap_bit(CAP_FOWNER);

/* `fn` on the bucket directory as the gateway, raised if denied.  At the
 * bucket-state operations themselves, not inside FSEnt::write_attrs(),
 * which object attribute writes share (§5.2). */
static int write_bucket_state(const DoutPrefixProvider* dpp,
			      const std::function<int()>& fn)
{
  const int ret = fn();
  if ((ret != -EACCES) && (ret != -EPERM)) {
    return ret;
  }
  return nsfs::with_capabilities(dpp, BUCKET_STATE_CAPS, fn);
}

int NSFSBucket::remove_dir_as(const DoutPrefixProvider* dpp, optional_yield y,
			      const nsfs::FSIdentity& id,
			      const nsfs::AccessStrategy* model,
			      bool delete_children)
{
  const std::filesystem::path path(dir->get_name());
  const std::string leaf = path.filename().string();
  const auto& rn = driver->get_reserved_names();

  /* the parent as the gateway, O_PATH:  the authenticated user acts
   * below it and never resolves the path above, which may not be
   * theirs to search */
  const int pfd = ::open(path.parent_path().c_str(),
			 O_PATH | O_DIRECTORY | O_CLOEXEC);
  if (pfd < 0) {
    return -errno;
  }
  auto close_pfd = make_scope_guard([pfd] { ::close(pfd); });

  int ret;
  if (parent_is_base_path(driver->default_bucket_location(),
			  dir->get_name())) {
    /* what is in it, as the authenticated user, on a descriptor of its
     * own:  a dup of the bucket's would share the offset check_empty()
     * has already read to the end */
    ret = nsfs::with_identity_or_raise(dpp, id, model,
				       nsfs::AccessClass::data,
				       BUCKET_DELETE_CAPS, [&]() -> int {
      const int efd = ::openat(pfd, leaf.c_str(),
			       O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
      if (efd < 0) {
	return -errno;
      }
      return delete_directory_entries(efd, leaf.c_str(), delete_children,
				      dpp, rn);
    }, y);
    if (ret < 0) {
      return ret;
    }
    /* its entry, as the gateway, whose directory the base path is:
     * raise site 3, CAP_FOWNER where the base path is sticky */
    auto rm = [&]() -> int {
      return ((::unlinkat(pfd, leaf.c_str(), AT_REMOVEDIR) < 0) &&
	      (errno != ENOENT)) ? -errno : 0;
    };
    ret = rm();
    if ((ret == -EACCES) || (ret == -EPERM)) {
      ret = nsfs::with_capabilities(dpp, nsfs::cap_bit(CAP_FOWNER), rm);
    }
    return ret;
  }

  /* a location the account owns:  all of it as the authenticated user, as
   * NooBaa's folder_delete() as the account */
  return nsfs::with_identity_or_raise(dpp, id, model,
				      nsfs::AccessClass::data,
				      BUCKET_DELETE_CAPS, [&]() -> int {
    return delete_directory(pfd, leaf.c_str(), delete_children, dpp, rn);
  }, y);
}

int NSFSBucket::remove(const DoutPrefixProvider* dpp,
			bool delete_children,
			optional_yield y,
			const rgw_owner& authenticated_user)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, this);
  if (mg.result() < 0) {
    return mg.result();
  }
  /* A non-empty bucket is rejected whether or not it owns its
   * directory, as S3 rejects one and as NooBaa rejects one over a
   * supplied directory.  Ownership decides only what an allowed delete
   * removes:  the directory, or just the bucket's record.
   *
   * The root object does not keep a bucket alive:  NooBaa judges a
   * bucket empty by listing it, and the root object is never listed
   * (DECISIONS.md 2026-10-13).  delete_directory() counts any `.folder`
   * as content, because in a directory it is that directory's object,
   * so once an owned bucket is known to be empty the root's is removed
   * here, before the directory.  A bucket that does not own its storage
   * keeps it, with the directory. */
  const bool owns = owns_storage(dpp);

  /* Who acts at each layer follows who had to be able to create it
   * (DECISIONS.md 2026-10-24).  The authenticated user, where a request
   * names one under impersonation, is checked at the bucket directory,
   * removes what is below it, and removes its entry from a location the
   * account owns;  the gateway removes its entry from the base path.
   * Each of that user's refusals stands under `posix-strict` and is
   * a model's raise under `s3-native`.  No authenticated user -- the
   * switch off, or a caller with no request -- is the gateway
   * throughout, raised after a denial as before. */
  nsfs::FSIdentity id;
  if (!is_internal() && !nsfs::owner_empty(authenticated_user)) {
    const int pret = pin_personality_for(dpp, driver, authenticated_user,
					 &id, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }
  const nsfs::AccessStrategy* model = driver->get_access_strategy();
  if ((dir->get_fd() < 0) && (dir->open(dpp) < 0)) {
    return -ENOENT;
  }
  const int dir_fd = dir->get_fd();

  /* the request-time check on the bucket directory (DECISIONS.md
   * 2026-10-15):  write and search to remove what is in it, read and
   * search to list one that keeps its directory.  Under a model that
   * hands data to S3 the check's refusal is S3's to override. */
  if (id.active() && !model->permits(nsfs::AccessClass::data)) {
    const int amode = owns ? (W_OK | X_OK) : (R_OK | X_OK);
    const int r = nsfs::with_identity(dpp, id, [dir_fd, amode]() -> int {
      return (::faccessat(dir_fd, "", amode, AT_EMPTY_PATH | AT_EACCESS) < 0)
	  ? -errno : 0;
    }, y);
    if (r < 0) {
      ldpp_dout(dpp, 10) << "nsfs: bucket " << get_name() << ":  uid "
	<< id.cred.uid << " may not delete it:  " << cpp_strerror(-r)
	<< dendl;
      return r;
    }
  }

  if (!delete_children) {
    int r = check_empty(dpp, y);
    if (r < 0) {
      return r;
    }
  }
  /* uploads in progress end with the bucket, as RadosBucket::remove()
   * ends them */
  if (!is_internal()) {
    int r = abort_multiparts_as(dpp, driver->ctx(), y, authenticated_user);
    if (r < 0) {
      return r;
    }
  }
  if (owns && !delete_children) {
    int r = 0;
    if (const auto* ps = path_strategy(); ps) {
      const std::string root_folder = ps->folder_object_name();
      auto unlink_root = [&]() -> int {
	return ((::unlinkat(dir_fd, root_folder.c_str(), 0) < 0) &&
		(errno != ENOENT)) ? -errno : 0;
      };
      if (id.active()) {
	r = nsfs::with_identity_or_raise(dpp, id, model,
					 nsfs::AccessClass::data,
					 RAISED_KEYDIR_CAPS, unlink_root, y);
      } else {
	r = unlink_root();
	if ((r == -EACCES) || (r == -EPERM)) {
	  r = nsfs::with_capabilities(dpp, BUCKET_DELETE_CAPS, unlink_root);
	}
      }
      if (r < 0) {
	ldpp_dout(dpp, removal_log_level(-r)) << removal_log_prefix(-r)
	  << "removing " << root_folder << " from bucket " << get_name()
	  << ": " << cpp_strerror(-r) << dendl;
	return r;
      }
    }
  }
  int ret = 0;
  if (owns && id.active()) {
    ret = remove_dir_as(dpp, y, id, model, delete_children);
  } else if (owns) {
    ret = dir->remove(dpp, y, delete_children);
    if ((ret == -EACCES) || (ret == -EPERM)) {
      ret = nsfs::with_capabilities(dpp, BUCKET_DELETE_CAPS, [&]() -> int {
	return dir->remove(dpp, y, delete_children);
      });
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << ":  its "
	  << "directory could not be removed even raised: "
	  << cpp_strerror(-ret) << dendl;
      }
    }
  } else {
    ret = release_nsfs_state(dpp);
  }
  if (ret < 0) {
    return ret;
  }

  driver->get_bucket_cache()->invalidate_bucket(dpp, get_name());

  ret = driver->get_identity_db()->remove_bucket_placement(dpp, get_name(),
							   get_tenant());
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << " was deleted, "
      << "but its placement row was not:  " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return ret;
}

/* Does this bucket's directory exist for it, or did it predate it?
 *
 * Absent means it owns the directory, which is what every bucket
 * created by the gateway is;  see OWNS_STORAGE_XATTR.  Anything other
 * than "0" is read as owning, so a value nobody wrote and nobody
 * understands does not make a bucket undeletable. */
bool NSFSBucket::owns_storage(const DoutPrefixProvider* dpp)
{
  if (dir->open(dpp) < 0) {
    return true;
  }
  char buf[8];
  ssize_t len = nsfs::fgetxattr(dir->get_fd(), nsfs::OWNS_STORAGE_XATTR,
			    buf, sizeof(buf));
  if (len <= 0) {
    return true;
  }
  return std::string_view(buf, len) != "0";
}

int NSFSBucket::set_owns_storage(const DoutPrefixProvider* dpp, bool owns)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }
  ret = write_bucket_state(dpp, [&]() -> int {
    if (owns) {
      return ((::fremovexattr(dir->get_fd(), nsfs::OWNS_STORAGE_XATTR) < 0) &&
	      (errno != ENODATA)) ? -errno : 0;
    }
    return (::fsetxattr(dir->get_fd(), nsfs::OWNS_STORAGE_XATTR, "0", 1,
			0) < 0) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: recording whether bucket " << get_name()
      << " owns its storage: " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

/* Remove the bucket without removing the directory it was made over,
 * by removing what nsfs wrote to that directory.
 *
 * It leaves the directory as it was beforehand:  present, found by the
 * base path's readdir, and carrying nothing of nsfs's, which is what
 * carries_nsfs_state() checks.  That is the same state every directory
 * nsfs has not made a bucket of is in, so this introduces no condition
 * the driver did not already have to handle.
 *
 * Of the attributes, the whole nsfs namespace goes, not a list of
 * names.  user.nsfs. is exactly what the gateway wrote, so naming the
 * prefix cannot leave behind an attribute a later version adds;
 * anything of NooBaa's, or anyone else's, is not the gateway's to
 * remove. */
int NSFSBucket::release_nsfs_state(const DoutPrefixProvider* dpp)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  nsfs::xattr_map_t present;
  ret = driver->get_fs_strategy()->get_xattrs(dpp, dir->get_fd(), present,
					      null_yield, nsfs::FSIdentity::none());
  if (ret < 0) {
    return ret;
  }

  static constexpr std::string_view nsfs_prefix{"user.nsfs."};
  for (const auto& [name, _] : present) {
    if (! std::string_view(name).starts_with(nsfs_prefix)) {
      continue;
    }
    ret = write_bucket_state(dpp, [&]() -> int {
      return ((::fremovexattr(dir->get_fd(), name.c_str()) < 0) &&
	      (errno != ENODATA)) ? -errno : 0;
    });
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: removing " << name << " from bucket "
	<< get_name() << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  /* The version lockfile too:  it is the gateway's mutex, which NooBaa
   * never writes, and no object.  `.versions` stays:  it is NooBaa's
   * layout as much as ours, so it is not ours alone to remove. */
  ret = write_bucket_state(dpp, [&]() -> int {
    return ((::unlinkat(dir->get_fd(), VERSIONS_LOCKFILE.c_str(), 0) < 0) &&
	    (errno != ENOENT)) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: removing " << VERSIONS_LOCKFILE
      << " from bucket " << get_name() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ldpp_dout(dpp, 1) << "nsfs: bucket " << get_name()
    << " removed;  its directory predates the bucket and is left in "
    << "place with its contents" << dendl;
  return 0;
}

int NSFSBucket::remove_bypass_gc(int concurrent_max,
				  bool keep_index_consistent,
				  optional_yield y,
				  const DoutPrefixProvider *dpp)
{
  return remove(dpp, true, y, rgw::sal::no_authenticated_user);
}

/* The extensions marker, read with its own fgetxattr.
 *
 * Not through XattrStrategy:  choosing the strategy is what the marker
 * decides, and a noobaa strategy would not claim a user.nsfs.* name at
 * all.  See bucket_profile.h. */
int NSFSBucket::resolve_profile(const DoutPrefixProvider* dpp)
{
  if (profile) {
    return 0;
  }

  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  char buf[32];
  uint32_t extensions = nsfs::EXTENSIONS_NONE;
  ssize_t len = nsfs::fgetxattr(dir->get_fd(), nsfs::EXTENSIONS_XATTR,
			    buf, sizeof(buf) - 1);
  if (len < 0) {
    if (errno == ENOTSUP) {
      /* The mount cannot tell us, so every bucket on it
       * would resolve to base -- including a strong one, whose shadow
       * subtree we would then expose as a key and whose attributes we
       * would write under NooBaa's names.  A filesystem without xattrs
       * cannot hold the nsfs format at all, so rejecting it costs nothing
       * that could have been served. */
      ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << " is on a "
	<< "filesystem which does not support " << nsfs::EXTENSIONS_XATTR
	<< ";  not serving it, because every profile would read as "
	<< "base" << dendl;
      return -ERR_NOT_IMPLEMENTED;
    }
    if (errno != ENODATA) {
      ret = -errno;
      ldpp_dout(dpp, 0) << "ERROR: reading " << nsfs::EXTENSIONS_XATTR
	<< " on bucket " << get_name() << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    /* No attribute is the base profile:  a NooBaa tree, or one nsfs
     * wrote and serves in NooBaa's format.  Nothing in the directory
     * refines that.  A name identifies nsfs scaffolding only where nsfs
     * is the only writer, and base is the profile for trees somebody else
     * wrote, so a directory called `.shadow` in one of them was put
     * there by whoever wrote the tree.
     *
     * The format still has to reach the directory.  Its strategies
     * are what a listing reads entries with, and without this the
     * directory keeps the driver's seed, which is the nsfs format's:
     * every NooBaa object listed with no owner, and its names were read
     * by the nsfs path conventions. */
    profile = driver->get_base_profile();
    apply_format();
    ldpp_dout(dpp, 20) << "bucket " << get_name() << " profile "
      << profile->name() << dendl;
    return 0;
  } else {
    buf[len] = '\0';
    char* end = nullptr;
    errno = 0;
    unsigned long v = ::strtoul(buf, &end, 10);
    if (errno || (end == buf) || (*end != '\0') || (v > UINT32_MAX)) {
      ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << " carries an "
	<< "unreadable " << nsfs::EXTENSIONS_XATTR << " (\"" << buf
	<< "\");  not serving it" << dendl;
      /* ERR_NOT_IMPLEMENTED, not ENOTSUP:  the latter has no entry in
       * rgw_common.cc's table and falls through to 500/UnknownError,
       * which tells a client we malfunctioned.  This is a deliberate
       * rejection and 501 says so. */
      return -ERR_NOT_IMPLEMENTED;
    }
    extensions = (uint32_t)v;
  }

  profile = driver->resolve_profile(extensions);
  if (! profile) {
    ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << " was written with "
      << "extension set " << extensions << ", which this gateway does not "
      << "implement (it implements " << nsfs::EXTENSIONS_KNOWN
      << ");  not serving it" << dendl;
    /* 501 NotImplemented, and terminal.  Not -ENOTSUP:  it has no entry
     * in rgw_common.cc's table, falls through to 500/UnknownError, and
     * that is malformed enough that clients abend rather than report --
     * `aws s3 ls` raises "argument of type 'NoneType' is not iterable".
     * 500 also tells every SDK to retry something that cannot succeed.
     *
     * Terminal is right only while a deployment is one gateway.  Once
     * several serve one filesystem, a bucket this node cannot read may
     * be readable by the node beside it -- mid-upgrade, or after a
     * partial rollback -- and a retryable 503 becomes the better response,
     * because the retry can land somewhere that succeeds.  Revisit when
     * multi-instance works;  it cannot start today. */
    return -ERR_NOT_IMPLEMENTED;
  }

  /* And whether a foreign reader is still needed.  On the same fd and
   * once per resolve, so it costs nothing per object.  Only a marked
   * bucket can be converting -- base IS NooBaa's format and reads it
   * directly -- so this is not asked of one. */
  converting = false;
  if (profile->extended()) {
    char cbuf[32];
    ssize_t clen = nsfs::fgetxattr(dir->get_fd(), nsfs::CONVERTING_XATTR,
			       cbuf, sizeof(cbuf) - 1);
    if (clen > 0) {
      std::string from(cbuf, clen);
      if (from == nsfs::CONVERTING_FROM_NOOBAA) {
	converting = true;
      } else {
	/* A format we do not have a reader for.  The objects written in
	 * it are unreadable whatever we do, and serving them through
	 * the wrong parser turns that into wrong results. */
	ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << " is being "
	  << "converted from \"" << from << "\", which this gateway "
	  << "cannot read;  not serving it" << dendl;
	profile = nullptr;
	return -ERR_NOT_IMPLEMENTED;
      }
    }
  }

  apply_format();

  ldpp_dout(dpp, 20) << "bucket " << get_name() << " profile "
    << profile->name() << (converting ? " (converting from noobaa)" : "")
    << dendl;
  return 0;
}

/* Set or clear it.  An empty `from` removes the attribute, which is
 * what finishing the conversion does. */
int NSFSBucket::mark_converting(const DoutPrefixProvider* dpp)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  /* The value is the format being converted FROM, and NooBaa's is the
   * only other format there is;  `load()` rejects a value it has no
   * reader for rather than guessing. */
  const std::string v{nsfs::CONVERTING_FROM_NOOBAA};
  ret = write_bucket_state(dpp, [&]() -> int {
    return (::fsetxattr(dir->get_fd(), nsfs::CONVERTING_XATTR,
			v.data(), v.size(), 0) < 0) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: writing " << nsfs::CONVERTING_XATTR
      << " on bucket " << get_name() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  converting = true;
  /* the fallbacks the subtree inherits follow the marker */
  apply_format();
  return 0;
}

int NSFSBucket::clear_converting(const DoutPrefixProvider* dpp)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  /* Absent already is the state being asked for, so ENODATA is not an
   * error -- a resumed run clears a marker a previous one removed. */
  ret = write_bucket_state(dpp, [&]() -> int {
    return ((::fremovexattr(dir->get_fd(), nsfs::CONVERTING_XATTR) < 0) &&
	    (errno != ENODATA)) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: removing " << nsfs::CONVERTING_XATTR
      << " from bucket " << get_name() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  converting = false;
  /* the fallbacks the subtree inherits follow the marker */
  apply_format();
  return 0;
}

/* Remove the extensions attribute, which is how a bucket reaches base.
 * Reached through set_profile(), which is where the conditions on
 * getting there are enforced. */
int NSFSBucket::unmark_extensions(const DoutPrefixProvider* dpp,
				  const UnmarkPasskey&)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = write_bucket_state(dpp, [&]() -> int {
    return ((::fremovexattr(dir->get_fd(), nsfs::EXTENSIONS_XATTR) < 0) &&
	    (errno != ENODATA)) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: removing " << nsfs::EXTENSIONS_XATTR
      << " from bucket " << get_name() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  profile = driver->get_base_profile();
  apply_format();
  return 0;
}

/* Set a bucket's profile.
 *
 * Adding extensions is a write and nothing else.  Shared and strong both
 * hold everything base does, so the attribute is the whole change and
 * the tree is already in the shape the new profile describes.
 *
 * Dropping them is a conversion.  The structures the dropped extensions
 * allowed are still in the tree afterwards, and nothing walks every
 * object to take an S3 ACL back off it, so the bucket has to hold no
 * content:  check_empty() decides that with the content-versus-
 * scaffolding rules the delete path already uses.  The scaffolding those
 * extensions own is then removed, because check_empty() counts an empty
 * .shadow as scaffolding and a bucket must never end up claiming a
 * profile while holding structure the profile does not describe.
 *
 * Leaving base takes a tree nsfs did not write into its management.  It
 * is never inferred from access, and nothing returns a bucket to base.
 */
nsfs::XattrStrategy* NSFSBucket::xattr_strategy() const
{
  auto* f = get_format();
  return (f && f->xattr_strategy) ? f->xattr_strategy
				  : driver->get_xattr_strategy();
}

nsfs::BucketStateStrategy* NSFSBucket::bucket_state_strategy() const
{
  auto* f = get_format();
  return (f && f->bucket_state_strategy) ? f->bucket_state_strategy
					 : driver->get_bucket_state_strategy();
}

nsfs::BucketStateStrategy* NSFSBucket::bucket_state_fallback() const
{
  /* only a base bucket has anything in NooBaa's store to fall back to;
   * a marked tree is one nsfs wrote */
  const auto* p = get_profile();
  if (p && (p->extensions != nsfs::EXTENSIONS_NONE)) {
    return nullptr;
  }
  return driver->get_noobaa_bucket_state_strategy();
}

nsfs::PathStrategy* NSFSBucket::path_strategy() const
{
  auto* f = get_format();
  return (f && f->path_strategy) ? f->path_strategy
				 : driver->get_path_strategy();
}

nsfs::MPUStrategy* NSFSBucket::mpu_strategy() const
{
  auto* f = get_format();
  return (f && f->mpu_strategy) ? f->mpu_strategy
				: driver->get_mpu_strategy();
}

/* An FSEnt carries its strategies and a child inherits its parent's
 * (see the constructors at the top of this file), so this is the one
 * place a subtree learns which format its bucket is in.
 *
 * The MPU member follows mpu_strategy():  a format which names no
 * staging layout takes the driver's, because per-part against strided
 * is a capability question and not a format one. */
void NSFSBucket::apply_format()
{
  if (!dir) {
    return;
  }
  dir->set_xattr_strategy(xattr_strategy());
  dir->set_path_strategy(path_strategy());
  dir->set_mpu_strategy(mpu_strategy());
  dir->set_xattr_fallback(xattr_fallback());
  dir->set_path_fallback(path_fallback());
  /* the bucket's own state, in the nsfs spelling whatever the format */
  dir->set_own_xattr_strategy(driver->get_xattr_strategy());
}

/* The fallback strategy.  Null unless the bucket is mid-upgrade, so
 * on every other bucket it is not merely unused -- there is nothing
 * there to call. */
nsfs::XattrStrategy* NSFSBucket::xattr_fallback() const
{
  return converting ? driver->get_noobaa_xattr_strategy() : nullptr;
}

nsfs::PathStrategy* NSFSBucket::path_fallback() const
{
  return converting ? driver->get_noobaa_path_strategy() : nullptr;
}

nsfs::MPUStrategy* NSFSBucket::mpu_fallback() const
{
  return converting ? driver->get_noobaa_mpu_strategy() : nullptr;
}

int NSFSBucket::set_profile(const DoutPrefixProvider* dpp, optional_yield y,
			    uint32_t target,
			    nsfs::convert_cb_t* cb,
			    nsfs::ConvertProgress* progress)
{
  if (! nsfs::is_named_profile(target)) {
    ldpp_dout(dpp, 0) << "ERROR: not setting bucket " << get_name()
      << " to extension set " << target << ":  no profile names it"
      << dendl;
    return -EINVAL;
  }

  int ret = resolve_profile(dpp);
  if (ret < 0) {
    return ret;
  }

  const uint32_t current = profile->extensions;

  /* Nothing takes a bucket back into base (DECISIONS.md 2026-10-21):
   * adoption is one-way, and an RGW bucket in NooBaa's format is one
   * nothing may write.  Rejected for every caller -- the profile
   * endpoint, the `bucket-profile` hint, adoption -- before anything is
   * removed.  A bucket already in base asking for base is the no-op
   * below. */
  if ((target == nsfs::EXTENSIONS_BASE) && (current != target)) {
    ldpp_dout(dpp, 0) << "ERROR: not returning bucket " << get_name()
      << " to the base profile:  adoption is one-way" << dendl;
    return -ERR_INVALID_BUCKET_STATE;
  }

  if (target == current) {
    /* Asking for the profile a bucket is already in is a no-op --
     * unless it is still converting, in which case it is a request to
     * finish.  That is what resuming a stopped run looks like from
     * outside:  the same call, issued again.  Without this the
     * re-issue returned success and did nothing, and the marker
     * stayed for ever. */
    if (progress) {
      /* the bucket is in the target profile either way -- that is
       * what makes this a no-op */
      progress->profile_set = true;
      progress->complete = !converting;
    }
    if (converting) {
      return convert_tree(dpp, y, cb, progress);
    }
    return 0;
  }

  if (target & ~current) {
    if (current == nsfs::EXTENSIONS_BASE) {
      /* Leaving base is adopting the bucket (DECISIONS.md 2026-10-11),
       * so its NooBaa record is imported first:  owner, versioning,
       * policy and the rest arrive as nsfs bucket state, and the
       * conversion below records that owner on the objects.  A bucket
       * NooBaa holds no record of has nothing to import and converts
       * with what load_bucket() reconstructed.
       *
       * The import writes through its own handle, so this one is
       * reloaded to carry what it wrote;  write_attrs() below would
       * otherwise put the pre-import state back. */
      ldpp_dout(dpp, 1) << "nsfs: bucket " << get_name()
	<< " leaves the base profile for extension set " << target << dendl;
      ret = driver->import_noobaa_bucket(dpp, y, get_name(), nullptr);
      if ((ret < 0) && (ret != -ENOENT)) {
	ldpp_dout(dpp, 0) << "ERROR: not converting bucket "
	  << get_name() << ":  its NooBaa record could not be imported: "
	  << cpp_strerror(-ret) << dendl;
	return ret;
      }
      if (ret == 0) {
	/* Cleared first:  load_bucket() reads into `attrs` without
	 * replacing a name already held, so this handle's NooBaa-format
	 * policy, its principals unrewritten, would survive the reload
	 * and write_attrs() below would put it back over the import's. */
	attrs.clear();
	ret = load_bucket(dpp, y);
	if (ret < 0) {
	  return ret;
	}
	default_owner.reset();
      }
    }
  }

  if (current & ~target) {
    ret = check_empty(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: not reducing bucket " << get_name()
	<< " from " << profile->name() << " to extension set " << target
	<< ":  it holds content, and the structures the dropped extensions "
	<< "allowed stay in the tree" << dendl;
      return ret;
    }
    if ((current & nsfs::EXT_SHADOW) && !(target & nsfs::EXT_SHADOW)) {
      ret = write_bucket_state(dpp, [&]() -> int {
	return ((::unlinkat(dir->get_fd(), HIDDEN_SHADOW_PATH.c_str(),
			    AT_REMOVEDIR) < 0) && (errno != ENOENT)) ? -errno : 0;
      });
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: removing " << HIDDEN_SHADOW_PATH
	  << " from bucket " << get_name() << ": " << cpp_strerror(-ret)
	  << dendl;
	return ret;
      }
    }
  }


  if (current != nsfs::EXTENSIONS_BASE) {
    /* Between two nsfs profiles.  Both formats are rgw-meta, so nothing in
     * the tree is in the wrong one and there is nothing to convert --
     * only the mask changes, and what the dropped extensions owned was
     * removed above. */
    ret = mark_extensions(dpp, target);
    if ((ret == 0) && progress) {
      progress->profile_set = true;
      progress->complete = true;
    }
    return ret;
  }

  /* Leaving base:  the upgrade.
   *
   * First, persist the bucket state this bucket is being served with.
   *
   * A base bucket's state comes from NooBaa's record through
   * bucket_state_fallback(), and that fallback is consulted only for
   * EXTENSIONS_NONE -- so the moment the mask goes down NooBaa's record
   * stops being read.  Without this write, load_bucket() then finds
   * no nsfs state, falls back to the seeded defaults, and a
   * versioned bucket reads as unversioned:  the next PUT overwrites in
   * place instead of demoting into .versions/, and object lock, policy
   * and the public access block all revert to permissive.
   *
   * Before the markers, not after, because the failure is then
   * harmless:  the bucket stays base and keeps reading NooBaa's record.
   * Writing after the mask would leave a marked bucket with no state
   * if it failed, and re-issuing set_profile() would not retry it --
   * the re-issue takes the target == current path, not this one.
   *
   * The bucket's state is written in the nsfs spelling whatever the
   * format (DECISIONS.md 2026-10-13), so it needs no rewriting
   * afterwards. */
  if (current == nsfs::EXTENSIONS_BASE) {
    ret = write_attrs(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: not converting bucket " << get_name()
	<< ":  its state could not be written, and converting would lose "
	<< "what NooBaa's record was supplying: " << cpp_strerror(-ret)
	<< dendl;
      return ret;
    }
  }

  /* The marker goes down before the mask, and the order is the crash
   * order.  Marked with no marker is a bucket in the nsfs format full
   * of objects in NooBaa's, with no reader that can see them;  the marker
   * with no mask is a base bucket carrying an attribute base does not
   * consult, which is nothing.  So the harmless one is written first.
   */
  ret = mark_converting(dpp);
  if (ret < 0) {
    return ret;
  }
  ret = mark_extensions(dpp, target);
  if (ret < 0) {
    /* the marker is inert on a base bucket;  leaving it costs a
     * read of one attribute on the next resolve */
    return ret;
  }
  if (progress) {
    progress->profile_set = true;
  }

  return convert_tree(dpp, y, cb, progress);
}

const char* nsfs::to_string(nsfs::ConvertEvent::Kind k)
{
  switch (k) {
  case nsfs::ConvertEvent::Kind::bucket:           return "bucket";
  case nsfs::ConvertEvent::Kind::object:           return "object";
  case nsfs::ConvertEvent::Kind::directory_object: return "directory object";
  case nsfs::ConvertEvent::Kind::version:          return "version";
  case nsfs::ConvertEvent::Kind::upload:           return "upload";
  case nsfs::ConvertEvent::Kind::part:             return "part";
  }
  return "?";
}

const char* nsfs::to_string(nsfs::ConvertEvent::Phase p)
{
  switch (p) {
  case nsfs::ConvertEvent::Phase::before:  return "before";
  case nsfs::ConvertEvent::Phase::written: return "written";
  case nsfs::ConvertEvent::Phase::pruned:  return "pruned";
  }
  return "?";
}

/* What the conversion's attribute rewrite raises, and nothing else.
 *
 * `CAP_DAC_OVERRIDE` alone.  The walk runs as the gateway on an
 * operator's instruction (see convert_attrs()), and an object written
 * under impersonation is its writer's at `0660`, so the gateway can
 * read it -- it holds `CAP_DAC_READ_SEARCH` for its whole life -- and
 * cannot write an attribute to it.  Without this the first object
 * stops the walk with EACCES and the converting marker stays.
 *
 * No `CAP_FOWNER`, which the two other raise sites pair with this one.
 * It answers only `xattr_permission()`'s sticky-directory gate, and
 * that gate needs a sticky directory the gateway does not own:  ours
 * are the gateway's (impersonation design §8a) and NooBaa's are `0777`
 * with no sticky bit (NOOBAA_WRITE_SURFACE.md).  Nothing produces the
 * intersection, so naming FOWNER here would raise a capability no call
 * under it can use.
 *
 * In the gateway's permitted set already, so this names a site and not
 * a new capability. */
static constexpr uint64_t CONVERT_REWRITE_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE);

/* What the three inherit_ids() calls raise, and nothing else.
 *
 * `CAP_CHOWN` alone, and only on an adopted tree.  Conversion gives
 * every file it writes the uid of the thing it re-expresses;  on a tree
 * this gateway laid down that is already the gateway's, so
 * inherit_ids() returns before the `fchown` and the raise costs a
 * capset pair and nothing else.  On NooBaa's it is whoever NooBaa ran
 * as, and without this conversion fails rather than leave an owner
 * that is wrong.
 *
 * No `CAP_DAC_OVERRIDE`:  `fchown` is gated on ownership, not on the
 * file's mode.  No `CAP_FOWNER` either -- inherit_ids() is called after
 * the mode is set and while the file is still the gateway's, which is
 * the whole reason one capability suffices there.
 *
 * Raised at each call rather than inside inherit_ids(), which is the
 * one helper in conversion general enough for a later caller to reach;
 * a caller that needs the raise should name it (§5.2). */
static constexpr uint64_t CONVERT_CHOWN_CAPS =
  nsfs::cap_bit(CAP_CHOWN);

/* Rewrite one file's attributes into the bucket's format.
 *
 * Not through FSEnt::write_attrs(), which does the write and the prune
 * in one call.  The phases have to be separable:  `written` -- both
 * spellings on disk -- is the only state the conversion creates that
 * did not exist before, and a caller that cannot be held there cannot
 * observe it.
 *
 * `fd` is the file carrying the attributes, which for a directory
 * object in the nsfs format is the sentinel and in NooBaa's is the directory.
 */
static int convert_attrs(const DoutPrefixProvider* dpp, optional_yield y,
			 int fd, const std::string& display,
			 nsfs::FSStrategy* fs,
			 const nsfs::XattrStrategy* xs,
			 const nsfs::XattrStrategy* xattr_fallback,
			 const ACLOwner& bucket_owner,
			 nsfs::convert_cb_t* cb,
			 const nsfs::ConvertEvent& base_ev,
			 const std::vector<std::string>* leave_alone = nullptr)
{
  Attrs attrs;
  std::vector<std::string> unknown;
  int ret = get_x_attrs(y, dpp, fd, attrs, display, xs, xattr_fallback, {},
			{}, &unknown);
  if (ret < 0) {
    return ret;
  }
  /* X4's third layer:  an attribute no reviewed release writes stops
   * the bucket's conversion, which resumes once the release is
   * reviewed */
  if (!unknown.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: not converting " << display << ":  it "
      << "carries " << unknown.front()
      << (unknown.size() > 1 ? " and others" : "")
      << ", which no reviewed release of NooBaa writes" << dendl;
    return -ERR_INVALID_OBJECT_STATE;
  }

  nsfs::xattr_map_t to_write;
  std::vector<std::string> to_remove;
  for (const auto& [key, bl] : attrs) {
    if (leave_alone &&
	(std::find(leave_alone->begin(), leave_alone->end(), key)
	 != leave_alone->end())) {
      continue;
    }
    const std::string disk_name = xs->disk_name(key);
    const std::string foreign = xattr_fallback->disk_name(key);
    if (!disk_name.empty()) {
      to_write.try_emplace(disk_name, attr_on_disk(xs, key, bl));
    }
    if (!foreign.empty() && (foreign != disk_name)) {
      to_remove.push_back(foreign);
    }
  }

  /* Record the owner:  the bucket's (DECISIONS.md 2026-10-11).
   *
   * NooBaa records no per-object owner;  `get_default_object_owner()`
   * returns the bucket's `owner_account` for every object, and the
   * account import has made that an RGW account.  Adoption is where the
   * tree becomes consistent with the imported accounts, so an object
   * that records no owner is given its bucket's here -- the owner an
   * unconverted listing shows, and the one RGW stamps on a write into a
   * BucketOwnerEnforced bucket.
   * Never the file's uid, which is the POSIX identity it was written
   * as, not an S3 owner.
   *
   * What it writes is RGW's default policy and not an owner alone,
   * because an owner-with-no-grants is a shape RGW never produces.
   *
   * Objects and versions.  A version entry is an object's earlier
   * content and records its owner exactly the same way, so it needs
   * the same capture;  a delete marker included, because RGW emits an
   * <Owner> for one -- rgw_rest_s3.cc's version listing calls
   * dump_owner() outside the is_delete_marker() guard, which suppresses
   * only ETag, Size and StorageClass.  No bytes does not mean no owner.
   *
   * A bucket's owner is not an inode fact and arrives with the account
   * import;  a directory object does not come through here at all,
   * because its attributes move from the directory to the sentinel
   * rather than being rewritten in place, and convert_directory()
   * captures its owner where that move happens. */
  if (((base_ev.kind == nsfs::ConvertEvent::Kind::object) ||
       (base_ev.kind == nsfs::ConvertEvent::Kind::version)) &&
      (attrs.find(RGW_ATTR_ACL) == attrs.end())) {
    RGWAccessControlPolicy policy;
    policy.create_default(bucket_owner.id, bucket_owner.display_name);
    bufferlist aclbl;
    policy.encode(aclbl);
    const std::string disk_name = xs->disk_name(RGW_ATTR_ACL);
    if (!disk_name.empty()) {
      to_write.insert_or_assign(disk_name, attr_on_disk(xs, RGW_ATTR_ACL,
						   aclbl));
    }
  }

  if (cb) {
    nsfs::ConvertEvent ev = base_ev;
    ev.phase = nsfs::ConvertEvent::Phase::before;
    ret = (*cb)(dpp, ev);
    if (ret != 0) {
      return ret;
    }
  }

  if (!to_write.empty()) {
    ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
      return fs->set_xattrs(dpp, fd, to_write, y);
    });
    if (ret < 0) {
      return ret;
    }
  }

  if (cb) {
    nsfs::ConvertEvent ev = base_ev;
    ev.phase = nsfs::ConvertEvent::Phase::written;
    ret = (*cb)(dpp, ev);
    if (ret != 0) {
      return ret;
    }
  }

  if (!to_remove.empty()) {
    /* the upgrade command's own rewrite, not a request:  it runs on
     * an operator's instruction across objects of many identities,
     * so it uses the gateway's credentials deliberately */
    ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
      return fs->remove_xattrs(dpp, fd, to_remove, y);
    });
    if (ret < 0) {
      return ret;
    }
  }

  if (cb) {
    nsfs::ConvertEvent ev = base_ev;
    ev.phase = nsfs::ConvertEvent::Phase::pruned;
    ret = (*cb)(dpp, ev);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

/* Split a ".versions/" entry into the object and the version id.
 *
 * "<leaf>_<version-id>", and a leaf may itself contain '_', so the
 * split is the rightmost separator whose suffix is a version id.  The
 * test is the one list_leaf_versions() applies:  "null", or the
 * "mtime-" form. */
static bool split_version_entry(const std::string& entry,
				std::string& leaf, std::string& vid)
{
  size_t pos = entry.rfind('_');
  while (pos != std::string::npos) {
    const std::string cand = entry.substr(pos + 1);
    if ((cand == NULL_VERSION_ID) || cand.starts_with("mtime-")) {
      if (pos == 0) {
	return false; /* no leaf */
      }
      leaf = entry.substr(0, pos);
      vid = cand;
      return true;
    }
    if (pos == 0) {
      break;
    }
    pos = entry.rfind('_', pos - 1);
  }
  return false;
}

/* The version entries of one directory.
 *
 * .versions/ is a reserved name, so the walk in convert_directory()
 * skips it, and until this existed the conversion skipped the versions
 * with it.  That was not a cosmetic gap.  Every read resolves an
 * attribute name through the *bucket's* strategy, there is one
 * strategy per bucket and no chaining reader, so once the mask is
 * written a user.noobaa.* name on a version entry is invisible:  a
 * NooBaa delete marker stops reading as a delete marker and the object
 * it deleted comes back, 200 with no bytes where S3 owes 404.
 *
 * A version also needs its owner recorded, like any object:  it is one
 * of the kinds convert_attrs() records the bucket's owner for.
 *
 * Flat:  the entries belong to leaves in this directory, so there is
 * nothing to recurse into.  Anything that is not a regular file is
 * left alone -- the lock file is nsfs's, and a subdirectory here is a
 * shape neither format produces.
 */
static int convert_versions(const DoutPrefixProvider* dpp, optional_yield y,
			    nsfs::Directory* d, CephContext* cct,
			    const std::string& prefix,
			    nsfs::FSStrategy* fs,
			    const nsfs::XattrStrategy* xs,
			    const nsfs::XattrStrategy* xattr_fallback,
			    const ACLOwner& bucket_owner,
			    nsfs::convert_cb_t* cb,
			    nsfs::ConvertProgress& progress)
{
  /* Checked before opening because most directories have no history,
   * and Directory::open() logs at level 0 on failure -- an ENOENT per
   * directory would bury the run's real output. */
  struct statx vstx;
  if (nsfs::statx(d->get_fd(), HIDDEN_VERSIONS_PATH.c_str(),
		  AT_SYMLINK_NOFOLLOW, STATX_MODE, &vstx) < 0) {
    return 0;
  }
  if (!S_ISDIR(vstx.stx_mode)) {
    return 0;
  }

  nsfs::Directory vers(HIDDEN_VERSIONS_PATH, d, cct);
  int ret = vers.open(dpp);
  if (ret < 0) {
    /* it is there and unreadable, which is the case this pass must not
     * pass over silently */
    return ret;
  }

  std::vector<std::string> entries;
  ret = vers.for_each(dpp, [&](const char* name) {
    std::string_view n(name);
    if ((n == ".") || (n == "..") || (n == VERSIONS_LOCK_NAME)) {
      return 0;
    }
    struct statx stx;
    if (nsfs::statx(vers.get_fd(), name, AT_SYMLINK_NOFOLLOW, STATX_MODE,
	      &stx) < 0) {
      return 0;
    }
    if (S_ISREG(stx.stx_mode)) {
      entries.emplace_back(name);
    }
    return 0;
  });
  if (ret < 0) {
    return ret;
  }

  for (const auto& name : entries) {
    int fd = ::openat(vers.get_fd(), name.c_str(), O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
      /* deleted under us;  one version, not a failed conversion */
      continue;
    }
    auto close_fd = make_scope_guard([fd] { ::close(fd); });

    nsfs::ConvertEvent ev{};
    ev.kind = nsfs::ConvertEvent::Kind::version;
    std::string leaf;
    if (!split_version_entry(name, leaf, ev.version_id)) {
      /* A name neither format produces.  Still converted, because the
       * attributes are the whole point and leaving them is the defect;
       * reported under the entry name, which is all there is. */
      leaf = name;
    }
    ev.key = prefix + leaf;
    ev.index = progress.objects + progress.directory_objects
      + progress.versions;

    ret = convert_attrs(dpp, y, fd, ev.key, fs, xs, xattr_fallback,
			bucket_owner, cb, ev);
    if (ret != 0) {
      return ret;
    }
    ++progress.versions;
  }

  return 0;
}

/* Convert one directory object from NooBaa's format.
 *
 * `dir_fd` is the directory the object is stored at, and `key` its key,
 * for the log and the callback.  Shared by the sweep and by a client
 * write, which converts the object it is about to change before changing
 * it (DECISIONS.md 2026-10-13), so the two cannot differ about what a
 * converted directory object is.
 *
 * A directory object in NooBaa's format:
 *
 * NooBaa's marker is user.noobaa.dir_content on the directory and
 * nsfs's is the sentinel file, and the attributes move with it:
 * NooBaa's live on the directory, nsfs's on `.folder`.  So
 * converting one is a move, not a rename of an attribute -- create
 * the sentinel if the object was empty, carry the attributes
 * across, and drop dir_content.
 *
 * The sentinel is created before the attributes are written, so a
 * reader that arrives between the two sees an empty object rather
 * than none.  A partial conversion is visible either way;  this is
 * the ordering that makes it visible as the object it is. */
static int convert_directory_object(const DoutPrefixProvider* dpp,
				    optional_yield y, int dir_fd,
				    const std::string& key,
				    nsfs::FSStrategy* fs,
				    const nsfs::XattrStrategy* xs,
				    const nsfs::XattrStrategy* xattr_fallback,
				    const ACLOwner& bucket_owner,
				    const nsfs::PathStrategy* path_fallback,
				    nsfs::convert_cb_t* cb,
				    nsfs::ConvertProgress& progress,
				    bool no_chown)
{
  int ret;
  const std::string sname = path_fallback->folder_object_name();

  /* An empty object has no sentinel, and the one created below is a new
   * inode with this moment's mtime.  The reader reports such an object
   * with the directory's stat, so that stat is taken first -- creating
   * the sentinel changes the directory's mtime -- and the object keeps
   * the ETag and Last-Modified it had (DECISIONS.md 2026-10-13). */
  struct statx probe{};
  const bool empty =
    (nsfs::statx(dir_fd, sname.c_str(), AT_SYMLINK_NOFOLLOW, STATX_INO,
	     &probe) < 0) && (errno == ENOENT);
  struct statx reported{};
  if (empty &&
      (nsfs::statx(dir_fd, "", AT_EMPTY_PATH, STATX_BASIC_STATS,
		   &reported) < 0)) {
    ret = -errno;
    ldpp_dout(dpp, 0) << "ERROR: cannot stat " << key << ": "
		      << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* NooBaa's object mode, as every object nsfs creates carries
   * (DECISIONS.md 2026-10-11).  Restored straight after the open,
   * while the gateway still owns the file, because the ids are
   * taken from the directory below.
   *
   * Raised, as the directory's other writes below are:  in an adopted
   * tree the directory is whoever NooBaa ran as, at `0770`. */
  int sfd = -1;
  ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
    sfd = ::openat(dir_fd, sname.c_str(),
		   O_RDONLY | O_NOFOLLOW | O_CREAT, nsfs::file_create_mode());
    return (sfd < 0) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: creating " << sname << " under "
		      << key << ": " << cpp_strerror(-ret)
		      << dendl;
    return ret;
  }
  auto close_sfd = make_scope_guard([sfd] { ::close(sfd); });
  ret = nsfs::apply_create_mode(dpp, sfd, nsfs::file_create_mode(), y);
  if (ret < 0) {
    return ret;
  }
  if (empty) {
    const struct timespec times[2] = {
      { 0, UTIME_OMIT },
      { reported.stx_mtime.tv_sec, reported.stx_mtime.tv_nsec },
    };
    if (::futimens(sfd, times) < 0) {
      ret = -errno;
      ldpp_dout(dpp, 0) << "ERROR: giving " << sname << " under " << key
	<< " its directory's mtime: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  /* The sentinel re-expresses the directory, so it takes the
   * directory's uid and gid. */
  struct statx dstx{};
  ret = statx_ids(dir_fd, dstx);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: cannot stat " << key
      << " to give " << sname << " its owner: "
      << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = nsfs::with_capabilities(dpp, CONVERT_CHOWN_CAPS, [&]() -> int {
    return inherit_ids(dpp, sfd, dstx, no_chown, sname);
  });
  if (ret < 0) {
    /* The sentinel exists by now, and the whole point of failing is
     * not to leave a file whose owner is wrong.  Take it back out --
     * the directory object is still in NooBaa's format and still readable
     * through the fallback, which is where a stopped conversion
     * leaves everything else.  Raised, as its creation was. */
    const int uret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS,
					     [&]() -> int {
      return (::unlinkat(dir_fd, sname.c_str(), 0) < 0) ? -errno : 0;
    });
    if (uret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not remove " << sname
	<< " after its creation failed: " << cpp_strerror(-uret)
	<< dendl;
    }
    return ret;
  }

  nsfs::ConvertEvent ev{};
  ev.kind = nsfs::ConvertEvent::Kind::directory_object;
  ev.key = key;
  ev.index = progress.objects + progress.directory_objects;

  Attrs dattrs;
  /* Not `user.nsfs.`:  on the bucket root those are the bucket's own
   * state, which is not the object's to carry, and nothing else in
   * NooBaa's format has them. */
  std::vector<std::string> unknown;
  ret = get_x_attrs(y, dpp, dir_fd, dattrs, ev.key, xs, xattr_fallback, {},
		    "user.nsfs.", &unknown);
  if (ret < 0) {
    return ret;
  }
  if (!unknown.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: not converting directory object " << ev.key
      << ":  it carries " << unknown.front()
      << (unknown.size() > 1 ? " and others" : "")
      << ", which no reviewed release of NooBaa writes" << dendl;
    return -ERR_INVALID_OBJECT_STATE;
  }

  if (cb) {
    nsfs::ConvertEvent b = ev;
    b.phase = nsfs::ConvertEvent::Phase::before;
    ret = (*cb)(dpp, b);
    if (ret != 0) {
      return ret;
    }
  }

  nsfs::xattr_map_t to_write;
  for (const auto& [key, bl] : dattrs) {
    const std::string disk_name = xs->disk_name(key);
    if (!disk_name.empty()) {
      to_write.try_emplace(disk_name, attr_on_disk(xs, key, bl));
    }
  }
  /* The ETag the reader reported for an empty object, from the
   * directory's stat, recorded so the new inode does not change it.  A
   * stored MD5 has already been carried across and is kept. */
  if (empty) {
    bufferlist ebl;
    ebl.append(synthesize_etag(reported));
    const std::string disk_name = xs->disk_name(RGW_ATTR_ETAG);
    if (!disk_name.empty()) {
      to_write.try_emplace(disk_name, attr_on_disk(xs, RGW_ATTR_ETAG, ebl));
    }
  }
  /* Record the owner, for the reason convert_attrs() gives:  the
   * bucket's.  It lands on the sentinel along with everything else
   * that moves. */
  if (dattrs.find(RGW_ATTR_ACL) == dattrs.end()) {
    RGWAccessControlPolicy policy;
    policy.create_default(bucket_owner.id, bucket_owner.display_name);
    bufferlist aclbl;
    policy.encode(aclbl);
    const std::string disk_name = xs->disk_name(RGW_ATTR_ACL);
    if (!disk_name.empty()) {
      to_write.insert_or_assign(disk_name, attr_on_disk(xs, RGW_ATTR_ACL,
						   aclbl));
    }
  }
  if (!to_write.empty()) {
    /* The adopted-tree case.  inherit_ids() above has given the
     * sentinel the directory's owner, and in a tree we laid down that
     * is the gateway, which needs no raise to write it.  In an adopted
     * one the directory is whoever NooBaa ran as, so the sentinel is
     * theirs at `0660` by the time this runs and the write fails as an
     * object's does. */
    ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
      return fs->set_xattrs(dpp, sfd, to_write, y);
    });
    if (ret < 0) {
      return ret;
    }
  }

  if (cb) {
    nsfs::ConvertEvent w = ev;
    w.phase = nsfs::ConvertEvent::Phase::written;
    ret = (*cb)(dpp, w);
    if (ret != 0) {
      return ret;
    }
  }

  /* dir_content last:  while it is there the directory is still an
   * object to NooBaa's reader, and removing it before the sentinel
   * exists would make the object vanish for the length of a
   * syscall. */
  std::vector<std::string> gone;
  for (const auto& [key, bl] : dattrs) {
    const std::string foreign = xattr_fallback->disk_name(key);
    if (!foreign.empty()) {
      gone.push_back(foreign);
    }
  }
  if (!gone.empty()) {
    /* upgrade command;  see convert_attrs, whose raise this needs as
     * well:  the directory is whoever NooBaa ran as */
    ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
      return fs->remove_xattrs(dpp, dir_fd, gone, y);
    });
    if (ret < 0) {
      return ret;
    }
  }
  ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
    return path_fallback->clear_directory_object(dpp, dir_fd);
  });
  if (ret < 0) {
    return ret;
  }

  if (cb) {
    nsfs::ConvertEvent pr = ev;
    pr.phase = nsfs::ConvertEvent::Phase::pruned;
    ret = (*cb)(dpp, pr);
    if (ret != 0) {
      return ret;
    }
  }
  ++progress.directory_objects;
  return 0;
}

/* The path a descriptor is open on, or empty. */
static std::string fd_path(int fd)
{
  char proc[64];
  char buf[PATH_MAX];
  snprintf(proc, sizeof(proc), "/proc/self/fd/%d", fd);
  const ssize_t n = ::readlink(proc, buf, sizeof(buf) - 1);
  return (n < 0) ? std::string() : std::string(buf, n);
}

/* Count and log one symbolic link met by the conversion walk.  `dir_fd`
 * is the directory the walk reached at `prefix` without following a
 * link, so the bucket's real path is that directory's with `prefix`
 * taken off.  The link resolves as NooBaa resolved it:  following it,
 * then comparing its real path with the bucket's. */
static void count_link(const DoutPrefixProvider* dpp, int dir_fd,
		       const std::string& prefix, const char* name,
		       nsfs::ConvertProgress& progress)
{
  const std::string key = prefix + name;
  char target[PATH_MAX];
  const ssize_t tlen = ::readlinkat(dir_fd, name, target, sizeof(target) - 1);
  const std::string to = (tlen < 0) ? std::string("?")
				     : std::string(target, tlen);

  /* With read-search, as the gateway's other reads take it, so the
   * count describes the tree and not the uid the gateway runs as */
  if (nsfs::impersonation_enabled()) {
    (void) nsfs::hold_thread_capabilities(dpp);
  }
  const char* what = nullptr;
  const int fd = ::openat(dir_fd, name, O_PATH | O_CLOEXEC);
  if (fd < 0) {
    const int err = errno;
    if (err == ELOOP) {
      ++progress.links.loop;
      what = "loops";
    } else {
      ++progress.links.dangling;
      what = "resolves to nothing";
      if ((err != ENOENT) && (err != ENOTDIR)) {
	/* a filesystem error, not a kind of link:  counted, and the
	 * conversion goes on, since a link is not converted */
	ldpp_dout(dpp, 0) << "ERROR: nsfs: convert: link " << key << " -> "
	  << to << " cannot be resolved: " << cpp_strerror(err)
	  << ";  counted dangling" << dendl;
      }
    }
  } else {
    auto close_fd = make_scope_guard([fd] { ::close(fd); });
    std::string root = fd_path(dir_fd);
    if (root.size() >= prefix.size()) {
      root.resize(root.size() - prefix.size());
    }
    const std::string real = fd_path(fd);
    const bool inside = !root.empty() &&
      ((real == root) || real.starts_with(root + "/"));
    struct stat st{};
    if (!inside) {
      ++progress.links.outside;
      what = "leaves the bucket";
    } else if ((::fstat(fd, &st) == 0) && S_ISDIR(st.st_mode)) {
      ++progress.links.directory;
      what = "is a directory in the bucket";
    } else {
      ++progress.links.file;
      what = "is a file in the bucket";
    }
  }
  ldpp_dout(dpp, 1) << "nsfs: convert: " << key << " is a symbolic link ("
    << to << "), which " << what << ";  a link is not an object, and it "
    << "is not served" << dendl;
}

/* One directory of the tree, and everything under it.
 *
 * Depth first, and the order within a directory is readdir's, so a
 * caller sequencing against this gets whatever the filesystem gives.
 * Nothing here depends on the order:  each item is converted on its
 * own and a run that stops leaves the rest for the next one.
 */
static int convert_directory(const DoutPrefixProvider* dpp, optional_yield y,
			     nsfs::Directory* d, CephContext* cct,
			     const std::string& prefix,
			     nsfs::FSStrategy* fs,
			     const nsfs::XattrStrategy* xs,
			     const nsfs::XattrStrategy* xattr_fallback,
			     const ACLOwner& bucket_owner,
			     const nsfs::PathStrategy* path_fallback,
			     const nsfs::ReservedNames& rn,
			     nsfs::convert_cb_t* cb,
			     nsfs::ConvertProgress& progress,
			     bool no_chown)
{
  int ret = d->open(dpp);
  if (ret < 0) {
    return ret;
  }

  /* The sentinel first, where there is one:  it carries this
   * directory's object attributes in the nsfs format, and converting it
   * before recursing keeps the whole of one object in one step. */
  std::vector<std::string> subdirs;
  std::vector<std::string> files;

  ret = d->for_each(dpp, [&](const char* name) {
    std::string_view n(name);
    if ((n == ".") || (n == "..")) {
      return 0;
    }
    /* Scaffolding is skipped, the sentinel included.  A directory
     * object in NooBaa's format keeps its attributes on the directory whether
     * or not it has a sentinel, so the sentinel is never where the
     * conversion has to look -- the directory branch below owns the
     * whole of that case.  Treating it as an object here converted
     * nothing and counted the same directory object twice. */
    if (is_reserved_name(name, rn)) {
      return 0;
    }
    struct statx stx;
    if (nsfs::statx(d->get_fd(), name, AT_SYMLINK_NOFOLLOW, STATX_MODE,
		    &stx) < 0) {
      return 0;
    }
    if (S_ISDIR(stx.stx_mode)) {
      subdirs.emplace_back(name);
    } else if (S_ISREG(stx.stx_mode)) {
      files.emplace_back(name);
    } else if (S_ISLNK(stx.stx_mode)) {
      count_link(dpp, d->get_fd(), prefix, name, progress);
    }
    return 0;
  });
  if (ret < 0) {
    return ret;
  }

  for (const auto& name : files) {
    int fd = ::openat(d->get_fd(), name.c_str(), O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
      /* deleted under us;  one object, not a failed conversion */
      continue;
    }
    auto close_fd = make_scope_guard([fd] { ::close(fd); });

    nsfs::ConvertEvent ev{};
    ev.kind = nsfs::ConvertEvent::Kind::object;
    ev.key = prefix + name;
    ev.index = progress.objects + progress.directory_objects;

    ret = convert_attrs(dpp, y, fd, ev.key, fs, xs, xattr_fallback,
			bucket_owner, cb, ev);
    if (ret != 0) {
      return ret;
    }
    ++progress.objects;
  }

  /* Before descending, so the whole of this directory -- its objects
   * and their history -- is done in one step. */
  ret = convert_versions(dpp, y, d, cct, prefix, fs, xs, xattr_fallback,
			 bucket_owner, cb,
			 progress);
  if (ret != 0) {
    return ret;
  }

  for (const auto& name : subdirs) {
    nsfs::Directory sub(name, d, cct);
    ret = sub.open(dpp);
    if (ret < 0) {
      continue;
    }

    nsfs::PathStrategy::DirectoryObject dobj{};
    if (path_fallback->directory_object(dpp, sub.get_fd(), dobj)) {
      ret = convert_directory_object(dpp, y, sub.get_fd(), prefix + name + "/",
				     fs, xs, xattr_fallback, bucket_owner,
				     path_fallback, cb, progress, no_chown);
      if (ret != 0) {
	return ret;
      }
    }

    ret = convert_directory(dpp, y, &sub, cct, prefix + name + "/", fs,
			    xs, xattr_fallback, bucket_owner, path_fallback, rn,
			    cb, progress, no_chown);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

/* Re-stage an upload the other format started.
 *
 * S3 lets a multipart upload stay open indefinitely, so the drain that
 * precedes an upgrade cannot wait for one:  UploadPart terminates, the
 * upload it belongs to does not.  A tree therefore arrives at the
 * conversion with staging in the old layout, and leaving it is the one
 * thing that would keep the bucket mixed after the walk had finished
 * -- for three operations that would then be reading the wrong layout,
 * which is worse than a fallback because it is a lie rather than a
 * fallback.  Aborting is the alternative and throws away work the
 * client already did.
 *
 * Re-staged one file per part, whatever the nsfs layout is.  The stride
 * exists because parts arrive one at a time and a writer can place
 * them as they come;  here every part is already on disk and there is
 * nothing to gain by reconstructing it.  A record saying the bytes are
 * in the part's own file is what the nsfs assembly reads for a
 * diverted part, so both nsfs layouts complete it.
 */
int NSFSBucket::admit_write(const DoutPrefixProvider* dpp,
			    optional_yield y, nsfs::Directory* dir,
			    const std::string& key)
{
  nsfs::PathStrategy::DirectoryObject dobj{};
  if (!dir->directory_object(dpp, dobj)) {
    return 0;
  }
  if (!converting) {
    ldpp_dout(dpp, 1) << "nsfs: rejecting a change to the directory object "
      << key << " in bucket " << get_name() << ":  it is in NooBaa's "
      << "format, and the bucket is not being converted" << dendl;
    return -EOPNOTSUPP;
  }
  /* A write that needs a conversion first waits for it and converts
   * nothing (DECISIONS.md 2026-10-21).  Here inside the mutation slot, so
   * no conversion is running -- it would hold the gate -- and the one
   * that stopped short of this object is resumed by its command or by
   * the next adoption pass:  the write is answered with the retryable
   * 503 rather than waiting for a resume nothing has started. */
  ldpp_dout(dpp, 4) << "nsfs: the directory object " << key << " in bucket "
    << get_name() << " has not been converted;  rejecting a write until "
    << "the bucket's adoption reaches it" << dendl;
  return -ERR_SERVICE_UNAVAILABLE;
}

int NSFSBucket::convert_uploads(const DoutPrefixProvider* dpp,
				optional_yield y,
				nsfs::convert_cb_t* cb,
				nsfs::ConvertProgress& progress)
{
  auto* nb_mpu = mpu_fallback();
  auto* mpu = mpu_strategy();
  if (!nb_mpu || !mpu) {
    return 0;
  }

  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  auto fallback_root = nb_mpu->staging_root(dpp, dir->get_fd());
  if (!fallback_root) {
    return 0;   /* they never took an upload here */
  }

  nsfs::Directory root(*fallback_root, dir.get(), driver->ctx());
  ret = root.open(dpp);
  if (ret < 0) {
    return ret;
  }

  std::vector<std::string> dnames;
  ret = root.for_each(dpp, [&dnames](const char* name) {
    std::string_view n(name);
    if ((n != ".") && (n != "..")) {
      dnames.emplace_back(name);
    }
    return 0;
  });
  if (ret < 0) {
    return ret;
  }

  auto* fs = driver->get_fs_strategy();

  for (const auto& dname : dnames) {
    nsfs::MPUStrategy::StagedUpload su;
    if (!nb_mpu->staged_upload(dpp, root.get_fd(), dname, su)) {
      continue;   /* not an upload of NooBaa's */
    }

    nsfs::ConvertEvent ev{};
    ev.kind = nsfs::ConvertEvent::Kind::upload;
    ev.key = su.key;
    ev.upload_id = su.upload_id;
    ev.index = progress.uploads;
    if (cb) {
      nsfs::ConvertEvent b = ev;
      b.phase = nsfs::ConvertEvent::Phase::before;
      ret = (*cb)(dpp, b);
      if (ret != 0) {
	return ret;
      }
    }

    nsfs::Directory fallback_dir(dname, &root, driver->ctx());
    ret = fallback_dir.open(dpp);
    if (ret < 0) {
      continue;
    }

    /* the nsfs one, beside it.  NooBaa's staging is two levels down
     * and nsfs's is the bucket directory, so this is not a rename. */
    const std::string meta = su.key + "." + su.upload_id;
    nsfs::Directory staging_dir(mpu->staging_dir_name(meta), dir.get(),
			   driver->ctx());
    bool existed = false;
    /* in the bucket directory, which in an adopted tree is whoever
     * NooBaa ran as;  the gateway's own until its ownership moves below.
     * null_yield:  a raise never spans a yield */
    ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
      return staging_dir.create(dpp, null_yield, &existed);
    });
    if (ret < 0) {
      return ret;
    }
    ret = staging_dir.open(dpp);
    if (ret < 0) {
      return ret;
    }
    /* Marked with this bucket's extension set, as a staging directory an
     * upload RGW starts is (the shadow bucket's create()).  The shadow
     * bucket a later part or completion opens over it resolves its format
     * from this marker, and without it resolved base and wrote the part
     * records in NooBaa's format, which the driver never writes.  Before
     * the directory's ownership moves, while the gateway owns it. */
    {
      const std::string v = std::to_string(profile->extensions);
      if (::fsetxattr(staging_dir.get_fd(), nsfs::EXTENSIONS_XATTR, v.data(),
		      v.size(), 0) < 0) {
	ret = -errno;
	ldpp_dout(dpp, 0) << "ERROR: marking the staging for " << meta
			  << ": " << cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
    /* whether any part now sits elsewhere than its record said;  see
     * the multipart cache below */
    bool placement_changed = false;

    /* the upload's own record, in the nsfs format */
    {
      const std::string mname = mpu->meta_name();
      int mfd = ::openat(staging_dir.get_fd(), mname.c_str(),
			 O_RDWR | O_NOFOLLOW | O_CREAT, nsfs::file_create_mode());
      if (mfd < 0) {
	ret = -errno;
	ldpp_dout(dpp, 0) << "ERROR: creating " << mname << " for "
			  << meta << ": " << cpp_strerror(-ret) << dendl;
	return ret;
      }
      auto close_mfd = make_scope_guard([mfd] { ::close(mfd); });
      ret = nsfs::apply_create_mode(dpp, mfd, nsfs::file_create_mode(), y);
      if (ret < 0) {
	return ret;
      }

      /* re-expresses NooBaa's staging directory, so it takes its ids */
      struct statx tdstx{};
      ret = statx_ids(fallback_dir.get_fd(), tdstx);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: cannot stat the staging of " << meta
	  << " to give " << mname << " its owner: " << cpp_strerror(-ret)
	  << dendl;
	return ret;
      }
      ret = nsfs::with_capabilities(dpp, CONVERT_CHOWN_CAPS, [&]() -> int {
	return inherit_ids(dpp, mfd, tdstx, driver->no_chown_injected(),
			   mname);
      });
      if (ret < 0) {
	if (::unlinkat(staging_dir.get_fd(), mname.c_str(), 0) < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: could not remove " << mname
	    << " after its creation failed: " << cpp_strerror(errno)
	    << dendl;
	}
	return ret;
      }

      ACLOwner mp_owner;
      /* The bucket's owner.  NooBaa's staging records none, so the
       * upload is the bucket's owner's, and the re-staged upload needs
       * it recorded now. */
      mp_owner.id = get_owner();
      NSFSMPObj mp(driver, su.key, su.upload_id, mp_owner);

      bufferlist bl;
      encode(mp, bl);
      const std::string xn = xattr_strategy()->disk_name(RGW_NSFS_ATTR_MPUPLOAD);
      const std::string xv = attr_on_disk(xattr_strategy(),
					  RGW_NSFS_ATTR_MPUPLOAD, bl);
      /* the adopted-tree case, as the object walk's writes:  the record
       * goes on a file inherit_ids() has just given away. */
      ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
	if (::fsetxattr(mfd, xn.c_str(), xv.data(), xv.size(), 0) < 0) {
	  return -errno;
	}
	return 0;
      });
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: writing the upload record for " << meta
			  << ": " << cpp_strerror(-ret) << dendl;
	return ret;
      }
      ret = mpu->write_staged_upload(dpp, staging_dir.get_fd(), su);
      if (ret < 0) {
	return ret;
      }
    }

    /* the parts */
    std::vector<std::string> pnames;
    ret = fallback_dir.for_each(dpp, [&](const char* name) {
      if (nb_mpu->part_number(name)) {
	pnames.emplace_back(name);
      }
      return 0;
    });
    if (ret < 0) {
      return ret;
    }

    for (const auto& pname : pnames) {
      auto num = nb_mpu->part_number(pname);
      if (!num) {
	continue;
      }

      Attrs pattrs;
      int pfd = ::openat(fallback_dir.get_fd(), pname.c_str(), O_RDONLY | O_NOFOLLOW);
      if (pfd >= 0) {
	std::vector<std::string> unknown;
	get_x_attrs(y, dpp, pfd, pattrs, pname, xattr_strategy(),
		    xattr_fallback(), {}, {}, &unknown);
	::close(pfd);
	/* X4's third layer, as for an object */
	if (!unknown.empty()) {
	  ldpp_dout(dpp, 0) << "ERROR: not converting part " << pname
	    << " of an upload in bucket " << get_name() << ":  it carries "
	    << unknown.front() << (unknown.size() > 1 ? " and others" : "")
	    << ", which no reviewed release of NooBaa writes" << dendl;
	  return -ERR_INVALID_OBJECT_STATE;
	}
      }

      /* The cache first, as every reader of a part record takes it:
       * under the writeback and volatile policies a part's record may
       * be in the cache and not yet on disk, and a part read from disk
       * alone was skipped -- and its bytes deleted with NooBaa's
       * staging below.  Re-staging writes every record to disk, so the
       * cache entry is not needed afterwards. */
      nsfs::MPUStrategy::PartRecord rec{};
      file::listing::MultipartPartInfo cached;
      auto* mc = driver->get_multipart_cache();
      if (mc && mc->peek_part({nsfs::mp_cache_bucket(get_tenant(), get_name()), meta},
				  *num, cached)) {
	rec.size = cached.size;
	/* a part in a file of its own leaves `stored` at 0 in the cache;
	 * its bytes are then the part's accounted size */
	rec.stored = cached.stored ? cached.stored : cached.size;
	rec.offset = cached.offset;
	rec.shared = cached.shared;
	rec.etag = cached.etag;
	rec.mtime = cached.mtime;
	rec.cksum = cached.cksum;
      } else if (!nb_mpu->part_record(dpp, fallback_dir.get_fd(), pname, pattrs,
				      rec)) {
	ldpp_dout(dpp, 4) << "no record for " << pname << " of " << meta
			  << ";  skipping it" << dendl;
	continue;
      }

      nsfs::ConvertEvent pev{};
      pev.kind = nsfs::ConvertEvent::Kind::part;
      pev.key = su.key;
      pev.upload_id = su.upload_id;
      pev.part = *num;
      pev.index = progress.parts;
      if (cb) {
	nsfs::ConvertEvent b = pev;
	b.phase = nsfs::ConvertEvent::Phase::before;
	ret = (*cb)(dpp, b);
	if (ret != 0) {
	  return ret;
	}
      }

      /* where NooBaa's bytes are:  the size-keyed file when the record
       * says the part shares one, the part's own file otherwise */
      std::string sname = pname;
      if (rec.shared) {
	auto sn = nb_mpu->shared_name(rec.stored);
	if (!sn) {
	  ldpp_dout(dpp, 0) << "ERROR: " << pname << " of " << meta
			    << " says it shares a file the layout does not "
			    << "name" << dendl;
	  return -EIO;
	}
	sname = *sn;
      }
      int sfd = ::openat(fallback_dir.get_fd(), sname.c_str(), O_RDONLY | O_NOFOLLOW);
      if (sfd < 0) {
	ret = -errno;
	ldpp_dout(dpp, 0) << "ERROR: opening " << sname << " for " << pname
			  << " of " << meta << ": " << cpp_strerror(-ret)
			  << dendl;
	return ret;
      }
      auto close_sfd = make_scope_guard([sfd] { ::close(sfd); });

      const std::string part_name = mpu->part_name(*num);
      int dfd = ::openat(staging_dir.get_fd(), part_name.c_str(),
			 O_RDWR | O_NOFOLLOW | O_CREAT | O_TRUNC, nsfs::file_create_mode());
      if (dfd < 0) {
	ret = -errno;
	ldpp_dout(dpp, 0) << "ERROR: creating " << part_name << " for " << meta
			  << ": " << cpp_strerror(-ret) << dendl;
	return ret;
      }
      auto close_dfd = make_scope_guard([dfd] { ::close(dfd); });
      ret = nsfs::apply_create_mode(dpp, dfd, nsfs::file_create_mode(), y);
      if (ret < 0) {
	return ret;
      }

      /* re-expresses NooBaa's part file, so it takes its ids */
      struct statx pstx{};
      ret = statx_ids(sfd, pstx);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: cannot stat " << sname << " to give "
	  << part_name << " its owner: " << cpp_strerror(-ret) << dendl;
	return ret;
      }
      ret = nsfs::with_capabilities(dpp, CONVERT_CHOWN_CAPS, [&]() -> int {
	return inherit_ids(dpp, dfd, pstx, driver->no_chown_injected(),
			   part_name);
      });
      if (ret < 0) {
	if (::unlinkat(staging_dir.get_fd(), part_name.c_str(), 0) < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: could not remove " << part_name
	    << " after its creation failed: " << cpp_strerror(errno)
	    << dendl;
	}
	return ret;
      }

      if (rec.stored > 0) {
	ret = fs->copy_range(dpp, sfd, rec.offset, dfd, 0, rec.stored,
			     driver->copy_hints_for(no_authenticated_user));
	if (ret < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: copying part " << *num << " of "
			    << meta << ": " << cpp_strerror(-ret) << dendl;
	  return ret;
	}
      }

      if (cb) {
	nsfs::ConvertEvent w = pev;
	w.phase = nsfs::ConvertEvent::Phase::written;
	ret = (*cb)(dpp, w);
	if (ret != 0) {
	  return ret;
	}
      }

      NSFSUploadPartInfo upi;
      upi.num = *num;
      upi.size = rec.size;
      upi.etag = rec.etag;
      upi.mtime = rec.mtime;
      upi.cksum = rec.cksum;
      /* its own file now, whatever it was in */
      upi.shared = false;
      upi.offset = 0;
      upi.stored = rec.stored;

      bufferlist pbl;
      encode(upi, pbl);
      const std::string pxn =
	  xattr_strategy()->disk_name(RGW_NSFS_ATTR_MPUPLOAD);
      const std::string pxv = attr_on_disk(xattr_strategy(),
					   RGW_NSFS_ATTR_MPUPLOAD, pbl);
      /* as the upload record above;  a separate raise from the chown
       * because copy_range() runs between the two and needs none --
       * dfd was opened before the file changed hands. */
      ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
	if (::fsetxattr(dfd, pxn.c_str(), pxv.data(), pxv.size(), 0) < 0) {
	  return -errno;
	}
	return 0;
      });
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: writing the record for part " << *num
			  << " of " << meta << ": " << cpp_strerror(-ret)
			  << dendl;
	return ret;
      }
      nsfs::MPUStrategy::PartRecord outrec = rec;
      outrec.shared = false;
      outrec.offset = 0;
      if ((outrec.shared != rec.shared) || (outrec.offset != rec.offset)) {
	placement_changed = true;
      }
      ret = mpu->write_part_record(dpp, staging_dir.get_fd(), part_name, outrec);
      if (ret < 0) {
	return ret;
      }

      if (cb) {
	nsfs::ConvertEvent pr = pev;
	pr.phase = nsfs::ConvertEvent::Phase::pruned;
	ret = (*cb)(dpp, pr);
	if (ret != 0) {
	  return ret;
	}
      }
      ++progress.parts;
    }

    if (cb) {
      nsfs::ConvertEvent w = ev;
      w.phase = nsfs::ConvertEvent::Phase::written;
      ret = (*cb)(dpp, w);
      if (ret != 0) {
	return ret;
      }
    }

    /* NooBaa's staging goes last.  Until it does the upload exists twice,
     * which a listing that scans one root at a time reports once;
     * removing it first would lose the parts if anything below
     * failed. */
    struct statx tdstx{};
    ret = statx_ids(fallback_dir.get_fd(), tdstx);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cannot stat the staging of " << meta
	<< " to give it its owner: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    fallback_dir.close();
    /* theirs, and in an adopted tree its uploader's, in a root its
     * account owns */
    ret = nsfs::with_capabilities(dpp, CONVERT_REWRITE_CAPS, [&]() -> int {
      return delete_directory(root.get_fd(), dname.c_str(), true, dpp,
			      driver->get_reserved_names());
    });
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: removing their staging for " << meta
			<< ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    /* The staging re-expresses NooBaa's, so it takes its ids, as the
     * meta object and the parts did:  staging is its uploader's
     * (DECISIONS.md 2026-10-23).  After theirs is gone, so a run
     * stopped before it resumes into a directory the gateway still
     * owns. */
    ret = nsfs::with_capabilities(dpp, CONVERT_CHOWN_CAPS, [&]() -> int {
      return inherit_ids(dpp, staging_dir.get_fd(), tdstx,
			 driver->no_chown_injected(), meta);
    });
    if (ret < 0) {
      return ret;
    }

    /* The multipart cache is keyed by the upload, not by where it is
     * staged, so its part records survive the move.  A record carries
     * the part's placement, and re-staging gives every part its own
     * file:  where NooBaa had several parts share one, the cached
     * placement is now wrong.  Dropped exactly then, and kept when every
     * part was already in its own file (DECISIONS.md 2026-10-13). */
    if (placement_changed) {
      if (auto* mc = driver->get_multipart_cache()) {
	mc->remove({nsfs::mp_cache_bucket(get_tenant(), get_name()), meta});
      }
    }

    if (cb) {
      nsfs::ConvertEvent pr = ev;
      pr.phase = nsfs::ConvertEvent::Phase::pruned;
      ret = (*cb)(dpp, pr);
      if (ret != 0) {
	return ret;
      }
    }
    ++progress.uploads;
  }

  return 0;
}

/* Rewrite whatever in this tree is still in the other format.
 *
 * Runs with the bucket already marked -- so writes go in the nsfs format
 * and the fallback strategies are in play -- and clears the marker
 * when it gets to
 * the end.  A run that stops leaves the marker, which is what makes
 * it resumable:  the state on disk is "some of this is still NooBaa's",
 * and that is exactly what the marker says.
 *
 * Idempotent.  An item already converted reads through the nsfs
 * strategy, writes back the same bytes under the same names, and
 * prunes a foreign spelling that is not there.
 */
/* How long a mutation waits on a closed gate before giving up, and how
 * long an upgrade waits for the mutations already running.
 *
 * Not configurable, and the two are different questions.  A mutation
 * must not wait out a whole conversion -- that is unbounded, and a
 * client holding a connection open for it is worse than a retry -- so
 * it waits about as long as a request may reasonably stall.  The
 * upgrade's wait is bounded by what is running when it arrives, and
 * every operation terminates, so this only has to be longer than the
 * slowest single operation rather than longer than any queue.
 */
/* Seeded in the header, where the members are declared.  A mutation
 * waits about as long as a request may reasonably stall;  the drain
 * only has to outlast the slowest single operation, because every
 * operation terminates. */


/* What the gate covers.
 *
 * There is no single choke point.  Rados has one -- every mutation
 * reaches the bucket index, which is where guard_reshard() sits -- and
 * a filesystem tree has none, so the guard is taken at each mutating
 * SAL entry point by name:
 *
 *   NSFSObject::set_obj_attrs, delete_obj_attrs, copy_object, rename
 *   NSFSObject::NSFSDeleteOp::delete_obj
 *   NSFSAtomicWriter::complete          (PUT)
 *   NSFSMultipartWriter::complete       (UploadPart)
 *   NSFSMultipartUpload::init, complete, abort
 *   NSFSBucket::merge_and_store_attrs, remove, set_acl
 *
 * The FSIO path -- how librgw and the NFS gateway write -- passes
 * through none of them, because its unit is not a request.  A handle
 * is opened once and written through many times, so opening one to
 * write is the mutation and releasing it is what retires it:  the
 * slot is taken in get_fsio_handle() and held for the handle's life.
 * A bucket with an NFS client holding a write handle open therefore
 * does not drain until that handle is released, and the conversion
 * does not start rather than run beside it.  That is the
 * intent, not a shortcoming:  converting under an open write handle
 * is the thing being prevented.
 */

/* A bucket not taken over takes no client writes (DECISIONS.md
 * 2026-10-21, the gate):  no `user.nsfs.extensions` means the bucket is
 * NooBaa's until adoption marks it, and InvalidBucketState says the
 * request is not valid in the bucket's current state, which a command
 * can change. */
int NSFSDriver::reject_unadopted(const DoutPrefixProvider* dpp,
				 NSFSBucket* b, MutationPurpose purpose)
{
  if (purpose != MutationPurpose::client) {
    return 0;
  }
  const int pret = b->resolve_profile(dpp);
  if (pret < 0) {
    return pret;
  }
  if (b->extended() || adoption_gate_lifted(b->get_name())) {
    return 0;
  }
  unadopted_writes_rejected.fetch_add(1, std::memory_order_relaxed);
  ldpp_dout(dpp, 4) << "nsfs: bucket " << b->get_name() << " is not adopted;"
    << "  a client write waits for adoption" << dendl;
  /* Under `automatic` the bucket is queued and its adoption needs no
   * command, so the write is worth retrying. */
  if (adoption_queue.load(std::memory_order_acquire) &&
      !adoption_complete()) {
    return -ERR_SERVICE_UNAVAILABLE;
  }
  return -ERR_INVALID_BUCKET_STATE;
}

int NSFSDriver::wait_for_conversion(const DoutPrefixProvider* dpp,
				    const std::string& bucket,
				    const std::function<bool()>& done)
{
  if (done()) {
    return 0;
  }
  std::unique_lock l{gate_lock};
  if (gate_cond.wait_for(l, mutation_gate_wait, done)) {
    return 0;
  }
  ldpp_dout(dpp, 4) << "nsfs: bucket " << bucket << " has not been converted "
    << "this far;  rejecting a write after waiting "
    << mutation_gate_wait.count() << "ms" << dendl;
  return -ERR_SERVICE_UNAVAILABLE;
}

bool NSFSBucket::upload_awaits_conversion(const DoutPrefixProvider* dpp,
					  const std::string& meta)
{
  if ((resolve_profile(dpp) < 0) || !converting) {
    return false;
  }
  auto* nb_mpu = mpu_fallback();
  if (!nb_mpu || (dir->open(dpp) < 0)) {
    return false;
  }
  auto root = nb_mpu->staging_root(dpp, dir->get_fd());
  if (!root) {
    return false;
  }
  std::string path = nb_mpu->staging_dir_name(meta);
  if (*root != ".") {
    path = *root + "/" + path;
  }
  return ::faccessat(dir->get_fd(), path.c_str(), F_OK,
		     AT_SYMLINK_NOFOLLOW | AT_EACCESS) == 0;
}

NSFSMutationGuard::NSFSMutationGuard(const DoutPrefixProvider* dpp,
				     NSFSDriver* _driver, NSFSBucket* _bucket,
				     MutationPurpose purpose)
  : driver(_driver), bucket(_bucket->get_name()),
    rc(_driver ? _driver->mutation_enter(dpp, _bucket, purpose) : 0)
{ }

int NSFSDriver::mutation_enter(const DoutPrefixProvider* dpp,
			       NSFSBucket* b, MutationPurpose purpose)
{
  const std::string& bucket = b->get_name();

  /* checked before the slot, so a rejection holds nothing */
  const int gret = reject_unadopted(dpp, b, purpose);
  if (gret < 0) {
    return gret;
  }

  std::unique_lock l{gate_lock};
  auto& g = bucket_gates[bucket];
  if (g.closed) {
    const bool opened = gate_cond.wait_for(l, mutation_gate_wait,
					   [&g] { return !g.closed; });
    if (!opened) {
      ldpp_dout(dpp, 4) << "nsfs: bucket " << bucket << " is being rewritten;"
	<< "  rejecting a mutation after waiting "
	<< mutation_gate_wait.count() << "ms" << dendl;
      return -ERR_SERVICE_UNAVAILABLE;
    }
  }
  ++g.inflight;
  return 0;
}

void NSFSDriver::mutation_leave(const std::string& bucket)
{
  std::lock_guard l{gate_lock};
  auto i = bucket_gates.find(bucket);
  if (i == bucket_gates.end()) {
    return;
  }
  if (i->second.inflight > 0) {
    --i->second.inflight;
  }
  /* The drain is waiting on this, and an entry with nothing in flight
   * and an open gate is not worth a map slot. */
  if ((i->second.inflight == 0) && !i->second.closed) {
    bucket_gates.erase(i);
  }
  gate_cond.notify_all();
}

int NSFSDriver::gate_close(const DoutPrefixProvider* dpp,
			   const std::string& bucket)
{
  std::unique_lock l{gate_lock};
  auto& g = bucket_gates[bucket];
  if (g.closed) {
    /* Somebody else is already rewriting this tree.  Two conversions
     * at once would each read what the other had half written. */
    return -EBUSY;
  }
  g.closed = true;

  if (g.inflight > 0) {
    const bool drained = gate_cond.wait_for(l, gate_drain_wait,
					    [&g] { return g.inflight == 0; });
    if (!drained) {
      g.closed = false;
      gate_cond.notify_all();
      ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket << " still had "
	<< g.inflight << " mutations running after "
	<< gate_drain_wait.count() << "ms;  not starting the rewrite"
	<< dendl;
      return -ETIMEDOUT;
    }
  }
  return 0;
}

void NSFSDriver::gate_open(const std::string& bucket)
{
  std::lock_guard l{gate_lock};
  auto i = bucket_gates.find(bucket);
  if (i == bucket_gates.end()) {
    return;
  }
  i->second.closed = false;
  if (i->second.inflight == 0) {
    bucket_gates.erase(i);
  }
  gate_cond.notify_all();
}

bool NSFSDriver::gate_is_closed(const std::string& bucket)
{
  std::lock_guard l{gate_lock};
  auto i = bucket_gates.find(bucket);
  return (i != bucket_gates.end()) && i->second.closed;
}

int NSFSBucket::convert_tree(const DoutPrefixProvider* dpp, optional_yield y,
			     nsfs::convert_cb_t* cb,
			     nsfs::ConvertProgress* progress)
{
  nsfs::ConvertProgress local;
  nsfs::ConvertProgress& prog = progress ? *progress : local;

  auto* nb_xattrs = xattr_fallback();
  auto* nb_paths = path_fallback();
  if (!nb_xattrs || !nb_paths) {
    /* Not converting, so there is nothing in another format to find.
     * Reached when a caller resumes a bucket whose marker is already
     * gone, which is a no-op and not an error. */
    prog.complete = true;
    return 0;
  }

  /* Mutations are held off for the length of the rewrite, and the
   * ones already running are waited for.  A write arriving between an
   * object's `before` and its `written` is lost:  the converter read
   * the attributes before it and writes them back after.  Reads are
   * not held -- a half-converted tree reads through the fallback
   * strategies, which is the whole reason they exist. */
  int ret = driver->gate_close(dpp, get_name());
  if (ret < 0) {
    return ret;
  }
  auto reopen = make_scope_guard(
      [this] { driver->gate_open(get_name()); });

  /* The bucket's own state needs nothing:  it is named in the nsfs
   * spelling in every format (DECISIONS.md 2026-10-13).
   *
   * The root object does.  NooBaa stores it as any directory object,
   * with the bucket's own directory as the directory, so it is
   * converted as one, before the walk -- which reaches only the
   * directories beneath.  convert_directory_object() takes nothing under
   * `user.nsfs.`, which on this directory is the bucket's state. */
  ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }
  {
    nsfs::PathStrategy::DirectoryObject dobj{};
    if (nb_paths->directory_object(dpp, dir->get_fd(), dobj)) {
      ret = convert_directory_object(dpp, y, dir->get_fd(), "/",
				     driver->get_fs_strategy(), xattr_strategy(),
				     nb_xattrs, default_object_owner(dpp, y),
				     nb_paths, cb, prog,
				     driver->no_chown_injected());
      if (ret != 0) {
	return ret;
      }
    }
  }

  ret = convert_directory(dpp, y, dir.get(), driver->ctx(),
			  std::string(), driver->get_fs_strategy(),
			  xattr_strategy(), nb_xattrs,
			  default_object_owner(dpp, y), nb_paths,
			  driver->get_reserved_names(), cb, prog,
			  driver->no_chown_injected());
  if (ret == 0) {
    ret = convert_uploads(dpp, y, cb, prog);
  }
  if (ret != 0) {
    ldpp_dout(dpp, 1) << "nsfs: converting bucket " << get_name()
      << " stopped after " << prog.objects << " objects, "
      << prog.directory_objects << " directory objects, "
      << prog.versions << " versions and "
      << prog.uploads << " uploads: "
      << cpp_strerror(-ret) << ";  the marker stays and a later run "
      << "resumes" << dendl;
    return ret;
  }

  ret = clear_converting(dpp);
  if (ret < 0) {
    return ret;
  }
  prog.complete = true;

  ldpp_dout(dpp, 1) << "nsfs: bucket " << get_name() << " converted: "
    << prog.objects << " objects, " << prog.directory_objects
    << " directory objects, " << prog.versions << " versions, "
    << prog.uploads << " uploads ("
    << prog.parts << " parts)" << dendl;
  if (prog.links.total() > 0) {
    ldpp_dout(dpp, 1) << "nsfs: bucket " << get_name() << " holds "
      << prog.links.total() << " symbolic links, not served:  "
      << prog.links.file << " to a file in the bucket, "
      << prog.links.directory << " to a directory in the bucket, "
      << prog.links.outside << " out of the bucket, "
      << prog.links.dangling << " dangling, "
      << prog.links.loop << " looping" << dendl;
  }
  return 0;
}

int NSFSBucket::mark_extensions(const DoutPrefixProvider* dpp,
				uint32_t version)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  /* as the gateway, raised where the directory is not its own (site 1);
   * a staging directory's as its uploader, who owns it */
  std::string v = std::to_string(version);
  ret = write_bucket_state(dpp, [&]() -> int {
    return nsfs::with_identity(dpp, dir->get_identity(), [&]() -> int {
      return (::fsetxattr(dir->get_fd(), nsfs::EXTENSIONS_XATTR,
			  v.data(), v.size(), 0) < 0) ? -errno : 0;
    }, null_yield);
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: writing " << nsfs::EXTENSIONS_XATTR
      << " on bucket " << get_name() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  profile = driver->resolve_profile(version);
  if (! profile) {
    /* The attribute is written and names nothing we serve.  Only a
     * caller passing a set we do not implement reaches this, and the
     * bucket is now unservable, which the next load reports. */
    ldpp_dout(dpp, 0) << "ERROR: marked bucket " << get_name()
      << " with extension set " << version << ", which resolves to no "
      << "profile" << dendl;
    return -EINVAL;
  }
  apply_format();
  return 0;
}

int NSFSBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  if (get_name()[0] == '.') {
    /* Skip dotfiles */
    return -ERR_INVALID_OBJECT_NAME;
  }
  ret = dir->stat(dpp);
  if (ret < 0) {
    return ret;
  }

  mtime = ceph::real_clock::from_time_t(dir->get_stx().stx_mtime.tv_sec);

  /* STATX_BTIME is optional and a filesystem which does not report it
   * leaves stx_btime zeroed rather than saying so -- GPFS is one, and a
   * bucket there listed with a creation date of the epoch.  Consult the
   * mask, and fall back to mtime, which is wrong but plausible;  this is
   * only a seed for a tree we did not create, since the authoritative
   * value arrives with the stored RGWBucketInfo below. */
  const auto& bstx = dir->get_stx();
  info.creation_time = ceph::real_clock::from_time_t(
    (bstx.stx_mask & STATX_BTIME) ? bstx.stx_btime.tv_sec
				  : bstx.stx_mtime.tv_sec);

  ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }
  /* a write reaches a bucket only after it is loaded, so its filesystem
   * is in the relaxed policy's set before the write is */
  if (!is_internal()) {
    driver->note_bucket_filesystem(dpp, dir->get_stx(), dir->get_fd());
  }

  /* before the attributes, because which names are nsfs's to read is
   * decided by the profile */
  ret = resolve_profile(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = dir->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  /* The markers live under user.nsfs. so they parse as nsfs names and arrive
   * here as ordinary attributes.  Neither is bucket metadata -- one is
   * what decided how to read this bucket, the other says the tree is
   * still being rewritten -- and surfacing either would put it in the
   * S3 attribute set.  Same reason bucket_info is erased below. */
  for (const char* m : { nsfs::EXTENSIONS_XATTR, nsfs::CONVERTING_XATTR }) {
    std::string marker_key;
    if (xattr_strategy()->parse_disk_name(m, marker_key)) {
      attrs.erase(marker_key);
    }
  }

  /* Bucket state, from wherever this bucket's format keeps it.  The
   * strategy erases what it consumed, so nothing of its own is left in
   * the S3 attribute set.
   *
   * -ENOENT is a bucket with no stored state, which is legitimate and
   * leaves the seeded defaults standing.
   *
   * Anything else is not the same thing, and this is where the two stop
   * being treated alike.  Defaults are permissive:  no object lock, no
   * policy, no public access block, versioning off.  Serving a bucket
   * whose stored state we cannot read as though it had none is
   * therefore fail-open on every setting that matters, and silently so.
   * The bucket fails instead. */
  RGWBucketInfo bak_info = info;
  ret = bucket_state_strategy()->load(dpp, dir->get_fd(), get_name(),
				      attrs, info);
  if (ret == -ENOENT) {
    /* No nsfs state.  A base bucket may still have a record in
     * NooBaa's store, and that is where its versioning lives.  When
     * the fallback is consulted is decided here and not inside either
     * strategy, so that authoritative-with-nsfs-as-cache and
     * recovery-only remain reachable without redesign. */
    auto* nb_state = bucket_state_fallback();
    ret = nb_state ? nb_state->load(dpp, dir->get_fd(), get_name(), attrs, info)
		 : -ENOENT;
    /* NooBaa's record names principals as NooBaa does;  served here as
     * adoption will write them */
    if (ret == 0) {
      ret = driver->rewrite_unadopted_policy(dpp, get_name(), attrs);
    }
  }
  if (ret == -ENOENT) {
    /* No state anywhere:  the info the caller seeded stands, owner
     * included.  A bucket's owner is not derived from its directory's
     * uid -- the uid is a POSIX identity, several accounts can share
     * one, and an owner comes only from stored state, nsfs's or
     * NooBaa's `owner_account` as the import maps it (DECISIONS.md
     * 2026-10-11). */
    info = bak_info;
  } else if (ret < 0) {
    return ret;
  }

  return 0;
}

int NSFSBucket::set_acl(const DoutPrefixProvider* dpp,
			 RGWAccessControlPolicy& acl,
			 optional_yield y)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, this);
  if (mg.result() < 0) {
    return mg.result();
  }
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  attrs[RGW_ATTR_ACL] = aclbl;
  const bool owner_changed = (info.owner != acl.get_owner().id);
  info.owner = acl.get_owner().id;

  int ret = write_attrs(dpp, y);
  if ((ret == 0) && owner_changed && !is_internal()) {
    ret = driver->get_identity_db()->set_bucket_placement_owner(
      dpp, get_name(), get_tenant(), to_string(info.owner));
  }
  return ret;
}

int NSFSBucket::read_stats(const DoutPrefixProvider *dpp, optional_yield y,
			    const bucket_index_layout_generation& idx_layout,
			    int shard_id, std::string* bucket_ver, std::string* master_ver,
			    std::map<RGWObjCategory, RGWStorageStats>& stats,
			    std::string* max_marker, bool* syncstopped)
{
  auto& main = stats[RGWObjCategory::Main];

  // TODO: bucket stats shouldn't have to list all objects
  const auto& rn = driver->get_reserved_names();
  return dir->for_each(dpp, [this, dpp, y, &main, &rn] (const char* name) {
    if (is_reserved_name(name, rn)) {
      return 0;
    }

    std::unique_ptr<FSEnt> dent;
    int ret = dir->get_ent(dpp, y, name, std::string(), dent);
    if ((ret == -ENOENT) || (ret == -EINVAL)) {
      /* not an object, or gone since readdir:  nothing to count */
      return 0;
    }
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not get ent for object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    ret = dent->stat(dpp);
    if (ret < 0) {
      ret = errno;
      ldpp_dout(dpp, 0) << "ERROR: could not stat object " << name << ": "
	<< cpp_strerror(ret) << dendl;
      return -ret;
    }

    struct statx& lstx = dent->get_stx();

    if (S_ISREG(lstx.stx_mode) || S_ISDIR(lstx.stx_mode)) {
      main.num_objects++;
      main.size += lstx.stx_size;
      main.size_rounded += lstx.stx_size;
      main.size_utilized += lstx.stx_size;
    }

    return 0;
  });
  return 0;
}

int NSFSBucket::read_stats_async(const DoutPrefixProvider *dpp,
				  const bucket_index_layout_generation& idx_layout,
				  int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
{
  return 0;
}

int NSFSBucket::sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                  RGWBucketEnt* ent)
{
  return 0;
}

int NSFSBucket::check_bucket_shards(const DoutPrefixProvider* dpp,
                                     uint64_t num_objs, optional_yield y)
{
  return 0;
}

int NSFSBucket::chown(const DoutPrefixProvider* dpp,
                       const rgw_owner& new_owner,
                       const std::string& new_owner_name,
                       optional_yield y) {
  return chown(dpp, new_owner, new_owner_name, ChownMode::metadata, y);
}

/* Give `uid` and `gid` to everything under `dirfd`, recursively,
 * without following symlinks.  `dirfd` itself is not changed.
 * Returns the first error, having changed what came before it. */
static int chown_tree(const DoutPrefixProvider* dpp, int dirfd,
		      uid_t uid, gid_t gid, size_t& changed)
{
  int fd = ::openat(dirfd, ".", O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    return -errno;
  }
  DIR* d = ::fdopendir(fd);
  if (!d) {
    int ret = -errno;
    ::close(fd);
    return ret;
  }
  auto close_d = make_scope_guard([d] { ::closedir(d); });

  while (struct dirent* de = ::readdir(d)) {
    const std::string_view name{de->d_name};
    if ((name == ".") || (name == "..")) {
      continue;
    }
    if (::fchownat(fd, de->d_name, uid, gid, AT_SYMLINK_NOFOLLOW) < 0) {
      int ret = -errno;
      ldpp_dout(dpp, 0) << "ERROR: chown of " << name << " to " << uid
	<< ":" << gid << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    ++changed;
    bool is_dir = (de->d_type == DT_DIR);
    if (de->d_type == DT_UNKNOWN) {
      /* not every filesystem fills d_type */
      struct stat st;
      is_dir = (nsfs::fstatat(fd, de->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0)
	&& S_ISDIR(st.st_mode);
    }
    if (is_dir) {
      int sub = ::openat(fd, de->d_name,
			 O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
      if (sub < 0) {
	return -errno;
      }
      int ret = chown_tree(dpp, sub, uid, gid, changed);
      ::close(sub);
      if (ret < 0) {
	return ret;
      }
    }
  }
  return 0;
}

int NSFSBucket::chown(const DoutPrefixProvider* dpp,
		      const rgw_owner& new_owner,
		      const std::string& new_owner_name,
		      ChownMode mode, optional_yield y)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, this);
  if (mg.result() < 0) {
    return mg.result();
  }

  /* The new owner's POSIX identity, resolved first so that a rejection
   * changes nothing.  An account by its row;  a user by its own row,
   * else its account's, as a request resolves. */
  nsfs::Credentials cred;
  if (mode == ChownMode::posix) {
    auto* db = driver->get_identity_db();
    if (!db) {
      return -ENOENT;
    }
    int ret;
    if (const auto* u = std::get_if<rgw_user>(&new_owner)) {
      std::unique_ptr<User> user = driver->get_user(*u);
      ret = user->load_user(dpp, y);
      if (ret < 0) {
	return ret;
      }
      ret = nsfs::resolve_credentials(dpp, *db, *u,
				      user->get_info().account_id, cred, y);
    } else {
      ret = nsfs::resolve_credentials(dpp, *db, new_owner, cred, y);
    }
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << ":  the new "
	<< "owner " << to_string(new_owner) << " has "
	<< ((ret == -EPERM) ? "a directory-backed identity, which is not "
	    "consulted" : "no POSIX identity")
	<< ";  rejecting the chown" << dendl;
      return ret;
    }
  }

  /* The bucket ACL, as RadosBucket::chown() rewrites it:  the old
   * owner's grant out, the new owner's FULL_CONTROL in. */
  if (auto i = attrs.find(RGW_ATTR_ACL); i != attrs.end()) {
    try {
      RGWAccessControlPolicy policy;
      auto p = i->second.cbegin();
      decode(policy, p);
      RGWAccessControlList& acl = policy.get_acl();
      acl.remove_canon_user_grant(policy.get_owner().id);
      ACLGrant grant;
      grant.set_canon(new_owner, new_owner_name, RGW_PERM_FULL_CONTROL);
      acl.add_grant(grant);
      policy.set_owner(ACLOwner{new_owner, new_owner_name});
      bufferlist bl;
      encode(policy, bl);
      i->second = std::move(bl);
      acls = policy;
    } catch (const buffer::error&) {
      /* not fatal, as for rados */
    }
  }
  info.owner = new_owner;
  int ret = put_info(dpp, false, ceph::real_clock::now(), y);
  if (ret < 0) {
    return ret;
  }
  ret = driver->get_identity_db()->set_bucket_placement_owner(
    dpp, get_name(), get_tenant(), to_string(new_owner));
  if (ret < 0) {
    return ret;
  }

  /* An object recording no owner lists as the bucket's, and the cached
   * listing entries hold the old one. */
  default_owner.reset();
  if (auto* bc = driver->get_bucket_cache()) {
    bc->invalidate_bucket(dpp, get_name());
  }

  if (mode == ChownMode::metadata) {
    return 0;
  }

  ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }
  size_t changed = 0;
  ret = chown_tree(dpp, dir->get_fd(), cred.uid, cred.gid, changed);
  ldpp_dout(dpp, 1) << "nsfs: bucket " << get_name() << " chowned to "
    << to_string(new_owner) << " (" << cred.uid << ":" << cred.gid
    << "), " << changed << " entries" << (ret < 0 ? ", incomplete" : "")
    << dendl;
  return ret;
}

int NSFSBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime, optional_yield y)
{
  /* a client's change to the bucket's own state -- versioning, object
   * lock, the rest of RGWBucketInfo -- so the gate on a bucket not taken
   * over asks, and a conversion holds it off;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, this);
  if (mg.result() < 0) {
    return mg.result();
  }
  mtime = _mtime;

  struct timespec ts[2];
  ts[0].tv_nsec = UTIME_OMIT;
  ts[1] = ceph::real_clock::to_timespec(mtime);
  /* A bucket served from a stored path has no parent Directory and its
   * own name is the absolute path, so resolve it from the cwd -- the
   * same way `Directory::open()` does (P2). */
  const bool placed = (dir->get_parent() == nullptr);
  int ret = write_bucket_state(dpp, [&]() -> int {
    return (utimensat(placed ? AT_FDCWD : dir->get_parent()->get_fd(),
		      placed ? dir->get_name().c_str()
			     : get_fname().c_str(),
		      ts, AT_SYMLINK_NOFOLLOW) < 0) ? -errno : 0;
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not set mtime on bucket " << get_name() << ": "
      << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return write_attrs(dpp, y);
}

int NSFSBucket::write_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  bufferlist bl;
  encode(info, bl);
  Attrs extra_attrs;
  extra_attrs[nsfs::BUCKET_INFO_KEY] = bl;

  return write_bucket_state(dpp, [&]() -> int {
    return dir->write_attrs(dpp, y, attrs, &extra_attrs);
  });
}

int NSFSBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  int bucket_fd = dir->get_fd();
  const auto& rn = driver->get_reserved_names();
  const auto* ps = path_strategy();
  return dir->for_each(dpp, [bucket_fd, &rn, ps](const char* name) {
    /* for_each filters out "." and ".." */
    /* The root object, the root's `.folder`, is not content:  it is
     * never listed, and NooBaa judges emptiness by listing (DECISIONS.md
     * 2026-10-13).  entry_is_content() counts any `.folder` as content,
     * because in a directory it is that directory's object;  this is the
     * one place it meets the root's. */
    if (ps && ps->names_directory_object(name)) {
      return 0;
    }
    if (entry_is_content(bucket_fd, name, rn)) {
      return -ENOTEMPTY;
    }
    return 0;
  });
}

int NSFSBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
  return driver->get_quota_handler()->check_quota(dpp, info.owner, get_key(),
                                                  quota, (check_size_only ? 0 : 1),
                                                  obj_size, y);
}

int NSFSBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y)
{
  *pmtime = mtime;

  int ret = dir->open(dpp);
  if (ret < 0) {
    return ret;
  }

  return dir->read_attrs(dpp, y, attrs);
}

int NSFSBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			    uint64_t end_epoch, uint32_t max_entries,
			    bool* is_truncated, RGWUsageIter& usage_iter,
			    std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return 0;
}

int NSFSBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  return 0;
}

int NSFSBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return 0;
}

int NSFSBucket::check_index(const DoutPrefixProvider *dpp, optional_yield y,
                             std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                             std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return 0;
}

int NSFSBucket::rebuild_index(const DoutPrefixProvider *dpp, optional_yield y)
{
  return 0;
}

int NSFSBucket::set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y, uint64_t timeout)
{
  return 0;
}

int NSFSBucket::purge_instance(const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

std::unique_ptr<MultipartUpload> NSFSBucket::get_multipart_upload(
				  const std::string& oid,
				  const rgw_owner& authenticated_user,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<NSFSMultipartUpload>(driver, this, oid, upload_id, owner, mtime,
					       authenticated_user);
}

namespace {

/* An upload as the directory shows it:  the key and upload id its
 * staging directory is named for, and that name, so the entry can be
 * revisited without decoding twice.  Ordered as S3 orders a listing,
 * by key and then upload id. */
struct UploadEnt {
  std::string key;
  std::string upload_id;
  std::string dname;
  /* which root it was found in, and which layout reads it.  A bucket
   * mid-upgrade has staging in both, and an entry has to remember
   * which one it came from so the owner and the mtime are read from
   * the right place. */
  nsfs::Directory* src{nullptr};
  nsfs::MPUStrategy* mpu{nullptr};

  bool operator<(const UploadEnt& o) const {
    return key != o.key ? key < o.key : upload_id < o.upload_id;
  }
};

/* Split a staging directory's meta into key and upload id.  An upload
 * id carries no dot, so the last one separates them. */
bool split_meta(const std::string& meta, std::string& key,
		std::string& upload_id)
{
  const auto dot = meta.rfind('.');
  if (dot == std::string::npos || dot == 0 || dot + 1 == meta.size()) {
    return false;
  }
  key = meta.substr(0, dot);
  upload_id = meta.substr(dot + 1);
  return true;
}

/* Holds at most max_uploads entries, keeping the smallest, from
 * entries arriving in no order.
 *
 * Every entry has to be examined before any one of them is known to
 * belong in the result.  A heap bounded at max_uploads holds only what
 * can still be returned, discarding the largest as each new entry
 * arrives, so memory follows max_uploads and not the directory. */
class UploadEntries {
  std::vector<UploadEnt> ents;
  size_t limit;
  bool dropped{false};

public:
  explicit UploadEntries(size_t _limit) : limit(_limit) {}

  void add(UploadEnt&& e) {
    ents.push_back(std::move(e));
    std::push_heap(ents.begin(), ents.end());
    if (ents.size() > limit) {
      std::pop_heap(ents.begin(), ents.end());
      ents.pop_back();
      dropped = true;
    }
  }

  /* an entry was discarded, so more uploads follow the ones returned */
  bool dropped_any() const { return dropped; }

  std::vector<UploadEnt> sorted() {
    std::sort_heap(ents.begin(), ents.end());
    return std::move(ents);
  }
};

} /* anonymous namespace */

int NSFSBucket::list_multiparts(const DoutPrefixProvider *dpp,
				  const std::string& prefix,
				  std::string& marker,
				  const std::string& delim,
				  const int& max_uploads,
				  std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				  std::map<std::string, bool> *common_prefixes,
				  bool *is_truncated, optional_yield y,
				  const rgw_owner& authenticated_user)
{
  /* S3 returns uploads ordered by key and then upload id, and a
   * directory offers no order.  Every entry is examined and the first
   * max_uploads after the marker are kept.  Examining one costs a
   * decode and a compare, because the staging directory's name
   * carries the key.
   *
   * The metadata read is paid only for the uploads returned, and only
   * because the response reports an owner. */
  std::string marker_key, marker_upload_id;
  const bool have_marker = !marker.empty();
  if (have_marker && !split_meta(marker, marker_key, marker_upload_id)) {
    marker_key = marker;
  }

  UploadEntries ents(max_uploads);
  std::map<std::string, bool> prefixes;
  /* One upload, once.
   *
   * The re-stage writes the nsfs upload before removing NooBaa's, so
   * for the length of one upload's conversion the same key and id are
   * in both roots.  The nsfs root is scanned first and wins, which is
   * also the right preference:  it is the copy that is complete. */
  std::set<std::pair<std::string, std::string>> seen;

  /* Where this format keeps staging directories.  None means no upload
   * has ever been started here, which is an empty listing and not an
   * error:  NooBaa creates multipart-uploads/ at the first
   * CreateMultipartUpload, and nsfs's is the bucket directory, which
   * always exists. */
  /* the fd, before anything asks the format about it:  for_each()
   * opens the directory itself, and staging_root() is now asked
   * first */
  int oret = dir->open(dpp);
  if (oret < 0) {
    return oret;
  }

  /* Both layouts, where the bucket is mid-upgrade.
   *
   * The conversion re-stages one upload at a time, so during it some
   * are in the nsfs root and the rest are still in NooBaa's.  Scanning one
   * would report half the uploads in flight and call the listing
   * complete, which is worse than the cost of the second scan:  a
   * staging_root() that returns nothing, on every bucket that is not
   * converting. */
  nsfs::MPUStrategy* layouts[2] = { mpu_strategy(), mpu_fallback() };
  std::unique_ptr<nsfs::Directory> roots[2];
  nsfs::Directory* scans[2] = { nullptr, nullptr };
  int ret = 0;

  for (int i = 0; i < 2; ++i) {
    auto* mpu = layouts[i];
    if (!mpu) {
      continue;
    }
    auto root = mpu->staging_root(dpp, dir->get_fd());
    if (!root) {
      continue;
    }

    nsfs::Directory* scan = dir.get();
    if (*root != ".") {
      roots[i] = std::make_unique<nsfs::Directory>(*root, dir.get(),
						   driver->ctx());
      int rret = roots[i]->open(dpp);
      if (rret < 0) {
	return rret;
      }
      scan = roots[i].get();
    }
    scans[i] = scan;

    ret = scan->for_each(dpp, [&, mpu, scan] (const char* name) {
    /* What upload is here, if any.  The nsfs format returns it from the
     * name;  NooBaa's opens the directory's metadata, because that is
     * where NooBaa keeps the key. */
    nsfs::MPUStrategy::StagedUpload su;
    if (!mpu->staged_upload(dpp, scan->get_fd(), name, su)) {
      return 0;
    }

    if (!seen.emplace(su.key, su.upload_id).second) {
      return 0;
    }

    UploadEnt ent;
    ent.key = std::move(su.key);
    ent.upload_id = std::move(su.upload_id);
    ent.dname = name;
    ent.src = scan;
    ent.mpu = mpu;

    if (!prefix.empty() && !ent.key.starts_with(prefix)) {
      return 0;
    }
    if (have_marker &&
	!(marker_key < ent.key ||
	  (marker_key == ent.key && marker_upload_id < ent.upload_id))) {
      return 0;
    }

    /* a key whose remainder after the prefix contains the delimiter is
     * reported as a common prefix and not as an upload */
    if (!delim.empty()) {
      auto pos = ent.key.find(delim, prefix.size());
      if (pos != std::string::npos) {
	prefixes[ent.key.substr(0, pos + delim.size())] = true;
	return 0;
      }
    }

    ents.add(std::move(ent));
    return 0;
    });
    if (ret < 0) {
      return ret;
    }
  }

  if (is_truncated) {
    *is_truncated = ents.dropped_any();
  }
  if (common_prefixes) {
    *common_prefixes = std::move(prefixes);
  }

  for (auto& ent : ents.sorted()) {
    struct statx stx;
    if (nsfs::statx(ent.src->get_fd(), ent.dname.c_str(), AT_SYMLINK_NOFOLLOW,
	      STATX_MTIME, &stx) < 0) {
      continue;
    }
    /* S3 reports an owner, so one has to be found -- and where it
     * lives is the format's business.  The nsfs format keeps it with
     * the upload's own record, which get_info() reads;  NooBaa's keeps
     * none anywhere, and the upload is its bucket's owner's.  This is a
     * read per upload RETURNED, not per entry examined. */
    ACLOwner owner;
    const int oret =
	ent.mpu->upload_owner(dpp, ent.src->get_fd(), ent.dname, owner);
    if (oret == -ENODATA) {
      owner = default_object_owner(dpp, y);
    }

    auto upload = std::make_unique<NSFSMultipartUpload>(
	driver, this, ent.key, ent.upload_id, owner,
	from_statx_timestamp(stx.stx_mtime), authenticated_user);

    if (oret == -ENOENT) {
      rgw_placement_rule* rule{nullptr};
      if (upload->get_info(dpp, y, &rule, nullptr) < 0) {
	continue;
      }
    }
    uploads.emplace_back(std::move(upload));
  }

  return 0;
}

/* Every upload in progress, aborted, as RadosBucket::abort_multiparts()
 * aborts them:  an upload does not keep its bucket (check_empty()), so
 * deleting the bucket ends it.  No authenticated user:  each abort runs
 * as the gateway, raised after a denial, since staging is its
 * uploader's and this is the bucket delete's scaffolding (raise
 * site 3). */
int NSFSBucket::abort_multiparts(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y)
{
  return abort_multiparts_as(dpp, cct, y, rgw::sal::no_authenticated_user);
}

int NSFSBucket::abort_multiparts_as(const DoutPrefixProvider* dpp,
				    CephContext* cct, optional_yield y,
				    const rgw_owner& authenticated_user)
{
  constexpr int max = 1000;
  std::vector<std::unique_ptr<MultipartUpload>> uploads;
  std::string marker;
  const std::string empty;
  bool is_truncated = false;
  int num_aborted = 0;

  do {
    uploads.clear();
    int ret = list_multiparts(dpp, empty, marker, empty, max, uploads,
			      nullptr, &is_truncated, y, authenticated_user);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: listing the uploads of bucket "
	<< get_name() << " to abort them: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    for (const auto& upload : uploads) {
      /* an authenticated user's abort goes through the access model itself
       * (remove_staging());  the gateway's is raised after a denial */
      auto abort = [&]() -> int { return upload->abort(dpp, cct, y); };
      ret = abort();
      if (nsfs::owner_empty(authenticated_user) &&
	  ((ret == -EACCES) || (ret == -EPERM))) {
	ret = nsfs::with_capabilities(dpp, BUCKET_DELETE_CAPS, abort);
      }
      if ((ret < 0) && (ret != -ENOENT) && (ret != -ERR_NO_SUCH_UPLOAD)) {
	ldpp_dout(dpp, 0) << "ERROR: aborting upload " << upload->get_meta()
	  << " of bucket " << get_name() << ": " << cpp_strerror(-ret)
	  << dendl;
	return ret;
      }
      ++num_aborted;
    }
  } while (is_truncated);

  if (num_aborted) {
    ldpp_dout(dpp, 4) << "nsfs: aborted " << num_aborted << " uploads in "
      << "progress in bucket " << get_name() << dendl;
  }
  return 0;
}

int NSFSBucket::make_dir(const DoutPrefixProvider* dpp, optional_yield y,
			 bool* existed, const rgw_owner& creator)
{
  if (is_internal() || nsfs::owner_empty(creator) ||
      parent_is_base_path(driver->default_bucket_location(),
			  dir->get_name())) {
    return dir->create(dpp, y, existed);
  }
  nsfs::FSIdentity id;
  const int pret = pin_personality_for(dpp, driver, creator, &id, y);
  if ((pret < 0) && (pret != -ENODATA)) {
    return pret;
  }
  if (!id.active()) {
    return dir->create(dpp, y, existed);
  }

  /* As the creator, below the parent the gateway opens O_PATH, as
   * NooBaa's mkdir as the account (bucketspace_fs.js:1062);  the path
   * above is never resolved as them.  Directory::create()'s identity
   * branch, at the bucket's own parent. */
  const std::filesystem::path path(dir->get_name());
  const std::string leaf = path.filename().string();
  const int pfd = ::open(path.parent_path().c_str(),
			 O_PATH | O_DIRECTORY | O_CLOEXEC);
  if (pfd < 0) {
    return -errno;
  }
  auto close_pfd = make_scope_guard([pfd] { ::close(pfd); });
  const mode_t mode = nsfs::dir_create_mode();
  bool raised = false;
  const int ret = nsfs::with_identity_or_raise(dpp, id,
					       driver->get_access_strategy(),
					       nsfs::AccessClass::data,
					       RAISED_CREATE_CAPS,
					       [&]() -> int {
    if (::mkdirat(pfd, leaf.c_str(), mode) < 0) {
      return -errno;
    }
    return nsfs::apply_create_mode_at(dpp, pfd, leaf.c_str(), mode);
  }, y, &raised);
  if (ret == -EEXIST) {
    if (existed != nullptr) {
      *existed = true;
    }
    return 0;
  }
  if (ret < 0) {
    ldpp_dout(dpp, removal_log_level(-ret)) << removal_log_prefix(-ret)
      << "could not create bucket " << dir->get_name() << " as uid "
      << id.cred.uid << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return raised ? chown_raised_dir(dpp, pfd, leaf.c_str()) : 0;
}

int NSFSBucket::create(const DoutPrefixProvider* dpp, optional_yield y, bool* existed,
		       const rgw_owner& creator)
{
  int ret = make_dir(dpp, y, existed, creator);
  if (ret < 0) {
    return ret;
  }

  /* A client's bucket is never created over a directory that is already
   * there:  NooBaa's mkdir fails and the bucket with it, and a directory
   * nobody recorded is not ours to take.  Rejected before anything is
   * written to it. */
  if (!is_internal() && existed && *existed) {
    ldpp_dout(dpp, 1) << "nsfs: not creating bucket " << get_name()
      << ":  its directory " << dir->get_name() << " already exists"
      << dendl;
    return -EEXIST;
  }

  const bool made = !existed || !*existed;

  /* Stamp a bucket we made, so that everything this deployment creates
   * is marked and only trees that came from somewhere else are not.
   * Only an internal bucket reaches here over a directory that already
   * exists -- a client's is rejected above -- and that directory keeps
   * the profile it already has:  marking is adoption's act, not a side
   * effect of re-creating scaffolding. */
  if (!is_internal() && driver->create_failure_injected()) {
    ret = -EIO;
  } else if (made) {
    ret = mark_extensions(dpp, driver->new_bucket_extensions());
  } else if (!profile) {
    profile = driver->get_base_profile();
  }
  if (ret == 0) {
    ret = write_attrs(dpp, y);
  }

  /* Raise site 4:  a client's bucket directory is its creator's, as NooBaa's
   * mkdir as the performing account makes it (bucketspace_fs.js:1055-1062,
   * DECISIONS.md 2026-10-23);  the owner's where no request made it.  Given
   * last, so the gateway writes the state above on a directory it still
   * owns.  Credentials that do not resolve deny the bucket rather than leave
   * the directory the gateway's (`RGW_NSFS_VADE_MECUM.md` §12, "No
   * authenticated user means refuse"). */
  if ((ret == 0) && made && !is_internal() &&
      nsfs::impersonation_enabled()) {
    const rgw_owner& who = nsfs::owner_empty(creator) ? info.owner : creator;
    nsfs::Credentials cred;
    if (!nsfs::owner_empty(creator)) {
      ret = resolve_credentials_for(dpp, driver, creator, y, nullptr, cred);
    } else {
      ret = nsfs::resolve_credentials(dpp, *driver->get_identity_db(),
				      info.owner, cred, y);
    }
    if (ret == 0) {
      ret = chown_raised(dpp, dir->get_fd(), "", cred.uid, cred.gid);
    } else if ((ret == -ENOENT) &&
	       driver->unresolved_as_gateway_injected(who)) {
      ret = 0;
    } else if (ret == -ENOENT) {
      ldpp_dout(dpp, 1) << "nsfs: bucket " << get_name() << " denied:  no "
	"credentials resolve for " << who << dendl;
      ret = -EACCES;
    }
  }

  /* A client's bucket that failed here is taken back whole:  the
   * directory this call made goes, and the caller then removes the row.
   * Left in place it would hold the name, since creation rejects a
   * directory that already exists. */
  if ((ret < 0) && made && !is_internal()) {
    /* by whoever made it:  under a location the account owns the
     * gateway cannot remove its entry (make_dir()) */
    nsfs::FSIdentity id;
    if (!nsfs::owner_empty(creator) &&
	!parent_is_base_path(driver->default_bucket_location(),
			     dir->get_name())) {
      (void) pin_personality_for(dpp, driver, creator, &id, y);
    }
    const int r = id.active()
      ? remove_dir_as(dpp, y, id, driver->get_access_strategy(), true)
      : dir->remove(dpp, y, true);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: bucket " << get_name() << " failed, and "
	<< "the directory it made, " << dir->get_name() << ", could not be "
	<< "removed: " << cpp_strerror(-r) << dendl;
    }
  }
  return ret;
}

std::string NSFSBucket::get_fname()
{
  /* A staging bucket wraps a directory MPUStrategy named, so ask the
   * directory rather than rendering the name a second way.  The two
   * renderings agreed while both were the upload id;  they do not
   * now, and a second renderer is a second thing to keep in step. */
  if (ns && *ns == mp_ns && dir) {
    return dir->get_name();
  }
  return driver->get_path_strategy()->bucket_dir_name(get_name(), ns);
}

int NSFSBucket::rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj)
{
  int ret;
  Directory* dst_dir = dir->get_parent();

  info.bucket.name = target_obj->get_key().get_oid();
  ns.reset();

  if (!target_obj->get_instance().empty()) {
    /* This is a versioned object.  Need to handle versioneddirectory */
    NSFSObject *to = static_cast<NSFSObject *>(target_obj);
    ret = to->open(dpp, y, true, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not open target obj " << to->get_name() << dendl;
      return ret;
    }
    dst_dir = static_cast<Directory *>(to->get_fsent());
  }

  return dir->rename(dpp, y, dst_dir, get_fname());
}

int NSFSObject::delete_object(const DoutPrefixProvider* dpp,
				optional_yield y,
				uint32_t flags,
                                std::list<rgw_obj_index_key>* remove_objs,
				RGWObjVersionTracker* objv)
{
  NSFSBucket *b = static_cast<NSFSBucket*>(get_bucket());
  if (!b) {
      ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name() << dendl;
      return -EINVAL;
  }

  int ret = stat(dpp);
  if (ret < 0) {
      if (ret == -ENOENT) {
	/* No object, but the name may hold a link, which is not one, and
	 * a DELETE of its name unlinks it */
	std::vector<std::unique_ptr<Directory>> chain;
	Directory* leaf_dir;
	std::string leaf_name;
	if ((resolve_key_path(dpp, b->get_dir(), state.obj.key,
			      get_fname(/*use_version=*/false),
			      driver->get_reserved_names(), b->is_internal(),
			      /*create_dirs=*/false, driver->ctx(),
			      chain, leaf_dir, leaf_name, pending_identity,
			      driver->get_access_strategy()) < 0) ||
	    (leaf_dir->open(dpp) < 0)) {
	  return 0;
	}
	return unlink_if_link(dpp, pending_identity,
			      driver->get_access_strategy(),
			      leaf_dir->get_fd(), leaf_name.c_str());
      }
      return ret;
  }

  ret = ent->remove(dpp, y, /*delete_children=*/false);
  if (ret < 0)
    return ret;

  for (auto it = dir_chain.rbegin(); it != dir_chain.rend(); ++it) {
    int r = (*it)->remove(dpp, y, /*delete_children=*/false);
    if (r < 0)
      break;
  }
  dir_chain.clear();

  cls_rgw_obj_key key;
  get_key().get_index_key(&key);

  driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);

  if (!key.instance.empty() && !ent->exists()) {
    /* Remove the non-versioned key as well */
    key.instance.clear();
    driver->get_bucket_cache()->remove_entry(dpp, b->get_name(), key);
  }
  driver->get_quota_handler()->update_stats(b->get_owner(), b->get_key(),
                                            -1, 0, state.accounted_size);
  return 0;
}

/* Move an object, with its history, without copying data.
 *
 * PROVISIONAL -- see RENAME_DESIGN.md.  This is the single-object case;  the
 * general operation is expected to be a prefix-capable rebind.
 *
 * The order is versions first and the leaf last, so the leaf rename is the
 * commit point and the object is resolvable at exactly one key throughout.
 * An intent xattr on the source .versions/ makes an interrupted move
 * recognisable;  the leaf's location tells recovery which way to go, so the
 * record carries no progress state.
 *
 * Serialised against other version mutations by the version lock.  Listing
 * does not take that lock -- it does not today for demote/promote either --
 * so a caller racing a rename may observe an intermediate state, and one who
 * is not racing never does.  That is a deliberate choice, argued in
 * RENAME_DESIGN.md;  locking listing would cost every versioned LIST.
 */
int NSFSObject::rename(const DoutPrefixProvider* dpp, optional_yield y,
		       rgw::sal::Bucket* dest_bucket,
		       const rgw_obj_key& dest_key, uint32_t flags,
		       const rgw_owner& authenticated_user)
{
  NSFSBucket* sb = static_cast<NSFSBucket*>(get_bucket());
  NSFSBucket* db = static_cast<NSFSBucket*>(dest_bucket);
  if (! sb || ! db) {
    return -EINVAL;
  }

  /* both trees:  a rename moves a name out of one bucket and into
   * another, and either may be mid-rewrite.  Ordered by name so two
   * renames crossing in opposite directions cannot each hold the
   * gate the other wants. */
  NSFSBucket* first = (sb->get_name() < db->get_name()) ? sb : db;
  NSFSBucket* second = (first == sb) ? db : sb;
  NSFSMutationGuard mg1(dpp, driver, first);
  if (mg1.result() < 0) {
    return mg1.result();
  }
  std::optional<NSFSMutationGuard> mg2;
  if (first->get_name() != second->get_name()) {
    mg2.emplace(dpp, driver, second);
    if (mg2->result() < 0) {
      return mg2->result();
    }
  }

  /* the authenticated user's, from the walk through the moves to the intent
   * record:  both key directories and their `.versions` are theirs.
   * Nobody named is the gateway, as for set_obj_attrs(). */
  nsfs::FSIdentity id;
  if (! nsfs::owner_empty(authenticated_user)) {
    const int pret = pin_personality_for(dpp, driver, authenticated_user,
					 &id, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
    if (id.active()) {
      set_access(id, /*ro=*/false);
    }
  }
  /* what this rename chose, for the `ran-as` hint:  before anything
   * touches the filesystem, so a rename that then fails still says */
  driver->note_ran_as("rename", to_string(authenticated_user), id);
  const nsfs::AccessStrategy* model = driver->get_access_strategy();

  int ret = stat(dpp);
  if (ret < 0) {
    return ret;
  }

  nsfs::FSEnt* sent = get_fsent();
  if (! sent || ! sent->get_parent()) {
    return -EINVAL;
  }
  auto* src_file = dynamic_cast<nsfs::File*>(sent);
  if (! src_file) {
    /* a directory object is the prefix case, which this is not */
    return -ENOTSUP;
  }

  nsfs::Directory* src_dir = sent->get_parent();
  int src_parent_fd = src_dir->get_fd();
  if (src_parent_fd < 0) {
    (void) src_dir->open(dpp);
    src_parent_fd = src_dir->get_fd();
  }
  if (src_parent_fd < 0) {
    return -EIO;
  }
  const std::string src_leaf = sent->get_name();

  /* Reject before deciding how to do it.
   *
   * Moving history into a bucket that cannot hold it is rejected further
   * down, under the version lock, where the list is authoritative.  But
   * that check used to sit behind the mechanism gates below:  a driver
   * which cannot move a name returns -ENOTSUP first, and RGWLibFS::rename
   * then emulates the rename by copy-and-delete (rgw_file.cc), performing
   * the very operation this rejects and dropping the history silently.  On
   * GPFS that is every rename.  So the policy is asked first and the
   * mechanism second.  The check under the lock stays and remains the
   * authoritative one;  this one exists to precede the gates. */
  if (! (flags & FLAG_SLICE_VERSIONS) && ! db->get_info().versioned()) {
    std::vector<LeafVersion> pre_versions;
    if ((list_leaf_versions(src_parent_fd, src_leaf, pre_versions) == 0) &&
	! pre_versions.empty()) {
      ldpp_dout(dpp, 4) << "rename: source has " << pre_versions.size()
	<< " non-current version(s) and " << db->get_name()
	<< " cannot hold them;  rejecting the rename.  "
	<< "FLAG_SLICE_VERSIONS moves the current version and drops the "
	<< "history" << dendl;
      return -EINVAL;
    }
  }

  if (! driver->ctx()->_conf->rgw_nsfs_enable_rename) {
    return -ENOTSUP;
  }
  FSStrategy* fs_strategy = driver->get_fs_strategy();
  if (! fs_strategy || ! fs_strategy->can_rename()) {
    /* GPFS lands here deliberately */
    return -ENOTSUP;
  }


  /* Serialise against other version mutations on the source.  Taken here
   * rather than just before the first move:  recovery below repairs the
   * source .versions/, and the version list gathered further down has to be
   * the one recovery left behind.
   *
   * The lock and the intent record are both per-directory, so two renames
   * out of the same directory serialise against each other and there is
   * never more than one outstanding record to recover. */
  int recovered = 0;
  auto vlock = version_lock_recovering(dpp, driver, xattr_strategy(), sb->get_name(),
				       src_parent_fd, id, &recovered);
  if (recovered < 0) {
    /* one intent record per directory:  writing a new one over a stuck
     * one would lose the only pointer to the stranded versions.
     * Unlike the other mutations, this caller cannot proceed. */
    ldpp_dout(dpp, 0) << "ERROR: rename: failing while an earlier move out "
      << "of this directory is unrecovered" << dendl;
    return recovered;
  }

  /* resolve the destination, creating intermediate directories */
  std::vector<std::unique_ptr<nsfs::Directory>> dchain;
  nsfs::Directory* dst_dir{nullptr};
  std::string dst_leaf;
  ret = nsfs::resolve_key_path(dpp, db->get_dir(), dest_key, dest_key.name,
			       driver->get_reserved_names(), db->is_internal(),
			       /*create_dirs=*/true, driver->ctx(),
			       dchain, dst_dir, dst_leaf,
			       id, driver->get_access_strategy());
  if (ret < 0 || ! dst_dir) {
    return (ret < 0) ? ret : -EIO;
  }
  int dst_parent_fd = dst_dir->get_fd();
  if (dst_parent_fd < 0) {
    (void) dst_dir->open(dpp);
    dst_parent_fd = dst_dir->get_fd();
  }
  if (dst_parent_fd < 0) {
    return -EIO;
  }

  /* what history there is, and whether the destination can hold it */
  std::vector<LeafVersion> versions;
  ret = list_leaf_versions(src_parent_fd, src_leaf, versions);
  if (ret < 0) {
    return ret;
  }

  /* versioned() covers suspended too:  such a bucket still holds
   * non-current versions, it only stops minting new ones */
  const bool dst_holds_history = db->get_info().versioned();
  const bool slice = (flags & FLAG_SLICE_VERSIONS);

  if (! versions.empty() && ! dst_holds_history && ! slice) {
    ldpp_dout(dpp, 4) << "rename: source has " << versions.size()
      << " non-current version(s) and " << db->get_name()
      << " cannot hold them;  rejecting the rename.  "
      << "FLAG_SLICE_VERSIONS moves the current version and drops the "
      << "history" << dendl;
    return -EINVAL;
  }
  const bool slicing = (! versions.empty() && ! dst_holds_history && slice);

  ret = write_rename_intent(dpp, xattr_strategy(), src_parent_fd, id, model,
			    src_leaf, db->get_tenant(),
			    db->get_name(), dest_key.name, slicing);
  if (ret < 0) {
    return ret;
  }

  int src_vfd = -1;
  int dst_vfd = -1;
  size_t moved = 0;

  auto roll_back = [&]() {
    for (size_t k = 0; k < moved; ++k) {
      std::string dname = dst_leaf + "_" + versions[k].ver_id;
      const int rret = renameat_as(dpp, id, model, dst_vfd, dname.c_str(),
				   src_vfd, versions[k].entry.c_str());
      if (rret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: rename rollback failed for " << dname
	  << ": " << cpp_strerror(-rret) << ".  The intent record is left in "
	  << "place;  " << src_leaf << " needs manual attention" << dendl;
	return false;
      }
    }
    return true;
  };

  if (! versions.empty() && ! slicing) {
    dst_vfd = open_versions_dir(dpp, dst_parent_fd, id, model);
    src_vfd = openat_as(dpp, id, model, src_parent_fd,
			HIDDEN_VERSIONS_PATH.c_str(),
			O_RDONLY | O_DIRECTORY);
    if (dst_vfd < 0 || src_vfd < 0) {
      ret = (dst_vfd < 0) ? dst_vfd : src_vfd;
      if (dst_vfd >= 0) { ::close(dst_vfd); }
      if (src_vfd >= 0) { ::close(src_vfd); }
      clear_rename_intent(dpp, xattr_strategy(), src_parent_fd, id, model);
      return ret;
    }

    for (; moved < versions.size(); ++moved) {
      std::string dname = dst_leaf + "_" + versions[moved].ver_id;
      const int fail_after = driver->rename_fail_after_injected();
      if (fail_after >= 0 && (int) moved >= fail_after) {
	ret = -EIO;
	if (driver->rename_abandon_injected() == 1) {
	  /* stand in for a crash:  versions moved, leaf not, record left */
	  ldpp_dout(dpp, 4) << "rename: injected abandon after " << moved
	    << " version(s);  leaving the intent record" << dendl;
	  ::close(src_vfd);
	  ::close(dst_vfd);
	  return ret;
	}
	ldpp_dout(dpp, 4) << "rename: injected failure after " << moved
	  << " version(s);  rolling back" << dendl;
	if (roll_back()) {
	  clear_rename_intent(dpp, xattr_strategy(), src_parent_fd, id, model);
	}
	::close(src_vfd);
	::close(dst_vfd);
	return ret;
      }
      ret = renameat_as(dpp, id, model, src_vfd, versions[moved].entry.c_str(),
			dst_vfd, dname.c_str());
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: rename: moving version "
	  << versions[moved].entry << " failed: " << cpp_strerror(-ret)
	  << ";  rolling back" << dendl;
	if (roll_back()) {
	  clear_rename_intent(dpp, xattr_strategy(), src_parent_fd, id, model);
	}
	::close(src_vfd);
	::close(dst_vfd);
	return ret;
      }
    }
  }

  /* COMMIT:  the leaf */
  ret = src_file->rename(dpp, y, dst_dir, dst_leaf);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: rename: moving " << src_leaf
      << " failed: " << cpp_strerror(-ret) << ";  rolling back" << dendl;
    if (roll_back()) {
      clear_rename_intent(dpp, xattr_strategy(), src_parent_fd, id, model);
    }
    if (src_vfd >= 0) { ::close(src_vfd); }
    if (dst_vfd >= 0) { ::close(dst_vfd); }
    return ret;
  }

  if (driver->rename_abandon_injected() == 2) {
    /* stand in for a crash between the commit and the cleanup:  the leaf has
     * landed, so recovery must roll *forward* from here */
    ldpp_dout(dpp, 4) << "rename: injected abandon after the leaf landed;  "
      << "leaving the intent record" << dendl;
    if (src_vfd >= 0) { ::close(src_vfd); }
    if (dst_vfd >= 0) { ::close(dst_vfd); }
    return -EIO;
  }

  /* slicing:  the history must not outlive the key it belonged to, or it
   * lists as versions of an object that is no longer there */
  if (slicing) {
    int svfd = openat_as(dpp, id, model, src_parent_fd,
			 HIDDEN_VERSIONS_PATH.c_str(),
			 O_RDONLY | O_DIRECTORY);
    if (svfd >= 0) {
      for (const auto& v : versions) {
	const int uret = unlinkat_as(dpp, id, model, svfd, v.entry.c_str());
	if (uret < 0) {
	  ldpp_dout(dpp, 0) << "ERROR: rename: sliced version " << v.entry
	    << " could not be removed: " << cpp_strerror(-uret) << dendl;
	}
      }
      ::close(svfd);
    }
  }

  clear_rename_intent(dpp, xattr_strategy(), src_parent_fd, id, model);
  if (src_vfd >= 0) { ::close(src_vfd); }
  if (dst_vfd >= 0) { ::close(dst_vfd); }

  /* Cache:  move the entries, do not drop the bucket and do not re-describe
   * them.
   *
   * A rename changes the key and nothing else, so the cached entries are
   * rekeyed in place -- see BucketCache::rename_entries(), which does it in
   * one bounded walk per chunk.  Re-reading each object to rebuild its entry
   * would duplicate fill_cache()'s job and risk describing it differently
   * from how it was described before, for no gain. */
  {
    auto* bcache = driver->get_bucket_cache();
    const std::string src_key = get_key().get_index_key_name();
    const bool dst_versioned = db->get_info().versioned();

    if (bcache) {
      if (slicing) {
	/* only the current version survives, and it lands somewhere that
	 * holds no versions:  it must shed its instance and FLAG_VER or it
	 * would list as a version in a bucket that has none */
	(void) bcache->rename_entries(dpp, sb->get_name(), src_key,
	  db->get_name(), dest_key.name,
	  [](const rgw_bucket_dir_entry& e) {
	    return (e.flags & rgw_bucket_dir_entry::FLAG_CURRENT) != 0;
	  },
	  [](rgw_bucket_dir_entry& e) {
	    e.key.instance.clear();
	    e.flags &= ~rgw_bucket_dir_entry::FLAG_VER;
	  });
	/* and the history that was unlinked above goes from the cache too */
	(void) bcache->remove_entries(dpp, sb->get_name(), src_key,
	  [](const rgw_bucket_dir_entry&) { return true; });
      } else {
	(void) bcache->rename_entries(dpp, sb->get_name(), src_key,
	  db->get_name(), dest_key.name,
	  [](const rgw_bucket_dir_entry&) { return true; },
	  [dst_versioned](rgw_bucket_dir_entry& e) {
	    if (! dst_versioned) {
	      e.key.instance.clear();
	      e.flags &= ~rgw_bucket_dir_entry::FLAG_VER;
	    }
	  });
      }
    }
  }

  ldpp_dout(dpp, 10) << "rename: " << sb->get_name() << "/" << src_leaf
    << " -> " << db->get_name() << "/" << dest_key.name << " ("
    << versions.size() << " version(s)"
    << (slicing ? ", sliced" : "") << ")" << dendl;
  return 0;
}

int NSFSObject::copy_object(const ACLOwner& owner,
                              const rgw_user& remote_user,
                              const rgw_owner& authenticated_user,
                              req_info* info,
                              const rgw_zone_id& source_zone,
                              rgw::sal::Object* dest_object,
                              rgw::sal::Bucket* dest_bucket,
                              rgw::sal::Bucket* src_bucket,
                              const rgw_placement_rule& dest_placement,
                              ceph::real_time* src_mtime,
                              ceph::real_time* mtime,
                              const ceph::real_time* mod_ptr,
                              const ceph::real_time* unmod_ptr,
                              bool high_precision_time,
                              const char* if_match,
                              const char* if_nomatch,
                              AttrsMod attrs_mod,
                              bool copy_if_newer,
                              Attrs& attrs,
                              RGWObjCategory category,
                              uint64_t olh_epoch,
                              boost::optional<ceph::real_time> delete_at,
                              std::string* version_id,
                              std::string* tag,
                              std::string* etag,
                              void (*progress_cb)(off_t, void *),
                              void* progress_data,
                              rgw::sal::DataProcessorFactory* dp_factory,
                              const DoutPrefixProvider* dpp,
                              optional_yield y)
{
  /* both trees, ordered by name;  see NSFSObject::rename().  The source
   * is read, not written, so the gate on a bucket not taken over asks
   * only of the destination;  a self-copy writes its one bucket. */
  NSFSBucket* csrc = static_cast<NSFSBucket*>(src_bucket);
  NSFSBucket* cdst = static_cast<NSFSBucket*>(dest_bucket);
  const bool cself = (csrc->get_name() == cdst->get_name());
  NSFSBucket* cfirst = (csrc->get_name() < cdst->get_name()) ? csrc : cdst;
  NSFSBucket* csecond = (cfirst == csrc) ? cdst : csrc;
  auto cpurpose = [&](NSFSBucket* b) {
    return (!cself && (b == csrc)) ? MutationPurpose::copy_source
				   : MutationPurpose::client;
  };
  NSFSMutationGuard cg1(dpp, driver, cfirst, cpurpose(cfirst));
  if (cg1.result() < 0) {
    return cg1.result();
  }
  std::optional<NSFSMutationGuard> cg2;
  if (!cself) {
    cg2.emplace(dpp, driver, csecond, cpurpose(csecond));
    if (cg2->result() < 0) {
      return cg2->result();
    }
  }
  int ret;
  NSFSBucket *db = static_cast<NSFSBucket*>(dest_bucket);
  NSFSBucket *sb = static_cast<NSFSBucket*>(src_bucket);
  NSFSObject *dobj = static_cast<NSFSObject*>(dest_object);

  /* Who copies:  the authenticated user reads the source and owns the
   * copy, as NooBaa's copy runs as the request's account.  Denied
   * where it cannot be applied, as the writers deny it. */
  nsfs::FSIdentity identity;
  {
    const int pret = pin_personality_for(dpp, driver, authenticated_user,
					 &identity, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }

  if (!db || !sb) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket to copy " << get_name()
                      << dendl;
    return -EINVAL;
  }
  ret = dobj->admit_write(dpp, y);
  if (ret < 0) {
    return ret;
  }
  bool has_instance = !get_key().instance.empty();

  ldpp_dout(dpp, 10) << "copy_object: src=" << get_key()
    << " dst=" << dobj->get_key()
    << " src_bucket=" << sb->get_name()
    << " dst_bucket=" << db->get_name() << dendl;

  // Source must exist, and we need to know if it's a shadow obj
  if (!check_exists(dpp)) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not stat object " << get_name() << ": "
                      << cpp_strerror(ret) << dendl;
    return -ret;
  }
  if (identity.active() && ent) {
    ent->set_identity(identity);
  }

  /* check copy-source preconditions against the source object */
  if (if_match) {
    std::string if_match_str = rgw_string_unquote(if_match);
    bufferlist etag_bl;
    if (get_attr(RGW_ATTR_ETAG, etag_bl) &&
	if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (if_nomatch) {
    std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
    bufferlist etag_bl;
    if (get_attr(RGW_ATTR_ETAG, etag_bl) &&
	if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (mod_ptr && state.mtime <= *mod_ptr) {
    return -ERR_PRECONDITION_FAILED;
  }
  if (unmod_ptr && state.mtime > *unmod_ptr) {
    return -ERR_PRECONDITION_FAILED;
  }

  if (!get_key().instance.empty() && !has_instance) {
    /* For copy, no instance meance copy all instances.  Clear intance id if it
     * was passed in clear. */
    get_key().instance.clear();
  }

  const auto& dbinfo = db->get_info();
  bool dest_versioned = dbinfo.versioned();
  bool dest_ver_enabled = dbinfo.versioning_enabled();
  DemoteResult demote;
  std::unique_ptr<VersionLockHandle> vlock;

  /* versioned copy: demote existing dest current to .versions/
   * (includes self-copy — S3 requires a new version even when
   * source == dest with MetadataDirective=REPLACE) */
  if (dest_versioned) {
    dobj->ent.reset();
    (void)dobj->stat(dpp);
    nsfs::FSEnt* dest_ent = dobj->get_fsent();

    int dest_parent_fd = -1;
    std::string dest_leaf;
    if (dest_ent && dest_ent->get_parent()) {
      nsfs::Directory* dp = dest_ent->get_parent();
      dest_parent_fd = dp->get_fd();
      if (dest_parent_fd < 0) {
        dp->open(dpp);
        dest_parent_fd = dp->get_fd();
      }
      dest_leaf = dest_ent->get_name();
    }

    if (dest_parent_fd >= 0) {
      vlock = version_lock_recovering(dpp, driver,
				      static_cast<NSFSBucket*>(dest_bucket)
					->xattr_strategy(),
				      dest_bucket->get_name(),
				      dest_parent_fd, identity);

      int dret2 = demote_current_version(dpp, driver->get_fs_strategy(),
			   xattr_strategy(),
					 dest_parent_fd, dest_leaf,
					 dest_ver_enabled, demote, identity,
					 driver->get_access_strategy(), y,
					 static_cast<NSFSBucket*>(dest_bucket)
					   ->xattr_fallback());
      if (dret2 < 0) {
        return dret2;
      }
    }
  }

  bool same_key = (get_bucket()->get_name() == dobj->get_bucket()->get_name() &&
                   get_key().name == dobj->get_key().name);
  if (!same_key) {
    /* cross-object copy */
    ret = copy(dpp, y, sb, db, dobj,
	       driver->copy_hints_for(authenticated_user));
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed to copy object " << get_key()
                          << dendl;
        return ret;
    }
  } else if (demote.did_demote && ent) {
    rgw_obj_key dst_key = dobj->get_key();
    std::vector<std::unique_ptr<nsfs::Directory>> dst_chain;
    nsfs::Directory* dst_leaf_dir = nullptr;
    std::string dst_leaf_name;
    ret = nsfs::resolve_key_path(dpp, db->get_dir(), dst_key,
        path_strategy()->object_name(dst_key, false),
        driver->get_reserved_names(), db->is_internal(),
        /*create_dirs=*/true, driver->ctx(),
        dst_chain, dst_leaf_dir, dst_leaf_name,
        identity, driver->get_access_strategy());
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: copy_object: self-copy resolve_path failed: "
        << cpp_strerror(-ret) << dendl;
      return ret;
    }
    /* the source as the authenticated user, and as the gateway where the
     * filesystem denies it and the model permits, as the cross-object
     * copy opens it (clone_file_by_copy());  clone_fd() then reopens
     * it as that user and falls back to this descriptor only where
     * the model permits.  The gateway's descriptor is this call's, never
     * the entry's (with_cross_account_retry()). */
    int src_fd = ent->get_fd();
    int gw_fd = -1;
    if (src_fd < 0) {
      int oret = ent->open(dpp);
      src_fd = ent->get_fd();
      if ((src_fd < 0) && (oret == -EACCES) && copy_source_as_gateway()) {
	oret = nsfs::with_capabilities(dpp, nsfs::cap_bit(CAP_DAC_OVERRIDE),
				       [&]() -> int {
	  gw_fd = ent->open_scratch_as_gateway(dpp, /*read_only=*/true);
	  return (gw_fd < 0) ? gw_fd : 0;
	});
	src_fd = gw_fd;
      }
      if (src_fd < 0) {
	return (oret < 0) ? oret : -EIO;
      }
    }
    auto close_gw = make_scope_guard([gw_fd] {
      if (gw_fd >= 0) {
	::close(gw_fd);
      }
    });
    ret = driver->get_fs_strategy()->clone_fd(
      dpp, src_fd, dst_leaf_dir->get_fd(), dst_leaf_name, y,
      driver->copy_hints_for(authenticated_user), identity,
      copy_source_as_gateway());
    if (ret < 0) {
      return ret;
    }
  }
  vlock.reset();
  dobj->set_access(identity, /*ro=*/false);
  dobj->make_ent(ent->get_type());
  /* the copy is the authenticated user's, so its attributes are
   * written as it */
  if (identity.active() && dobj->get_fsent()) {
    dobj->get_fsent()->set_identity(identity);
  }

  /* Set up attributes for destination */
  Attrs src_attrs = state.attrset;
  /* Come attrs are never copied */
  src_attrs.erase(RGW_ATTR_DELETE_AT);
  src_attrs.erase(RGW_ATTR_OBJECT_RETENTION);
  src_attrs.erase(RGW_ATTR_OBJECT_LEGAL_HOLD);
  /* Some attrs, if they exist, always come from the call */
  src_attrs[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  bufferlist rt;
  if (get_attr(RGW_ATTR_OBJECT_RETENTION, rt)) {
    src_attrs[RGW_ATTR_OBJECT_RETENTION] = rt;
  }
  bufferlist lh;
  if (get_attr(RGW_ATTR_OBJECT_LEGAL_HOLD, lh)) {
    src_attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = lh;
  }

  bufferlist tt;
  switch (attrs_mod) {
  case ATTRSMOD_REPLACE:
    /* Keep tags if not set */
    if (!attrs[RGW_ATTR_ETAG].length()) {
      attrs[RGW_ATTR_ETAG] = src_attrs[RGW_ATTR_ETAG];
    }
    if (!attrs[RGW_ATTR_TAIL_TAG].length() &&
	rgw::sal::get_attr(src_attrs, RGW_ATTR_TAIL_TAG, tt)) {
      attrs[RGW_ATTR_TAIL_TAG] = tt;
    }
    break;

  case ATTRSMOD_MERGE:
    for (auto it = src_attrs.begin(); it != src_attrs.end(); ++it) {
      if (attrs.find(it->first) == attrs.end()) {
	attrs[it->first] = it->second;
      }
    }
    break;
  case ATTRSMOD_NONE:
    attrs = src_attrs;
    ret = 0;
    break;
  }

  /* Some attrs always come from the source */
  bufferlist com;
  if (rgw::sal::get_attr(src_attrs, RGW_ATTR_COMPRESSION, com)) {
    attrs[RGW_ATTR_COMPRESSION] = com;
  }
  bufferlist mpu;
  if (rgw::sal::get_attr(src_attrs, RGW_NSFS_ATTR_MPUPLOAD, mpu)) {
    attrs[RGW_NSFS_ATTR_MPUPLOAD] = mpu;
  }
  /* A replace that lands on the object itself has to remove the user
   * metadata it is replacing.
   *
   * set_obj_attrs() merges, so for a distinct destination the new
   * object simply carries `attrs` and there is nothing stale to
   * remove -- which is why REPLACE was already right there.  A
   * self-copy updates the object in place, so every key the request
   * did not send survived:  an object PUT with x-amz-meta-srcmeta and
   * then self-copied with REPLACE and x-amz-meta-dstmeta came back
   * carrying both, so a client replacing its metadata in order to
   * *remove* a key did not remove it.
   *
   * Only the x-amz-meta- family, and only by name.
   * AttrWriteMode::REPLACE_ALL would do this generically and is the
   * wrong tool:  it prunes every name that parses as an nsfs name and
   * is absent from the write set, which on a versioned object includes
   * user.nsfs.version_id and the delete marker.  Removing a name the
   * object does not carry is a no-op -- remove_xattrs() ignores
   * ENODATA -- so this is safe where the destination is fresh. */
  Attrs stale_meta;
  if ((attrs_mod == rgw::sal::ATTRSMOD_REPLACE) &&
      (dobj->get_bucket()->get_name() == get_bucket()->get_name()) &&
      (dobj->get_key().name == get_key().name)) {
    const std::string meta_prefix{RGW_ATTR_META_PREFIX};
    for (const auto& [key, bl] : src_attrs) {
      if (key.starts_with(meta_prefix) && (attrs.find(key) == attrs.end())) {
	stale_meta[key] = bl;
      }
    }
    if (!stale_meta.empty()) {
      ldpp_dout(dpp, 20) << "NSFSObject::copy: replacing in place, dropping "
	<< stale_meta.size() << " superseded metadata attributes from "
	<< get_key() << dendl;
    }
  }

  ret = dobj->set_obj_attrs(dpp, &attrs,
			    stale_meta.empty() ? nullptr : &stale_meta,
			    y, rgw::sal::FLAG_LOG_OP, authenticated_user);
  if (ret < 0) {
    return ret;
  }

  /* versioned copy: compute version ID and update cache */
  if (dest_versioned && dobj->get_fsent()) {
    dobj->get_fsent()->stat(dpp, /*force=*/true);
    const struct statx& new_stx = dobj->get_fsent()->get_stx();
    std::string new_ver_id = dest_ver_enabled
      ? nsfs_version_id_from_statx(new_stx)
      : NULL_VERSION_ID;
    dest_object->set_instance(new_ver_id);
    if (version_id) {
      *version_id = new_ver_id;
    }
    int obj_fd = dobj->get_fsent()->get_fd();
    if (obj_fd >= 0) {
      std::string vid_xattr = xattr_strategy()->disk_name(RGW_NSFS_ATTR_VERSION_ID);
      /* as the copy's owner, and checked, as link_temp_file() is */
      const int vret = nsfs::with_identity(dpp, identity, [&]() -> int {
        if (driver->attr_write_fail_injected(db->get_name())) {
          driver->record_attr_write_fail();
          return -EIO;
        }
        return (::fsetxattr(obj_fd, vid_xattr.c_str(),
                            new_ver_id.c_str(), new_ver_id.size(), 0) < 0)
          ? -errno : 0;
      }, y);
      if (vret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not record the version id on "
          << dobj->get_name() << ": " << cpp_strerror(-vret)
          << ";  a later demote would file it as the null version"
          << dendl;
        return vret;
      }
    }

    auto* bcache = driver->get_bucket_cache();
    std::string obj_name = dobj->get_key().get_index_key_name();

    if (demote.did_demote) {
      cls_rgw_obj_key old_key;
      old_key.name = obj_name;
      old_key.instance = demote.demoted_ver_id;
      bcache->remove_entry(dpp, db->get_name(), old_key);

      rgw_bucket_dir_entry dem_bde{};
      fill_demoted_entry(obj_name, demote, db->default_object_owner(dpp, y),
			 dem_bde);
      bcache->add_entry(dpp, db->get_name(), dem_bde);
    }

    rgw_bucket_dir_entry new_bde{};
    new_bde.key.name = obj_name;
    new_bde.key.instance = new_ver_id;
    new_bde.ver.pool = 1;
    new_bde.ver.epoch = 1;
    new_bde.exists = true;
    new_bde.meta.category = RGWObjCategory::Main;
    new_bde.meta.size = new_stx.stx_size;
    new_bde.meta.accounted_size = new_stx.stx_size;
    new_bde.meta.mtime = from_statx_timestamp(new_stx.stx_mtime);
    new_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
    /* the source's digest, propagated into attrs above;  a copy preserves
     * the source etag */
    auto eit = attrs.find(RGW_ATTR_ETAG);
    new_bde.meta.etag = (eit != attrs.end())
      ? eit->second.to_str() : synthesize_etag(new_stx);
    new_bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                    rgw_bucket_dir_entry::FLAG_CURRENT;
    {
      ACLOwner acl_owner;
      set_dir_entry_owner(new_bde,
                          xattr_strategy()->object_owner(dobj->get_attrs(),
                                                         acl_owner),
                          acl_owner, db->default_object_owner(dpp, y));
    }
    bcache->add_entry(dpp, db->get_name(), new_bde);
  } else if (dobj->get_fsent()) {
    /* An unversioned destination needs its entry too.
     *
     * Only the versioned branch above touched the cache, so a copy
     * into an ordinary bucket added nothing:  the object was on disk,
     * complete, and answerable by HEAD, and invisible to
     * ListObjectsV2 for as long as that bucket's listing cache stayed
     * warm.  A listing that misses refills from the store and shows
     * it, which is why this went unnoticed -- every test that copies
     * into a fresh bucket and lists once passes.
     *
     * make_dir_entry() rather than another hand-built entry:  it is
     * what link_temp_file() uses on the PUT path and what fill_cache()
     * uses when a listing misses, so the fields come from the file
     * rather than from whatever the caller happens to be holding. */
    auto* bcache = driver->get_bucket_cache();
    if (bcache) {
      dobj->get_fsent()->stat(dpp, /*force=*/true);
      rgw_bucket_dir_entry bde{};
      int r = dobj->get_fsent()->make_dir_entry(
	  dpp, y, dobj->get_key(), nsfs::FSEnt::FLAG_NONE,
	  db->default_object_owner(dpp, y), bde);
      if (r < 0) {
	ldpp_dout(dpp, 0) << "ERROR: could not compose the listing entry for "
	  << dobj->get_key() << " after a copy: " << cpp_strerror(-r)
	  << ";  it will not list until the cache misses" << dendl;
      } else {
	bcache->add_entry(dpp, db->get_name(), bde);
      }
    }
  }

  /* the copy's mtime, which RGWCopyObj gives its notification as the
   * event's time */
  if (auto* ent = dobj->get_fsent(); mtime && ent &&
      (ent->stat(dpp, /*force=*/true) == 0)) {
    *mtime = from_statx_timestamp(ent->get_stx().stx_mtime);
  }

  return 0;
} /* NSFSObject::copy_object */

int NSFSObject::list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			    int max_parts, int marker, int* next_marker,
			    bool* truncated, list_parts_each_t&& each_func,
			    optional_yield y)
{
  *truncated = false;

  std::vector<uint64_t> part_sizes;
  if (!decode_raw_attr(state.attrset, RGW_NSFS_ATTR_MULTIPART_PART_SIZES, part_sizes)) {
    return 0;
  }

  int nparts = part_sizes.size();
  int start = marker;
  int emitted = 0;

  for (int i = start; i < nparts; ++i) {
    if (emitted >= max_parts) {
      *truncated = true;
      break;
    }
    Part part;
    part.part_number = i + 1;
    part.part_size = part_sizes[i];
    int ret = each_func(part);
    if (ret < 0) {
      return ret;
    }
    *next_marker = i + 1;
    ++emitted;
  }

  return 0;
} /* int NSFSObject::list_parts */

/* Directory::open() does openat(parent->get_fd(), ...), so a directory
 * whose ancestors were never opened yields EBADF.  ent->get_parent()
 * hands back exactly such a directory for a nested key, which is why
 * this resolves the chain from the bucket root instead -- the bucket's
 * own directory is always open.  For a key at the bucket root the walk
 * is empty and this returns the root, as before. */
int NSFSObject::resolve_parent_dir(const DoutPrefixProvider* dpp,
				   bool create_dirs,
				   std::vector<std::unique_ptr<nsfs::Directory>>& chain,
				   nsfs::Directory*& dir,
				   std::string& leaf)
{
  auto* root = static_cast<NSFSBucket*>(bucket)->get_dir();
  if (!root) {
    return -EINVAL;
  }

  /* resolve_path() opens each component with openat(parent fd), so the
   * root has to be open before the walk starts.  a key at the bucket
   * root hides this: the walk is empty, and the root then opens itself
   * against AT_FDCWD further down */
  if (root->get_fd() < 0) {
    int ret = root->open(dpp);
    if (ret < 0) {
      return ret;
    }
  }

  nsfs::Directory* leaf_dir{nullptr};
  int ret = nsfs::resolve_key_path(dpp, root, state.obj.key,
    get_fname(/*use_version=*/false),
    driver->get_reserved_names(),
    static_cast<NSFSBucket*>(bucket)->is_internal(),
    create_dirs,
    driver->ctx(),
    chain, leaf_dir, leaf,
    pending_identity, driver->get_access_strategy());
  if (ret < 0) {
    return ret;
  }
  if (!leaf_dir) {
    return -ENOENT;
  }
  dir = leaf_dir;
  return 0;
}

/* FSIO is an extension, so it is rejected on a bucket that does not carry
 * the marker.
 *
 * The shadow model puts a .shadow subtree and attributes into the bucket
 * which NooBaa has no reading of.  Serving it on an unmarked bucket would
 * add exactly the structure whose absence is that bucket's whole claim,
 * and would do it silently, on a bucket nobody has asked to convert.
 * Reject rather than degrade:  a caller handed a handle whose
 * semantics are missing is worse off than one told the bucket does not
 * offer them. */
int NSFSObject::fsio_allowed(const DoutPrefixProvider* dpp)
{
  auto* b = static_cast<NSFSBucket*>(get_bucket());
  if (! b) {
    return -EINVAL;
  }

  int ret = b->resolve_profile(dpp);
  if (ret < 0) {
    return ret;
  }
  /* FSIO needs somewhere to put a shadow, so the question is EXT_SHADOW
   * and not whether the bucket is marked.  Shared is marked, with
   * EXT_RGW_META and no shadow subtree. */
  if (! b->has_extension(nsfs::EXT_SHADOW)) {
    ldpp_dout(dpp, 4) << "FSIO rejected on bucket " << b->get_name()
      << " (profile " << b->profile_name() << "):  it does not carry "
      << "EXT_SHADOW, so it must not acquire a shadow tree" << dendl;
    return -ENOTSUP;
  }
  return 0;
}

int NSFSObject::stat_fsio_view(const DoutPrefixProvider* dpp,
			       struct stat* st, Attrs* attrs, uint32_t flags)
{
  int aret = fsio_allowed(dpp);
  if (aret < 0) {
    return aret;
  }

  if (!ent) {
    (void) stat(dpp);
  }

  nsfs::Directory* dir{nullptr};
  std::string leaf;
  std::vector<std::unique_ptr<nsfs::Directory>> resolved_dirs;

  /* a probe must not create anything on the way to a result */
  int rret = resolve_parent_dir(dpp, /*create_dirs=*/false,
			        resolved_dirs, dir, leaf);
  if (rret < 0) {
    return rret;
  }

  int parent_fd = dir->get_fd();
  if (parent_fd < 0) {
    dir->open(dpp);
    parent_fd = dir->get_fd();
  }
  if (parent_fd < 0) {
    return -EBADF;
  }

  /* the shadow is the NFS view whenever one exists;  otherwise the
   * published object is.  note the shadow directory is only opened if
   * one is there, so the common case costs a single fstatat */
  int dir_fd = parent_fd;
  int sdir_fd = -1;

  if (::faccessat(parent_fd, HIDDEN_SHADOW_PATH.c_str(), F_OK, AT_EACCESS) == 0) {
    sdir_fd = ::openat(parent_fd, HIDDEN_SHADOW_PATH.c_str(),
		       O_RDONLY | O_NOFOLLOW | O_DIRECTORY);
    if ((sdir_fd >= 0) &&
	(::faccessat(sdir_fd, leaf.c_str(), F_OK, AT_EACCESS) == 0)) {
      dir_fd = sdir_fd;
    }
  }

  int ret = 0;

  if (st) {
    if (nsfs::fstatat(dir_fd, leaf.c_str(), st, AT_SYMLINK_NOFOLLOW) < 0) {
      ret = -errno;
      goto out;
    }
  }

  if (attrs) {
    /* A directory's attributes live on its .folder sentinel, not on the
     * directory inode -- get_fname() maps a key ending in '/' to it.
     * The caller resolves a name without the trailing form, so make the
     * hop here rather than making every caller know the convention.
     * A directory created outside RGW has no sentinel;  that is not an
     * error, it simply carries no attributes. */
    std::string attr_path{leaf};
    if (st && S_ISDIR(st->st_mode)) {
      attr_path += "/";
      attr_path += path_strategy()->folder_object_name();
    }

    /* xattrs need a descriptor;  it is transient, unlike the ones an
     * FSIO handle retains */
    int fd = ::openat(dir_fd, attr_path.c_str(), O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
      if ((errno == ENOENT) && (attr_path != leaf)) {
	goto out; /* no sentinel:  no attributes, not a failure */
      }
      ret = -errno;
      goto out;
    }
    ret = get_x_attrs(null_yield, dpp, fd, *attrs, attr_path, xattr_strategy());
    ::close(fd);
  } else if (!st) {
    /* existence only */
    if (::faccessat(dir_fd, leaf.c_str(), F_OK, AT_EACCESS) < 0) {
      ret = -errno;
    }
  }

out:
  if (sdir_fd >= 0) {
    ::close(sdir_fd);
  }
  return ret;
}


static std::atomic<uint64_t> fsio_build_seq{0};

/* Apply what a create asked for to a shadow which has no name yet.
 *
 * Order matters.  Size first, because a truncate moves mtime;  then the
 * xattrs, which move only ctime;  then the times last, so what the inode
 * ends up carrying is what the caller asked for.  That ordering is
 * load-bearing for NFS exclusive create -- ganesha leaves its verifier in
 * atime and mtime (set_common_verifier()) and compares tv_sec back exactly,
 * and RGWFileHandle::stat() returns mtime from the inode whenever a handle
 * is live, so the inode is what has to hold it. */
static int apply_create_spec(const nsfs::XattrStrategy* xs,
			     const DoutPrefixProvider* dpp,
			     NSFSDriver* driver, int fd,
			     const Object::FSIOCreateSpec* spec)
{
  /* checked before the null-spec shortcut, so the hook can fail an
   * ordinary create too -- the cleanup paths are what it exists to reach */
  if (unlikely(driver->fsio_attrs_fail_injected())) {
    return -EIO;
  }
  if (! spec) {
    return 0;
  }
  if (spec->size >= 0) {
    if (::ftruncate(fd, spec->size) < 0) {
      return -errno;
    }
  }
  if (spec->attrs) {
    for (auto& [key, bl] : *spec->attrs) {
      if (! bl.length()) {
	continue; /* empty value means "leave this attr alone" */
      }
      std::string xname = xs->disk_name(key);
      std::string xval = attr_on_disk(xs, key, bl);
      if (::fsetxattr(fd, xname.c_str(), xval.data(), xval.size(), 0) < 0) {
	return -errno;
      }
    }
  }
  if (spec->times) {
    if (::futimens(fd, spec->times) < 0) {
      return -errno;
    }
  }
  return 0;
}

Object::FSIOResult NSFSObject::get_fsio_handle(const DoutPrefixProvider* dpp,
						const rgw_owner& authenticated_user,
						uint32_t flags,
						const FSIOCreateSpec* spec)
{
  int aret = fsio_allowed(dpp);
  if (aret < 0) {
    return FSIOResult{aret, nullptr};
  }

  /* Who the handle is for (n plan D4a):  librgw's mount user, pinned
   * here and held for the handle's life.  Denied where it cannot be
   * applied, as the writers deny it. */
  nsfs::FSIdentity identity;
  {
    const int pret = pin_personality_for(dpp, driver, authenticated_user,
					 &identity, null_yield);
    if ((pret < 0) && (pret != -ENODATA)) {
      return FSIOResult{pret, nullptr};
    }
  }
  const nsfs::AccessStrategy* model = driver->get_access_strategy();
  /* the source read and the destination create are on the access-model
   * axis, as for a copy (copy_source_as_gateway()) */
  const bool as_gateway =
    identity.active() && model->permits(nsfs::AccessClass::data);
  set_access(identity, /*ro=*/false);

  /* Opening to write is the mutation.
   *
   * An FSIO handle is not a request:  it is opened once and written
   * through many times, and the writes are parts of one mutation
   * rather than separate ones.  So the slot is taken here and held
   * until the handle is destroyed, and a bucket drains as its write
   * handles are released.
   *
   * Taken before anything is resolved or created, so a rejection costs
   * nothing and leaves nothing behind.  A read handle takes none --
   * reads are served throughout a conversion, which is what the
   * fallback strategies are for. */
  std::optional<NSFSMutationGuard> gate;
  if (flags & FSIOObject::OPEN_FLAG_WRITE) {
    gate.emplace(dpp, driver, static_cast<NSFSBucket*>(get_bucket()));
    if (gate->result() < 0) {
      return FSIOResult{gate->result(), nullptr};
    }
  }

  if (!ent) {
    (void) stat(dpp);
  }

  nsfs::Directory* dir{nullptr};
  std::string leaf;
  std::vector<std::unique_ptr<nsfs::Directory>> resolved_dirs;

  /* resolve the parent from the key path so the shadow lands beside its
   * object for a hierarchical key (e.g. photos/vacation/pic.jpg) */
  int rret = resolve_parent_dir(dpp, /*create_dirs=*/true,
			        resolved_dirs, dir, leaf);
  if (rret < 0) {
    return FSIOResult{rret, nullptr};
  }
  int parent_fd = dir->get_fd();
  if (parent_fd < 0) {
    dir->open(dpp);
    parent_fd = dir->get_fd();
  }
  if (parent_fd < 0) {
    return FSIOResult{-EBADF, nullptr};
  }

  int sdir_fd = open_shadow_dir(dpp, parent_fd, identity, model);
  if (sdir_fd < 0) {
    return FSIOResult{sdir_fd, nullptr};
  }
  const int shadow_found = exists_as(dpp, identity, model, sdir_fd, leaf.c_str());
  if (shadow_found < 0) {
    ::close(sdir_fd);
    return FSIOResult{shadow_found, nullptr};
  }
  bool shadow_exists = (shadow_found == 1);
  bool ephemeral = (flags & FSIOObject::OPEN_FLAG_EPHEMERAL) != 0;
  bool for_write = (flags & FSIOObject::OPEN_FLAG_WRITE) != 0;

  auto hdl = std::unique_ptr<NSFSFSIOObject>(
    new NSFSFSIOObject(this, driver, ephemeral, std::move(gate)));
  hdl->parent_fd = parent_fd;
  hdl->leaf_name = leaf;
  hdl->dir_chain = std::move(resolved_dirs);
  hdl->identity = identity;
  hdl->authenticated_user = authenticated_user;

  /* sdir_fd stays this function's until a success path hands it over.
   *
   * It used to be assigned to hdl->shadow_dir_fd here, above every error
   * path -- and those paths close it and then return, destroying hdl, whose
   * destructor closes it a second time.  Worse, the destructor's cleanup
   * runs with the default Binding::SHADOW, so under OPEN_FLAG_EPHEMERAL it
   * also unlinks .shadow/<leaf> -- which on the rendezvous path is a shadow
   * this call did not create. */
  bool sdir_committed = false;
  auto sdir_guard = make_scope_guard([&sdir_fd, &sdir_committed] {
    if (! sdir_committed) {
      ::close(sdir_fd);
    }
  });
  auto commit_sdir = [&]() {
    hdl->shadow_dir_fd = sdir_fd;
    hdl->shadow_name = leaf;
    sdir_committed = true;
  };

  if (shadow_exists && (flags & FSIOObject::OPEN_FLAG_EXCL)) {
    return FSIOResult{-EEXIST, nullptr};
  }

  if (shadow_exists) {
    /* rendezvous: reuse existing shadow */
    const int fd = openat_as(dpp, identity, model, sdir_fd, leaf.c_str(),
			     O_RDWR);
    if (fd < 0) {
      return FSIOResult{fd, nullptr};
    }
    hdl->shadow_fd = fd;
    /* before anything which can fail:  this shadow is not this call's to
     * remove, and the destructor decides that from resumed_existing */
    hdl->resumed_existing = true;
    if (for_write && (flags & FSIOObject::OPEN_FLAG_TRUNC)) {
      /* truncate the shadow in place;  unlinking it would strand
       * every client already rendezvoused on it */
      if (::ftruncate(hdl->shadow_fd, 0) < 0) {
	return FSIOResult{-errno, nullptr};
      }
    }
    hdl->binding = FSIOObject::Binding::SHADOW;
    commit_sdir();
    return FSIOResult{0, std::move(hdl)};
  }

  /* Ask the resolved parent directly rather than going through ent,
   * which is null for a hierarchical key even when the object is on
   * disk.  reclone() already tests it this way. */
  const int src_found = exists_as(dpp, identity, model, parent_fd, leaf.c_str());
  if (src_found < 0) {
    return FSIOResult{src_found, nullptr};
  }
  bool src_exists = (src_found == 1);

  if (!src_exists && !(flags & FSIOObject::OPEN_FLAG_CREATE)) {
    ::close(sdir_fd);
    return FSIOResult{-ENOENT, nullptr};
  }

  if (src_exists && (flags & FSIOObject::OPEN_FLAG_EXCL)) {
    return FSIOResult{-EEXIST, nullptr};
  }

  if (! for_write && src_exists) {
    /* no shadow exists and the caller is a reader:  bind to the
     * published object.  readers never construct a shadow--if a
     * writer creates one, reclone() dup2()s it onto this fd and the
     * reader follows it */
    const int fd = openat_as(dpp, identity, model, parent_fd, leaf.c_str(),
			     O_RDONLY);
    if (fd < 0) {
      return FSIOResult{fd, nullptr};
    }
    hdl->shadow_fd = fd;
    hdl->binding = FSIOObject::Binding::PUBLISHED;
    commit_sdir();
    return FSIOResult{0, std::move(hdl)};
  }

  /* falling through with !for_write means OPEN_FLAG_CREATE on an
   * object which does not exist:  there is nothing to bind to, so
   * the shadow is the object's initial content */

  if (unlikely(driver->fork_race_injected()) && for_write) {
    /* inject-fork-race: stand in for another instance which forked first,
     * so the linkat below loses with EEXIST and joins its shadow */
    int rfd = ::openat(sdir_fd, leaf.c_str(),
		       O_RDWR | O_CREAT | O_EXCL, nsfs::file_create_mode());
    if (rfd >= 0) {
      static const char marker[] = "RACEWON";
      [[maybe_unused]] ssize_t nw =
	::write(rfd, marker, sizeof(marker) - 1);
      ::close(rfd);
    }
  }

  /* Build the shadow away from its name, then link it into place.
   *
   * The name must appear only when the view is complete.  Creating it at
   * .shadow/<leaf> first and finishing it afterwards leaves a window in
   * which another opener rendezvouses onto a bare inode -- and if the
   * finishing step then fails, an attribute-less shadow is left behind
   * which rejects every later exclusive create on that name, with nothing
   * to reap it.  linkat(2) fails with EEXIST rather than replacing, so it
   * supplies the same exclusivity O_CREAT|O_EXCL did, at the point where
   * the file is ready.  Same shape as POSIXStrategy::link_temp_file(). */
  int build_fd = -1;
  std::string tmp_name;  /* empty when the build used O_TMPFILE */

  if (src_exists && !(flags & FSIOObject::OPEN_FLAG_TRUNC)) {
    /* COW clone of the published object.  clone_file() works by name, so
     * this branch needs a named temp rather than O_TMPFILE. */
    tmp_name = std::string(nsfs::TMP_LINK_PREFIX) + std::to_string(::getpid()) +
      "_" + std::to_string(fsio_build_seq.fetch_add(1));
    int ret = driver->get_fs_strategy()->clone_file(
      dpp, parent_fd, leaf, sdir_fd, tmp_name, null_yield,
      driver->copy_hints_for(authenticated_user), true /* excl */,
      identity, as_gateway);
    if (ret < 0) {
      return FSIOResult{ret, nullptr};
    }
    build_fd = openat_as(dpp, identity, model, sdir_fd, tmp_name.c_str(),
			 O_RDWR);
    if (build_fd < 0) {
      int err = build_fd;
      (void) unlinkat_as(dpp, identity, model, sdir_fd, tmp_name.c_str());
      return FSIOResult{err, nullptr};
    }
    /* carry the source's attributes onto the clone */
    int src_fd = openat_as(dpp, identity, model, parent_fd, leaf.c_str(),
			   O_RDONLY);
    if (src_fd >= 0) {
      (void) nsfs::with_identity_or_raise(dpp, identity, model,
					  nsfs::AccessClass::data,
					  RAISED_KEYDIR_CAPS, [&]() -> int {
	return copy_xattrs_fd(dpp, src_fd, build_fd);
      }, null_yield);
      ::close(src_fd);
    }
  } else {
    /* New object.  O_TMPFILE keeps it nameless until the link, so a
     * failure or a crash leaves nothing at all behind -- the inode is
     * reclaimed when the descriptor closes. */
    bool raised = false;
    build_fd = nsfs::open_as_or_raise(dpp, identity, model,
				      nsfs::AccessClass::data,
				      RAISED_CREATE_CAPS, sdir_fd, ".",
				      O_TMPFILE | O_RDWR,
				      nsfs::file_create_mode(), &raised);
    if ((build_fd == -EOPNOTSUPP) || (build_fd == -EISDIR) ||
	(build_fd == -EINVAL)) {
      /* filesystem without O_TMPFILE:  fall back to a named temp */
      tmp_name = std::string(nsfs::TMP_LINK_PREFIX) + std::to_string(::getpid()) +
	"_" + std::to_string(fsio_build_seq.fetch_add(1));
      build_fd = nsfs::open_as_or_raise(dpp, identity, model,
					nsfs::AccessClass::data,
					RAISED_CREATE_CAPS, sdir_fd,
					tmp_name.c_str(),
					O_RDWR | O_CREAT | O_EXCL,
					nsfs::file_create_mode(), &raised);
    }
    if (build_fd < 0) {
      return FSIOResult{build_fd, nullptr};
    }
    if (raised) {
      /* the gateway made it:  the identity's, before anything else is
       * done to it as the identity */
      if (int cret = chown_raised_object(dpp, build_fd, identity); cret < 0) {
	::close(build_fd);
	return FSIOResult{cret, nullptr};
      }
    }
    if (int mret = nsfs::apply_create_mode(dpp, build_fd,
					   nsfs::file_create_mode(), null_yield,
					   identity);
	mret < 0) {
      ::close(build_fd);
      return FSIOResult{mret, nullptr};
    }
    /* store a default private ACL (owner with FULL_CONTROL) */
    RGWAccessControlPolicy policy;
    ACLOwner acl_owner;
    acl_owner.id = bucket->get_owner();
    policy.set_owner(acl_owner);
    policy.get_acl().create_default(acl_owner.id,
				    acl_owner.display_name);
    bufferlist acl_bl;
    policy.encode(acl_bl);
    std::string xname = xattr_strategy()->disk_name(RGW_ATTR_ACL);
    std::string xval = attr_on_disk(xattr_strategy(),
				    RGW_ATTR_ACL, acl_bl);
    if (int err = nsfs::with_identity_or_raise(dpp, identity, model,
					       nsfs::AccessClass::data,
					       RAISED_KEYDIR_CAPS,
					       [&]() -> int {
	  return (::fsetxattr(build_fd, xname.c_str(),
			      xval.data(), xval.size(), 0) < 0) ? -errno : 0;
	}, null_yield);
	err < 0) {
      ::close(build_fd);
      if ((! tmp_name.empty()) &&
	  (! driver->fsio_skip_cleanup_injected())) {
	(void) unlinkat_as(dpp, identity, model, sdir_fd, tmp_name.c_str());
      }
      return FSIOResult{err, nullptr};
    }
  }

  /* whatever the caller wants set at create, applied before the name
   * exists rather than by a second call afterwards */
  {
    int ret = nsfs::with_identity_or_raise(dpp, identity, model,
					   nsfs::AccessClass::data,
					   RAISED_KEYDIR_CAPS, [&]() -> int {
      return apply_create_spec(xattr_strategy(), dpp, driver, build_fd, spec);
    }, null_yield);
    if (ret < 0) {
      ::close(build_fd);
      if ((! tmp_name.empty()) &&
	  (! driver->fsio_skip_cleanup_injected())) {
	(void) unlinkat_as(dpp, identity, model, sdir_fd, tmp_name.c_str());
      }
      return FSIOResult{ret, nullptr};
    }
  }

  /* COMMIT:  the name appears, complete, or not at all */
  int link_rc = 0;
  if (unlikely(driver->fsio_link_fail_injected())) {
    link_rc = -EIO;
  } else {
    link_rc = nsfs::with_identity_or_raise(dpp, identity, model,
					   nsfs::AccessClass::data,
					   RAISED_KEYDIR_CAPS, [&]() -> int {
      if (tmp_name.empty()) {
	char proc_path[PATH_MAX];
	snprintf(proc_path, sizeof(proc_path), "/proc/self/fd/%d", build_fd);
	return (::linkat(AT_FDCWD, proc_path, sdir_fd, leaf.c_str(),
			 AT_SYMLINK_FOLLOW) < 0) ? -errno : 0;
      }
      return (::linkat(sdir_fd, tmp_name.c_str(), sdir_fd, leaf.c_str(),
		       0) < 0) ? -errno : 0;
    }, null_yield);
  }

  /* the temp has served its purpose on every outcome -- including an
   * injected failure, which short-circuits the link above */
  if ((! tmp_name.empty()) && (! driver->fsio_skip_cleanup_injected())) {
    (void) unlinkat_as(dpp, identity, model, sdir_fd, tmp_name.c_str());
  }

  if (link_rc == -EEXIST) {
    /* another instance forked the same shadow first.  Join the winner
     * rather than clobbering it;  this one is discarded unnamed. */
    ::close(build_fd);
    const int fd = openat_as(dpp, identity, model, sdir_fd, leaf.c_str(),
			     O_RDWR);
    if (fd < 0) {
      return FSIOResult{fd, nullptr};
    }
    hdl->shadow_fd = fd;
    hdl->resumed_existing = true;
    hdl->binding = FSIOObject::Binding::SHADOW;
    commit_sdir();
    return FSIOResult{0, std::move(hdl)};
  }

  if (link_rc < 0) {
    ::close(build_fd);
    return FSIOResult{link_rc, nullptr};
  }

  hdl->shadow_fd = build_fd;
  hdl->binding = FSIOObject::Binding::SHADOW;
  commit_sdir();
  return FSIOResult{0, std::move(hdl)};
} /* get_fsio_handle */

int NSFSObject::NSFSFSIOObject::preadv(const struct iovec* iov, int iovcnt,
				       uint64_t ofs, uint64_t* bytes_read,
				       uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  ssize_t ret = ::preadv(shadow_fd, iov, iovcnt, ofs);
  if (ret < 0) {
    return -errno;
  }
  if (bytes_read) {
    *bytes_read = ret;
  }
  return 0;
}

int NSFSObject::NSFSFSIOObject::pwritev(const struct iovec* iov, int iovcnt,
					uint64_t ofs, uint64_t* bytes_written,
					uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  ssize_t ret = ::pwritev(shadow_fd, iov, iovcnt, ofs);
  if (ret < 0) {
    return -errno;
  }
  if (bytes_written) {
    *bytes_written = ret;
  }
  return 0;
}

int NSFSObject::NSFSFSIOObject::commit(const DoutPrefixProvider* dpp, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  return ::fsync(shadow_fd) < 0 ? -errno : 0;
}

int NSFSObject::NSFSFSIOObject::publish(const DoutPrefixProvider* dpp, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  if (doomed) {
    return -ESTALE; /* object was unlinked;  never publish */
  }
  if (binding == Binding::SHADOW_PUBLISHED) {
    return 0;
  }
  if (binding != Binding::SHADOW) {
    return -EINVAL; /* no shadow to publish */
  }

  ::fsync(shadow_fd);

  /* fixup pass: store the etag as an xattr.
   *
   * no trailing NUL, in either branch:  RGW_ATTR_ETAG is stored as bare
   * hex, the way the S3 PUT path stores it (rgw_op.cc), and dump_etag()
   * emits the attribute verbatim.  A stored NUL reaches the client inside
   * the quoted ETag header and makes If-Match compare unequal, since
   * rgw_string_unquote() yields 32 bytes and the attribute is 33. */
  {
    bufferlist etag_bl;
    struct statx estx;

    /* rgw_non_md5_etag reaches this path too.  RGWPutObj::execute()'s
     * skip covers the S3 writers, but an NFS write never passes through
     * it, and hashing here is the worst case the option exists to avoid:
     * a full extra pass over every byte just written, pread back through
     * a 64 KiB buffer purely to digest it.
     *
     * Use the same mtime-ino string nsfs gives version ids.  Setting an
     * xattr updates ctime, not mtime, and the renameat() below moves the
     * name rather than the file -- so this is the value the version-id
     * write further down computes as well, and ETag matches versionId
     * exactly as it does on the S3 path. */
    if (dpp->get_cct()->_conf->rgw_non_md5_etag &&
	nsfs::statx(shadow_fd, "", AT_EMPTY_PATH, STATX_ALL, &estx) == 0) {
      std::string opaque = nsfs_version_id_from_statx(estx);
      etag_bl.append(opaque.c_str(), opaque.size());
    } else {
      MD5 hash;
      hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
      unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
      char buf[65536];
      off_t off = 0;

      for (;;) {
	ssize_t nr = ::pread(shadow_fd, buf, sizeof(buf), off);
	if (nr <= 0) {
	  break;
	}
	hash.Update((const unsigned char*)buf, nr);
	off += nr;
      }
      hash.Final(m);

      append_bl(etag_bl, CEPH_CRYPTO_MD5_DIGESTSIZE * 2, [&](auto iter) {
	return buf_to_hex(m, iter);
      });
    }
    fsetattr(dpp, RGW_ATTR_ETAG, etag_bl, 0);
  }

  auto* bucket = static_cast<NSFSBucket*>(src_obj->get_bucket());
  const auto& binfo = bucket->get_info();

  /* scoped at function level:  the result has to outlive the lock, because
   * the listing cache below must flip the demoted entry from current to
   * non-current.  Declared inside the block it was discarded, so nothing
   * could -- and two NFS writes left two entries flagged current. */
  DemoteResult demote;
  if (binfo.versioned() && parent_fd >= 0) {
    auto vlock = version_lock_recovering(dpp, driver,
					src_obj->xattr_strategy(),
					bucket->get_name(), parent_fd,
					identity);

    /* as the handle's identity, which links the current object as an
     * S3 overwrite's demote does (n plan D4a) */
    demote_current_version(dpp, driver->get_fs_strategy(),
			   src_obj->xattr_strategy(),
			   parent_fd, leaf_name,
			   binfo.versioning_enabled(), demote, identity,
			   driver->get_access_strategy(),
			   null_yield,
			   src_obj->xattr_fallback());
  }
  {

  int ret = renameat_as(dpp, identity, driver->get_access_strategy(),
			shadow_dir_fd, shadow_name.c_str(),
			parent_fd, leaf_name.c_str());
  if (ret < 0) {
    return ret;
  }

  /* Stamp the version id, as the S3 write path does in link_temp_file().
   *
   * demote_current_version() decides what a later write is demoting by
   * reading this xattr, and is_null_version_fd() treats its absence as
   * the null version.  Without it every object written over NFS was the
   * null version:  a second write demoted the first to <leaf>_null, a
   * third collided on that same slot, and the versions collapsed onto
   * one entry instead of accumulating.
   *
   * shadow_fd still refers to this inode -- the rename moved the name,
   * not the file -- so writing through it reaches the published
   * object. */
  std::string ver_id;
  if (binfo.versioned()) {
    ver_id = NULL_VERSION_ID;
    struct statx vstx;
    if (binfo.versioning_enabled() &&
	nsfs::statx(shadow_fd, "", AT_EMPTY_PATH, STATX_ALL, &vstx) == 0) {
      ver_id = nsfs_version_id_from_statx(vstx);
    }
    std::string vid_x = src_obj->xattr_strategy()->disk_name(RGW_NSFS_ATTR_VERSION_ID);
    (void) nsfs::with_identity_or_raise(dpp, identity,
					driver->get_access_strategy(),
					nsfs::AccessClass::data,
					RAISED_KEYDIR_CAPS, [&]() -> int {
      return (::fsetxattr(shadow_fd, vid_x.c_str(), ver_id.c_str(),
			  ver_id.size(), 0) < 0) ? -errno : 0;
    }, null_yield);
    src_obj->set_instance(ver_id);
  }

  /* update bucket listing cache */
  auto* bcache = driver->get_bucket_cache();
  if (bcache) {
    struct statx pub_stx;
    if (nsfs::statx(parent_fd, leaf_name.c_str(),
	      AT_SYMLINK_NOFOLLOW, STATX_ALL, &pub_stx) == 0) {
      std::string obj_name =
	src_obj->get_key().get_index_key_name();
      rgw_bucket_dir_entry bde{};
      bde.key.name = obj_name;
      bde.ver.pool = 1;
      bde.ver.epoch = 1;
      bde.exists = true;
      if (binfo.versioned()) {
	/* the id written above:  without it every publish writes the
	 * same cache key and a new version replaces its predecessor's
	 * row rather than taking a place beside it */
	bde.key.instance = ver_id;
	bde.flags = rgw_bucket_dir_entry::FLAG_VER |
		    rgw_bucket_dir_entry::FLAG_CURRENT;
      }
      bde.meta.category = RGWObjCategory::Main;
      bde.meta.size = pub_stx.stx_size;
      bde.meta.accounted_size = pub_stx.stx_size;
      bde.meta.mtime = from_statx_timestamp(pub_stx.stx_mtime);
      bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
      {
	Attrs shadow_attrs;
	if (fgetattrs(dpp, shadow_attrs, 0) == 0) {
	  ACLOwner acl_owner;
	  set_dir_entry_owner(bde,
			      src_obj->xattr_strategy()->object_owner(
				  shadow_attrs, acl_owner),
			      acl_owner,
			      bucket->default_object_owner(dpp, null_yield));
	  /* the digest written above, which is what HEAD returns;  the
	   * synthesized change token is for objects that carry no etag,
	   * i.e. sideloaded files, and reporting it here disagreed with
	   * HEAD for every object published from NFS */
	  bufferlist etag_bl;
	  if (rgw::sal::get_attr(shadow_attrs, RGW_ATTR_ETAG, etag_bl)) {
	    bde.meta.etag = etag_bl.to_str();
	  }
	}
	if (bde.meta.etag.empty()) {
	  bde.meta.etag = synthesize_etag(pub_stx);
	}
      }
      bcache->add_entry(dpp, bucket->get_name(), bde);

      /* and flip the version this publish demoted from current to
       * non-current, as NSFSAtomicWriter::complete() does for an S3 PUT.
       * Without it the previous current entry stays in the cache still
       * flagged current, so the object lists with two.  It was invisible for
       * a while because the first listing of a bucket fills from the store,
       * which derives the flags correctly and repairs it -- so only a reader
       * that got there after the cache was already filled ever saw it. */
      if (demote.did_demote) {
	std::string obj_name = src_obj->get_key().get_index_key_name();

	cls_rgw_obj_key old_key;
	old_key.name = obj_name;
	old_key.instance = demote.demoted_ver_id;
	bcache->remove_entry(dpp, bucket->get_name(), old_key);

	rgw_bucket_dir_entry dem_bde{};
	fill_demoted_entry(obj_name, demote,
			   bucket->default_object_owner(dpp, null_yield), dem_bde);
	bcache->add_entry(dpp, bucket->get_name(), dem_bde);
      }
    }
  }

  }
  binding = Binding::SHADOW_PUBLISHED;
  return 0;
}

int NSFSObject::NSFSFSIOObject::reclone(const DoutPrefixProvider* dpp, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EINVAL;
  }
  if (binding == Binding::SHADOW) {
    return 0; /* already writable */
  }
  if (unlikely(driver->skip_reclone_injected())) {
    return 0; /* inject-skip-reclone */
  }

  const nsfs::AccessStrategy* model = driver->get_access_strategy();
  const bool as_gateway =
    identity.active() && model->permits(nsfs::AccessClass::data);

  if (shadow_dir_fd < 0) {
    shadow_dir_fd = open_shadow_dir(dpp, parent_fd, identity, model);
    if (shadow_dir_fd < 0) {
      return shadow_dir_fd;
    }
  }

  int new_fd{-1};
  bool joined{false};
  const int src_found = exists_as(dpp, identity,
				  driver->get_access_strategy(),
				  parent_fd, leaf_name.c_str());
  if (src_found < 0) {
    return src_found;
  }
  bool src_exists = (src_found == 1);

  if (src_exists) {
    int ret = driver->get_fs_strategy()->clone_file(
      dpp, parent_fd, leaf_name, shadow_dir_fd, leaf_name, null_yield,
      driver->copy_hints_for(authenticated_user), true /* excl */, identity,
      as_gateway);
    if (ret == -EEXIST) {
      joined = true;
    } else if (ret < 0) {
      return ret;
    } else {
      new_fd = openat_as(dpp, identity, model, shadow_dir_fd,
			 leaf_name.c_str(), O_RDWR);
    }
  } else {
    bool raised = false;
    new_fd = nsfs::open_as_or_raise(dpp, identity, model,
				    nsfs::AccessClass::data,
				    RAISED_CREATE_CAPS, shadow_dir_fd,
				    leaf_name.c_str(),
				    O_RDWR | O_CREAT | O_EXCL,
				    nsfs::file_create_mode(), &raised);
    if (new_fd == -EEXIST) {
      joined = true;
    } else if (new_fd >= 0) {
      int mret = raised ? chown_raised_object(dpp, new_fd, identity) : 0;
      if (mret == 0) {
	mret = nsfs::apply_create_mode(dpp, new_fd, nsfs::file_create_mode(),
				       null_yield, identity);
      }
      if (mret < 0) {
	::close(new_fd);
	return mret;
      }
    }
  }

  if (joined) {
    /* another instance re-forked first;  adopt its shadow */
    new_fd = openat_as(dpp, identity, model, shadow_dir_fd,
		       leaf_name.c_str(), O_RDWR);
  }
  if (new_fd < 0) {
    return new_fd;
  }

  if (src_exists && ! joined) {
    /* copy xattrs from published object to new shadow */
    int src_fd = openat_as(dpp, identity, model, parent_fd,
			   leaf_name.c_str(), O_RDONLY);
    if (src_fd >= 0) {
      (void) nsfs::with_identity_or_raise(dpp, identity, model,
					  nsfs::AccessClass::data,
					  RAISED_KEYDIR_CAPS, [&]() -> int {
	return copy_xattrs_fd(dpp, src_fd, new_fd);
      }, null_yield);
      ::close(src_fd);
    }
  }

  /* atomically swap the fd so readers transition seamlessly */
  if (::dup2(new_fd, shadow_fd) < 0) {
    int err = -errno;
    ::close(new_fd);
    (void) unlinkat_as(dpp, identity, model, shadow_dir_fd,
		       shadow_name.c_str());
    return err;
  }
  ::close(new_fd);

  binding = Binding::SHADOW;
  return 0;
}

int NSFSObject::NSFSFSIOObject::fstat(struct stat* st, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  if (::fstat(shadow_fd, st) < 0) {
    return -errno;
  }
  return 0;
}

int NSFSObject::NSFSFSIOObject::fgetattr(const DoutPrefixProvider* dpp, const std::string& name,
					  bufferlist& dest, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  std::string xname = src_obj->xattr_strategy()->disk_name(name);
  return nsfs::with_identity_or_raise(dpp, identity,
				      driver->get_access_strategy(),
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS, [&]() -> int {
    ssize_t len = ::fgetxattr(shadow_fd, xname.c_str(), nullptr, 0);
    if (len < 0) {
      return -errno;
    }
    if (len == 0) {
      dest.clear();
      return 0;
    }
    bufferptr bp(len);
    len = ::fgetxattr(shadow_fd, xname.c_str(), bp.c_str(), len);
    if (len < 0) {
      return -errno;
    }
    bp.set_length(len);
    dest.push_back(std::move(bp));
    return 0;
  }, null_yield);
}

int NSFSObject::NSFSFSIOObject::fsetattr(const DoutPrefixProvider* dpp, const std::string& name,
					  const bufferlist& val,
					  uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  if (!val.length()) {
    /* an empty value means "leave this attr alone", as in
     * RGWRados::set_attrs() and FSEnt::write_attrs()'s MERGE branch.
     * Writing it would replace a good value with one that does not
     * decode -- which is how an empty ACL became EIO. */
    return 0;
  }
  std::string xname = src_obj->xattr_strategy()->disk_name(name);
  std::string sval = attr_on_disk(src_obj->xattr_strategy(), name, val);
  return nsfs::with_identity_or_raise(dpp, identity,
				      driver->get_access_strategy(),
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS, [&]() -> int {
    return (::fsetxattr(shadow_fd, xname.c_str(),
			sval.data(), sval.size(), 0) < 0) ? -errno : 0;
  }, null_yield);
}

int NSFSObject::NSFSFSIOObject::fgetattrs(const DoutPrefixProvider* dpp, Attrs& attrs, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  return nsfs::with_identity_or_raise(dpp, identity,
				      driver->get_access_strategy(),
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS, [&]() -> int {
    return get_x_attrs(null_yield, dpp, shadow_fd, attrs, shadow_name,
		       src_obj->xattr_strategy());
  }, null_yield);
}

int NSFSObject::NSFSFSIOObject::fsetattrs(const DoutPrefixProvider* dpp, Attrs& attrs, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  return nsfs::with_identity_or_raise(dpp, identity,
				      driver->get_access_strategy(),
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS, [&]() -> int {
    for (auto& [key, bl] : attrs) {
      if (!bl.length()) {
	continue; /* empty value means "leave this attr alone" */
      }
      std::string xname = src_obj->xattr_strategy()->disk_name(key);
      std::string xval = attr_on_disk(src_obj->xattr_strategy(), key, bl);
      if (::fsetxattr(shadow_fd, xname.c_str(),
		      xval.data(), xval.size(), 0) < 0) {
	return -errno;
      }
    }
    return 0;
  }, null_yield);
}

int NSFSObject::NSFSFSIOObject::fremovexattr(const DoutPrefixProvider* dpp, const std::string& name,
					     uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  std::string xname = src_obj->xattr_strategy()->disk_name(name);
  return nsfs::with_identity_or_raise(dpp, identity,
				      driver->get_access_strategy(),
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS, [&]() -> int {
    return (::fremovexattr(shadow_fd, xname.c_str()) < 0) ? -errno : 0;
  }, null_yield);
}

int NSFSObject::NSFSFSIOObject::ftruncate(const DoutPrefixProvider* dpp,
					  uint64_t size, uint32_t flags)
{
  if (shadow_fd < 0) {
    return -EBADF;
  }
  if (binding != Binding::SHADOW) {
    /* only a shadow is mutable--the published object is not */
    return -EPERM;
  }
  return (::ftruncate(shadow_fd, size) < 0) ? -errno : 0;
}

int NSFSObject::NSFSFSIOObject::discard(const DoutPrefixProvider* dpp,
					uint32_t flags)
{
  if (doomed) {
    return 0;
  }

  /* posix unlink:  remove the shadow's name now, but leave the open
   * descriptors alone--readers and writers continue to operate on it
   * normally, and the filesystem reclaims the storage when the last
   * one is closed.  a subsequent open finds no shadow, and so creates
   * a new one rather than rendezvousing with this (doomed) view */
  if (binding != Binding::PUBLISHED &&
      shadow_dir_fd >= 0 && !shadow_name.empty()) {
    const int ret = unlinkat_as(dpp, identity,
				driver->get_access_strategy(),
				shadow_dir_fd, shadow_name.c_str());
    if ((ret < 0) && (ret != -ENOENT)) {
      return ret;
    }
  }

  doomed = true;
  return 0;
}

int NSFSObject::NSFSFSIOObject::close(const DoutPrefixProvider* dpp, uint32_t flags)
{
  if (shadow_fd < 0 && shadow_dir_fd < 0) {
    return 0;
  }

  if (!doomed && binding == Binding::SHADOW && ephemeral &&
      !resumed_existing && shadow_dir_fd >= 0 && !shadow_name.empty()) {
    (void) unlinkat_as(dpp, identity, driver->get_access_strategy(),
		       shadow_dir_fd, shadow_name.c_str());
  }

  if (shadow_fd >= 0) {
    ::close(shadow_fd);
    shadow_fd = -1;
  }

  if (shadow_dir_fd >= 0) {
    ::close(shadow_dir_fd);
    shadow_dir_fd = -1;
  }

  return 0;
}

NSFSObject::NSFSFSIOObject::~NSFSFSIOObject()
{
  close(driver->get_dpp(), CLOSE_FLAG_DETACH);
}

bool NSFSObject::is_sync_completed(const DoutPrefixProvider* dpp, optional_yield y,
                                    const ceph::real_time& obj_mtime)
{
  return false;
}

int NSFSObject::load_obj_state(const DoutPrefixProvider* dpp, optional_yield y, bool follow_olh)
{
  int ret = stat(dpp);
  if (ret < 0) {
    if (state.is_dm && !dm_version_id.empty()) {
      state.obj.key.instance = dm_version_id;
    }
    return ret;
  }

  ret = get_obj_attrs(y, dpp);

  return ret;
}

int NSFSObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y, uint32_t flags,
                            const rgw_owner& authenticated_user)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, static_cast<NSFSBucket*>(get_bucket()));
  if (mg.result() < 0) {
    return mg.result();
  }
  int ret = admit_write(dpp, y);
  if (ret < 0) {
    return ret;
  }
  std::vector<std::string> rmattrs;
  if (delattrs) {
    for (auto& it : *delattrs) {
      state.attrset.erase(it.first);
      rmattrs.push_back(it.first);
    }
  }
  if (setattrs) {
    for (auto& it : *setattrs) {
      state.attrset[it.first] = it.second;
    }
  }

  ret = pin_identity_for_attrs(dpp, authenticated_user, y);
  if (ret < 0) {
    return ret;
  }
  return with_cross_account_retry(dpp, nsfs::AccessClass::object_attrs,
				  [&](int use_fd) -> int {
    return write_attrs(dpp, y, &rmattrs, use_fd);
  });
}

int NSFSObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp)
{
  /* An attribute read, so it is on the access-model axis:  where the
   * filesystem denies an identity S3 authorised, the gateway reads
   * the attributes if the deployment has said so.  Only the
   * attributes -- this function never touches the object's bytes. */
  /* The open *and* the read, inside one retry.
   *
   * Scoping it to the open alone does not work, and the way it fails
   * is quiet:  the open is retried as the gateway and succeeds, the
   * entry's identity is restored on the way out, and the attribute
   * read that follows runs as the authenticated user and is denied again, so
   * the read fails although S3 authorised it.  Both halves are the
   * operation. */
  /* Both failures reach the caller, and has_attrs is false on either.
   *
   * An object that could not be opened is a permission the tree does
   * not grant, and reaches the client as a 403.  An object that opened
   * and whose attributes could not be read is not an object with no
   * attributes:  the core takes a 0 here to mean the attributes were
   * read, and decides object lock on them -- for a delete and for a
   * retention change -- so a failed read taken for an empty one
   * deleted a version under a legal hold.  Measured 2026-10-21 with
   * inject-attr-read-fail.
   *
   * And the open failure kept apart from a missing etag:  collapsed
   * into a 0, the caller saw an object with no etag and returned 400,
   * "could not open ... Permission denied" followed by "has no etag
   * after get_obj_attrs()".  Thirteen acl tests, measured 2026-10-16. */
  int ret = with_cross_account_retry(dpp, nsfs::AccessClass::object_attrs,
				     [&](int use_fd) -> int {
    if (use_fd < 0) {
      const int oret = open(dpp, y, false);
      if (oret < 0) {
	return oret;
      }
    }
    /* EIO, not a permission:  the same failure on every access model,
     * without the retry a denial sets off */
    if (get_bucket() &&
	driver->attr_read_fail_injected(get_bucket()->get_name())) {
      driver->record_attr_read_fail();
      return -EIO;
    }
    std::vector<std::string> unknown;
    const int rret = ent->read_attrs(dpp, y, state.attrset, use_fd, &unknown);
    if ((rret == 0) && !unknown.empty()) {
      /* X4's third layer (DECISIONS.md 2026-10-26):  an attribute the
       * reviewed releases of NooBaa do not write may change what the
       * bytes mean or who may read them, so the object is not handed
       * out until the release is reviewed */
      ldpp_dout(dpp, 0) << "ERROR: " << get_name() << " carries "
	<< unknown.front() << (unknown.size() > 1 ? " and others" : "")
	<< ", which no reviewed release of NooBaa writes;  not read" << dendl;
      state.attrset.clear();
      return -ERR_INVALID_OBJECT_STATE;
    }
    return rret;
  });
  state.has_attrs = (ret == 0);

  if (state.has_attrs && state.attrset.find(RGW_ATTR_ETAG) == state.attrset.end()) {
    bufferlist bl;
    std::string etag = synthesize_etag(ent->get_stx());
    bl.append(etag);
    state.attrset[RGW_ATTR_ETAG] = std::move(bl);
  }

  if (state.has_attrs && state.attrset.find(RGW_ATTR_CONTENT_TYPE) == state.attrset.end()) {
    std::string name = ent->get_name();
    auto dot = name.rfind('.');
    if (dot != std::string::npos && dot < name.size() - 1) {
      auto mime = rgw_find_mime_by_ext(name.substr(dot + 1));
      if (!mime.empty()) {
        bufferlist bl;
        bl.append(mime.data(), mime.size());
        state.attrset[RGW_ATTR_CONTENT_TYPE] = std::move(bl);
      }
    }
  }

  return ret;
}

int NSFSObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y, const DoutPrefixProvider* dpp,
                               const rgw_owner& authenticated_user, uint32_t flags)
{
  /* held off while this bucket's tree is being rewritten, as the other
   * attribute writes are;  see NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, static_cast<NSFSBucket*>(get_bucket()));
  if (mg.result() < 0) {
    return mg.result();
  }
  int ret = admit_write(dpp, y);
  if (ret < 0) {
    return ret;
  }
  state.attrset[attr_name] = attr_val;
  {
    const int pret = pin_identity_for_attrs(dpp, authenticated_user, y);
    if (pret < 0) {
      return pret;
    }
  }
  return with_cross_account_retry(dpp, nsfs::AccessClass::object_attrs,
				  [&](int use_fd) -> int {
    return write_attrs(dpp, y, nullptr, use_fd);
  });
}

/* What a cross-account attribute operation raises:  DAC_OVERRIDE for
 * the file's mode, FOWNER for the ownership tests a `user.*` write
 * meets on somebody else's file. */
static constexpr uint64_t CROSS_ACCOUNT_CAPS =
  nsfs::cap_bit(CAP_DAC_OVERRIDE) | nsfs::cap_bit(CAP_FOWNER);

int NSFSObject::with_cross_account_retry(const DoutPrefixProvider* dpp,
					 nsfs::AccessClass what,
					 std::function<int(int use_fd)> fn)
{
  /* -1:  the operation on its own terms, as the entry's identity */
  int ret = fn(-1);
  if (ret != -EACCES) {
    return ret;
  }
  if (!driver->get_access_strategy()->permits(what)) {
    return ret;
  }
  if (!ent) {
    return ret;
  }

  /* The filesystem denied an identity S3 authorised, and this
   * deployment says S3 decides for this class of operation.  Perform
   * it on a descriptor of our own, opened as the gateway under a
   * raise, and close it before returning.
   *
   * Nothing is stored on the object.  Earlier versions cleared the
   * entry's identity, and then the object's, and both were wrong in
   * the same way:  the entry caches its descriptor, so the one the
   * gateway opened outlived the operation and a data read found it
   * already open -- alt could GET another identity's bytes, measured
   * 2026-10-16 against `acl::check_alt_read`.  A descriptor the
   * caller owns cannot be found by anything it is not handed to. */
  int fd = -1;
  const int rret = nsfs::with_capabilities(dpp, CROSS_ACCOUNT_CAPS,
					   [&]() -> int {
    fd = ent->open_scratch_as_gateway(dpp, /*read_only=*/false);
    if (fd < 0) {
      return fd;
    }
    return fn(fd);
  });
  if (fd >= 0) {
    ::close(fd);
  }

  if (rret < 0) {
    ldpp_dout(dpp, 4) << "nsfs: cross-account attribute operation on "
      << get_name() << " failed even raised: " << cpp_strerror(-rret)
      << dendl;
    return rret;
  }
  if (what == nsfs::AccessClass::data) {
    driver->cross_account_data_served.fetch_add(1, std::memory_order_relaxed);
  } else {
    driver->cross_account_attrs_served.fetch_add(1, std::memory_order_relaxed);
  }
  ldpp_dout(dpp, 10) << "nsfs: " << get_name()
    << ": the filesystem denied the authenticated user and S3 authorised it,"
    " so the gateway performed it (access model "
    << driver->get_access_strategy()->name() << ")" << dendl;
  return rret;
}

int NSFSObject::pin_identity_for_attrs(const DoutPrefixProvider* dpp,
				       const rgw_owner& who, optional_yield y)
{
  if (nsfs::owner_empty(who)) {
    return 0;				/* nobody asked;  see the header */
  }
  nsfs::FSIdentity id;
  const int pret = pin_personality_for(dpp, driver, who, &id, y);
  if ((pret < 0) && (pret != -ENODATA)) {
    return pret;
  }
  if (id.active()) {
    /* on the object, not the entry:  the entry may not exist yet, and
     * it re-reads this on every open anyway.  read_only false -- an
     * attribute write needs O_RDWR. */
    set_access(id, /*ro=*/false);
  }
  /* what this attribute write chose, for the `ran-as` hint */
  driver->note_ran_as("attrs", to_string(who), id);
  return 0;
}

int NSFSObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
                               optional_yield y, const rgw_owner& authenticated_user)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, static_cast<NSFSBucket*>(get_bucket()));
  if (mg.result() < 0) {
    return mg.result();
  }
  int ret = admit_write(dpp, y);
  if (ret < 0) {
    return ret;
  }
  state.attrset.erase(attr_name);

  /* No open here.  One outside the retry fails as the authenticated user and
   * returns before the cross-account path is reached -- which it did,
   * leaving DeleteObjectTagging a 403 while the same deployment let a
   * put through.  The retry's own attempt opens, both times. */
  ret = pin_identity_for_attrs(dpp, authenticated_user, y);
  if (ret < 0) {
    return ret;
  }

  /* with the identity:  fremovexattr of a `user.*` name needs write
   * permission on the file, tested against the calling thread, so as
   * the gateway this fails on an identity's 0660 object --
   * "could not remove attribute user.nsfs.rgw.x-amz-tagging ...
   * Permission denied", a 403 from DeleteObjectTagging.  The
   * parameter has been here all along and no caller passed one. */
  ret = with_cross_account_retry(dpp, nsfs::AccessClass::object_attrs,
				 [&](int use_fd) -> int {
    if (use_fd < 0) {
      const int oret = open(dpp, y);
      if (oret < 0) {
	return oret;
      }
    }
    /* the caller's descriptor carries no identity;  the entry's
     * carries the authenticated user's */
    return remove_x_attr(dpp, y,
			 (use_fd >= 0) ? use_fd : ent->get_fd(),
			 attr_name, get_name(),
			 xattr_strategy(), xattr_fallback(),
			 driver->get_fs_strategy(),
			 (use_fd >= 0) ? nsfs::FSIdentity::none()
				       : ent->get_identity());
  });
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not remover attribute " << attr_name << " for " << get_name() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

bool NSFSObject::is_expired()
{
  utime_t delete_at;
  if (!decode_attr(state.attrset, RGW_ATTR_DELETE_AT, delete_at)) {
    ldout(driver->ctx(), 0)
        << "ERROR: " << __func__
        << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
    return false;
  }

  if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
    return true;
  }

  return false;
}

void NSFSObject::gen_rand_obj_instance_name()
{
  state.obj.key.set_instance(gen_rand_instance_name());
}

std::unique_ptr<MPSerializer> NSFSObject::get_serializer(const DoutPrefixProvider *dpp, optional_yield y, const std::string& lock_name)
{
  return std::make_unique<MPNSFSSerializer>(dpp, driver, this, lock_name);
}

int MPNSFSSerializer::try_lock(const DoutPrefixProvider *dpp, ceph::timespan dur, optional_yield y)
{
  if (!obj->check_exists(dpp)) {
    return -ENOENT;
  }

  NSFSBucket* b = static_cast<NSFSBucket*>(obj->get_bucket());
  if (b->get_dir()->get_type() == ObjectType::MULTIPART && b->get_dir_fd(dpp) > 0) {
    locked = true;
    return 0;
  }

  return -ENOENT;
}

int MPNSFSSerializer::unlock(const DoutPrefixProvider *dpp, optional_yield y)
{
  clear_locked();
  return 0;
}

int NSFSObject::transition(Bucket* bucket,
			    const rgw_placement_rule& placement_rule,
			    const real_time& mtime,
			    uint64_t olh_epoch,
			    const DoutPrefixProvider* dpp,
			    optional_yield y,
                            uint32_t flags)
{
  return -ERR_NOT_IMPLEMENTED;
}

int NSFSObject::transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

int NSFSObject::restore_obj_from_cloud(Bucket* bucket,
          rgw::sal::PlacementTier* tier,
	  CephContext* cct,
          std::optional<uint64_t> days,
          bool& in_progress,
	  uint64_t& size,
          const DoutPrefixProvider* dpp,
          optional_yield y)
{
  return -ERR_NOT_IMPLEMENTED;
}

bool NSFSObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  return (r1 == r2);
}

int NSFSObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
{
    return 0;
}

int NSFSObject::swift_versioning_restore(const ACLOwner& owner, const rgw_user& remote_user, bool& restored,
				       const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int NSFSObject::swift_versioning_copy(const ACLOwner& owner, const rgw_user& remote_user,
				    const DoutPrefixProvider* dpp, optional_yield y)
{
  return 0;
}

int NSFSObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
					  const std::set<std::string>& keys,
					  Attrs* vals)
{
  /* TODO Figure out omap */
  return 0;
}

int NSFSObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  /* TODO Figure out omap */
  return 0;
}

/* The recorded owner, rewritten as RadosObject::chown() rewrites it;
 * nothing on disk changes (DECISIONS.md 2026-10-11).  This used to
 * fchown the file to uid 0.  An object that records no owner is
 * already its bucket's owner's, so there is nothing to write. */
int NSFSObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret = get_obj_attrs(y, dpp);
  if (ret < 0) {
    return ret;
  }
  auto i = get_attrs().find(RGW_ATTR_ACL);
  if (i == get_attrs().end()) {
    return 0;
  }

  RGWAccessControlPolicy policy;
  try {
    auto p = i->second.cbegin();
    policy.decode(p);
  } catch (const buffer::error&) {
    ldpp_dout(dpp, 0) << "ERROR: chown of " << get_name()
      << ":  its ACL will not decode" << dendl;
    return -EIO;
  }
  RGWAccessControlList& acl = policy.get_acl();
  acl.remove_canon_user_grant(policy.get_owner().id);
  ACLGrant grant;
  grant.set_canon(new_user.get_id(), new_user.get_display_name(),
		  RGW_PERM_FULL_CONTROL);
  acl.add_grant(grant);
  policy.set_owner(ACLOwner{new_user.get_id(), new_user.get_display_name()});

  Attrs setattrs;
  encode(policy, setattrs[RGW_ATTR_ACL]);
  /* the chown is the operator's, and no request authenticated it */
  return set_obj_attrs(dpp, &setattrs, nullptr, y, rgw::sal::FLAG_LOG_OP,
		       rgw::sal::no_authenticated_user);
}

int NSFSObject::get_cur_version(const DoutPrefixProvider* dpp, rgw_obj_key& key)
{
  return 0;
}

int NSFSObject::stat(const DoutPrefixProvider* dpp)
{
  int ret;

  if (!ent) {
    dir_chain.clear();
    state.is_dm = false;
    dm_version_id.clear();
    names_directory = false;
    Directory* leaf_dir;
    std::string leaf_name;
    ret = resolve_key_path(dpp,
        static_cast<NSFSBucket *>(bucket)->get_dir(),
        state.obj.key,
        get_fname(/*use_version=*/false),
        driver->get_reserved_names(),
        static_cast<NSFSBucket *>(bucket)->is_internal(),
        /*create_dirs=*/false,
        driver->ctx(),
        dir_chain, leaf_dir, leaf_name,
        pending_identity, driver->get_access_strategy());
    if (ret < 0) {
      state.exists = false;
      return ret;
    }

    const std::string& req_ver = state.obj.key.instance;
    NSFSBucket* b = static_cast<NSFSBucket*>(bucket);
    bool versioned = b && b->get_info().versioned();

    /* A directory object in NooBaa's format:  the directory is marked,
     * and may hold no sentinel at all.  Unversioned buckets only, for
     * now;  NooBaa keeps a versioned directory object's history
     * differently. */
    nsfs::PathStrategy::DirectoryObject dobj{};
    const std::string& kname = state.obj.key.name;
    if (!versioned && !kname.empty() && (kname.back() == '/') &&
	leaf_dir->directory_object(dpp, dobj)) {
      ent = std::make_unique<NooBaaDirectoryObject>(leaf_name, leaf_dir, dobj,
						    driver->ctx());
      apply_pending_access();
      ret = ent->stat(dpp);
      if (ret < 0) {
	state.exists = false;
	return ret;
      }
    } else if (versioned && !req_ver.empty()) {
      /* version-aware stat: check current first, fall back to .versions/ */
      ret = leaf_dir->get_ent(dpp, null_yield, leaf_name, req_ver, ent);
      apply_pending_access();
      if (ret == 0) {
        ret = ent->stat(dpp);
      }

      bool cur_matches = false;
      if (ret == 0 && ent && ent->exists()) {
        if (req_ver == NULL_VERSION_ID) {
          int chk_fd = ent->get_fd();
          if (chk_fd < 0) {
            ent->open(dpp);
            chk_fd = ent->get_fd();
          }
          cur_matches = (chk_fd >= 0 &&
			 is_null_version_fd(chk_fd, xattr_strategy(),
					    xattr_fallback()));
        } else {
          int chk_fd = ent->get_fd();
          if (chk_fd < 0) {
            ent->open(dpp);
            chk_fd = ent->get_fd();
          }
          cur_matches = (chk_fd >= 0) &&
			(recorded_version_id(chk_fd, xattr_strategy(),
					     xattr_fallback()) == req_ver);
        }
      }

      if (!cur_matches) {
        /* look in .versions/ */
        int parent_fd = leaf_dir->get_fd();
        if (parent_fd < 0) {
          leaf_dir->open(dpp);
          parent_fd = leaf_dir->get_fd();
        }
        if (parent_fd >= 0) {
          std::string ver_name = nsfs_ver_entry(leaf_name, req_ver);
          /* .versions/ as the walk opens a key's directories:  as the
           * authenticated user, and as the gateway after a denial only where
           * the model permits the path class */
          auto ver_dir = std::make_unique<Directory>(
            HIDDEN_VERSIONS_PATH, leaf_dir, driver->ctx());
          ver_dir->set_identity(pending_identity);
          ver_dir->set_access_model(driver->get_access_strategy());
          if (ver_dir->open(dpp) == 0) {
            ent = std::make_unique<File>(ver_name, ver_dir.get(), driver->ctx());
            dir_chain.push_back(std::move(ver_dir));
            apply_pending_access();

            ret = ent->stat(dpp);
            if (ret < 0) {
              state.exists = false;
              return ret;
            }
          } else {
            state.exists = false;
            return -ENOENT;
          }
        } else {
          state.exists = false;
          return -ENOENT;
        }
      }
    } else {
      /* non-versioned or no specific version requested */
      ret = leaf_dir->get_ent(dpp, null_yield, leaf_name,
          state.obj.key.instance, ent);
      apply_pending_access();
      if (ret < 0 && versioned) {
        int parent_fd = leaf_dir->get_fd();
        if (parent_fd < 0) {
          leaf_dir->open(dpp);
          parent_fd = leaf_dir->get_fd();
        }
        if (parent_fd >= 0) {
          /* as above:  .versions/ opened as the walk would */
          Directory vdir_ent(HIDDEN_VERSIONS_PATH, leaf_dir, driver->ctx());
          vdir_ent.set_identity(pending_identity);
          vdir_ent.set_access_model(driver->get_access_strategy());
          const int vfd = (vdir_ent.open(dpp) == 0)
            ? ::dup(vdir_ent.get_fd()) : -1;
          if (vfd >= 0) {
            /* find the newest version entry for this key */
            DIR* vdir = fdopendir(dup(vfd));
            if (vdir) {
              uint64_t max_mtime = 0;
              std::string max_name;
              struct dirent* de;
              while ((de = readdir(vdir)) != nullptr) {
                /* no dot filter:  the key match below rejects ".", ".."
                 * and .lock, and filtering on the dot would hide the
                 * versions of an object whose name begins with one */
                std::string vname(de->d_name);
                /* match entries for this key: "leafname_..." */
                if (vname.size() <= leaf_name.size() + 1 ||
                    vname.compare(0, leaf_name.size(), leaf_name) != 0 ||
                    vname[leaf_name.size()] != '_') {
                  continue;
                }
                std::string vid = vname.substr(leaf_name.size() + 1);
                nsfs_version_info vinfo;
                if (!nsfs_parse_version_id(vid, vinfo)) {
                  continue;
                }
                uint64_t entry_mtime;
                if (vid == NULL_VERSION_ID) {
                  struct statx nstx;
                  if (nsfs::statx(vfd, de->d_name, AT_SYMLINK_NOFOLLOW,
                            STATX_MTIME, &nstx) < 0) {
                    continue;
                  }
                  entry_mtime = statx_mtime_ns(nstx);
                } else {
                  entry_mtime = vinfo.mtime_ns;
                }
                if (entry_mtime > max_mtime || max_name.empty()) {
                  max_mtime = entry_mtime;
                  max_name = vname;
                }
              }
              closedir(vdir);

              if (!max_name.empty()) {
                /* check if it's a delete marker */
                int dm_fd = openat_as(dpp, pending_identity,
                                      driver->get_access_strategy(), vfd,
                                      max_name.c_str(), O_RDONLY);
                if (dm_fd >= 0) {
                  char buf[8];
                  std::string dm_xattr =
                    xattr_strategy()->disk_name(RGW_NSFS_ATTR_DELETE_MARKER);
                  ssize_t xlen = nsfs::fgetxattr(
                    dm_fd, dm_xattr.c_str(), buf, sizeof(buf));
                  if (xlen > 0) {
                    state.is_dm = true;
                    state.exists = false;
                    dm_version_id =
                      max_name.substr(leaf_name.size() + 1);
                    struct statx dm_stx;
                    if (nsfs::statx(dm_fd, "", AT_EMPTY_PATH,
                              STATX_MTIME, &dm_stx) == 0) {
                      state.mtime =
                        from_statx_timestamp(dm_stx.stx_mtime);
                    }
                  }
                  ::close(dm_fd);
                }
              }
            }
            ::close(vfd);

            if (state.is_dm) {
              state.accounted_size = state.size = 0;
              return -ENOENT;
            }
          }
        }
        state.exists = false;
        return ret;
      } else if (ret < 0) {
        state.exists = false;
        return ret;
      }

      ret = ent->stat(dpp);
      if (ret < 0) {
        state.exists = false;
        return ret;
      }
    }

    /* an object is a file, so a key without the trailing '/' (which
     * maps a key to its sentinel) that names a directory has no
     * object:  404, as NooBaa's read_object_md() returns */
    if (ent && (ent->get_type() != ObjectType::FILE)) {
      names_directory = (ent->get_type() == ObjectType::DIRECTORY);
      ent.reset();
      state.exists = false;
      return -ENOENT;
    }
  } else {
    ret = ent->stat(dpp);
    if (ret < 0) {
      state.exists = false;
      return ret;
    }
  }

  if (state.obj.key.instance.empty()) {
    state.obj.key.instance = ent ? ent->get_cur_version() : "";
  }

  state.exists = ent ? ent->exists() : false;
  if (!state.exists) {
    return 0;
  }

  state.accounted_size = state.size = ent->get_stx().stx_size;
  state.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);

  /* check for delete marker xattr */
  NSFSBucket* bk = static_cast<NSFSBucket*>(bucket);
  if (bk && bk->get_info().versioned() && ent->get_parent()) {
    int pfd = ent->get_parent()->get_fd();
    if (pfd >= 0) {
      int tfd = ::openat(pfd, ent->get_name().c_str(), O_RDONLY | O_NOFOLLOW);
      if (tfd >= 0) {
        char buf[8];
        std::string dm_xattr = xattr_strategy()->disk_name(RGW_NSFS_ATTR_DELETE_MARKER);
        ssize_t xlen = nsfs::fgetxattr(tfd, dm_xattr.c_str(), buf, sizeof(buf));
        state.is_dm = (xlen > 0);
        ::close(tfd);
      }
    }
  }

  return 0;
}

int NSFSObject::admit_write(const DoutPrefixProvider* dpp,
			    optional_yield y)
{
  const std::string& k = state.obj.key.name;
  if (k.empty() || (k.back() != '/') || !state.obj.key.ns.empty()) {
    return 0;
  }
  NSFSBucket* b = static_cast<NSFSBucket*>(bucket);
  /* the reader's limit too:  NooBaa versions a directory object
   * differently, and neither side handles that yet */
  if (b->get_info().versioned()) {
    return 0;
  }

  std::vector<std::unique_ptr<nsfs::Directory>> chain;
  nsfs::Directory* leaf_dir{nullptr};
  std::string leaf;
  int ret = nsfs::resolve_key_path(dpp, b->get_dir(), state.obj.key,
				   get_fname(/*use_version=*/false),
				   driver->get_reserved_names(),
				   b->is_internal(), /*create_dirs=*/false,
				   driver->ctx(), chain, leaf_dir, leaf,
				   /* conversion is the gateway's */
				   nsfs::FSIdentity::none(), nullptr);
  if (ret == -ENOENT) {
    /* no directory, so no object to convert */
    return 0;
  }
  if (ret < 0) {
    return ret;
  }
  ret = b->admit_write(dpp, y, leaf_dir, k);
  if (ret < 0) {
    return ret;
  }

  /* An entry built to read the NooBaa format describes an object that
   * is no longer there;  the next stat() finds the converted one. */
  if (dynamic_cast<nsfs::NooBaaDirectoryObject*>(ent.get())) {
    ent.reset();
    dir_chain.clear();
    return stat(dpp);
  }
  return 0;
}

int NSFSObject::make_ent(ObjectType type)
{
  /* A NooBaaDirectoryObject is for reading:  a writer needs the sentinel
   * itself, and admit_write() has rejected a write to one the
   * bucket's adoption has not converted yet, so by the time a write is
   * published the object is in RGW's format. */
  if (ent && !dynamic_cast<nsfs::NooBaaDirectoryObject*>(ent.get())) {
    return 0;
  }
  ent.reset();

  Directory* leaf_dir;
  std::string leaf_name;
  /* the walk opens and makes directories as the object's writer,
   * and both need a prefix provider to report through */
  const DoutPrefix dp(driver->ctx(), dout_subsys, "nsfs make_ent: ");
  int ret = resolve_key_path(&dp,
      static_cast<NSFSBucket *>(bucket)->get_dir(),
      state.obj.key,
      get_fname(/*use_version=*/true),
      driver->get_reserved_names(),
      static_cast<NSFSBucket *>(bucket)->is_internal(),
      /*create_dirs=*/true,
      driver->ctx(),
      dir_chain, leaf_dir, leaf_name,
      pending_identity, driver->get_access_strategy());
  if (ret < 0)
    return ret;

  switch (type.type) {
    case ObjectType::UNKNOWN:
      return -EINVAL;
    case ObjectType::FILE:
      ent = std::make_unique<File>(leaf_name, leaf_dir, driver->ctx());
      break;
    case ObjectType::DIRECTORY:
      ent = std::make_unique<Directory>(leaf_name, leaf_dir, driver->ctx());
      break;
    case ObjectType::MULTIPART:
      ent = std::make_unique<MPDirectory>(leaf_name, leaf_dir, driver->ctx());
      break;
  }

  apply_pending_access();
  return 0;
}

int NSFSObject::get_owner(const DoutPrefixProvider *dpp, optional_yield y, std::unique_ptr<User> *owner)
{
  /* An object that records no owner is its bucket's owner's, the same
   * owner its listing entry shows (DECISIONS.md 2026-10-11). */
  ACLOwner acl_owner;
  int ret = xattr_strategy()->object_owner(get_attrs(), acl_owner);
  if (ret == -ENODATA) {
    acl_owner = static_cast<NSFSBucket*>(get_bucket())->default_object_owner(
	dpp, y);
  } else if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__
        << ": unreadable " RGW_ATTR_ACL " attr" << dendl;
    return ret;
  }

  if (const auto* u = std::get_if<rgw_user>(&acl_owner.id)) {
    *owner = driver->get_user(*u);
  } else {
    *owner = driver->get_user(rgw_user(std::get<rgw_account_id>(acl_owner.id)));
  }
  (*owner)->load_user(dpp, y);
  return 0;
}

std::unique_ptr<Object::ReadOp> NSFSObject::get_read_op(const rgw_owner& authenticated_user)
{
  return std::make_unique<NSFSReadOp>(this, authenticated_user);
}

std::unique_ptr<Object::DeleteOp> NSFSObject::get_delete_op(const rgw_owner& authenticated_user)
{
  return std::make_unique<NSFSDeleteOp>(this, authenticated_user);
}

int NSFSObject::open(const DoutPrefixProvider* dpp, optional_yield y,
		     bool create, bool temp_file)
{
  int ret{0};

  if (!ent) {
    ret = stat(dpp);
    if (ret < 0) {
      if (!create) {
	return ret;
      }
      ret = make_ent(ObjectType::FILE);
    }
  }
  if (ret < 0) {
    return ret;
  }

  if (create) {
    ret = ent->create(dpp, y, nullptr, temp_file);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not create " << ent->get_name() << dendl;
      return ret;
    }
  }

  return ent->open(dpp);
}

int NSFSObject::link_temp_file(const DoutPrefixProvider *dpp, optional_yield y, bool excl)
{
  std::string temp_fname = gen_temp_fname();

  /* The publish is on the access-model axis, and the model decides it
   * before the first syscall rather than after a denial.
   *
   * A bucket directory is its owner's at `0770`, and a user
   * outside its group may open and write an `O_TMPFILE` there under a
   * raise, and still be denied the link and the rename that publish
   * it.  The write therefore fails at the publish even after the open
   * and the bytes have succeeded, which is what `s3-native` is for.
   *
   * It cannot be a retry.  The publish links the `O_TMPFILE` into the
   * directory under a temp name and then renames it onto the object,
   * and the strategy unlinks that temp name when the rename
   * fails -- so a first attempt spends the inode's one link.  An
   * `O_TMPFILE` is linkable exactly once:  the kernel sets
   * `I_LINKABLE` at creation and clears it on the first `linkat`, and
   * `vfs_link()` then returns `ENOENT` for an inode at `nlink == 0`
   * whatever capability the caller holds.  Measured 2026-10-19
   * (probes/tmpfile/relinkprobe.c).
   *
   * So under a model that permits it the whole publish is the
   * gateway's, for every write and not only a denied one.  The
   * published inode is still the writer's `O_TMPFILE` -- only the
   * directory operations move.
   *
   * `CAP_FOWNER`, and only the link needs it.  The link is of the
   * writer's inode, and `fs.protected_hardlinks` allows a hardlink to another's
   * inode only to its owner or to someone holding read *and* write on
   * it.  The gateway holds `CAP_DAC_READ_SEARCH`, so
   * `safe_hardlink_source()` returns false and the kernel falls to
   * `inode_owner_or_capable(CAP_FOWNER)`.  Measured 2026-10-19:
   * without the raise the publish runs as uid 1000 and `linkat`
   * returns EPERM.  The same rule bit the demote from the other side
   * (`d75e5561a39`).
   *
   * And `CAP_DAC_OVERRIDE`, because the directory is not the
   * gateway's -- a bucket's is its owner's and a key directory its
   * maker's, at 0770 -- and both the link and the rename write in it. */
  static constexpr uint64_t PUBLISH_AS_GATEWAY_CAPS =
    nsfs::cap_bit(CAP_FOWNER) | nsfs::cap_bit(CAP_DAC_OVERRIDE);
  const bool as_gateway = ent->get_identity().active() &&
    driver->get_access_strategy()->permits(nsfs::AccessClass::data);

  if (excl) {
    /* If-None-Match:  * finds no object in a link, and the exclusive
     * link would fail on it */
    const int lret = unlink_if_link(dpp, ent->get_identity(),
				    driver->get_access_strategy(),
				    ent->get_parent()->get_fd(),
				    ent->get_name().c_str());
    if (lret < 0) {
      return lret;
    }
  }

  int ret;
  if (as_gateway) {
    ret = nsfs::with_capabilities(dpp, PUBLISH_AS_GATEWAY_CAPS,
				  [&]() -> int {
      return ent->link_temp_file(dpp, y, temp_fname, excl,
				 /*as_gateway=*/true);
    });
    if (ret == 0) {
      driver->publishes_as_gateway.fetch_add(1, std::memory_order_relaxed);
      ldpp_dout(dpp, 10) << "nsfs: " << get_name()
	<< ": the gateway performed the publish itself, which the access"
	" model permits (" << driver->get_access_strategy()->name()
	<< ")" << dendl;
    }
  } else {
    ret = ent->link_temp_file(dpp, y, temp_fname, excl);
  }
  if (ret < 0)
    return ret;

  NSFSBucket *b = static_cast<NSFSBucket *>(get_bucket());
  if (!b) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << get_name()
		      << dendl;
    return -EINVAL;
  }

  ret = open(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20)
        << "ERROR: NSFSAtomicWriter failed opening file" << dendl;
    return ret;
  }

  ret = ent->stat(dpp, /*force=*/true);
  if (ret < 0) {
    ldpp_dout(dpp, 20)
        << "ERROR: NSFSAtomicWriter failed stat after link" << dendl;
    return ret;
  }

  /* The value into the map, then onto the object, and the second one
   * checked.  complete() writes the attributes before it links
   * (`write_attrs()` then `link_temp_file()`), and the statx this etag
   * is derived from does not exist until the link -- so nothing will
   * persist this map again and the attribute write is the only thing
   * that reaches disk.  It used to run unchecked, so under
   * impersonation the object published carrying the opaque etag
   * RGWPutObj wrote while the client was told the derived one. */
  if (nsfs_non_md5_etag_into_attrs(dpp, ent->get_stx(), get_attrs())) {
    const int eret = nsfs_record_non_md5_etag(
      dpp, ent->get_fd(), ent->get_stx(), xattr_strategy(),
      ent->get_identity(), driver->etag_as_gateway_injected(), y);
    if (eret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not record the etag on "
        << get_name() << ": " << cpp_strerror(-eret)
        << ";  the object would carry a different etag than the one "
        << "returned to the client" << dendl;
      return eret;
    }
  }

  uint32_t flags = FSEnt::FLAG_NONE;
  const auto& binfo = b->get_info();
  if (binfo.versioned()) {
    int fd = ent->get_fd();
    if (fd >= 0) {
      const struct statx& stx = ent->get_stx();
      std::string ver_id = binfo.versioning_enabled()
        ? nsfs_version_id_from_statx(stx) : NULL_VERSION_ID;
      std::string xattr = xattr_strategy()->disk_name(RGW_NSFS_ATTR_VERSION_ID);
      /* As the identity that wrote the object, and checked.
       *
       * A `user.*` attribute needs write permission on the file,
       * tested against the *calling* thread rather than against
       * whoever opened the descriptor -- so the gateway writing
       * through the writer's fd fails on a 0660 file it does
       * not own.  This ignored its result, so under impersonation
       * every versioned PUT published an object carrying no recorded
       * version id while reporting the derived one to the client.
       * The next demote then read no record, filed the object as the
       * null version (`6ac7174791c`), and the id the client holds
       * named nothing:  20 versioning tests, all of them a GET or a
       * HEAD by a version id that exists only in the response. */
      const int vret = nsfs::with_identity(dpp, ent->get_identity(),
					   [&]() -> int {
	if (::fsetxattr(fd, xattr.c_str(), ver_id.c_str(),
			ver_id.size(), 0) < 0) {
	  return -errno;
	}
	return 0;
      }, y);
      if (vret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not record the version id on "
          << get_name() << ": " << cpp_strerror(-vret)
          << ";  a later demote would file it as the null version"
          << dendl;
        return vret;
      }
      set_instance(ver_id);
    }
    flags = FSEnt::FLAG_LIST_VERSIONS;
  }

  /* Add this object's listing entry from the key we already have.  Going
   * through fill_cache() here composed the key from an empty path prefix,
   * so a nested object was cached under its bare leaf name -- absent
   * under its real key, and colliding with any same-named leaf in another
   * directory. */
  auto* bcache = driver->get_bucket_cache();
  if (bcache) {
    rgw_bucket_dir_entry bde{};
    ret = ent->make_dir_entry(dpp, y, get_key(), flags,
			      b->default_object_owner(dpp, y), bde);
    if (ret < 0) {
      return ret;
    }
    bcache->add_entry(dpp, b->get_name(), bde);
  }
  return 0;
}


int NSFSObject::close()
{
  if (ent)
    return ent->close();

  return 0;
}

int NSFSObject::read(int64_t ofs, int64_t left, bufferlist& bl,
		      const DoutPrefixProvider* dpp, optional_yield y)
{
  if (!ent)
    return -ENOENT;
  /* Data asks the access model too.  `posix-strict` denies it;  `s3-native` performs the read as the
   * gateway, which is what that model is for.  An earlier version of
   * this comment said every configured model denies it, which was true
   * when it was written and is why only this path was wired up --
   * the publish was not, and an overwrite under `s3-native` failed at
   * the rename until 2026-10-19. */
  return with_cross_account_retry(dpp, nsfs::AccessClass::data,
				  [&](int use_fd) -> int {
    return ent->read(ofs, left, bl, dpp, y, use_fd);
  });
}

int NSFSObject::write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp,
		       optional_yield y)
{
  return ent->write(ofs, bl, dpp, y);
}

int NSFSObject::write_attrs(const DoutPrefixProvider* dpp, optional_yield y,
			    const std::vector<std::string>* rmattrs,
			    int use_fd)
{
  /* an object nothing has loaded has no entry yet:  open it, as
   * delete_obj_attrs() does, rather than dereference one */
  if (!ent) {
    const int ret = open(dpp, y);
    if (ret < 0) {
      return ret;
    }
  }
  return ent->write_attrs(dpp, y, state.attrset, nullptr,
			  AttrWriteMode::MERGE, rmattrs, use_fd);
}

/* An upload's staging -- its meta object -- is the gateway's, and is
 * read as the gateway whoever asks:  S3 authorizes ListParts and its
 * permission check, and the authenticated user has no access to scaffolding
 * (DECISIONS.md 2026-10-15). */
static bool reads_staging(rgw::sal::Object* obj)
{
  auto* b = static_cast<NSFSBucket*>(obj->get_bucket());
  return b && b->is_internal();
}

int NSFSObject::NSFSReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  /* Deny here, before anything is opened, wherever the identity
   * cannot be pinned:  credentials that do not resolve, and no
   * authenticated user.  A metadata request -- HEAD, the tag, ACL
   * and attribute reads -- reaches neither iterate() nor read(), so
   * a denial deferred to them served it as the gateway.  The
   * authorisation read carries the authenticated user, so it is
   * denied here too, which is the earlier 403 rgw_op.cc expects. */
  if (!reads_staging(source)) {
    const int pret = pin_personality(dpp, y);
    if ((pret < 0) && (pret != -ENODATA) &&
	!((pret == -EACCES) && source->driver->unresolved_as_gateway_injected(
	    acting_for(auth_user, params.on_behalf_of)))) {
      return pret;
    }
    /* the identity this read will run as;  inactive only when
     * impersonation is off, or under inject-unresolved-as-gateway */
    ldpp_dout(dpp, 20) << "nsfs: read " << source->get_name()
      << " as '" << auth_user << "' resolved=" << identity.active()
      << dendl;
    /* Before stat(), which is what creates the entry and opens it.
     * A read op never writes the object. */
    source->set_access(identity, /*read_only=*/true);
  }

  int ret = source->stat(dpp);
  if (ret < 0)
    return ret;

  /* set version ID for GET/HEAD response if not already set */
  NSFSBucket* sb = static_cast<NSFSBucket*>(source->get_bucket());
  if (sb && sb->get_info().versioned() &&
      source->get_instance().empty() &&
      source->get_fsent() && source->get_fsent()->exists()) {
    auto* fe = source->get_fsent();
    int vfd = fe->get_fd();
    if (vfd < 0) {
      fe->open(dpp);
      vfd = fe->get_fd();
    }
    source->set_instance(
      (vfd >= 0) ? recorded_version_id(vfd, source->xattr_strategy(),
				       source->xattr_fallback())
		 : nsfs_version_id_from_statx(fe->get_stx()));
  }

  ret = source->get_obj_attrs(y, dpp);
  if (ret < 0)
    return ret;

  /* `get_obj_attrs()` synthesizes an etag for an object that carries
   * none, so by here one exists.  Checked rather than assumed, because
   * everything below reports it. */
  bufferlist etag_bl;
  if (!source->get_attr(RGW_ATTR_ETAG, etag_bl)) {
    ldpp_dout(dpp, 0) << "ERROR: " << source->get_name()
      << " has no etag after get_obj_attrs()" << dendl;
    return -EINVAL;
  }

  {
    uint16_t pc = 0;
    if (decode_raw_attr(source->get_attrs(), RGW_NSFS_ATTR_MULTIPART_PART_COUNT, pc) && pc > 0) {
      params.parts_count = pc;
    }
  }

  if (params.part_num) {
    int pn = *params.part_num;
    std::vector<uint64_t> part_sizes;
    if (!decode_raw_attr(source->get_attrs(), RGW_NSFS_ATTR_MULTIPART_PART_SIZES, part_sizes)) {
      if (pn == 1) {
        params.parts_count = 1;
      } else {
        return -ERR_INVALID_PART;
      }
    } else {
      if (pn < 1 || pn > (int)part_sizes.size()) {
        return -ERR_INVALID_PART;
      }
      int64_t ofs = 0;
      for (int i = 0; i < pn - 1; ++i) {
        ofs += part_sizes[i];
      }
      part_ofs = ofs;
      source->set_obj_size(part_sizes[pn - 1]);
    }
  }

#if 0 // WIP
  if (params.mod_ptr || params.unmod_ptr) {
    obj_time_weight src_weight;
    src_weight.init(astate);
    src_weight.high_precision = params.high_precision_time;

    obj_time_weight dest_weight;
    dest_weight.high_precision = params.high_precision_time;

    if (params.mod_ptr && !params.if_nomatch) {
      dest_weight.init(*params.mod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (!(dest_weight < src_weight)) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (params.unmod_ptr && !params.if_match) {
      dest_weight.init(*params.unmod_ptr, params.mod_zone_id, params.mod_pg_ver);
      ldpp_dout(dpp, 10) << "If-UnModified-Since: " << dest_weight << " Last-Modified: " << src_weight << dendl;
      if (dest_weight < src_weight) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
#endif

  if (params.mod_ptr || params.unmod_ptr) {
    if (params.mod_ptr && !params.if_nomatch) {
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.mod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
      if (!(*params.mod_ptr < source->get_mtime())) {
        return -ERR_NOT_MODIFIED;
      }
    }

    if (params.unmod_ptr && !params.if_match) {
      ldpp_dout(dpp, 10) << "If-Modified-Since: " << *params.unmod_ptr << " Last-Modified: " << source->get_mtime() << dendl;
      if (*params.unmod_ptr < source->get_mtime()) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  if (params.if_match) {
    std::string if_match_str = rgw_string_unquote(params.if_match);
    ldpp_dout(dpp, 10) << "If-Match: " << if_match_str << " ETAG: " << etag_bl.c_str() << dendl;

    if (if_match_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) != 0) {
      return -ERR_PRECONDITION_FAILED;
    }
  }
  if (params.if_nomatch) {
    std::string if_nomatch_str = rgw_string_unquote(params.if_nomatch);
    ldpp_dout(dpp, 10) << "If-No-Match: " << if_nomatch_str << " ETAG: " << etag_bl.c_str() << dendl;
    if (if_nomatch_str.compare(0, etag_bl.length(), etag_bl.c_str(), etag_bl.length()) == 0) {
      return -ERR_NOT_MODIFIED;
    }
  }

  if (params.lastmod) {
    *params.lastmod = source->get_mtime();
  }

  return 0;
}

int NSFSObject::NSFSReadOp::read(int64_t ofs, int64_t end, bufferlist& bl,
				     optional_yield y, const DoutPrefixProvider* dpp)
{
  /* see iterate() */
  if (nsfs::impersonation_enabled() && !identity.active() &&
      !reads_staging(source)) {
    ldpp_dout(dpp, 0) << "ERROR: not reading " << source->get_name()
      << " as the gateway;  this read pinned no identity" << dendl;
    return -EACCES;
  }

  return source->read(ofs + part_ofs, end + 1, bl, dpp, y);
}

/* Resolved through the bucket's format, so an object in a base bucket
 * is read and written in NooBaa's names.  The driver's are the
 * fallback for an object constructed without a bucket. */
nsfs::XattrStrategy* NSFSObject::xattr_strategy() const
{
  auto* b = static_cast<NSFSBucket*>(get_bucket());
  return b ? b->xattr_strategy() : driver->get_xattr_strategy();
}

nsfs::XattrStrategy* NSFSObject::xattr_fallback() const
{
  auto* b = static_cast<NSFSBucket*>(get_bucket());
  return b ? b->xattr_fallback() : nullptr;
}

nsfs::PathStrategy* NSFSObject::path_strategy() const
{
  auto* b = static_cast<NSFSBucket*>(get_bucket());
  return b ? b->path_strategy() : driver->get_path_strategy();
}

const std::string NSFSObject::get_fname(bool use_version)
{
  return path_strategy()->object_name(state.obj.key,
						  use_version);
}

std::string NSFSObject::gen_temp_fname()
{
  std::string temp_fname;
  enum { RAND_SUFFIX_SIZE = 8 };
  char buf[RAND_SUFFIX_SIZE + 1];

  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, RAND_SUFFIX_SIZE);
  std::string key_path = get_fname(/*use_version=*/true);
  auto last_slash = key_path.rfind('/');
  std::string basename = (last_slash != std::string::npos)
    ? key_path.substr(last_slash + 1) : key_path;
  temp_fname = "." + basename + ".";
  temp_fname.append(buf);

  return temp_fname;
}

static int nsfs_data_file_fd(nsfs::FSEnt* ent)
{
  if (!ent) {
    return -1;
  }
  if (ent->get_type() == ObjectType::FILE) {
    return ent->get_fd();
  }
  return -1;
}


/* Resolve this operation's identity into `ident`:  the key and the
 * credentials, and a personality registered for it on this thread.
 *
 * The personality is not kept.  An id names a credential only on the
 * ring it was registered on, and the operation may resume on another
 * thread after any yield, so each submission looks it up where it runs
 * (IdentityPersonality, open_as()).  Registering here still checks the
 * credentials can be applied, before anything is served.
 *
 * Returns -ENODATA when impersonation is off, which is not a
 * failure:  the caller submits with no personality and the request
 * is served exactly as it was before any of this existed.
 *
 * Every other negative is a denial.  With the switch on, a request
 * whose credentials cannot be applied must not fall back to the
 * gateway's own -- the daemon reaches more of the filesystem than
 * the user it would be acting for.
 */
static int pin_personality_for(const DoutPrefixProvider* dpp,
			       NSFSDriver* driver, const rgw_owner& who,
			       nsfs::FSIdentity* ident, optional_yield y)
{
  if (! nsfs::impersonation_enabled()) {
    return -ENODATA;
  }
  if (ident->active()) {
    return 0;				/* already resolved */
  }
  if (nsfs::owner_empty(who)) {
    /* This request specified no identity.  Under the switch that
     * is a gap in the call site rather than an anonymous request,
     * and serving it as the daemon is the thing to avoid. */
    ldpp_dout(dpp, 0) << "ERROR: impersonation is on but this operation"
      " carries no authenticated identity" << dendl;
    return -EACCES;
  }

  /* Level 1 first.  A personality already registered on this ring
   * is all a caller needs -- the slot carries the credentials
   * it was registered with -- so a hit costs a hash lookup and a
   * pin, and skips the identity-row read below.  That read is
   * otherwise paid on every impersonated request, because nothing
   * caches it. */
  nsfs::PersonalityRef ref;
  if (nsfs::find_personality(who, &ref, dpp) == 0) {
    ident->key = who;
    ident->cred = ref.credentials();
    ident->gen = ref.generation();
    return 0;
  }

  /* Read before resolving:  an identity write changes the row and then
   * the generation, so credentials read after this are at least this
   * new, and a change landing in between leaves them marked stale. */
  const uint64_t gen = nsfs::identity_generation(who);
  rgw_account_id account;
  nsfs::Credentials cred;
  const int ret = resolve_credentials_for(dpp, driver, who, y,
					  &account, cred);
  if (ret == -ENOENT) {
    /* A valid account always has an identity record;  one without
     * is a provisioning fault, reported rather than repaired. */
    ldpp_dout(dpp, 0) << "ERROR: no credentials resolve for " << who
      << (account.empty() ? "" : " or its account ") << account
      << ":  no identity row under it carries a uid;  impersonation is"
      " enabled, so the request is denied rather than served as the"
      " gateway" << dendl;
    return -EACCES;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not resolve a POSIX identity for "
      << who << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << "nsfs: resolved " << who << " to uid " << cred.uid
    << " gid " << cred.gid << " and " << cred.groups.size()
    << " supplementary groups" << dendl;
  const int ret2 = nsfs::acquire_personality(dpp, who, cred, gen, &ref);
  if (ret2 == 0) {
    ident->key = who;
    ident->cred = cred;
    ident->gen = gen;
  }
  return ret2;
}

/* Who an operation is for:  the user it authenticated as, else whom
 * work no request asked for is done for (lifecycle's bucket owner;
 * see rgw::sal::no_authenticated_user).  Empty when it is neither. */
static const rgw_owner& acting_for(const rgw_owner& authenticated_user,
				   const rgw_owner& on_behalf_of)
{
  return nsfs::owner_empty(authenticated_user) ? on_behalf_of
					       : authenticated_user;
}

int NSFSObject::NSFSReadOp::pin_personality(const DoutPrefixProvider* dpp,
					    optional_yield y)
{
  return pin_personality_for(dpp, source->driver,
			     acting_for(auth_user, params.on_behalf_of),
			     &identity, y);
}

int NSFSAtomicWriter::pin_personality(const DoutPrefixProvider* dpp,
				      optional_yield y)
{
  return pin_personality_for(dpp, driver, auth_user, &identity, y);
}

int NSFSMultipartWriter::pin_personality(const DoutPrefixProvider* dpp,
					 optional_yield y)
{
  return pin_personality_for(dpp, driver, auth_user, &identity, y);
}

int NSFSObject::NSFSReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs,
					int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  /* prepare() denies an identity it cannot pin, so this holds only
   * for a caller that skipped it:  serving object data as the
   * gateway is the escalation impersonation exists to prevent */
  if (nsfs::impersonation_enabled() && !identity.active() &&
      !reads_staging(source)) {
    ldpp_dout(dpp, 0) << "ERROR: not reading " << source->get_name()
      << " as the gateway;  this read pinned no identity" << dendl;
    return -EACCES;
  }

  int64_t left;
  int64_t cur_ofs = ofs + part_ofs;
  end += part_ofs;

  if (end < 0)
    left = 0;
  else
    left = end - ofs + 1;

  if (posix_try_use_uring(dpp, y, /*nsfs=*/true)) {
    int fd = nsfs_data_file_fd(source->get_fsent());
    if (fd >= 0) {
      unsigned qd = dpp->get_cct()->_conf.get_val<uint64_t>("rgw_nsfs_get_iodepth");
      bool dio = dpp->get_cct()->_conf.get_val<bool>("rgw_nsfs_direct_io");
      int64_t chunk = dpp->get_cct()->_conf.get_val<Option::size_t>(
          "rgw_nsfs_read_chunk_size");
      if (chunk <= 0) {
        chunk = READ_SIZE;
      }
      /* resolved in prepare();  inactive means impersonation is off */
      nsfs::IdentityPersonality who(identity);
      UringReadWindow win(dpp, y, fd, qd, chunk, dio, /*nsfs=*/true,
			  identity.active() ? &who : nullptr);
      return win.iterate(cur_ofs, left,
          [dpp, cb, source = source](bufferlist& bl, int len) {
            int ret = cb->handle_data(bl, 0, len);
            if (ret < 0) {
              ldpp_dout(dpp, 0) << " ERROR: callback failed on "
                                << source->get_name() << ": " << ret << dendl;
            }
            return ret;
          });
    }
  }

  /* QD2 read-ahead: while handle_data() yields in the beast frontend's
   * async_write, spawn the next File::read() on the same executor so the
   * disk fetch of chunk N+1 overlaps the network send of chunk N.  One
   * prefetch at a time, in order.  Without a yield_context there is
   * nothing to overlap with, so the loop stays sequential. */
  std::optional<ceph::async::spawn_throttle> throttle;
  unsigned iodepth = posix_sync_clamp_iodepth(
      dpp, dpp->get_cct()->_conf.get_val<uint64_t>("rgw_nsfs_get_iodepth"),
      "rgw_nsfs_get_iodepth");
  if (y && iodepth >= 2) {
    throttle.emplace(y.get_yield_context(), 1);
  }

  bufferlist slots[2];
  int cur = 0;
  int prefetch_len = 0;
  bool prefetch_pending = false;

  auto drain_prefetch = [&]() {
    if (prefetch_pending) {
      prefetch_pending = false;
      throttle->wait();
    }
  };
  auto drain_guard = make_scope_guard([&] {
    if (prefetch_pending) {
      prefetch_pending = false;
      try {
        throttle->wait();
      } catch (...) {}
    }
  });

  while (left > 0) {
    int len;
    if (prefetch_pending) {
      drain_prefetch();
      cur = 1 - cur;
      len = prefetch_len;
    } else {
      slots[cur].clear();
      len = source->read(cur_ofs, left, slots[cur], dpp, y);
    }
    if (len < 0) {
      ldpp_dout(dpp, 0) << " ERROR: could not read " << source->get_name() <<
	  " ofs: " << cur_ofs << " error: " << cpp_strerror(len) << dendl;
      drain_prefetch();
      return len;
    } else if (len == 0) {
      /* Done */
      break;
    }

    left -= len;
    cur_ofs += len;

    if (left > 0 && throttle) {
      const int next = 1 - cur;
      const int64_t pofs = cur_ofs;
      const int64_t pleft = left;
      slots[next].clear();
      throttle->spawn([source = source, dpp, next, pofs, pleft, &slots, &prefetch_len]
                      (boost::asio::yield_context /* child */) {
        prefetch_len = source->read(pofs, pleft, slots[next], dpp, null_yield);
      });
      prefetch_pending = true;
    }

    int ret = cb->handle_data(slots[cur], 0, len);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << " ERROR: callback failed on " << source->get_name() << ": " << ret << dendl;
      drain_prefetch();
      return ret;
    }
  }

  return 0;
}

int NSFSObject::NSFSReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  /* a caller that skips prepare() -- lifecycle's tag read -- still reads
   * as whom it is for */
  if (nsfs::impersonation_enabled() && !identity.active()) {
    const int pret = pin_personality(dpp, y);
    if ((pret < 0) && (pret != -ENODATA) &&
	!((pret == -EACCES) && source->driver->unresolved_as_gateway_injected(
	    acting_for(auth_user, params.on_behalf_of)))) {
      return pret;
    }
    source->set_access(identity, /*read_only=*/true);
  }
  if (!source->check_exists(dpp)) {
    return -ENOENT;
  }
  if (source->get_obj_attrs(y, dpp) < 0) {
    return -ENODATA;
  }
  if (!source->get_attr(name, dest)) {
    return -ENODATA;
  }

  return 0;
}

/*
 * Check delete preconditions (if_match, last_mod_time_match, size_match)
 * against the given target metadata.  Returns 0 if all pass, or
 * -ERR_PRECONDITION_FAILED on mismatch.
 */
static int check_delete_preconditions(
    const rgw::sal::Object::DeleteOp::Params& params,
    const rgw::sal::Attrs& attrs,
    ceph::real_time mtime,
    uint64_t size)
{
  if (params.if_match && strcmp(params.if_match, "*") != 0) {
    auto it = attrs.find(RGW_ATTR_ETAG);
    if (it == attrs.end()) {
      return -ERR_PRECONDITION_FAILED;
    }
    std::string etag = it->second.to_str();
    std::string if_match_str = rgw_string_unquote(params.if_match);
    if (if_match_str != etag) {
      return -ERR_PRECONDITION_FAILED;
    }
  }

  if (!real_clock::is_zero(params.last_mod_time_match)) {
    if (params.last_mod_time_match_precise) {
      if (params.last_mod_time_match != mtime) {
        return -ERR_PRECONDITION_FAILED;
      }
    } else {
      if (real_clock::to_time_t(params.last_mod_time_match) !=
          real_clock::to_time_t(mtime)) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  if (params.size_match.has_value()) {
    if (*params.size_match != size) {
      return -ERR_PRECONDITION_FAILED;
    }
  }

  return 0;
}

/*
 * Publish the freshly-promoted current version into the listing cache.
 *
 * promote_version() returns the id and etag when it actually promoted;
 * both were free there.  When it promoted nothing -- a current version
 * already existed, so another writer won the race -- describe what is on
 * disk instead.  An empty instance would make a *distinct* cache key and
 * add a row beside that writer's rather than overwriting it, which shows
 * up as a surplus version in the listing.
 */
static void cache_promoted_current(NSFSDriver* driver,
                                   const nsfs::XattrStrategy* xs,
                                   const DoutPrefixProvider* dpp,
                                   const ACLOwner& bucket_owner,
                                   const std::string& bucket_name,
                                   const std::string& obj_name,
                                   int parent_fd, const std::string& leaf,
                                   PromoteResult& promoted,
                                   const struct statx& pstx)
{
  /* One descriptor, one attribute sweep.  get_x_attrs(xs) lists and reads
   * every xattr on the file, and XattrStrategy::parse_disk_name() accepts
   * both the
   * user.nsfs.rgw. and user.nsfs. prefixes -- so the digest, the version
   * id and the delete-marker flag all arrive in pattrs, and the separate
   * fgetxattr() for each is redundant with the sweep. */
  Attrs pattrs;
  int fd = ::openat(parent_fd, leaf.c_str(), O_RDONLY | O_NOFOLLOW);
  if (fd >= 0) {
    get_x_attrs(null_yield, dpp, fd, pattrs, leaf, xs);
    ::close(fd);
  }

  std::string promoted_ver = promoted.version_id;
  if (!promoted.promoted) {
    /* promote_version() promoted nothing -- a current version already
     * existed, so another writer won -- so describe what is on disk.  An
     * empty instance would make a *distinct* cache key and add a row
     * beside that writer's rather than overwriting it, which shows up as
     * a surplus version in the listing. */
    auto vit = pattrs.find(RGW_NSFS_ATTR_VERSION_ID);
    /* absent or empty reads as the null version, matching
     * is_null_version_fd(xs)'s len <= 0 case */
    bool is_null = (vit == pattrs.end()) || (vit->second.length() == 0) ||
                   (vit->second.to_str() == NULL_VERSION_ID);
    /* the record, as everywhere an id is reported */
    promoted_ver = is_null ? NULL_VERSION_ID : vit->second.to_str();
  }
  if (promoted.etag.empty()) {
    auto eit = pattrs.find(RGW_ATTR_ETAG);
    if (eit != pattrs.end()) {
      promoted.etag = eit->second.to_str();
    }
  }

  rgw_bucket_dir_entry bde{};
  bde.key.name = obj_name;
  bde.key.instance = promoted_ver;
  bde.ver.pool = 1;
  bde.ver.epoch = 1;
  bde.exists = true;
  bde.meta.category = RGWObjCategory::Main;
  bde.meta.size = pstx.stx_size;
  bde.meta.accounted_size = pstx.stx_size;
  bde.meta.mtime = from_statx_timestamp(pstx.stx_mtime);
  bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
  bde.meta.etag = promoted.etag.empty()
    ? synthesize_etag(pstx) : promoted.etag;
  bde.flags = rgw_bucket_dir_entry::FLAG_VER |
              rgw_bucket_dir_entry::FLAG_CURRENT;
  auto dit = pattrs.find(RGW_NSFS_ATTR_DELETE_MARKER);
  if (dit != pattrs.end() && dit->second.length() > 0) {
    bde.flags |= rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
  }
  ACLOwner acl_owner;
  set_dir_entry_owner(bde, xs->object_owner(pattrs, acl_owner), acl_owner,
		      bucket_owner);

  driver->get_bucket_cache()->add_entry(dpp, bucket_name, bde);
}

int NSFSObject::NSFSDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
					   optional_yield y, uint32_t flags)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, source->driver,
		       static_cast<NSFSBucket*>(source->get_bucket()));
  if (mg.result() < 0) {
    return mg.result();
  }
  {
    const int cret = source->admit_write(dpp, y);
    if (cret < 0) {
      return cret;
    }
  }
  /* Who this delete is for.  Resolved once, here, rather than at
   * each site that needs it:  a delete in a versioned bucket links
   * and unlinks inside `.versions`, and those are writes against an
   * identity's files.
   *
   * -ENODATA is impersonation off, which is not a failure -- the
   * identity stays inactive and every call below runs exactly as it
   * did before.  Any other error fails the delete rather than
   * falling back to the gateway. */
  const rgw_owner& who = acting_for(auth_user, params.on_behalf_of);
  nsfs::FSIdentity identity;
  {
    const int pret = pin_personality_for(dpp, source->driver, who,
					 &identity, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }
  /* what this delete chose, for the `ran-as` hint */
  source->driver->note_ran_as("delete", to_string(who), identity);
  /* and the object's walk and removals with it.  Read-only:  a delete
   * reads the entry, and its unlink is the directory's */
  if (identity.active()) {
    source->set_access(identity, /*ro=*/true);
  }
  const nsfs::AccessStrategy* model = source->driver->get_access_strategy();
  /* a denial is the delete's answer, never an absent object */
  const bool denial_injected = source->driver->denial_as_absence_injected();
  auto denied = [denial_injected](int r) {
    return !denial_injected && ((r == -EACCES) || (r == -EPERM));
  };

  bool has_cond = params.if_match ||
    !real_clock::is_zero(params.last_mod_time_match) ||
    params.size_match.has_value();

  NSFSBucket *b = static_cast<NSFSBucket*>(source->get_bucket());
  if (!b) {
    return -EINVAL;
  }

  const auto& binfo = b->get_info();
  bool versioned = binfo.versioned();
  bool ver_enabled = binfo.versioning_enabled();
  /*
   * Single DELETE with ?versionId= → params.marker_version_id.
   * Multi-object DeleteObjects → object key instance.
   * Plain DELETE without versionId → both empty.
   *
   * When load_obj_state detects a delete marker, it copies dm_version_id
   * into instance.  Skip that case so plain DELETE stacks new DMs rather
   * than deleting the existing one.  Multi-delete sets instance from
   * the request (dm_version_id stays empty), so it is not skipped.
   */
  std::string req_version_id = params.marker_version_id;
  if (req_version_id.empty()) {
    std::string inst = source->get_instance();
    if (!inst.empty() && inst != source->dm_version_id) {
      req_version_id = inst;
    }
  }

  ldpp_dout(dpp, 10) << "delete_obj: key=" << source->get_name()
    << " versioned=" << versioned << " ver_enabled=" << ver_enabled
    << " req_version_id=" << req_version_id << dendl;

  if (!versioned) {
    if (has_cond) {
      int ret = source->stat(dpp);
      if (ret == -ENOENT) {
        return 0;
      }
      if (ret < 0) {
        return ret;
      }
      ret = check_delete_preconditions(params, source->get_attrs(),
                                       source->get_mtime(),
                                       source->get_size());
      if (ret < 0) {
        return ret;
      }
    }
    return source->delete_object(dpp, y, flags, nullptr, nullptr);
  }

  /* versioned delete with specific versionId */
  if (!req_version_id.empty()) {
    ldpp_dout(dpp, 10) << "delete_obj: version-specific vid="
      << req_version_id << " key=" << source->get_name() << dendl;

    /* resolve path to get parent dir for version lock;
     * stat without instance so we always get the object's parent
     * directory, not .versions/ */
    std::string saved_instance = source->get_instance();
    source->clear_instance();
    source->ent.reset();
    int ret = source->stat(dpp);
    /* a denial is the delete's answer:  read as absence, the branch
     * finds no directory and answers success for a version it did not
     * touch */
    if (denied(ret)) {
      return ret;
    }
    nsfs::FSEnt* ent = source->get_fsent();

    /* acquire version lock — must use the object's parent dir,
     * not .versions/, so all competing threads contend on the
     * same lock file */
    int lock_parent_fd = -1;
    if (ent && ent->get_parent()) {
      lock_parent_fd = ent->get_parent()->get_fd();
      if (lock_parent_fd < 0) {
        ent->get_parent()->open(dpp);
        lock_parent_fd = ent->get_parent()->get_fd();
      }
    } else {
      /* object doesn't exist as current — resolve path just for lock */
      std::vector<std::unique_ptr<nsfs::Directory>> lk_chain;
      nsfs::Directory* lk_dir = nullptr;
      std::string lk_leaf;
      int r = nsfs::resolve_key_path(dpp, b->get_dir(), source->get_key(),
          source->get_fname(/*use_version=*/false),
          source->driver->get_reserved_names(), b->is_internal(),
          /*create_dirs=*/false, source->driver->ctx(),
          lk_chain, lk_dir, lk_leaf,
          identity, source->driver->get_access_strategy());
      if (r == 0 && lk_dir) {
        lock_parent_fd = lk_dir->get_fd();
        if (lock_parent_fd < 0) {
          lk_dir->open(dpp);
          lock_parent_fd = lk_dir->get_fd();
        }
      }
    }
    auto vlock = (lock_parent_fd >= 0)
      ? version_lock_recovering(dpp, source->driver, source->xattr_strategy(),
				source->get_bucket()->get_name(),
				lock_parent_fd, identity)
      : std::unique_ptr<VersionLockHandle>();

    /* re-stat under lock with original instance to get fresh state */
    source->set_instance(saved_instance);
    source->ent.reset();
    ret = source->stat(dpp);
    if (denied(ret)) {
      return ret;
    }
    ent = source->get_fsent();

    /* check if the requested version is the current */
    ldpp_dout(dpp, 20) << "delete_obj: stat ret=" << ret
      << " ent=" << (ent ? "yes" : "null")
      << " req_version_id=" << req_version_id << dendl;
    if (ret == 0 && ent) {
      bool cur_match;
      if (req_version_id == NULL_VERSION_ID) {
        int chk_fd = ent->get_fd();
        if (chk_fd < 0) {
          ent->open(dpp);
          chk_fd = ent->get_fd();
        }
        cur_match = (chk_fd >= 0 &&
		     is_null_version_fd(chk_fd, source->xattr_strategy(),
					source->xattr_fallback()));
        ldpp_dout(dpp, 20) << "delete_obj: null version check is_null="
          << cur_match << dendl;
      } else {
        int chk_fd = ent->get_fd();
        if (chk_fd < 0) {
          ent->open(dpp);
          chk_fd = ent->get_fd();
        }
        const std::string cur_ver_id = (chk_fd >= 0)
          ? recorded_version_id(chk_fd, source->xattr_strategy(),
				source->xattr_fallback())
          : nsfs_version_id_from_statx(ent->get_stx());
        cur_match = (req_version_id == cur_ver_id);
        ldpp_dout(dpp, 10) << "delete_obj: cur_ver_id=" << cur_ver_id
          << " cur_match=" << cur_match << dendl;
      }
      if (cur_match) {
        if (has_cond) {
          int r = check_delete_preconditions(
            params, source->get_attrs(),
            source->get_mtime(), source->get_size());
          if (r < 0) {
            return r;
          }
        }
        ldpp_dout(dpp, 10) << "delete_obj: deleting current version "
          << req_version_id << dendl;
        result.version_id = req_version_id;
        /* resolve the object's parent directory and leaf name in the
         * bucket hierarchy (not .versions/) for promote_version */
        int promote_fd = -1;
        std::string promote_leaf;
        {
          std::string fname = source->get_fname(/*use_version=*/false);
          std::vector<std::unique_ptr<nsfs::Directory>> prom_chain;
          nsfs::Directory* prom_dir = nullptr;
          int r = nsfs::resolve_key_path(dpp, b->get_dir(), source->get_key(),
              fname, source->driver->get_reserved_names(), b->is_internal(),
              /*create_dirs=*/false, source->driver->ctx(),
              prom_chain, prom_dir, promote_leaf,
              identity, source->driver->get_access_strategy());
          if (r == 0 && prom_dir) {
            int pfd = prom_dir->get_fd();
            if (pfd < 0) {
              prom_dir->open(dpp);
              pfd = prom_dir->get_fd();
            }
            if (pfd >= 0) {
              promote_fd = ::dup(pfd);
            }
          }
        }
        ret = source->delete_object(dpp, y, flags, nullptr, nullptr);
        if (ret < 0) {
          ldpp_dout(dpp, 10) << "delete_obj: delete_object failed ret="
            << ret << dendl;
          if (promote_fd >= 0) { ::close(promote_fd); }
          return ret;
        }
        if (promote_fd >= 0) {
          ldpp_dout(dpp, 10) << "delete_obj: promoting after delete of "
            << promote_leaf << dendl;
          PromoteResult promoted;
          /* Reported where it fails, and not reported to the client:
           * the version the request named is gone, which is what the
           * delete undertook.  What is left behind is an operator's
           * problem, not something DeleteObject can express. */
          (void) promote_version(promote_fd, promote_leaf, dpp,
                          source->driver->get_fs_strategy(),
			  source->xattr_strategy(),
			  source->driver->get_identity_db(), identity, model, y,
			  &promoted);
          struct statx pstx;
          if (nsfs::statx(promote_fd, promote_leaf.c_str(),
                    AT_SYMLINK_NOFOLLOW, STATX_ALL, &pstx) == 0) {
            cache_promoted_current(source->driver, source->xattr_strategy(),
                                   dpp, b->default_object_owner(dpp, y),
                                   b->get_name(),
                                   source->get_key().get_index_key_name(),
                                   promote_fd, promote_leaf, promoted, pstx);
          }
          ::close(promote_fd);
        }
        return 0;
      }
    }

    /* look in .versions/ — need parent dir even if current is gone */
    int parent_fd = -1;
    std::string leaf_name;
    if (ent && ent->get_parent()) {
      nsfs::Directory* parent = ent->get_parent();
      parent_fd = parent->get_fd();
      if (parent_fd < 0) {
        parent->open(dpp);
        parent_fd = parent->get_fd();
      }
      leaf_name = ent->get_name();
    } else {
      /* current doesn't exist, resolve path to get parent dir */
      std::vector<std::unique_ptr<nsfs::Directory>> chain;
      nsfs::Directory* leaf_dir = nullptr;
      ret = nsfs::resolve_key_path(dpp, b->get_dir(), source->get_key(),
          source->get_fname(/*use_version=*/false),
          source->driver->get_reserved_names(), b->is_internal(),
          /*create_dirs=*/false, source->driver->ctx(),
          chain, leaf_dir, leaf_name,
          identity, source->driver->get_access_strategy());
      if (denied(ret)) {
        return ret;
      }
      if (ret == 0 && leaf_dir) {
        parent_fd = leaf_dir->get_fd();
        if (parent_fd < 0) {
          leaf_dir->open(dpp);
          parent_fd = leaf_dir->get_fd();
        }
      }
    }

    if (parent_fd >= 0) {
      int vfd = openat_as(dpp, identity, model, parent_fd,
			  HIDDEN_VERSIONS_PATH.c_str(), O_RDONLY | O_DIRECTORY);
      if (denied(vfd)) {
        return vfd;
      }
      if (vfd >= 0) {
        std::string ver_name = nsfs_ver_entry(leaf_name, req_version_id);
        bool is_dm = false;
        int ver_fd = openat_as(dpp, identity, model, vfd, ver_name.c_str(),
                               O_RDONLY);
        if (denied(ver_fd)) {
          ::close(vfd);
          return ver_fd;
        }
        if (ver_fd < 0) {
          ldpp_dout(dpp, 10) << "delete_obj: " << ver_name
            << " not found in .versions/ (already deleted?)" << dendl;
          ::close(vfd);
          result.version_id = req_version_id;
          return 0;
        }
        ldpp_dout(dpp, 10) << "delete_obj: unlinking " << ver_name
          << " from .versions/" << dendl;

        /* read delete-marker flag */
        {
          char buf[8];
          std::string dm_xattr =
            source->xattr_strategy()->disk_name(RGW_NSFS_ATTR_DELETE_MARKER);
          ssize_t xlen = nsfs::fgetxattr(ver_fd, dm_xattr.c_str(),
                                     buf, sizeof(buf));
          if (xlen > 0) {
            is_dm = true;
          }
        }

        /* conditional check against version's metadata */
        if (has_cond) {
          rgw::sal::Attrs ver_attrs;
          struct statx ver_stx;
          ceph::real_time ver_mtime;
          uint64_t ver_size = 0;

          if (nsfs::statx(ver_fd, "", AT_EMPTY_PATH,
                    STATX_MTIME | STATX_SIZE, &ver_stx) == 0) {
            ver_mtime = from_statx_timestamp(ver_stx.stx_mtime);
            ver_size = ver_stx.stx_size;
          }

          /* read etag xattr if not a delete marker */
          if (!is_dm) {
            char ebuf[256];
            std::string etag_x = source->xattr_strategy()->disk_name(RGW_ATTR_ETAG);
            ssize_t elen = nsfs::fgetxattr(ver_fd, etag_x.c_str(),
                                       ebuf, sizeof(ebuf));
            if (elen > 0) {
              bufferlist bl;
              bl.append(ebuf, elen);
              ver_attrs[RGW_ATTR_ETAG] = std::move(bl);
            }
          }

          int r = check_delete_preconditions(
            params, ver_attrs, ver_mtime, ver_size);
          if (r < 0) {
            ::close(ver_fd);
            ::close(vfd);
            return r;
          }
        }
        ::close(ver_fd);

        {
          const int uret = unlinkat_as(dpp, identity, model, vfd,
                                       ver_name.c_str());
          if ((uret < 0) && (uret != -ENOENT)) {
            ldpp_dout(dpp, 4) << "delete_obj: unlinking " << ver_name
              << ": " << cpp_strerror(-uret) << dendl;
            ::close(vfd);
            return uret;
          }
        }
        ::close(vfd);
        result.version_id = req_version_id;
        result.delete_marker = is_dm;
        {
          cls_rgw_obj_key cache_key;
          cache_key.name = source->get_key().get_index_key_name();
          cache_key.instance = req_version_id;
          source->driver->get_bucket_cache()->remove_entry(
            dpp, b->get_name(), cache_key);
        }
        /* if we deleted a DM from .versions/ and no current file
         * exists, promote the newest non-DM version */
        if (is_dm && parent_fd >= 0) {
          PromoteResult promoted;
          /* as above:  reported where it fails, and the delete of the
           * marker still succeeded */
          (void) promote_version(parent_fd, leaf_name, dpp,
                          source->driver->get_fs_strategy(),
			  source->xattr_strategy(),
			  source->driver->get_identity_db(), identity, model, y,
			  &promoted);
          /* update cache for the promoted version */
          struct statx pstx;
          if (nsfs::statx(parent_fd, leaf_name.c_str(),
                    AT_SYMLINK_NOFOLLOW, STATX_ALL, &pstx) == 0) {
            cache_promoted_current(source->driver, source->xattr_strategy(),
                                   dpp, b->default_object_owner(dpp, y),
                                   b->get_name(),
                                   source->get_key().get_index_key_name(),
                                   parent_fd, leaf_name, promoted, pstx);
          }
        }
        return 0;
      }
    }
    result.version_id = req_version_id;
    return 0;
  }

  /* versioned delete without versionId — create delete marker */
  {
  /* clear instance and ent so stat() re-resolves from scratch
   * (instance may have been set by load_obj_state's dm detection,
   * and ent may point to a .versions/ entry from a prior stat) */
  source->clear_instance();
  source->ent.reset();
  int ret = source->stat(dpp);
  /* a denial is the delete's answer, not an absent object:  read as
   * absence, the path looks for the key's directory, cannot open it
   * either, and returns -EINVAL */
  if (denied(ret)) {
    return ret;
  }

  if (has_cond) {
    if (ret == -ENOENT && source->dm_version_id.empty()) {
      return 0;
    }
    if (ret == -ENOENT) {
      /*
       * DM is current.  stat() set state.mtime and state.size = 0.
       * DMs have no etag, so any specific if_match fails; "*" passes
       * via the helper (no RGW_ATTR_ETAG in empty attrs → skip).
       */
      rgw::sal::Attrs dm_attrs;
      int r = check_delete_preconditions(
        params, dm_attrs, source->get_mtime(), 0);
      if (r < 0) {
        return r;
      }
    } else if (ret < 0) {
      return ret;
    } else {
      int r = check_delete_preconditions(
        params, source->get_attrs(),
        source->get_mtime(), source->get_size());
      if (r < 0) {
        return r;
      }
    }
  }

  /* a key that names a directory has no object, and none can be
   * written while the directory holds its name (DECISIONS.md
   * 2026-10-24), so its delete writes no marker:  204 and nothing
   * else, as NooBaa's delete_object() returns for it */
  if ((ret == -ENOENT) && source->names_directory) {
    ldpp_dout(dpp, 10) << "delete_obj: " << source->get_name()
      << " names a directory;  no delete marker" << dendl;
    return 0;
  }

  nsfs::FSEnt* ent = source->get_fsent();
  int parent_fd = -1;
  std::string dm_leaf;

  if (ent && ent->get_parent()) {
    nsfs::Directory* parent = ent->get_parent();
    parent_fd = parent->get_fd();
    if (parent_fd < 0) {
      parent->open(dpp);
      parent_fd = parent->get_fd();
    }
    dm_leaf = ent->get_name();
  }

  std::vector<std::unique_ptr<nsfs::Directory>> dm_chain;
  if (parent_fd < 0) {
    /* current doesn't exist — resolve path just to get the parent dir */
    nsfs::Directory* leaf_dir = nullptr;
    int r = nsfs::resolve_key_path(dpp, b->get_dir(), source->get_key(),
        source->get_fname(/*use_version=*/false),
        source->driver->get_reserved_names(), b->is_internal(),
        /*create_dirs=*/true, source->driver->ctx(),
        dm_chain, leaf_dir, dm_leaf,
        identity, source->driver->get_access_strategy());
    if (denied(r)) {
      return r;
    }
    if (r == 0 && leaf_dir) {
      parent_fd = leaf_dir->get_fd();
      if (parent_fd < 0) {
        leaf_dir->open(dpp);
        parent_fd = leaf_dir->get_fd();
      }
    }
  }

  if (parent_fd < 0) {
    return -EINVAL;
  }

  int vfd = open_versions_dir(dpp, parent_fd, identity, model);
  if (vfd < 0) {
    return vfd;
  }

  auto vlock = version_lock_recovering(dpp, source->driver, source->xattr_strategy(),
				       source->get_bucket()->get_name(),
				       parent_fd, identity);

  /* determine if current is a null version */
  bool cur_is_null = false;
  if (ret == 0 && ent && ent->exists()) {
    int chk_fd = ent->get_fd();
    if (chk_fd < 0) {
      ent->open(dpp);
      chk_fd = ent->get_fd();
    }
    if (chk_fd >= 0) {
      cur_is_null = is_null_version_fd(chk_fd, source->xattr_strategy(),
				       source->xattr_fallback());
    }
  }

  /* in suspended mode, remove any existing _null from .versions/ */
  if (!ver_enabled) {
    std::string null_name = nsfs_ver_entry(dm_leaf, NULL_VERSION_ID);
    const int uret = unlinkat_as(dpp, identity, model, vfd, null_name.c_str());
    if ((uret < 0) && (uret != -ENOENT)) {
      ldpp_dout(dpp, 4) << "delete_obj: unlinking " << null_name
        << ": " << cpp_strerror(-uret) << dendl;
      ::close(vfd);
      return uret;
    }
  }

  if (ret == -ENOENT) {
    /* no current version, but the name may hold a link:  not an object,
     * so not a version, and the delete unlinks it before the marker */
    const int lret = unlink_if_link(dpp, identity, model, parent_fd,
				    dm_leaf.c_str());
    if (lret < 0) {
      ::close(vfd);
      return lret;
    }
  }

  std::string demoted_ver_id;
  bool did_demote = false;

  /* demote current to .versions/ with retry on CAS mismatch;
   * skip when suspended and current is null (S3: replace null version) */
  if (ret == 0 && ent && ent->exists() &&
      !(!ver_enabled && cur_is_null)) {
    for (int attempt = 0; attempt < NSFS_VERSION_RETRIES; ++attempt) {
      struct statx cur_stx;
      if (nsfs::statx(parent_fd, dm_leaf.c_str(), AT_SYMLINK_NOFOLLOW,
                STATX_ALL, &cur_stx) < 0) {
        break;
      }
      /* the record, read again on each attempt:  a retry follows a
       * concurrent writer, whose file it may now be */
      std::string cur_ver_id = cur_is_null
        ? NULL_VERSION_ID
        : nsfs_version_id_from_statx(cur_stx);
      if (!cur_is_null) {
        int rfd = openat_as(dpp, identity, model, parent_fd, dm_leaf.c_str(),
			    O_RDONLY);
        if (rfd >= 0) {
          cur_ver_id = recorded_version_id(rfd, source->xattr_strategy(),
					   source->xattr_fallback());
          ::close(rfd);
        }
      }
      std::string ver_name = nsfs_ver_entry(dm_leaf, cur_ver_id);
      uint64_t cur_mtime = statx_mtime_ns(cur_stx);
      uint64_t cur_ino = cur_stx.stx_ino;

      /* As the authenticated user, which phase C's DeleteOp parameter now
       * supplies.  The link's target is the current version, which an
       * identity wrote, and fs.protected_hardlinks denies it to the
       * gateway -- the same denial the writer's demote had before it
       * carried one. */
      SafeResult sr = SafeResult::ERROR;
      (void) nsfs::with_identity_or_raise(dpp, identity, model,
                                          nsfs::AccessClass::data,
                                          RAISED_KEYDIR_CAPS, [&]() -> int {
        sr = source->driver->get_fs_strategy()->safe_link(
                                dpp, parent_fd, dm_leaf,
                                vfd, ver_name,
                                cur_mtime, cur_ino, null_yield,
                                nsfs::FSIdentity::none());
        return (sr == SafeResult::DENIED) ? -EACCES : 0;
      }, y);
      if (sr == SafeResult::OK) {
        /* when it stopped being current, for NooBaa's lifecycle and
         * (D5) RGW's:  on the demoted file, which is the authenticated
         * user's, and a `user.*` write is checked against the calling
         * thread */
        const auto now_ms = std::to_string(
          std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        const std::string ts_x =
          source->xattr_strategy()->disk_name(RGW_NSFS_ATTR_NON_CURRENT_TS);
        const int tret = nsfs::with_identity_or_raise(dpp, identity, model,
                                                      nsfs::AccessClass::data,
                                                      RAISED_KEYDIR_CAPS,
                                                      [&]() -> int {
          const int dfd = ::openat(vfd, ver_name.c_str(), O_RDONLY | O_NOFOLLOW);
          if (dfd < 0) {
            return -errno;
          }
          const int r = (::fsetxattr(dfd, ts_x.c_str(), now_ms.c_str(),
                                     now_ms.size(), 0) < 0) ? -errno : 0;
          ::close(dfd);
          return r;
        }, y);
        if (tret < 0) {
          ldpp_dout(dpp, 4) << "delete_obj: " << ver_name
            << " demoted, but its non-current timestamp was not written: "
            << cpp_strerror(-tret) << dendl;
        }
        /* CAS unlink: remove current only if it still matches.  The
         * authenticated user's too:  the entry it removes is in the bucket
         * directory, which phase D hands to the account. */
        (void) nsfs::with_identity_or_raise(dpp, identity, model,
                                            nsfs::AccessClass::data,
                                            RAISED_KEYDIR_CAPS, [&]() -> int {
          const SafeResult ur = source->driver->get_fs_strategy()->safe_unlink(
                      dpp, parent_fd, dm_leaf, vfd,
                      cur_mtime, cur_ino, null_yield,
                      nsfs::FSIdentity::none());
          return (ur == SafeResult::DENIED) ? -EACCES : 0;
        }, y);
        demoted_ver_id = cur_ver_id;
        did_demote = true;
        break;
      }
      if (sr == SafeResult::DENIED) {
        /* the tree denied the authenticated user, which is a 403 and not the
         * silent give-up this used to be:  without it the delete
         * marker is written over a current version that was never
         * demoted, and the version history loses an entry */
        ldpp_dout(dpp, 4) << "delete_obj: demote of " << dm_leaf
          << " denied by the filesystem" << dendl;
        /* vfd is this function's;  parent_fd is the Directory's and
         * is not ours to close */
        ::close(vfd);
        return -EACCES;
      }
      if (sr == SafeResult::ERROR) {
        break;
      }
      if (sr == SafeResult::MISMATCH && cur_is_null) {
        struct statx chk;
        if (nsfs::statx(vfd, ver_name.c_str(), AT_SYMLINK_NOFOLLOW,
                  STATX_INO, &chk) == 0) {
          break;
        }
      }
      struct timespec ts = {0, 100000 + (rand() % 500000)};
      nanosleep(&ts, nullptr);
    }
  } else if (!ver_enabled && ret == 0 && ent && ent->exists() && cur_is_null) {
    /* suspended + null current: just unlink (rename will fail if no current) */
    const int uret = unlinkat_as(dpp, identity, model, parent_fd,
                                 dm_leaf.c_str());
    if ((uret < 0) && (uret != -ENOENT)) {
      ldpp_dout(dpp, 4) << "delete_obj: unlinking " << dm_leaf
        << ": " << cpp_strerror(-uret) << dendl;
      ::close(vfd);
      return uret;
    }
  }

  /* create delete marker as zero-byte file in .versions/ */
  static std::atomic<uint64_t> dm_counter{0};
  std::string dm_tmp = ".dm_tmp_" +
    std::to_string(getpid()) + "_" + std::to_string(dm_counter.fetch_add(1));

  /* As the authenticated user, all of it.
   *
   * A delete marker is a version in that user's tree, so it is
   * that user's file by the same rule as any other object.  It
   * was the gateway's, and that is not only an ownership
   * inconsistency:  the *next* delete demotes this marker, and a
   * demote hard-links the file it demotes, which
   * fs.protected_hardlinks permits only to the source inode's owner.
   * A gateway-owned marker is therefore undemotable by the identity
   * that made it -- `delete_obj: demote of <key> denied by the
   * filesystem`, a 403 on the second delete of a key.
   *
   * The attribute writes are inside the same switch for the reason
   * the version id is (`0dcf4a191f6`):  a `user.*` write is tested
   * against the calling thread, so the gateway writing to the
   * authenticated user's file would fail. */
  int dm_fd = -1;
  bool dm_raised = false;
  const int dmret = nsfs::with_identity_or_raise(dpp, identity, model,
                                                 nsfs::AccessClass::data,
                                                 RAISED_CREATE_CAPS,
                                                 [&]() -> int {
    dm_fd = ::openat(vfd, dm_tmp.c_str(),
                     O_WRONLY | O_CREAT | O_EXCL,
                     nsfs::file_create_mode());
    if (dm_fd < 0) {
      return -errno;
    }
    return 0;
  }, y, &dm_raised);
  if (dmret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: delete_obj: could not create the delete"
      " marker for " << dm_leaf << ": " << cpp_strerror(-dmret) << dendl;
    ::close(vfd);
    return dmret;
  }
  if (dm_fd >= 0) {
    (void) nsfs::apply_create_mode(dpp, dm_fd, nsfs::file_create_mode(), y);
    /* made by the gateway, and the authenticated user's as any object is */
    if (dm_raised) {
      (void) chown_raised(dpp, dm_fd, "", identity.cred.uid,
                          identity.cred.gid);
    }
    /* Every attribute on the marker is written as its owner, for the
     * reason the version id is:  a `user.*` write is tested against
     * the calling thread, so the gateway writing to the authenticated
     * user's file fails.  Each is checked:  a marker missing its flag
     * is an empty object, which a GET would return. */
    std::string dm_ver_id;
    const int aret = nsfs::with_identity(dpp, identity, [&]() -> int {
      if (source->driver->attr_write_fail_injected(b->get_name())) {
	source->driver->record_attr_write_fail();
	return -EIO;
      }
      /* set delete_marker xattr */
      std::string dm_xattr = source->xattr_strategy()->disk_name(RGW_NSFS_ATTR_DELETE_MARKER);
      std::string dm_val = "true";
      if (::fsetxattr(dm_fd, dm_xattr.c_str(), dm_val.c_str(),
		      dm_val.size(), 0) < 0) {
	return -errno;
      }

      /* stat for version ID */
      struct statx dm_stx;
      if (nsfs::statx(dm_fd, "", AT_EMPTY_PATH, STATX_ALL, &dm_stx) == 0) {
        dm_ver_id = ver_enabled
          ? nsfs_version_id_from_statx(dm_stx)
          : NULL_VERSION_ID;
      } else {
        dm_ver_id = NULL_VERSION_ID;
      }

      /* set version_id xattr */
      std::string vid_xattr = source->xattr_strategy()->disk_name(RGW_NSFS_ATTR_VERSION_ID);
      if (::fsetxattr(dm_fd, vid_xattr.c_str(),
		      dm_ver_id.c_str(), dm_ver_id.size(), 0) < 0) {
	return -errno;
      }

      /* copy ACL from the source object so the DM has owner info.
       *
       * There is not always one to copy.  A DELETE of a key that does
       * not exist still writes a marker, and then the source carries no
       * ACL, so the marker got none and listed as unknown/unknown --
       * RGW emits an <Owner> for a delete marker, and on this backend
       * the recorded ACL is the only thing that field reads from.
       * params.obj_owner is the caller the SAL contract supplies for
       * exactly this ("acl owner for delete marker if necessary"), so
       * the marker is owned by whoever deleted, which is also what S3
       * says it is. */
      bufferlist acl_bl;
      if (!rgw::sal::get_attr(source->get_attrs(), RGW_ATTR_ACL, acl_bl)) {
        RGWAccessControlPolicy policy;
        policy.create_default(params.obj_owner.id,
  			    params.obj_owner.display_name);
        policy.encode(acl_bl);
      }
      if (acl_bl.length() > 0) {
        std::string acl_xattr = source->xattr_strategy()->disk_name(RGW_ATTR_ACL);
        std::string acl_v = attr_on_disk(source->xattr_strategy(),
                                         RGW_ATTR_ACL, acl_bl);
        if (::fsetxattr(dm_fd, acl_xattr.c_str(),
			acl_v.data(), acl_v.size(), 0) < 0) {
	  return -errno;
	}
      }
      return 0;
    }, y);
    ::close(dm_fd);
    /* unnamed yet, so taken back whole;  the demote above stands, as
     * it does when the marker cannot be created or named */
    if (aret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: delete_obj: could not write the delete"
	" marker's attributes for " << dm_leaf << ": "
	<< cpp_strerror(-aret) << dendl;
      (void) unlinkat_as(dpp, identity, model, vfd, dm_tmp.c_str());
      ::close(vfd);
      return aret;
    }

    /* rename to final name, as the marker's maker */
    std::string dm_name = nsfs_ver_entry(dm_leaf, dm_ver_id);
    {
      const int rret = renameat_as(dpp, identity, model, vfd, dm_tmp.c_str(),
                                   vfd, dm_name.c_str());
      if (rret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: delete_obj: naming the delete marker "
          << dm_name << ": " << cpp_strerror(-rret) << dendl;
        ::close(vfd);
        return rret;
      }
    }

    result.delete_marker = true;
    result.version_id = dm_ver_id;
  }

  ::close(vfd);

  /* surgical cache update: demoted current → non-current, add delete marker */
  auto* bcache = source->driver->get_bucket_cache();
  std::string obj_name = source->get_key().get_index_key_name();

  if (did_demote) {
    /* remove old current entry, add non-current entry */
    cls_rgw_obj_key old_key;
    old_key.name = obj_name;
    old_key.instance = demoted_ver_id;
    bcache->remove_entry(dpp, b->get_name(), old_key);

    rgw_bucket_dir_entry dem_bde{};
    dem_bde.key.name = obj_name;
    dem_bde.key.instance = demoted_ver_id;
    dem_bde.ver.pool = 1;
    dem_bde.ver.epoch = 1;
    dem_bde.exists = true;
    dem_bde.meta.category = RGWObjCategory::Main;
    dem_bde.meta.size = ent->get_stx().stx_size;
    dem_bde.meta.accounted_size = ent->get_stx().stx_size;
    dem_bde.meta.mtime = from_statx_timestamp(ent->get_stx().stx_mtime);
    dem_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
    {
      /* its own digest, as HEAD reports it;  the synthesized change
       * token is for objects that carry no etag */
      bufferlist etag_bl;
      if (rgw::sal::get_attr(source->get_attrs(), RGW_ATTR_ETAG, etag_bl)) {
	dem_bde.meta.etag = etag_bl.to_str();
      } else {
	dem_bde.meta.etag = synthesize_etag(ent->get_stx());
      }
    }
    dem_bde.flags = rgw_bucket_dir_entry::FLAG_VER;
    {
      ACLOwner acl_owner;
      set_dir_entry_owner(dem_bde,
                          source->xattr_strategy()->object_owner(
                              source->get_attrs(), acl_owner),
                          acl_owner, b->default_object_owner(dpp, y));
    }
    bcache->add_entry(dpp, b->get_name(), dem_bde);
  }

  if (!result.version_id.empty()) {
    /* add delete marker entry */
    rgw_bucket_dir_entry dm_bde{};
    dm_bde.key.name = obj_name;
    dm_bde.key.instance = result.version_id;
    dm_bde.ver.pool = 1;
    dm_bde.ver.epoch = 1;
    dm_bde.exists = true;
    dm_bde.meta.category = RGWObjCategory::Main;
    dm_bde.meta.size = 0;
    dm_bde.meta.accounted_size = 0;
    dm_bde.meta.mtime = ceph::real_clock::now();
    dm_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
    dm_bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                   rgw_bucket_dir_entry::FLAG_CURRENT |
                   rgw_bucket_dir_entry::FLAG_DELETE_MARKER;
    {
      /* The source has no ACL when the key being deleted does not
       * exist, and a DELETE of a key that does not exist still writes
       * a marker.  params.obj_owner is what the SAL hands us for
       * exactly that case, and it is the same owner the marker's own
       * ACL was just written with, so the cache entry and the disk
       * agree. */
      ACLOwner acl_owner;
      if (source->xattr_strategy()->object_owner(source->get_attrs(),
						 acl_owner) >= 0) {
        dm_bde.meta.owner = to_string(acl_owner.id);
        dm_bde.meta.owner_display_name = acl_owner.display_name;
      } else {
        dm_bde.meta.owner = to_string(params.obj_owner.id);
        dm_bde.meta.owner_display_name = params.obj_owner.display_name;
      }
    }
    bcache->add_entry(dpp, b->get_name(), dm_bde);
  }

  return 0;
  } /* versioned delete without versionId */
}

/* The copy's source read is on the access-model axis:  S3 may have
 * authorised a read of an object the filesystem denies this
 * authenticated user, and under a model that permits it the gateway performs
 * the read.  Resolved here rather than inside FSStrategy, because
 * whether the model is a daemon setting or a per-bucket one is not
 * settled (inflight/access-model-axis.md) and the caller is what
 * knows the bucket. */
bool NSFSObject::copy_source_as_gateway() const
{
  return ent && ent->get_identity().active() &&
    driver->get_access_strategy()->permits(nsfs::AccessClass::data);
}

int NSFSObject::copy(const DoutPrefixProvider *dpp, optional_yield y,
                      NSFSBucket *sb, NSFSBucket *db, NSFSObject *dobj,
                      const nsfs::CopyHints& hints)
{
  rgw_obj_key dst_key = dobj->get_key();

  std::vector<std::unique_ptr<Directory>> dst_chain;
  Directory* dst_leaf_dir;
  std::string dst_leaf_name;
  int ret = resolve_key_path(dpp, db->get_dir(), dst_key,
      path_strategy()->object_name(dst_key, false),
      driver->get_reserved_names(), db->is_internal(),
      /*create_dirs=*/true, driver->ctx(),
      dst_chain, dst_leaf_dir, dst_leaf_name,
      /* copy_object() gave the source entry the authenticated user's
       * identity */
      ent ? ent->get_identity() : nsfs::FSIdentity::none(),
      driver->get_access_strategy());
  if (ret < 0)
    return ret;

  return ent->copy(dpp, y, dst_leaf_dir, dst_leaf_name, hints,
		   copy_source_as_gateway());
}

void NSFSMPObj::init_gen(NSFSDriver* driver, const std::string& _oid, ACLOwner& _owner)
{
  char buf[33];
  std::string new_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
  /* Generate an upload ID */

  gen_rand_alphanumeric(driver->ctx(), buf, sizeof(buf) - 1);
  new_id.append(buf);
  init(_oid, new_id, _owner);
}

int NSFSMultipartPart::load(const DoutPrefixProvider* dpp, optional_yield y,
			     NSFSDriver* driver, rgw_obj_key& key)
{
  if (part_file) {
    /* Already loaded */
    return 0;
  }

  part_file = std::make_unique<File>(
      upload->get_shadow()->path_strategy()->object_name(key, false),
      upload->get_shadow()->get_dir(), driver->ctx());

  // Stat the part_file object to get things like size
  int ret = part_file->stat(dpp, y);
  if (ret < 0) {
    return ret;
  }

  Attrs attrs;
  ret = part_file->read_attrs(dpp, y, attrs);
  if (ret < 0) {
    return ret;
  }

  if (!decode_attr(attrs, RGW_NSFS_ATTR_MPUPLOAD, info)) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": failed to decode part info: " << key << dendl;
    return -EIO;
  }

  return 0;
}

int NSFSMultipartUpload::load(const DoutPrefixProvider *dpp, bool create)
{
  int ret = 0;
  if (!shadow) {
    NSFSBucket* pb = static_cast<NSFSBucket*>(bucket);
    std::optional<std::string> ns{mp_ns};

    /* Where this format keeps its staging directories.  The nsfs
     * format returns ".", the bucket directory.  NooBaa's is two levels
     * down, under a temp directory whose name carries a bucket id from
     * NooBaa's config store -- so it is found and never invented.
     * Nothing gives nsfs that id, so a format which wants a root and
     * has none is rejected rather than served somewhere else. */
    /* Placing an upload asks where staging WOULD go, which may create
     * the intermediate directory;  reading asks where it IS, which
     * must not name one that is absent. */
    auto root = create
	? mpu_strategy()->staging_root_for_write(dpp,
						 pb->get_dir()->get_fd())
	: mpu_strategy()->staging_root(dpp, pb->get_dir()->get_fd());
    if (!root) {
      ldpp_dout(dpp, 4) << "no staging root for bucket " << pb->get_name()
			<< " in format " << pb->profile_name()
			<< ";  not placing an upload" << dendl;
      return -ENOTSUP;
    }

    /* Held by the upload, not by this scope:  the staging directory
     * keeps a raw parent pointer, so the root has to outlive it. */
    Directory* staging_parent = pb->get_dir();
    if (*root != ".") {
      staging_root_dir = std::make_unique<Directory>(*root, pb->get_dir(),
						     driver->ctx());
      ret = staging_root_dir->open(dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 4) << "could not open staging root " << *root
			  << " under " << pb->get_name() << ": "
			  << cpp_strerror(-ret) << dendl;
	return ret;
      }
      staging_parent = staging_root_dir.get();
    }

    std::string staging_name = get_fname();
    std::unique_ptr<Directory> mpdir = std::make_unique<MPDirectory>(staging_name, staging_parent, driver->ctx());
    /* Placed as the uploader init() pinned;  read as the gateway
     * (DECISIONS.md 2026-10-15, ListParts) */
    if (create && identity.active()) {
      mpdir->set_identity(identity);
      mpdir->set_access_model(driver->get_access_strategy());
    }

    shadow = std::make_unique<NSFSBucket>(driver, std::move(mpdir), rgw_bucket(std::string(), mp_obj.upload_id), mp_ns);

    ret = shadow->load_bucket(dpp, null_yield);
    if (ret == -ENOENT && create) {
      ret = shadow->create(dpp, null_yield, nullptr,
			   rgw::sal::no_authenticated_user);
    }
  }

  return ret;
}

std::unique_ptr<rgw::sal::Object> NSFSMultipartUpload::get_meta_obj()
{
  std::unique_ptr<rgw::sal::Object> meta_obj{nullptr};

  load(nullptr);

  const std::string meta_name = mpu_strategy()->meta_name();
  if (!shadow) {
    meta_obj = bucket->get_object(rgw_obj_key(get_meta(), std::string(), mp_ns));
  } else {
    meta_obj = shadow->get_object(rgw_obj_key(meta_name, std::string()));
  }

  auto nsfs_meta_obj = static_cast<NSFSObject*>(meta_obj.get());
  if (shadow) {
    nsfs_meta_obj->pin_bucket(shadow->clone());
  }
  rgw::sal::Attrs attrs;
  if (obj_retention) {
    buffer::list obj_retention_bl;
    obj_retention->encode(obj_retention_bl);
    attrs[RGW_ATTR_OBJECT_RETENTION] = std::move(obj_retention_bl);
  }
  if (obj_legal_hold) {
    buffer::list obj_legal_hold_bl;
    obj_legal_hold->encode(obj_legal_hold_bl);
    attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = std::move(obj_legal_hold_bl);
  }
  nsfs_meta_obj->set_attrs(attrs);

  return meta_obj;
}

int NSFSMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y,
				ACLOwner& owner, rgw_placement_rule& dest_placement,
				rgw::sal::Attrs& attrs)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, static_cast<NSFSBucket*>(bucket));
  if (mg.result() < 0) {
    return mg.result();
  }
  int ret;

  /* who staging, the meta object and its record are made as */
  {
    const int pret = pin_personality_for(dpp, driver, auth_user, &identity, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }

  /* Create the shadow bucket */
  ret = load(dpp, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << " ERROR: could not get shadow dir for mp upload "
      << get_key() << dendl;
    return ret;
  }

  /* Now create the meta object */
  std::unique_ptr<rgw::sal::Object> meta_obj;

  meta_obj = get_meta_obj();

  if (identity.active()) {
    static_cast<NSFSObject*>(meta_obj.get())->set_access(identity,
							 /*ro=*/false);
  }
  ret = static_cast<NSFSObject*>(meta_obj.get())->open(dpp, y, true);
  if (ret < 0) {
    return ret;
  }

  mp_obj.upload_info.cksum_type = cksum_type;
  mp_obj.upload_info.cksum_flags = cksum_flags;

  if (obj_retention) {
    mp_obj.upload_info.obj_retention_exist = true;
    mp_obj.upload_info.obj_retention = *obj_retention;
  }
  if (obj_legal_hold) {
    mp_obj.upload_info.obj_legal_hold_exist = true;
    mp_obj.upload_info.obj_legal_hold = *obj_legal_hold;
  }

  mp_obj.upload_info.dest_placement = dest_placement;
  mp_obj.owner = owner;

  bufferlist bl;
  encode(mp_obj, bl);

  attrs[RGW_NSFS_ATTR_MPUPLOAD] = bl;

  /* As the uploader, who made the meta object (DECISIONS.md
   * 2026-10-23). */
  ret = meta_obj->set_obj_attrs(dpp, &attrs, nullptr, y,
				rgw::sal::FLAG_LOG_OP, auth_user);
  if (ret < 0) {
    return ret;
  }

  /* and whatever the format records for itself.  The nsfs record is
   * the attribute just written;  NooBaa's is the key, in the meta
   * file's content, which is where NooBaa's gateway and the nsfs
   * listing look for it. */
  nsfs::MPUStrategy::StagedUpload su;
  su.key = mp_obj.oid;
  su.upload_id = mp_obj.upload_id;
  su.bucket = bucket->get_name();
  su.storage_class = dest_placement.storage_class;

  auto str_attr = [&attrs](const char* name) -> std::string {
    auto i = attrs.find(name);
    if (i == attrs.end()) {
      return {};
    }
    std::string v = i->second.to_str();
    /* the attribute is stored counted, trailing NUL included */
    while (!v.empty() && (v.back() == '\0')) {
      v.pop_back();
    }
    return v;
  };
  su.content_type = str_attr(RGW_ATTR_CONTENT_TYPE);
  su.content_encoding = str_attr(RGW_ATTR_CONTENT_ENC);

  static const std::string meta_prefix{RGW_ATTR_META_PREFIX};
  for (const auto& [k, v] : attrs) {
    if (k.starts_with(meta_prefix)) {
      std::string val = v.to_str();
      while (!val.empty() && (val.back() == '\0')) {
	val.pop_back();
      }
      su.xattr[k.substr(meta_prefix.size())] = std::move(val);
    }
  }

  if (mp_obj.upload_info.obj_retention_exist) {
    su.retention_mode = mp_obj.upload_info.obj_retention.get_mode();
    rgw_to_iso8601(mp_obj.upload_info.obj_retention.get_retain_until_date(),
		   &su.retain_until);
  }
  if (mp_obj.upload_info.obj_legal_hold_exist) {
    su.legal_hold = mp_obj.upload_info.obj_legal_hold.get_status();
  }

  return mpu_strategy()->write_staged_upload(dpp, shadow->get_dir()->get_fd(),
					     su);
}

int NSFSMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				      int num_parts, int marker,
				      int *next_marker, bool *truncated, optional_yield y,
				      bool assume_unsorted)
{
  int ret = load(dpp);
  if (ret < 0) {
    return ret;
  }

  auto* mc = driver->get_multipart_cache();
  if (!mc) {
    return -ENOENT;
  }

  file::listing::MultipartCacheKey cache_key{
    nsfs::mp_cache_bucket(bucket->get_tenant(), bucket->get_name()),
    mp_obj.meta};

  auto result = mc->list_parts(
    cache_key, marker, num_parts,
    [this, dpp, y](
      boost::container::flat_map<uint32_t, file::listing::MultipartPartInfo>& pm) {
      auto* dir = shadow->get_dir();
      dir->for_each(dpp,
	[&](const char* name) -> int {
	  std::string sname(name);
	  auto parsed = mpu_strategy()->part_number(sname);
	  if (!parsed) {
	    return 0;
	  }
	  const uint32_t pnum = *parsed;
	  if (pm.contains(pnum)) {
	    return 0;
	  }
	  auto pf = std::make_unique<nsfs::File>(sname, dir, driver->ctx());
	  if (const int sret = pf->stat(dpp, y); sret < 0) {
	    ldpp_dout(dpp, 10) << "nsfs: list_parts skips " << sname
	      << ": " << cpp_strerror(-sret) << dendl;
	    return 0;
	  }
	  file::listing::MultipartPartInfo pi;
	  pi.num = pnum;
	  /* the file's length, until the record says otherwise:  under a
	   * shared layout the file is the whole upload, so a record is
	   * what makes this part's own length knowable */
	  pi.size = pf->get_size();

	  /* Ask the format what its record says.  The nsfs record arrives
	   * decoded in `attrs`, through the bucket's XattrStrategy;  NooBaa
	   * keeps its record in attributes that reader drops as foreign, so
	   * NooBaa's strategy reads the file itself. */
	  Attrs attrs;
	  (void) pf->read_attrs(dpp, y, attrs);

	  nsfs::MPUStrategy::PartRecord rec;
	  if (mpu_strategy()->part_record(dpp, dir->get_fd(), sname, attrs,
					  rec)) {
	    pi.etag = std::move(rec.etag);
	    pi.mtime = rec.mtime;
	    pi.cksum = std::move(rec.cksum);
	    if (rec.size) pi.size = rec.size;
	    pi.shared = rec.shared;
	    pi.offset = rec.offset;
	    pi.stored = rec.stored;
	  }
	  pm[pnum] = std::move(pi);
	  return 0;
	});
    });

  /* each call returns one page;  callers such as complete() sum
   * parts.size() across pages, so the map must turn over */
  parts.clear();

  for (auto& pi : result.parts) {
    auto part = std::make_unique<NSFSMultipartPart>(this);
    auto* ppart = static_cast<NSFSMultipartPart*>(part.get());
    ppart->info.num = pi.num;
    ppart->info.size = pi.size;
    ppart->info.etag = std::move(pi.etag);
    ppart->info.mtime = pi.mtime;
    ppart->info.cksum = std::move(pi.cksum);
    ppart->info.shared = pi.shared;
    ppart->info.offset = pi.offset;
    ppart->info.stored = pi.stored;
    parts[pi.num] = std::move(part);
  }

  if (truncated)
    *truncated = result.truncated;

  if (next_marker)
    *next_marker = result.next_marker;

  return 0;
}

int NSFSMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, static_cast<NSFSBucket*>(bucket));
  if (mg.result() < 0) {
    return mg.result();
  }
  /* An upload NooBaa staged that this bucket's adoption has not
   * reached:  the guard waited out a running conversion, so the one that
   * stopped short of it is resumed by its command or the next pass, and
   * the write is answered with the retryable 503 rather than waiting in
   * the slot (DECISIONS.md 2026-10-21). */
  if (static_cast<NSFSBucket*>(bucket)->upload_awaits_conversion(
	dpp, mp_obj.meta)) {
    ldpp_dout(dpp, 4) << "nsfs: upload " << mp_obj.meta << " has not been "
      << "converted;  rejecting until the bucket's adoption reaches it"
      << dendl;
    return -ERR_SERVICE_UNAVAILABLE;
  }
  int ret;

  ret = load(dpp);
  if (ret < 0) {
    if (ret == -ENOENT)
      ret = ERR_NO_SUCH_UPLOAD;
    return ret;
  }

  /* Nobody asked when the bucket's deletion aborts it
   * (NSFSBucket::abort_multiparts()), which raises for it itself. */
  const rgw_owner& who = acting_for(auth_user, on_behalf_of);
  if (!nsfs::owner_empty(who)) {
    const int pret = pin_personality_for(dpp, driver, who, &identity, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }
  /* a denial is the abort's answer;  any other failure leaves what it
   * could not remove, as the removal always has */
  ret = remove_staging(dpp, y);
  if ((ret == -EACCES) || (ret == -EPERM)) {
    return ret;
  }
  if (auto* mc = driver->get_multipart_cache()) {
    mc->remove({nsfs::mp_cache_bucket(bucket->get_tenant(),
					bucket->get_name()), mp_obj.meta});
  }

  return 0;
}

/* As the authenticated user, through the access model (DECISIONS.md
 * 2026-10-23):  staging is its uploader's, so another identity is
 * denied under posix-strict and performed for under s3-native.  Under
 * the staging root, which is the bucket directory only where the
 * format stages there.  Removing `<bucket>/<name>` left NooBaa's
 * staging in place, and the upload was still listed after it had
 * completed. */
int NSFSMultipartUpload::remove_staging(const DoutPrefixProvider* dpp,
					optional_yield y)
{
  NSFSBucket* pb = static_cast<NSFSBucket*>(bucket);
  const int at = staging_root_dir ? staging_root_dir->get_fd()
				  : pb->get_dir()->get_fd();
  const std::string name = get_fname();
  shadow->get_dir()->close();
  return nsfs::with_identity_or_raise(dpp, identity,
				      driver->get_access_strategy(),
				      nsfs::AccessClass::data,
				      RAISED_KEYDIR_CAPS, [&]() -> int {
    return delete_directory(at, name.c_str(), true, dpp,
			    driver->get_reserved_names());
  }, y);
}

int NSFSMultipartUpload::complete(const DoutPrefixProvider *dpp,
				    optional_yield y, CephContext* cct,
				    std::map<int, std::string>& part_etags,
				    std::list<rgw_obj_index_key>& remove_objs,
				    uint64_t& accounted_size, bool& compressed,
				    RGWCompressionInfo& cs_info, off_t& ofs,
				    std::string& tag, ACLOwner& owner,
				    uint64_t olh_epoch,
				    rgw::sal::Object* target_obj,
				    prefix_map_t& processed_prefixes,
            const char *if_match,
            const char *if_nomatch)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(dpp, driver, static_cast<NSFSBucket*>(bucket));
  if (mg.result() < 0) {
    return mg.result();
  }
  /* An upload NooBaa staged that this bucket's adoption has not
   * reached:  the guard waited out a running conversion, so the one that
   * stopped short of it is resumed by its command or the next pass, and
   * the write is answered with the retryable 503 rather than waiting in
   * the slot (DECISIONS.md 2026-10-21). */
  if (static_cast<NSFSBucket*>(bucket)->upload_awaits_conversion(
	dpp, mp_obj.meta)) {
    ldpp_dout(dpp, 4) << "nsfs: upload " << mp_obj.meta << " has not been "
      << "converted;  rejecting until the bucket's adoption reaches it"
      << dendl;
    return -ERR_SERVICE_UNAVAILABLE;
  }
  {
    const int cret =
      static_cast<NSFSObject*>(target_obj)->admit_write(dpp, y);
    if (cret < 0) {
      return cret;
    }
  }
  /* who the object will belong to.  Denied where it cannot be applied,
   * as the writers deny it, rather than made as the gateway. */
  {
    const int pret = pin_personality_for(dpp, driver, auth_user, &identity, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs& attrs = target_obj->get_attrs();
  std::vector<uint64_t> part_sizes;
  std::vector<nsfs::MPUStrategy::PartPlacement> placements;

  ofs = accounted_size = 0;

  do {
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated, y);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret < 0)
      return ret;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << part_etags.size() << dendl;
      ret = -ERR_INVALID_PART;
      return ret;
    }

    for (auto obj_iter = parts.begin(); etags_iter != part_etags.end() && obj_iter != parts.end(); ++etags_iter, ++obj_iter, ++handled_parts) {
      NSFSMultipartPart* part = static_cast<rgw::sal::NSFSMultipartPart*>(obj_iter->second.get());
      uint64_t part_size = part->get_size();
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return ret;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
			 << etags_iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }
      std::string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->get_etag()) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: " << etags_iter->first
			 << " etag: " << etags_iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      rgw_part_etag_to_digest(part->get_etag(), petag);
      hash.Update((const unsigned char *)petag, sizeof(petag));

      // Compression is not supported yet
#if 0
      RGWUploadPartInfo& obj_part = part->info;

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != obj_part.cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << obj_part.cs_info.compression_type << ")" << dendl;
          ret = -ERR_INVALID_PART;
          return ret;
      }

      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }
#endif

      /* friend access:  the placement lives on the part record, and
       * this is the one caller which needs it.  Read here rather than
       * in assemble(), which has no attribute format to read it with. */
      placements.push_back({part->info.num, part->info.shared,
			    part->info.offset, part->info.stored});

      part_sizes.push_back(part->get_size());
      ofs += part->get_size();
      accounted_size += part->get_size();
    }
  } while (truncated);
  hash.Final((unsigned char *)final_etag);

  bufferlist etag_bl;
  append_bl(etag_bl, CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16, [&](auto iter) {
    iter = buf_to_hex(final_etag, iter);
    iter = fmt::format_to(iter, "-{}", part_etags.size());
    return iter;
  });

  attrs[RGW_ATTR_ETAG] = std::move(etag_bl);

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  {

    uint16_t pc = total_parts;
    encode_attr(attrs, RGW_NSFS_ATTR_MULTIPART_PART_COUNT, pc);
    encode_attr(attrs, RGW_NSFS_ATTR_MULTIPART_PART_SIZES, part_sizes);
  }

  NSFSBucket* pb = static_cast<NSFSBucket*>(bucket);
  int staging_fd = shadow->get_dir()->get_fd();

  // assemble parts into a single file
  std::string assembled_name = mpu_strategy()->assembled_name();
  ret = mpu_strategy()->assemble(dpp, driver->get_fs_strategy(),
					    staging_fd, placements,
					    assembled_name,
					    established_stride(dpp, y),
					    y, driver->copy_hints_for(auth_user),
					    identity);
  if (ret < 0) {
    return ret;
  }

  /* write xattrs on assembled file, as its owner:  the assembled file is
   * the identity's, and fsetxattr(2) asks the caller's credentials */
  ret = nsfs::with_identity(dpp, identity, [&]() -> int {
    int afd = openat(staging_fd, assembled_name.c_str(), O_RDWR | O_NOFOLLOW);
    if (afd < 0) {
      return -errno;
    }

    /* owner is already in attrs[RGW_ATTR_ACL] from the generic layer */

    for (auto& [k, v] : attrs) {
      std::string xattr_name = xattr_strategy()->disk_name(k);
      std::string xattr_val = attr_on_disk(xattr_strategy(), k, v);
      if (fsetxattr(afd, xattr_name.c_str(),
		    xattr_val.data(), xattr_val.size(), 0) < 0) {
	const int err = errno;
	::close(afd);
	return -err;
      }
    }
    ::close(afd);
    return 0;
  }, y);
  if (ret < 0) {
    return ret;
  }

  // resolve hierarchical target path
  std::string target_key = target_obj->get_name();
  std::vector<std::unique_ptr<Directory>> dir_chain;
  Directory* leaf_dir;
  std::string leaf_name;
  ret = resolve_key_path(dpp, pb->get_dir(), target_obj->get_key(), target_key,
                         driver->get_reserved_names(), pb->is_internal(),
                         /*create_dirs=*/true, driver->ctx(),
                         dir_chain, leaf_dir, leaf_name,
                         identity, driver->get_access_strategy());
  if (ret < 0) {
    return ret;
  }

  bool versioned = pb->get_info().versioned();
  bool ver_enabled = pb->get_info().versioning_enabled();
  int leaf_fd = leaf_dir->get_fd();

  ret = refuse_directory_name(dpp, leaf_fd, leaf_name.c_str());
  if (ret < 0) {
    return ret;
  }

  /* conditional write checks against existing target object */
  if (if_match || if_nomatch) {
    int existing_fd = ::openat(leaf_fd, leaf_name.c_str(), O_RDONLY | O_NOFOLLOW);
    bool target_exists = (existing_fd >= 0);
    std::string existing_etag;
    if (target_exists) {
      char buf[256];
      std::string xattr_name = xattr_strategy()->disk_name(RGW_ATTR_ETAG);
      ssize_t len = nsfs::fgetxattr(existing_fd, xattr_name.c_str(),
                                buf, sizeof(buf));
      if (len > 0) {
        existing_etag.assign(buf, len);
      }
      ::close(existing_fd);
    }

    if (if_match) {
      if (strcmp(if_match, "*") == 0) {
        if (!target_exists) {
          return -ENOENT;
        }
      } else {
        if (!target_exists) {
          return -ENOENT;
        }
        std::string if_match_str = rgw_string_unquote(if_match);
        if (if_match_str != existing_etag) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
    if (if_nomatch) {
      if (strcmp(if_nomatch, "*") == 0) {
        if (target_exists) {
          return -ERR_PRECONDITION_FAILED;
        }
      } else if (target_exists && !existing_etag.empty()) {
        std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
        if (if_nomatch_str == existing_etag) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  DemoteResult demote;

  if (versioned && leaf_fd >= 0) {
    auto vlock = version_lock_recovering(dpp, driver, xattr_strategy(),
					target_obj->get_bucket()->get_name(),
					leaf_fd, identity);

    int dret = demote_current_version(dpp, driver->get_fs_strategy(),
			   xattr_strategy(),
				      leaf_fd, leaf_name,
				      ver_enabled, demote, identity,
				      driver->get_access_strategy(), y,
				      xattr_fallback());
    if (dret < 0) {
      return dret;
    }

    /* As the authenticated user:  the assembled file leaves the uploader's
     * staging (DECISIONS.md 2026-10-23), as NooBaa's completion moves
     * it.  Under the version lock. */
    ret = renameat_as(dpp, identity, driver->get_access_strategy(),
		      staging_fd, assembled_name.c_str(),
		      leaf_fd, leaf_name.c_str());
  } else {
    ret = renameat_as(dpp, identity, driver->get_access_strategy(),
		      staging_fd, assembled_name.c_str(),
		      leaf_fd, leaf_name.c_str());
  }

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to rename assembled file to "
                      << target_key << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* versioned: compute version ID from published file's stat */
  int vid_ret = 0;
  if (versioned && leaf_fd >= 0) {
    struct statx new_stx;
    if (nsfs::statx(leaf_fd, leaf_name.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_ALL, &new_stx) == 0) {
      std::string new_ver_id = ver_enabled
        ? nsfs_version_id_from_statx(new_stx) : NULL_VERSION_ID;
      target_obj->set_instance(new_ver_id);
      /* as the object's owner, for the same reason as the attributes
       * above, and checked;  the failure is returned after the
       * listing and the staging are settled, not before */
      vid_ret = nsfs::with_identity(dpp, identity, [&]() -> int {
        if (driver->attr_write_fail_injected(pb->get_name())) {
          driver->record_attr_write_fail();
          return -EIO;
        }
        int obj_fd = ::openat(leaf_fd, leaf_name.c_str(), O_RDONLY | O_NOFOLLOW);
        if (obj_fd < 0) {
          return -errno;
        }
        std::string vid_xattr = xattr_strategy()->disk_name(RGW_NSFS_ATTR_VERSION_ID);
        const int r = (::fsetxattr(obj_fd, vid_xattr.c_str(),
                                   new_ver_id.c_str(), new_ver_id.size(),
                                   0) < 0) ? -errno : 0;
        ::close(obj_fd);
        return r;
      }, y);
      if (vid_ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not record the version id on "
          << leaf_name << ": " << cpp_strerror(-vid_ret)
          << ";  a later demote would file it as the null version"
          << dendl;
      }

      auto* bcache = driver->get_bucket_cache();
      std::string obj_name = target_obj->get_key().get_index_key_name();

      if (demote.did_demote) {
        cls_rgw_obj_key old_key;
        old_key.name = obj_name;
        old_key.instance = demote.demoted_ver_id;
        bcache->remove_entry(dpp, pb->get_name(), old_key);

        rgw_bucket_dir_entry dem_bde{};
        fill_demoted_entry(obj_name, demote, pb->default_object_owner(dpp, y),
                           dem_bde);
        bcache->add_entry(dpp, pb->get_name(), dem_bde);
      }

      rgw_bucket_dir_entry new_bde{};
      new_bde.key.name = obj_name;
      new_bde.key.instance = new_ver_id;
      new_bde.ver.pool = 1;
      new_bde.ver.epoch = 1;
      new_bde.exists = true;
      new_bde.meta.category = RGWObjCategory::Main;
      new_bde.meta.size = new_stx.stx_size;
      new_bde.meta.accounted_size = new_stx.stx_size;
      new_bde.meta.mtime = from_statx_timestamp(new_stx.stx_mtime);
      new_bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
      /* complete() computed the real <md5-of-md5s>-<parts> etag above and
       * stored it;  listing the token instead made a completed multipart
       * object HEAD and LIST differently */
      auto eit = attrs.find(RGW_ATTR_ETAG);
      new_bde.meta.etag = (eit != attrs.end())
	? eit->second.to_str() : synthesize_etag(new_stx);
      new_bde.flags = rgw_bucket_dir_entry::FLAG_VER |
                      rgw_bucket_dir_entry::FLAG_CURRENT;
      {
        /* The ACL comes from the attrs this write was handed, for the
         * same reason the etag just above does:  target_obj's
         * in-memory state is the object as it was *before* the write,
         * so a new version has no ACL there and the entry went into
         * the cache with no owner at all.
         *
         * Invisible on a cold cache, because a listing that misses
         * runs fill_cache(), which reads the xattrs and supplies one.
         * It showed up only when a version was written to a bucket
         * that had already been listed -- found by running the
         * conversion corpus twice against one gateway. */
        ACLOwner acl_owner;
        const Attrs& osrc = (attrs.find(RGW_ATTR_ACL) != attrs.end())
          ? attrs : target_obj->get_attrs();
        set_dir_entry_owner(new_bde,
                            xattr_strategy()->object_owner(osrc, acl_owner),
                            acl_owner, pb->default_object_owner(dpp, y));
      }
      bcache->add_entry(dpp, pb->get_name(), new_bde);
    }
  }

  /* non-versioned: update bucket listing cache after rename */
  if (!versioned && leaf_fd >= 0) {
    struct statx new_stx;
    if (nsfs::statx(leaf_fd, leaf_name.c_str(), AT_SYMLINK_NOFOLLOW,
              STATX_ALL, &new_stx) == 0) {
      auto* bcache = driver->get_bucket_cache();
      std::string obj_name = target_obj->get_key().get_index_key_name();
      rgw_bucket_dir_entry bde{};
      bde.key.name = obj_name;
      bde.ver.pool = 1;
      bde.ver.epoch = 1;
      bde.exists = true;
      bde.meta.category = RGWObjCategory::Main;
      bde.meta.size = new_stx.stx_size;
      bde.meta.accounted_size = new_stx.stx_size;
      bde.meta.mtime = from_statx_timestamp(new_stx.stx_mtime);
      bde.meta.storage_class = RGW_STORAGE_CLASS_STANDARD;
      auto eit2 = attrs.find(RGW_ATTR_ETAG);
      bde.meta.etag = (eit2 != attrs.end())
	? eit2->second.to_str() : synthesize_etag(new_stx);
      {
        /* the write's attrs, not the pre-write state;  see the
         * versioned branch above */
        ACLOwner acl_owner;
        const Attrs& osrc = (attrs.find(RGW_ATTR_ACL) != attrs.end())
          ? attrs : target_obj->get_attrs();
        set_dir_entry_owner(bde,
                            xattr_strategy()->object_owner(osrc, acl_owner),
                            acl_owner, pb->default_object_owner(dpp, y));
      }
      bcache->add_entry(dpp, pb->get_name(), bde);
    }
  }

  // remove multipart cache entry and staging directory
  if (auto* mc = driver->get_multipart_cache()) {
    mc->remove({nsfs::mp_cache_bucket(bucket->get_tenant(),
					bucket->get_name()), mp_obj.meta});
  }
  (void) remove_staging(dpp, y);

  // update bucket cache
  target_obj->set_obj_size(ofs);

  return vid_ret;
}

int NSFSMultipartUpload::cleanup_orphaned_parts(const DoutPrefixProvider *dpp,
    CephContext *cct, optional_yield y,
    const rgw_obj& obj,
    std::list<rgw_obj_index_key>& remove_objs,
    prefix_map_t& processed_prefixes)
{
  return -ENOTSUP;
}

int NSFSMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y,
				   rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
{
  std::unique_ptr<rgw::sal::Object> meta_obj;
  int ret;

  if (!rule && !attrs) {
    return 0;
  }

  if (attrs) {
      meta_obj = get_meta_obj();
      int ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
	  << get_key() << dendl;
	return ret;
      }
      *attrs = meta_obj->get_attrs();

      /* and what the format keeps outside them.
       *
       * On a NooBaa tree the meta file has no attributes at all and
       * the create-time metadata is in its content, so without this a
       * completion of an upload NooBaa started produced an object with
       * no content type and no user metadata.  The nsfs format adds
       * nothing:  for it these already ARE attributes on the meta
       * object, and the read above has them. */
      int lret = load(dpp);
      auto* pb = static_cast<NSFSBucket*>(bucket);
      nsfs::Directory* root = staging_root_dir ? staging_root_dir.get()
					       : (pb ? pb->get_dir() : nullptr);
      if ((lret == 0) && root && (root->open(dpp) == 0)) {
	multipart_upload_info ignored;
	Attrs from_format;
	if (mpu_strategy()->upload_info(dpp, root->get_fd(), get_fname(),
					*attrs, ignored, &from_format)) {
	  /* what is already there wins:  an attribute on the meta
	   * object was written after the create request and is the
	   * later word */
	  for (auto& [k, v] : from_format) {
	    attrs->try_emplace(k, v);
	  }
	}
      }
  }

  if (rule) {
    if (mp_obj.upload_info.dest_placement.name.empty()) {
      if (!meta_obj) {
	meta_obj = get_meta_obj();
      }
      ret = meta_obj->get_obj_attrs(y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << " ERROR: could not get meta object for mp upload "
                          << get_key() << dendl;
        return ret;
      }

      /* Asked of the format, which is the only thing that knows where
       * its own record is.  The nsfs record is the encoded attribute
       * the read above already fetched;  NooBaa's is the content of
       * create_object_upload, and a NooBaa tree carries no nsfs
       * attribute at all -- so decoding one here returned -EIO for every
       * upload NooBaa started, and UploadPart, ListParts and
       * CompleteMultipartUpload all failed on it. */
      ret = load(dpp);
      if (ret < 0) {
	return ret;
      }
      auto* pb = static_cast<NSFSBucket*>(bucket);
      nsfs::Directory* root = staging_root_dir ? staging_root_dir.get()
					       : (pb ? pb->get_dir() : nullptr);
      if (!root || ((ret = root->open(dpp)) < 0)) {
	return root ? ret : -EINVAL;
      }
      /* the owner with it:  the nsfs record carries it, and the listing
       * path depends on this -- upload_owner() returns -ENOENT for the
       * nsfs format, so list_multiparts() comes here for it.  A record
       * that carries none, as NooBaa's does not, leaves the bucket's
       * owner, which is the owner of an upload nothing records one
       * for. */
      if (pb) {
	mp_obj.owner = pb->default_object_owner(dpp, y);
      }
      if (!mpu_strategy()->upload_info(dpp, root->get_fd(), get_fname(),
				       meta_obj->get_attrs(),
				       mp_obj.upload_info, nullptr,
				       &mp_obj.owner)) {
	ldpp_dout(dpp, 0) << " ERROR: mp upload " << get_key()
	  << " carries no record this gateway can read, in "
	  << mpu_strategy()->name() << dendl;
	return -EIO;
      }
    }
    *rule = &mp_obj.upload_info.dest_placement;

    if (mp_obj.upload_info.obj_retention_exist) {
      obj_retention = mp_obj.upload_info.obj_retention;
    }
    if (mp_obj.upload_info.obj_legal_hold_exist) {
      obj_legal_hold = mp_obj.upload_info.obj_legal_hold;
    }

    /* no te olvides los cksum */
    cksum_type = mp_obj.upload_info.cksum_type;
    cksum_flags = mp_obj.upload_info.cksum_flags;
  }

  return 0;
}

nsfs::MPUStrategy* NSFSMultipartUpload::mpu_strategy() const
{
  auto* b = static_cast<NSFSBucket*>(bucket);
  return b ? b->mpu_strategy() : driver->get_mpu_strategy();
}

nsfs::XattrStrategy* NSFSMultipartUpload::xattr_strategy() const
{
  auto* b = static_cast<NSFSBucket*>(bucket);
  return b ? b->xattr_strategy() : driver->get_xattr_strategy();
}

nsfs::XattrStrategy* NSFSMultipartUpload::xattr_fallback() const
{
  auto* b = static_cast<NSFSBucket*>(bucket);
  return b ? b->xattr_fallback() : nullptr;
}

nsfs::PathStrategy* NSFSMultipartUpload::path_strategy() const
{
  auto* b = static_cast<NSFSBucket*>(bucket);
  return b ? b->path_strategy() : driver->get_path_strategy();
}

std::string NSFSMultipartUpload::get_fname()
{
  return mpu_strategy()->staging_dir_name(mp_obj.meta);
}

/* The stride this upload runs at, or nothing if it has none.
 *
 * The shared file's name is the authority.  It is created by one
 * linkat whose success publishes both that a stride exists and what it
 * is, so there is no window in which the stride is half-written --
 * which matters because parts arrive concurrently.
 *
 * Part 1's record is consulted only as an index, to learn which name
 * to look for;  the faccessat then confirms it or finds it absent.  A stale or
 * wrong record therefore costs a failed lookup and the per-part
 * layout, not a wrong offset.  That is the difference from reading a
 * recorded flag and trusting it.
 */
std::optional<uint64_t>
NSFSMultipartUpload::established_stride(const DoutPrefixProvider* dpp,
					optional_yield y)
{
  auto* dir = shadow ? shadow->get_dir() : nullptr;
  if (!dir) {
    return std::nullopt;
  }

  /* Part 1's stored length, from the cache if it is there and from the
   * part's record otherwise.
   *
   * The cache first, because under the writeback policy the record has
   * not reached disk yet -- it is written when the entry is evicted.
   * Reading disk alone would find no stored length, conclude no stride
   * was established, and stage every part after the first in its own
   * file:  correct objects, and the layout silently never used on the
   * one filesystem it exists for. */
  uint64_t stored = 0;
  if (auto* mc = driver->get_multipart_cache(); mc) {
    file::listing::MultipartPartInfo pi;
    if (mc->peek_part({nsfs::mp_cache_bucket(bucket->get_tenant(),
						 bucket->get_name()),
			     mp_obj.meta}, 1, pi)) {
      stored = pi.stored;
    }
  }

  if (!stored) {
    nsfs::File pf(mpu_strategy()->part_name(1), dir,
		  driver->ctx());
    if (pf.stat(dpp, y) < 0) {
      return std::nullopt;
    }
    Attrs attrs;
    if (pf.read_attrs(dpp, y, attrs) != 0) {
      return std::nullopt;
    }
    NSFSUploadPartInfo upi;
    if (!decode_attr(attrs, RGW_NSFS_ATTR_MPUPLOAD, upi)) {
      return std::nullopt;
    }
    stored = upi.stored;
  }
  if (!stored) {
    return std::nullopt;
  }

  auto sname = mpu_strategy()->shared_name(stored);
  if (!sname) {
    return std::nullopt;
  }
  if (faccessat(dir->get_fd(), sname->c_str(), F_OK, AT_EACCESS) != 0) {
    return std::nullopt;
  }
  return stored;
}

std::unique_ptr<Writer> NSFSMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str,
				  const rgw_owner& authenticated_user)
{
  load(dpp);

  file::listing::MultipartCacheKey cache_key{
    nsfs::mp_cache_bucket(bucket->get_tenant(), bucket->get_name()),
    mp_obj.meta};

  /* Ask the layout where the bytes go.  The stride is the upload's
   * established uniform part size, which part 1 sets at its completion
   * and which is unset until then;  a layout which cannot place the
   * part without one returns nullopt, and we stage the part in its own
   * file, which is what part_name() names. */
  const std::optional<uint64_t> stride = established_stride(dpp, y);
  auto target = mpu_strategy()->part_target(part_num, stride);
  const uint64_t base_offset = target ? target->offset : 0;
  const bool shared_target = target ? target->shared : false;
  const std::optional<uint64_t> extent = target ? target->extent
						: std::nullopt;

  /* the record is always the part's own file;  the target names where
   * the BYTES go, which is a different file only when it is shared */
  rgw_obj_key part_key(mpu_strategy()->part_name(part_num));
  const std::string shared_name = shared_target ? target->name : std::string();

  return std::make_unique<NSFSMultipartWriter>(dpp, y, shadow.get(), part_key,
                                                driver, owner,
                                                ptail_placement_rule, part_num,
						mpu_strategy(),
						std::move(cache_key),
						authenticated_user,
						base_offset, shared_target,
						shared_name,
						mp_obj.upload_id, extent,
						static_cast<NSFSBucket*>(bucket));
}

int NSFSMultipartWriter::prepare(optional_yield y)
{
  /* The gate first:  a part's record and bytes are written here and in
   * process(), before complete() takes the mutation slot, so a part for a
   * bucket not taken over is rejected before anything lands in its
   * staging (DECISIONS.md 2026-10-21). */
  if (bucket) {
    const int gret = driver->reject_unadopted(dpp, bucket,
					      MutationPurpose::client);
    if (gret < 0) {
      return gret;
    }
    /* and an upload NooBaa staged, which this bucket's adoption has not
     * reached, is waited for:  this writer stages in RGW's layout */
    const std::string& meta = mp_cache_key.upload_meta;
    const int wret = driver->wait_for_conversion(dpp, bucket->get_name(),
      [this, &meta] { return !bucket->upload_awaits_conversion(dpp, meta); });
    if (wret < 0) {
      return wret;
    }
  }

  /* Resolve and pin before anything else this operation does.
   *
   * Here rather than where the SQE is built, because that is one
   * branch of one path:  with the sync engine, or on a small
   * transfer, no window is ever constructed, and a denial placed
   * there simply never fires.  An operation whose identity cannot
   * be resolved must fail wherever it would otherwise have been
   * served as the gateway. */
  {
    const int pret = pin_personality(dpp, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }

  /* the record, always;  it stays empty when the bytes go elsewhere.
   *
   * As the identity, as NooBaa writes a part as its uploader:  part 1's
   * file may become the shared file and so the object, which then
   * belongs to whoever made it.  The multipart cache rewrites a record
   * at eviction as the file's owner. */
  if (identity.active()) {
    part_file->set_identity(identity);
  }
  int ret = part_file->create(dpp, /*existed=*/y, nullptr, /*tempfile=*/false);
  if (ret < 0) {
    return ret;
  }
  ret = part_file->open(dpp);
  if (ret < 0) {
    return ret;
  }

  if (!shared_file) {
    return 0;
  }

  /* File::create opens O_CREAT|O_RDWR and does NOT truncate, which is
   * what a shared file needs:  other parts have already written into
   * it, and this one only fills its own extent.
   *
   * As the identity, as the record above is:  assembly may link this
   * file in as the object. */
  if (identity.active()) {
    shared_file->set_identity(identity);
  }
  ret = shared_file->create(dpp, /*existed=*/y, nullptr, /*tempfile=*/false);
  if (ret < 0) {
    return ret;
  }
  return shared_file->open(dpp);
}

/* Copy what this part has already written into its own file, and
 * continue there.  `written` is the part's length so far, which is
 * what has to move;  the shared file keeps its bytes, and nothing
 * reads them again because the part's record will say it lives
 * elsewhere. */
int NSFSMultipartWriter::divert(uint64_t written)
{
  /* everything queued must reach the shared file before it is copied
   * out of it, and the window is bound to that fd in any case */
  int ret = uring_write ? uring_write->drain() : pending_write.drain();
  if (ret < 0) {
    return ret;
  }
  uring_write.reset();

  if (written > 0) {
    ret = driver->get_fs_strategy()->copy_range(
	dpp, shared_file->get_fd(), base_offset,
	part_file->get_fd(), 0, written, driver->copy_hints_for(auth_user));
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not move part " << part_num
			<< " out of the shared file: " << cpp_strerror(-ret)
			<< dendl;
      return ret;
    }
  }

  ldpp_dout(dpp, 10) << "NSFSMultipartWriter::divert: part " << part_num
		     << " exceeded its extent of " << (extent ? *extent : 0)
		     << " after " << written << " bytes;  moved to its own"
		     << " file" << dendl;

  shared_file->close();
  shared_file.reset();
  base_offset = 0;
  shared_target = false;
  extent.reset();
  diverted = true;
  driver->record_mpu_divert();
  return 0;
}

/* Put this part where its length says it belongs.
 *
 * The file was chosen before the body was read, from the stride part 1
 * established.  A layout whose shared file is named for the size it
 * holds cannot read a part of another size out of it, so the part
 * moves;  a layout which reads a part at the offset in its record
 * returns nothing here and this does not run.  S3 requires every part
 * but the last to be equal, so at most one part per upload moves.
 */
int NSFSMultipartWriter::relocate(optional_yield y)
{
  if (!shared_file || !shared_target || (high_water == 0)) {
    return 0;
  }
  auto tgt = mpu_strat->relocation_target(part_num, high_water, extent);
  if (!tgt || !tgt->shared) {
    return 0;
  }
  if ((tgt->name == shared_file->get_name()) && (tgt->offset == base_offset)) {
    return 0;
  }

  /* everything queued must reach the shared file before it is copied
   * out of it, and the window is bound to that fd in any case */
  int ret = uring_write ? uring_write->drain() : pending_write.drain();
  if (ret < 0) {
    return ret;
  }
  uring_write.reset();

  auto dst = std::make_unique<nsfs::File>(tgt->name, upload_dir.get(),
					  driver->ctx());
  /* as the identity, as prepare() makes the shared file */
  if (identity.active()) {
    dst->set_identity(identity);
  }
  /* create() opens O_CREAT|O_RDWR without truncating, which is what a
   * shared file needs:  another part of this size may already be in
   * it */
  ret = dst->create(dpp, /*existed=*/y, nullptr, /*tempfile=*/false);
  if (ret < 0) {
    return ret;
  }
  ret = dst->open(dpp);
  if (ret < 0) {
    return ret;
  }

  ret = driver->get_fs_strategy()->copy_range(dpp, shared_file->get_fd(),
					      base_offset, dst->get_fd(),
					      tgt->offset, high_water,
					      driver->copy_hints_for(auth_user));
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: could not move part " << part_num
		      << " into " << tgt->name << ": " << cpp_strerror(-ret)
		      << dendl;
    return ret;
  }

  ldpp_dout(dpp, 10) << "NSFSMultipartWriter::relocate: part " << part_num
		     << " is " << high_water << " bytes and the stride is "
		     << (extent ? *extent : 0) << ";  moved to "
		     << tgt->name << " at " << tgt->offset << dendl;

  shared_file->close();
  shared_file = std::move(dst);
  base_offset = tgt->offset;
  return 0;
}

int NSFSMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  /* counted before rebasing, so this is the part's own length:  each
   * length-changing filter rebases the offset it passes down (see
   * RGWPutObj_Compress::process), so what arrives here is a true output
   * offset within the part.  The chain's final flush arrives as an
   * ordinary zero-length call, which costs nothing here. */
  const uint64_t written = high_water;
  high_water = std::max(high_water, offset + data.length());

  /* Reject a write that crosses the extent, before submitting rather
   * than after.
   * With io_uring a check at drain would come too late:  the writes
   * that overran would already have landed in the next part's
   * region. */
  if (shared_file && extent && offset + data.length() > *extent) {
    int ret = divert(written);
    if (ret < 0) {
      return ret;
    }
  }

  /* offset is within the part;  base_offset places the part within the
   * file it shares, and is zero where it does not share one */
  offset += base_offset;

  nsfs::File* df = data_file();
  if (posix_try_use_uring(dpp, yield, /*nsfs=*/true) && df &&
      df->get_fd() >= 0) {
    if (!uring_write) {
      unsigned qd = dpp->get_cct()->_conf.get_val<uint64_t>("rgw_nsfs_put_iodepth");
      bool dio = dpp->get_cct()->_conf.get_val<bool>("rgw_nsfs_direct_io");
      uring_write = std::make_unique<UringWriteWindow>(
          dpp, yield, df->get_fd(), qd, dio, /*nsfs=*/true,
          identity.active() ? &uring_identity : nullptr);
      df->set_sync_on_close(true);
    }
    return uring_write->process(std::move(data), offset);
  }
  unsigned iodepth = posix_sync_clamp_iodepth(
      dpp, dpp->get_cct()->_conf.get_val<uint64_t>("rgw_nsfs_put_iodepth"),
      "rgw_nsfs_put_iodepth");
  bool pipeline = iodepth >= 2;
  return pending_write.process(std::move(data), offset, pipeline,
      [this, df](uint64_t ofs, bufferlist& bl) {
        return df->write(ofs, bl, dpp, null_yield);
      });
}

int NSFSMultipartWriter::complete(
		       size_t accounted_size,
		       const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(rctx.dpp, driver, bucket);
  if (mg.result() < 0) {
    return mg.result();
  }
  int ret = 0;
  if (uring_write) {
    ret = uring_write->drain();
  } else {
    ret = pending_write.drain();
  }
  if (ret < 0) {
    return ret;
  }
  /* The comparison the shared-file layout will gate on:  they agree
   * when nothing between the op layer and here changed the byte count,
   * and diverge under compression or AEAD.  Logged before anything
   * depends on it, so a real run can confirm both polarities. */
  const uint64_t accounted_checked =
      accounted_size + driver->stored_size_delta_injected(upload_id);
  if (high_water != accounted_checked) {
    ldpp_dout(dpp, 10) << "NSFSMultipartWriter::complete: part " << part_num
		       << " stored " << high_water << " for accounted "
		       << accounted_checked
		       << " -- a filter changed the byte count" << dendl;
  }

  NSFSUploadPartInfo info;

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      // test the object is existing
      if (!part_file->exists()) {
        return -ERR_PRECONDITION_FAILED;
      }
    } else {
      Attrs attrs;
      bufferlist bl;
      ret = part_file->read_attrs(rctx.dpp, rctx.y, attrs);
      if (ret < 0) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (!get_attr(attrs, RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      if (strncmp(if_match, bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }

  std::string part_etag = etag;
  if (part_file) {
    int sret = part_file->stat(dpp, /*force=*/true);
    /* the map only:  part_file->write_attrs() below persists it, so
     * recording the attribute here would write the etag twice */
    if (sret == 0 && nsfs_non_md5_etag_into_attrs(dpp,
                                                  part_file->get_stx(),
                                                  attrs)) {
      bufferlist bl;
      if (get_attr(attrs, RGW_ATTR_ETAG, bl)) {
        part_etag.assign(bl.c_str(), bl.length());
      }
    }
  }

  /* The part's true length is known now, and the file it was written
   * into was chosen from the stride.  Where those disagree and the
   * layout cares, the bytes move before the record is written. */
  ret = relocate(rctx.y);
  if (ret < 0) {
    return ret;
  }

  /* Establishment.  Part 1, and only part 1, may set the upload's
   * stride -- which is why no lock is needed and none exists
   * (MPNSFSSerializer::try_lock only checks existence).  Exactly one
   * writer can ever reach this.
   *
   * Part 1's offset is zero, so the file it just wrote already IS a
   * valid prefix of the shared file:  promotion is a link, at no cost
   * whatever the part's size.  Afterwards the two names are one inode,
   * which is why part sizes are read from records and never from a
   * stat -- see NSFSUploadPartInfo. */
  bool promoted = false;
  if (part_num == 1 && !shared_target && high_water > 0 &&
      high_water == accounted_checked) {
    /* does this layout share at all, and under what name?  Asked of the
     * strategy rather than assumed, so the one-file-per-part layout
     * returns no and nothing here fires. */
    auto tgt = mpu_strat->part_target(1, high_water);
    bool aligned = true;
    if (dpp->get_cct()->_conf.get_val<bool>("rgw_nsfs_direct_io")) {
      /* an unaligned stride puts part K's end mid-block, sharing that
       * block with part K+1's start;  two parts doing a
       * read-modify-write on it would lose one of the two updates */
      aligned = (high_water % 4096) == 0;
    }
    if (tgt && tgt->shared && tgt->offset == 0 && aligned) {
      /* One atomic act:  the name carries the stride, so this link
       * both creates the shared file and publishes its value.  EEXIST
       * means a concurrent completion of this same part got there
       * first with the same stride -- same name, same bytes -- so the
       * upload is established either way. */
      /* as the identity, which owns part 1's file:  linking a file one
       * does not own is its owner's */
      const int dfd = upload_dir->get_fd();
      const int lret = nsfs::with_identity(dpp, identity, [&]() -> int {
	return (linkat(dfd, part_file->get_name().c_str(), dfd,
		       tgt->name.c_str(), 0) < 0) ? -errno : 0;
      }, rctx.y);
      if (lret == 0 || lret == -EEXIST) {
	promoted = true;
	ldpp_dout(dpp, 15) << "NSFSMultipartWriter::complete: established"
			   << " stride " << high_water << " for "
			   << mp_cache_key.upload_meta << dendl;
      } else {
	/* nothing is broken by failing to establish:  every part takes
	 * its own file and assembly copies, which is the layout we had */
	ldpp_dout(dpp, 5) << "NSFSMultipartWriter::complete: could not"
			  << " promote part 1: " << cpp_strerror(-lret)
			  << dendl;
      }
    }
  }

  info.num = part_num;
  info.size = accounted_size;
  /* `shared` is what later parts read as "a stride exists";  it is true
   * for a part written into the shared file, and for part 1 once its
   * own file has become that shared file */
  info.shared = shared_target || promoted;
  info.offset = base_offset;
  info.stored = high_water;
  info.etag = part_etag;
  info.cksum = cksum;
  info.mtime = set_mtime;

  auto* mc = driver->get_multipart_cache();
  /* seeded from `info`, not from shared_target:  part 1 is promoted
   * after its bytes are written, so the flag the writer started with
   * says unshared where the record it just wrote says shared.  Seeding
   * from the raw flag put a stale value in front of a correct one,
   * and the cache is consulted first. */
  bool added = mc ? mc->add_part(mp_cache_key,
    {static_cast<uint32_t>(part_num), accounted_size, part_etag, set_mtime,
     cksum, info.shared, info.offset, info.stored}) : false;

  if (!added || mc->policy == file::listing::MultipartCachePolicy::writethrough) {
    bufferlist bl;
    encode(info, bl);
    attrs[RGW_NSFS_ATTR_MPUPLOAD] = bl;

    ret = part_file->write_attrs(rctx.dpp, rctx.y, attrs, /*extra_attrs=*/nullptr);
    if (ret < 0) {
      ldpp_dout(rctx.dpp, 20) << "ERROR: failed writing attrs for " << part_file->get_name() << dendl;
      return ret;
    }

    nsfs::MPUStrategy::PartRecord rec;
    rec.size = info.size;
    rec.stored = info.stored;
    rec.offset = info.offset;
    rec.shared = info.shared;
    rec.etag = info.etag;
    rec.mtime = info.mtime;
    rec.cksum = info.cksum;
    /* as the identity, which owns the part file */
    ret = nsfs::with_identity(rctx.dpp, identity, [&]() -> int {
      return mpu_strat->write_part_record(rctx.dpp, upload_dir->get_fd(),
					  part_file->get_name(), rec);
    }, rctx.y);
    if (ret < 0) {
      return ret;
    }
  }

  if (shared_file) {
    shared_file->set_sync_on_close(false);
    shared_file->close();
  }
  part_file->set_sync_on_close(false);
  ret = part_file->close();
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: failed closing file" << dendl;
    return ret;
  }

  return 0;
}

int NSFSAtomicWriter::prepare(optional_yield y)
{
  /* Resolve and pin before anything else this operation does.
   *
   * Here rather than where the SQE is built, because that is one
   * branch of one path:  with the sync engine, or on a small
   * transfer, no window is ever constructed, and a denial placed
   * there simply never fires.  An operation whose identity cannot
   * be resolved must fail wherever it would otherwise have been
   * served as the gateway. */
  {
    const int pret = pin_personality(dpp, y);
    if ((pret < 0) && (pret != -ENODATA)) {
      return pret;
    }
  }

  /* Reject a key the listing cache cannot hold, before anything is
   * written.
   *
   * A listing is served from the cache, so a key LMDB will not accept
   * is an object that lands on disk and appears in no listing -- the
   * client is told the write succeeded and can never find it again.
   * The ceiling is the library's `mdb_env_get_maxkeysize()`, 511 by
   * default, against S3's 1024;  the cache key is `name \0 instance`,
   * so the name itself has one byte less.
   *
   * Rejecting it is a divergence from S3, and a smaller one than it
   * looks:  nsfs already cannot store a path component longer than
   * NAME_MAX, so many legal S3 keys are rejected already.  What this fixes is
   * that the limit was silent.  It goes away if LMDB is ever built
   * with ceph, where MDB_MAXKEYSIZE is ceph's to raise. */
  if (auto* bc = driver->get_bucket_cache(); bc) {
    const int cap = bc->max_key_size();
    const size_t klen = obj->get_name().size();
    if ((cap > 0) && (klen + 1 > static_cast<size_t>(cap))) {
      ldpp_dout(dpp, 4) << "nsfs: rejecting key of " << klen
	<< " bytes;  the listing cache holds at most " << (cap - 1)
	<< dendl;
      return -ERR_KEY_TOO_LONG;
    }
  }

  /* A key whose components collide with nsfs scaffolding is rejected by
   * resolve_key_path(), which make_ent() below goes through, as every
   * operation's key does. */

  int ret;

  /* before make_ent(), whose walk makes the key's directories as the
   * writer */
  if (identity.active()) {
    obj->set_access(identity, /*ro=*/false);
  }
  /* a key under a file is refused by resolve_key_path(), which
   * make_ent() calls */
  ret = obj->make_ent(ObjectType::FILE);
  if (ret < 0) {
    return ret;
  }
  /* a key whose name holds a directory is refused here, by
   * refuse_directory_name() */
  if (auto* ent = obj->get_fsent(); ent && ent->get_parent() &&
      (ent->get_parent()->get_fd() >= 0)) {
    ret = refuse_directory_name(dpp, ent->get_parent()->get_fd(),
				ent->get_name().c_str());
    if (ret < 0) {
      return ret;
    }
  }
  /* after make_ent, so there is an entry to set it on */
  if (identity.active() && obj->get_fsent()) {
    obj->get_fsent()->set_identity(identity);
  }
  obj->get_obj_attrs(y, dpp);
  obj->close();
  return obj->open(dpp, y, true, true);
}

int NSFSAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  int fd = nsfs_data_file_fd(obj->get_fsent());
  if (posix_try_use_uring(dpp, yield, /*nsfs=*/true) && fd >= 0) {
    if (!uring_write) {
      unsigned qd = dpp->get_cct()->_conf.get_val<uint64_t>("rgw_nsfs_put_iodepth");
      bool dio = dpp->get_cct()->_conf.get_val<bool>("rgw_nsfs_direct_io");
      uring_write = std::make_unique<UringWriteWindow>(
          dpp, yield, fd, qd, dio, /*nsfs=*/true,
          identity.active() ? &uring_identity : nullptr);
      if (obj->get_fsent()) {
        obj->get_fsent()->set_sync_on_close(true);
      }
    }
    return uring_write->process(std::move(data), offset);
  }
  unsigned iodepth = posix_sync_clamp_iodepth(
      dpp, dpp->get_cct()->_conf.get_val<uint64_t>("rgw_nsfs_put_iodepth"),
      "rgw_nsfs_put_iodepth");
  bool pipeline = iodepth >= 2;
  return pending_write.process(std::move(data), offset, pipeline,
      [this](uint64_t ofs, bufferlist& bl) {
        return obj->write(ofs, bl, dpp, null_yield);
      });
}

int NSFSAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags)
{
  /* held off while this bucket's tree is being rewritten;  see
   * NSFSDriver::mutation_enter() */
  NSFSMutationGuard mg(rctx.dpp, driver,
		       static_cast<NSFSBucket*>(obj->get_bucket()));
  if (mg.result() < 0) {
    return mg.result();
  }
  int ret = obj->admit_write(rctx.dpp, rctx.y);
  if (ret < 0) {
    return ret;
  }
  if (uring_write) {
    ret = uring_write->drain();
  } else {
    ret = pending_write.drain();
  }
  if (ret < 0) {
    return ret;
  }

  uint64_t orig_size = 0;
  auto exists = obj->check_exists(dpp);
  if (exists) {
    orig_size = obj->get_size();
  }

  if (if_match) {
    if (strcmp(if_match, "*") == 0) {
      if (!exists) {
	return -ENOENT;
      }
    } else {
      if (!exists) {
	return -ENOENT;
      }
      bufferlist bl;
      if (!get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        return -ERR_PRECONDITION_FAILED;
      }
      std::string if_match_str = rgw_string_unquote(if_match);
      if (if_match_str.compare(0, bl.length(), bl.c_str(), bl.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }
  }
  /* If-None-Match: * is decided by the publish, not here.  `exists` came
   * from a stat taken before the attributes were written, so two writers
   * can both see the name free;  the exclusive link below is what makes
   * exactly one of them win.  This early check is kept only so the
   * uncontended case fails before doing the work. */
  const bool excl_publish = if_nomatch && (strcmp(if_nomatch, "*") == 0);

  if (if_nomatch) {
    if (strcmp(if_nomatch, "*") == 0) {
      if (exists) {
	return -ERR_PRECONDITION_FAILED;
      }
    } else {
      bufferlist bl;
      if (get_attr(obj->get_attrs(), RGW_ATTR_ETAG, bl)) {
        std::string if_nomatch_str = rgw_string_unquote(if_nomatch);
        if (if_nomatch_str.compare(0, bl.length(), bl.c_str(), bl.length()) == 0) {
          return -ERR_PRECONDITION_FAILED;
        }
      }
    }
  }

  /* The requested storage class, stored so it can be returned.
   *
   * nsfs accepted `x-amz-storage-class` and dropped it:  the op layer
   * assigns the header to `s->dest_placement.storage_class` without
   * validating it (rgw_op.cc), and the only check anywhere is in
   * rados's zone-placement lookup, which exists because rados has to
   * resolve a pool.  nsfs needs no pool, so nothing looked and nothing
   * stored it -- a client asking for GLACIER got a 200, HEAD reported
   * no class at all, and the listing said STANDARD.
   *
   * Stored rather than rejected, because interop requires us to hold
   * it:  NooBaa keeps a class per object in `user.storage_class` --
   * including the unmigrated `GLACIER_DA` -- so an adopted tree
   * carries classes we must be able to serve, and rejecting one
   * would leave us able to read a class we could never write.
   *
   * dbstore is the precedent for a driver storing it
   * (`dbstore.cc`, on its meta write):  a plain string, when the rule
   * names one.  The generic layer does the rest -- `rgw_rest.cc` maps
   * this attribute to the `X-Amz-Storage-Class` response header, so
   * HEAD and GET return it without anything further here. */
  if (ptail_placement_rule && !ptail_placement_rule->storage_class.empty() &&
      (attrs.find(RGW_ATTR_STORAGE_CLASS) == attrs.end())) {
    bufferlist scbl;
    scbl.append(ptail_placement_rule->storage_class);
    attrs[RGW_ATTR_STORAGE_CLASS] = std::move(scbl);
  }

  /* A bucket's default retention applies to the version this PUT
   * creates, unless the request set one of its own.
   *
   * The op layer does not do this -- each driver does it in its own
   * write path, and four of them do:  rados (rgw_rados.cc, on
   * PUT_OBJ_CREATE), dbstore, daos and motr.  nsfs did not, so an
   * object uploaded into a lock-configured bucket came back from
   * GetObjectRetention with ObjectLockConfigurationNotFoundError --
   * no retention at all, where every other backend reports the
   * bucket's rule.
   *
   * Found by the conversion corpus's object lock axis.  The suite's
   * existing coverage sets an explicit retention immediately after the
   * PUT, so it never observed what the default gave the object.
   *
   * Same shape as rados:  only when the bucket has a rule, only when
   * the request supplied no retention of its own, and computed from
   * this write's mtime so the date belongs to the object. */
  {
    const auto& lock_info = obj->get_bucket()->get_info();
    if (lock_info.obj_lock_enabled() && lock_info.obj_lock.has_rule() &&
	(attrs.find(RGW_ATTR_OBJECT_RETENTION) == attrs.end())) {
      const ceph::real_time base = ceph::real_clock::is_zero(set_mtime)
	  ? ceph::real_clock::now() : set_mtime;
      RGWObjectRetention retention(lock_info.obj_lock.get_mode(),
				   lock_info.obj_lock.get_lock_until_date(base));
      bufferlist rbl;
      retention.encode(rbl);
      attrs[RGW_ATTR_OBJECT_RETENTION] = std::move(rbl);
    }
  }

  /* owner is already in attrs[RGW_ATTR_ACL] from the generic layer */

  obj->set_attrs(attrs);
  ret = obj->write_attrs(rctx.dpp, rctx.y);
  if (ret < 0) {
    ldpp_dout(rctx.dpp, 20) << "ERROR: NSFSAtomicWriter failed writing attrs for "
                       << obj->get_name() << dendl;
    return ret;
  }

  NSFSBucket *b = static_cast<NSFSBucket*>(obj->get_bucket());
  if (!b) {
    ldpp_dout(dpp, 0) << "ERROR: could not get bucket for " << obj->get_name() << dendl;
    return -EINVAL;
  }

  const auto& binfo = b->get_info();
  bool versioned = binfo.versioned();
  bool ver_enabled = binfo.versioning_enabled();
  DemoteResult demote;

  /* What this write believes about the bucket, which decides whether
   * anything is demoted at all.  The bucket object here is the one
   * the request resolved, so a stale or unread `flags` shows up as a
   * PUT that quietly overwrites the current version instead of
   * keeping it. */
  ldpp_dout(dpp, 10) << "nsfs: publish " << obj->get_name()
    << " versioned=" << versioned << " enabled=" << ver_enabled
    << " flags=0x" << std::hex << binfo.flags << std::dec << dendl;

  /* versioned PUT: demote current version to .versions/ before publishing */
  if (versioned) {
    nsfs::Directory* parent = obj->get_fsent()
      ? obj->get_fsent()->get_parent() : nullptr;
    int parent_fd = -1;

    if (parent) {
      parent_fd = parent->get_fd();
      if (parent_fd < 0) {
        parent->open(dpp);
        parent_fd = parent->get_fd();
      }
    }

    if (parent_fd >= 0) {
      auto vlock = version_lock_recovering(dpp, driver,
					  static_cast<NSFSBucket*>(
					    obj->get_bucket())
					      ->xattr_strategy(),
					  obj->get_bucket()->get_name(),
					  parent_fd, identity);

      int dret = demote_current_version(dpp, driver->get_fs_strategy(),
					obj->xattr_strategy(),
					parent_fd,
					obj->get_fsent()->get_name(),
					ver_enabled, demote, identity,
					driver->get_access_strategy(), rctx.y,
					obj->xattr_fallback());
      if (dret < 0) {
        return dret;
      }

      ret = obj->link_temp_file(rctx.dpp, rctx.y, excl_publish);
    } else {
      ret = obj->link_temp_file(rctx.dpp, rctx.y, excl_publish);
    }
  } else {
    ret = obj->link_temp_file(rctx.dpp, rctx.y, excl_publish);
  }

  if (ret == -EEXIST && excl_publish) {
    /* somebody else took the name between the check above and the link;
     * that is the case this publish exists to catch */
    return -ERR_PRECONDITION_FAILED;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "ERROR: NSFSAtomicWriter failed writing temp file"
                       << dendl;
    return ret;
  }

  if (dpp->get_cct()->_conf->rgw_non_md5_etag) {
    auto it = obj->get_attrs().find(RGW_ATTR_ETAG);
    if (it != obj->get_attrs().end()) {
      attrs[RGW_ATTR_ETAG] = it->second;
    }
  }

  /* versioned PUT: link_temp_file already set the version_id xattr,
   * set_instance, and added the new current cache entry; handle the
   * demoted entry here */
  if (versioned && demote.did_demote) {
    auto* bcache = driver->get_bucket_cache();
    std::string obj_name = obj->get_key().get_index_key_name();

    cls_rgw_obj_key old_key;
    old_key.name = obj_name;
    old_key.instance = demote.demoted_ver_id;
    bcache->remove_entry(rctx.dpp, b->get_name(), old_key);

    rgw_bucket_dir_entry dem_bde{};
    fill_demoted_entry(obj_name, demote,
		       b->default_object_owner(rctx.dpp, rctx.y), dem_bde);
    bcache->add_entry(rctx.dpp, b->get_name(), dem_bde);
  }

  /* The object's cached size is stale the moment the file changes, and
   * this is the point where it stopped being so:  link_temp_file() has
   * just opened and stat()ed the published file, so the on-disk size is
   * already in hand and costs no syscall.  load_obj_state() computes
   * state.size the same way.
   *
   * NSFSMultipartUpload::complete() has always done this for the object
   * it completes into.  An ordinary write left the caller holding a
   * size from before it. */
  if (auto* ent = obj->get_fsent(); ent) {
    obj->set_obj_size(ent->get_stx().stx_size);
    /* the caller's mtime, which RGWPutObj gives its notification as the
     * event's time */
    if (mtime) {
      *mtime = from_statx_timestamp(ent->get_stx().stx_mtime);
    }
  }

  driver->get_quota_handler()->update_stats(b->get_owner(), b->get_key(),
                                            (exists ? 0 : 1), orig_size, accounted_size);

  return 0;
}

int NSFSLifecycle::get_entry(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, const std::string& marker,
                            LCEntry& entry)
{
  return driver->get_user_db()->get_entry(oid, marker, entry);
}

int NSFSLifecycle::get_next_entry(const DoutPrefixProvider* dpp, optional_yield y,
				  const std::string& oid, const std::string& marker,
                                  LCEntry& entry)
{
  return driver->get_user_db()->get_next_entry(oid, marker, entry);
}

int NSFSLifecycle::set_entry(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, const LCEntry& entry)
{
  return driver->get_user_db()->set_entry(oid, entry);
}

int NSFSLifecycle::list_entries(const DoutPrefixProvider* dpp, optional_yield y,
				const std::string& oid, const std::string& marker,
                                uint32_t max_entries, std::vector<LCEntry>& entries)
{
  return driver->get_user_db()->list_entries(oid, marker, max_entries, entries);
}

int NSFSLifecycle::rm_entry(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, const LCEntry& entry)
{
  return driver->get_user_db()->rm_entry(oid, entry);
}

int NSFSLifecycle::get_head(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, LCHead& head)
{
  return driver->get_user_db()->get_head(oid, head);
}

int NSFSLifecycle::put_head(const DoutPrefixProvider* dpp, optional_yield y,
                            const std::string& oid, const LCHead& head)
{
  return driver->get_user_db()->put_head(oid, head);
}

std::unique_ptr<LCSerializer> NSFSLifecycle::get_serializer(const std::string& lock_name,
                                                            const std::string& oid,
                                                            const std::string& cookie)
{
  return std::make_unique<LCNSFSSerializer>(driver, oid, lock_name, cookie);
}

void NSFSDriver::get_features(std::map<std::string, std::string>& features)
{
  /* Each of these is something the driver knows and a client cannot work
   * out without reading a failed operation as a defect.  Meanings, since
   * this is where these names are first defined:
   *
   *   fsio            the driver implements the FSIO interfaces -- rgw_open2
   *                   and the shadow model.  A suite written against them
   *                   is testing nothing on a driver which returns no.
   *                   It describes the DRIVER;  whether a given bucket
   *                   serves them is the next two rows.
   *   extensions      the extension set this build implements, as the
   *                   integer it writes into user.nsfs.extensions.  A
   *                   bucket without that marker is read as NooBaa wrote
   *                   it and the extension-only interfaces are rejected
   *                   on it.
   *   bucket_profile  the profile a bucket created here is marked with,
   *                   rgw_nsfs_bucket_profile.  Every bucket created here
   *                   is marked.
   *   hint_unmarked_buckets
   *                   buckets the `unmarked-bucket` test hint has
   *                   unmarked;  0 on any gateway not under test.
   *   strategy        which FSStrategy was selected by probing at startup.
   *   can_rename      a rename moves the name.  When false, RGWLibFS::rename
   *                   emulates it by copy-and-delete, so the object does not
   *                   keep its inode and history does not move.
   *   rename_enabled  rgw_nsfs_enable_rename;  the same outcome by config
   *                   rather than by backend.
   *   shares_extents  copy_file_range clones rather than copies, measured
   *                   at mount.  When false a copy costs its own size.
   *   key_paths_exclusive
   *                   a name holds a key or its directory form, not both:
   *                   with `a` present, a PUT of `a/` returns
   *                   ERR_KEY_PATH_CONFLICT, and the reverse. */
  features["fsio"] = "true";
  features["key_paths_exclusive"] = "true";
  if (fs_strategy) {
    features["strategy"] = fs_strategy->name();
    features["can_rename"] = fs_strategy->can_rename() ? "true" : "false";
    features["shares_extents"] =
      fs_strategy->shares_extents() ? "true" : "false";
  }
  features["rename_enabled"] =
    ctx()->_conf->rgw_nsfs_enable_rename ? "true" : "false";
  features["extensions"] = std::to_string(nsfs::EXTENSIONS_KNOWN);
  features["hint_unmarked_buckets"] =
    std::to_string(hint_unmarked_buckets.load(std::memory_order_relaxed));
  {
    nsfs::BucketProfile p;
    p.extensions = new_bucket_extensions();
    features["bucket_profile"] = p.name();
  }
  /* Whether requests are served as their own POSIX identity.  A test
   * needs to know this from the gateway rather than infer it:  the
   * credential endpoint resolves a record whether or not the switch
   * is on, so a successful resolve says nothing about it. */
  features["impersonate"] =
    nsfs::impersonation_enabled() ? "true" : "false";
  /* Whether the etag is the mtime-ino string rather than a content
   * digest.  Off by default and reachable only from a gateway started
   * for it, so a test of that path has to be told rather than infer it
   * from the shape of an etag it has no second opinion about. */
  features["non_md5_etag"] =
    ctx()->_conf->rgw_non_md5_etag ? "true" : "false";
  /* Whether lifecycle runs, and how long its day is:  with
   * rgw_lc_debug_interval positive, that many seconds stand for a day,
   * and otherwise a day is a day.  A test that waits for an expiry has
   * to be told both by the gateway rather than by how it was started --
   * s3tests.conf defaults the interval when it says nothing, so a run
   * without fast lifecycle looks like one with it. */
  features["lc_threads"] =
    ctx()->_conf.get_val<bool>("rgw_enable_lc_threads") ? "true" : "false";
  features["lc_debug_interval"] =
    std::to_string(ctx()->_conf->rgw_lc_debug_interval);
  /* Which access model this gateway runs:  whether an S3 grant the
   * filesystem denies takes effect on an object's metadata.  A test
   * cannot infer it -- both models answer the same for every request
   * where S3 and POSIX agree, which is nearly all of them -- so the
   * gateway says which it is and the suite asserts accordingly. */
  /* the model in force, not the one configured:  the `access-model`
   * hint can point the gateway at another for the length of a test,
   * and a suite branching on the configured name would then assert
   * against a model that is not running */
  features["access_model"] = get_access_strategy()->name();
  features["access_model_configured"] = access_strategy->name();
  /* I9's two overrides, as in force -- the `identity-override` hint can
   * change them for a test -- so a run's labels can be checked against
   * the gateway rather than taken from how it was started */
  if (userDB) {
    features["user_identity_override"] =
      userDB->user_identity_override() ? "true" : "false";
    features["role_identity_override"] =
      userDB->role_identity_override() ? "true" : "false";
  }
}

/* Set the profile of a bucket that already exists.
 *
 * The reason setting one at creation is not enough:  a tree that came
 * from NooBaa has buckets nobody wrote an attribute onto.  It is a
 * declared act on a named bucket rather than something inferred on
 * first access, because inference would convert a bucket nobody had
 * decided to adopt. */
int NSFSDriver::set_bucket_profile(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   const std::string& name, uint32_t target,
				   uint32_t* had, std::string* had_profile,
				   nsfs::ConvertProgress* progress)
{
  std::unique_ptr<rgw::sal::Bucket> loaded;
  int ret = load_bucket_internal(dpp, rgw_bucket{"", name}, &loaded, y);
  if (ret < 0) {
    return ret;
  }
  auto& bucket = *static_cast<NSFSBucket*>(loaded.get());

  const nsfs::BucketProfile* was = bucket.get_profile();
  if (had) {
    *had = was ? was->extensions : nsfs::EXTENSIONS_NONE;
  }
  if (had_profile) {
    /* The set alone does not say which profile it was:  base carries no
     * attribute and reports zero. */
    *had_profile = bucket.profile_name();
  }

  return bucket.set_profile(dpp, y, target, nullptr, progress);
}

int NSFSDriver::get_bucket_profile(const DoutPrefixProvider* dpp,
				   optional_yield y,
				   const std::string& name,
				   uint32_t* extensions, std::string* pname,
				   bool* converting)
{
  /* -EBADMSG is unreadable bucket state, and a profile does not come
   * from bucket state -- resolve_profile() has already read the marker
   * by the time the state is loaded.  An operator diagnosing a bucket
   * that fails closed needs this endpoint precisely then, so it is the
   * one caller that does not treat unreadable state as fatal.
   * load_bucket() hands back the bucket whatever it returns. */
  std::unique_ptr<rgw::sal::Bucket> loaded;
  int ret = load_bucket_internal(dpp, rgw_bucket{"", name}, &loaded, y);
  if ((ret < 0) && (ret != -EBADMSG)) {
    return ret;
  }
  auto& bucket = *static_cast<NSFSBucket*>(loaded.get());

  const nsfs::BucketProfile* p = bucket.get_profile();
  if (extensions) {
    *extensions = p ? p->extensions : nsfs::EXTENSIONS_NONE;
  }
  if (pname) {
    *pname = bucket.profile_name();
  }
  if (converting) {
    *converting = bucket.is_converting();
  }
  return 0;
}

void NSFSDriver::register_admin_apis(RGWRESTMgr* mgr)
{
  mgr->register_resource("user", new RGWRESTMgr_User);
  auto* driver_mgr = new RGWRESTMgr;
  driver_mgr->register_resource("hint", new RGWRESTMgr_Driver_Hint);
  mgr->register_resource("driver", driver_mgr);
  auto* nsfs_mgr = new RGWRESTMgr;
  nsfs_mgr->register_resource("profile", new RGWRESTMgr_NSFS_Profile);
  nsfs_mgr->register_resource("identity", new RGWRESTMgr_NSFS_Identity);
  nsfs_mgr->register_resource("credentials",
			      new RGWRESTMgr_NSFS_Credentials);
  nsfs_mgr->register_resource("abandoned", new RGWRESTMgr_NSFS_Abandoned);
  nsfs_mgr->register_resource("bucket", new RGWRESTMgr_NSFS_Bucket);
  mgr->register_resource("nsfs", nsfs_mgr);
}

/* Import the store's identities:  phases 0 and 1 of
 * ACCOUNT_IMPORT.md §6.1, store-wide and first, because every bucket's
 * owner and every policy principal resolves through them.  A command in
 * `command` mode (the `adoption` driver hint), and the first step of
 * run_adoption().
 *
 * Failures report to the log.  A phase-0 failure costs nothing, which
 * is the point of the phase;  a later one leaves RGW state behind and
 * re-running is the recovery, keyed on NooBaa's `_id` (§6.2). */
int NSFSDriver::adopt_identities(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 std::map<std::string, std::string>* report)
{
  auto put_report = [report](const char* k, const std::string& v) {
    if (report) {
      (*report)[k] = v;
    }
  };

  if (!adoption_configured()) {
    put_report("ran", "false");
    put_report("why", "no NooBaa configuration store is configured");
    return -ENOENT;
  }

  const auto& root =
    g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");

  /* ---- phase 0:  preflight.  Nothing is written. ---- */

  nsfs::NooBaaConfigStore store(root);
  nsfs::NooBaaStoreContents contents;
  int ret = store.read(dpp, contents);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: adoption: cannot read " << root << ": "
      << cpp_strerror(-ret) << dendl;
    put_report("ran", "false");
    put_report("error", cpp_strerror(-ret));
    return ret;
  }
  if (!contents.unreadable.empty()) {
    for (const auto& u : contents.unreadable) {
      ldpp_dout(dpp, 0) << "ERROR: adoption: " << u.path << ": " << u.why
	<< dendl;
    }
    put_report("ran", "false");
    put_report("unreadable", std::to_string(contents.unreadable.size()));
    return -EBADMSG;
  }
  if (const auto skew = contents.version_skew(); !skew.empty()) {
    for (const auto& why : skew) {
      ldpp_dout(dpp, 0) << "ERROR: adoption: " << root << ": " << why
	<< dendl;
    }
    put_report("ran", "false");
    put_report("version_skew", std::to_string(skew.size()));
    return -ENOTSUP;
  }
  if (const size_t n = drop_abandoned_identities(dpp, contents); n) {
    put_report("identities_abandoned", std::to_string(n));
  }

  nsfs::NooBaaMasterKeys keys;
  ret = keys.load(dpp, contents.settings, root);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: adoption: no usable master key: "
      << cpp_strerror(-ret) << dendl;
    put_report("ran", "false");
    put_report("error", "master keys");
    return ret;
  }
  std::vector<std::string> failures;
  ret = nsfs::decrypt_store_secrets(dpp, keys, contents, failures);
  if (ret < 0) {
    for (const auto& r : failures) {
      ldpp_dout(dpp, 0) << "ERROR: adoption: " << r << dendl;
    }
    put_report("ran", "false");
    put_report("error", "secrets");
    return ret;
  }

  nsfs::ImportPlan plan;
  ret = nsfs::build_import_plan(dpp, ctx(), contents, plan,
				  get_identity_db());
  put_report("drift", std::to_string(plan.drift.size()));
  put_report("limits_to_raise", std::to_string(plan.raised.size()));
  put_report("roots_widened", std::to_string(plan.widened.size()));
  if (ret < 0) {
    put_report("ran", "false");
    put_report("collisions", std::to_string(plan.collisions.size()));
    return ret;
  }
  std::vector<std::string> collisions;
  ret = nsfs::check_rgw_state(dpp, y, this, plan, collisions);
  if (ret < 0) {
    put_report("ran", "false");
    put_report("collisions", std::to_string(collisions.size()));
    return ret;
  }

  /* ---- phase 1:  identities ---- */

  nsfs::ImportResult ir;
  ret = nsfs::import_identities(dpp, y, this, contents, plan, ir);
  /* the principals an unadopted bucket's policy maps to have changed */
  invalidate_unadopted_mapping();
  put_report("accounts", std::to_string(ir.accounts_created));
  put_report("accounts_present", std::to_string(ir.accounts_skipped));
  put_report("users", std::to_string(ir.users_created));
  put_report("users_present", std::to_string(ir.users_skipped));
  put_report("identity_rows", std::to_string(ir.identity_rows));
  put_report("user_rows", std::to_string(ir.user_rows));
  put_report("limits_raised", std::to_string(ir.limits_raised));
  put_report("anonymous", ir.anonymous_imported ? "true" : "false");
  put_report("anonymous_present", ir.anonymous_present ? "true" : "false");
  put_report("roles", std::to_string(ir.roles_created));
  put_report("role_rights", ir.role_rights);
  put_report("role_policies", std::to_string(ir.role_policies_written));
  put_report("unresolved_names", std::to_string(ir.unresolved_names.size()));
  for (const auto& u : ir.unresolved_names) {
    ldpp_dout(dpp, 0) << "WARNING: adoption:  an identity names a user"
      " nsswitch does not know:  " << u << dendl;
  }
  put_report("unresolved_uids", std::to_string(ir.unresolved_uids.size()));
  for (const auto& u : ir.unresolved_uids) {
    ldpp_dout(dpp, 0) << "WARNING: adoption:  an identity with no group "
      "list runs as a uid nsswitch does not know:  " << u << dendl;
  }
  if (ret < 0) {
    put_report("ran", "partial");
    return ret;
  }

  put_report("ran", "true");
  return 0;
}

/* Adopt the whole store:  its identities, then every bucket, each the
 * way a command adopts one.  What `automatic` runs at startup when
 * there is no io_context or no background tasks, and what `run=true`
 * runs;  a gateway that serves adopts through the queue instead
 * (start_adoption_queue()).  A bucket that fails is reported and the
 * rest continue;  re-running adopts what is left. */
int NSFSDriver::run_adoption(const DoutPrefixProvider* dpp,
			     optional_yield y,
			     std::map<std::string, std::string>* report)
{
  int ret = adopt_identities(dpp, y, report);
  if (ret < 0) {
    return ret;
  }

  const auto& root =
    g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
  nsfs::NooBaaConfigStore store(root);
  nsfs::NooBaaStoreContents contents;
  ret = store.read(dpp, contents);
  if (ret < 0) {
    return ret;
  }
  size_t adopted = 0, failed = 0, abandoned = 0;
  for (const auto& b : contents.buckets) {
    nsfs::AbandonedRecord ab;
    if (userDB->get_abandoned(dpp, b.id, ab) == 0) {
      ++abandoned;
      continue;
    }
    std::map<std::string, std::string> one;
    if (adopt_one_bucket(dpp, y, b.name, &one) < 0) {
      ++failed;
      ldpp_dout(dpp, 0) << "ERROR: adoption: bucket " << b.name << ": "
	<< (one.count("error") ? one["error"] : one["why"]) << dendl;
    } else {
      ++adopted;
    }
  }
  if (report) {
    (*report)["buckets_adopted"] = std::to_string(adopted);
    (*report)["buckets_abandoned"] = std::to_string(abandoned);
    (*report)["buckets_failed"] = std::to_string(failed);
  }
  ldpp_dout(dpp, 1) << "nsfs: adopted " << root << ": " << adopted
    << " buckets, " << failed << " failed" << dendl;
  if (failed) {
    return -EIO;
  }
  return observe_adoption_completion(dpp, y, root, contents, report);
}

/* Abandon one NooBaa record (DECISIONS.md 2026-10-13):  find it by
 * `_id`, work out how far it got, and record that with the operator's
 * reason.  A record fully consumed has nothing to abandon, and nothing
 * is abandoned once adoption is complete. */
int NSFSDriver::abandon_noobaa_record(const DoutPrefixProvider* dpp,
				      optional_yield y,
				      const std::string& noobaa_id,
				      const std::string& reason,
				      nsfs::AbandonedRecord& out)
{
  if (noobaa_id.empty() || reason.empty()) {
    return -EINVAL;
  }
  int ret = load_adoption_completion(dpp);
  if (ret < 0) {
    return ret;
  }
  if (adoption_complete()) {
    ldpp_dout(dpp, 1) << "nsfs: not abandoning " << noobaa_id
      << ":  adoption is complete" << dendl;
    return -EEXIST;
  }
  if (!adoption_configured()) {
    return -ENOENT;
  }

  const auto& root =
    g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
  nsfs::NooBaaConfigStore store(root);
  nsfs::NooBaaStoreContents contents;
  ret = store.read(dpp, contents);
  if (ret < 0) {
    return ret;
  }

  nsfs::AbandonedRecord r;
  r.noobaa_id = noobaa_id;
  r.reason = reason;
  for (const auto& id : contents.identities) {
    if (id.id != noobaa_id) {
      continue;
    }
    std::string owner;
    if (userDB->get_owner_by_noobaa_id(dpp, id.id, owner) == 0) {
      ldpp_dout(dpp, 1) << "nsfs: not abandoning identity " << id.name
	<< ":  it is imported" << dendl;
      return -EEXIST;
    }
    r.kind = "identity";
    r.name = id.name;
    r.stage = "not imported";
    for (const auto& b : contents.buckets) {
      if (b.owner_account == id.id) {
	r.dependent_buckets.push_back(b.name);
      }
    }
  }
  for (const auto& b : contents.buckets) {
    if (b.id != noobaa_id) {
      continue;
    }
    r.kind = "bucket";
    r.name = b.name;
    nsfs::BucketPlacement row;
    if (userDB->get_bucket_placement(dpp, b.name, "", row) < 0) {
      r.stage = "not imported";
      break;
    }
    r.stage = "not converted";
    std::unique_ptr<Bucket> loaded;
    if (load_bucket_internal(dpp, rgw_bucket{"", b.name}, &loaded, y) == 0) {
      auto* nb = static_cast<NSFSBucket*>(loaded.get());
      if (nb->is_converting()) {
	/* an escape hatch for a conversion that will not finish:
	 * allowed, and said, since the bucket stays half-converted */
	r.stage = "partly converted";
      } else if (std::strcmp(nb->profile_name(), "base")) {
	ldpp_dout(dpp, 1) << "nsfs: not abandoning bucket " << b.name
	  << ":  it is adopted" << dendl;
	return -EEXIST;
      }
    }
  }
  if (r.kind.empty()) {
    return -ENOENT;
  }

  r.at = to_iso_8601(ceph::real_clock::now());
  ret = userDB->put_abandoned(dpp, r);
  if (ret < 0) {
    return ret;
  }
  if (r.kind == "bucket") {
    /* an operator acted:  the escape hatch decides what is served */
    std::lock_guard l{adoption_failures_mtx};
    adoption_failures.erase(r.name);
  }
  ldpp_dout(dpp, 0) << "nsfs: abandoned " << r.kind << " " << r.name << " ("
    << r.noobaa_id << "), " << r.stage << ":  " << r.reason << dendl;
  if (!r.dependent_buckets.empty()) {
    ldpp_dout(dpp, 0) << "nsfs: " << r.name << " owns "
      << r.dependent_buckets.size() << " bucket records, which will now fail "
      << "their imports and block completion until each is decided" << dendl;
  }
  /* the record may have been the last thing blocking completion, and
   * under `automatic` nothing else would look again */
  if (auto* q = adoption_queue.load(std::memory_order_acquire); q) {
    q->recheck();
  } else if ((adoption_mode == AdoptionMode::Automatic) && io_context &&
	     background_tasks) {
    /* no queue:  the identity import failed at startup, and this
     * record may be what stopped it */
    if (int r2 = start_adoption_queue(dpp); r2 < 0) {
      ldpp_dout(dpp, 0) << "ERROR: adoption did not start: "
	<< cpp_strerror(-r2) << dendl;
    }
  }
  out = std::move(r);
  return 0;
}

int NSFSDriver::abandoned_bucket(const DoutPrefixProvider* dpp,
				 const std::string& bucket_name,
				 nsfs::AbandonedRecord& out)
{
  if (!nb_bucket_state_strategy) {
    return -ENOENT;
  }
  std::string id;
  int ret = static_cast<nsfs::NooBaaBucketStateStrategy*>(
    nb_bucket_state_strategy.get())->recorded_id(dpp, bucket_name, id);
  if (ret < 0) {
    return ret;
  }
  return userDB->get_abandoned(dpp, id, out);
}

int NSFSDriver::unabandon_noobaa_record(const DoutPrefixProvider* dpp,
					const std::string& noobaa_id)
{
  int ret = load_adoption_completion(dpp);
  if (ret < 0) {
    return ret;
  }
  if (adoption_complete()) {
    return -EEXIST;
  }
  return userDB->remove_abandoned(dpp, noobaa_id);
}

int NSFSDriver::list_abandoned(const DoutPrefixProvider* dpp,
			       std::vector<nsfs::AbandonedRecord>& out)
{
  return userDB->list_abandoned(dpp, out);
}

int NSFSDriver::load_adoption_completion(const DoutPrefixProvider* dpp)
{
  nsfs::AdoptionCompletion c;
  const int ret = userDB->get_adoption_completion(dpp, c);
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    return ret;
  }
  set_completion(std::move(c));
  return 0;
}

void NSFSDriver::set_completion(nsfs::AdoptionCompletion c)
{
  {
    std::lock_guard l{completion_mtx};
    if (completion) {
      return;
    }
    completion = std::move(c);
    completed.store(true, std::memory_order_release);
  }
  /* every bucket is adopted or abandoned, and no policy is served
   * through NooBaa's identities */
  {
    std::lock_guard l{adoption_failures_mtx};
    adoption_failures.clear();
  }
  invalidate_unadopted_mapping();
  if (auto* q = adoption_queue.load(std::memory_order_acquire); q) {
    q->stop();
  }
}

bool NSFSDriver::reread_adoption_complete(const DoutPrefixProvider* dpp)
{
  if (adoption_complete()) {
    return true;
  }
  if (int r = load_adoption_completion(dpp); r < 0) {
    ldpp_dout(dpp, 1) << "nsfs: the completion record did not read: "
      << cpp_strerror(-r) << ";  NooBaa's store is read" << dendl;
  }
  return adoption_complete();
}

/* Whether this pass has consumed NooBaa's store, and if it has, the
 * record that says so.  Observed, not declared:  every identity
 * imported, every bucket record with a row, and every bucket that came
 * from NooBaa adopted -- out of base, and not converting.  A store with
 * a record that would not read is not consumed. */
int NSFSDriver::observe_adoption_completion(
  const DoutPrefixProvider* dpp, optional_yield y, const std::string& root,
  const nsfs::NooBaaStoreContents& contents,
  std::map<std::string, std::string>* report)
{
  auto not_yet = [&](const std::string& why) {
    ldpp_dout(dpp, 1) << "nsfs: adoption not complete:  " << why << dendl;
    if (report) {
      (*report)["complete"] = "false";
      (*report)["why_not_complete"] = why;
    }
    return 0;
  };

  if (!contents.unreadable.empty()) {
    return not_yet(contents.unreadable.front().path + " does not read");
  }
  auto abandoned = [&](const std::string& id) {
    nsfs::AbandonedRecord r;
    return userDB->get_abandoned(dpp, id, r) == 0;
  };
  for (const auto& id : contents.identities) {
    std::string owner;
    if (abandoned(id.id)) {
      continue;
    }
    if (userDB->get_owner_by_noobaa_id(dpp, id.id, owner) < 0) {
      return not_yet("identity " + id.name + " (" + id.id +
		     ") is not imported");
    }
  }
  for (const auto& b : contents.buckets) {
    if (abandoned(b.id)) {
      continue;
    }
    nsfs::BucketPlacement row;
    if (userDB->get_bucket_placement(dpp, b.name, "", row) < 0) {
      return not_yet("bucket " + b.name + " has no placement row");
    }
    std::unique_ptr<Bucket> loaded;
    if (load_bucket_internal(dpp, rgw_bucket{"", b.name}, &loaded, y) < 0) {
      return not_yet("bucket " + b.name + " does not load");
    }
    auto* nb = static_cast<NSFSBucket*>(loaded.get());
    if (nb->is_converting() || !std::strcmp(nb->profile_name(), "base")) {
      return not_yet("bucket " + b.name + " is not converted");
    }
  }

  nsfs::AdoptionCompletion c;
  c.completed_at = to_iso_8601(ceph::real_clock::now());
  c.config_root = root;
  c.identities = contents.identities.size();
  c.buckets = contents.buckets.size();
  c.gateway_version = pretty_version_to_str();
  c.extensions = new_bucket_extensions();
  int ret = userDB->put_adoption_completion(dpp, c);
  if (ret == -EEXIST) {
    /* another gateway got there first;  theirs is the record */
    return load_adoption_completion(dpp);
  }
  if (ret < 0) {
    return ret;
  }
  set_completion(std::move(c));
  ldpp_dout(dpp, 0) << "nsfs: adoption is complete:  " << root << " is "
    << "consumed and is not read again" << dendl;
  if (report) {
    (*report)["complete"] = "true";
    (*report)["completed_at"] = completion->completed_at;
  }
  return 0;
}

/* Import one bucket's NooBaa record:  its owner, versioning, policy,
 * logging and the rest of its configuration, written as nsfs bucket
 * state.  The tree is not touched;  set_profile() leaving base calls
 * this and then rewrites the tree, which is what adopting a bucket is.
 *
 * The bucket half of the import (§6.1), against identities already
 * imported.  A principal may name any identity, so the mapping is
 * built over the whole store -- but only from identities that are
 * imported:  a plan mints an id for one that is not, and a bucket
 * written with that id would name an account that does not exist.  An
 * owner or principal not yet imported is a failure of this bucket,
 * reported by the import, and `identities=true` is the remedy.
 *
 * Returns -ENOENT when the store holds no record of the bucket, or no
 * store is configured. */
int NSFSDriver::import_noobaa_bucket(const DoutPrefixProvider* dpp,
				     optional_yield y,
				     const std::string& bucket_name,
				     std::map<std::string, std::string>* report)
{
  auto put_report = [report](const char* k, const std::string& v) {
    if (report) {
      (*report)[k] = v;
    }
  };

  if (!adoption_configured()) {
    put_report("configured", "false");
    put_report("why", "no NooBaa configuration store is configured");
    return -ENOENT;
  }
  /* once adoption is complete NooBaa's store is not an input, so there
   * is no record to import */
  if (adoption_complete()) {
    put_report("why", "adoption is complete");
    return -ENOENT;
  }
  /* an operator decided this record goes no further;  a profile change
   * that would import it is rejected with it, not quietly converted
   * without its state */
  if (nsfs::AbandonedRecord ab; abandoned_bucket(dpp, bucket_name, ab) == 0) {
    put_report("why", "abandoned:  " + ab.reason);
    return -EEXIST;
  }

  const auto& root =
    g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");

  nsfs::NooBaaConfigStore store(root);
  nsfs::NooBaaStoreContents contents;
  int ret = store.read(dpp, contents);
  if (ret < 0) {
    put_report("configured", "false");
    put_report("error", cpp_strerror(-ret));
    return ret;
  }
  /* X4's version gate, here as well as in adopt_identities():  this is
   * what set_profile() leaving base calls before it rewrites a tree */
  if (const auto skew = contents.version_skew(); !skew.empty()) {
    for (const auto& why : skew) {
      ldpp_dout(dpp, 0) << "ERROR: not importing bucket " << bucket_name
	<< ": " << root << ": " << why << dendl;
    }
    put_report("configured", "false");
    put_report("version_skew", std::to_string(skew.size()));
    return -ENOTSUP;
  }
  /* and its record must read:  one that does not, or that holds a field
   * NooBaa at the store's release does not write (X4's second layer), is
   * not imported, as adopt_identities() refuses the store for it.  So
   * must every connection, since its notifications may name any (I10). */
  {
    const std::string path = root + "/buckets/" + bucket_name + ".json";
    const std::string connections = root + "/connections/";
    for (const auto& u : contents.unreadable) {
      if ((u.path != path) && !u.path.starts_with(connections)) {
	continue;
      }
      ldpp_dout(dpp, 0) << "ERROR: not importing bucket " << bucket_name
	<< ": " << u.path << ": " << u.why << dendl;
      put_report("configured", "false");
      put_report("unreadable", u.why);
      return -EBADMSG;
    }
  }

  nsfs::IdentityMapping mapping;
  ret = imported_identity_mapping(dpp, contents, mapping);
  if (ret < 0) {
    put_report("configured", "false");
    return ret;
  }

  /* a connection's auth is encrypted with the master key, as an access
   * key's secret is (I10) */
  if (!contents.connections.empty()) {
    nsfs::NooBaaMasterKeys keys;
    ret = keys.load(dpp, contents.settings, root);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: not importing bucket " << bucket_name
	<< ": no usable master key: " << cpp_strerror(-ret) << dendl;
      put_report("configured", "false");
      put_report("error", "master keys");
      return ret;
    }
    std::vector<std::string> failures;
    ret = nsfs::decrypt_store_secrets(dpp, keys, contents, failures);
    if (ret < 0) {
      for (const auto& r : failures) {
	ldpp_dout(dpp, 0) << "ERROR: not importing bucket " << bucket_name
	  << ": " << r << dendl;
      }
      put_report("configured", "false");
      put_report("error", "secrets");
      return ret;
    }
  }

  nsfs::BucketImportResult br;
  ret = nsfs::import_one_bucket(dpp, y, this, root, bucket_name, contents,
				mapping, br);
  put_report("configured", (ret == 0) ? "true" : "false");
  put_report("buckets", std::to_string(br.buckets_written));
  put_report("policies", std::to_string(br.policies_rewritten));
  put_report("policies_refused", std::to_string(br.policies_refused.size()));
  put_report("logging", std::to_string(br.logging_carried));
  put_report("topics", std::to_string(br.topics_written));
  put_report("notifications", std::to_string(br.notifications_carried));
  put_report("notifications_reported",
	     std::to_string(br.notifications_reported.size()));
  if (ret < 0) {
    put_report("error", cpp_strerror(-ret));
  }
  return ret;
}

/* An identity an operator abandoned is not imported, and does not stop
 * the others -- a collision is the usual reason to abandon one.
 * Returns how many were left out. */
size_t NSFSDriver::drop_abandoned_identities(
  const DoutPrefixProvider* dpp, nsfs::NooBaaStoreContents& contents)
{
  const size_t before = contents.identities.size();
  std::erase_if(contents.identities, [this, dpp](const auto& id) {
    nsfs::AbandonedRecord r;
    return userDB->get_abandoned(dpp, id.id, r) == 0;
  });
  return before - contents.identities.size();
}

int NSFSDriver::imported_identity_mapping(const DoutPrefixProvider* dpp,
					  const nsfs::NooBaaStoreContents& contents,
					  nsfs::IdentityMapping& out)
{
  /* planned as the identity import was, or an abandoned identity's
   * collision fails every bucket's mapping */
  nsfs::NooBaaStoreContents planned = contents;
  drop_abandoned_identities(dpp, planned);
  nsfs::ImportPlan plan;
  const int ret = nsfs::build_import_plan(dpp, ctx(), planned, plan,
					  get_identity_db());
  if (ret < 0) {
    return ret;
  }
  std::erase_if(plan.entries, [this, dpp](const nsfs::ImportEntry& e) {
    std::string owner;
    return get_identity_db()->get_owner_by_noobaa_id(dpp, e.noobaa_id,
						     owner) < 0;
  });
  out = nsfs::mapping_from(plan);
  return 0;
}

void NSFSDriver::invalidate_unadopted_mapping()
{
  std::lock_guard l(unadopted_mapping_mtx);
  unadopted_mapping.reset();
}

int NSFSDriver::rewrite_unadopted_policy(const DoutPrefixProvider* dpp,
					 const std::string& bucket,
					 Attrs& attrs)
{
  auto i = attrs.find(RGW_ATTR_IAM_POLICY);
  if (i == attrs.end()) {
    return 0;
  }
  std::shared_ptr<const nsfs::IdentityMapping> m;
  {
    std::lock_guard l(unadopted_mapping_mtx);
    m = unadopted_mapping;
  }
  if (!m) {
    const auto& root =
      g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
    nsfs::NooBaaConfigStore store(root);
    nsfs::NooBaaStoreContents contents;
    int ret = store.read(dpp, contents);
    auto built = std::make_shared<nsfs::IdentityMapping>();
    if (ret == 0) {
      ret = imported_identity_mapping(dpp, contents, *built);
    }
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket << ":  NooBaa's "
	"principals cannot be mapped (" << cpp_strerror(-ret) << ");  its "
	"policy is not served" << dendl;
      attrs.erase(i);
      return ret;
    }
    std::lock_guard l(unadopted_mapping_mtx);
    unadopted_mapping = built;
    m = std::move(built);
  }
  std::vector<std::string> unresolved;
  std::string rewritten;
  if (nsfs::rewrite_policy_principals(dpp, i->second.to_str(), *m,
				      rewritten, unresolved) < 0) {
    std::string who;
    for (const auto& u : unresolved) {
      who += " " + u;
    }
    ldpp_dout(dpp, 1) << "nsfs: bucket " << bucket << "'s policy names "
      "principals the import has not written (" << who << " );  it is not "
      "served until it can be rewritten, as the import would not write it"
      << dendl;
    attrs.erase(i);
    return 0;
  }
  bufferlist bl;
  bl.append(rewritten);
  i->second = std::move(bl);
  return 0;
}

/* Adopt one bucket:  import its NooBaa record and bring its tree into
 * the shared profile (DECISIONS.md 2026-10-11).  set_profile() leaving
 * base does both;  this is that call, by name, and the same thing the
 * profile endpoint does with whichever profile it is given. */
int NSFSDriver::adopt_one_bucket(const DoutPrefixProvider* dpp,
				 optional_yield y,
				 const std::string& bucket_name,
				 std::map<std::string, std::string>* report,
				 nsfs::convert_cb_t* cb)
{
  auto put_report = [report](const char* k, const std::string& v) {
    if (report) {
      (*report)[k] = v;
    }
  };

  /* An operator decided this record goes no further, so adopting it by
   * name is rejected as the full pass skips it.  Take the abandonment
   * back first to adopt it. */
  if (nsfs::AbandonedRecord ab; abandoned_bucket(dpp, bucket_name, ab) == 0) {
    put_report("adopted", "false");
    put_report("why", "abandoned:  " + ab.reason);
    return -EEXIST;
  }

  /* A refusal of the whole store is not this bucket's failure:  adoption
   * is in place, so it proceeds only where success is expected, and
   * every bucket stays served as found (DECISIONS.md 2026-10-29). */
  std::string store_why;
  if (int r = store_admits_adoption(dpp, store_why); r < 0) {
    put_report("adopted", "false");
    put_report("why", store_why);
    return r;
  }

  std::unique_ptr<rgw::sal::Bucket> b;
  rgw_bucket rb;
  rb.name = bucket_name;
  int ret = load_bucket_internal(dpp, rb, &b, y);
  if (ret < 0) {
    put_report("adopted", "false");
    put_report("error", cpp_strerror(-ret));
    return ret;
  }

  nsfs::ConvertProgress prog;
  ret = static_cast<NSFSBucket*>(b.get())->set_profile(
    dpp, y, nsfs::EXTENSIONS_SHARED, cb, &prog);
  /* Every adoption path comes here, so a failure is recorded once, the
   * same for the command, the full pass and the queue.  A walk stopped
   * by shutdown has not failed;  its marker stands for the next start. */
  {
    std::lock_guard l{adoption_failures_mtx};
    if ((ret < 0) ? (ret != -ECANCELED) : !prog.complete) {
      adoption_failures[bucket_name] = (ret < 0) ? ret : -EAGAIN;
    } else if (ret == 0) {
      adoption_failures.erase(bucket_name);
    }
  }
  /* Reported as the struct counts them, which is deliberately not one
   * number:  a resumed run has only these and its marker to report
   * from, and a half-finished walk otherwise reads the same as a bucket
   * with history. */
  put_report("objects", std::to_string(prog.objects));
  put_report("directory_objects", std::to_string(prog.directory_objects));
  put_report("versions", std::to_string(prog.versions));
  put_report("uploads", std::to_string(prog.uploads));
  put_report("parts", std::to_string(prog.parts));
  put_report("links_file", std::to_string(prog.links.file));
  put_report("links_directory", std::to_string(prog.links.directory));
  put_report("links_outside", std::to_string(prog.links.outside));
  put_report("links_dangling", std::to_string(prog.links.dangling));
  put_report("links_loop", std::to_string(prog.links.loop));
  put_report("profile_set", prog.profile_set ? "true" : "false");
  put_report("profile", static_cast<NSFSBucket*>(b.get())->profile_name());
  if (ret < 0) {
    put_report("adopted", "false");
    put_report("error", cpp_strerror(-ret));
    return ret;
  }
  /* `complete` and `profile_set` fail apart, and the struct says why:
   * a run that stops halfway has changed the profile and not finished
   * the tree, and the bucket is servable in exactly that state.  So
   * adoption is complete only when nothing foreign is left. */
  put_report("adopted", prog.complete ? "true" : "false");
  if (!prog.complete) {
    put_report("why", "the walk did not finish;  the converting marker stands "
	       "and a later run resumes it");
  }
  return 0;
}

int NSFSDriver::start_adoption_queue(const DoutPrefixProvider* dpp,
				     std::optional<uint64_t> workers)
{
  std::lock_guard start_l{adoption_queue_start_mtx};
  if (adoption_queue_shut) {
    return -ESHUTDOWN;
  }
  if (adoption_queue_owner || adoption_complete()) {
    return 0;
  }
  if (adopt_identities(dpp, null_yield) < 0) {
    return -EAGAIN;
  }
  const auto& root =
    g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
  nsfs::NooBaaConfigStore store(root);
  nsfs::NooBaaStoreContents contents;
  if (int ret = store.read(dpp, contents); ret < 0) {
    return ret;
  }

  auto adopt = [this] (const std::string& bucket,
		       const std::atomic<bool>& stopping) {
    {
      std::unique_lock l{inject_hold_adoption_lock};
      if (inject_hold_adoption_buckets.contains(bucket)) {
	inject_hold_adoption_waiting.insert(bucket);
	/* polled, because the queue sets `stopping` and then waits for
	 * this thread */
	while (inject_hold_adoption_buckets.contains(bucket) && !stopping) {
	  inject_hold_adoption_cond.wait_for(l, std::chrono::milliseconds(100));
	}
	inject_hold_adoption_waiting.erase(bucket);
	if (stopping) {
	  return -ECANCELED;
	}
      }
    }
    /* between objects:  a walk stopped here leaves its marker, and the
     * next start resumes it */
    nsfs::convert_cb_t cb =
      [&stopping] (const DoutPrefixProvider*, const nsfs::ConvertEvent&) {
	return stopping ? -ECANCELED : 0;
      };
    std::map<std::string, std::string> one;
    const int ret = adopt_one_bucket(&driver_prefix, null_yield, bucket,
				     &one, &cb);
    if (ret < 0) {
      ldpp_dout(&driver_prefix, 0) << "ERROR: adoption: bucket " << bucket
	<< ": " << (one.count("error") ? one["error"] : one["why"]) << dendl;
      return ret;
    }
    if (one["adopted"] != "true") {
      return -EAGAIN;
    }
    ldpp_dout(&driver_prefix, 1) << "nsfs: adopted bucket " << bucket << dendl;
    return 0;
  };
  auto drained = [this, root] {
    if (adoption_complete()) {
      return;
    }
    nsfs::NooBaaConfigStore store(root);
    nsfs::NooBaaStoreContents contents;
    if (store.read(&driver_prefix, contents) < 0) {
      return;
    }
    observe_adoption_completion(&driver_prefix, null_yield, root, contents,
				nullptr);
  };

  auto q = std::make_unique<nsfs::AdoptionQueue>(
    *io_context,
    workers.value_or(g_conf().get_val<uint64_t>("rgw_nsfs_adoption_workers")),
    std::move(adopt), std::move(drained));
  size_t queued = 0;
  for (const auto& b : contents.buckets) {
    nsfs::AbandonedRecord ab;
    if (userDB->get_abandoned(dpp, b.id, ab) == 0) {
      continue;
    }
    q->enqueue(b.name);
    ++queued;
  }
  q->start();
  adoption_queue.store(q.get(), std::memory_order_release);
  adoption_queue_owner = std::move(q);
  ldpp_dout(dpp, 1) << "nsfs: adopting " << root << " while serving:  "
    << queued << " buckets queued" << dendl;
  return 0;
}

bool NSFSDriver::awaits_adoption(const DoutPrefixProvider* dpp,
				 NSFSBucket& b)
{
  if (!adoption_queue.load(std::memory_order_acquire) ||
      adoption_complete() || b.extended()) {
    return false;
  }
  if (!b.get_tenant().empty()) {
    return false;
  }
  /* the import writes the row, so a bucket with one has been adopted
   * as far as its profile takes it;  one left base by extensions off
   * stays unmarked */
  nsfs::BucketPlacement row;
  if (userDB->get_bucket_placement(dpp, b.get_name(), "", row) == 0) {
    return false;
  }
  std::string path;
  auto* nb_state = static_cast<nsfs::NooBaaBucketStateStrategy*>(
    nb_bucket_state_strategy.get());
  if (nb_state->recorded_path(dpp, b.get_name(), path) < 0) {
    return false;
  }
  nsfs::AbandonedRecord ab;
  const bool awaits = abandoned_bucket(dpp, b.get_name(), ab) < 0;
  if (awaits) {
    ldpp_dout(dpp, 10) << "nsfs: bucket " << b.get_name()
      << " is not adopted;  promoting it" << dendl;
  }
  return awaits;
}

/* A user's credentials as a request resolves them -- its own row, else
 * its account's -- for the hints that act as a user.  -ENOENT for an
 * empty user, or one with no record, or one whose record cannot be
 * applied. */
static int hint_credentials(const DoutPrefixProvider* dpp, NSFSDriver* driver,
			    const std::string& user, nsfs::Credentials& cred)
{
  rgw_user who;
  who.from_str(user);
  if (who.empty()) {
    return -ENOENT;
  }
  rgw_account_id account;
  {
    std::unique_ptr<User> u = driver->get_user(who);
    if (u->load_user(dpp, null_yield) == 0) {
      account = u->get_info().account_id;
    }
  }
  const int ret = nsfs::resolve_credentials(dpp, *driver->get_identity_db(),
					    who, account, cred, null_yield);
  return (ret == -EPERM) ? -ENOENT : ret;
}

/* `unmarked-bucket`:  make a test's unmarked bucket.  The one place an
 * UnmarkPasskey is made.
 *
 * Narrow on purpose (DECISIONS.md 2026-10-21, the gate):  only a bucket
 * this gateway marked, holding no content, which NooBaa's store has no
 * record of.  So all it can produce is an empty directory with no
 * marker -- never RGW-written objects in a bucket that claims to be
 * NooBaa's, and never a bucket adoption would treat as NooBaa's. */
int NSFSDriver::hint_unmarked_bucket(
    const DoutPrefixProvider* dpp,
    const std::map<std::string, std::string>& params,
    std::map<std::string, std::string>* out)
{
  auto it = params.find("bucket");
  if ((it == params.end()) || it->second.empty()) {
    return -EINVAL;
  }
  const std::string& name = it->second;

  std::unique_ptr<rgw::sal::Bucket> loaded;
  int ret = load_bucket_internal(dpp, rgw_bucket{"", name}, &loaded, null_yield);
  if (ret < 0) {
    return ret;
  }
  auto& b = *static_cast<NSFSBucket*>(loaded.get());
  ret = b.resolve_profile(dpp);
  if (ret < 0) {
    return ret;
  }
  if (!b.extended() || b.is_converting()) {
    ldpp_dout(dpp, 1) << "nsfs: unmarked-bucket:  " << name
      << " is not a marked bucket at rest" << dendl;
    return -ERR_INVALID_BUCKET_STATE;
  }
  if (adoption_configured()) {
    const std::string record =
      g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root") +
      "/buckets/" + name + ".json";
    if (::access(record.c_str(), F_OK) == 0) {
      ldpp_dout(dpp, 1) << "nsfs: unmarked-bucket:  NooBaa holds a record "
	"of " << name << dendl;
      return -ERR_INVALID_BUCKET_STATE;
    }
  }
  ret = b.check_empty(dpp, null_yield);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "nsfs: unmarked-bucket:  " << name
      << " holds content" << dendl;
    return ret;
  }

  ret = b.unmark_extensions(dpp, UnmarkPasskey{});
  if (ret < 0) {
    return ret;
  }
  hint_unmarked_buckets.fetch_add(1, std::memory_order_relaxed);
  ldpp_dout(dpp, 1) << "nsfs: unmarked-bucket:  " << name
    << " is now unmarked, for a test" << dendl;
  if (out) {
    (*out)["bucket"] = name;
    (*out)["profile"] = b.profile_name();
    (*out)["unmarked"] = std::to_string(hint_unmarked_buckets.load());
  }
  return 0;
}

nsfs::CopyHints NSFSDriver::copy_hints_for(const rgw_owner& who)
{
  CopyBundle* b = &daemon_copy_bundle;
  if (any_copy_bundle && ! nsfs::owner_empty(who)) {
    std::lock_guard l{copy_bundles_lock};
    if (auto it = copy_bundles.find(to_string(who));
	it != copy_bundles.end()) {
      b = &it->second;
    }
  }
  return nsfs::CopyHints{b->force_buffered, &b->buffered_bytes};
}

int NSFSDriver::driver_hint(const DoutPrefixProvider* dpp,
			    const std::string& hint,
			    const std::map<std::string, std::string>& params,
			    std::map<std::string, std::string>* out)
{
  ldpp_dout(dpp, 10) << "NSFSDriver::driver_hint: " << hint << dendl;

  if (hint == "inject-fork-race") {
    /* stand in for another instance which forks a shadow first, so that
     * the exclusive fork and the join which follows it can be exercised
     * deterministically from one process */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_fork_race = (it->second == "true");
    if (out) {
      (*out)["enabled"] = inject_fork_race ? "true" : "false";
    }
    return 0;
  }

  /* `adoption` -- what the mode is, and a way to run it.
   *
   * The import has no endpoint on purpose, so this is how the suite
   * drives it.  `run=true` runs it;  without that the hint only
   * reports, which is what a test asserting the default needs. */
  if (hint == "adoption") {
    if (out) {
      (*out)["configured"] = adoption_configured() ? "true" : "false";
      /* what this gateway serves, so a caller can tell whether it is the
       * gateway over the tree it means before adopting through it */
      (*out)["base_path"] = default_bucket_location();
      (*out)["config_root"] =
	g_conf().get_val<std::string>("rgw_nsfs_noobaa_config_root");
      (*out)["mode"] =
	adoption_mode == AdoptionMode::Automatic ? "automatic" : "command";
    }
    auto it = params.find("mode");
    if (it != params.end()) {
      /* Set the mode, for a test that cannot restart the gateway.  A
       * deployment sets it in configuration and this is the same
       * assignment, reachable only through a hint. */
      if (it->second == "automatic") {
	adoption_mode = AdoptionMode::Automatic;
      } else if (it->second == "command") {
	adoption_mode = AdoptionMode::Command;
      } else {
	return -EINVAL;
      }
      if (out) {
	(*out)["mode"] = it->second;
      }
    }
    /* Re-read, so that a record another gateway wrote is seen before a
     * command acts;  once it is there every command is rejected, since
     * NooBaa's store is no longer an input. */
    if (int r = load_adoption_completion(dpp); r < 0) {
      return r;
    }
    const auto done = adoption_completion();
    if (out && done) {
      (*out)["complete"] = "true";
      (*out)["completed_at"] = done->completed_at;
      (*out)["completed_from"] = done->config_root;
      (*out)["completed_identities"] = std::to_string(done->identities);
      (*out)["completed_buckets"] = std::to_string(done->buckets);
      (*out)["completed_by"] = done->gateway_version;
    } else if (out) {
      (*out)["complete"] = "false";
    }
    /* `wait_ms=N` sets the quantum, so a test can reach a wait that
     * outlasts it and one that does not */
    if (auto w = params.find("wait_ms"); w != params.end()) {
      auto ms = ceph::parse<uint64_t>(w->second);
      if (!ms) {
	return -EINVAL;
      }
      adoption_wait_ms = *ms;
    }
    if (out) {
      (*out)["wait_ms"] = std::to_string(adoption_wait_ms.load());
    }
    /* `mode=automatic` in a gateway that serves does what startup in
     * that mode does:  it starts the queue.  A test sets its holds
     * first, so no worker takes a bucket before the test can hold it.
     * `workers=N` overrides rgw_nsfs_adoption_workers. */
    if ((it != params.end()) && (it->second == "automatic") && !done &&
	adoption_configured() && io_context && background_tasks) {
      std::optional<uint64_t> workers;
      if (auto w = params.find("workers"); w != params.end()) {
	workers = ceph::parse<uint64_t>(w->second);
	if (!workers || (*workers == 0)) {
	  return -EINVAL;
	}
      }
      if (int r = start_adoption_queue(dpp, workers); r < 0) {
	return r;
      }
    }
    /* `automatic` while serving:  the queue's counts since startup */
    if (auto* q = adoption_queue.load(std::memory_order_acquire);
	out && q) {
      const auto s = q->stats();
      (*out)["queue_queued"] = std::to_string(s.queued);
      (*out)["queue_running"] = std::to_string(s.running);
      (*out)["queue_adopted"] = std::to_string(s.adopted);
      (*out)["queue_failed"] = std::to_string(s.failed);
      (*out)["queue_promotions"] = std::to_string(s.promotions);
      (*out)["queue_waits_timed_out"] = std::to_string(s.waits_timed_out);
    }
    if (done) {
      for (const char* cmd : {"identities", "bucket", "run"}) {
	if (params.count(cmd)) {
	  if (out) {
	    (*out)["why"] = "adoption is complete";
	  }
	  /* 409:  EALREADY has no HTTP mapping and would be a 500 */
	  return -EEXIST;
	}
      }
    }
    /* The commands (DECISIONS.md 2026-10-11).  `identities=true`
     * imports the store's identities;  `bucket=<name>` adopts that
     * bucket -- writes its state from NooBaa's configuration store, marks
     * it, converts its tree -- against identities already imported;
     * `run=true` runs what `automatic` runs at startup -- the
     * identities, then every bucket. */
    auto ids = params.find("identities");
    if ((ids != params.end()) && (ids->second == "true")) {
      return adopt_identities(dpp, null_yield, out);
    }
    auto one = params.find("bucket");
    if (one != params.end()) {
      return adopt_one_bucket(dpp, null_yield, one->second, out);
    }
    auto run = params.find("run");
    if ((run != params.end()) && (run->second == "true")) {
      return run_adoption(dpp, null_yield, out);
    }
    return 0;
  }

  if (hint == "mpu-state") {
    /* Read-back, the half that lets a test check a result.
     *
     * `layout` is always reported, because the staging layout is probed
     * from the filesystem and a test cannot know which one is live --
     * a reflink-capable root selects per-part, where no stride is ever
     * established and asserting one would fail everywhere but a root
     * without reflink.
     *
     * `stride` needs bucket, key and upload, and is the fact that
     * cannot be seen through S3:  whether part 1 met the establishment
     * conditions.  Empty when it did not. */
    if (out) {
      (*out)["layout"] = mpu_strategy ? mpu_strategy->name() : "";
      (*out)["diverts"] = std::to_string(mpu_diverts_seen());
    }

    auto b_it = params.find("bucket");
    auto k_it = params.find("key");
    auto u_it = params.find("upload");
    if (b_it == params.end() || k_it == params.end() ||
	u_it == params.end()) {
      return 0;
    }

    std::unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket rb;
    rb.name = b_it->second;
    int ret = load_bucket_internal(dpp, rb, &bucket, null_yield);
    if (ret < 0) {
      return ret;
    }
    auto upload = bucket->get_multipart_upload(k_it->second, rgw::sal::no_authenticated_user,
					       u_it->second);
    auto* nu = static_cast<NSFSMultipartUpload*>(upload.get());
    ret = nu->load_for_hint(dpp);
    if (ret < 0) {
      return ret;
    }
    if (auto stride = nu->established_stride(dpp, null_yield); stride) {
      if (out) {
	(*out)["stride"] = std::to_string(*stride);
      }
    } else if (out) {
      (*out)["stride"] = "";
    }
    return 0;
  }

  if (hint == "inject-stored-size-delta") {
    /* make the multipart writer see a stored byte count which differs
     * from the accounted one, as it would if a length-changing filter
     * were in the chain.  Nothing else changes -- the part record still
     * describes the file truthfully -- so what this exercises is the
     * branch, not a corrupted tree.
     *
     * Keyed by upload:  parts upload concurrently and so do tests, and
     * a driver-wide delta suppresses stride establishment for every
     * upload in flight, not just the one under test. */
    auto it = params.find("bytes");
    auto up = params.find("upload");
    if (it == params.end() || up == params.end()) {
      return -EINVAL;
    }
    set_stored_size_delta(up->second, std::stoll(it->second));
    if (out) {
      (*out)["bytes"] = it->second;
      (*out)["upload"] = up->second;
    }
    return 0;
  }

  if (hint == "inject-rename-fail-after") {
    /* fail a rename after N version entries have moved.  Without this the
     * rollback path only ever runs in production:  every test rename either
     * succeeds outright or fails before it has moved anything. */
    auto it = params.find("count");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_rename_fail_after = std::stoi(it->second);

    /* abandon=versions|leaf gives up without rolling back, leaving the
     * intent record -- the state a crash leaves and the only state the
     * recovery path can be tested against.  "versions" stops after count
     * entries have moved, "leaf" after the leaf has landed. */
    auto ab = params.find("abandon");
    inject_rename_abandon = 0;
    if (ab != params.end()) {
      if (ab->second == "versions") {
	inject_rename_abandon = 1;
      } else if (ab->second == "leaf") {
	inject_rename_abandon = 2;
      }
    }
    if (out) {
      (*out)["count"] = std::to_string(inject_rename_fail_after);
      (*out)["abandon"] = std::to_string(inject_rename_abandon);
    }
    return 0;
  }

  if ((hint == "inject-fsio-attrs-fail") ||
      (hint == "inject-fsio-link-fail") ||
      (hint == "inject-fsio-skip-cleanup")) {
    /* Make a shadow build fail after the inode exists.  Without these the
     * cleanup paths only ever run in production:  every test create either
     * succeeds outright or fails before anything was made. */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    const bool on = (it->second == "true");
    if (hint == "inject-fsio-attrs-fail") {
      inject_fsio_attrs_fail = on;
    } else if (hint == "inject-fsio-link-fail") {
      inject_fsio_link_fail = on;
    } else {
      inject_fsio_skip_cleanup = on;
    }
    if (out) {
      (*out)["enabled"] = on ? "true" : "false";
    }
    return 0;
  }

  if (hint == "fs-features") {
    /* the same set GET /admin/features reports, for a test which holds the
     * driver rather than a socket */
    if (! out) {
      return -EINVAL;
    }
    get_features(*out);
    return out->empty() ? -EINVAL : 0;
  }

  if (hint == "inject-buffered-copy") {
    /* make FSStrategy::copy_range skip copy_file_range and take the
     * buffered path.  On XFS copy_file_range always succeeds, so the
     * fallback -- the only path GPFS ever takes, since GPFS returns
     * EOPNOTSUPP at every granularity -- would otherwise never run
     * under test on a development filesystem */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    const bool on = (it->second == "true");
    /* `user=` arms that user's bundle alone, so a test's copies
     * are the only ones forced and counted;  without it, the daemon's */
    CopyBundle* b = &daemon_copy_bundle;
    if (auto u = params.find("user"); u != params.end()) {
      std::lock_guard l{copy_bundles_lock};
      b = &copy_bundles.try_emplace(u->second).first->second;
      any_copy_bundle = true;
    }
    b->force_buffered = on;
    if (out) {
      (*out)["enabled"] = on ? "true" : "false";
      /* bytes the fallback has copied for this bundle since this hint
       * was last called, so a test can prove which path ran rather
       * than assume it */
      (*out)["buffered_bytes"] = std::to_string(b->buffered_bytes.exchange(0));
    }
    return 0;
  }

  if (hint == "inject-no-chown") {
    /* behave as a daemon without CAP_CHOWN, so a test can assert that
     * conversion fails rather than creating a file owned by the
     * gateway.  The capability cannot be dropped in a test process --
     * a thread that drops it cannot regain it, which would poison
     * every later test -- so the condition is injected instead. */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_no_chown = (it->second == "true");
    if (out) {
      (*out)["enabled"] = inject_no_chown ? "true" : "false";
    }
    return 0;
  }

  if (hint == "inject-create-failure") {
    /* fail a client's bucket creation after its directory is made, or a
     * placement after its directory is marked, so the rollback is
     * reachable from a test */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_create_failure = (it->second == "true");
    if (out) {
      (*out)["enabled"] = inject_create_failure ? "true" : "false";
    }
    return 0;
  }

  if (hint == "inject-attr-read-fail") {
    /* fail an object's attribute read after its open, in one bucket,
     * so a test can assert what a delete does when the object lock
     * cannot be read */
    auto it = params.find("enable");
    auto b_it = params.find("bucket");
    if ((it == params.end()) || (b_it == params.end()) ||
	b_it->second.empty()) {
      return -EINVAL;
    }
    const bool on = (it->second == "true");
    {
      std::lock_guard l{inject_attr_read_fail_lock};
      if (on) {
	inject_attr_read_fail_buckets.insert(b_it->second);
      } else {
	inject_attr_read_fail_buckets.erase(b_it->second);
      }
      inject_attr_read_fail_armed = !inject_attr_read_fail_buckets.empty();
    }
    if (out) {
      (*out)["enabled"] = on ? "true" : "false";
      (*out)["bucket"] = b_it->second;
      (*out)["fired"] = std::to_string(attr_read_fails_injected.load());
    }
    return 0;
  }

  if (hint == "inject-attr-write-fail") {
    /* fail the attribute writes made after an object exists -- a
     * copy's version id, a delete marker's attributes, a multipart
     * completion's version id -- in one bucket, so a test can assert
     * each returns the failure */
    auto it = params.find("enable");
    auto b_it = params.find("bucket");
    if ((it == params.end()) || (b_it == params.end()) ||
	b_it->second.empty()) {
      return -EINVAL;
    }
    const bool on = (it->second == "true");
    {
      std::lock_guard l{inject_attr_write_fail_lock};
      if (on) {
	inject_attr_write_fail_buckets.insert(b_it->second);
      } else {
	inject_attr_write_fail_buckets.erase(b_it->second);
      }
      inject_attr_write_fail_armed = !inject_attr_write_fail_buckets.empty();
    }
    if (out) {
      (*out)["enabled"] = on ? "true" : "false";
      (*out)["bucket"] = b_it->second;
      (*out)["fired"] = std::to_string(attr_write_fails_injected.load());
    }
    return 0;
  }

  if (hint == "unmarked-bucket") {
    return hint_unmarked_bucket(dpp, params, out);
  }

  if (hint == "inject-lift-adoption-gate") {
    /* lift the gate on a bucket not taken over, in one bucket, so a test
     * can show a rejection is the gate's:  the same write succeeds with
     * it lifted.  `rejected` counts every write the gate turned away. */
    auto it = params.find("enable");
    auto b_it = params.find("bucket");
    if ((it == params.end()) || (b_it == params.end()) ||
	b_it->second.empty()) {
      return -EINVAL;
    }
    const bool on = (it->second == "true");
    {
      std::lock_guard l{inject_lift_adoption_gate_lock};
      if (on) {
	inject_lift_adoption_gate_buckets.insert(b_it->second);
      } else {
	inject_lift_adoption_gate_buckets.erase(b_it->second);
      }
      inject_lift_adoption_gate_armed =
	  !inject_lift_adoption_gate_buckets.empty();
    }
    if (out) {
      (*out)["enabled"] = on ? "true" : "false";
      (*out)["bucket"] = b_it->second;
      (*out)["rejected"] = std::to_string(unadopted_writes_rejected.load());
    }
    return 0;
  }

  if (hint == "inject-hold-adoption") {
    /* keep one bucket not adopted while the queue runs, so a test can
     * reach a wait that outlasts its quantum, and release it during a
     * wait that does not.  `waiting` is true once a worker has taken
     * the bucket and is held on it. */
    auto it = params.find("enable");
    auto b_it = params.find("bucket");
    if ((it == params.end()) || (b_it == params.end()) ||
	b_it->second.empty()) {
      return -EINVAL;
    }
    const bool on = (it->second == "true");
    bool waiting;
    {
      std::lock_guard l{inject_hold_adoption_lock};
      if (on) {
	inject_hold_adoption_buckets.insert(b_it->second);
      } else {
	inject_hold_adoption_buckets.erase(b_it->second);
      }
      waiting = inject_hold_adoption_waiting.contains(b_it->second);
    }
    inject_hold_adoption_cond.notify_all();
    if (out) {
      (*out)["enabled"] = on ? "true" : "false";
      (*out)["bucket"] = b_it->second;
      (*out)["waiting"] = waiting ? "true" : "false";
    }
    return 0;
  }

  if (hint == "symlink") {
    /* A symbolic link in a bucket, for the suite's tests of what a link is
     * (DECISIONS.md 2026-10-23).  A filesystem client makes links and S3
     * cannot, so the gateway makes one, as `unplaced-directory` makes a
     * directory.
     *
     *   op=create bucket= key= target=   fails if the key's name exists
     *   op=stat bucket= key=             kind=link|file|directory|other|none,
     *                                    and the entry's uid where it exists
     *
     * The key's directories must exist.  as=<user>:  made as that user,
     * as a filesystem client of that identity would make it, so that
     * identity may remove it under any access model.  Both report the
     * bucket's real path as `bucket_path`, from which a test makes an
     * absolute target. */
    auto op = params.find("op");
    auto bk = params.find("bucket");
    auto ky = params.find("key");
    if ((op == params.end()) || (bk == params.end()) ||
	(ky == params.end()) || bk->second.empty() || ky->second.empty()) {
      return -EINVAL;
    }
    const std::string& key = ky->second;
    if ((key.front() == '/') || (key.back() == '/') ||
	(("/" + key + "/").find("/../") != std::string::npos)) {
      return -EINVAL;
    }
    std::optional<nsfs::Credentials> as;
    if (auto a = params.find("as"); a != params.end()) {
      nsfs::Credentials cred;
      const int r = hint_credentials(dpp, this, a->second, cred);
      if (r < 0) {
	return r;
      }
      as = cred;
    }

    std::unique_ptr<rgw::sal::Bucket> loaded;
    int ret = load_bucket_internal(dpp, rgw_bucket{"", bk->second}, &loaded,
			  null_yield);
    if (ret < 0) {
      return ret;
    }
    auto* bdir = static_cast<NSFSBucket*>(loaded.get())->get_dir();
    ret = bdir->open(dpp);
    if (ret < 0) {
      return ret;
    }
    const auto slash = key.rfind('/');
    const std::string parent = (slash == std::string::npos)
      ? std::string(".") : key.substr(0, slash);
    const std::string leaf = (slash == std::string::npos)
      ? key : key.substr(slash + 1);
    const int pfd = ::openat(bdir->get_fd(), parent.c_str(),
			     O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (pfd < 0) {
      return -errno;
    }
    auto close_pfd = make_scope_guard([pfd] { ::close(pfd); });

    if (op->second == "create") {
      auto tg = params.find("target");
      if ((tg == params.end()) || tg->second.empty()) {
	return -EINVAL;
      }
      auto make = [&]() -> int {
	return (::symlinkat(tg->second.c_str(), pfd, leaf.c_str()) < 0)
	  ? -errno : 0;
      };
      ret = as ? nsfs::run_as(dpp, *as, make, null_yield) : make();
      if (ret < 0) {
	return ret;
      }
    } else if (op->second != "stat") {
      return -EINVAL;
    }

    const char* kind = "none";
    std::optional<uid_t> uid;
    struct statx stx;
    if (nsfs::statx(pfd, leaf.c_str(), AT_SYMLINK_NOFOLLOW,
		    STATX_MODE | STATX_UID, &stx) == 0) {
      kind = S_ISLNK(stx.stx_mode) ? "link"
	: S_ISDIR(stx.stx_mode) ? "directory"
	: S_ISREG(stx.stx_mode) ? "file" : "other";
      uid = stx.stx_uid;
    } else if (errno != ENOENT) {
      return -errno;
    }
    if (out) {
      (*out)["kind"] = kind;
      if (uid) {
	(*out)["uid"] = std::to_string(*uid);
      }
      (*out)["bucket_path"] = fd_path(bdir->get_fd());
    }
    return 0;
  }

  if (hint == "unplaced-directory") {
    /* A directory under the base path that nothing has placed, for the
     * placement endpoint's tests:  the gateway makes it, so a test needs
     * no access to the gateway's filesystem, and the gateway owns it, as
     * it owns the base path.
     *
     *   op=create name= [mode=] [file= content=]   rejected if it exists
     *   op=mode name= mode=
     *   op=remove name=   its regular files and empty directories, then it
     *
     * `mode` and `remove` reject a directory carrying nsfs state, so
     * neither touches a bucket's.
     *
     * as=<user>:  the directory is that user's, as a NooBaa bucket's is
     * its account's, changed and emptied as the user on the helper
     * pool.  The base path is the gateway's, where no identity may
     * create, so the gateway makes `<name>` at 0711 and `<name>/d` in
     * it, and gives `d` to the user as it gives a bucket directory to
     * its owner;  that is the path reported, and remove takes both. */
    auto op = params.find("op");
    auto nm = params.find("name");
    if ((op == params.end()) || (nm == params.end())) {
      return -EINVAL;
    }
    const std::string& name = nm->second;
    if (name.empty() || (name == ".") || (name == "..") ||
	(name.find('/') != std::string::npos)) {
      return -EINVAL;
    }
    std::optional<nsfs::Credentials> as;
    if (auto a = params.find("as"); a != params.end()) {
      nsfs::Credentials cred;
      const int r = hint_credentials(dpp, this, a->second, cred);
      if (r < 0) {
	return r;
      }
      as = cred;
    }
    const std::string parent = default_bucket_location() + "/" + name;
    const std::string path = as ? (parent + "/d") : parent;
    auto mode_of = [&params](mode_t& m) -> int {
      auto it = params.find("mode");
      if (it == params.end()) {
	return -ENOENT;
      }
      try {
	m = static_cast<mode_t>(std::stoul(it->second, nullptr, 8)) & 07777;
      } catch (const std::exception&) {
	return -EINVAL;
      }
      return 0;
    };
    /* `fn` as the `as` user, or as the gateway */
    auto acting = [&](const nsfs::ImpersonatedFn& fn) -> int {
      return as ? nsfs::run_as(dpp, *as, fn, null_yield) : fn();
    };

    if (op->second == "create") {
      mode_t m = 0755;
      int r = mode_of(m);
      if ((r < 0) && (r != -ENOENT)) {
	return r;
      }
      if (as) {
	if (::mkdir(parent.c_str(), 0711) < 0) {
	  return -errno;
	}
	if (::chmod(parent.c_str(), 0711) < 0) {
	  return -errno;
	}
      }
      auto f = params.find("file");
      if ((f != params.end()) &&
	  (f->second.empty() || (f->second.find('/') != std::string::npos))) {
	return -EINVAL;
      }
    }

    /* The user acts relative to a descriptor the gateway opens, as the
     * driver does:  the user need not be able to search the path down
     * to the base path. */
    const std::string dir = as ? parent : default_bucket_location();
    const char* leaf = as ? "d" : name.c_str();
    const int pfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (pfd < 0) {
      return -errno;
    }
    auto close_pfd = make_scope_guard([pfd] { ::close(pfd); });

    if (op->second == "create") {
      mode_t m = 0755;
      (void) mode_of(m);
      auto f = params.find("file");
      auto c = params.find("content");
      if (as) {
	if (::mkdirat(pfd, leaf, m) < 0) {
	  return -errno;
	}
	const int r = chown_raised(dpp, pfd, leaf, as->uid, as->gid);
	if (r < 0) {
	  return r;
	}
      }
      int r = acting([&]() -> int {
	if (!as && (::mkdirat(pfd, leaf, m) < 0)) {
	  return -errno;
	}
	/* not subject to the umask */
	if (::fchmodat(pfd, leaf, m, 0) < 0) {
	  return -errno;
	}
	if (f == params.end()) {
	  return 0;
	}
	const std::string content = (c == params.end()) ? "" : c->second;
	const std::string fname = std::string(leaf) + "/" + f->second;
	int fd = ::openat(pfd, fname.c_str(),
			  O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
	if (fd < 0) {
	  return -errno;
	}
	const ssize_t n = ::write(fd, content.data(), content.size());
	const int werr = (n < 0) ? errno : 0;
	::close(fd);
	if (werr || (static_cast<size_t>(n) != content.size())) {
	  return werr ? -werr : -EIO;
	}
	return 0;
      });
      if (r < 0) {
	return r;
      }
    } else if (op->second == "mode") {
      mode_t m = 0;
      int r = mode_of(m);
      if (r < 0) {
	return (r == -ENOENT) ? -EINVAL : r;
      }
      r = carries_nsfs_state(path);
      if (r != 0) {
	return (r > 0) ? -EEXIST : r;
      }
      r = acting([&]() -> int {
	return (::fchmodat(pfd, leaf, m, 0) < 0) ? -errno : 0;
      });
      if (r < 0) {
	return r;
      }
    } else if (op->second == "remove") {
      int r = carries_nsfs_state(path);
      if (r != 0) {
	return (r > 0) ? -EEXIST : r;
      }
      r = acting([&]() -> int {
	const int fd = ::openat(pfd, leaf, O_RDONLY | O_NOFOLLOW | O_DIRECTORY | O_CLOEXEC);
	if (fd < 0) {
	  return -errno;
	}
	DIR* d = ::fdopendir(fd);
	if (!d) {
	  const int err = errno;
	  ::close(fd);
	  return -err;
	}
	while (struct dirent* e = ::readdir(d)) {
	  if (e->d_type == DT_REG) {
	    ::unlinkat(::dirfd(d), e->d_name, 0);
	  } else if ((e->d_type == DT_DIR) && std::strcmp(e->d_name, ".") &&
		     std::strcmp(e->d_name, "..")) {
	    ::unlinkat(::dirfd(d), e->d_name, AT_REMOVEDIR);
	  }
	}
	::closedir(d);
	/* `d` itself is in the gateway's `<name>` */
	return (!as && (::unlinkat(pfd, leaf, AT_REMOVEDIR) < 0)) ? -errno : 0;
      });
      if (r < 0) {
	return r;
      }
      if (as && ((::unlinkat(pfd, leaf, AT_REMOVEDIR) < 0) ||
		 (::rmdir(parent.c_str()) < 0))) {
	return -errno;
      }
    } else {
      return -EINVAL;
    }
    if (out) {
      (*out)["path"] = path;
    }
    return 0;
  }

  if (hint == "inject-skip-reclone") {
    /* make reclone() a no-op, so a write open stays bound to the
     * published object.  the guards which reject a mutation or publish
     * of a non-shadow binding are otherwise unreachable--every caller
     * reclones first */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_skip_reclone = (it->second == "true");
    if (out) {
      (*out)["enabled"] = inject_skip_reclone ? "true" : "false";
    }
    return 0;
  }

  if (hint == "inject-denial-as-absence") {
    /* reproduce the delete defect:  NSFSDeleteOp::delete_obj() reads a
     * denied stat, walk or open as an absent object, so a denied
     * version delete answers success and a denied plain delete
     * -EINVAL.  A test asserts both polarities against one build */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_denial_as_absence = (it->second == "true");
    if (out) {
      (*out)["enabled"] = inject_denial_as_absence ? "true" : "false";
    }
    return 0;
  }

  if (hint == "inject-unresolved-as-gateway") {
    /* reproduce the defects work-plan stage N-F2 fixed:  prepare() passed over
     * a user whose credentials do not resolve, so HEAD and the
     * attribute reads were served as the gateway, and
     * check_owner_access() allowed one, so a bucket was placed at a
     * path its owner was never checked against, and create() kept one's
     * bucket directory the gateway's.  Arming takes
     * `user=`, since the suite runs in parallel;  one user at a time */
    auto en = params.find("enable");
    if (en == params.end()) {
      return -EINVAL;
    }
    auto u = params.find("user");
    const bool arm = (en->second == "true");
    if (arm && ((u == params.end()) || u->second.empty())) {
      return -EINVAL;
    }
    std::lock_guard l{unresolved_as_gateway_lock};
    if (arm) {
      unresolved_as_gateway_user = u->second;
    } else {
      unresolved_as_gateway_user.clear();
    }
    if (out) {
      (*out)["enabled"] = arm ? "true" : "false";
      (*out)["user"] = unresolved_as_gateway_user;
    }
    return 0;
  }

  if (hint == "inject-etag-as-gateway") {
    /* reproduce the non-md5 etag defect in a running gateway:  the
     * attribute is written by the calling thread and the result is
     * discarded, so under impersonation it fails on a file the
     * gateway does not own and the object keeps the md5 etag
     * write_attrs() put there before the link.  The suite asserts both
     * polarities against one build */
    auto it = params.find("enable");
    if (it == params.end()) {
      return -EINVAL;
    }
    inject_etag_as_gateway = (it->second == "true");
    if (out) {
      (*out)["enabled"] = inject_etag_as_gateway ? "true" : "false";
    }
    return 0;
  }

  if (hint == "bucket-profile") {
    /* Move a bucket to a named profile, so a suite can reach one this
     * gateway does not create on its own.  Every bucket it makes is
     * `strong`, so without this the `base` and `shared` branches of any
     * interlock are unreachable and a test asserting them would prove
     * nothing.
     *
     * It goes through set_profile() and gets the same rejections as the
     * REST path, so a suite cannot arrive at a state an operator
     * cannot. */
    auto it = params.find("bucket");
    auto pv = params.find("profile");
    if ((it == params.end()) || (pv == params.end())) {
      return -EINVAL;
    }
    auto target = nsfs::extensions_for_profile(pv->second);
    if (! target) {
      return -EINVAL;
    }

    std::unique_ptr<rgw::sal::Bucket> loaded;
    int ret = load_bucket_internal(dpp, rgw_bucket{"", it->second}, &loaded,
			  null_yield);
    if (ret < 0) {
      return ret;
    }
    auto& bucket = *static_cast<NSFSBucket*>(loaded.get());
    if (out) {
      (*out)["had_profile"] = bucket.profile_name();
    }
    ret = bucket.set_profile(dpp, null_yield, *target);
    if (out) {
      (*out)["profile"] = bucket.profile_name();
    }
    return ret;
  }

  /* `access-model` -- read, and briefly change, what the gateway does
   * when the filesystem denies something S3 authorised.
   *
   *   model=<name>        the model in force
   *   data=<result>      what it does for object *bytes*
   *   object_attrs=<...>  and for an object's tags and ACL
   *
   * `set=` points it at another model for the rest of the process:
   * `posix-strict`, `s3-native`, or `default`
   * (whatever was configured).  Switching is how the invariant test
   * gets its second polarity -- `posix-strict` denies the bytes and
   * `s3-native` serves them -- rather than by editing the source and
   * rebuilding, which this project does not do.
   *
   * The suite must put it back.  A model left switched would make
   * every later test in the run meaningless, so a test that sets it
   * restores it in the same call sequence. */
  if (hint == "access-model") {
    static const nsfs::PosixStrictAccess strict;
    static const nsfs::S3NativeAccess native;

    if (auto it = params.find("set"); it != params.end()) {
      const nsfs::AccessStrategy* want = nullptr;
      if (it->second == "posix-strict") {
	want = &strict;
      } else if (it->second == "s3-native") {
	want = &native;
      } else if (it->second == "default") {
	want = access_strategy.get();
      } else {
	return -EINVAL;
      }
      const auto* had = get_access_strategy();
      access_strategy_live.store(want, std::memory_order_relaxed);
      ldpp_dout(dpp, 1) << "nsfs: access model " << had->name() << " -> "
	<< want->name() << " by driver hint" << dendl;
      if (out) {
	(*out)["had"] = had->name();
      }
    }

    const auto* m = get_access_strategy();
    if (out) {
      (*out)["model"] = m->name();
      (*out)["configured"] = access_strategy->name();
      /* the values the suite reads:  held as they were rendered from
       * the enum this replaced */
      (*out)["data"] = m->permits(nsfs::AccessClass::data)
	? "as_gateway_raised" : "refuse";
      (*out)["object_attrs"] = m->permits(nsfs::AccessClass::object_attrs)
	? "as_gateway_raised" : "refuse";
      /* why, not just whether:  how many operations the gateway has
       * performed itself, which the model permits, by class */
      (*out)["attrs_served"] = std::to_string(
	cross_account_attrs_served.load(std::memory_order_relaxed));
      (*out)["data_served"] = std::to_string(
	cross_account_data_served.load(std::memory_order_relaxed));
      (*out)["publishes_as_gateway"] = std::to_string(
	publishes_as_gateway.load(std::memory_order_relaxed));
      /* directory operations in another identity's key directory,
       * performed by the gateway (with_identity_or_raise()) */
      (*out)["raised_operations"] = std::to_string(
	nsfs::raised_operations());
      (*out)["copy_sources_as_gateway"] = std::to_string(
	get_fs_strategy()->cross_account_source_reads());
    }
    return 0;
  }

  /* `raise` -- the raise primitive, which no test can reach from S3
   * because the only site that uses it is the promote's residue, and
   * reaching that needs a version file owned by a uid no identity row
   * claims.
   *
   * Three answers in one call, and each reports what the kernel said
   * rather than only a status, so none can pass vacuously:
   *
   *   inside=1          the capability is effective inside the call
   *   after=0           and is gone when it returns
   *   impersonating=-1  a raise on a thread already running as an
   *                     identity is rejected, -EPERM:  the capability
   *                     would be the identity's
   *
   * `cap` names what to raise, `dac_override` by default;  `user` is
   * an rgw_user string for the impersonating case, which is skipped
   * without one.  On a gateway started without the file capabilities
   * the first two are `result=not-permitted`, which is the
   * deployment answer and not a failure of the primitive. */
  if (hint == "raise") {
    auto effective_set = []() -> uint64_t {
      struct __user_cap_header_struct hdr = {};
      struct __user_cap_data_struct data[2] = {};
      hdr.version = _LINUX_CAPABILITY_VERSION_3;
      hdr.pid = 0;
      if (::syscall(SYS_capget, &hdr, data) < 0) {
	return ~0ull;
      }
      return (static_cast<uint64_t>(data[1].effective) << 32) |
	     data[0].effective;
    };

    uint64_t want = nsfs::cap_bit(CAP_DAC_OVERRIDE);
    if (auto c_it = params.find("cap"); c_it != params.end()) {
      if (c_it->second == "fowner") {
	want = nsfs::cap_bit(CAP_FOWNER);
      } else if (c_it->second == "promote") {
	want = PROMOTE_RESIDUE_CAPS;
      } else if (c_it->second != "dac_override") {
	return -EINVAL;
      }
    }

    /* before the raise:  0 is what the startup lowering is for, and
     * the reason it is reported here is that nothing else would
     * notice its absence -- a gateway that never lowered would serve
     * every request with DAC_OVERRIDE effective and pass every other
     * assertion in this hint. */
    const uint64_t before = effective_set();
    uint64_t inside = 0;
    const int ret = nsfs::with_capabilities(dpp, want,
					    [&]() -> int {
      inside = effective_set();
      return 0;
    });
    const uint64_t after = effective_set();

    if (out) {
      (*out)["result"] = (ret == 0) ? "ran" : "not-permitted";
      (*out)["requested"] = std::to_string(want);
      (*out)["before"] = std::to_string((before & want) ? 1 : 0);
      (*out)["inside"] =
	std::to_string((ret == 0) && ((inside & want) == want) ? 1 : 0);
      (*out)["after"] = std::to_string((after & want) ? 1 : 0);
    }

    /* and that it is rejected where it would be the identity's */
    if (auto u_it = params.find("user"); u_it != params.end()) {
      if (!nsfs::impersonation_enabled()) {
	if (out) {
	  (*out)["impersonating"] = "not-impersonating";
	}
      } else {
	nsfs::Credentials cred;
	if (hint_credentials(dpp, this, u_it->second, cred) < 0) {
	  if (out) {
	    (*out)["impersonating"] = "no-identity";
	  }
	} else {
	  int raise_ret = 0;
	  (void) nsfs::run_as(dpp, cred, [&]() -> int {
	    raise_ret = nsfs::with_capabilities(dpp, want,
						[]() -> int { return 0; });
	    return 0;
	  }, null_yield);
	  if (out) {
	    (*out)["impersonating"] = std::to_string(raise_ret);
	  }
	}
      }
    }
    return 0;
  }

  /* `nested-identity` -- that a nested run_as() cannot run as the
   * wrong identity.
   *
   * `run_as` takes a fast path when the calling thread is already
   * impersonating, because enqueueing to the helper pool from a pool
   * thread can deadlock.  That path used to be taken for *any*
   * identity, so a nested call asking for a different one ran as the
   * outer one and said nothing.  The multipart cache's eviction is
   * reachable that way.
   *
   * Both polarities in one call, and each reports the uid the inner
   * call actually observed rather than only its status, so neither
   * assertion can pass vacuously:
   *
   *   same=<uid>        nested as the same identity still runs, and
   *                     the uid is the outer one
   *   different=-35     nested as another identity is rejected, -EDEADLK
   *
   * Only reachable on a gateway that can assume an identity, so the
   * suite skips it when `result=not-impersonating`. */
  if (hint == "ran-as") {
    /* Which POSIX identity a converted consumer chose for one object.
     *
     * `select` names the user and clears whatever was recorded;
     * a later call with no `select` drains what has been seen since
     * and stops.  Scoped to one user rather than global because
     * the suite runs in parallel, and a global slot would answer for
     * whichever request happened to run last.  By user rather
     * than by object:  the question is whether this caller's
     * operations ran as it, and a bucket-level attribute write names
     * no object at all.
     *
     * This reports the identity the driver *chose*, not the uid a
     * syscall ran under.  That the switch itself works is what
     * `nested-identity` and `raise` establish;  what phase C left
     * open is whether the right identity reaches the call, and a
     * wrong one is a wrong choice. */
    auto a_it = params.find("select");
    if (a_it != params.end()) {
      select_ran_as(a_it->second);
      if (out) {
	(*out)["selecting"] = a_it->second;
      }
      return 0;
    }
    const auto seen = take_ran_as();
    if (out) {
      (*out)["count"] = std::to_string(seen.size());
      for (const auto& [site, uid] : seen) {
	(*out)[std::string("ran_as_") + site] = std::to_string(uid);
      }
    }
    return 0;
  }

  if (hint == "nested-identity") {
    if (!nsfs::impersonation_enabled()) {
      if (out) {
	(*out)["result"] = "not-impersonating";
      }
      return 0;
    }
    auto o_it = params.find("outer");
    auto i_it = params.find("inner");
    if ((o_it == params.end()) || (i_it == params.end())) {
      return -EINVAL;
    }
    nsfs::Credentials outer, inner;
    if ((hint_credentials(dpp, this, o_it->second, outer) < 0) ||
	(hint_credentials(dpp, this, i_it->second, inner) < 0)) {
      if (out) {
	(*out)["result"] = "no-identity";
      }
      return 0;
    }
    /* the inner call returns who it is, so a pass proves the identity
     * and not merely the status */
    const nsfs::ImpersonatedFn who = []() -> int {
      return static_cast<int>(::geteuid());
    };
    int same = 0, different = 0;
    (void) nsfs::run_as(dpp, outer, [&]() -> int {
      same = nsfs::run_as(dpp, outer, who, null_yield);
      different = nsfs::run_as(dpp, inner, who, null_yield);
      return 0;
    }, null_yield);
    if (out) {
      (*out)["result"] = "ran";
      (*out)["outer_uid"] = std::to_string(outer.uid);
      (*out)["inner_uid"] = std::to_string(inner.uid);
      (*out)["same"] = std::to_string(same);
      (*out)["different"] = std::to_string(different);
    }
    return 0;
  }

  if (hint == "access") {
    /* The permission check on a request, for the tests that check
     * the gateway against it:  whether `user`, served as its POSIX
     * identity, may read (`mode=r`) or write (`mode=w`) the current
     * object at `key` in `bucket`, or create one there (`mode=c`):
     * write and search on the key's directory, which decides a PUT of
     * a key that does not exist yet -- or delete it (`mode=d`):  the
     * same, and in a sticky directory owning the file or the
     * directory, which faccessat(2) does not ask.  Asked of the kernel as that
     * identity -- faccessat(AT_EACCESS) on the helper pool -- so the
     * suite never re-implements the check.
     *
     *   result=not-impersonating   the switch is off;  S3 alone decides
     *   result=no-identity         the user has no identity row
     *   result=permitted | refused
     *
     * `user` is an rgw_user string, `tenant$id` for a tenant's;  empty
     * is anonymous, which has no identity.  `tenant` names the bucket's
     * tenant, if it has one. */
    auto put = [out](const char* v) {
      if (out) {
	(*out)["result"] = v;
      }
    };
    if (!nsfs::impersonation_enabled()) {
      put("not-impersonating");
      return 0;
    }
    auto b_it = params.find("bucket");
    auto k_it = params.find("key");
    auto u_it = params.find("user");
    auto m_it = params.find("mode");
    if ((b_it == params.end()) || (k_it == params.end()) ||
	(u_it == params.end()) || (m_it == params.end()) ||
	((m_it->second != "r") && (m_it->second != "w") &&
	 (m_it->second != "c") && (m_it->second != "d"))) {
      return -EINVAL;
    }

    /* resolved as a request resolves it:  the user's row, else its
     * account's */
    nsfs::Credentials cred;
    int ret = hint_credentials(dpp, this, u_it->second, cred);
    if (ret == -ENOENT) {
      put("no-identity");
      return 0;
    }
    if (ret < 0) {
      return ret;
    }

    std::unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket rb;
    if (auto t_it = params.find("tenant"); t_it != params.end()) {
      rb.tenant = t_it->second;
    }
    rb.name = b_it->second;
    ret = load_bucket_internal(dpp, rb, &bucket, null_yield);
    if (ret < 0) {
      return ret;
    }
    auto obj = bucket->get_object(rgw_obj_key(k_it->second));
    auto* nobj = static_cast<NSFSObject*>(obj.get());
    if ((m_it->second == "c") || (m_it->second == "d")) {
      /* The key's directory, walked as the gateway, and asked as the
       * identity.  -ENOENT when a directory on the way is missing:  ask
       * after they exist. */
      std::vector<std::unique_ptr<nsfs::Directory>> chain;
      nsfs::Directory* dir{nullptr};
      std::string leaf;
      ret = nobj->resolve_parent_dir(dpp, /*create_dirs=*/false, chain, dir,
				     leaf);
      if (ret < 0) {
	return ret;
      }
      if ((dir->get_fd() < 0) && ((ret = dir->open(dpp)) < 0)) {
	return ret;
      }
      const int dfd = dir->get_fd();
      ret = nsfs::run_as(dpp, cred, [dfd]() -> int {
	return (::faccessat(dfd, ".", W_OK | X_OK, AT_EACCESS) < 0)
	    ? -errno : 0;
      }, null_yield);
      if ((ret == 0) && (m_it->second == "d")) {
	/* unlink(2) in a sticky directory:  only the file's owner or
	 * the directory's */
	struct statx dstx, fstx;
	if (nsfs::statx(dfd, "", AT_EMPTY_PATH, STATX_MODE | STATX_UID,
			&dstx) < 0) {
	  return -errno;
	}
	if (nsfs::statx(dfd, leaf.c_str(), AT_SYMLINK_NOFOLLOW, STATX_UID,
			&fstx) < 0) {
	  return -errno;
	}
	if ((dstx.stx_mode & S_ISVTX) && (cred.uid != fstx.stx_uid) &&
	    (cred.uid != dstx.stx_uid)) {
	  ret = -EACCES;
	}
      }
      if (ret == 0) {
	put("permitted");
      } else if (ret == -EACCES) {
	put("refused");
      } else {
	return ret;
      }
      if (out) {
	(*out)["uid"] = std::to_string(cred.uid);
      }
      return 0;
    }
    ret = nobj->stat(dpp);
    if (ret < 0) {
      return ret;
    }
    nsfs::FSEnt* ent = nobj->get_fsent();
    if (!ent || !ent->get_parent()) {
      return -ENOENT;
    }
    nsfs::Directory* parent = ent->get_parent();
    if ((parent->get_fd() < 0) && ((ret = parent->open(dpp)) < 0)) {
      return ret;
    }
    const int pfd = parent->get_fd();
    const std::string name = ent->get_name();
    const int amode = (m_it->second == "r") ? R_OK : W_OK;
    ret = nsfs::run_as(dpp, cred, [pfd, &name, amode]() -> int {
      return (::faccessat(pfd, name.c_str(), amode, AT_EACCESS) < 0)
	  ? -errno : 0;
    }, null_yield);
    if (ret == 0) {
      put("permitted");
    } else if (ret == -EACCES) {
      put("refused");
    } else {
      return ret;
    }
    if (out) {
      (*out)["uid"] = std::to_string(cred.uid);
    }
    return 0;
  }

  if (hint == "dynamic-groups") {
    /* I8:  rgw_nsfs_dynamic_supplemental_groups for the suite's
     * dynamic-groups profile, as `access-model` switches the model.
     * Daemon-global, so the profile runs alone. */
    auto it = params.find("set");
    if (it != params.end()) {
      if (it->second == "on") {
	userDB->nsswitch_cache().override_dynamic_groups(true);
      } else if (it->second == "off") {
	userDB->nsswitch_cache().override_dynamic_groups(false);
      } else if (it->second == "default") {
	userDB->nsswitch_cache().override_dynamic_groups(std::nullopt);
      } else {
	return -EINVAL;
      }
      ldpp_dout(dpp, 1) << "nsfs: dynamic supplementary groups "
	<< it->second << " by driver hint" << dendl;
    }
    if (out) {
      (*out)["dynamic_groups"] =
	userDB->nsswitch_cache().dynamic_groups() ? "true" : "false";
    }
    return 0;
  }

  if (hint == "identity-override") {
    /* I9:  rgw_nsfs_user_identity_override and
     * rgw_nsfs_role_identity_override for the tests, `user=` and `role=`
     * each on, off or default.  Daemon-global, as dynamic-groups is.
     * Both values are checked before either is applied. */
    std::optional<std::optional<bool>> user, role;
    for (const auto& [name, slot] :
	   {std::pair{"user", &user}, std::pair{"role", &role}}) {
      auto it = params.find(name);
      if (it == params.end()) {
	continue;
      }
      if (it->second == "on") {
	*slot = std::optional<bool>(true);
      } else if (it->second == "off") {
	*slot = std::optional<bool>(false);
      } else if (it->second == "default") {
	*slot = std::optional<bool>();
      } else {
	return -EINVAL;
      }
    }
    if (user) {
      userDB->force_user_identity_override(*user);
      ldpp_dout(dpp, 1) << "nsfs: user identity override "
	<< params.at("user") << " by driver hint" << dendl;
    }
    if (role) {
      userDB->force_role_identity_override(*role);
      ldpp_dout(dpp, 1) << "nsfs: role identity override "
	<< params.at("role") << " by driver hint" << dendl;
    }
    if (out) {
      (*out)["user_identity_override"] =
	userDB->user_identity_override() ? "true" : "false";
      (*out)["role_identity_override"] =
	userDB->role_identity_override() ? "true" : "false";
    }
    return 0;
  }

  if (hint == "nsswitch-cache") {
    /* clear one nsswitch-resolved identity's resolution (I7), so a test
     * can make the next request look it up without waiting for the
     * sweep */
    bool cleared;
    if (auto u = params.find("forget_uid"); u != params.end()) {
      uint32_t uid = 0;
      const auto [p, ec] = std::from_chars(u->second.data(),
					   u->second.data() + u->second.size(),
					   uid);
      if ((ec != std::errc{}) || (p != u->second.data() + u->second.size())) {
	return -EINVAL;
      }
      cleared = userDB->nsswitch_cache().forget_uid(uid);
    } else if (auto it = params.find("forget"); it != params.end()) {
      cleared = userDB->nsswitch_cache().forget(it->second);
    } else {
      return -EINVAL;
    }
    if (out) {
      (*out)["cleared"] = cleared ? "true" : "false";
    }
    return 0;
  }

  if (hint == "invalidate-cache") {
    /* drop a bucket's listing cache, so the next listing rebuilds it by
     * enumerating the store.  The incremental and rebuild paths compose
     * keys differently and only the rebuild walks .versions/, so a test
     * that means to exercise the rebuild has no other way to reach it
     * from inside one process.
     *
     * The side branch carries this verb too, along with
     * invalidate-quota-cache;  they want merging. */
    auto it = params.find("bucket");
    if (it == params.end()) {
      return -EINVAL;
    }
    auto* bcache = get_bucket_cache();
    if (!bcache) {
      return -ENOTSUP;
    }
    int ret = bcache->invalidate_bucket(dpp, it->second);
    if (out) {
      (*out)["invalidated"] = (ret == 0) ? "true" : "false";
    }
    return ret;
  }

  return -ENOTSUP;
}

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newNSFSDriver(CephContext *cct,
				boost::asio::io_context& io_context)
{
  rgw::sal::NSFSDriver* driver = new rgw::sal::NSFSDriver(cct, &io_context);

  int ret = -1;
  const static std::string tenant = "default_ns";
  /* get_identity_db(), not get_user_db():  neither Initialize() is
   * virtual, so the base one would run and the identity table
   * would never be created */
  if ((ret = driver->get_identity_db()->Initialize("", -1)) < 0) {
    ldout(cct, 0) << "DB initialization failed for tenant("<<tenant<<")" << dendl;
    delete driver;
    return nullptr;
  }

  driver->set_context(cct);

  return driver;
}

}
