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

#include "xattr_strategy.h"

#include <sys/xattr.h>

#include "common/errno.h"
#include "rgw_object_lock.h"
#include "rgw_tag.h"

#include <cerrno>

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

/* The prefixes, in one place.
 *
 * RGW's own attributes arrive named user.rgw.* and are re-prefixed rather
 * than stored as they come, so that this driver's namespace and a
 * generic one cannot collide on a tree something else also writes. */
static const std::string NSFS_XATTR_PREFIX = "user.nsfs.";
static const std::string NSFS_RGW_XATTR_PREFIX = "user.nsfs.rgw.";
static const std::string RGW_ATTR_PFX = "user.rgw.";

std::string PrefixedXattrStrategy::disk_name(const std::string& key) const
{
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return NSFS_RGW_XATTR_PREFIX + key.substr(RGW_ATTR_PFX.size());
  }
  return NSFS_XATTR_PREFIX + key;
}

bool PrefixedXattrStrategy::parse_disk_name(const std::string& disk,
				       std::string& key) const
{
  if (disk.compare(0, NSFS_RGW_XATTR_PREFIX.size(),
		   NSFS_RGW_XATTR_PREFIX) == 0) {
    key = RGW_ATTR_PFX + disk.substr(NSFS_RGW_XATTR_PREFIX.size());
    return true;
  }
  if (disk.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = disk.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }
  return false;
}

int PrefixedXattrStrategy::object_owner(const Attrs& attrs,
				   const struct statx* stx,
				   ACLOwner& owner) const
{
  /* recorded, and separate from the file's uid.  Nothing keeps the two
   * consistent, which is why a file created outside this gateway has no
   * answer here at all. */
  auto i = attrs.find(RGW_ATTR_ACL);
  if (i == attrs.end()) {
    return -EINVAL;
  }
  RGWAccessControlPolicy policy;
  try {
    auto bp = i->second.cbegin();
    policy.decode(bp);
  } catch (const buffer::error&) {
    return -EIO;
  }
  owner = policy.get_owner();
  return 0;
}

/* The attributes RGW writes as counted strings, from the five writers
 * catalogued in docs/RGW_COUNTED_STRING_ATTRS.md.  Everything else --
 * the encoded ACL, object_type, bucket_info, the multipart blobs -- is
 * ceph-encoded and must not be touched. */
bool PrefixedXattrStrategy::counted_string_value(const std::string& key) const
{
  /* x-amz-meta-*, from rgw_get_request_metadata() and the librgw
   * setxattr path */
  if (key.compare(0, sizeof(RGW_ATTR_META_PREFIX) - 1,
		  RGW_ATTR_META_PREFIX) == 0) {
    return true;
  }

  /* s->generic_attrs, written by rgw_op.cc:3861;  the table is
   * generic_attrs[] at rgw_rest.cc:122 */
  return key == RGW_ATTR_CONTENT_TYPE ||
         key == RGW_ATTR_CONTENT_LANG ||
         key == RGW_ATTR_EXPIRES ||
         key == RGW_ATTR_CACHE_CONTROL ||
         key == RGW_ATTR_CONTENT_DISP ||
         key == RGW_ATTR_CONTENT_ENC ||
         key == RGW_ATTR_X_ROBOTS_TAG;

  /* NOT RGW_ATTR_ETAG.  Only the opaque-etag path (rgw_op.cc:9000)
   * writes it counted;  the ordinary MD5 path does not, and nsfs stores
   * it bare since 15386ec4a26.  Listing it made the strip a no-op and
   * the restore a corruption -- HEAD returned an etag with a NUL inside
   * the quotes. */
}

/* --- NooBaaXattrStrategy ---------------------------------------------- */

static const std::string NB_USER_PREFIX = "user.";
static const std::string NB_INTERNAL_PREFIX = "user.noobaa.";
static const std::string NB_CONTENT_MD5 = "user.content_md5";

/* RGW's logical key on the left, theirs on the right.  One to one;  the
 * rows which are not are named in the header and deferred. */
struct NBKeyMap {
  const char* logical;
  const char* disk;
};

static const NBKeyMap NB_KEYS[] = {
  { RGW_ATTR_ETAG,            "user.content_md5" },
  { RGW_ATTR_CONTENT_TYPE,    "user.noobaa.content_type" },
  { RGW_ATTR_CONTENT_ENC,     "user.noobaa.content_encoding" },
  { "version_id",             "user.noobaa.version_id" },
  { "delete_marker",          "user.noobaa.delete_marker" },
  { "non_current_timestamp",  "user.noobaa.non_current_timestamp" },
  /* Not under user.noobaa., and the only name their own reader
   * excludes from user metadata -- XATTR_METADATA_IGNORE_LIST holds
   * this and nothing else (`namespace_fs.js:111`, `:281`).  Without
   * the mapping it fell through to the metadata rule below and came
   * back as `x-amz-meta-storage_class`, which is a key no client set
   * and their gateway does not report. */
  { RGW_ATTR_STORAGE_CLASS,   "user.storage_class" },
};

/* Three answers, not two.
 *
 * A key they keep goes to their name.  A key of ours with no
 * counterpart -- bucket_info, rename intent -- goes under user.nsfs.,
 * where it is inert to them;  the convergence report already records
 * that we write bucket_info on a base bucket for exactly that reason.
 * Only an S3 attribute they deliberately do not keep is dropped, and
 * that is the ACL:  the empty name says "nowhere", and a write path
 * which sees one skips the attribute rather than inventing a place for
 * it.
 *
 * Returning the key unchanged would be the wrong third answer -- it
 * would write under a name neither format owns, which is how two
 * gateways come to disagree about what an object carries. */
std::string NooBaaXattrStrategy::disk_name(const std::string& key) const
{
  for (const auto& m : NB_KEYS) {
    if (key == m.logical) {
      return m.disk;
    }
  }

  /* user metadata is theirs key for key:  to_fs_xattr() prefixes with
   * "user." and nothing else */
  if (key.compare(0, std::strlen(RGW_ATTR_META_PREFIX),
		  RGW_ATTR_META_PREFIX) == 0) {
    return NB_USER_PREFIX + key.substr(std::strlen(RGW_ATTR_META_PREFIX));
  }

  /* nowhere:  they store no ACL, and their own gateway accepts the
   * request and keeps nothing (`s3_put_object_acl.js`) */
  if (key == RGW_ATTR_ACL) {
    return std::string();
  }

  /* ours, and inert to them */
  return NSFS_XATTR_PREFIX + key;
}

bool NooBaaXattrStrategy::parse_disk_name(const std::string& disk,
					  std::string& key) const
{
  for (const auto& m : NB_KEYS) {
    if (disk == m.disk) {
      key = m.logical;
      return true;
    }
  }

  /* ours, written on their tree because it has nowhere else to go */
  if (disk.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = disk.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }

  /* Everything else under user.noobaa. is theirs and structured --
   * tags, the object-lock family, the part record -- and this format
   * does not yet map any of it.  Claimed as unmapped rather than
   * surfaced:  handing a caller one of their plain strings under an RGW
   * key would feed a NooBaa value to a ceph decoder. */
  if (disk.compare(0, NB_INTERNAL_PREFIX.size(), NB_INTERNAL_PREFIX) == 0) {
    return false;
  }

  /* what is left under user. is user metadata */
  if (disk.compare(0, NB_USER_PREFIX.size(), NB_USER_PREFIX) == 0) {
    key = std::string(RGW_ATTR_META_PREFIX) +
	  disk.substr(NB_USER_PREFIX.size());
    return true;
  }

  return false;
}

/* From the inode.  They record no owner anywhere:  the write happened
 * under the account's identity, so the file's uid is the answer, and it
 * is the only answer a natively created file has. */
int NooBaaXattrStrategy::object_owner(const Attrs& attrs,
				      const struct statx* stx,
				      ACLOwner& owner) const
{
  if (!stx || !(stx->stx_mask & STATX_UID)) {
    return -EINVAL;
  }
  owner.id = rgw_user(std::to_string(stx->stx_uid));
  return 0;
}

/* Never.  The terminator is an RGW-side accident that our own writers
 * append;  nothing in their tree carries one, and restoring a byte they
 * never wrote would corrupt the value.  See
 * docs/RGW_COUNTED_STRING_ATTRS.md. */
/* Their tag attribute prefix.  One attribute per tag, the key after
 * the prefix, the value raw (`namespace_fs.js:85`, `:1443`, `:2251`).
 * Hardcoded a second time in their GPFS ILM policy generator
 * (`nc_lifecycle.js:1359`), which is a second writer on our own
 * platform. */
static const std::string NB_TAG_PREFIX = "user.noobaa.tag.";

/* The object-lock family:  three plain attributes of theirs against
 * two ceph-encoded ones of ours (`namespace_fs.js:86-88`).  Legal
 * hold is one-to-one and still needs the widened pair, because the
 * shapes differ -- theirs is the literal `ON` or `OFF` and ours is an
 * encoded RGWObjectLegalHold.  Retention is two-to-one. */
static const std::string NB_LEGAL_HOLD = "user.noobaa.legal_hold";
static const std::string NB_RETENTION_MODE = "user.noobaa.retention_mode";
static const std::string NB_RETENTION_DATE = "user.noobaa.retention_date";

/* one of their plain attributes, or nothing */
static std::optional<std::string> nb_read(const DoutPrefixProvider* dpp,
					  int fd, const std::string& name)
{
  char buf[512];
  const ssize_t len = ::fgetxattr(fd, name.c_str(), buf, sizeof(buf));
  if (len < 0) {
    return std::nullopt;
  }
  return std::string(buf, len);
}

/* Is this on-disk name one of their tags?
 *
 * Their reader tests with String.includes() -- a SUBSTRING match, not
 * a prefix one (`namespace_fs.js:316`) -- and strips with
 * .replace(prefix,''), which removes only the first occurrence.  Their
 * own clear path uses startsWith instead (`fs_napi.cpp:551`), so the
 * two disagree;  a name that merely contains the prefix is reported as
 * a tag and not removed by DeleteObjectTagging.
 *
 * We follow their reader, because it is what decides what a client
 * sees.  The consequence is theirs and is reproduced deliberately:  a
 * PUT header `x-amz-meta-noobaa.tag.colour` becomes the on-disk name
 * `user.noobaa.tag.colour` (their user metadata is `user.` + the key
 * with `x-amz-meta-` stripped) and comes back as a tag rather than as
 * metadata. */
static bool nb_tag_name(const std::string& disk, std::string& key)
{
  const auto at = disk.find(NB_TAG_PREFIX);
  if (at == std::string::npos) {
    return false;
  }
  key = disk.substr(0, at) + disk.substr(at + NB_TAG_PREFIX.size());
  return !key.empty();
}

void NooBaaXattrStrategy::parse_disk_attrs(
    const DoutPrefixProvider* dpp, int fd,
    const std::vector<std::string>& unclaimed, Attrs& out) const
{
  if (out.contains(RGW_ATTR_TAGS)) {
    /* ours is already there, from the per-name pass on a converting
     * object;  it is the later word */
    return;
  }

  RGWObjTags tags;
  bool any = false;
  for (const auto& disk : unclaimed) {
    std::string key;
    if (!nb_tag_name(disk, key)) {
      continue;
    }
    char buf[1024];
    const ssize_t len = ::fgetxattr(fd, disk.c_str(), buf, sizeof(buf));
    if (len < 0) {
      /* raced with a delete, or longer than any tag may be -- their
       * own limit is far below this.  One tag, not the set. */
      ldpp_dout(dpp, 4) << "could not read " << disk << ": "
			<< cpp_strerror(errno) << ";  skipping it" << dendl;
      continue;
    }
    tags.add_tag(key, std::string(buf, len));
    any = true;
  }

  if (any) {
    bufferlist bl;
    tags.encode(bl);
    out.emplace(RGW_ATTR_TAGS, std::move(bl));
  }

  /* The object-lock family.
   *
   * Retention is both attributes or neither, which is their rule too:
   * their reader returns undefined unless mode AND date are present
   * (`namespace_fs.js:2890`).  A half-written pair is not a retention
   * with a missing field, it is no retention -- and a retention
   * carrying the epoch because a date would not parse reads as
   * expired, which is the one wrong answer object lock must never
   * give. */
  bool has_hold = false, has_mode = false, has_date = false;
  for (const auto& disk : unclaimed) {
    has_hold |= (disk == NB_LEGAL_HOLD);
    has_mode |= (disk == NB_RETENTION_MODE);
    has_date |= (disk == NB_RETENTION_DATE);
  }

  if (has_hold && !out.contains(RGW_ATTR_OBJECT_LEGAL_HOLD)) {
    if (auto v = nb_read(dpp, fd, NB_LEGAL_HOLD)) {
      RGWObjectLegalHold lh;
      lh.set_status(*v);
      bufferlist bl;
      lh.encode(bl);
      out.emplace(RGW_ATTR_OBJECT_LEGAL_HOLD, std::move(bl));
    }
  }

  if (has_mode && has_date && !out.contains(RGW_ATTR_OBJECT_RETENTION)) {
    auto mode = nb_read(dpp, fd, NB_RETENTION_MODE);
    auto date = nb_read(dpp, fd, NB_RETENTION_DATE);
    ceph::real_time until;
    if (mode && date && (parse_time(date->c_str(), &until) == 0)) {
      RGWObjectRetention r;
      r.set_mode(*mode);
      r.set_retain_until_date(until);
      bufferlist bl;
      r.encode(bl);
      out.emplace(RGW_ATTR_OBJECT_RETENTION, std::move(bl));
    } else if (mode && date) {
      ldpp_dout(dpp, 0) << "ERROR: object carries a retention date this "
	<< "gateway cannot parse (\"" << *date << "\");  reporting no "
	<< "retention rather than one that reads as expired" << dendl;
    }
  }
}

bool NooBaaXattrStrategy::disk_attrs(const std::string& key,
				     const bufferlist& val,
				     const std::vector<std::string>& current,
				     xattr_map_t& to_write,
				     std::vector<std::string>& to_remove) const
{
  if (key == RGW_ATTR_OBJECT_LEGAL_HOLD) {
    if (val.length() == 0) {
      to_remove.push_back(NB_LEGAL_HOLD);
      return true;
    }
    RGWObjectLegalHold lh;
    try {
      auto bufit = val.cbegin();
      lh.decode(bufit);
    } catch (buffer::error&) {
      to_remove.push_back(NB_LEGAL_HOLD);
      return true;
    }
    to_write.insert_or_assign(NB_LEGAL_HOLD, lh.get_status());
    return true;
  }

  if (key == RGW_ATTR_OBJECT_RETENTION) {
    /* both or neither, the way their reader takes it */
    if (val.length() == 0) {
      to_remove.push_back(NB_RETENTION_MODE);
      to_remove.push_back(NB_RETENTION_DATE);
      return true;
    }
    RGWObjectRetention r;
    try {
      auto bufit = val.cbegin();
      r.decode(bufit);
    } catch (buffer::error&) {
      to_remove.push_back(NB_RETENTION_MODE);
      to_remove.push_back(NB_RETENTION_DATE);
      return true;
    }
    std::string iso;
    rgw_to_iso8601(r.get_retain_until_date(), &iso);
    to_write.insert_or_assign(NB_RETENTION_MODE, r.get_mode());
    to_write.insert_or_assign(NB_RETENTION_DATE, iso);
    return true;
  }

  if (key != RGW_ATTR_TAGS) {
    return false;
  }

  RGWObjTags tags;
  if (val.length() > 0) {
    try {
      auto bufit = val.cbegin();
      tags.decode(bufit);
    } catch (buffer::error&) {
      /* An unreadable tag set is not a reason to write half of one,
       * and not a reason to leave the old one either:  claimed, with
       * nothing written, so the supersede below clears it.  No dpp
       * here to say so -- the caller has one and sees an object whose
       * tags went away, which is the same information. */
      tags.get_tags().clear();
    }
  }

  for (const auto& [k, v] : tags.get_tags()) {
    to_write.insert_or_assign(NB_TAG_PREFIX + k, v);
  }

  /* The set replaces, it does not merge -- which is S3's contract for
   * PutObjectTagging and what their own path does, clearing the whole
   * prefix before writing (`namespace_fs.js:2258`).  Anything on disk
   * this write does not name goes. */
  for (const auto& disk : current) {
    std::string tk;
    if (nb_tag_name(disk, tk) && !to_write.contains(disk)) {
      to_remove.push_back(disk);
    }
  }
  return true;
}

bool NooBaaXattrStrategy::counted_string_value(const std::string& key) const
{
  return false;
}

}}} // namespace rgw::sal::nsfs
