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

#include <fcntl.h>
#include <unistd.h>

#include "common/ceph_json.h"
#include "rgw_object_lock.h"
#include "rgw_iam_policy.h"
#include "common/errno.h"
#include "include/scope_guard.h"

#include "bucket_state_strategy.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

int RgwBucketStateStrategy::load(const DoutPrefixProvider* dpp, int dir_fd,
				 const std::string& bucket_name,
				 Attrs& attrs, RGWBucketInfo& info) const
{
  auto i = attrs.find(BUCKET_INFO_KEY);
  if (i == attrs.end()) {
    return -ENOENT;
  }

  /* erased whether or not it decodes.  It is not object metadata and
   * surfacing it would put it in the bucket's S3 attribute set, which
   * is true of a corrupt one too. */
  bufferlist bl = i->second;
  attrs.erase(i);

  try {
    auto p = bl.cbegin();
    decode(info, p);
  } catch (buffer::error& err) {
    /* present and unreadable, which is not the same as absent:  the
     * bucket has stored state and we cannot see it */
    ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket_name << " has a "
      << BUCKET_INFO_KEY << " attribute that does not decode" << dendl;
    return -EBADMSG;
  }

  return 0;
}

namespace {

/* Their record whole, in one read.  No iostreams and no incremental
 * parse:  a bucket record is small, and a cap keeps a corrupt or
 * hostile file from being read into memory whole. */
constexpr size_t NB_RECORD_MAX = 1u << 20;

int slurp(const DoutPrefixProvider* dpp, const std::string& path,
	  std::string& out)
{
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return -errno;
  }
  auto close_fd = make_scope_guard([fd] { ::close(fd); });

  out.clear();
  char buf[8192];
  for (;;) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      return -errno;
    }
    if (n == 0) {
      break;
    }
    if ((out.size() + n) > NB_RECORD_MAX) {
      ldpp_dout(dpp, 0) << "ERROR: " << path << " exceeds "
	<< NB_RECORD_MAX << " bytes" << dendl;
      return -EFBIG;
    }
    out.append(buf, n);
  }
  return 0;
}

/* The last inch of a conversion:  materialise an RGW type from the
 * document a `from_noobaa_*` function has just written into `f`.
 *
 * This exists to keep the driver inside the type's public interface.
 * `RGWObjectLock` and the two classes under it hold their members
 * protected and offer `decode_json()` as the way in, so that is the way
 * we go in.  Adding setters would have been fewer lines here and would
 * have put an nsfs requirement into a header every other driver
 * compiles;  reading NooBaa's JSON is this driver's problem and stays
 * this driver's problem.
 *
 * Not a general mechanism.  Of the other bucket features their records
 * carry, all but this one reach RGW types through public mutators or
 * constructors -- `RGWObjTags::add_tag()`,
 * `RGWLifecycleConfiguration::add_rule()`,
 * `RGWCORSConfiguration::stack_rule()`,
 * `RGWBucketEncryptionConfig`'s constructor, and plain structs for the
 * public access block and the website configuration -- and a bucket
 * policy is document text that goes into the attribute map as it
 * stands.  So this is for the one awkward type, not a pattern to
 * follow.
 *
 * `c_str()` coalesces, which is why it is avoided on the data path.
 * Here the buffer is a few hundred bytes this process just produced,
 * in one segment, once per bucket load. */
template <class T>
int decode_into(const DoutPrefixProvider* dpp, JSONFormatter& f, T& out)
{
  bufferlist bl;
  f.flush(bl);

  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    ldpp_dout(dpp, 0) << "ERROR: could not re-parse a document we wrote"
      << dendl;
    return -EBADMSG;
  }
  try {
    out.decode_json(&p);
  } catch (const JSONDecoder::err& e) {
    ldpp_dout(dpp, 0) << "ERROR: a document we wrote was rejected: "
      << e.what() << dendl;
    return -EBADMSG;
  }
  return 0;
}

/* object_lock_configuration.
 *
 * Theirs:   { object_lock_enabled: "Enabled"|"Disabled",
 *             rule: { default_retention: { days|years, mode } } }
 * RGW's:    { enabled: bool, rule_exist: bool,
 *             rule: { defaultRetention: { mode, days, years } } }
 *
 * "Disabled" is their internal state for a bucket that is not locked;
 * S3 only ever validates "Enabled" on the wire.  So Disabled means the
 * bucket has no lock, not a lock that is off. */
int from_noobaa_object_lock(const DoutPrefixProvider* dpp, JSONObj* cfg,
			    RGWBucketInfo& info)
{
  std::string enabled;
  JSONDecoder::decode_json("object_lock_enabled", enabled, cfg);
  if (enabled != "Enabled") {
    return 0;
  }

  int days = 0, years = 0;
  std::string mode;
  if (JSONObj* rule = cfg->find_obj("rule"); rule) {
    if (JSONObj* dr = rule->find_obj("default_retention"); dr) {
      JSONDecoder::decode_json("days", days, dr);
      JSONDecoder::decode_json("years", years, dr);
      JSONDecoder::decode_json("mode", mode, dr);
    }
  }

  /* their schema requires one of days or years with a mode, and RGW
   * requires exactly one of the two as well -- retention_period_valid()
   * says so.  A rule that satisfies neither is a rule we cannot honour,
   * and honouring it wrongly is a retention failure. */
  const bool has_rule = !mode.empty();
  if (has_rule && ((years > 0) == (days > 0))) {
    ldpp_dout(dpp, 0) << "ERROR: default_retention names "
      << (days > 0 ? "both days and years" : "neither days nor years")
      << dendl;
    return -EBADMSG;
  }

  JSONFormatter f;
  f.open_object_section("lock");
  encode_json("enabled", true, &f);
  encode_json("rule_exist", has_rule, &f);
  if (has_rule) {
    f.open_object_section("rule");
    f.open_object_section("defaultRetention");
    encode_json("mode", mode, &f);
    encode_json("days", days, &f);
    encode_json("years", years, &f);
    f.close_section();
    f.close_section();
  }
  f.close_section();

  RGWObjectLock lock;
  int ret = decode_into(dpp, f, lock);
  if (ret < 0) {
    return ret;
  }

  info.obj_lock = lock;
  info.flags |= BUCKET_OBJ_LOCK_ENABLED;
  return 0;
}

/* s3_policy.
 *
 * The one feature that needs no translation:  a bucket policy is an
 * AWS policy document, RGW keeps the document, and theirs is the same
 * document held parsed.  `JSONObj::init()` stores
 * `json_spirit::write_string()` for a non-leaf node, so asking their
 * `s3_policy` object for its data hands back the whole document as
 * text.
 *
 * Parsed before it is stored, which is not extra care -- it is what
 * the S3 path does.  `RGWPutBucketPolicy` builds an
 * `rgw::IAM::Policy` and stores `p.text` rather than the bytes the
 * client sent, so a policy arriving this way is normalised by the same
 * parser and ends up in the same form as one set through the API.  It
 * also means a document RGW cannot parse is caught here, where it
 * reads as a bucket that will not load, rather than later inside every
 * request that evaluates policy. */
int from_noobaa_bucket_policy(const DoutPrefixProvider* dpp, JSONObj* pol,
			      RGWBucketInfo& info, Attrs& attrs)
{
  const std::string doc = pol->get_data();
  if (doc.empty()) {
    return 0;
  }

  CephContext* cct = dpp->get_cct();
  try {
    const rgw::IAM::Policy p(
      cct, &info.bucket.tenant, doc,
      cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));

    bufferlist bl;
    bl.append(p.text);
    attrs[RGW_ATTR_IAM_POLICY] = std::move(bl);
  } catch (const rgw::IAM::PolicyParseException& e) {
    ldpp_dout(dpp, 0) << "ERROR: bucket policy does not parse: "
      << e.what() << dendl;
    return -EBADMSG;
  }
  return 0;
}

} // namespace

int NooBaaBucketStateStrategy::load(const DoutPrefixProvider* dpp, int dir_fd,
				    const std::string& bucket_name,
				    Attrs& attrs, RGWBucketInfo& info) const
{
  if (config_root.empty()) {
    return -ENOENT;
  }

  const std::string path =
    config_root + "/buckets/" + bucket_name + ".json";

  std::string doc;
  int ret = slurp(dpp, path, doc);
  if (ret == -ENOENT) {
    /* no record:  this bucket has no state in their store, which is
     * the absent case and not a failure */
    return -ENOENT;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: reading " << path << ": "
      << cpp_strerror(-ret) << dendl;
    return ret;
  }

  JSONParser p;
  if (!p.parse(doc.data(), doc.size())) {
    ldpp_dout(dpp, 0) << "ERROR: " << path << " is not JSON" << dendl;
    return -EBADMSG;
  }

  /* Versioning.  Their enum is the whole of it:  DISABLED, SUSPENDED,
   * ENABLED.  RGW keeps a suspended bucket versioned and marks it
   * suspended beside that, because it still holds versions. */
  std::string versioning;
  JSONDecoder::decode_json("versioning", versioning, &p);
  if (versioning == "ENABLED") {
    info.flags |= BUCKET_VERSIONED;
  } else if (versioning == "SUSPENDED") {
    info.flags |= BUCKET_VERSIONED | BUCKET_VERSIONS_SUSPENDED;
  } else if (!versioning.empty() && (versioning != "DISABLED")) {
    ldpp_dout(dpp, 0) << "ERROR: " << path << " has versioning \""
      << versioning << "\", which is none of theirs" << dendl;
    return -EBADMSG;
  }

  std::string created;
  JSONDecoder::decode_json("creation_date", created, &p);
  if (!created.empty()) {
    ceph::real_time t;
    if (parse_time(created.c_str(), &t) == 0) {
      info.creation_time = t;
    } else {
      /* not fatal:  a creation date we cannot read costs a listing the
       * right timestamp, and nothing else.  Versioning is the reason
       * this reader exists. */
      ldpp_dout(dpp, 4) << "nsfs: " << path << " has an unparsable "
	<< "creation_date \"" << created << "\"" << dendl;
    }
  }

  /* Object lock.  First of their optional features, and the one whose
   * absence is a compliance breach rather than a missing feature:  a
   * WORM bucket that reads as unlocked accepts overwrite and delete. */
  if (JSONObj* lock = p.find_obj("object_lock_configuration"); lock) {
    ret = from_noobaa_object_lock(dpp, lock, info);
    if (ret < 0) {
      return ret;
    }
  }

  /* Bucket policy.  Into the attribute map rather than onto the info,
   * because rgw_op.cc reads it from there. */
  if (JSONObj* pol = p.find_obj("s3_policy"); pol) {
    ret = from_noobaa_bucket_policy(dpp, pol, info, attrs);
    if (ret < 0) {
      return ret;
    }
  }

  ldpp_dout(dpp, 10) << "nsfs: bucket " << bucket_name
    << " took versioning from " << path << ": "
    << (versioning.empty() ? "unset" : versioning) << dendl;

  return 0;
}

}}} // namespace rgw::sal::nsfs
