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
#include "rgw_public_access.h"
#include "rgw_website.h"
#include "rgw_bucket_encryption.h"
#include "rgw_tag.h"
#include "rgw_lc.h"
#include "rgw_lc_s3.h"
#include "rgw_xml.h"
#include "common/XMLFormatter.h"
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

  const bool has_rule = !mode.empty();

  /* Neither RGW's JSON decoder nor ours would otherwise check the
   * mode -- only RGW's XML path does -- so an arbitrary string would
   * reach info.obj_lock and be reported as the retention mode of a
   * locked bucket. */
  if (has_rule && (mode != "GOVERNANCE") && (mode != "COMPLIANCE")) {
    ldpp_dout(dpp, 0) << "ERROR: default_retention names mode \"" << mode
      << "\", which is neither of theirs" << dendl;
    return -EBADMSG;
  }

  /* Their schema requires exactly one of days or years, and so does
   * RGW:  rgw_op.cc:9707 rejects anything else with
   * ERR_INVALID_RETENTION_PERIOD.  It matters because
   * get_lock_until_date() takes days when days > 0 and years
   * otherwise -- so both set silently discards the years, and neither
   * set yields mtime + years(0), a retention that expires the instant
   * the object is written. */
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

/* public_access_block.
 *
 * Four booleans, the same four, spelled snake_case by them and
 * PascalCase by RGW.  PublicAccessBlockConfiguration is a plain struct
 * with public members, so there is nothing to build and nothing to
 * hand to a decoder.
 *
 * A field they omit stays false, which is what S3 does:
 * PutPublicAccessBlock takes whatever subset the client sends and the
 * rest are off.  The attribute is written whenever they carry the key
 * at all, including an all-false block, because a block they set is a
 * fact about the bucket even when it blocks nothing. */
int from_noobaa_public_access(const DoutPrefixProvider* dpp, JSONObj* pab,
			      Attrs& attrs)
{
  PublicAccessBlockConfiguration conf;
  JSONDecoder::decode_json("block_public_acls", conf.BlockPublicAcls, pab);
  JSONDecoder::decode_json("ignore_public_acls", conf.IgnorePublicAcls, pab);
  JSONDecoder::decode_json("block_public_policy", conf.BlockPublicPolicy,
			   pab);
  JSONDecoder::decode_json("restrict_public_buckets",
			   conf.RestrictPublicBuckets, pab);

  bufferlist bl;
  conf.encode(bl);
  attrs[RGW_ATTR_PUBLIC_ACCESS] = std::move(bl);
  return 0;
}

/* A field they type as a JSON string where RGW holds a uint16_t.
 *
 * The integer decoder reads the node's data, so a quoted "301" parses
 * and anything that is not a number throws -- which the boundary in
 * load() turns into a bucket that fails closed.  That is the point:
 * silently becoming zero would turn a redirect code into 0 and a
 * condition on one error code into a condition on any. */
void u16_from(const char* name, JSONObj* obj, uint16_t& out)
{
  int v = 0;
  if (!JSONDecoder::decode_json(name, v, obj)) {
    return;
  }
  if ((v < 0) || (v > UINT16_MAX)) {
    throw JSONDecoder::err(std::string(name) + " is out of range");
  }
  out = static_cast<uint16_t>(v);
}

/* bucket_website.
 *
 * Their website_configuration is an anyOf of two shapes, which is S3's
 * own rule:  either redirect every request, or serve an index document
 * with an optional error document and routing rules.  The branches are
 * exclusive, so taking the redirect first and the rest otherwise
 * matches both their schema and RGW's is_redirect_all.
 *
 * Every RGW type here is a plain struct with public members, so this
 * is assignment rather than construction.  It lands on the info rather
 * than in the attribute map, which is where RGW keeps it. */
int from_noobaa_website(const DoutPrefixProvider* dpp, JSONObj* web,
			RGWBucketInfo& info)
{
  JSONObj* cfg = web->find_obj("website_configuration");
  if (!cfg) {
    return 0;
  }

  RGWBucketWebsiteConf conf;

  if (JSONObj* all = cfg->find_obj("redirect_all_requests_to"); all) {
    JSONDecoder::decode_json("host_name", conf.redirect_all.hostname, all);
    JSONDecoder::decode_json("protocol", conf.redirect_all.protocol, all);
    conf.is_redirect_all = true;
  } else {
    if (JSONObj* idx = cfg->find_obj("index_document"); idx) {
      JSONDecoder::decode_json("suffix", conf.index_doc_suffix, idx);
      conf.is_set_index_doc = !conf.index_doc_suffix.empty();
    }
    if (JSONObj* err = cfg->find_obj("error_document"); err) {
      JSONDecoder::decode_json("key", conf.error_doc, err);
    }
    if (JSONObj* rules = cfg->find_obj("routing_rules"); rules) {
      for (auto i = rules->find_first(); !i.end(); ++i) {
	RGWBWRoutingRule rule;
	if (JSONObj* c = (*i)->find_obj("condition"); c) {
	  JSONDecoder::decode_json("key_prefix_equals",
				   rule.condition.key_prefix_equals, c);
	  u16_from("http_error_code_returned_equals", c,
		   rule.condition.http_error_code_returned_equals);
	}
	if (JSONObj* r = (*i)->find_obj("redirect"); r) {
	  auto& ri = rule.redirect_info;
	  JSONDecoder::decode_json("protocol", ri.redirect.protocol, r);
	  JSONDecoder::decode_json("host_name", ri.redirect.hostname, r);
	  JSONDecoder::decode_json("replace_key_prefix_with",
				   ri.replace_key_prefix_with, r);
	  JSONDecoder::decode_json("replace_key_with",
				   ri.replace_key_with, r);
	  u16_from("http_redirect_code", r, ri.redirect.http_redirect_code);
	}
	conf.routing_rules.rules.push_back(std::move(rule));
      }
    }
  }

  info.website_conf = std::move(conf);
  info.has_website = true;
  return 0;
}

/* bucket_encryption.
 *
 * Three fields onto a three-argument constructor:  algorithm, key id,
 * and whether a bucket key is in use.  RGWBucketEncryptionConfig takes
 * exactly those, so there is nothing to build.
 *
 * An empty or absent algorithm configures nothing -- their schema
 * requires none of the three, so `{}` is a valid record that says
 * nothing.
 *
 * An algorithm that is neither of theirs is stored anyway, and that is
 * deliberate.  The string becomes the x-amz-server-side-encryption
 * header value on every PUT (rgw_rest_s3.cc, get_encryption_defaults),
 * so RGW already refuses writes into a bucket configured with one it
 * does not know.  That is fail-closed at the granularity the gap
 * actually has.  Failing the bucket at load instead would also deny
 * reads of objects sitting there perfectly readable, which the gap
 * does not justify -- unlike versioning, where there is no safe
 * default, or a retention period, where RGW itself refuses the same
 * input. */
int from_noobaa_encryption(const DoutPrefixProvider* dpp, JSONObj* enc,
			   Attrs& attrs)
{
  std::string algorithm;
  JSONDecoder::decode_json("algorithm", algorithm, enc);
  if (algorithm.empty()) {
    return 0;
  }
  if ((algorithm != "AES256") && (algorithm != "aws:kms")) {
    /* warned on every load of this bucket, on purpose:  writes into it
     * will fail and the reason should be findable without reading the
     * record by hand */
    ldpp_dout(dpp, 1) << "nsfs: bucket encryption names algorithm \""
      << algorithm << "\", which is neither of theirs;  RGW will refuse "
      << "writes into this bucket" << dendl;
  }

  std::string key_id;
  bool bucket_key = false;
  JSONDecoder::decode_json("kms_key_id", key_id, enc);
  JSONDecoder::decode_json("bucket_key_enabled", bucket_key, enc);

  RGWBucketEncryptionConfig conf(algorithm, key_id, bucket_key);
  bufferlist bl;
  conf.encode(bl);
  attrs[RGW_ATTR_BUCKET_ENCRYPTION_POLICY] = std::move(bl);
  return 0;
}

/* tag -- the bucket's tag set, not an object's.
 *
 * An array of {key, value} into RGWObjTags, which has a public
 * add_tag().  Their `tagging` definition requires both fields of each
 * entry and constrains neither.
 *
 * Stored whatever the set looks like.  RGW's own S3 path builds an
 * RGWObjTags(50) and refuses a request breaking that or the 128/256
 * byte key and value limits, but their schema imposes none of it, so a
 * NooBaa bucket may legitimately carry more or longer tags than the
 * API would accept.  Refusing to serve such a bucket would deny
 * everything over metadata that gates nothing and destroys nothing --
 * the same disproportion as failing a bucket over an encryption
 * algorithm.  A set that exceeds what the API would take is warned
 * about, because that is the only signal there will be. */
int from_noobaa_tags(const DoutPrefixProvider* dpp, JSONObj* tagging,
		     const std::string& bucket_name, Attrs& attrs)
{
  /* S3's limits, not RGW's:  50 tags on a bucket, 128 bytes of key,
   * 256 of value.  RGWObjTags keeps the same numbers protected, and
   * they are named here rather than exposed, because this is a
   * driver's warning and not a reason to widen a shared type. */
  constexpr size_t S3_MAX_BUCKET_TAGS = 50;
  constexpr size_t S3_MAX_TAG_KEY = 128;
  constexpr size_t S3_MAX_TAG_VAL = 256;

  RGWObjTags tags;
  size_t oversize = 0;

  for (auto i = tagging->find_first(); !i.end(); ++i) {
    std::string key, val;
    JSONDecoder::decode_json("key", key, *i);
    JSONDecoder::decode_json("value", val, *i);
    if (key.empty()) {
      /* a tag with no key is not a tag;  RGW rejects it and there is
       * nothing to keep */
      ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket_name
	<< " has a tag with no key" << dendl;
      return -EBADMSG;
    }
    if ((key.size() > S3_MAX_TAG_KEY) || (val.size() > S3_MAX_TAG_VAL)) {
      ++oversize;
    }
    tags.emplace_tag(std::move(key), std::move(val));
  }

  if (tags.count() == 0) {
    return 0;
  }
  if ((tags.count() > S3_MAX_BUCKET_TAGS) || oversize) {
    ldpp_dout(dpp, 1) << "nsfs: bucket " << bucket_name << " carries "
      << tags.count() << " tags" << (oversize ? ", some over length" : "")
      << ";  more than PutBucketTagging would accept" << dendl;
  }

  bufferlist bl;
  tags.encode(bl);
  attrs[RGW_ATTR_TAGS] = std::move(bl);
  return 0;
}

/* Their dates are epoch milliseconds;  RGW's are ISO 8601 strings that
 * check_date() additionally requires to be exact midnight UTC.  The
 * conversion is here;  the midnight rule is not, because the parser
 * enforces it and a date that breaks it is a rule PutBucketLifecycle
 * would refuse. */
bool idate_iso8601(JSONObj* parent, const char* name, std::string& out)
{
  int64_t ms = 0;
  if (!JSONDecoder::decode_json(name, ms, parent)) {
    return false;
  }
  using namespace std::chrono;
  out = ceph::to_iso_8601(
    ceph::real_clock::time_point(duration_cast<ceph::timespan>(
      milliseconds(ms))));
  return true;
}

/* lifecycle_configuration_rules.
 *
 * WHAT WE ACCEPT IS WHAT PutBucketLifecycle ACCEPTS, by construction:
 * their record is rendered as the XML that request carries and handed
 * to the same parser, decoder and rebuild().  Matt, 2026-09-29 --
 * "accept only what our XML parser can handle."
 *
 * That is worth the render rather than reimplementing the rules,
 * because the rules are many and the cost of getting one wrong is
 * deletion.  Their schema is looser than S3's in several places at
 * once:  an expiration may carry days, a date and a delete-marker flag
 * together where LCExpiration_S3 requires exactly one;  a rule may mix
 * days and date across its expiration and transitions where
 * LCRule::valid() forbids it;  a date need not be midnight UTC where
 * check_date() insists.  Each of those is a rule of theirs that RGW
 * cannot express, and none has a safe reading -- guessing which of
 * days or date wins deletes objects on the wrong day -- so the parser
 * refusing it fails the bucket closed.
 *
 * Transitions are rendered like everything else.  The parser takes
 * them, so we take them;  whether this driver has anywhere to
 * transition to is lifecycle processing's question and not this
 * reader's. */
int from_noobaa_lifecycle(const DoutPrefixProvider* dpp, CephContext* cct,
			  JSONObj* rules, const std::string& bucket_name,
			  Attrs& attrs)
{
  XMLFormatter f;
  f.open_object_section("LifecycleConfiguration");

  size_t count = 0;
  for (auto ri = rules->find_first(); !ri.end(); ++ri) {
    JSONObj* r = *ri;
    ++count;
    f.open_object_section("Rule");

    std::string id, status;
    JSONDecoder::decode_json("id", id, r);
    JSONDecoder::decode_json("status", status, r);
    encode_xml("ID", id, &f);
    encode_xml("Status", status, &f);

    /* Their `and` flag records whether the original XML wrapped the
     * conditions in <And>, which is how they rebuild it themselves.
     * LCFilter_S3 looks for that element first and falls back to the
     * filter itself, so reproducing it keeps a multi-condition filter
     * reading the way it was written. */
    if (JSONObj* flt = r->find_obj("filter"); flt) {
      bool conjunction = false;
      JSONDecoder::decode_json("and", conjunction, flt);
      f.open_object_section("Filter");
      if (conjunction) {
	f.open_object_section("And");
      }

      std::string prefix;
      if (JSONDecoder::decode_json("prefix", prefix, flt)) {
	encode_xml("Prefix", prefix, &f);
      }
      int64_t sz = 0;
      if (JSONDecoder::decode_json("object_size_greater_than", sz, flt)) {
	encode_xml("ObjectSizeGreaterThan", sz, &f);
      }
      if (JSONDecoder::decode_json("object_size_less_than", sz, flt)) {
	encode_xml("ObjectSizeLessThan", sz, &f);
      }
      if (JSONObj* tags = flt->find_obj("tags"); tags) {
	for (auto ti = tags->find_first(); !ti.end(); ++ti) {
	  std::string k, v;
	  JSONDecoder::decode_json("key", k, *ti);
	  JSONDecoder::decode_json("value", v, *ti);
	  f.open_object_section("Tag");
	  encode_xml("Key", k, &f);
	  encode_xml("Value", v, &f);
	  f.close_section();
	}
      }

      if (conjunction) {
	f.close_section();
      }
      f.close_section();
    }

    if (JSONObj* exp = r->find_obj("expiration"); exp) {
      f.open_object_section("Expiration");
      int64_t days = 0;
      std::string date;
      if (JSONDecoder::decode_json("days", days, exp)) {
	encode_xml("Days", days, &f);
      }
      if (idate_iso8601(exp, "date", date)) {
	encode_xml("Date", date, &f);
      }
      bool dm = false;
      if (JSONDecoder::decode_json("expired_object_delete_marker", dm, exp)) {
	encode_xml("ExpiredObjectDeleteMarker", dm, &f);
      }
      f.close_section();
    }

    if (JSONObj* nce = r->find_obj("noncurrent_version_expiration"); nce) {
      f.open_object_section("NoncurrentVersionExpiration");
      int64_t v = 0;
      if (JSONDecoder::decode_json("noncurrent_days", v, nce)) {
	encode_xml("NoncurrentDays", v, &f);
      }
      if (JSONDecoder::decode_json("newer_noncurrent_versions", v, nce)) {
	encode_xml("NewerNoncurrentVersions", v, &f);
      }
      f.close_section();
    }

    if (JSONObj* mp = r->find_obj("abort_incomplete_multipart_upload"); mp) {
      f.open_object_section("AbortIncompleteMultipartUpload");
      int64_t v = 0;
      if (JSONDecoder::decode_json("days_after_initiation", v, mp)) {
	encode_xml("DaysAfterInitiation", v, &f);
      }
      f.close_section();
    }

    if (JSONObj* trs = r->find_obj("transitions"); trs) {
      for (auto ti = trs->find_first(); !ti.end(); ++ti) {
	f.open_object_section("Transition");
	int64_t days = 0;
	std::string date, sc;
	if (JSONDecoder::decode_json("days", days, *ti)) {
	  encode_xml("Days", days, &f);
	}
	if (idate_iso8601(*ti, "date", date)) {
	  encode_xml("Date", date, &f);
	}
	JSONDecoder::decode_json("storage_class", sc, *ti);
	encode_xml("StorageClass", sc, &f);
	f.close_section();
      }
    }

    if (JSONObj* nts = r->find_obj("noncurrent_version_transitions"); nts) {
      for (auto ti = nts->find_first(); !ti.end(); ++ti) {
	f.open_object_section("NoncurrentVersionTransition");
	int64_t v = 0;
	std::string sc;
	if (JSONDecoder::decode_json("noncurrent_days", v, *ti)) {
	  encode_xml("NoncurrentDays", v, &f);
	}
	if (JSONDecoder::decode_json("newer_noncurrent_versions", v, *ti)) {
	  encode_xml("NewerNoncurrentVersions", v, &f);
	}
	JSONDecoder::decode_json("storage_class", sc, *ti);
	encode_xml("StorageClass", sc, &f);
	f.close_section();
      }
    }

    f.close_section();
  }
  f.close_section();

  if (count == 0) {
    return 0;
  }

  bufferlist xml;
  f.flush(xml);

  RGWXMLParser parser;
  if (!parser.init()) {
    return -EIO;
  }
  if (!parser.parse(xml.c_str(), xml.length(), 1)) {
    ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket_name << ": their "
      << "lifecycle rules did not render as parsable XML" << dendl;
    return -EBADMSG;
  }

  RGWLifecycleConfiguration_S3 config(cct);
  try {
    RGWXMLDecoder::decode_xml("LifecycleConfiguration", config, &parser);
  } catch (RGWXMLDecoder::err& e) {
    ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket_name << " has a "
      << "lifecycle rule PutBucketLifecycle would refuse: "
      << e.what() << dendl;
    return -EBADMSG;
  }

  RGWLifecycleConfiguration dest(cct);
  if (int r = config.rebuild(dest); r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: bucket " << bucket_name << " has a "
      << "lifecycle configuration RGW will not rebuild: "
      << cpp_strerror(-r) << dendl;
    return -EBADMSG;
  }

  bufferlist bl;
  dest.encode(bl);
  attrs[RGW_ATTR_LC] = std::move(bl);
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

  /* Everything below decodes fields out of a file we found on disk,
   * and JSONDecoder::decode_json() throws JSONDecoder::err on a value
   * of the wrong shape -- "days": "ten" and the like.  Caught here
   * rather than per field, so that a feature added later is covered
   * by having been written inside this boundary.  A record we cannot
   * decode is state that is there and unreadable, which is -EBADMSG
   * and fails the bucket closed. */
  try {

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

  /* Public access block.  Also the attribute map, same reason. */
  if (JSONObj* pab = p.find_obj("public_access_block"); pab) {
    ret = from_noobaa_public_access(dpp, pab, attrs);
    if (ret < 0) {
      return ret;
    }
  }

  /* Website.  Onto the info, which is where RGW keeps it. */
  if (JSONObj* web = p.find_obj("website"); web) {
    ret = from_noobaa_website(dpp, web, info);
    if (ret < 0) {
      return ret;
    }
  }

  /* Default encryption.  The attribute map. */
  if (JSONObj* enc = p.find_obj("encryption"); enc) {
    ret = from_noobaa_encryption(dpp, enc, attrs);
    if (ret < 0) {
      return ret;
    }
  }

  /* Bucket tags.  The attribute map, under the same key object tags
   * use;  which one it is follows from what carries it. */
  if (JSONObj* tg = p.find_obj("tag"); tg) {
    ret = from_noobaa_tags(dpp, tg, bucket_name, attrs);
    if (ret < 0) {
      return ret;
    }
  }

  /* Lifecycle.  The attribute map. */
  if (JSONObj* lc = p.find_obj("lifecycle_configuration_rules"); lc) {
    ret = from_noobaa_lifecycle(dpp, dpp->get_cct(), lc, bucket_name, attrs);
    if (ret < 0) {
      return ret;
    }
  }

  ldpp_dout(dpp, 10) << "nsfs: bucket " << bucket_name
    << " took versioning from " << path << ": "
    << (versioning.empty() ? "unset" : versioning) << dendl;

  } catch (const JSONDecoder::err& e) {
    ldpp_dout(dpp, 0) << "ERROR: " << path << " has a field this gateway "
      << "cannot decode: " << e.what() << dendl;
    return -EBADMSG;
  }

  return 0;
}

}}} // namespace rgw::sal::nsfs
