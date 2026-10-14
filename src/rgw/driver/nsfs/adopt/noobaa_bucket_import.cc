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

#include <set>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "common/ceph_json.h"
#include "common/errno.h"

#include "rgw_bucket_logging.h"
#include "rgw_common.h"
#include "rgw_object_ownership.h"

#include "../rgw_sal_nsfs.h"
#include "noobaa_bucket_import.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

std::optional<std::string>
IdentityMapping::resolve(const std::string& principal) const
{
  /* Id first.  A name and an id are both bare strings and cannot be
   * told apart syntactically -- an account may legally be *named*
   * twenty-four hex characters -- so the order is the whole of the
   * disambiguation, and it mirrors NooBaa's. */
  if (auto i = user_arn_by_noobaa_id.find(principal);
      i != user_arn_by_noobaa_id.end()) {
    return i->second;
  }
  if (auto i = account_by_noobaa_id.find(principal);
      i != account_by_noobaa_id.end()) {
    return i->second;
  }

  /* Then name. */
  if (auto n = noobaa_id_by_name.find(principal);
      n != noobaa_id_by_name.end()) {
    if (auto i = user_arn_by_noobaa_id.find(n->second);
	i != user_arn_by_noobaa_id.end()) {
      return i->second;
    }
    if (auto i = account_by_noobaa_id.find(n->second);
	i != account_by_noobaa_id.end()) {
      return i->second;
    }
  }
  return std::nullopt;
}

IdentityMapping mapping_from(const ImportPlan& plan)
{
  IdentityMapping m;
  for (const auto& e : plan.entries) {
    if (e.iam_user) {
      /* RGW matches a user principal on `display_name`, which the
       * import set to the NooBaa name. */
      m.user_arn_by_noobaa_id[e.noobaa_id] =
	"arn:aws:iam::" + e.account_id + ":user/" + e.display_name;
    } else {
      m.account_by_noobaa_id[e.noobaa_id] = e.account_id;
    }
    if (!e.noobaa_name.empty()) {
      m.noobaa_id_by_name[e.noobaa_name] = e.noobaa_id;
    }
  }
  return m;
}

namespace {

/* Rewrite one principal value in place, or record it as unresolved. */
void rewrite_one(rapidjson::Value& v, rapidjson::Document& d,
		 const IdentityMapping& mapping,
		 std::vector<std::string>& unresolved)
{
  if (!v.IsString()) {
    return;
  }
  const std::string before{v.GetString(), v.GetStringLength()};
  /* `*` is everyone and means the same thing on both sides. */
  if (before == "*") {
    return;
  }
  auto after = mapping.resolve(before);
  if (!after) {
    unresolved.push_back(before);
    return;
  }
  v.SetString(after->c_str(),
	      static_cast<rapidjson::SizeType>(after->size()),
	      d.GetAllocator());
}

/* The `Principal` (or `NotPrincipal`) of one statement.
 *
 * NooBaa's shape is AWS's:  a string, an array of strings, or an object
 * whose `AWS` member is either.  Only the AWS arm names an identity we
 * could have imported;  `Service` and `Federated` are AWS's own
 * namespaces and are left exactly as they are. */
void rewrite_principal(rapidjson::Value& principal, rapidjson::Document& d,
		       const IdentityMapping& mapping,
		       std::vector<std::string>& unresolved)
{
  if (principal.IsString()) {
    rewrite_one(principal, d, mapping, unresolved);
    return;
  }
  if (principal.IsArray()) {
    for (auto& v : principal.GetArray()) {
      rewrite_one(v, d, mapping, unresolved);
    }
    return;
  }
  if (!principal.IsObject()) {
    return;
  }
  for (const char* arm : {"AWS", "aws"}) {
    auto i = principal.FindMember(arm);
    if (i == principal.MemberEnd()) {
      continue;
    }
    if (i->value.IsString()) {
      rewrite_one(i->value, d, mapping, unresolved);
    } else if (i->value.IsArray()) {
      for (auto& v : i->value.GetArray()) {
	rewrite_one(v, d, mapping, unresolved);
      }
    }
  }
}

} // namespace

int rewrite_policy_principals(const DoutPrefixProvider* dpp,
			      const std::string& doc,
			      const IdentityMapping& mapping,
			      std::string& out,
			      std::vector<std::string>& unresolved)
{
  rapidjson::Document d;
  d.Parse(doc.c_str(), doc.size());
  if (d.HasParseError() || !d.IsObject()) {
    ldpp_dout(dpp, 0) << "ERROR: a bucket policy will not parse" << dendl;
    return -EBADMSG;
  }

  auto stmts = d.FindMember("Statement");
  if (stmts == d.MemberEnd()) {
    /* a policy with no statements grants nothing and needs no
     * rewriting;  carried through unchanged */
    out = doc;
    return 0;
  }

  auto do_statement = [&](rapidjson::Value& s) {
    if (!s.IsObject()) {
      return;
    }
    for (const char* key : {"Principal", "NotPrincipal"}) {
      auto p = s.FindMember(key);
      if (p != s.MemberEnd()) {
	rewrite_principal(p->value, d, mapping, unresolved);
      }
    }
  };

  if (stmts->value.IsArray()) {
    for (auto& s : stmts->value.GetArray()) {
      do_statement(s);
    }
  } else {
    do_statement(stmts->value);
  }

  if (!unresolved.empty()) {
    /* Not written.  A principal that resolves to nothing is a grant or
     * a denial that will not take effect. */
    return -ENOENT;
  }

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> w(buf);
  if (!d.Accept(w)) {
    return -EIO;
  }
  out.assign(buf.GetString(), buf.GetSize());
  return 0;
}

namespace {

/* NooBaa's `logging` into RGW's bucket-logging configuration.
 *
 * `{log_bucket, log_prefix}` is the whole of NooBaa's record, and it maps
 * onto `target_bucket` and `target_prefix` with logging enabled.  The
 * rest of RGW's configuration -- the key format, the roll time, the
 * batch size -- has no counterpart in NooBaa's record, so it keeps RGW's
 * defaults rather than being invented.
 *
 * This is one of the three fields `NOOBAA_WRITE_SURFACE.md` lists as
 * not read during conversion, and a bucket converted without it loses
 * its logging silently. */
int carry_logging(const DoutPrefixProvider* dpp, JSONObj* obj, Attrs& attrs)
{
  std::string target, prefix;
  try {
    JSONDecoder::decode_json("log_bucket", target, obj);
    JSONDecoder::decode_json("log_prefix", prefix, obj);
  } catch (const JSONDecoder::err& e) {
    ldpp_dout(dpp, 0) << "ERROR: logging does not decode: " << e.what()
      << dendl;
    return -EBADMSG;
  }
  if (target.empty()) {
    /* a logging record naming no target is not a configuration */
    return 0;
  }

  rgw::bucketlogging::configuration conf;
  /* `enabled` is set for a reader of the object in memory and is
   * **not part of the encoded form** -- the serialisation carries
   * target, prefix, key format, roll time, type, batch size and date
   * source, and nothing else.  So on disk the *presence* of this
   * attribute is what says logging is configured, which is why the
   * absent case writes no attribute rather than a disabled one. */
  conf.enabled = true;
  conf.target_bucket = target;
  conf.target_prefix = prefix;

  bufferlist bl;
  conf.encode(bl);
  attrs[RGW_ATTR_BUCKET_LOGGING] = std::move(bl);
  return 1;
}

} // namespace

int import_one_bucket(const DoutPrefixProvider* dpp, optional_yield y,
		      NSFSDriver* driver,
		      const std::string& config_root,
		      const std::string& bucket_name,
		      const NooBaaStoreContents& contents,
		      const IdentityMapping& mapping,
		      BucketImportResult& result)
{
  /* One record, by name.  Everything else is the same operation, so
   * this narrows the input rather than taking a second path through
   * it -- a second path is a second thing to keep in step. */
  NooBaaStoreContents one;
  one.settings = contents.settings;
  one.identities = contents.identities;
  for (const auto& b : contents.buckets) {
    if (b.name == bucket_name) {
      one.buckets.push_back(b);
      break;
    }
  }
  if (one.buckets.empty()) {
    ldpp_dout(dpp, 4) << "nsfs: no NooBaa record for bucket "
      << bucket_name << dendl;
    return -ENOENT;
  }
  return import_bucket_state(dpp, y, driver, config_root, one, mapping,
			     result);
}

int import_bucket_state(const DoutPrefixProvider* dpp, optional_yield y,
			NSFSDriver* driver,
			const std::string& config_root,
			const NooBaaStoreContents& contents,
			const IdentityMapping& mapping,
			BucketImportResult& result)
{
  auto* theirs = driver->get_noobaa_bucket_state_strategy();
  if (!theirs) {
    /* no configured store, so there is nothing to read records from */
    return -ENOENT;
  }

  for (const auto& rec : contents.buckets) {
    RGWBucketInfo info;
    Attrs attrs;

    /* The name, which `load()` does not set:  it takes the name as a
     * parameter and fills the state, so an info it filled still has an
     * empty `bucket.name`.  Without this the bucket handle below names
     * nothing and its directory does not exist. */
    info.bucket.name = rec.name;

    /* NooBaa's record, through the reader that already translates most of
     * it.  `-1` for the dir_fd:  this reader finds
     * `<config_root>/buckets/<name>.json` from the name alone. */
    int ret = theirs->load(dpp, -1, rec.name, attrs, info);
    if (ret < 0) {
      result.failures.push_back(rec.name + ": " + cpp_strerror(-ret));
      continue;
    }

    /* Everything below needs the record again, for the fields the
     * reader deliberately leaves alone. */
    JSONParser p;
    std::string doc;
    const std::string path =
      config_root + "/buckets/" + rec.name + ".json";
    if ((read_record_file(dpp, path, doc) < 0) ||
	!p.parse(doc.data(), doc.size())) {
      result.failures.push_back(rec.name + ": could not re-read " + path);
      continue;
    }

    /* The owner.  An id in NooBaa's store, which is why the strategy
     * reads it and does not apply it. */
    std::string owner_account;
    try {
      JSONDecoder::decode_json("owner_account", owner_account, &p);
    } catch (const JSONDecoder::err&) {
      owner_account.clear();
    }
    if (!owner_account.empty()) {
      auto owner = mapping.account_by_noobaa_id.find(owner_account);
      if (owner == mapping.account_by_noobaa_id.end()) {
	result.failures.push_back(
	    rec.name + ": owner_account " + owner_account +
	    " is not an account this import wrote");
	continue;
      }
      info.owner = rgw_owner{rgw_account_id{owner->second}};
    }

    /* The policy, with its principals rewritten.  The strategy stored
     * it verbatim;  what it holds names identities the way *they* name
     * them. */
    if (auto i = attrs.find(RGW_ATTR_IAM_POLICY); i != attrs.end()) {
      std::vector<std::string> unresolved;
      std::string rewritten;
      ret = rewrite_policy_principals(dpp, i->second.to_str(), mapping,
				      rewritten, unresolved);
      if (ret == 0) {
	bufferlist bl;
	bl.append(rewritten);
	i->second = std::move(bl);
	++result.policies_rewritten;
      } else {
	/* The bucket's other state is correct and worth having;  a
	 * policy pointing at nothing is not.  Dropped from the write
	 * and reported (§6.1a). */
	attrs.erase(i);
	std::string why = rec.name + ": policy not written, ";
	if (unresolved.empty()) {
	  why += "it does not parse";
	} else {
	  why += "these principals resolve to nothing:";
	  for (const auto& u : unresolved) {
	    why += " " + u;
	  }
	}
	result.policies_refused.push_back(std::move(why));
      }
    }

    /* `logging`, which nothing read before. */
    if (JSONObj* lg = p.find_obj("logging")) {
      ret = carry_logging(dpp, lg, attrs);
      if (ret < 0) {
	result.failures.push_back(rec.name + ": logging");
	continue;
      }
      result.logging_carried += ret;
    }

    /* `notifications` needs an RGW topic per destination, and the
     * topic tables to go with them.  Read and reported rather than
     * invented. */
    if (p.find_obj("notifications")) {
      result.deferred.push_back(
	  rec.name + ": notifications are not translated;  each "
	  "destination needs an RGW topic");
    }

    /* BucketOwnerEnforced (DECISIONS.md 2026-10-11).  NooBaa records no
     * per-object owner and gives every object its bucket's
     * `owner_account`;  this is RGW's name for that rule, so an object
     * RGW writes into the adopted bucket is the bucket owner's too, and
     * object ACLs stop applying, as they never applied in NooBaa. */
    {
      rgw::s3::OwnershipControls oc;
      oc.object_ownership = rgw::s3::ObjectOwnership::BucketOwnerEnforced;
      bufferlist bl;
      encode(oc, bl);
      attrs[RGW_ATTR_OWNERSHIP_CONTROLS] = std::move(bl);
    }

    /* `path` not under the base path.
     *
     * Serving it from there is done -- P2 resolves an index row and
     * P5 falls back to this very record -- so what is left is
     * *recording* it in the index, which is P6.  Reported so an
     * operator knows the bucket is being served from NooBaa's record
     * rather than from a fact nsfs holds. */
    std::string their_path;
    try {
      JSONDecoder::decode_json("path", their_path, &p);
    } catch (const JSONDecoder::err&) {
      their_path.clear();
    }
    /* A bucket nsfs already holds under this name, somewhere else, is a
     * different bucket:  creation refuses a name NooBaa's store holds,
     * so this is a record that appeared after the bucket did.  Refused,
     * not merged into it. */
    {
      nsfs::BucketPlacement row;
      if ((driver->get_identity_db()->get_bucket_placement(
	     dpp, rec.name, "", row) == 0) &&
	  !their_path.empty() && (row.path != their_path)) {
	result.failures.push_back(
	    rec.name + ": nsfs already has a bucket of this name, at " +
	    row.path + ";  their record says " + their_path);
	continue;
      }
    }
    if (!their_path.empty()) {
      const std::string expected =
	driver->get_base_path() + "/" + rec.name;
      if (their_path != expected) {
	result.deferred.push_back(
	    rec.name + ": its storage is at " + their_path +
	    ", which is not under the base path;  it is served from their "
	    "record until the index records it (P6)");
      }
    }

    /* And write it.
     *
     * `merge_and_store_attrs()` is the path every bucket mutation
     * uses:  it takes the driver's mutation guard, merges the map, and
     * calls `write_attrs()`, which encodes `info` into the
     * `bucket_info` attribute and writes the map beside it.  So
     * setting `info` on the bucket first is what carries the
     * versioning, the creation date and the owner across;  the map
     * carries everything else.
     *
     * `load_bucket()` first, because the bucket has to be open before
     * anything can be written to it -- the handle from `get_bucket()`
     * is a name and an info, not an opened directory.  A bucket
     * NooBaa's tree holds and this cannot open is a failure worth naming
     * rather than working around. */
    std::unique_ptr<Bucket> bucket = driver->get_bucket(info);
    if (!bucket) {
      result.failures.push_back(rec.name + ": no bucket handle");
      continue;
    }
    ret = bucket->load_bucket(dpp, y);
    if (ret < 0) {
      result.failures.push_back(
	  rec.name + ": load_bucket: " + cpp_strerror(-ret));
      continue;
    }
    /* After the load, which overwrites info with what is on disk. */
    bucket->get_info() = info;
    ret = bucket->merge_and_store_attrs(dpp, attrs, y);
    if (ret < 0) {
      result.failures.push_back(
	  rec.name + ": merge_and_store_attrs: " + cpp_strerror(-ret));
      continue;
    }

    /* Whether deleting the bucket deletes its directory.  NooBaa records
     * it as should_create_underlying_storage:  false for a bucket put
     * over a directory that was already there, which a delete must leave
     * in place (`bucketspace_fs.js` delete_bucket).  Required on every
     * NooBaa record.  A record that lacks it is read as NooBaa's delete
     * reads it, as false, which leaves the directory:  the safe way to be
     * wrong, since the other deletes a tree the bucket may not own. */
    bool should_create = false;
    try {
      JSONDecoder::decode_json("should_create_underlying_storage",
			       should_create, &p);
    } catch (const JSONDecoder::err&) {
      should_create = false;
    }
    ret = static_cast<NSFSBucket*>(bucket.get())
	    ->set_owns_storage(dpp, should_create);
    if (ret < 0) {
      result.failures.push_back(
	  rec.name + ": recording its storage ownership: " +
	  cpp_strerror(-ret));
      continue;
    }
    ++result.buckets_written;
  }

  ldpp_dout(dpp, 4) << "nsfs: imported " << result.buckets_written
    << " buckets, " << result.policies_rewritten << " policies rewritten, "
    << result.policies_refused.size() << " refused, "
    << result.logging_carried << " logging configurations, "
    << result.deferred.size() << " deferred, "
    << result.failures.size() << " failures" << dendl;
  for (const auto& r : result.policies_refused) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << r << dendl;
  }
  for (const auto& d : result.deferred) {
    ldpp_dout(dpp, 1) << "nsfs: import: " << d << dendl;
  }
  for (const auto& f : result.failures) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << f << dendl;
  }
  return result.failures.empty() ? 0 : -EIO;
}

}}} // namespace rgw::sal::nsfs
