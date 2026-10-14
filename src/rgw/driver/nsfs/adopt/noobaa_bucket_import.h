// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Writing a NooBaa store's bucket state into RGW.
 *
 * I5 of the account import (`ACCOUNT_IMPORT.md` §6.1 phase 2, §9).
 * Runs after I4, because a policy principal resolves through the
 * identity mapping and the mapping is not complete until every
 * identity is written.
 *
 * MOST OF THE TRANSLATION ALREADY EXISTS.
 * `NooBaaBucketStateStrategy::load()` reads NooBaa's record and fills an
 * `RGWBucketInfo` and an attribute map -- versioning, creation date,
 * object lock, public access block, website, encryption, tags,
 * lifecycle and CORS.  What it does **not** do is the three things
 * that need more than the record:
 *
 *   owner_account   an id in NooBaa's store.  Turning one into an
 *                   `rgw_owner` needs the identity mapping, so the
 *                   strategy reads it and does not apply it
 *   s3_policy       stored verbatim, principals untouched.  NooBaa's
 *                   name identities the way *they* name them
 *   logging         not read at all, and RGW has the feature
 *
 * So I5 is the mapping, the rewriting, the two unread fields, and the
 * write.
 *
 * REWRITING PRINCIPALS IS THE PART WITH TEETH.  A principal that
 * resolves to nothing is a grant or a denial that will not take
 * effect, and nobody notices until an access decision goes the wrong
 * way.  NooBaa's principals are bare strings -- an account's `email` is
 * a copy of its name -- and each must be resolved against the
 * imported identity set, **id first and then name**, mirroring
 * `config_fs.js:662-678`.  A principal may name an IAM user or a
 * role, not only an account, so the mapping is
 * `NooBaa id -> (account id | user ARN)`.
 *
 * **The document is a DOM, not a template.**  RGW stores a policy as
 * its own source text -- `from_noobaa_bucket_policy` keeps `p.text`
 * -- and has no serialiser, so rewriting means changing strings
 * inside a document whose storage form is the text it arrived as.
 * This uses rapidjson, which `rgw_iam_policy.cc` already builds on:
 * parse to a DOM, replace the values under `Principal` and nothing
 * else, serialise back.  Every member we do not understand survives
 * because it was never modelled, and no value outside a `Principal`
 * can be touched because none is visited.  A string substitution over
 * the text would have risked rewriting a resource or a condition
 * value that happened to equal a principal.
 *
 * **An unresolved principal refuses the policy, not the bucket.**
 * The bucket's other state is correct and worth having;  the policy
 * is not, and writing a policy whose principals point at nothing is
 * worse than writing none and saying so (§6.1a).  Reported either way
 * -- §6.1 phase 3 makes this the report an operator must read.
 *
 * WHAT WAITS FOR ANOTHER STAGE.  `path` and
 * `should_create_underlying_storage` are **P6's**:  the placement
 * table exists as of P1, but putting a row in it is what P6 is, and
 * resolution is P2.  A bucket whose path is outside the data root is
 * reported here, which is all this stage can honestly do about it.
 * `notifications` needs an RGW topic per destination and the topic
 * tables to go with them;  read and reported, not translated.
 */

#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "common/dout.h"

#include "noobaa_import_plan.h"

namespace rgw { namespace sal {

class NSFSDriver;

namespace nsfs {

/* NooBaa's identifiers, to what an RGW principal should say.
 *
 * Built from the plan, so it is the same mapping I4 wrote by.  Not
 * durable:  the NooBaa id table (IdentityDB::get_owner_by_noobaa_id)
 * is the durable form, and a later run rebuilds this from it. */
struct IdentityMapping {
  /* NooBaa's `_id` -> the RGW account id */
  std::map<std::string, std::string> account_by_noobaa_id;
  /* NooBaa's `_id` -> `arn:aws:iam::<account>:user/<name>`, for an IAM
   * user.  RGW matches a user principal on `display_name`, which the
   * import set to the NooBaa name (§6.6) */
  std::map<std::string, std::string> user_arn_by_noobaa_id;
  /* NooBaa name -> NooBaa `_id`, for the second half of "id first, then
   * name" */
  std::map<std::string, std::string> noobaa_id_by_name;

  /* Id first, then name.  Nothing if neither resolves -- which is a
   * fact to report, not a value to invent. */
  std::optional<std::string> resolve(const std::string& principal) const;
};

IdentityMapping mapping_from(const ImportPlan& plan);

/* Rewrite a policy document's principals.
 *
 * Returns 0 and the rewritten document, or -ENOENT when any principal
 * resolves to nothing -- with each one named in `unresolved`, because
 * an operator fixing them needs the list rather than the first.
 * -EBADMSG when the document will not parse.
 *
 * Only values under `Principal` are visited.  Exposed for its own
 * tests:  it is the part of this stage where being subtly wrong
 * changes who may do what. */
int rewrite_policy_principals(const DoutPrefixProvider* dpp,
			      const std::string& doc,
			      const IdentityMapping& mapping,
			      std::string& out,
			      std::vector<std::string>& unresolved);

struct BucketImportResult {
  size_t buckets_written{0};
  size_t policies_rewritten{0};
  size_t logging_carried{0};

  /* A policy we would not write, and why.  The bucket's other state
   * was written. */
  std::vector<std::string> policies_refused;
  /* Read and not translated:  notifications, which need topics. */
  std::vector<std::string> deferred;
  std::vector<std::string> failures;
};

/* Write one bucket's state, named.
 *
 * The import is one operation over the whole store (§6.1), because a
 * policy principal resolves through the complete identity mapping and
 * the mapping is not complete until every identity is written.  So
 * this is **not** a smaller import:  it is the bucket half of one,
 * run against a mapping that already exists, which is what the
 * background sweep will do per bucket once it exists (C9).
 *
 * Returns -ENOENT when the store holds no record of that name. */
int import_one_bucket(const DoutPrefixProvider* dpp, optional_yield y,
		      NSFSDriver* driver,
		      const std::string& config_root,
		      const std::string& bucket_name,
		      const NooBaaStoreContents& contents,
		      const IdentityMapping& mapping,
		      BucketImportResult& result);

/* Write every bucket's state.
 *
 * `config_root` is the store, passed rather than asked of the
 * strategy:  this reads NooBaa's record a second time for the fields the
 * strategy leaves alone, so it needs the directory, and a function
 * that needs something should be given it.
 *
 * `mapping` comes from the identities this run imported.  Returns 0
 * when every bucket's state was written -- a refused policy is not a
 * failure of the bucket -- and -EIO otherwise. */
int import_bucket_state(const DoutPrefixProvider* dpp, optional_yield y,
			NSFSDriver* driver,
			const std::string& config_root,
			const NooBaaStoreContents& contents,
			const IdentityMapping& mapping,
			BucketImportResult& result);

}}} // namespace rgw::sal::nsfs
