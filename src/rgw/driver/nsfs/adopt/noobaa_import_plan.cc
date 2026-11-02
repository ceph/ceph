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

#include <algorithm>
#include <map>
#include <set>

#include "common/errno.h"

#include "rgw_account.h"
#include "rgw_sal.h"
#include "rgw_rest_iam.h"

#include "noobaa_import_plan.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

size_t ImportPlan::accounts() const
{
  size_t n = 0;
  for (const auto& e : entries) {
    if (!e.iam_user) {
      ++n;
    }
  }
  return n;
}

size_t ImportPlan::iam_users() const
{
  size_t n = 0;
  for (const auto& e : entries) {
    if (e.iam_user) {
      ++n;
    }
  }
  return n;
}

std::vector<std::string> ImportPlan::failures() const
{
  std::vector<std::string> out;
  for (const auto& e : entries) {
    for (const auto& f : e.failed) {
      if (std::find(out.begin(), out.end(), f) == out.end()) {
	out.push_back(f);
      }
    }
  }
  return out;
}

void fail_users_of_failed_accounts(ImportPlan& plan)
{
  for (auto& u : plan.entries) {
    if (!u.iam_user || !u.failed.empty() || u.account_id.empty()) {
      continue;
    }
    auto a = std::find_if(plan.entries.begin(), plan.entries.end(),
			  [&](const ImportEntry& e) {
			    return !e.iam_user && e.account_id == u.account_id;
			  });
    if ((a != plan.entries.end()) && !a->failed.empty()) {
      u.failed.push_back(u.noobaa_name + " is an IAM user of the account " +
			 a->noobaa_name + ", which is not imported");
    }
  }
}

namespace {

/* `<name>` or, for an identity we can only name by its id, the id.
 * Used in collision messages, which an operator reads to go and fix
 * something -- so it has to be the thing they can look up. */
std::string label(const NooBaaIdentity& id)
{
  if (!id.name.empty()) {
    return id.name;
  }
  return std::string("_id ") + id.id;
}

/* The fields of a member's identity row that differ from its
 * account's, by name.
 *
 * Every field the account's row carries is compared:  the member's own
 * row holds the difference, and without the user identity override
 * the member runs as the account and does not see it (I9).
 * `allow_bypass_governance` is not among them:  it is imported as a
 * grant on each IAM user, so a member keeps its own. */
std::vector<std::string> row_drift(const NooBaaIdentity& account,
				   const NooBaaIdentity& member)
{
  const auto& a = account.posix;
  const auto& m = member.posix;
  std::vector<std::string> d;
  if ((a.kind != m.kind) || (a.uid != m.uid) || (a.gid != m.gid)) {
    d.push_back("uid/gid");
  }
  if (a.distinguished_name != m.distinguished_name) {
    d.push_back("distinguished_name");
  }
  if ((a.have_supplemental_groups != m.have_supplemental_groups) ||
      (a.supplemental_groups != m.supplemental_groups)) {
    d.push_back("supplemental_groups");
  }
  if (a.new_buckets_path != m.new_buckets_path) {
    d.push_back("new_buckets_path");
  }
  if (a.custom_bucket_path_allowed_list != m.custom_bucket_path_allowed_list) {
    d.push_back("custom_bucket_path_allowed_list");
  }
  if (a.fs_backend != m.fs_backend) {
    d.push_back("fs_backend");
  }
  if ((account.have_force_md5_etag != member.have_force_md5_etag) ||
      (account.force_md5_etag != member.force_md5_etag)) {
    d.push_back("force_md5_etag");
  }
  return d;
}

} // namespace

int build_import_plan(const DoutPrefixProvider* dpp, CephContext* cct,
		      const NooBaaStoreContents& contents,
		      ImportPlan& plan,
		      const IdentityDB* stored)
{
  plan = ImportPlan{};

  /* Root accounts first, so an IAM user can be given its owner's
   * minted account id without a second pass.  NooBaa's `owner` names a
   * root account's `_id`. */
  std::map<std::string, std::string> account_id_by_noobaa_id;
  int ret = 0;

  for (const auto& id : contents.identities) {
    if (id.is_iam_user()) {
      continue;
    }
    ImportEntry e;
    e.noobaa_id = id.id;
    e.noobaa_name = id.name;
    /* Reuse the id this identity was imported with, if it was.  A
     * second mint for an account that already exists is the defect
     * this exists to avoid:  the bucket would be written an owner
     * naming an account no imported user belongs to. */
    e.account_id.clear();
    if (stored) {
      std::string owner;
      if (stored->get_owner_by_noobaa_id(dpp, id.id, owner) == 0) {
	/* to_string(rgw_owner) -- for a root account, the account id,
	 * which is the same thing a bucket's OwnerID holds, so reusing
	 * it is what makes the two agree. */
	e.account_id = owner;
	ldpp_dout(dpp, 10) << "nsfs: import plan: " << id.name
	  << " keeps account " << e.account_id << dendl;
      }
    }
    if (e.account_id.empty()) {
      e.account_id = rgw::account::generate_id(cct);
    }
    e.name = id.name;
    e.email = id.email;
    /* The account user's display_name, which is what principal
     * matching consults (§6.6).  NooBaa's name, so a name-form principal
     * in a policy still resolves. */
    e.display_name = id.name;
    e.anonymous = id.is_anonymous();
    if (!e.anonymous && !id.posix.allow_bypass_governance) {
      plan.widened.push_back(
	  "account " + label(id) + ":  NooBaa did not allow it to bypass "
	  "governance retention, and as RGW's account root it can");
    }
    for (const auto& ak : id.access_keys) {
      e.access_key_ids.push_back(ak.access_key);
    }
    account_id_by_noobaa_id[e.noobaa_id] = e.account_id;
    plan.entries.push_back(std::move(e));
  }

  for (const auto& id : contents.identities) {
    if (!id.is_iam_user()) {
      continue;
    }
    ImportEntry e;
    e.iam_user = true;
    e.noobaa_id = id.id;
    e.noobaa_name = id.name;
    e.name = id.name;
    e.email = id.email;
    e.display_name = id.name;
    e.iam_path = id.iam_path;
    for (const auto& ak : id.access_keys) {
      e.access_key_ids.push_back(ak.access_key);
    }

    auto owner = account_id_by_noobaa_id.find(id.owner);
    std::string why;
    if (!id.iam_path.empty() && !validate_iam_path(id.iam_path, why)) {
      e.failed.push_back(
	  label(id) + " has an IAM path RGW will not accept (" + id.iam_path +
	  "): " + why);
    }

    if (owner == account_id_by_noobaa_id.end()) {
      /* A user whose account is not in the store cannot be placed.
       * Reported rather than guessed:  there is no correct account to
       * invent. */
      e.failed.push_back(
	  label(id) + " is an IAM user whose owner " + id.owner +
	  " is not an account in this store");
    } else {
      e.account_id = owner->second;
      for (const auto& root : contents.identities) {
	if (root.id != id.owner) {
	  continue;
	}
	const auto fields = row_drift(root, id);
	if (!fields.empty()) {
	  std::string f;
	  for (const auto& x : fields) {
	    f += (f.empty() ? "" : ", ") + x;
	  }
	  /* what it runs as is the override's, so the report says which,
	   * and names the setting that serves it as NooBaa did (I9) */
	  const bool overriding = stored ? stored->user_identity_override()
	    : (cct && cct->_conf.get_val<bool>("rgw_nsfs_user_identity_override"));
	  plan.drift.push_back(
	      label(id) + " differs from its account " + label(root) +
	      " in " + f + ";  its own row is imported, and " +
	      (overriding
	       ? "rgw_nsfs_user_identity_override serves it from that row, "
		 "as NooBaa did"
	       : "it runs as the account until rgw_nsfs_user_identity_override "
		 "is on, which serves it from that row as NooBaa did"));
	}
	break;
      }
    }
    plan.entries.push_back(std::move(e));
  }

  /* ---- the limits ---- */

  /* RGW counts an account's root user among its users, so an account
   * holds one more than its IAM users.  A key limit is per user, so the
   * account's is the most any one of its identities carries. */
  {
    std::map<std::string, std::pair<int32_t, int32_t>> need;
    for (const auto& e : plan.entries) {
      if (e.anonymous || e.account_id.empty()) {
	continue;
      }
      auto& [users, keys] = need[e.account_id];
      ++users;
      keys = std::max(keys, static_cast<int32_t>(e.access_key_ids.size()));
    }
    for (auto& e : plan.entries) {
      if (e.iam_user || e.anonymous) {
	continue;
      }
      const auto& [users, keys] = need[e.account_id];
      if (users > RGWAccountInfo::DEFAULT_USER_LIMIT) {
	e.max_users = users;
	plan.raised.push_back(
	    "account " + e.name + " holds " + std::to_string(users) +
	    " users, above RGW's default max_users of " +
	    std::to_string(RGWAccountInfo::DEFAULT_USER_LIMIT) +
	    ";  it is created with max_users " + std::to_string(users));
      }
      if (keys > RGWAccountInfo::DEFAULT_ACCESS_KEY_LIMIT) {
	e.max_access_keys = keys;
	plan.raised.push_back(
	    "account " + e.name + " has a user holding " +
	    std::to_string(keys) + " access keys, above RGW's default "
	    "max_access_keys of " +
	    std::to_string(RGWAccountInfo::DEFAULT_ACCESS_KEY_LIMIT) +
	    ";  it is created with max_access_keys " + std::to_string(keys));
      }
    }
  }

  /* ---- the gate ---- */

  /* RGW's `validate_name` rejects `$` and `:` and requires valid
   * utf8.  NooBaa validates only *new* names and says so, so a legacy
   * name may contain anything -- and a name that has to be rewritten
   * invalidates every name-form principal referring to it (§6.6).
   * Reported, not rewritten:  rewriting silently breaks
   * authorization. */
  for (auto& e : plan.entries) {
    std::string why;
    if (!rgw::account::validate_name(e.name, &why)) {
      e.failed.push_back(e.noobaa_name.empty()
	  ? "_id " + e.noobaa_id + " has a name RGW will not accept: " + why
	  : e.noobaa_name + " has a name RGW will not accept: " + why);
    }
    /* An account user's display_name must be non-empty
     * (`svc_user_rados.cc`), and it is the NooBaa name. */
    if (e.display_name.empty()) {
      e.failed.push_back(
	  std::string("_id ") + e.noobaa_id + " has no name, so it has no "
	  "display_name, which RGW requires");
    } else if (!e.anonymous && !validate_iam_user_name(e.display_name, why)) {
      /* RGW's user-add path holds every account user's display_name,
       * the root's included, to the IAM UserName rules */
      e.failed.push_back(
	  e.noobaa_name + " has a name RGW will not accept as an account "
	  "user's: " + why);
    }
  }

  /* Each constraint below fails both records it names. */
  auto fail_both = [&plan](size_t a, size_t b, const std::string& why) {
    plan.entries[a].failed.push_back(why);
    plan.entries[b].failed.push_back(why);
  };

  /* Account name, unique per tenant -- and every tenant here is the
   * same one, so globally unique. */
  {
    std::map<std::string, size_t> seen;
    for (size_t n = 0; n < plan.entries.size(); ++n) {
      const auto& e = plan.entries[n];
      if (e.iam_user) {
	continue;
      }
      const std::string key = e.tenant + "$" + e.name;
      auto [i, fresh] = seen.emplace(key, n);
      if (!fresh) {
	fail_both(i->second, n,
	    "two accounts are named " + e.name + " (_id " +
	    plan.entries[i->second].noobaa_id + " and _id " + e.noobaa_id +
	    "), and RGW requires an account name to be unique");
      }
    }
  }

  /* Email, globally unique **across users and accounts**.  Only an
   * account's is written:  an IAM user is written with no email, as an
   * AWS IAM user has none, so two users of one name under different
   * accounts -- legal in NooBaa and in AWS -- do not collide. */
  {
    std::map<std::string, size_t> seen;
    for (size_t n = 0; n < plan.entries.size(); ++n) {
      const auto& e = plan.entries[n];
      if (e.iam_user || e.email.empty()) {
	continue;
      }
      auto [i, fresh] = seen.emplace(e.email, n);
      if (!fresh) {
	fail_both(i->second, n,
	    "_id " + plan.entries[i->second].noobaa_id + " and _id " +
	    e.noobaa_id + " both carry the email \"" + e.email + "\", and "
	    "RGW's email index is shared across users and accounts");
      }
    }
  }

  /* Access key id, globally unique. */
  {
    std::map<std::string, size_t> seen;
    for (size_t n = 0; n < plan.entries.size(); ++n) {
      const auto& e = plan.entries[n];
      for (const auto& k : e.access_key_ids) {
	auto [i, fresh] = seen.emplace(k, n);
	if (!fresh) {
	  fail_both(i->second, n,
	      "_id " + plan.entries[i->second].noobaa_id + " and _id " +
	      e.noobaa_id + " both carry the access key " + k +
	      ", which RGW requires to be unique");
	}
      }
    }
  }

  /* An account user's display_name, unique within its account. */
  {
    std::map<std::pair<std::string, std::string>, size_t> seen;
    for (size_t n = 0; n < plan.entries.size(); ++n) {
      const auto& e = plan.entries[n];
      if (e.account_id.empty()) {
	continue;
      }
      auto [i, fresh] = seen.emplace(std::pair{e.account_id, e.display_name}, n);
      if (!fresh) {
	fail_both(i->second, n,
	    "account " + e.account_id + " has two users whose display_name "
	    "is \"" + e.display_name + "\" (_id " +
	    plan.entries[i->second].noobaa_id + " and _id " + e.noobaa_id +
	    "), which RGW requires to be unique within an account");
      }
    }
  }

  /* The NooBaa id itself, which nothing else in the store should
   * repeat.  I1 keys identities by their directory, so a repeat means
   * two records claiming one `_id` -- and the id is what re-import
   * recognises an identity by, so a duplicate makes that recognition
   * ambiguous. */
  {
    std::set<std::string> seen;
    for (const auto& e : plan.entries) {
      if (!seen.insert(e.noobaa_id).second) {
	plan.collisions.push_back(
	    "_id " + e.noobaa_id + " appears on more than one record, and "
	    "re-import recognises an identity by it");
	ret = -EEXIST;
      }
    }
  }

  /* An identity already imported stays imported:  a collision with a
   * record added since fails only the newcomer. */
  if (stored) {
    for (auto& e : plan.entries) {
      std::string owner;
      if (!e.failed.empty() &&
	  (stored->get_owner_by_noobaa_id(dpp, e.noobaa_id, owner) == 0)) {
	e.failed.clear();
      }
    }
  }
  fail_users_of_failed_accounts(plan);

  const auto failures = plan.failures();
  ldpp_dout(dpp, 4) << "nsfs: import plan: " << plan.accounts()
    << " accounts, " << plan.iam_users() << " IAM users, "
    << failures.size() << " failures, "
    << plan.collisions.size() << " collisions, " << plan.drift.size()
    << " drifted, " << plan.raised.size() << " limits raised, "
    << plan.widened.size() << " roots widened" << dendl;
  for (const auto& f : failures) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << f << dendl;
  }
  for (const auto& c : plan.collisions) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << c << dendl;
  }
  for (const auto& d : plan.drift) {
    ldpp_dout(dpp, 0) << "WARNING: import: " << d << dendl;
  }
  for (const auto& l : plan.raised) {
    ldpp_dout(dpp, 0) << "NOTICE: import: " << l << dendl;
  }
  for (const auto& w : plan.widened) {
    ldpp_dout(dpp, 0) << "NOTICE: import: " << w << dendl;
  }
  return ret;
}

}}} // namespace rgw::sal::nsfs
