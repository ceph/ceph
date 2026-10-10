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

#include <map>
#include <set>

#include "common/errno.h"

#include "rgw_account.h"

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
 * Every field the account's row carries is compared, because a
 * member's row no longer exists to hold a different value.
 * `allow_bypass_governance` is not among them:  it is imported as a
 * grant on each identity, so a member keeps its own. */
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
   * minted account id without a second pass.  Their `owner` names a
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
     * matching consults (§6.6).  Their name, so a name-form principal
     * in a policy still resolves. */
    e.display_name = id.name;
    e.anonymous = id.is_anonymous();
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
    for (const auto& ak : id.access_keys) {
      e.access_key_ids.push_back(ak.access_key);
    }

    auto owner = account_id_by_noobaa_id.find(id.owner);
    if (owner == account_id_by_noobaa_id.end()) {
      /* A user whose account is not in the store cannot be placed.
       * Reported rather than guessed:  there is no correct account to
       * invent, and an orphan user is a store we should not import
       * from. */
      plan.collisions.push_back(
	  label(id) + " is an IAM user whose owner " + id.owner +
	  " is not an account in this store");
      ret = -EINVAL;
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
	  plan.drift.push_back(
	      label(id) + " differs from its account " + label(root) +
	      " in " + f + ";  it will run as the account");
	}
	break;
      }
    }
    plan.entries.push_back(std::move(e));
  }

  /* ---- the gate ---- */

  /* RGW's `validate_name` rejects `$` and `:` and requires valid
   * utf8.  NooBaa validates only *new* names and says so, so a legacy
   * name may contain anything -- and a name that has to be rewritten
   * invalidates every name-form principal referring to it (§6.6).
   * Reported, not rewritten:  rewriting silently breaks
   * authorization. */
  for (const auto& e : plan.entries) {
    std::string why;
    if (!rgw::account::validate_name(e.name, &why)) {
      plan.collisions.push_back(e.noobaa_name.empty()
	  ? std::string("an identity has a name RGW will not accept: ") + why
	  : e.noobaa_name + " has a name RGW will not accept: " + why);
      ret = -EINVAL;
    }
    /* An account user's display_name must be non-empty
     * (`svc_user_rados.cc`), and it is their name. */
    if (e.display_name.empty()) {
      plan.collisions.push_back(
	  std::string("_id ") + e.noobaa_id + " has no name, so it has no "
	  "display_name, which RGW requires");
      ret = -EINVAL;
    }
  }

  /* Account name, unique per tenant -- and every tenant here is the
   * same one, so globally unique. */
  {
    std::map<std::string, std::string> seen;
    for (const auto& e : plan.entries) {
      if (e.iam_user) {
	continue;
      }
      const std::string key = e.tenant + "$" + e.name;
      auto [i, fresh] = seen.emplace(key, e.noobaa_id);
      if (!fresh) {
	plan.collisions.push_back(
	    "two accounts are named " + e.name + " (_id " + i->second +
	    " and _id " + e.noobaa_id + "), and RGW requires an account "
	    "name to be unique");
	ret = -EEXIST;
      }
    }
  }

  /* Email, globally unique **across users and accounts**.  This is
   * the one §6.3 names as the likeliest first failure, because their
   * email is a copy of the name. */
  {
    std::map<std::string, std::string> seen;
    for (const auto& e : plan.entries) {
      if (e.email.empty()) {
	continue;
      }
      auto [i, fresh] = seen.emplace(e.email, e.noobaa_id);
      if (!fresh) {
	plan.collisions.push_back(
	    "_id " + i->second + " and _id " + e.noobaa_id +
	    " both carry the email \"" + e.email + "\", and RGW's email "
	    "index is shared across users and accounts.  Their email is a "
	    "copy of the name, so two identities of the same name collide "
	    "even under different accounts");
	ret = -EEXIST;
      }
    }
  }

  /* Access key id, globally unique. */
  {
    std::map<std::string, std::string> seen;
    for (const auto& e : plan.entries) {
      for (const auto& k : e.access_key_ids) {
	auto [i, fresh] = seen.emplace(k, e.noobaa_id);
	if (!fresh) {
	  plan.collisions.push_back(
	      "_id " + i->second + " and _id " + e.noobaa_id +
	      " both carry the access key " + k +
	      ", which RGW requires to be unique");
	  ret = -EEXIST;
	}
      }
    }
  }

  /* An account user's display_name, unique within its account. */
  {
    std::set<std::pair<std::string, std::string>> seen;
    for (const auto& e : plan.entries) {
      if (e.account_id.empty()) {
	continue;
      }
      if (!seen.emplace(e.account_id, e.display_name).second) {
	plan.collisions.push_back(
	    "account " + e.account_id + " has two users whose display_name "
	    "is \"" + e.display_name + "\", which RGW requires to be unique "
	    "within an account");
	ret = -EEXIST;
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

  ldpp_dout(dpp, 4) << "nsfs: import plan: " << plan.accounts()
    << " accounts, " << plan.iam_users() << " IAM users, "
    << plan.collisions.size() << " collisions, " << plan.drift.size()
    << " drifted" << dendl;
  for (const auto& c : plan.collisions) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << c << dendl;
  }
  for (const auto& d : plan.drift) {
    ldpp_dout(dpp, 0) << "WARNING: import: " << d << dendl;
  }
  return ret;
}

}}} // namespace rgw::sal::nsfs
