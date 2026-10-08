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

#include "common/ceph_json.h"
#include "common/errno.h"

#include "rgw_account.h"
#include "rgw_auth.h"
#include "rgw_common.h"
#include "rgw_role.h"

#include "../rgw_sal_nsfs.h"
#include "noobaa_import.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

namespace {

const NooBaaIdentity* find_source(const NooBaaStoreContents& contents,
				  const std::string& noobaa_id)
{
  for (const auto& i : contents.identities) {
    if (i.id == noobaa_id) {
      return &i;
    }
  }
  return nullptr;
}

/* Their POSIX arm into ours.
 *
 * `uid`/`gid` or a distinguished name, never both -- their schema is a
 * `oneOf` and ours is a CHECK, so a record that violated it could not
 * be stored here either.  `user` takes the distinguished-name arm for
 * the anonymous account (§6.8). */
void fill_posix(const NooBaaIdentity& src, Identity& row)
{
  switch (src.posix.kind) {
  case NooBaaPosixConfig::Kind::Ids:
    row.uid = static_cast<uint32_t>(src.posix.uid);
    row.gid = static_cast<uint32_t>(src.posix.gid);
    break;
  case NooBaaPosixConfig::Kind::DistinguishedName:
    row.distinguished_name = src.posix.distinguished_name;
    break;
  case NooBaaPosixConfig::Kind::Unset:
    break;
  }

  /* An explicit list overrides the directory entirely, and an empty
   * list is not the same as no list:  empty means no supplementary
   * groups, which is a grant decision (§2.4).  The column carries the
   * same three states, so the distinction survives. */
  if (src.posix.have_supplemental_groups) {
    std::vector<uint32_t> g;
    for (int64_t v : src.posix.supplemental_groups) {
      g.push_back(static_cast<uint32_t>(v));
    }
    row.groups = std::move(g);
  }

  row.new_buckets_path = src.posix.new_buckets_path;
  row.custom_bucket_path_allowed_list =
    src.posix.custom_bucket_path_allowed_list;
  row.fs_backend = src.posix.fs_backend;

  if (src.have_force_md5_etag) {
    row.force_md5_etag = src.force_md5_etag;
  }
}

/* The grant their `allow_bypass_governance` means.
 *
 * Their flag is a boolean on the account's `nsfs_account_config`;
 * RGW expresses the same permission as an IAM action, so the import
 * writes the policy rather than inventing a column
 * (`ACCOUNT_IMPORT.md` §2.2).  Named so a reader of the policy list
 * can tell where it came from. */
const char* bypass_governance_policy()
{
  return R"({"Version":"2012-10-17","Statement":[{"Effect":"Allow",)"
	 R"("Action":["s3:BypassGovernanceRetention"],)"
	 R"("Resource":["arn:aws:s3:::*"]}]})";
}

} // namespace

int check_rgw_state(const DoutPrefixProvider* dpp, optional_yield y,
		    NSFSDriver* driver, const ImportPlan& plan,
		    std::vector<std::string>& collisions)
{
  IdentityDB* db = driver->get_identity_db();
  if (!db) {
    return -ENOENT;
  }

  for (const auto& e : plan.entries) {
    /* Already imported is not a collision.  Recognised by their id,
     * which is the whole reason the column exists -- a name-keyed
     * check would report every renamed account as a conflict with
     * itself. */
    Identity existing;
    if (db->get_identity_by_noobaa_id(dpp, e.noobaa_id, existing) == 0) {
      continue;
    }
    if (e.anonymous) {
      /* No account and no user, so there is no name or email to
       * collide.  The row either exists or does not. */
      continue;
    }

    RGWAccountInfo info;
    Attrs attrs;
    RGWObjVersionTracker objv;

    if (!e.iam_user) {
      if (driver->load_account_by_name(dpp, y, e.tenant, e.name,
				       info, attrs, objv) == 0) {
	collisions.push_back(
	    "an RGW account named " + e.name + " already exists, and it is "
	    "not the one _id " + e.noobaa_id + " was imported as");
      }
    }
    if (!e.email.empty() &&
	driver->load_account_by_email(dpp, y, e.email,
				      info, attrs, objv) == 0) {
      collisions.push_back(
	  "the email \"" + e.email + "\" is already in RGW's index, which "
	  "is shared across users and accounts, so _id " + e.noobaa_id +
	  " cannot take it");
    }
  }

  for (const auto& c : collisions) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << c << dendl;
  }
  return collisions.empty() ? 0 : -EEXIST;
}

int import_identities(const DoutPrefixProvider* dpp, optional_yield y,
		      NSFSDriver* driver,
		      const NooBaaStoreContents& contents,
		      const ImportPlan& plan,
		      ImportResult& result)
{
  IdentityDB* db = driver->get_identity_db();
  if (!db) {
    return -ENOENT;
  }

  /* `noobaa_id` -> the owner string an identity row is keyed on, for
   * this run.  In memory, because the column is the durable form
   * (§3.4). */
  std::map<std::string, std::string> owner_by_noobaa_id;

  auto fail = [&result](const std::string& who, const std::string& what) {
    result.failures.push_back(who + ": " + what);
  };

  /* ---- pass one:  identities ---- */

  for (const auto& e : plan.entries) {
    const NooBaaIdentity* src = find_source(contents, e.noobaa_id);
    if (!src) {
      fail(e.noobaa_name, "no source record, which cannot happen unless the "
	   "plan and the store disagree");
      continue;
    }

    /* Already imported?  Their id is the key.  Skipped rather than
     * rewritten:  their store is the source, but an identity RGW
     * already holds may have been changed on the RGW side since, and
     * reconciling divergence is explicitly not attempted (§6.2). */
    Identity existing;
    if (db->get_identity_by_noobaa_id(dpp, e.noobaa_id, existing) == 0) {
      owner_by_noobaa_id[e.noobaa_id] = existing.key;
      if (e.iam_user) {
	++result.users_skipped;
      } else {
	++result.accounts_skipped;
      }
      continue;
    }

    /* The anonymous account:  a row and nothing else.  RGW supplies
     * the owner string, so nothing is minted (§6.8). */
    if (e.anonymous) {
      Identity row;
      row.key = RGW_USER_ANON_ID;
      row.noobaa_id = e.noobaa_id;
      fill_posix(*src, row);
      int ret = db->put_identity(dpp, row);
      if (ret < 0) {
	fail("anonymous", cpp_strerror(-ret));
	continue;
      }
      owner_by_noobaa_id[e.noobaa_id] = row.key;
      result.anonymous_imported = true;
      ++result.identity_rows;
      continue;
    }

    /* A root account:  the account, then the user that holds its
     * keys.  An IAM user:  only the user, in its owner's account. */
    if (!e.iam_user) {
      RGWAccountInfo ai;
      ai.id = e.account_id;
      ai.tenant = e.tenant;
      ai.name = e.name;
      ai.email = e.email;

      Attrs attrs;
      RGWObjVersionTracker objv;
      constexpr bool exclusive = true;
      int ret = driver->store_account(dpp, y, exclusive, ai, nullptr,
				      attrs, objv);
      if (ret < 0) {
	fail(e.name, std::string("store_account: ") + cpp_strerror(-ret));
	continue;
      }
      ++result.accounts_created;
    }

    RGWUserInfo ui;
    ui.account_id = e.account_id;
    /* What principal matching consults for an account user, so it
     * carries their name and a name-form principal still resolves
     * (§6.6). */
    ui.display_name = e.display_name;
    ui.path = e.iam_user && !src->iam_path.empty() ? src->iam_path : "/";
    ui.create_date = ceph::real_clock::now();
    /* A random UUID, as `CreateUser` mints.  Their `_id` is not usable
     * here -- it is not an RGW id -- and the mapping back to it lives
     * in the identity row. */
    {
      uuid_d uuid;
      uuid.generate_random();
      ui.user_id.id = uuid.to_string();
      ui.user_id.tenant = e.tenant;
    }

    /* The decrypted keys.  I2 opened them;  an empty secret here
     * means I2 was not run or refused, and a user written with no
     * usable credential is worse than one not written. */
    for (const auto& ak : src->access_keys) {
      if (ak.secret_key.empty()) {
	fail(e.name, "access key " + ak.access_key + " has no decrypted "
	     "secret;  run the master-key step first");
	continue;
      }
      RGWAccessKey k;
      k.id = ak.access_key;
      k.key = ak.secret_key;
      k.active = !ak.deactivated;
      ui.access_keys[ak.access_key] = std::move(k);
    }
    if (ui.access_keys.size() != src->access_keys.size()) {
      /* some key could not be carried;  already reported above */
      continue;
    }

    std::unique_ptr<User> user = driver->get_user(ui.user_id);
    user->get_info() = ui;

    /* Their `allow_bypass_governance` as an IAM grant on the user.
     * RGW expresses the permission as an action, so the import writes
     * a policy rather than a column (§2.2). */
    if (src->posix.allow_bypass_governance) {
      std::map<std::string, std::string> policies;
      policies["noobaa-allow-bypass-governance"] =
	bypass_governance_policy();
      bufferlist bl;
      ceph::encode(policies, bl);
      user->get_attrs()[RGW_ATTR_USER_POLICY] = std::move(bl);
      ++result.grants_written;
    }

    constexpr bool exclusive = true;
    int ret = user->store_user(dpp, y, exclusive, nullptr);
    if (ret < 0) {
      fail(e.name, std::string("store_user: ") + cpp_strerror(-ret));
      continue;
    }
    ++result.users_created;

    /* The nsfs row:  the POSIX arm, the placement fields and their
     * id.  Keyed on the flattened owner, which for an account user is
     * the account rather than the user -- impersonation resolves an
     * owner, and the account is what a bucket is owned by. */
    Identity row;
    row.key = e.account_id;
    row.noobaa_id = e.noobaa_id;
    fill_posix(*src, row);
    ret = db->put_identity(dpp, row);
    if (ret < 0) {
      fail(e.name, std::string("put_identity: ") + cpp_strerror(-ret));
      continue;
    }
    owner_by_noobaa_id[e.noobaa_id] = row.key;
    ++result.identity_rows;
  }

  /* ---- pass two:  the policies that need the finished mapping ---- */

  for (const auto& e : plan.entries) {
    const NooBaaIdentity* src = find_source(contents, e.noobaa_id);
    if (!src || src->role_config.empty()) {
      continue;
    }
    auto owner = owner_by_noobaa_id.find(e.noobaa_id);
    if (owner == owner_by_noobaa_id.end()) {
      /* its identity was not written, so there is nothing to attach a
       * role to;  the failure is already recorded */
      continue;
    }

    /* Their `role_config` carries a role name and an assume-role
     * policy document.  The document's principals name identities by
     * their email, which is their name -- so the mapping is what makes
     * it resolvable, and that is why this is a second pass. */
    JSONParser p;
    if (!p.parse(src->role_config.data(), src->role_config.size())) {
      fail(e.noobaa_name, "role_config is not JSON");
      continue;
    }
    std::string role_name;
    try {
      JSONDecoder::decode_json("role_name", role_name, &p);
    } catch (const JSONDecoder::err& e2) {
      fail(e.noobaa_name, std::string("role_config: ") + e2.what());
      continue;
    }
    if (role_name.empty()) {
      fail(e.noobaa_name, "role_config names no role");
      continue;
    }

    JSONObj* doc = p.find_obj("assume_role_policy_document");
    if (!doc) {
      fail(e.noobaa_name, "role_config has no assume_role_policy_document");
      continue;
    }

    RGWRoleInfo ri;
    ri.name = role_name;
    ri.path = "/";
    ri.account_id = e.account_id;
    ri.tenant = e.tenant;
    /* The document as their record holds it.  Rewriting its
     * principals through the mapping is §4.4's and belongs with the
     * bucket-policy rewriting in I5, which is where that code will
     * live;  carried verbatim here so nothing is lost. */
    ri.trust_policy = doc->get_data();

    std::unique_ptr<RGWRole> role = driver->get_role(ri);
    constexpr bool exclusive = true;
    int ret = role->store_info(dpp, exclusive, y);
    if (ret == -EEXIST) {
      /* a re-run meeting its own output */
      continue;
    }
    if (ret < 0) {
      fail(e.noobaa_name, std::string("store_info: ") + cpp_strerror(-ret));
      continue;
    }
    ++result.roles_created;
  }

  ldpp_dout(dpp, 4) << "nsfs: imported " << result.accounts_created
    << " accounts (" << result.accounts_skipped << " already present), "
    << result.users_created << " users (" << result.users_skipped
    << " already present), " << result.identity_rows << " identity rows, "
    << result.roles_created << " roles, " << result.grants_written
    << " grants, anonymous "
    << (result.anonymous_imported ? "imported" : "not in this store")
    << ", " << result.failures.size() << " failures" << dendl;
  for (const auto& f : result.failures) {
    ldpp_dout(dpp, 0) << "ERROR: import: " << f << dendl;
  }
  return result.failures.empty() ? 0 : -EIO;
}

}}} // namespace rgw::sal::nsfs
