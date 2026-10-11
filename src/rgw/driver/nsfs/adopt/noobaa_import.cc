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
#include "rgw_formats.h"
#include "rgw_role.h"
#include "driver/rados/rgw_user.h"

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

/* NooBaa's POSIX arm into the nsfs identity row.
 *
 * `uid`/`gid` or a distinguished name, never both -- NooBaa's schema is
 * a `oneOf` and the nsfs table's is a CHECK, so a record that violated
 * it could not be stored here either.  `user` takes the
 * distinguished-name arm for the anonymous account (§6.8). */
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

/* The grant NooBaa's `allow_bypass_governance` means.
 *
 * NooBaa's flag is a boolean on the account's `nsfs_account_config`;
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

/* `rgw::account::create()` writes the account it made to a flusher,
 * as the admin command it serves does.  Nothing reads it here. */
class DiscardFlusher : public RGWFormatterFlusher {
  JSONFormatter f;
protected:
  void do_flush() override {
    f.reset();
  }
public:
  DiscardFlusher() : RGWFormatterFlusher(&f) {}
};

/* NooBaa's ISO 8601 timestamp, or nothing:  an unreadable one leaves
 * RGW to stamp the time of import, as it would anyway. */
std::optional<ceph::real_time> noobaa_time(const std::string& text)
{
  ceph::real_time t;
  if (text.empty() || (parse_time(text.c_str(), &t) < 0)) {
    return std::nullopt;
  }
  return t;
}

/* One access key, through the path `radosgw-admin key create` runs.
 *
 * That path refuses a key id another RGW user holds.  It creates every
 * key active, so a key NooBaa holds deactivated is made inactive by a
 * second operation, a modify, which is how RGW deactivates one. */
int add_access_key(const DoutPrefixProvider* dpp, optional_yield y,
		   NSFSDriver* driver, const rgw_user& uid,
		   const NooBaaAccessKey& ak, std::string& err)
{
  RGWUserAdminOpState op(driver);
  op.set_user_id(uid);
  op.set_key_type(KEY_TYPE_S3);
  op.set_access_key(ak.access_key);
  op.set_secret_key(ak.secret_key);
  op.create_date = noobaa_time(ak.creation_date);

  RGWUser user;
  int ret = user.init(dpp, driver, op, y);
  if (ret < 0) {
    return ret;
  }
  ret = user.keys.add(dpp, op, y, &err);
  if ((ret < 0) || !ak.deactivated) {
    return ret;
  }

  RGWUserAdminOpState mod(driver);
  mod.set_user_id(uid);
  mod.set_key_type(KEY_TYPE_S3);
  mod.set_access_key(ak.access_key);
  mod.set_access_key_active(false);

  RGWUser again;
  ret = again.init(dpp, driver, mod, y);
  if (ret < 0) {
    return ret;
  }
  return again.keys.add(dpp, mod, y, &err);
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
    /* Already imported is not a collision.  Recognised by NooBaa's id,
     * which is the whole reason the column exists -- a name-keyed
     * check would report every renamed account as a conflict with
     * itself. */
    std::string existing;
    if (db->get_owner_by_noobaa_id(dpp, e.noobaa_id, existing) == 0) {
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
    /* An access key is unique across RGW.  The user-add path refuses
     * one already held, but only when it reaches that key -- after the
     * account and the user exist -- so it is checked here, before
     * anything is written. */
    for (const auto& k : e.access_key_ids) {
      std::unique_ptr<User> holder;
      if (driver->get_user_by_access_key(dpp, k, y, &holder) == 0) {
	collisions.push_back(
	    "the access key " + k + " of _id " + e.noobaa_id + " is already "
	    "held by RGW user " + holder->get_id().to_str());
      }
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

    /* Already imported?  NooBaa's id is the key.  Skipped rather than
     * rewritten:  NooBaa's store is the source, but an identity RGW
     * already holds may have been changed on the RGW side since, and
     * reconciling divergence is explicitly not attempted (§6.2). */
    std::string existing;
    if (db->get_owner_by_noobaa_id(dpp, e.noobaa_id, existing) == 0) {
      owner_by_noobaa_id[e.noobaa_id] = existing;
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
      fill_posix(*src, row);
      int ret = db->put_identity(dpp, row);
      if (ret < 0) {
	fail("anonymous", cpp_strerror(-ret));
	continue;
      }
      ret = db->put_noobaa_id(dpp, e.noobaa_id, row.key);
      if (ret < 0) {
	fail("anonymous", std::string("put_noobaa_id: ") + cpp_strerror(-ret));
	continue;
      }
      owner_by_noobaa_id[e.noobaa_id] = row.key;
      result.anonymous_imported = true;
      ++result.identity_rows;
      continue;
    }

    /* A root account:  the account, then the user that holds its
     * keys.  An IAM user:  only the user, in its owner's account.
     *
     * Both through RGW's admin path -- what `radosgw-admin account
     * create`, `user create` and `key create` run -- so an imported
     * identity is created the way RGW creates one:  its names
     * validated, the default quotas applied, its keys checked against
     * every key RGW holds, and the account's root marked as the root.
     * A user that is not the root has no implicit grant, which is
     * RGW's account model and not the import's choice. */
    if (!e.iam_user) {
      rgw::account::AdminOpState aop;
      aop.account_id = e.account_id;
      aop.tenant = e.tenant;
      aop.account_name = e.name;
      aop.email = e.email;
      aop.max_users = e.max_users;
      aop.max_access_keys = e.max_access_keys;

      std::string err;
      DiscardFlusher flusher;
      int ret = rgw::account::create(dpp, driver, aop, err, flusher, y);
      if (ret < 0) {
	fail(e.name, "account create: " + cpp_strerror(-ret) +
	     (err.empty() ? "" : " (" + err + ")"));
	continue;
      }
      ++result.accounts_created;
      if (e.max_users || e.max_access_keys) {
	++result.limits_raised;
      }
    }

    /* The decrypted keys.  I2 opened them;  an empty secret here
     * means I2 was not run or refused, and a user written with no
     * usable credential is worse than one not written. */
    bool usable = true;
    for (const auto& ak : src->access_keys) {
      if (ak.secret_key.empty()) {
	fail(e.name, "access key " + ak.access_key + " has no decrypted "
	     "secret;  run the master-key step first");
	usable = false;
      }
    }
    if (!usable) {
      continue;
    }

    /* A random UUID, as `CreateUser` mints.  NooBaa's `_id` is not usable
     * here -- it is not an RGW id -- and the mapping back to it lives
     * in the NooBaa id table. */
    rgw_user uid;
    {
      uuid_d uuid;
      uuid.generate_random();
      uid.id = uuid.to_string();
      uid.tenant = e.tenant;
    }

    RGWUserAdminOpState op(driver);
    op.set_user_id(uid);
    /* What principal matching consults for an account user, so it
     * carries the NooBaa name and a name-form principal still resolves
     * (§6.6). */
    op.set_display_name(e.display_name);
    op.account_id = e.account_id;
    op.set_account_root(!e.iam_user);
    op.path = e.iam_user && !src->iam_path.empty() ? src->iam_path : "/";
    op.create_date = noobaa_time(src->creation_date);
    /* NooBaa's `allow_bucket_creation`, as RGW expresses it:  a negative
     * bucket limit refuses CreateBucket to this user, whoever owns the
     * bucket (`rgw_op.cc`).  NooBaa refuses an identity whose record
     * lacks the flag, and so does this. */
    if (!src->allow_bucket_creation) {
      op.set_max_buckets(-1);
    }

    std::string err;
    RGWUser ruser;
    int ret = ruser.init(dpp, driver, op, y);
    if (ret >= 0) {
      ret = ruser.add(dpp, op, y, &err);
    }
    if (ret < 0) {
      fail(e.name, "user create: " + cpp_strerror(-ret) +
	   (err.empty() ? "" : " (" + err + ")"));
      continue;
    }
    ++result.users_created;

    for (const auto& ak : src->access_keys) {
      ret = add_access_key(dpp, y, driver, uid, ak, err);
      if (ret < 0) {
	fail(e.name, "access key " + ak.access_key + ": " +
	     cpp_strerror(-ret) + (err.empty() ? "" : " (" + err + ")"));
	break;
      }
    }
    if (ret < 0) {
      continue;
    }

    /* NooBaa's `allow_bypass_governance` as an IAM grant on an IAM user.
     * RGW expresses the permission as an action, so the import writes
     * a policy rather than a column (§2.2), the way PutUserPolicy
     * stores one.  A root needs none:  it is allowed every action on
     * what its account owns, and the plan reports each root NooBaa
     * did not allow this. */
    if (e.iam_user && src->posix.allow_bypass_governance) {
      std::unique_ptr<User> user = driver->get_user(uid);
      ret = user->load_user(dpp, y);
      if (ret < 0) {
	fail(e.name, std::string("load_user: ") + cpp_strerror(-ret));
	continue;
      }
      std::map<std::string, std::string> policies;
      policies["noobaa-allow-bypass-governance"] =
	bypass_governance_policy();
      bufferlist bl;
      ceph::encode(policies, bl);
      user->get_attrs()[RGW_ATTR_USER_POLICY] = std::move(bl);
      ret = user->store_user(dpp, y, false);
      if (ret < 0) {
	fail(e.name, std::string("store_user: ") + cpp_strerror(-ret));
	continue;
      }
      ++result.grants_written;
    }

    /* The nsfs row, for a root account only:  the POSIX arm and the
     * placement fields, keyed on the account.  Every member of the
     * account runs as it and has no row of its own (§2.8);  a member
     * whose NooBaa copy differs was reported by the plan. */
    std::string owner_key;
    if (e.iam_user) {
      owner_key = to_string(rgw_owner{uid});
    } else {
      Identity row;
      row.key = e.account_id;
      fill_posix(*src, row);
      ret = db->put_identity(dpp, row);
      if (ret < 0) {
	fail(e.name, std::string("put_identity: ") + cpp_strerror(-ret));
	continue;
      }
      owner_key = row.key;
      ++result.identity_rows;
    }

    /* Last, so an identity is recognised on a re-run only once
     * everything above it is written. */
    ret = db->put_noobaa_id(dpp, e.noobaa_id, owner_key);
    if (ret < 0) {
      fail(e.name, std::string("put_noobaa_id: ") + cpp_strerror(-ret));
      continue;
    }
    owner_by_noobaa_id[e.noobaa_id] = owner_key;
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

    /* NooBaa's `role_config` carries a role name and an assume-role
     * policy document.  The document's principals name identities by
     * the NooBaa email, which is the name -- so the mapping is what makes
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
    /* The document as NooBaa's record holds it.  Rewriting its
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
