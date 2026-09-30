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

#pragma once

#include "common/errno.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"

#include "rgw_basic_types.h"
#include "rgw_sal_nsfs.h"
#include "identity_db.h"

/* The POSIX identity an owner is served as.
 *
 * The record is managed from outside the gateway because nothing
 * inside it can invent one:  a uid and gid come from a directory or
 * from an operator, and until the importer exists an operator is the
 * only source.  It is deliberately not checked against the account
 * or user table -- the row may be written before or after the
 * account it names, and an ordering constraint there would be a trap
 * rather than a safeguard.
 *
 * Validation lives here.  The schema's CHECK constraints are the
 * last line and answer with an errno;  a caller deserves to be told
 * which field was wrong before the statement runs. */
namespace rgw::sal::nsfs {

/* A key no owner can render to.
 *
 * Resolution finds a row by `to_string(owner)` for the owner on the
 * request, so a key that is not what `to_string` produces is
 * unreachable by construction -- not merely pointing at something
 * absent.  `rgw_user::from_str` followed by `to_str` is not an
 * identity:  `$alice` parses to an empty tenant and renders back as
 * `alice`, and a row keyed `$alice` would sit there forever.  An
 * empty id -- `alice$` -- round-trips but is a user no request can
 * present.
 *
 * This is a property of the string alone, so it costs no lookup and
 * constrains nothing about what exists yet. */
inline bool is_canonical_owner_key(const std::string& key)
{
  if (key.empty()) {
    return false;
  }
  const rgw_owner owner = parse_owner(key);
  if (to_string(owner) != key) {
    return false;
  }
  if (const auto* u = std::get_if<rgw_user>(&owner); u && u->empty()) {
    return false;
  }
  return true;
}

/* Whether the key names something the gateway knows today.
 *
 * Reported, never enforced.  A record may legitimately be written
 * before the account it names -- an operator staging a deployment,
 * an importer resuming after a failure -- and refusing that would
 * make the order of two independent operations load-bearing.  What
 * an operator actually lacks is any sign that a key matched nothing,
 * so the answer travels in the response. */
inline bool owner_resolves(const DoutPrefixProvider* dpp, optional_yield y,
			   rgw::sal::Driver* driver, const std::string& key)
{
  const rgw_owner owner = parse_owner(key);
  if (const auto* a = std::get_if<rgw_account_id>(&owner)) {
    RGWAccountInfo info;
    rgw::sal::Attrs attrs;
    RGWObjVersionTracker objv;
    return driver->load_account_by_id(dpp, y, *a, info, attrs, objv) == 0;
  }
  auto user = driver->get_user(std::get<rgw_user>(owner));
  return user->load_user(dpp, y) == 0;
}

/* A record renders its optional fields by omission.
 *
 * `groups` absent and `groups` empty are different answers -- no one
 * has said, against an explicit "this identity has none" -- and the
 * response has to carry that difference or a read-modify-write
 * cannot preserve it. */
inline void dump_identity(Formatter* f, const Identity& id)
{
  f->dump_string("identity", id.key);
  if (id.uid) {
    f->dump_unsigned("uid", *id.uid);
  }
  if (id.gid) {
    f->dump_unsigned("gid", *id.gid);
  }
  if (id.groups) {
    f->open_array_section("groups");
    for (auto g : *id.groups) {
      f->dump_unsigned("gid", g);
    }
    f->close_section();
  }
  if (!id.distinguished_name.empty()) {
    f->dump_string("distinguished_name", id.distinguished_name);
  }
  if (!id.new_buckets_path.empty()) {
    f->dump_string("new_buckets_path", id.new_buckets_path);
  }
  if (!id.custom_bucket_path_allowed_list.empty()) {
    f->dump_string("custom_bucket_path_allowed_list",
		   id.custom_bucket_path_allowed_list);
  }
  if (!id.fs_backend.empty()) {
    f->dump_string("fs_backend", id.fs_backend);
  }
  if (!id.noobaa_id.empty()) {
    f->dump_string("noobaa_id", id.noobaa_id);
  }
}

} // namespace rgw::sal::nsfs

/* GET /admin/nsfs/identity[?identity=<owner>]
 *
 * With a key, one record;  without one, all of them.  The list is
 * what an importer reconciles against and what a test asserts a
 * delete against, so it is not a convenience. */
class RGWOp_NSFS_Identity_Get : public RGWRESTOp {
public:
  RGWOp_NSFS_Identity_Get() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }
    auto* db = nsfs_driver->get_identity_db();

    std::string key;
    bool have_key = false;
    RESTArgs::get_string(s, "identity", "", &key, &have_key);

    Formatter* f = flusher.get_formatter();

    if (have_key && !key.empty()) {
      rgw::sal::nsfs::Identity id;
      op_ret = db->get_identity(s, key, id);
      if (op_ret < 0) {
	return;
      }
      flusher.start(0);
      f->open_object_section("dummy");   /* outermost is not rendered */
      rgw::sal::nsfs::dump_identity(f, id);
      /* not on the list below, which would be one lookup per row */
      f->dump_bool("resolves",
		   rgw::sal::nsfs::owner_resolves(s, y, driver, key));
      f->close_section();
      flusher.flush();
      return;
    }

    std::vector<rgw::sal::nsfs::Identity> all;
    op_ret = db->list_identities(s, all);
    if (op_ret < 0) {
      return;
    }
    flusher.start(0);
    f->open_object_section("dummy");   /* outermost is not rendered */
    f->open_array_section("identities");
    for (const auto& id : all) {
      f->open_object_section("identity");
      rgw::sal::nsfs::dump_identity(f, id);
      f->close_section();
    }
    f->close_section();
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_identity_get"; }
};

/* PUT /admin/nsfs/identity?identity=<owner>&...
 *
 * A full replace, matching the storage layer underneath it.  An
 * absent parameter means the field is unset, not that it keeps its
 * previous value, so every field can be cleared without a sentinel
 * and `groups` can travel in all three of its states:  absent is
 * unset, `groups=` is the explicit empty list, `groups=10,20` is the
 * list.  The cost is that a caller changing one field resends the
 * rest;  a merge verb would spend the distinction to save that, and
 * the distinction is the part that is hard to get back. */
class RGWOp_NSFS_Identity_Put : public RGWRESTOp {
public:
  RGWOp_NSFS_Identity_Put() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }

    rgw::sal::nsfs::Identity id;
    bool have_key = false;
    RESTArgs::get_string(s, "identity", "", &id.key, &have_key);
    if (!have_key || !rgw::sal::nsfs::is_canonical_owner_key(id.key)) {
      op_ret = -EINVAL;
      return;
    }

    bool have_uid = false, have_gid = false;
    uint32_t uid = 0, gid = 0;
    if (RESTArgs::get_uint32(s, "uid", 0, &uid, &have_uid) < 0) {
      op_ret = -EINVAL;
      return;
    }
    if (RESTArgs::get_uint32(s, "gid", 0, &gid, &have_gid) < 0) {
      op_ret = -EINVAL;
      return;
    }
    if (have_uid != have_gid) {
      /* half an identity cannot be impersonated with */
      op_ret = -EINVAL;
      return;
    }
    /* (uid_t)-1 is what chown() reads as "leave alone" and what
     * setresuid() refuses;  it is never an identity to serve as */
    if (have_uid && ((uid == static_cast<uint32_t>(-1)) ||
		     (gid == static_cast<uint32_t>(-1)))) {
      op_ret = -EINVAL;
      return;
    }
    if (have_uid) {
      id.uid = uid;
      id.gid = gid;
    }

    bool have_groups = false;
    std::string groups;
    RESTArgs::get_string(s, "groups", "", &groups, &have_groups);
    if (have_groups) {
      std::vector<uint32_t> parsed;
      if (! rgw::sal::nsfs::groups_from_text(groups, parsed)) {
	op_ret = -EINVAL;
	return;
      }
      id.groups = std::move(parsed);
    }

    RESTArgs::get_string(s, "distinguished_name", "",
			 &id.distinguished_name);
    RESTArgs::get_string(s, "new_buckets_path", "", &id.new_buckets_path);
    RESTArgs::get_string(s, "custom_bucket_path_allowed_list", "",
			 &id.custom_bucket_path_allowed_list);
    RESTArgs::get_string(s, "fs_backend", "", &id.fs_backend);
    RESTArgs::get_string(s, "noobaa_id", "", &id.noobaa_id);

    /* The two arms name the same thing twice and would disagree the
     * moment the directory changed. */
    if (id.local() && id.directory_backed()) {
      op_ret = -EINVAL;
      return;
    }

    op_ret = nsfs_driver->get_identity_db()->put_identity(s, id);
    if (op_ret < 0) {
      return;
    }

    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_object_section("dummy");   /* outermost is not rendered */
    rgw::sal::nsfs::dump_identity(f, id);
    /* false is not an error:  the row is written and will be found
     * as soon as the account it names exists.  It is here so a typo
     * is visible at the moment it is made. */
    f->dump_bool("resolves",
		 rgw::sal::nsfs::owner_resolves(s, y, driver, id.key));
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_identity_put"; }
};

/* DELETE /admin/nsfs/identity?identity=<owner>
 *
 * Removing what is not there succeeds:  the caller asked for the row
 * to be gone and it is, and an importer re-run should not have to
 * distinguish the cases. */
class RGWOp_NSFS_Identity_Delete : public RGWRESTOp {
public:
  RGWOp_NSFS_Identity_Delete() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }

    std::string key;
    RESTArgs::get_string(s, "identity", "", &key);
    if (! rgw::sal::nsfs::is_canonical_owner_key(key)) {
      op_ret = -EINVAL;
      return;
    }

    op_ret = nsfs_driver->get_identity_db()->remove_identity(s, key);
  }

  const char* name() const override { return "nsfs_identity_delete"; }
};

/* GET /admin/nsfs/credentials[?identity=<owner>]
 *
 * What a request would be served as.  Without a parameter it
 * resolves the *caller*, which is the only way to exercise the key
 * an authenticated request actually produces -- a test that builds
 * an applier by hand assumes the answer it is trying to check.  With
 * `identity=` it resolves that key instead, which is what an
 * operator needs when the row in question is not their own.
 *
 * Reporting, not enforcing:  "no impersonation" and "asked for and
 * unavailable" are answers, not failures, and come back with a 200
 * and an `outcome`.  Failing the request would make the diagnostic
 * useless in exactly the situation it exists for.
 *
 * The response names the key it used.  A row that is never found is
 * almost always a key that is not the one the request produces, and
 * nothing else shows that. */
class RGWOp_NSFS_Credentials_Get : public RGWRESTOp {
public:
  RGWOp_NSFS_Credentials_Get() = default;

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("nsfs", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    auto* nsfs_driver = dynamic_cast<rgw::sal::NSFSDriver*>(driver);
    if (! nsfs_driver) {
      op_ret = -ENOTSUP;
      return;
    }

    std::string key;
    bool have_key = false;
    RESTArgs::get_string(s, "identity", "", &key, &have_key);

    rgw::sal::nsfs::Credentials cred;
    int ret;

    if (have_key && !key.empty()) {
      if (! rgw::sal::nsfs::is_canonical_owner_key(key)) {
	/* a key no owner renders to would report "none" forever;
	 * say it is malformed instead */
	op_ret = -EINVAL;
	return;
      }
      ret = rgw::sal::nsfs::resolve_credentials(
	  s, *nsfs_driver->get_identity_db(), parse_owner(key), cred);
    } else {
      /* the caller.  This is the path get_credentials() serves, and
       * the key comes from s->user rather than s->owner. */
      if (! s->user) {
	op_ret = -EINVAL;
	return;
      }
      key = s->user->get_id().to_str();
      ret = nsfs_driver->get_credentials(s, s, cred);
    }

    const char* outcome;
    switch (ret) {
    case 0:		outcome = "resolved"; break;
    case -ENOENT:	outcome = "none"; break;
    case -EPERM:	outcome = "refused"; break;
    default:
      op_ret = ret;
      return;
    }
    op_ret = 0;

    Formatter* f = flusher.get_formatter();
    flusher.start(0);
    f->open_object_section("dummy");   /* outermost is not rendered */
    f->dump_string("identity", key);
    f->dump_string("outcome", outcome);
    if (ret == 0) {
      f->dump_unsigned("uid", cred.uid);
      f->dump_unsigned("gid", cred.gid);
      /* always emitted when resolved, empty included:  an empty
       * vector is the instruction to clear, not an absence */
      f->open_array_section("groups");
      for (auto g : cred.groups) {
	f->dump_unsigned("gid", g);
      }
      f->close_section();
    }
    f->close_section();
    flusher.flush();
  }

  const char* name() const override { return "nsfs_credentials_get"; }
};

class RGWHandler_NSFS_Credentials : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override { return new RGWOp_NSFS_Credentials_Get; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_NSFS_Credentials() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_NSFS_Credentials : public RGWRESTMgr {
public:
  RGWRESTMgr_NSFS_Credentials() = default;
  ~RGWRESTMgr_NSFS_Credentials() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state* const s,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string& frontend_prefix) override {
    return new RGWHandler_NSFS_Credentials(auth_registry);
  }
};

class RGWHandler_NSFS_Identity : public RGWHandler_Auth_S3 {
protected:
  RGWOp* op_get() override { return new RGWOp_NSFS_Identity_Get; }
  RGWOp* op_put() override { return new RGWOp_NSFS_Identity_Put; }
  RGWOp* op_delete() override { return new RGWOp_NSFS_Identity_Delete; }
public:
  using RGWHandler_Auth_S3::RGWHandler_Auth_S3;
  ~RGWHandler_NSFS_Identity() override = default;

  int read_permissions(RGWOp*, optional_yield) override {
    return 0;
  }
};

class RGWRESTMgr_NSFS_Identity : public RGWRESTMgr {
public:
  RGWRESTMgr_NSFS_Identity() = default;
  ~RGWRESTMgr_NSFS_Identity() override = default;

  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
			       req_state* const s,
			       const rgw::auth::StrategyRegistry& auth_registry,
			       const std::string& frontend_prefix) override {
    return new RGWHandler_NSFS_Identity(auth_registry);
  }
};
