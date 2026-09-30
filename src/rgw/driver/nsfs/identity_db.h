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

#include <filesystem>

#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

#include "rgw_basic_types.h"
#include "driver/posix/posixDB.h"

namespace rgw { namespace sal { namespace nsfs {

/* The Spectrum Scale data an identity carries, which RGW has nowhere
 * to put.
 *
 * One record per *identity* rather than per account:  a NooBaa IAM
 * user holds its own copy of this block and the request path reads
 * the authenticated identity's, not its account's.  See
 * docs/ACCOUNT_IMPORT.md section 2.8.
 *
 * `uid`+`gid` and `distinguished_name` are mutually exclusive, which
 * the schema enforces rather than leaving to a caller.  An identity
 * is either locally defined or directory-backed, never a mixture.
 *
 * `groups` distinguishes three states, and the distinction is
 * load-bearing:
 *
 *   nullopt   no list was set.  Today that means no supplementary
 *             groups;  once the directory arm is implemented it will
 *             mean "resolve from the directory".
 *   empty     a list was set and it is empty -- this identity has no
 *             supplementary groups, deliberately.
 *   non-empty the vector to install.
 *
 * Collapsing the first two would make an explicit "no groups" and an
 * unanswered question indistinguishable. */
struct Identity {
  /* to_string(rgw_owner) -- the flattened owner, which covers an
   * account id and a user alike without a discriminator column, and
   * is what dbstore already stores in Bucket.OwnerID */
  std::string key;

  std::optional<uint32_t> uid;
  std::optional<uint32_t> gid;
  std::optional<std::vector<uint32_t>> groups;

  std::string distinguished_name;

  /* carried and managed, not yet read by any request path */
  std::string new_buckets_path;
  std::string custom_bucket_path_allowed_list;
  std::string fs_backend;
  std::string noobaa_id;

  bool directory_backed() const { return !distinguished_name.empty(); }
  bool local() const { return uid.has_value(); }
};

/* The driver's database, plus the identity table.
 *
 * A subclass rather than a second database:  one file, one
 * connection, and a foreign key is possible because the connection
 * already runs with `PRAGMA foreign_keys=ON` (SQLiteDB::openDB).
 * posixDB is untouched.
 *
 * `Initialize()` is overridden so that the table exists wherever the
 * connection is opened, rather than depending on a caller to
 * remember.  A process which never opens the database -- most of the
 * unit suite -- never creates it either. */
class IdentityDB : public rgw::store::POSIXUserDB {
  const std::string identity_table;

  /* The one handle, qualified.
   *
   * `POSIXUserDB` declares a `void *db` that shadows `DB::db`
   * (dbstore.h).  Both end up holding the same pointer today --
   * `SQLiteDB::openDB()` opens into the base member and
   * `POSIXUserDB::Initialize()` copies the return into the shadow --
   * but the base one is what SQLiteDB itself opens and closes, so
   * that is the one to use. */
  sqlite3* handle() const { return reinterpret_cast<sqlite3*>(DB::db); }

public:
  IdentityDB(std::string db_name, CephContext* cct)
    : POSIXUserDB(db_name, cct),
      /* the basename, which is how DB names every other table.
       * db_name is a full path here, and a table named after the
       * path is orphaned the moment the directory moves. */
      identity_table(std::filesystem::path(db_name).filename().string()
		     + "_nsfs_identity") {}

  int Initialize(std::string logfile, int loglevel);

  /* Insert or replace.  `id.key` must be set;  everything else is
   * optional subject to the exclusivity rule. */
  int put_identity(const DoutPrefixProvider* dpp, const Identity& id);

  /* -ENOENT when there is no row, which is not an error:  it means
   * this identity has no Scale data and impersonation does not
   * apply. */
  int get_identity(const DoutPrefixProvider* dpp, const std::string& key,
		   Identity& out) const;

  /* The re-import lookup.  Indexed, because an importer resolves
   * every record this way and a scan would be quadratic. */
  int get_identity_by_noobaa_id(const DoutPrefixProvider* dpp,
				const std::string& noobaa_id,
				Identity& out) const;

  int remove_identity(const DoutPrefixProvider* dpp, const std::string& key);

  int list_identities(const DoutPrefixProvider* dpp,
		      std::vector<Identity>& out) const;

  const std::string& get_identity_table() const { return identity_table; }
};

/* The group vector's text form, exposed for testing.
 *
 * Decimal, comma separated, no spaces -- the shape the schema's CHECK
 * constraint permits.  Parsing failure is reported rather than
 * silently yielding an empty vector, because an empty vector is a
 * meaningful value here. */
/* The credentials a request is served under.
 *
 * `groups` is a plain vector and not an optional, deliberately.  An
 * empty vector means no supplementary groups and the caller must
 * install it;  there is no value here a caller can read as "leave
 * whatever the process holds".  That reading is the error
 * docs/ACCOUNT_IMPORT.md section 2.5 calls the sharpest of the
 * three, because it grants access rather than denying it, and the
 * type is the only place it can be made unsayable. */
struct Credentials {
  uid_t uid{0};
  gid_t gid{0};
  std::vector<gid_t> groups;
};

/* Resolve an owner to the credentials to serve it as.
 *
 *   0        resolved;  `out` is authoritative, groups included
 *   -ENOENT  nothing asks for impersonation -- no row, or a row
 *            carrying only placement data.  The caller proceeds
 *            exactly as it does today, which is what keeps this
 *            additive.
 *   -EPERM   a row that asks for impersonation which cannot be
 *            supplied.  Today that is the directory arm, whose uid
 *            and gid live in the directory rather than in the
 *            record.  Serving such a request unimpersonated would
 *            run it as the daemon, which holds more access than the
 *            user, so it fails closed.  -EPERM rather than a more
 *            descriptive errno so a caller which forgets to map it
 *            still denies.
 *
 * Only stored columns are read.  Their branch 2 -- resolving the
 * vector through getgrouplist(3) at request time -- is deliberately
 * not implemented:  the group data belongs in this database, put
 * there by the import and refreshed out of band, rather than looked
 * up on the gateway host on the request path.
 */
int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const rgw_owner& owner, Credentials& out);

/* The key an authenticated request resolves under.
 *
 * `s->user` is the *authenticated* user:  every applier's
 * `load_acct_info()` puts it there, and for a member of an account
 * that is the member, not the account.  `s->owner` is the other
 * answer -- `get_aclowner()` substitutes the account -- and keying
 * on it would serve every member of an account as one uid and gid.
 *
 * For an account member the id is a randomly generated UUID
 * (`rgw_rest_iam_user.cc:192-194`), so it is unique across accounts;
 * the display name is not the key.
 *
 * Under a system request carrying `rgwx-uid`, `SysReqApplier`
 * replaces `s->user` with the impersonated owner, so the request is
 * served as that owner.  That is the wanted behaviour:  the system
 * user's own reach is the daemon's, and confining it to the owner it
 * is acting for is the narrower of the two.
 */
int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const req_state* s, Credentials& out);

std::string groups_to_text(const std::vector<uint32_t>& groups);
bool groups_from_text(const std::string& text, std::vector<uint32_t>& out);

}}} // namespace rgw::sal::nsfs
