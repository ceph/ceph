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

  /* X1.  NooBaa's per-account `force_md5_etag`, carried and **not
   * enforced** (`ACCOUNT_IMPORT.md` §6.1a).
   *
   * Three states, like `groups` above, and for the same reason:
   * their precedence is a cascade -- bucket if defined, else account
   * if defined, else their global -- and `false` counts as defined.
   * So an account that turns it off is not an account that said
   * nothing, and collapsing the two would change which value a later
   * decision inherits.
   *
   * Nothing consults it at request time.  `rgw_non_md5_etag` is the
   * single deployment-wide control and their global maps to it;  the
   * option exists so a proof of concept would not read slower than
   * NooBaa, and making it per-bucket turns a deployment posture into
   * per-object luck. */
  std::optional<bool> force_md5_etag;

  bool directory_backed() const { return !distinguished_name.empty(); }
  bool local() const { return uid.has_value(); }
};

/* Where a bucket's storage is, when it is not under the data root.
 *
 * P1 of `NSFS_BUCKET_PLACEMENT_PLAN.md`.  **The column and the
 * accessor only:  nothing reads a path yet.**  Resolution is P2 and
 * enumeration is P3.
 *
 * `path` empty means the default location, `<data root>/<bucket>`,
 * which is where every bucket is today.  So a bucket at the default
 * location needs no row at all -- the same rule §7's third arm states
 * -- and an existing deployment behaves exactly as it does now.
 *
 * `owns_storage` is their `should_create_underlying_storage`, and is a
 * plain bool rather than a tri-state:  their schema makes it required,
 * and a bucket this driver creates owns the directory it created.
 * Compare `Identity::force_md5_etag`, which needs three states because
 * their *precedence* is a cascade and `false` counts as defined.  A
 * third state nothing needs is weight, not safety. */
struct BucketPlacement {
  std::string bucket_name;
  /* Part of the key from the start.  dbstore's own bucket table has
   * `BucketName NOT NULL UNIQUE` with the tenant outside the key, and
   * its comment has said for years that both should be the key once
   * one handle holds several tenants;  a table of ours need not
   * inherit that. */
  std::string tenant;

  /* absolute, canonical, no `..`.  Empty is the default location. */
  std::string path;
  bool owns_storage{true};
};

/* Whether a path may be stored.
 *
 * A stored path is input:  it arrives over Admin Ops or is translated
 * out of a NooBaa bucket record at adopt time -- operator-initiated,
 * so semi-trusted, but adopt reads a store this driver does not own
 * and did not write.
 *
 * **Checked where it is stored, never on the request path.**  By then
 * it is a row we wrote, and re-checking each time would put a policy
 * decision on the hot path (`NSFS_BUCKET_PLACEMENT_PLAN.md` §9).
 *
 * This is the lexical half:  absolute, no `..`, no empty component,
 * no trailing slash.  Opening it safely is a different question and a
 * later stage -- `openat2` with `RESOLVE_NO_SYMLINKS` -- and so is the
 * per-account allowlist their `custom_bucket_path_allowed_list`
 * already expresses.  Neither is attempted here, and a lexically
 * clean path is not an authorized one. */
bool placement_path_is_acceptable(std::string_view path);

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
  /* The driver's second owned table.  Placement is bucket metadata
   * unique to this backend, so it lives in schema this backend owns
   * rather than changing dbstore's shared bucket table -- which nsfs
   * does not use at all.  Decided 2026-10-08.
   *
   * The class name is narrower than what it now holds. */
  const std::string placement_table;

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
		     + "_nsfs_identity"),
      placement_table(std::filesystem::path(db_name).filename().string()
		      + "_nsfs_bucket_placement") {}

  int Initialize(std::string logfile, int loglevel);

  /* Bucket placement.  Write, read, and remove;  **enumeration is
   * P3's**, which is where the union with the data root's readdir is
   * defined and where a de-duplication rule is needed.  Adding it
   * here would be a reader for nobody.
   *
   * `put` refuses a path `placement_path_is_acceptable()` rejects,
   * with -EINVAL;  `get` returns -ENOENT when a bucket has no row,
   * which is the ordinary case for a bucket at the default
   * location. */
  int put_bucket_placement(const DoutPrefixProvider* dpp,
			   const BucketPlacement& bp);
  int get_bucket_placement(const DoutPrefixProvider* dpp,
			   const std::string& bucket_name,
			   const std::string& tenant,
			   BucketPlacement& out) const;
  int remove_bucket_placement(const DoutPrefixProvider* dpp,
			      const std::string& bucket_name,
			      const std::string& tenant);

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
