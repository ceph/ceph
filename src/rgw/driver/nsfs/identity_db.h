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

#include <atomic>
#include <filesystem>

#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

#include "rgw_basic_types.h"
#include "driver/posix/posixDB.h"
#include "common/async/yield_context.h"
#include "name_resolve.h"

namespace rgw { namespace sal { namespace nsfs {

/* The Spectrum Scale data an identity carries, which RGW has nowhere
 * to put.
 *
 * A record per account, and one per user in an account:  NooBaa
 * serves an IAM user from its own copy of this block, made from its
 * account's when the user is created and never updated, so a user's
 * row is that copy.  Whether a user's row is consulted is
 * rgw_nsfs_user_identity_override;  without it every user and role in
 * an account runs as the account (see resolve_credentials() below).
 *
 * `uid`+`gid` and `distinguished_name` are mutually exclusive, which
 * the schema enforces rather than leaving to a caller.  An identity
 * is either locally defined or directory-backed, never a mixture.
 *
 * `groups` distinguishes three states, and the distinction is
 * load-bearing:
 *
 *   nullopt   no list was set.  Today that means no supplementary
 *             groups, unless rgw_nsfs_dynamic_supplemental_groups is
 *             on, when it means "the user's groups from nsswitch", for
 *             either form (I8).
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

  /* Placement.  `new_buckets_path` seeds a new bucket's path and
   * `custom_bucket_path_allowed_list` admits one the request asks
   * for;  both are read on CreateBucket (rgw_sal_nsfs.cc:5275 and
   * :5341).  `fs_backend` is carried and managed and nothing reads it
   * yet. */
  std::string new_buckets_path;
  std::string custom_bucket_path_allowed_list;
  std::string fs_backend;

  /* X1.  NooBaa's per-account `force_md5_etag`, carried and **not
   * enforced** (`ACCOUNT_IMPORT.md` §6.1a).
   *
   * Three states, like `groups` above, and for the same reason:
   * NooBaa's precedence is a cascade -- bucket if defined, else account
   * if defined, else NooBaa's global -- and `false` counts as defined.
   * So an account that turns it off is not an account that said
   * nothing, and collapsing the two would change which value a later
   * decision inherits.
   *
   * Nothing consults it at request time.  `rgw_non_md5_etag` is the
   * single deployment-wide control and NooBaa's global maps to it;  the
   * option exists so a proof of concept would not read slower than
   * NooBaa, and making it per-bucket turns a deployment posture into
   * per-object luck. */
  std::optional<bool> force_md5_etag;

  bool directory_backed() const { return !distinguished_name.empty(); }
  bool local() const { return uid.has_value(); }
};

/* Where a bucket's storage is.
 *
 * Every bucket nsfs creates has one, carrying its path, as every NooBaa
 * bucket record carries `path` (DECISIONS.md 2026-10-13:  nsfs enacts
 * NooBaa's placement model).  A bucket at the base path has a row naming
 * `<base path>/<bucket>` like any other;  there is no empty path meaning
 * "the default".
 *
 * Whether the bucket owns its directory is not here:  it is kept on the
 * directory itself, as user.nsfs.owns_storage, and nowhere else
 * (DECISIONS.md 2026-10-13). */
struct BucketPlacement {
  std::string bucket_name;
  /* Part of the key from the start.  dbstore's own bucket table has
   * `BucketName NOT NULL UNIQUE` with the tenant outside the key, and
   * its comment has said for years that both should be the key once
   * one handle holds several tenants;  an nsfs table need not
   * inherit that. */
  std::string tenant;

  /* absolute, canonical, no `..`.  Never empty. */
  std::string path;

  /* to_string(rgw_owner), the key identity rows use.  An index of the
   * owner bucket_info records on the directory, so that ListBuckets
   * opens only the caller's buckets:  written where the owner is set
   * (creation, the import, chown, set_acl) and nowhere else. */
  std::string owner;
};

/* Adoption's completion record:  NooBaa's store has been consumed and
 * is never read again (DECISIONS.md 2026-10-13).  One row, written by
 * the adoption pass that observes it, beside the placement rows it
 * describes and restored with them. */
struct AdoptionCompletion {
  /* ISO 8601, UTC */
  std::string completed_at;
  /* the store that was consumed */
  std::string config_root;
  uint64_t identities{0};
  uint64_t buckets{0};
  /* what did the conversion */
  std::string gateway_version;
  uint32_t extensions{0};
};

/* A NooBaa record an operator decided will not be carried further, and
 * why (DECISIONS.md 2026-10-13).  It counts as consumed for adoption's
 * completion, and is fixed once adoption is complete. */
struct AbandonedRecord {
  std::string noobaa_id;
  /* "identity" or "bucket" */
  std::string kind;
  std::string name;
  /* "not imported", "not converted" or "partly converted" -- what was
   * left undone, which decides what becomes of a bucket:  one not
   * imported leaves S3 at completion;  one not converted stays, in
   * NooBaa's format;  one partly converted stays, served through the
   * conversion's fallback indefinitely, since nothing finishes it */
  std::string stage;
  std::string reason;
  /* ISO 8601, UTC */
  std::string at;
  /* Not stored:  for an identity, the bucket records it owns at the
   * moment it is abandoned.  Each of them then fails its import, and
   * needs a decision of its own. */
  std::vector<std::string> dependent_buckets;
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
 * decision on the hot path (`archive/NSFS_BUCKET_PLACEMENT_PLAN.md` §9).
 *
 * This is the lexical half:  absolute, no `..`, no empty component,
 * no trailing slash.  Opening it safely is a different question and a
 * later stage -- `openat2` with `RESOLVE_NO_SYMLINKS` -- and so is the
 * per-account allowlist NooBaa's `custom_bucket_path_allowed_list`
 * already expresses.  Neither is attempted here, and a lexically
 * clean path is not an authorized one. */
bool placement_path_is_acceptable(std::string_view path);

/* A custom-bucket-path allowlist, NooBaa's colon-separated string of path
 * prefixes, checked and normalised where it is stored (DECISIONS.md
 * 2026-10-14).  Entries are trimmed of blanks and empty ones dropped, as
 * NooBaa's parse does;  each that remains must be a path
 * placement_path_is_acceptable() accepts.  `out` is the entries joined
 * by ':' again.  -EINVAL names the first bad entry in `bad`. */
int normalize_allowed_list(std::string_view in, std::string& out,
			   std::string& bad);

/* Whether `path` starts with an entry of a normalised allowlist:  a
 * string prefix, as NooBaa matches (DECISIONS.md 2026-10-14). */
bool allowed_list_admits(std::string_view list, std::string_view path);

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
  /* Which RGW owner each NooBaa `_id` was imported as.
   *
   * Provenance, kept apart from the identity table so that table holds
   * only what a request is served as.  A root account maps to its
   * account id, an IAM user to its user, and NooBaa's anonymous account to
   * RGW_USER_ANON_ID.  Re-import recognises an identity by it, because
   * NooBaa's name is not stable across a rename (ACCOUNT_IMPORT.md
   * §3.4). */
  const std::string noobaa_id_table;
  /* The driver's second owned table.  Placement is bucket metadata
   * unique to this backend, so it lives in schema this backend owns
   * rather than changing dbstore's shared bucket table -- which nsfs
   * does not use at all.  Decided 2026-10-08.
   *
   * The class name is narrower than what it now holds. */
  const std::string placement_table;
  /* adoption's completion record:  at most one row */
  const std::string completion_table;
  /* NooBaa records abandoned by an operator */
  const std::string abandoned_table;

  /* what each row's `distinguished_name` resolved to (I7) */
  mutable NsswitchCache names;

  /* the identity-override hint's overrides of the two I9 options:
   * -1 follows the option, 0 off, 1 on */
  std::atomic<int> user_override{-1};
  std::atomic<int> role_override{-1};

  /* The one handle, qualified.
   *
   * `POSIXUserDB` declares a `void *db` that shadows `DB::db`
   * (dbstore.h).  Both end up holding the same pointer today --
   * `SQLiteDB::openDB()` opens into the base member and
   * `POSIXUserDB::Initialize()` copies the return into the shadow --
   * but the base one is what SQLiteDB itself opens and closes, so
   * that is the one to use. */
  sqlite3* handle() const { return reinterpret_cast<sqlite3*>(DB::db); }

  int write_bucket_placement(const DoutPrefixProvider* dpp,
			     const BucketPlacement& bp, bool repoint);

public:
  IdentityDB(std::string db_name, CephContext* cct)
    : POSIXUserDB(db_name, cct),
      /* the basename, which is how DB names every other table.
       * db_name is a full path here, and a table named after the
       * path is orphaned the moment the directory moves. */
      identity_table(std::filesystem::path(db_name).filename().string()
		     + "_nsfs_identity"),
      noobaa_id_table(std::filesystem::path(db_name).filename().string()
		      + "_nsfs_noobaa_id"),
      placement_table(std::filesystem::path(db_name).filename().string()
		      + "_nsfs_bucket_placement"),
      completion_table(std::filesystem::path(db_name).filename().string()
		       + "_nsfs_adoption_complete"),
      abandoned_table(std::filesystem::path(db_name).filename().string()
		      + "_nsfs_noobaa_abandoned"),
      names(cct) {}

  int Initialize(std::string logfile, int loglevel);

  /* Bucket placement.  Write, read, and remove;  enumeration is P3's.
   *
   * Both writers reject a path `placement_path_is_acceptable()` does
   * not accept, with -EINVAL.  `insert` is creation's:  -EEXIST when the name is
   * taken, so of two creations of one name exactly one succeeds.  `put`
   * repoints an existing row instead.  Both return -EADDRINUSE when
   * another bucket's directory is this path, contains it, or lies inside
   * it.  `get` returns -ENOENT when a bucket has no row. */
  int insert_bucket_placement(const DoutPrefixProvider* dpp,
			      const BucketPlacement& bp);
  int put_bucket_placement(const DoutPrefixProvider* dpp,
			   const BucketPlacement& bp);
  int get_bucket_placement(const DoutPrefixProvider* dpp,
			   const std::string& bucket_name,
			   const std::string& tenant,
			   BucketPlacement& out) const;
  int remove_bucket_placement(const DoutPrefixProvider* dpp,
			      const std::string& bucket_name,
			      const std::string& tenant);

  /* The owner changed.  A bucket with no row -- one NooBaa's record
   * still places -- has nothing to update, which is not an error. */
  int set_bucket_placement_owner(const DoutPrefixProvider* dpp,
				 const std::string& bucket_name,
				 const std::string& tenant,
				 const std::string& owner);

  /* Rows in name order, after `marker` and up to `end_marker` when it
   * is not empty, at most `max`.  The first is one owner's in one
   * tenant, which is ListBuckets;  the second is every row, which is
   * the metadata listing. */
  int list_bucket_placements_by_owner(const DoutPrefixProvider* dpp,
				      const std::string& owner,
				      const std::string& tenant,
				      const std::string& marker,
				      const std::string& end_marker,
				      uint64_t max,
				      std::vector<BucketPlacement>& out) const;
  int list_bucket_placements(const DoutPrefixProvider* dpp,
			     const std::string& marker, uint64_t max,
			     std::vector<BucketPlacement>& out) const;

  /* Account ids in order, after `marker`, at most `max`:  the metadata
   * listing's `account` section.  The shared user database stores
   * accounts and has no operation to enumerate them. */
  int list_account_ids(const DoutPrefixProvider* dpp,
		       const std::string& marker, uint64_t max,
		       std::list<std::string>& out);
  /* Topic metadata keys, `<tenant>:<name>`, likewise:  the `topic`
   * section, which ListTopics reads for a tenant that is not an
   * account. */
  int list_topic_keys(const DoutPrefixProvider* dpp,
		      const std::string& marker, uint64_t max,
		      std::list<std::string>& out);

  /* Insert or replace.  `id.key` must be set;  everything else is
   * optional subject to the exclusivity rule. */
  int put_identity(const DoutPrefixProvider* dpp, const Identity& id);

  /* -ENOENT when there is no row, which is not an error:  it means
   * this identity has no Scale data and impersonation does not
   * apply. */
  int get_identity(const DoutPrefixProvider* dpp, const std::string& key,
		   Identity& out) const;

  /* Record that NooBaa's `noobaa_id` was imported as `owner_key`, a
   * to_string(rgw_owner).
   *
   * A plain INSERT:  -EEXIST when the id is already recorded, whatever
   * it maps to.  An id is imported once, and re-recording one is the
   * importer failing to recognise its own output, which should be
   * loud rather than silently repointed. */
  int put_noobaa_id(const DoutPrefixProvider* dpp,
		    const std::string& noobaa_id,
		    const std::string& owner_key);

  /* The completion record.  `put` is a plain INSERT:  -EEXIST once it
   * is written, since completion happens once.  `get` is -ENOENT before
   * it. */
  int put_adoption_completion(const DoutPrefixProvider* dpp,
			      const AdoptionCompletion& c);
  int get_adoption_completion(const DoutPrefixProvider* dpp,
			      AdoptionCompletion& c) const;

  /* Abandoned records.  `put` is a plain INSERT, -EEXIST when the id is
   * abandoned already;  `remove` is -ENOENT when it is not;  `get` is
   * -ENOENT when it is not. */
  int put_abandoned(const DoutPrefixProvider* dpp, const AbandonedRecord& r);
  int remove_abandoned(const DoutPrefixProvider* dpp,
		       const std::string& noobaa_id);
  int get_abandoned(const DoutPrefixProvider* dpp,
		    const std::string& noobaa_id, AbandonedRecord& r) const;
  int list_abandoned(const DoutPrefixProvider* dpp,
		     std::vector<AbandonedRecord>& out) const;

  /* Whether `owner_key` was imported from NooBaa. */
  int owner_was_imported(const DoutPrefixProvider* dpp,
			 const std::string& owner_key, bool& imported) const;

  /* The re-import lookup.  -ENOENT when the id was never imported. */
  int get_owner_by_noobaa_id(const DoutPrefixProvider* dpp,
			     const std::string& noobaa_id,
			     std::string& owner_key) const;

  int remove_identity(const DoutPrefixProvider* dpp, const std::string& key);

  int list_identities(const DoutPrefixProvider* dpp,
		      std::vector<Identity>& out) const;

  /* The rows carrying `uid`, in Identity order.
   *
   * The one read keyed on `Uid` rather than on `Identity`, for
   * completing a credential built from a file (see
   * complete_file_credentials()).  An empty result is not an error:  a
   * uid no row claims is ordinary in a sideloaded tree, which
   * inherit_ids() already treats as real.
   *
   * Several rows is also not an error.  `Uid` is not unique, and on
   * Scale every user of an account runs as the account
   * (ACCOUNT_IMPORT.md section 2.8), so the account's row and each
   * user's carry one uid. */
  int get_identities_by_uid(const DoutPrefixProvider* dpp, uid_t uid,
			    std::vector<Identity>& out) const;

  /* The rows carrying `distinguished_name`, in Identity order;  the
   * same read for an nsswitch-resolved row, whose uid is not stored. */
  int get_identities_by_name(const DoutPrefixProvider* dpp,
			     const std::string& name,
			     std::vector<Identity>& out) const;

  NsswitchCache& nsswitch_cache() const { return names; }

  /* I9:  whether an account user's, or a role's, own row is consulted
   * before its account's:  rgw_nsfs_user_identity_override and
   * rgw_nsfs_role_identity_override, unless the `identity-override`
   * driver hint overrides them. */
  bool user_identity_override() const;
  bool role_identity_override() const;

  /* The hint's overrides;  `nullopt` returns to the option.  Every
   * identity is invalidated, since which row a principal runs as
   * changes. */
  void force_user_identity_override(std::optional<bool> on);
  void force_role_identity_override(std::optional<bool> on);

  /* What the sweep's first pass resolves:  every nsswitch-resolved row's
   * name, and under the switch every list-less local row's uid. */
  NsswitchCache::Table sweep_table(const DoutPrefixProvider* dpp) const;

  /* The sweep, whose first pass resolves every name the table carries:
   * a name nsswitch does not know is logged then rather than found by a
   * refused request. */
  void start_name_sweep();

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

/* A role's identity row (I9), keyed apart from every owner's:  an
 * owner's key is to_string(rgw_owner), which never begins `role:`
 * (is_canonical_owner_key() refuses it), so a role's row cannot be
 * written, read or resolved as a user's.  The role is RGW's
 * (`RGWRoleInfo`);  the row is the identity it was given, consulted for
 * its sessions only under rgw_nsfs_role_identity_override. */
inline constexpr std::string_view ROLE_IDENTITY_PREFIX = "role:";
inline std::string role_identity_key(std::string_view role_id)
{
  return std::string(ROLE_IDENTITY_PREFIX) + std::string(role_id);
}
inline bool is_role_identity_key(std::string_view key)
{
  return key.starts_with(ROLE_IDENTITY_PREFIX);
}

/* Resolve an owner to the credentials to serve it as.
 *
 *   0        resolved;  `out` is authoritative, groups included
 *   -ENOENT  nothing asks for impersonation -- no row, or a row
 *            carrying only placement data.  The caller proceeds
 *            exactly as it does today, which is what keeps this
 *            additive.
 *   -EPERM   a row that asks for impersonation which cannot be
 *            supplied:  an nsswitch-resolved row whose name nsswitch does not
 *            know, or whose lookup failed.  Serving such a request
 *            unimpersonated would run it as the daemon, which holds
 *            more access than the user, so it fails closed.  -EPERM
 *            rather than a more descriptive errno so a caller which
 *            forgets to map it still denies.
 *
 * An nsswitch-resolved row -- `distinguished_name`, which NooBaa passes to
 * getpwnam -- takes its uid and gid from nsswitch, through the
 * database's NsswitchCache;  `y` is where a lookup on a miss suspends.
 * The supplementary groups are the row's list, else none, in both
 * forms;  NooBaa's getgrouplist(3) branch is I8.
 */
int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const rgw_owner& owner, Credentials& out,
			optional_yield y);

/* An authenticated user's credentials:  its own row, else its
 * account's.
 *
 * A user in an account runs as the account unless
 * rgw_nsfs_user_identity_override is on (I9);  only then is a row keyed
 * on the user consulted, and it wins where one exists, as NooBaa serves
 * an IAM user from its own record.  `account` empty is a user outside
 * any account, which resolves under its own key alone, either way.
 *
 * The user's row returning -ENOENT -- absent, or carrying no ids --
 * is what falls through;  -EPERM does not, since that row asked for an
 * identity it cannot be given.
 *
 * `row`, if given, receives the key of the row that resolved, on
 * success or on -EPERM;  it is left alone otherwise. */
int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const rgw_user& user, const rgw_account_id& account,
			Credentials& out, optional_yield y,
			std::string* row = nullptr);

/* A role session's credentials (I9):  the role's own row under
 * rgw_nsfs_role_identity_override, when it has been given one, else its
 * account's.  `row`, if given, receives the key of the row that
 * resolved, as above. */
int resolve_role_credentials(const DoutPrefixProvider* dpp,
			     const IdentityDB& db, const std::string& role_id,
			     const rgw_account_id& account, Credentials& out,
			     optional_yield y, std::string* row = nullptr);

/* Complete a credential built from a file's owner.
 *
 * A credential derived from `stat()` carries a uid and a gid and
 * nothing else, and `ScopedCredentials` installs that as
 * `setgroups(0, NULL)`, so an identity whose access to its tree comes
 * through a group loses it.  Two sites build such a credential -- the
 * multipart part eviction and promote-on-delete -- because they act
 * on one file with no request in hand, and the file is the only thing
 * that says who wrote it.
 *
 * Only `groups` is taken from the row.  The uid and gid on `inout`
 * came from the file and are authoritative for the file;  the row's
 * are the account's, and a user writing in a directory that is not
 * SGID creates files with its own primary gid.
 *
 *   0         `inout.groups` is filled from the row, and `key`, if
 *             given, receives that row's identity -- which is also
 *             what makes an `FSIdentity` active, so a caller may use
 *             a registered personality rather than a thread hop
 *   -ENOENT   no row carries that uid, nor a name the cache last
 *             resolved to it.  `inout` is unchanged and the
 *             caller proceeds on the file owner's own rights, which
 *             is what both sites did before this existed
 *   -ENODATA  several rows carry that uid and they do not agree on
 *             the group list, so the file cannot say which one to
 *             use.  `inout` is unchanged, and this reports it
 *
 * Where several rows carry the uid and agree on the groups there is
 * no ambiguity in fact, so the groups are returned and `key` is left
 * alone:  the rows are distinct identities and only their credentials
 * coincide.  Guessing between rows that disagree is what the -ENODATA
 * return prevents -- it would lend one identity's reach to another
 * identity's file, which is the error docs/ACCOUNT_IMPORT.md section
 * 2.5 calls the sharpest, in a second place.
 */
int complete_file_credentials(const DoutPrefixProvider* dpp,
			      const IdentityDB& db, Credentials& inout,
			      std::string* key = nullptr);

std::string groups_to_text(const std::vector<uint32_t>& groups);
bool groups_from_text(const std::string& text, std::vector<uint32_t>& out);

}}} // namespace rgw::sal::nsfs
