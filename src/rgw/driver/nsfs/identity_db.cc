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
#include <charconv>
#include <climits>
#include <sqlite3.h>

#include "common/errno.h"
#include "include/scope_guard.h"

#include "rgw_common.h"
#include "rgw_sal.h"
#include "identity_db.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal { namespace nsfs {

std::string groups_to_text(const std::vector<uint32_t>& groups)
{
  std::string s;
  for (auto g : groups) {
    if (!s.empty()) {
      s += ',';
    }
    s += std::to_string(g);
  }
  return s;
}

bool groups_from_text(const std::string& text, std::vector<uint32_t>& out)
{
  out.clear();
  if (text.empty()) {
    return true;			/* an explicit empty list */
  }

  size_t pos = 0;
  while (pos <= text.size()) {
    size_t comma = text.find(',', pos);
    if (comma == std::string::npos) {
      comma = text.size();
    }
    if (comma == pos) {
      return false;			/* empty element:  "1,,2" or ",1" */
    }
    uint32_t v = 0;
    auto [end, ec] = std::from_chars(text.data() + pos, text.data() + comma, v);
    if ((ec != std::errc{}) || (end != text.data() + comma)) {
      return false;
    }
    out.push_back(v);
    pos = comma + 1;
  }
  return true;
}

namespace {

/* Bind a std::string that may be absent.  An empty string is stored
 * as SQL NULL for the optional text columns, because "not set" and
 * "set to nothing" are the same thing for a path or a name -- unlike
 * the group vector, where they differ (see Identity::groups). */
int bind_text_or_null(sqlite3_stmt* st, int pos, const std::string& v)
{
  return v.empty() ? sqlite3_bind_null(st, pos)
		   : sqlite3_bind_text(st, pos, v.c_str(), -1, SQLITE_TRANSIENT);
}

std::string column_text(sqlite3_stmt* st, int col)
{
  const unsigned char* p = sqlite3_column_text(st, col);
  return p ? std::string(reinterpret_cast<const char*>(p)) : std::string();
}

std::optional<uint32_t> column_u32(sqlite3_stmt* st, int col)
{
  if (sqlite3_column_type(st, col) == SQLITE_NULL) {
    return std::nullopt;
  }
  return static_cast<uint32_t>(sqlite3_column_int64(st, col));
}

/* Column order is fixed by this constant and every statement below
 * indexes against it, so a column added in the middle breaks loudly
 * rather than silently shifting a read. */
constexpr const char* IDENTITY_COLUMNS =
  "Identity, Uid, Gid, SupplementalGroups, DistinguishedName, "
  "NewBucketsPath, CustomBucketPathAllowedList, FsBackend, NoobaaId, "
  "ForceMd5Etag";

int row_to_identity(const DoutPrefixProvider* dpp, sqlite3_stmt* st,
		    Identity& out)
{
  out = Identity{};
  out.key = column_text(st, 0);
  out.uid = column_u32(st, 1);
  out.gid = column_u32(st, 2);

  if (sqlite3_column_type(st, 3) != SQLITE_NULL) {
    std::vector<uint32_t> g;
    if (!groups_from_text(column_text(st, 3), g)) {
      ldpp_dout(dpp, 0) << "ERROR: identity " << out.key
	<< " has an unparsable supplemental group list" << dendl;
      return -EBADMSG;
    }
    out.groups = std::move(g);
  }

  out.distinguished_name = column_text(st, 4);
  out.new_buckets_path = column_text(st, 5);
  out.custom_bucket_path_allowed_list = column_text(st, 6);
  out.fs_backend = column_text(st, 7);
  out.noobaa_id = column_text(st, 8);
  /* NULL is "said nothing", which is not the same as 0 */
  if (sqlite3_column_type(st, 9) != SQLITE_NULL) {
    out.force_md5_etag = (sqlite3_column_int64(st, 9) != 0);
  }
  return 0;
}

} // namespace

bool placement_path_is_acceptable(std::string_view path)
{
  /* Empty is the default location, and saying so is not a path. */
  if (path.empty()) {
    return false;
  }
  if (path.front() != '/') {
    return false;
  }
  /* `/` itself is absolute and is not a place to put a bucket. */
  if (path.size() == 1) {
    return false;
  }
  if (path.back() == '/') {
    return false;
  }
  if (path.size() > PATH_MAX) {
    return false;
  }

  /* Canonical:  every component non-empty, and none of them `.` or
   * `..`.  Checked lexically rather than by resolving, because
   * resolving answers a different question -- what the path points at
   * now -- and the answer changes under us.  What this rules out is a
   * stored path that *means* something other than it reads. */
  size_t i = 1;
  while (i <= path.size()) {
    const size_t end = std::min(path.find('/', i), path.size());
    const std::string_view part = path.substr(i, end - i);
    if (part.empty() || (part == ".") || (part == "..")) {
      return false;
    }
    /* A newline in a path would be legal on the filesystem and would
     * make every log line that quotes it ambiguous. */
    if (part.find_first_of("\n\r") != std::string_view::npos) {
      return false;
    }
    i = end + 1;
  }
  return true;
}

int IdentityDB::Initialize(std::string logfile, int loglevel)
{
  int ret = POSIXUserDB::Initialize(logfile, loglevel);
  if (ret < 0) {
    return ret;
  }

  /* The exclusivity rule is a CHECK rather than a caller's
   * responsibility, so a record NooBaa's own schema would reject
   * cannot exist here either.
   *
   * The group list is guarded by a GLOB that rejects any character
   * outside digits and commas.  It cannot express the full grammar --
   * stock SQLite has no regex -- but it stops the parser in
   * groups_from_text() from ever seeing the inputs most likely to
   * break it, and the schema is the only place that guard survives a
   * caller written later. */
  const std::string schema =
    "CREATE TABLE IF NOT EXISTS '" + identity_table + "' ("
    "  Identity TEXT NOT NULL PRIMARY KEY,"
    "  Uid INTEGER,"
    "  Gid INTEGER,"
    "  SupplementalGroups TEXT,"
    "  DistinguishedName TEXT,"
    "  NewBucketsPath TEXT,"
    "  CustomBucketPathAllowedList TEXT,"
    "  FsBackend TEXT,"
    "  NoobaaId TEXT,"
    /* X1.  A column added here does not reach a table that already
     * exists -- `CREATE TABLE IF NOT EXISTS` does nothing to one --
     * and that is accepted rather than migrated:  nsfs has not
     * shipped, so no deployment holds this table, and vstart.sh
     * removes the userdb on every start.  A migration belongs here
     * once there is a released schema to migrate *from*, written
     * against that rather than against a guess. */
    "  ForceMd5Etag INTEGER,"
    "  CHECK (ForceMd5Etag IS NULL OR ForceMd5Etag IN (0, 1)),"
    "  CHECK ((Uid IS NULL) = (Gid IS NULL)),"
    "  CHECK (NOT (Uid IS NOT NULL AND DistinguishedName IS NOT NULL)),"
    "  CHECK (Uid IS NULL OR (typeof(Uid) = 'integer' AND Uid >= 0)),"
    "  CHECK (Gid IS NULL OR (typeof(Gid) = 'integer' AND Gid >= 0)),"
    "  CHECK (SupplementalGroups IS NULL"
    "         OR SupplementalGroups NOT GLOB '*[^0-9,]*')"
    ");"
    "CREATE INDEX IF NOT EXISTS '" + identity_table + "_noobaa_id' ON '"
    + identity_table + "' (NoobaaId);"

    /* P1.  Bucket placement, in a table this backend owns.
     *
     * `(BucketName, Tenant)` is the key from the start.  dbstore's
     * own bucket table makes BucketName unique and leaves the tenant
     * out, and its comment has said for years that both should be the
     * key;  a table of ours need not inherit that.
     *
     * The path guard is a CHECK as well as a C++ check, for the
     * reason the group list's GLOB is one:  the schema is the only
     * place a guard survives a caller written later.  It cannot
     * express the whole rule -- stock SQLite has no regex -- so it
     * rejects what matters most:  a relative path, and `..` in any
     * position. */
    "CREATE TABLE IF NOT EXISTS '" + placement_table + "' ("
    "  BucketName TEXT NOT NULL,"
    "  Tenant TEXT NOT NULL DEFAULT '',"
    "  Path TEXT,"
    "  OwnsStorage INTEGER NOT NULL DEFAULT 1,"
    "  PRIMARY KEY (BucketName, Tenant),"
    "  CHECK (OwnsStorage IN (0, 1)),"
    "  CHECK (Path IS NULL OR Path = '' OR ("
    "           Path GLOB '/*'"
    "           AND Path NOT GLOB '*/../*'"
    "           AND Path NOT GLOB '*/..'"
    "           AND Path NOT GLOB '*//*'))"
    ");";

  ret = exec(&dp, schema.c_str(), nullptr);
  if (ret < 0) {
    ldpp_dout(&dp, 0) << "ERROR: could not create " << identity_table
      << dendl;
    return ret;
  }

  return 0;
}

int IdentityDB::put_identity(const DoutPrefixProvider* dpp, const Identity& id)
{
  if (id.key.empty()) {
    return -EINVAL;
  }

  const std::string sql =
    "INSERT OR REPLACE INTO '" + identity_table + "' (" + IDENTITY_COLUMNS
    + ") VALUES (?,?,?,?,?,?,?,?,?,?);";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: preparing identity insert: "
      << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });

  sqlite3_bind_text(st, 1, id.key.c_str(), -1, SQLITE_TRANSIENT);
  id.uid ? sqlite3_bind_int64(st, 2, *id.uid) : sqlite3_bind_null(st, 2);
  id.gid ? sqlite3_bind_int64(st, 3, *id.gid) : sqlite3_bind_null(st, 3);
  if (id.groups) {
    /* an explicit empty list is the empty string, not NULL:  the
     * difference is the whole point of the optional */
    const std::string g = groups_to_text(*id.groups);
    sqlite3_bind_text(st, 4, g.c_str(), -1, SQLITE_TRANSIENT);
  } else {
    sqlite3_bind_null(st, 4);
  }
  bind_text_or_null(st, 5, id.distinguished_name);
  bind_text_or_null(st, 6, id.new_buckets_path);
  bind_text_or_null(st, 7, id.custom_bucket_path_allowed_list);
  bind_text_or_null(st, 8, id.fs_backend);
  bind_text_or_null(st, 9, id.noobaa_id);
  id.force_md5_etag ? sqlite3_bind_int64(st, 10, *id.force_md5_etag ? 1 : 0)
		    : sqlite3_bind_null(st, 10);

  if (sqlite3_step(st) != SQLITE_DONE) {
    /* a CHECK violation lands here, and it is the expected way an
     * invalid record is refused */
    ldpp_dout(dpp, 4) << "identity " << id.key << " refused: "
      << sqlite3_errmsg(handle()) << dendl;
    return -EINVAL;
  }
  return 0;
}

int IdentityDB::get_identity(const DoutPrefixProvider* dpp,
			     const std::string& key, Identity& out) const
{
  const std::string sql =
    std::string("SELECT ") + IDENTITY_COLUMNS + " FROM '" + identity_table
    + "' WHERE Identity = ?;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, key.c_str(), -1, SQLITE_TRANSIENT);

  int rc = sqlite3_step(st);
  if (rc == SQLITE_DONE) {
    return -ENOENT;
  }
  if (rc != SQLITE_ROW) {
    return -EIO;
  }
  return row_to_identity(dpp, st, out);
}

int IdentityDB::get_identity_by_noobaa_id(const DoutPrefixProvider* dpp,
					  const std::string& noobaa_id,
					  Identity& out) const
{
  if (noobaa_id.empty()) {
    return -EINVAL;
  }

  const std::string sql =
    std::string("SELECT ") + IDENTITY_COLUMNS + " FROM '" + identity_table
    + "' WHERE NoobaaId = ?;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, noobaa_id.c_str(), -1, SQLITE_TRANSIENT);

  int rc = sqlite3_step(st);
  if (rc == SQLITE_DONE) {
    return -ENOENT;
  }
  if (rc != SQLITE_ROW) {
    return -EIO;
  }
  return row_to_identity(dpp, st, out);
}

int IdentityDB::remove_identity(const DoutPrefixProvider* dpp,
				const std::string& key)
{
  const std::string sql =
    "DELETE FROM '" + identity_table + "' WHERE Identity = ?;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, key.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    return -EIO;
  }
  /* deleting what is not there is not an error;  the caller asked for
   * it to be gone and it is */
  return 0;
}

int IdentityDB::list_identities(const DoutPrefixProvider* dpp,
				std::vector<Identity>& out) const
{
  const std::string sql =
    std::string("SELECT ") + IDENTITY_COLUMNS + " FROM '" + identity_table
    + "' ORDER BY Identity;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });

  int rc;
  while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
    Identity id;
    int ret = row_to_identity(dpp, st, id);
    if (ret < 0) {
      return ret;
    }
    out.push_back(std::move(id));
  }
  return (rc == SQLITE_DONE) ? 0 : -EIO;
}

int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const rgw_owner& owner, Credentials& out)
{
  const std::string key = to_string(owner);

  Identity id;
  int ret = db.get_identity(dpp, key, id);
  if (ret < 0) {
    return ret;			/* -ENOENT among them */
  }

  if (id.directory_backed()) {
    ldpp_dout(dpp, 4) << "identity " << key << " is directory-backed ("
      << id.distinguished_name << ") and the directory is not"
      " consulted;  refusing to serve it unimpersonated" << dendl;
    return -EPERM;
  }

  if (! id.local()) {
    /* a row carrying only placement or provenance data asks for no
     * impersonation, which is not the same as asking for one we
     * cannot give */
    return -ENOENT;
  }

  out.uid = *id.uid;
  out.gid = *id.gid;

  /* Both an explicitly empty list and an absent one give an empty
   * vector.  Absent means "no supplementary groups" today, and the
   * caller installs that rather than skipping the call -- their
   * ThreadScope issues setgroups(0, NULL) for exactly this case.
   * The two stay distinct in the record because the directory arm
   * will read them differently. */
  out.groups.clear();
  if (id.groups) {
    out.groups.assign(id.groups->begin(), id.groups->end());
  }
  return 0;
}

int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const req_state* s, Credentials& out)
{
  if (!s || !s->user) {
    /* nothing authenticated:  nothing to impersonate as */
    return -ENOENT;
  }
  return resolve_credentials(dpp, db, s->user->get_id(), out);
}


/* ---------------------------------------------------------------- *
 * Bucket placement.  P1:  the column and the accessor.
 * ---------------------------------------------------------------- */

namespace {

constexpr const char* PLACEMENT_COLUMNS =
  "BucketName, Tenant, Path, OwnsStorage";

} // namespace

int IdentityDB::put_bucket_placement(const DoutPrefixProvider* dpp,
				     const BucketPlacement& bp)
{
  if (bp.bucket_name.empty()) {
    return -EINVAL;
  }
  /* Empty is the default location and is stored as empty;  anything
   * else has to be a path we will accept.  Refused here with a
   * message rather than left to the CHECK, so an operator learns what
   * was wrong with it (`NSFS_BUCKET_PLACEMENT_PLAN.md` §9). */
  if (!bp.path.empty() && !placement_path_is_acceptable(bp.path)) {
    ldpp_dout(dpp, 4) << "bucket placement " << bp.bucket_name
      << " refused: " << bp.path << " is not an absolute canonical path"
      << dendl;
    return -EINVAL;
  }

  const std::string sql =
    "INSERT OR REPLACE INTO '" + placement_table + "' ("
    + PLACEMENT_COLUMNS + ") VALUES (?,?,?,?);";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });

  sqlite3_bind_text(st, 1, bp.bucket_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, bp.tenant.c_str(), -1, SQLITE_TRANSIENT);
  /* The empty path is stored as the empty string rather than NULL.
   * Both mean the default location and one of them is enough;  the
   * CHECK accepts either, because a row written by hand is a thing
   * this table will see. */
  sqlite3_bind_text(st, 3, bp.path.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 4, bp.owns_storage ? 1 : 0);

  if (sqlite3_step(st) != SQLITE_DONE) {
    ldpp_dout(dpp, 4) << "bucket placement " << bp.bucket_name
      << " refused: " << sqlite3_errmsg(handle()) << dendl;
    return -EINVAL;
  }
  return 0;
}

int IdentityDB::get_bucket_placement(const DoutPrefixProvider* dpp,
				     const std::string& bucket_name,
				     const std::string& tenant,
				     BucketPlacement& out) const
{
  const std::string sql =
    std::string("SELECT ") + PLACEMENT_COLUMNS + " FROM '"
    + placement_table + "' WHERE BucketName = ? AND Tenant = ?;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });

  sqlite3_bind_text(st, 1, bucket_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, tenant.c_str(), -1, SQLITE_TRANSIENT);

  const int step = sqlite3_step(st);
  if (step == SQLITE_DONE) {
    /* No row, which is what a bucket at the default location looks
     * like.  The ordinary case, and not an error. */
    return -ENOENT;
  }
  if (step != SQLITE_ROW) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }

  out = BucketPlacement{};
  out.bucket_name = column_text(st, 0);
  out.tenant = column_text(st, 1);
  out.path = column_text(st, 2);
  out.owns_storage = (sqlite3_column_int64(st, 3) != 0);
  return 0;
}

int IdentityDB::remove_bucket_placement(const DoutPrefixProvider* dpp,
					const std::string& bucket_name,
					const std::string& tenant)
{
  const std::string sql =
    "DELETE FROM '" + placement_table
    + "' WHERE BucketName = ? AND Tenant = ?;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });

  sqlite3_bind_text(st, 1, bucket_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, tenant.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  /* Removing a row that is not there is not a failure:  a bucket at
   * the default location has none. */
  return 0;
}

}}} // namespace rgw::sal::nsfs
