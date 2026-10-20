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
#include "common/split.h"
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
  "NewBucketsPath, CustomBucketPathAllowedList, FsBackend, ForceMd5Etag";

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
  /* NULL is "said nothing", which is not the same as 0 */
  if (sqlite3_column_type(st, 8) != SQLITE_NULL) {
    out.force_md5_etag = (sqlite3_column_int64(st, 8) != 0);
  }
  return 0;
}

} // namespace

int normalize_allowed_list(std::string_view in, std::string& out,
			   std::string& bad)
{
  out.clear();
  for (std::string_view entry : ceph::split(in, ":")) {
    while (!entry.empty() && ((entry.front() == ' ') || (entry.front() == '\t'))) {
      entry.remove_prefix(1);
    }
    while (!entry.empty() && ((entry.back() == ' ') || (entry.back() == '\t'))) {
      entry.remove_suffix(1);
    }
    if (entry.empty()) {
      continue;
    }
    if (!placement_path_is_acceptable(entry)) {
      bad = std::string(entry);
      return -EINVAL;
    }
    if (!out.empty()) {
      out += ':';
    }
    out += entry;
  }
  return 0;
}

bool allowed_list_admits(std::string_view list, std::string_view path)
{
  for (std::string_view prefix : ceph::split(list, ":")) {
    if (path.starts_with(prefix)) {
      return true;
    }
  }
  return false;
}

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
   * resolving establishes something different -- what the path points
   * at now -- and that changes under us.  What this rules out is a
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

    /* Completing a credential built from a file reads by uid, which
     * is the only lookup here not keyed on `Identity`.  Unlike a
     * column, an index does reach a table that already exists, so
     * this needs none of X1's reasoning. */
    "CREATE INDEX IF NOT EXISTS '" + identity_table + "_uid' ON '"
    + identity_table + "' (Uid);"

    /* Provenance:  NooBaa's `_id` to the owner it was imported as.
     * The primary key is the whole uniqueness rule, and nothing writes
     * here with OR REPLACE, so a second import of one id is rejected
     * rather than repointed. */
    "CREATE TABLE IF NOT EXISTS '" + noobaa_id_table + "' ("
    "  NoobaaId TEXT NOT NULL PRIMARY KEY,"
    "  Owner TEXT NOT NULL,"
    "  CHECK (NoobaaId != '' AND Owner != '')"
    ");"

    /* P1.  Bucket placement, in a table this backend owns.
     *
     * `(BucketName, Tenant)` is the key from the start.  dbstore's
     * own bucket table makes BucketName unique and leaves the tenant
     * out, and its comment has said for years that both should be the
     * key;  an nsfs table need not inherit that.
     *
     * The path guard is a CHECK as well as a C++ check, for the
     * reason the group list's GLOB is one:  the schema is the only
     * place a guard survives a caller written later.  It cannot
     * express the whole rule -- stock SQLite has no regex -- so it
     * rejects what matters most:  a relative path, and `..` in any
     * position.
     *
     * Every row carries a path, and no two rows share one:  two
     * buckets in one directory would be one tree served twice.  That a
     * path lies inside another row's is rejected by the writers, since
     * a CHECK cannot see other rows. */
    "CREATE TABLE IF NOT EXISTS '" + placement_table + "' ("
    "  BucketName TEXT NOT NULL,"
    "  Tenant TEXT NOT NULL DEFAULT '',"
    "  Path TEXT NOT NULL,"
    "  Owner TEXT NOT NULL,"
    "  PRIMARY KEY (BucketName, Tenant),"
    "  UNIQUE (Path),"
    "  CHECK (Owner != ''),"
    "  CHECK (Path GLOB '/*'"
    "         AND Path NOT GLOB '*/../*'"
    "         AND Path NOT GLOB '*/..'"
    "         AND Path NOT GLOB '*//*')"
    ");"

    /* ListBuckets is one owner's rows in one tenant, in name order. */
    "CREATE INDEX IF NOT EXISTS '" + placement_table + "_owner' ON '"
    + placement_table + "' (Owner, Tenant, BucketName);"

    /* Adoption's completion record.  Id is fixed at 1, so the key is
     * the whole of "at most one row". */
    "CREATE TABLE IF NOT EXISTS '" + completion_table + "' ("
    "  Id INTEGER NOT NULL PRIMARY KEY CHECK (Id = 1),"
    "  CompletedAt TEXT NOT NULL,"
    "  ConfigRoot TEXT NOT NULL,"
    "  Identities INTEGER NOT NULL,"
    "  Buckets INTEGER NOT NULL,"
    "  GatewayVersion TEXT NOT NULL,"
    "  Extensions INTEGER NOT NULL"
    ");"

    /* NooBaa records abandoned by an operator, by `_id`.  A reason is
     * required:  the point of the table is that an operator can see
     * later what was left behind and why. */
    "CREATE TABLE IF NOT EXISTS '" + abandoned_table + "' ("
    "  NoobaaId TEXT NOT NULL PRIMARY KEY,"
    "  Kind TEXT NOT NULL,"
    "  Name TEXT NOT NULL,"
    "  Stage TEXT NOT NULL,"
    "  Reason TEXT NOT NULL,"
    "  At TEXT NOT NULL,"
    "  CHECK (NoobaaId != '' AND Reason != ''),"
    "  CHECK (Kind IN ('identity', 'bucket')),"
    "  CHECK (Stage IN ('not imported', 'not converted',"
    "                   'partly converted'))"
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
  /* The custom-bucket-path allowlist is checked here, where it is stored,
   * so a bad entry is rejected with a message rather than found by a
   * CreateBucket later.  Both writers come through here:  the import and
   * the identity Admin Ops. */
  /* new_buckets_path, the same way:  its form is checked here, where it
   * is stored (DECISIONS.md 2026-10-14).  A trailing slash, which an
   * operator may well write, is dropped rather than rejected.  Whether the
   * directory exists is not asked:  storage on Scale is attachable later,
   * and a missing parent fails CreateBucket plainly. */
  std::string nbp = id.new_buckets_path;
  while ((nbp.size() > 1) && (nbp.back() == '/')) {
    nbp.pop_back();
  }
  if (!nbp.empty() && !placement_path_is_acceptable(nbp)) {
    ldpp_dout(dpp, 0) << "ERROR: identity " << id.key << " rejected:  its "
      << "new_buckets_path \"" << id.new_buckets_path << "\" is not an "
      << "absolute canonical path" << dendl;
    return -EINVAL;
  }
  std::string allowed;
  if (!id.custom_bucket_path_allowed_list.empty()) {
    std::string bad;
    if (normalize_allowed_list(id.custom_bucket_path_allowed_list, allowed,
			       bad) < 0) {
      ldpp_dout(dpp, 0) << "ERROR: identity " << id.key << " rejected:  its "
	<< "custom_bucket_path_allowed_list entry \"" << bad << "\" is not "
	<< "an absolute canonical path" << dendl;
      return -EINVAL;
    }
  }

  const std::string sql =
    "INSERT OR REPLACE INTO '" + identity_table + "' (" + IDENTITY_COLUMNS
    + ") VALUES (?,?,?,?,?,?,?,?,?);";

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
  bind_text_or_null(st, 6, nbp);
  bind_text_or_null(st, 7, allowed);
  bind_text_or_null(st, 8, id.fs_backend);
  id.force_md5_etag ? sqlite3_bind_int64(st, 9, *id.force_md5_etag ? 1 : 0)
		    : sqlite3_bind_null(st, 9);

  if (sqlite3_step(st) != SQLITE_DONE) {
    /* a CHECK violation lands here, and it is the expected way an
     * invalid record is rejected */
    ldpp_dout(dpp, 4) << "identity " << id.key << " rejected: "
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

int IdentityDB::put_noobaa_id(const DoutPrefixProvider* dpp,
			       const std::string& noobaa_id,
			       const std::string& owner_key)
{
  if (noobaa_id.empty() || owner_key.empty()) {
    return -EINVAL;
  }

  const std::string sql =
    "INSERT INTO '" + noobaa_id_table + "' (NoobaaId, Owner) VALUES (?,?);";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: preparing NooBaa id insert: "
      << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, noobaa_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, owner_key.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    if (sqlite3_extended_errcode(handle()) == SQLITE_CONSTRAINT_PRIMARYKEY) {
      ldpp_dout(dpp, 4) << "NooBaa id " << noobaa_id
	<< " is already recorded" << dendl;
      return -EEXIST;
    }
    ldpp_dout(dpp, 0) << "ERROR: recording NooBaa id " << noobaa_id << ": "
      << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  return 0;
}

int IdentityDB::get_owner_by_noobaa_id(const DoutPrefixProvider* dpp,
				       const std::string& noobaa_id,
				       std::string& owner_key) const
{
  if (noobaa_id.empty()) {
    return -EINVAL;
  }

  const std::string sql =
    "SELECT Owner FROM '" + noobaa_id_table + "' WHERE NoobaaId = ?;";

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
  owner_key = column_text(st, 0);
  return 0;
}

int IdentityDB::put_adoption_completion(const DoutPrefixProvider* dpp,
					const AdoptionCompletion& c)
{
  const std::string sql =
    "INSERT INTO '" + completion_table + "' (Id, CompletedAt, ConfigRoot, "
    "Identities, Buckets, GatewayVersion, Extensions) "
    "VALUES (1, ?, ?, ?, ?, ?, ?);";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, c.completed_at.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, c.config_root.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 3, static_cast<sqlite3_int64>(c.identities));
  sqlite3_bind_int64(st, 4, static_cast<sqlite3_int64>(c.buckets));
  sqlite3_bind_text(st, 5, c.gateway_version.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 6, c.extensions);

  if (sqlite3_step(st) != SQLITE_DONE) {
    if (sqlite3_extended_errcode(handle()) == SQLITE_CONSTRAINT_PRIMARYKEY) {
      return -EEXIST;
    }
    ldpp_dout(dpp, 0) << "ERROR: writing the completion record: "
      << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  return 0;
}

int IdentityDB::get_adoption_completion(const DoutPrefixProvider* dpp,
					AdoptionCompletion& c) const
{
  const std::string sql =
    "SELECT CompletedAt, ConfigRoot, Identities, Buckets, GatewayVersion, "
    "Extensions FROM '" + completion_table + "' WHERE Id = 1;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });

  const int step = sqlite3_step(st);
  if (step == SQLITE_DONE) {
    return -ENOENT;
  }
  if (step != SQLITE_ROW) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  c.completed_at = column_text(st, 0);
  c.config_root = column_text(st, 1);
  c.identities = static_cast<uint64_t>(sqlite3_column_int64(st, 2));
  c.buckets = static_cast<uint64_t>(sqlite3_column_int64(st, 3));
  c.gateway_version = column_text(st, 4);
  c.extensions = static_cast<uint32_t>(sqlite3_column_int64(st, 5));
  return 0;
}

namespace {

constexpr const char* ABANDONED_COLUMNS =
  "NoobaaId, Kind, Name, Stage, Reason, At";

AbandonedRecord row_to_abandoned(sqlite3_stmt* st)
{
  AbandonedRecord r;
  r.noobaa_id = column_text(st, 0);
  r.kind = column_text(st, 1);
  r.name = column_text(st, 2);
  r.stage = column_text(st, 3);
  r.reason = column_text(st, 4);
  r.at = column_text(st, 5);
  return r;
}

} // namespace

int IdentityDB::put_abandoned(const DoutPrefixProvider* dpp,
			      const AbandonedRecord& r)
{
  if (r.noobaa_id.empty() || r.reason.empty()) {
    return -EINVAL;
  }
  const std::string sql =
    "INSERT INTO '" + abandoned_table + "' (" + ABANDONED_COLUMNS + ") "
    "VALUES (?, ?, ?, ?, ?, ?);";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, r.noobaa_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, r.kind.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, r.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 4, r.stage.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 5, r.reason.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 6, r.at.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    if (sqlite3_extended_errcode(handle()) == SQLITE_CONSTRAINT_PRIMARYKEY) {
      return -EEXIST;
    }
    ldpp_dout(dpp, 4) << "abandoning " << r.noobaa_id << " failed: "
      << sqlite3_errmsg(handle()) << dendl;
    return -EINVAL;
  }
  return 0;
}

int IdentityDB::remove_abandoned(const DoutPrefixProvider* dpp,
				 const std::string& noobaa_id)
{
  const std::string sql =
    "DELETE FROM '" + abandoned_table + "' WHERE NoobaaId = ?;";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, noobaa_id.c_str(), -1, SQLITE_TRANSIENT);
  if (sqlite3_step(st) != SQLITE_DONE) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  return (sqlite3_changes(handle()) == 0) ? -ENOENT : 0;
}

int IdentityDB::get_abandoned(const DoutPrefixProvider* dpp,
			      const std::string& noobaa_id,
			      AbandonedRecord& r) const
{
  const std::string sql =
    std::string("SELECT ") + ABANDONED_COLUMNS + " FROM '" + abandoned_table
    + "' WHERE NoobaaId = ?;";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, noobaa_id.c_str(), -1, SQLITE_TRANSIENT);
  const int step = sqlite3_step(st);
  if (step == SQLITE_DONE) {
    return -ENOENT;
  }
  if (step != SQLITE_ROW) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  r = row_to_abandoned(st);
  return 0;
}

int IdentityDB::list_abandoned(const DoutPrefixProvider* dpp,
			       std::vector<AbandonedRecord>& out) const
{
  out.clear();
  const std::string sql =
    std::string("SELECT ") + ABANDONED_COLUMNS + " FROM '" + abandoned_table
    + "' ORDER BY NoobaaId;";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  int rc;
  while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
    out.push_back(row_to_abandoned(st));
  }
  if (rc != SQLITE_DONE) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  return 0;
}

int IdentityDB::owner_was_imported(const DoutPrefixProvider* dpp,
				   const std::string& owner_key,
				   bool& imported) const
{
  const std::string sql =
    "SELECT 1 FROM '" + noobaa_id_table + "' WHERE Owner = ? LIMIT 1;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, owner_key.c_str(), -1, SQLITE_TRANSIENT);

  const int step = sqlite3_step(st);
  if ((step != SQLITE_ROW) && (step != SQLITE_DONE)) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  imported = (step == SQLITE_ROW);
  return 0;
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

int IdentityDB::get_identities_by_uid(const DoutPrefixProvider* dpp,
				      uid_t uid,
				      std::vector<Identity>& out) const
{
  const std::string sql =
    std::string("SELECT ") + IDENTITY_COLUMNS + " FROM '" + identity_table
    + "' WHERE Uid = ? ORDER BY Identity;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_int64(st, 1, static_cast<sqlite3_int64>(uid));

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

int complete_file_credentials(const DoutPrefixProvider* dpp,
			      const IdentityDB& db, Credentials& inout,
			      std::string* key)
{
  std::vector<Identity> rows;
  int ret = db.get_identities_by_uid(dpp, inout.uid, rows);
  if (ret < 0) {
    return ret;
  }

  /* A directory-backed row carries no uid, so it cannot be here;  a
   * row carrying only placement data can, and it asks for no
   * impersonation.  Drop those rather than read an empty group list
   * off them. */
  std::erase_if(rows, [](const Identity& id) { return !id.local(); });
  if (rows.empty()) {
    return -ENOENT;
  }

  const auto& first = rows.front();
  for (const auto& id : rows) {
    if (id.groups != first.groups) {
      ldpp_dout(dpp, 0) << "ERROR: uid " << inout.uid << " is carried by "
	<< rows.size() << " identities whose supplemental group lists "
	"differ (" << first.key << ", " << id.key << ");  a file names its "
	"owner by uid alone, so it cannot say which applies.  Acting on "
	"the file owner's own rights instead" << dendl;
      return -ENODATA;
    }
  }

  inout.groups.clear();
  if (first.groups) {
    inout.groups.assign(first.groups->begin(), first.groups->end());
  }
  /* Only one row names an identity.  Where several agree, their
   * credentials coincide and their keys do not, so there is nothing
   * to name. */
  if (key && (rows.size() == 1)) {
    *key = first.key;
  }
  return 0;
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
      " consulted;  denying it rather than serving it unimpersonated"
      << dendl;
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
   * caller installs that rather than skipping the call -- NooBaa's
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
			const rgw_user& user, const rgw_account_id& account,
			Credentials& out, std::string* row)
{
  rgw_owner owner{user};
  int ret = resolve_credentials(dpp, db, owner, out);
  if ((ret == -ENOENT) && !account.empty()) {
    owner = account;
    ret = resolve_credentials(dpp, db, owner, out);
  }
  if (row && ((ret == 0) || (ret == -EPERM))) {
    *row = to_string(owner);
  }
  return ret;
}

int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const req_state* s, Credentials& out, std::string* row)
{
  if (!s || !s->user) {
    /* nothing authenticated:  nothing to impersonate as */
    return -ENOENT;
  }
  return resolve_credentials(dpp, db, s->user->get_id(),
			     s->user->get_info().account_id, out, row);
}


/* ---------------------------------------------------------------- *
 * Bucket placement.  P1:  the column and the accessor.
 * ---------------------------------------------------------------- */

namespace {

constexpr const char* PLACEMENT_COLUMNS =
  "BucketName, Tenant, Path, Owner";

BucketPlacement row_to_placement(sqlite3_stmt* st)
{
  BucketPlacement bp;
  bp.bucket_name = column_text(st, 0);
  bp.tenant = column_text(st, 1);
  bp.path = column_text(st, 2);
  bp.owner = column_text(st, 3);
  return bp;
}

} // namespace

/* Both writers.  `repoint` is put's:  an existing row for the key
 * takes the new path.  Without it a second row for the key is rejected,
 * which is what makes creation exclusive.
 *
 * Neither uses OR REPLACE.  With UNIQUE (Path) it would delete
 * whichever other bucket's row held the path, silently. */
int IdentityDB::write_bucket_placement(const DoutPrefixProvider* dpp,
				       const BucketPlacement& bp,
				       bool repoint)
{
  if (bp.bucket_name.empty() || bp.owner.empty()) {
    return -EINVAL;
  }
  /* Rejected here with a message rather than left to the CHECK, so an
   * operator learns what was wrong with it
   * (`NSFS_BUCKET_PLACEMENT_PLAN.md` §9). */
  if (!placement_path_is_acceptable(bp.path)) {
    ldpp_dout(dpp, 4) << "bucket placement " << bp.bucket_name
      << " rejected: \"" << bp.path
      << "\" is not an absolute canonical path" << dendl;
    return -EINVAL;
  }

  /* One statement, so that the containment test and the write cannot
   * be separated by another writer:  the row goes in only if no other
   * bucket's path contains this one or lies inside it.  Equality is
   * UNIQUE (Path)'s. */
  std::string sql =
    "INSERT INTO '" + placement_table + "' (" + PLACEMENT_COLUMNS + ") "
    "SELECT ?1, ?2, ?3, ?4 WHERE NOT EXISTS ("
    "  SELECT 1 FROM '" + placement_table + "'"
    "  WHERE NOT (BucketName = ?1 AND Tenant = ?2)"
    "  AND (substr(?3, 1, length(Path) + 1) = Path || '/'"
    "       OR substr(Path, 1, length(?3) + 1) = ?3 || '/'))";
  if (repoint) {
    sql += " ON CONFLICT (BucketName, Tenant) DO UPDATE SET Path = excluded.Path,"
	   " Owner = excluded.Owner";
  }
  sql += ";";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });

  sqlite3_bind_text(st, 1, bp.bucket_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, bp.tenant.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, bp.path.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 4, bp.owner.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    const int ext = sqlite3_extended_errcode(handle());
    ldpp_dout(dpp, 4) << "bucket placement " << bp.bucket_name << " at "
      << bp.path << " failed: " << sqlite3_errmsg(handle()) << dendl;
    if ((ext != SQLITE_CONSTRAINT_PRIMARYKEY) &&
	(ext != SQLITE_CONSTRAINT_UNIQUE)) {
      return -EINVAL;
    }
    /* A row this name already has is -EEXIST, whichever constraint
     * SQLite names:  the same name at the same path breaks both the key
     * and UNIQUE (Path), and it reports the latter. */
    BucketPlacement held;
    if (get_bucket_placement(dpp, bp.bucket_name, bp.tenant, held) == 0) {
      return -EEXIST;
    }
    return -EADDRINUSE;
  }
  if (sqlite3_changes(handle()) == 0) {
    ldpp_dout(dpp, 4) << "bucket placement " << bp.bucket_name << " at "
      << bp.path << " rejected: another bucket's directory contains it or "
      << "lies inside it" << dendl;
    return -EADDRINUSE;
  }
  return 0;
}

int IdentityDB::put_bucket_placement(const DoutPrefixProvider* dpp,
				     const BucketPlacement& bp)
{
  return write_bucket_placement(dpp, bp, true);
}

int IdentityDB::insert_bucket_placement(const DoutPrefixProvider* dpp,
					const BucketPlacement& bp)
{
  return write_bucket_placement(dpp, bp, false);
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

  out = row_to_placement(st);
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

int IdentityDB::set_bucket_placement_owner(const DoutPrefixProvider* dpp,
					   const std::string& bucket_name,
					   const std::string& tenant,
					   const std::string& owner)
{
  if (owner.empty()) {
    return -EINVAL;
  }
  const std::string sql =
    "UPDATE '" + placement_table + "' SET Owner = ? "
    "WHERE BucketName = ? AND Tenant = ?;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, owner.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, bucket_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, tenant.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(handle()) << dendl;
    return -EIO;
  }
  return 0;
}

namespace {

/* Run a placement query with its parameters bound, collecting rows. */
template <typename Bind>
int collect_placements(const DoutPrefixProvider* dpp, sqlite3* h,
		       const std::string& sql, Bind&& bind,
		       std::vector<BucketPlacement>& out)
{
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(h, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(h) << dendl;
    return -EIO;
  }
  auto finalize = make_scope_guard([st] { sqlite3_finalize(st); });
  bind(st);

  int rc;
  while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
    out.push_back(row_to_placement(st));
  }
  if (rc != SQLITE_DONE) {
    ldpp_dout(dpp, 0) << "ERROR: " << sqlite3_errmsg(h) << dendl;
    return -EIO;
  }
  return 0;
}

} // namespace

int IdentityDB::list_bucket_placements_by_owner(
  const DoutPrefixProvider* dpp, const std::string& owner,
  const std::string& tenant, const std::string& marker,
  const std::string& end_marker, uint64_t max,
  std::vector<BucketPlacement>& out) const
{
  out.clear();
  const std::string sql =
    std::string("SELECT ") + PLACEMENT_COLUMNS + " FROM '"
    + placement_table + "' WHERE Owner = ?1 AND Tenant = ?2"
    " AND BucketName > ?3 AND (?4 = '' OR BucketName < ?4)"
    " ORDER BY BucketName LIMIT ?5;";
  return collect_placements(dpp, handle(), sql, [&](sqlite3_stmt* st) {
      sqlite3_bind_text(st, 1, owner.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 2, tenant.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 3, marker.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 4, end_marker.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_int64(st, 5, static_cast<sqlite3_int64>(max));
    }, out);
}

int IdentityDB::list_bucket_placements(const DoutPrefixProvider* dpp,
				       const std::string& marker,
				       uint64_t max,
				       std::vector<BucketPlacement>& out) const
{
  out.clear();
  const std::string sql =
    std::string("SELECT ") + PLACEMENT_COLUMNS + " FROM '"
    + placement_table + "' WHERE BucketName > ?1"
    " ORDER BY BucketName, Tenant LIMIT ?2;";
  return collect_placements(dpp, handle(), sql, [&](sqlite3_stmt* st) {
      sqlite3_bind_text(st, 1, marker.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(max));
    }, out);
}

namespace {
/* One text column per row, in order, after `marker`, at most `max`.
 * `sql` binds the marker as ?1 and the limit as ?2. */
int list_keys(const DoutPrefixProvider* dpp, sqlite3* db,
	      const std::string& sql, const char* what,
	      const std::string& marker, uint64_t max,
	      std::list<std::string>& out)
{
  out.clear();
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
    ldpp_dout(dpp, 0) << "ERROR: listing " << what << ": "
      << sqlite3_errmsg(db) << dendl;
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, marker.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, static_cast<sqlite3_int64>(max));
  int rc;
  while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
    const unsigned char* key = sqlite3_column_text(st, 0);
    if (key) {
      out.emplace_back(reinterpret_cast<const char*>(key));
    }
  }
  return (rc == SQLITE_DONE) ? 0 : -EIO;
}
} // namespace

int IdentityDB::list_account_ids(const DoutPrefixProvider* dpp,
				 const std::string& marker, uint64_t max,
				 std::list<std::string>& out)
{
  return list_keys(dpp, handle(),
		   "SELECT AccountID FROM '" + getAccountTable() + "'"
		   " WHERE AccountID > ?1 ORDER BY AccountID LIMIT ?2;",
		   "accounts", marker, max, out);
}

int IdentityDB::list_topic_keys(const DoutPrefixProvider* dpp,
				const std::string& marker, uint64_t max,
				std::list<std::string>& out)
{
  /* RGW's topic metadata key, `<tenant>:<name>`;  the table's Tenant is
   * the owner's tenant, or its account id, which is the key's first
   * half either way (`get_topic_metadata_key()`) */
  return list_keys(dpp, handle(),
		   "SELECT Tenant || ':' || TopicName AS MetaKey FROM '"
		   + getTopicTable() + "'"
		   " WHERE MetaKey > ?1 ORDER BY MetaKey LIMIT ?2;",
		   "topics", marker, max, out);
}

}}} // namespace rgw::sal::nsfs
