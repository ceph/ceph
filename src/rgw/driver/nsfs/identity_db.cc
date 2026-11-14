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
#include <charconv>
#include <mutex>
#include <sqlite3.h>
#include "common/errno.h"
#include "include/scope_guard.h"

#include "rgw_common.h"
#include "rgw_sal.h"
#include "identity_db.h"
#include "impersonate.h"

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

/* The connection is dbstore's, shared by every thread
 * (SQLITE_OPEN_FULLMUTEX):  a step and the sqlite3_changes() or
 * sqlite3_extended_errcode() read after it are two calls, and another
 * thread's statement between them replaces what the second returns.
 * The connection's mutex, held across both, keeps them together;  it is
 * recursive in that mode, so the step takes it again.  A BasicLockable
 * for std::lock_guard. */
struct ConnectionMutex {
  sqlite3_mutex* m;
  void lock() { sqlite3_mutex_enter(m); }
  void unlock() { sqlite3_mutex_leave(m); }
};

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
      ldpp_dout_fmt(dpp, 0, "ERROR: identity {} has an unparsable "
			    "supplemental group list", out.key);
      return -EBADMSG;
    }
    out.groups = std::move(g);
  }

  out.distinguished_name = column_text(st, 4);
  out.new_buckets_path = column_text(st, 5);
  out.custom_bucket_path_allowed_list = column_text(st, 6);
  out.fs_backend = column_text(st, 7);
  if (sqlite3_column_type(st, 8) != SQLITE_NULL) {
    out.force_md5_etag = (sqlite3_column_int64(st, 8) != 0);
  }
  return 0;
}

} // namespace
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

    /* Completing a credential built from a file reads by uid, which
     * is the only lookup here not keyed on `Identity`.  Unlike a
     * column, an index does reach a table that already exists, so
     * this needs none of X1's reasoning. */
    "CREATE INDEX IF NOT EXISTS '" + identity_table + "_uid' ON '"
    + identity_table + "' (Uid);"

  ret = exec(&dp, schema.c_str(), nullptr);
  if (ret < 0) {
    ldpp_dout_fmt(&dp, 0, "ERROR: could not create {}", identity_table);
    return ret;
  }

  return 0;
}

int IdentityDB::put_identity(const DoutPrefixProvider* dpp, const Identity& id)
{
  if (id.key.empty()) {
    return -EINVAL;
  Identity old;
  const bool had = (get_identity(dpp, id.key, old) == 0);

  const std::string sql =
    "INSERT OR REPLACE INTO '" + identity_table + "' (" + IDENTITY_COLUMNS
    + ") VALUES (?,?,?,?,?,?,?,?,?);";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout_fmt(dpp, 0, "ERROR: preparing identity insert: {}",
			  sqlite3_errmsg(handle()));
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
  bind_text_or_null(st, 8, id.fs_backend);
  id.force_md5_etag ? sqlite3_bind_int64(st, 9, *id.force_md5_etag ? 1 : 0)
		    : sqlite3_bind_null(st, 9);

  if (sqlite3_step(st) != SQLITE_DONE) {
    /* a CHECK violation lands here, and it is the expected way an
     * invalid record is rejected */
    ldpp_dout_fmt(dpp, 4, "identity {} rejected: {}", id.key,
			  sqlite3_errmsg(handle()));
    return -EINVAL;
  }
  /* A row written is a reason to look its name up again, even the same
   * name:  whoever wrote it may know the directory changed.  The name it
   * no longer carries is cleared too.  Neither invalidates anything
   * unless the cache held it. */
  if (had && !old.distinguished_name.empty()) {
    names.forget(old.distinguished_name);
  }
  if (!id.distinguished_name.empty()) {
    names.forget(id.distinguished_name);
  }
  if (had && old.uid) {
    names.forget_uid(*old.uid);
  }
  if (id.uid) {
    names.forget_uid(*id.uid);
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

  if (noobaa_id.empty() || owner_key.empty()) {
    return -EINVAL;
  }

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    ldpp_dout_fmt(dpp, 0, "ERROR: preparing NooBaa id insert: {}",
			  sqlite3_errmsg(handle()));
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, noobaa_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, owner_key.c_str(), -1, SQLITE_TRANSIENT);
  ConnectionMutex conn{sqlite3_db_mutex(handle())};
  std::lock_guard held{conn};
  if (sqlite3_step(st) != SQLITE_DONE) {
    if (sqlite3_extended_errcode(handle()) == SQLITE_CONSTRAINT_PRIMARYKEY) {
      ldpp_dout_fmt(dpp, 4, "NooBaa id {} is already recorded", noobaa_id);
      return -EEXIST;
    }
    ldpp_dout_fmt(dpp, 0, "ERROR: recording NooBaa id {}: {}", noobaa_id,
			  sqlite3_errmsg(handle()));
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
  ConnectionMutex conn{sqlite3_db_mutex(handle())};
  std::lock_guard held{conn};
  ConnectionMutex conn{sqlite3_db_mutex(handle())};
  std::lock_guard held{conn};
  ConnectionMutex conn{sqlite3_db_mutex(handle())};
  std::lock_guard held{conn};
int IdentityDB::remove_identity(const DoutPrefixProvider* dpp,
				const std::string& key)
{
  Identity old;
  const bool had = (get_identity(dpp, key, old) == 0);

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
  if (had && !old.distinguished_name.empty()) {
    names.forget(old.distinguished_name);
  }
  if (had && old.uid) {
    names.forget_uid(*old.uid);
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

int IdentityDB::get_identities_by_name(const DoutPrefixProvider* dpp,
				       const std::string& name,
				       std::vector<Identity>& out) const
{
  const std::string sql =
    std::string("SELECT ") + IDENTITY_COLUMNS + " FROM '" + identity_table
    + "' WHERE DistinguishedName = ? ORDER BY Identity;";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(handle(), sql.c_str(), -1, &st, nullptr)
      != SQLITE_OK) {
    return -EIO;
  }
  auto done = make_scope_guard([st] { sqlite3_finalize(st); });
  sqlite3_bind_text(st, 1, name.c_str(), -1, SQLITE_TRANSIENT);

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

NsswitchCache::Table IdentityDB::sweep_table(
  const DoutPrefixProvider* dpp) const
{
  std::vector<Identity> rows;
  NsswitchCache::Table out;
  if (list_identities(dpp, rows) < 0) {
    return out;
  }
  const bool dynamic = names.dynamic_groups();
  for (const auto& id : rows) {
    if (id.directory_backed()) {
      out.names.push_back(id.distinguished_name);
    } else if (dynamic && id.local() && !id.groups) {
      out.uids.push_back(*id.uid);
    }
  }
  return out;
}

void IdentityDB::start_name_sweep()
{
  names.start([this] (const DoutPrefixProvider* dpp) {
    return sweep_table(dpp);
  });
}

namespace {

bool overridden(const std::atomic<int>& o, CephContext* cct,
		const char* option)
{
  const int v = o.load(std::memory_order_relaxed);
  if (v >= 0) {
    return v != 0;
  }
  return cct->_conf.get_val<bool>(option);
}

void force(std::atomic<int>& o, std::optional<bool> on)
{
  o.store(on ? (*on ? 1 : 0) : -1, std::memory_order_relaxed);
  note_all_identities_changed();
}

} // anonymous namespace

bool IdentityDB::user_identity_override() const
{
  return overridden(user_override, cct, "rgw_nsfs_user_identity_override");
}

bool IdentityDB::role_identity_override() const
{
  return overridden(role_override, cct, "rgw_nsfs_role_identity_override");
}

void IdentityDB::force_user_identity_override(std::optional<bool> on)
{
  force(user_override, on);
}

void IdentityDB::force_role_identity_override(std::optional<bool> on)
{
  force(role_override, on);
}

namespace {

/* The supplementary groups a row installs (I8):  its own list if it has
 * one, an empty one included;  else, under
 * rgw_nsfs_dynamic_supplemental_groups, the user's groups from nsswitch
 * -- by name for an nsswitch-resolved row, by uid for a local one -- as
 * NooBaa's request path takes them (`object_sdk.js:313`);  else none.
 * -ENOENT for a local uid nsswitch does not know, or the lookup's
 * -errno. */
int row_groups(const DoutPrefixProvider* dpp, const IdentityDB& db,
	       const Identity& id, uid_t uid, std::vector<gid_t>& out,
	       optional_yield y)
{
  out.clear();
  if (id.groups) {
    out.assign(id.groups->begin(), id.groups->end());
    return 0;
  }
  if (! db.nsswitch_cache().dynamic_groups()) {
    return 0;
  }
  std::vector<uint32_t> g;
  const int ret = id.directory_backed()
    ? db.nsswitch_cache().groups_by_name(dpp, id.distinguished_name, g, y)
    : db.nsswitch_cache().groups_by_uid(dpp, uid, g, y);
  if (ret < 0) {
    return ret;
  }
  out.assign(g.begin(), g.end());
  return 0;
}

} // namespace

int complete_file_credentials(const DoutPrefixProvider* dpp,
			      const IdentityDB& db, Credentials& inout,
			      std::string* key)
{
  std::vector<Identity> rows;
  int ret = db.get_identities_by_uid(dpp, inout.uid, rows);
  if (ret < 0) {
    return ret;
  }
  /* An nsswitch-resolved row stores no uid;  the cache knows which names last
   * resolved to this one. */
  for (const auto& name : db.nsswitch_cache().names_for_uid(inout.uid)) {
    ret = db.get_identities_by_name(dpp, name, rows);
    if (ret < 0) {
      return ret;
    }
  }

  /* A row carrying only placement data asks for no impersonation.  Drop
   * those rather than read an empty group list off them. */
  std::erase_if(rows, [](const Identity& id) {
    return !id.local() && !id.directory_backed();
  });
  if (rows.empty()) {
    /* I8, decision 4:  under the switch a uid no row carries completes
     * with its Unix groups.  The operation runs as the file's owner, and
     * those give it no more than it already has. */
    if (! db.nsswitch_cache().dynamic_groups()) {
      return -ENOENT;
    }
    std::vector<uint32_t> g;
    ret = db.nsswitch_cache().groups_by_uid(dpp, inout.uid, g, null_yield);
    if (ret == -ENOENT) {
      return -ENOENT;
    }
    if (ret < 0) {
      ldpp_dout_fmt(dpp, 0, "ERROR: the groups of uid {}, which owns the file "
			    "and no identity carries, could not be looked up; "
			    " acting on its own rights", inout.uid);
      return -ENODATA;
    }
    inout.groups.assign(g.begin(), g.end());
    return 0;
  }

  /* Rows disagree when what each would install differs (I8):  an
   * explicit list against nsswitch's is a disagreement as two lists are. */
  std::vector<std::vector<gid_t>> installs(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    ret = row_groups(dpp, db, rows[i], inout.uid, installs[i], null_yield);
    if (ret < 0) {
      ldpp_dout_fmt(dpp, 0, "ERROR: the groups of identity {}, which carries "
			    "uid {}, could not be looked up ({});  acting on "
			    "the file owner's own rights", rows[i].key,
			    inout.uid, cpp_strerror(-ret));
      return -ENODATA;
    }
  }
  const auto& first = rows.front();
  for (size_t i = 1; i < rows.size(); ++i) {
    if (installs[i] != installs[0]) {
      ldpp_dout_fmt(dpp, 0, "ERROR: uid {} is carried by {} identities whose "
			    "supplemental group lists differ ({}, {});  a "
			    "file names its owner by uid alone, so it cannot "
			    "say which applies.  Acting on the file owner's "
			    "own rights instead", inout.uid, rows.size(),
			    first.key, rows[i].key);
      return -ENODATA;
    }
  }

  inout.groups = std::move(installs[0]);
  /* Only one row names an identity.  Where several agree, their
   * credentials coincide and their keys do not, so there is nothing
   * to name. */
  if (key && (rows.size() == 1)) {
    *key = first.key;
  }
  return 0;
}

namespace {

int resolve_key(const DoutPrefixProvider* dpp, const IdentityDB& db,
		const std::string& key, Credentials& out, optional_yield y)
{

  Identity id;
  int ret = db.get_identity(dpp, key, id);
  if (ret < 0) {
    return ret;			/* -ENOENT among them */
  }

  if (id.directory_backed()) {
    ResolvedName rn;
    ret = db.nsswitch_cache().resolve(dpp, id.distinguished_name, rn, y);
    if (ret < 0) {
      ldpp_dout_fmt(dpp, 4, "identity {} names {}, which nsswitch did not "
			    "resolve ({});  denying it rather than serving it "
			    "unimpersonated", key, id.distinguished_name,
			    cpp_strerror(-ret));
      return -EPERM;
    }
    out.uid = rn.uid;
    out.gid = rn.gid;
  } else if (! id.local()) {
    /* a row carrying only placement or provenance data asks for no
     * impersonation, which is not the same as asking for one we
     * cannot give */
    return -ENOENT;
  } else {
    out.uid = *id.uid;
    out.gid = *id.gid;
  }

  /* An explicitly empty list gives an empty vector, as does an absent
   * one with the switch off, and the caller installs it rather than
   * skipping the call -- NooBaa's ThreadScope issues setgroups(0, NULL)
   * for exactly this case.  Under the switch an absent list is
   * nsswitch's (I8), and a lookup that cannot give it refuses, as
   * NooBaa's request does. */
  ret = row_groups(dpp, db, id, out.uid, out.groups, y);
  if (ret < 0) {
    ldpp_dout_fmt(dpp, 4, "identity {} has no group list, and nsswitch did "
			  "not give uid {}'s groups ({});  denying it rather "
			  "than serving it with fewer groups than the site "
			  "gives it", key, out.uid, cpp_strerror(-ret));
    return -EPERM;
  }
  return 0;
}

} // anonymous namespace

int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const rgw_owner& owner, Credentials& out,
			optional_yield y)
{
  return resolve_key(dpp, db, to_string(owner), out, y);
}

int resolve_role_credentials(const DoutPrefixProvider* dpp,
			     const IdentityDB& db, const std::string& role_id,
			     const rgw_account_id& account, Credentials& out,
			     optional_yield y, std::string* row)
{
  std::string key;
  int ret = -ENOENT;
  if (db.role_identity_override()) {
    key = role_identity_key(role_id);
    ret = resolve_key(dpp, db, key, out, y);
  }
  if ((ret == -ENOENT) && !account.empty()) {
    key = to_string(rgw_owner{account});
    ret = resolve_key(dpp, db, key, out, y);
  }
  if (row && ((ret == 0) || (ret == -EPERM))) {
    *row = key;
  }
  return ret;
}

int resolve_credentials(const DoutPrefixProvider* dpp, const IdentityDB& db,
			const rgw_user& user, const rgw_account_id& account,
			Credentials& out, optional_yield y, std::string* row)
{
  /* I9:  an account user's own row is consulted only under
   * rgw_nsfs_user_identity_override;  without it the user runs as its
   * account, whatever rows exist */
  if (!account.empty() && !db.user_identity_override()) {
    rgw_owner owner{account};
    int ret = resolve_credentials(dpp, db, owner, out, y);
    if (row && ((ret == 0) || (ret == -EPERM))) {
      *row = to_string(owner);
    }
    return ret;
  }
  rgw_owner owner{user};
  int ret = resolve_credentials(dpp, db, owner, out, y);
  if ((ret == -ENOENT) && !account.empty()) {
    owner = account;
    ret = resolve_credentials(dpp, db, owner, out, y);
  }
  if (row && ((ret == 0) || (ret == -EPERM))) {
    *row = to_string(owner);
  }
  return ret;
}
  ConnectionMutex conn{sqlite3_db_mutex(handle())};
  std::lock_guard held{conn};
}}} // namespace rgw::sal::nsfs
