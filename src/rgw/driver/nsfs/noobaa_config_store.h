// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Reading NooBaa's configuration store.
 *
 * I1 of the account import (`ACCOUNT_IMPORT.md` §9).  **Read only.**
 * Nothing here writes, creates or removes anything in their store, and
 * nothing here writes RGW state either -- the stages that do are I4
 * and I5.
 *
 * THE STORE IS AN IMPORT SOURCE, NOT A BACKING STORE.  It is read once,
 * at import, and what the import produces is RGW's own state.  After a
 * bucket is adopted nothing in the request path may consult their store
 * again:  the adopted bucket carries the RGW bucket-state strategy, and
 * that is what makes this an adoption rather than a permanent bridge.
 * The one place their store *is* read per request is the `serve` mode
 * of `NSFS_ADOPTION.md` §7 -- a deliberately read-only view of a tree
 * nothing has changed yet -- and that reader is
 * `NooBaaBucketStateStrategy`, not this.
 *
 * WHAT IT READS.  Four of the eight entries a NooBaa configuration
 * directory holds (`ACCOUNT_IMPORT.md` §1):
 *
 *   identities/<id>/identity.json   every account, IAM user and role;
 *                                   one schema serves all three
 *   buckets/<name>.json             one record per bucket
 *   master_keys.json                presence only here;  decrypting a
 *                                   secret with it is I2
 *   config.json                     the settings that change how an
 *                                   account behaves (§2.5)
 *
 * `accounts_by_name/` and `access_keys/` are symlink indexes into
 * `identities/` and so are derivable;  `system.json` and
 * `certificates/` describe a NooBaa deployment's own state.
 *
 * IT REPORTS WHAT IT CANNOT READ RATHER THAN STOPPING.  A record that
 * is present and will not parse is collected into `unreadable` and the
 * traversal continues, so one call says everything that is wrong with a
 * store.  An import that discovered its third bad record after writing
 * two accounts would leave a state nobody asked for -- see §6.1a, and
 * `read()` returning 0 with a non-empty `unreadable` is what a
 * preflight acts on.
 *
 * Pinned at noobaa-core `08d063f86`.  Field names and the record
 * layout below were read from that commit's schemas and verified
 * against a store their own CLI and gateway wrote.
 */

#pragma once

#include <string>
#include <vector>

#include "common/dout.h"

namespace rgw { namespace sal { namespace nsfs {

/* One of an identity's access keys.
 *
 * The secret stays as their record holds it -- encrypted.  Decrypting
 * it needs the master key, which is I2;  an access key's *id* is
 * cleartext, which is why I3's collision gate depends on I1 alone. */
struct NooBaaAccessKey {
  std::string access_key;
  std::string encrypted_secret_key;
  std::string creation_date;
  bool deactivated{false};
};

/* The POSIX half of an identity.
 *
 * Their schema makes this a `oneOf`:  either uid and gid, or a
 * `distinguished_name`, never both.  The two behave differently and
 * §2.3 is about the difference, so the reader keeps which one it was
 * rather than flattening them into an optional uid. */
struct NooBaaPosixConfig {
  enum class Kind { Unset, Ids, DistinguishedName };
  Kind kind{Kind::Unset};

  int64_t uid{-1};
  int64_t gid{-1};
  std::string distinguished_name;

  std::string new_buckets_path;
  std::string fs_backend;
  std::string custom_bucket_path_allowed_list;
  bool allow_bypass_governance{false};

  /* An explicit list overrides the directory entirely (§2.4 branch 1),
   * and an *empty* list is not the same as no list:  empty means "no
   * supplementary groups", which is a grant decision.  So presence is
   * carried separately from contents. */
  bool have_supplemental_groups{false};
  std::vector<int64_t> supplemental_groups;
};

/* One identity record, whole.
 *
 * Accounts, IAM users and roles share one schema
 * (`nsfs_schema_utils.js`), so this is all three.  An IAM user is the
 * one with `owner` set -- §2.8 -- and a role is the one with
 * `role_config`. */
struct NooBaaIdentity {
  /* their identifier, read verbatim and never rewritten (§3) */
  std::string id;
  std::string name;
  std::string email;
  std::string creation_date;

  /* set on an IAM user:  the account that owns it */
  std::string owner;
  std::string creator;
  std::string iam_path;

  std::string master_key_id;
  bool allow_bucket_creation{false};
  bool iam_operate_on_root_account{false};

  /* X1 carries this and enforces nothing (§6.1a).  Absent is not false:
   * `rgw_non_md5_etag` is the deployment-wide control and a record that
   * does not mention the flag must not be read as disabling it. */
  bool have_force_md5_etag{false};
  bool force_md5_etag{false};

  std::vector<NooBaaAccessKey> access_keys;
  NooBaaPosixConfig posix;

  /* A role's trust policy, kept as the JSON text their record holds.
   * Translating it into an RGW role is I4 (§6.1);  I1 only has to not
   * lose it. */
  std::string role_config;

  /* The distinguished case of §6.8.  Their CLI writes it under the
   * reserved name with an empty key list, which is how it is told from
   * an ordinary account that happens to have no keys yet -- it cannot
   * be told by uid, since nothing stops a real account sharing one. */
  bool is_anonymous() const { return name == "anonymous"; }

  /* An IAM user rather than a root account (§2.8). */
  bool is_iam_user() const { return !owner.empty(); }
};

/* What `config.json` says that changes how an account behaves.
 *
 * Not service tunables.  §2.5 point 4:  the switch between an explicit
 * group list and dynamic resolution travels with the deployment,
 * because its value changes what a request may do.  Defaulting it on
 * where a deployment had it off grants permissions that deployment
 * never had;  defaulting it off where it was on strips permissions the
 * site depends on. */
struct NooBaaSettings {
  bool have_config_json{false};

  /* `NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS`, false by default in
   * their `config.js` at the pin.  With it off, a directory-backed
   * identity carrying no explicit list gets *no* supplementary groups
   * -- their `change_user()` issues `setgroups(0, NULL)` -- so off is
   * a live permission fact and not an absence. */
  bool dynamic_supplemental_groups{false};
  bool have_dynamic_supplemental_groups{false};
};

/* A record that is there and could not be read. */
struct NooBaaUnreadable {
  std::string path;
  std::string why;
};

/* Everything one read of the store found. */
struct NooBaaStoreContents {
  NooBaaSettings settings;
  std::vector<NooBaaIdentity> identities;

  /* Bucket record *names*.  Their contents are translated by
   * `NooBaaBucketStateStrategy`, which already reads one record per
   * bucket;  I1 establishes which buckets exist and that each record
   * parses, and I5 is what carries the fields across. */
  std::vector<std::string> buckets;

  bool master_keys_present{false};

  /* Non-empty means the store has records an import cannot act on.
   * Collected rather than thrown, so one read reports all of them. */
  std::vector<NooBaaUnreadable> unreadable;

  size_t accounts() const;
  size_t iam_users() const;
};

/* A NooBaa configuration directory, read.
 *
 * Constructed with the directory itself.  Resolving where that is
 * belongs to whatever drives the import:  the store is an input to a
 * one-time operation, not an address the request path keeps. */
class NooBaaConfigStore {
  std::string root;

public:
  explicit NooBaaConfigStore(std::string dir) : root(std::move(dir)) {}

  const std::string& directory() const { return root; }

  /* Read the whole store.
   *
   * Returns 0 when the store was traversable, whether or not every
   * record parsed -- check `unreadable`.  Returns a negative errno
   * only when the store itself could not be read:  -ENOENT for a
   * directory that is not there, -ENOTDIR when it is not a directory.
   *
   * Serial, never concurrent, following the only prior art for walking
   * one of these trees -- their own `config_dir_restructure.js`
   * (§6.4). */
  int read(const DoutPrefixProvider* dpp, NooBaaStoreContents& out) const;
};

/* One record file, whole, into a string.
 *
 * Shared with `bucket_state_strategy.cc`, which reads a bucket record
 * on the request path.  Capped:  a record is small, and a cap keeps a
 * corrupt or hostile file out of memory.  Returns -EFBIG past the cap,
 * and no iostreams anywhere near it. */
int read_record_file(const DoutPrefixProvider* dpp, const std::string& path,
		     std::string& out);

}}} // namespace rgw::sal::nsfs
