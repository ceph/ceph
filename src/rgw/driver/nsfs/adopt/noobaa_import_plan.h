// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * What a NooBaa store becomes on the RGW side, and what stops it.
 *
 * I3 of the account import (`ACCOUNT_IMPORT.md` §9).  Two things:  the
 * mapping, and the gate.  **Nothing is written.**  I4 writes.
 *
 * IT NEEDS NO SECRETS.  An access key's *id* is cleartext in NooBaa's
 * record, and the four constraints below are all about ids and names,
 * so this depends on I1 alone and not on I2.  A deployment whose
 * master key is missing can still be told what its import would hit.
 *
 * THE MAPPING.  Each NooBaa root account becomes an RGW **account
 * plus an account user**:  `RGWAccountInfo` holds no credentials and
 * every NooBaa root account is a credential holder (§3.4).  The
 * account id is minted with RGW's own generator rather than derived
 * from NooBaa's -- NooBaa's `_id` is a 24-character hex ObjectId and
 * RGW's is `RGW` followed by 17 digits, so no transformation of one is
 * the other (§3.3).  NooBaa's id is kept because re-import needs it:  NooBaa
 * supports renaming, and a rename rewrites `name` and `email` while
 * leaving `_id` alone, so the name is not a stable key and the id is.
 *
 * THE GATE.  RGW enforces four uniqueness constraints a NooBaa store
 * does not (§6.3), and the one most likely to stop a real import is
 * **email**:  NooBaa's `email` is a copy of the account name, not an
 * email -- `manage_nsfs.js` sets `email: String(user_input.name)` with
 * the comment "temp, keep the email internally" -- and RGW's email
 * index is shared **across users and accounts**.  So two IAM users
 * named `alice` under different root accounts collide, deterministically,
 * partway through an import that did not check first.
 *
 * Discarding the value is not available:  it is a matchable principal
 * in NooBaa's trust policies, so dropping it changes authorization.
 *
 * **Every collision is reported and the plan is refused.**  Not the
 * first one -- an operator needs the list, because fixing them one
 * import at a time means one failed import per collision.  And
 * refused rather than reported, because a partial import leaves a
 * state nobody asked for (§6.1a).
 *
 * WHAT THIS DOES NOT CHECK:  collisions against **RGW's existing
 * state**.  A NooBaa account whose name matches an email already in
 * RGW's index is just as fatal and is not visible here -- it needs
 * lookups into the account and user indexes, which is where I4
 * writes.  Building that interface now would be a reader for nobody.
 * The store-internal half is complete, and it is the half §6.3 names
 * as the likeliest first failure.
 */

#pragma once

#include <optional>
#include <string>
#include <vector>
#include "../identity_db.h"

#include "common/dout.h"

#include "noobaa_config_store.h"

namespace rgw { namespace sal { namespace nsfs {

/* One identity, and what RGW will hold for it. */
struct ImportEntry {
  /* NooBaa's, kept:  the stable key across a rename (§3.4) */
  std::string noobaa_id;
  std::string noobaa_name;

  /* The account this identity belongs to.  Minted for a root
   * account;  for an IAM user it is the owner's, so a user and its
   * account agree without a second pass. */
  std::string account_id;

  /* Empty, for every identity.  NooBaa has no tenants, and importing
   * each account as a tenant named by its id was considered and set
   * aside (§3.5).  One consequence matters to the gate:  RGW's
   * "account name unique per tenant" becomes globally unique when
   * every tenant is the same. */
  std::string tenant;

  std::string name;
  /* NooBaa's `email`, carried.  See the header:  it is a copy of the
   * name and it is a matchable principal, so it can be neither
   * trusted as an address nor discarded. */
  std::string email;
  /* What principal matching consults for an account user
   * (`rgw_auth.cc`, `is_identity`), so it is the field that must
   * carry the NooBaa name (§6.6). */
  std::string display_name;

  std::vector<std::string> access_key_ids;

  /* For a root account, the limits RGW's IAM API enforces on it,
   * raised where the store carries more than RGW's defaults allow.
   * RGW's admin path, which the import writes through, does not check
   * them;  CreateUser and CreateAccessKey do, so an account imported
   * above its limit would refuse every later one.  Unset keeps the
   * default. */
  std::optional<int32_t> max_users;
  std::optional<int32_t> max_access_keys;

  bool iam_user{false};
  bool anonymous{false};
};

/* The plan, and what refuses it. */
struct ImportPlan {
  std::vector<ImportEntry> entries;

  /* Every collision, each naming the identities involved and the
   * constraint.  Non-empty refuses the plan. */
  std::vector<std::string> collisions;

  /* Every IAM user whose POSIX block differs from its account's,
   * naming the fields.  A warning, not a collision:  the user is
   * imported and runs as its account, which changes what it can reach
   * on disk (ACCOUNT_IMPORT.md §2.8).  NooBaa copies the account's
   * block into a user at creation and never updates the copy, so a
   * difference means the account was changed afterwards. */
  std::vector<std::string> drift;

  /* Every account limit raised to fit what the store carries, naming
   * the account, the limit and both values.  What the import will do
   * when it creates the account;  `ImportResult::limits_raised` counts
   * what it did. */
  std::vector<std::string> raised;

  /* Every root account NooBaa did not allow to bypass governance
   * retention.  As RGW's account root it can, since a root is allowed
   * every action on what its account owns unless a policy denies it.
   * RGW's root semantics were accepted over NooBaa's restriction
   * (DECISIONS.md 2026-10-10), so this is reported, not refused. */
  std::vector<std::string> widened;

  size_t accounts() const;
  size_t iam_users() const;
};

/* Map a store, and gate it.
 *
 * Returns 0 only when the plan is clean.  -EEXIST when a constraint
 * is violated, with every violation in `collisions`;  -EINVAL when a
 * record cannot be mapped at all.  The plan is returned either way,
 * because an operator reading a refusal wants to see what would have
 * been imported.
 *
 * `cct` is for RGW's account id generator and nothing else. */
/* `stored`, when given, is consulted before an account id is minted:
 * an identity already imported keeps the id it was imported with,
 * found by its NooBaa `_id` in the provenance table.
 *
 * Without it every call mints, and two calls over one store produce
 * two different ids for the same NooBaa account -- which is what
 * `adopt_one_bucket()` did.  It builds its own plan to resolve a
 * principal against the complete mapping, and wrote the bucket an
 * owner naming an account that the imported *user* was not in, so
 * every request to an adopted bucket was an implicit deny.  Found by
 * the first full stand-up cycle, 2026-10-10.
 *
 * `nullptr` is for a caller with no store to consult -- the plan tests
 * build one from store contents alone. */
int build_import_plan(const DoutPrefixProvider* dpp, CephContext* cct,
		      const NooBaaStoreContents& contents,
		      ImportPlan& plan,
		      const IdentityDB* stored = nullptr);

}}} // namespace rgw::sal::nsfs
