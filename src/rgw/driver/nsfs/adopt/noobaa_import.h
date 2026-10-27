// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Writing a NooBaa store's identities into RGW.
 *
 * I4 of the account import (`ACCOUNT_IMPORT.md` §6.1 phase 1, §9).
 * **The first stage that writes.**  It reads the NooBaa store and
 * writes RGW state;  it does not modify NooBaa's store, move object
 * data, or delete anything.  The data tree is adopted in place and is
 * untouched.
 *
 * WHAT EACH IDENTITY BECOMES.  A NooBaa root account becomes an RGW
 * **account plus an account user**:  `RGWAccountInfo` holds no
 * credentials and every NooBaa root account is a credential holder,
 * so the keys go on the user (§3.4).  The account also gets an nsfs
 * identity row, which is where the POSIX fields and the placement
 * fields live -- neither of which RGW has anywhere to put.  An IAM user
 * gets no row:  on Scale every member of an account runs as the
 * account (§2.8).  Every identity's `_id` is recorded in the NooBaa id
 * table, against the owner it became.
 *
 * TWO PASSES, IN ONE OPERATION.  Identities first, then the policies.
 * A trust policy's principals are rewritten through the identity
 * mapping (§4.4), and the mapping is not complete until every
 * identity is written -- so a role written during the first pass
 * would carry principals naming identities that did not exist yet.
 *
 * IT IS SAFE TO RE-RUN, NOT ATOMIC.  There is no transaction spanning
 * account creation and there cannot be one, so a failure part-way
 * leaves RGW state behind and the operation has to tolerate meeting
 * its own output (§6.2).  Re-running is keyed on **NooBaa's `_id`**,
 * recorded in the NooBaa id table:  the account name is not stable,
 * because a rename rewrites `name` and `email` and leaves `_id`
 * alone, so a name-keyed re-run would create a duplicate for every
 * renamed account and would re-mint an id for an identity already
 * imported.
 *
 * Two things it deliberately does not do.  It does not notice an
 * identity deleted from NooBaa's store since the last run -- removal is
 * an administrative act, not an import decision.  And it does not
 * reconcile divergence where both sides changed:  NooBaa's store is the
 * source and RGW the destination, in that direction only.
 *
 * THE ANONYMOUS ACCOUNT IS THE ONE EXCEPTION, AND IT IS NOT OPTIONAL.
 * NC keeps a real account named `anonymous` carrying a uid and gid for
 * unauthenticated requests, with no keys.  RGW's anonymous identity is
 * built in and is not an account, so importing it as one is wrong and
 * skipping it loses the uid mapping nsfs needs.  It gets **an identity
 * row and nothing else** -- no account, no user, nothing minted --
 * because `RGW_USER_ANON_ID` already supplies the owner string the nsfs
 * table is keyed on (§6.8).  Skip an ordinary account and one
 * tenant's credentials stop working;  skip this one and every
 * public-bucket read either breaks or runs with the wrong privilege.
 */

#pragma once

#include <string>
#include <vector>

#include "common/dout.h"

#include "noobaa_import_plan.h"

namespace rgw { namespace sal {

class NSFSDriver;

namespace nsfs {

/* What a run did. */
struct ImportResult {
  size_t accounts_created{0};
  /* already present, recognised by NooBaa's `_id` */
  size_t accounts_skipped{0};
  size_t users_created{0};
  /* already present:  an IAM user, or the root user of an account
   * already present, so a re-run reports the users the first one
   * created */
  size_t users_skipped{0};
  size_t identity_rows{0};
  /* I9:  IAM users' own rows, each its tuple as its NooBaa record holds
   * it;  counted apart from `identity_rows`, which are accounts' */
  size_t user_rows{0};
  size_t roles_created{0};
  /* `rgw_nsfs_adoption_role_rights` as this run read it, and the roles
   * it wrote `NooBaaRoleMigration` on */
  std::string role_rights;
  size_t role_policies_written{0};
  size_t grants_written{0};
  /* accounts created with a limit above RGW's default */
  size_t limits_raised{0};
  bool anonymous_imported{false};
  /* the store has an anonymous account, and an earlier run imported it */
  bool anonymous_present{false};

  /* Nsswitch-resolved identities whose name nsswitch did not resolve, as
   * "<owner>: <name> (<why>)".  Written all the same, as NooBaa's record
   * stood:  each is refused until its name resolves, as it was under
   * NooBaa, and this is where the operator learns of it. */
  std::vector<std::string> unresolved_names;

  /* I8:  under rgw_nsfs_dynamic_supplemental_groups, list-less local
   * identities whose uid nsswitch does not know, as "<owner>: uid <n>".
   * Written, and refused until the uid resolves, as under NooBaa. */
  std::vector<std::string> unresolved_uids;

  /* Every identity that could not be written, named.  Non-empty means
   * the import did not finish, and re-running it is the recovery. */
  std::vector<std::string> failures;
};

/* The half of the preflight that needs RGW rather than the store.
 *
 * I3 checks the collisions visible in the store alone;  this checks
 * the same four constraints against **state RGW already holds** -- a
 * NooBaa account whose name matches an existing account, an email
 * already in RGW's index, which is shared across users and accounts,
 * or an access key another RGW user already holds.  Just as fatal,
 * and invisible without a lookup.
 *
 * An identity already imported is **not** a collision:  it is
 * recognised by NooBaa's `_id` and skipped, which is what makes a re-run
 * safe.  So this consults the NooBaa id table first and only then
 * asks whether a name or email is taken by something else.
 *
 * Returns -EEXIST with every collision in `collisions`, 0 when clear.
 */
int check_rgw_state(const DoutPrefixProvider* dpp, optional_yield y,
		    NSFSDriver* driver, const ImportPlan& plan,
		    std::vector<std::string>& collisions);

/* Write the plan.
 *
 * `contents` is needed as well as `plan` because the plan carries
 * what RGW will hold and the records carry what only the nsfs row
 * holds -- the POSIX identity, the placement fields, the trust policy.
 *
 * Returns 0 when everything was written, -EIO when anything was not,
 * with each failure named in `result.failures`.  Partial success is
 * reported rather than rolled back:  there is nothing to roll back
 * to, and re-running is the recovery. */
int import_identities(const DoutPrefixProvider* dpp, optional_yield y,
		      NSFSDriver* driver,
		      const NooBaaStoreContents& contents,
		      const ImportPlan& plan,
		      ImportResult& result);

}}} // namespace rgw::sal::nsfs
