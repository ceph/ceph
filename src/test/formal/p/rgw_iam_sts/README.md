# RGW IAM and STS: a P model

A model of RGW's IAM front end for one account and of the STS operations
that issue and accept temporary credentials. It covers these entities:
roles, users and their access keys, groups and their members, and inline
and managed policies. It also covers AssumeRole, GetSessionToken, and S3
requests signed with an access key or with session credentials. The
requests race each other, one RADOS op at a time, and an RGW can die
between any two ops.

The model follows the code on main as of `504b1a2b334`, which includes the
cross-account AssumeRole of PR 66909. Line numbers below refer to that
commit. The contract is AWS's Smithy models of IAM and STS
([`aws/api-models-aws`](https://github.com/aws/api-models-aws) at
`69ae840c`, `models/iam/service/2010-05-08/iam-2010-05-08.json` and
`models/sts/service/2011-06-15/sts-2011-06-15.json`): the errors each
operation lists, and what its documentation says it does.

## The properties

The specs treat each request as one atomic step. That step falls between
the moment its client sent the request and the moment of the answer.

- DeleteNeedsEmpty. DeleteRole, DeleteUser and DeleteGroup never remove an
  entity that still has what IAM requires the caller to remove first. For a
  role, that is its inline and attached policies. For a user, it is the
  access keys and policies. For a group, it is the policies and members.
  IAM answers DeleteConflict instead.
- DeleteUserNeedsNoGroups. DeleteUser never removes a user that is still in
  a group. This is the same rule for group memberships, kept apart because
  RGW leaves it out by design.
- DeletesTakeEffect. A delete answered success removed the entity it acted
  on.
- IndexesMatch. Once every request is done, the indexes match the info
  objects:
  - Each entity is reachable by its name, and each name object and
    account entry names a live entity of that name.
  - Each account index counts its entries.
  - Each active access key, and only those, is in the key index.
  - A group's member entries are its users' memberships, and a user is
    only in groups that exist.
- LimitsHold. The account never has more roles, users or groups than its
  limits allow, and a user never has more access keys than its limit.
- NamesUnique. No two roles, users or groups share a name.
- NoLostUpdate. An addition answered success is still there at the end,
  unless a request removed it or the entity is gone. An addition is an
  inline policy, an attached policy, an access key or a group membership.
- KeysHonored. An S3 request is authenticated only with an access key that
  its user holds active. UpdateAccessKey's documentation says an inactive
  key "cannot be used for API calls".
- TrustHonored. AssumeRole issues credentials only for a role whose trust
  policy allowed the caller at some point while the request ran. The rule
  is AWS's: the trust policy names the caller, or it names the account
  and an identity policy of the caller allows `sts:AssumeRole`.
- SessionsDieWithRole. Session credentials of a role work only while the
  role exists. This also covers credentials obtained with them.
- IamAnswers. Each request is answered as its operation's contract allows:
  - Each error is one that the operation's Smithy model lists. Any request
    can also get AccessDenied, or InvalidAccessKeyId for unknown
    credentials. The model's AssumeRole and GetSessionToken can reach no
    listed error, so a missing or untrusting role must be AccessDenied.
  - For EntityAlreadyExists, an entity had the name while the request ran,
    or a create, rename or delete of the name was in flight. For
    NoSuchEntity about the name that a request addresses, no entity had the
    name at some point. For a create that succeeds, the name was free at
    some point.
  - GetSessionToken is answered success only for long-term credentials.

## What is modelled

- The Store holds the RADOS state, and each of its handlers is one atomic
  op. The semantics of each op come from `../common/Rados.p`:
  - Metadata objects, written with `rgw_put_system_obj` and removed with
    `rgw_delete_system_obj`. An exclusive create fails with EEXIST. A
    write or removal with an `RGWObjVersionTracker` read version carries
    `cls_version_check` and fails with ECANCELED on another version or on
    no object. A tracker with no read version adds no check.
  - These objects are each entity's info object, the role and group name
    objects, and the access key index (`user_keys_pool`).
  - The `cls_user` account resource omaps (`cls_account_resource_add` and
    `_rm`): the account's users (by display name), roles and groups, and
    each group's members (`users.<group id>`). The header counts the
    entries. For a name that is already present, an exclusive add fails.
    A non-exclusive add replaces the entry and does not count it.
- An Rgw machine serves one request, as the handlers in
  `rgw_rest_role.cc`, `rgw_rest_iam_user.cc`, `rgw_rest_user_policy.cc`,
  `rgw_rest_iam_group.cc`, `rgw_rest_sts.cc` and `rgw_sts.cc` do:
  - Role writes go through `rgwrados::role::write` and `role::remove`.
  - User writes go through `RGWSI_User_RADOS`'s `PutOperation` (`prepare`,
    `put`, `complete`, `remove_old_indexes`) and `remove_user_info`.
  - Group writes go through `rgwrados::group::write` and `group::remove`.
  - Read-modify-writes retry with `retry_raced_*_write`.
- Authentication follows `LocalEngine::authenticate` for access keys and
  `STSEngine::authenticate` for session tokens. A token carries the role
  ID, the user and the account type, as `STS::SessionToken` does.
- The trust policy evaluation is reduced to the same-account cases of
  `evaluate_iam_policies`. A trust policy names a user, the account, or
  neither. A user can have an identity policy that allows `sts:AssumeRole`.
- The Driver of `../common/Common.p` runs a scenario's requests in phases.
  The requests of a phase run concurrently. Session credentials are handed
  from one phase to the next in slots.

## Configurations and results

The configuration `Main()` is the code on main. Each proposed fix is a
flag, and `Fixed()` sets them all:

| Flag | Fix |
|---|---|
| `roleDeleteById` | DeleteRole removes the role it loaded and checked, by ID, under the version it read. A lost race reloads and checks again. The name object and account entry are removed only while they still name that ID. |
| `roleWriteNeedsRole` | A role update that finds the role gone fails with NoSuchEntity, and writes no name object or account entry. |
| `limitAtomic` | `cls_account_resource_add` adds the account's user, role and group entries exclusively and with the account's limit. It does so before the info object write. If that write fails, the entry is rolled back. Today the code passes no limit and checks a count that it read first. |
| `userDeleteGuarded` | DeleteUser removes the user object first, under the version it read. A lost race reloads and checks again. Only then are the indexes removed. |
| `authChecksActive` | Authentication refuses a key that the user info marks inactive. |
| `policyOpsPassOld` | The user policy ops pass the old user info to `store_user`, as the other user ops do. |
| `groupLinkGuarded` | AddUserToGroup writes the member entry, then rewrites the group under the version it read, and only then writes the user. If the group or the user is gone, it takes the entry back. DeleteGroup checks every member entry, not only the first. |
| `renameMovesMembers` | A renamed user's member entries move to its new name. |
| `assumeRoleOneRead` | AssumeRole issues its credentials for the role whose trust policy it checked. |
| `trustRequired` | AssumeRole requires the trust policy to allow the caller, as AWS does. |
| `assumeDeniesMissing` | AssumeRole answers AccessDenied for a role that does not exist. |
| `gstLongTermOnly` | GetSessionToken refuses temporary credentials, as AWS does. |
| `userDeleteNeedsNoGroups` | DeleteUser answers DeleteConflict for a user in a group, as IAM does. `Fixed()` leaves it off, because RGW's behavior looks deliberate. |
| `retriesOutServiceFailure` | When `retry_raced_*_write` runs out of retries, an operation whose API does not list ConcurrentModification answers ServiceFailure. `maxRetries = -1` models the other option, retries with no limit. |

One more flag changes how the fixes are built, not what they do:
`existingOpsOnly` makes them use only the ops that RADOS and cls_user
offer today. Some fixes remove a name object or an account entry only
while it still names a given ID. With this flag, that removal is a read,
a check and a removal. A name object is then removed under the version
that was read. An account entry has no version, so another request can
change it between the check and the removal.

`expect.txt` lists all 123 cases. Run them with `../run.sh rgw_iam_sts`.
All of them match at 20,000 schedules each. `tcFix<Scenario>` runs every
scenario that has no crashes with every fix, against every property except
DeleteUserNeedsNoGroups. All of them hold. `tcExisting<Scenario>` runs
the same scenarios with `existingOpsOnly`, and all of them hold too. So
none of the fixes needs a new RADOS or cls_user op.

## The filed issues and their fixes

Each finding without a security impact is filed on tracker.ceph.com. The
table names the fix flag for each issue, and the test cases that show the
fix.

| Issue | Finding | Flag | Test cases |
|---|---|---|---|
| [81345](https://tracker.ceph.com/issues/81345) | 1 | `roleDeleteById` | `tcDelRoleVsPutById`, `tcDelRoleVsAttachById`, `tcDelRoleRecreateById` |
| [81346](https://tracker.ceph.com/issues/81346) | 2 | `roleWriteNeedsRole` | `tcDelRoleVsPutNeedsRole` |
| [81347](https://tracker.ceph.com/issues/81347) | 3 | `limitAtomic` | `tcRolesLimitAtomic`, `tcUsersLimitAtomic`, `tcGroupsLimitAtomic` |
| [81348](https://tracker.ceph.com/issues/81348) | 4 | `limitAtomic` | `tcSameUserAtomic`, `tcRenameSameAtomic`, `tcSameGroupAtomic` |
| [81349](https://tracker.ceph.com/issues/81349) | 7 | `groupLinkGuarded` | `tcDelGroupVsAddGuarded` |
| [81350](https://tracker.ceph.com/issues/81350) | 8 | `renameMovesMembers` | `tcRenameMemberMoves` |
| [81351](https://tracker.ceph.com/issues/81351) | 9 | `groupLinkGuarded` | `tcStaleMemberGuarded` |
| [81352](https://tracker.ceph.com/issues/81352) | 12 | `assumeDeniesMissing` | `tcAssumeNoRoleDenied` |
| [81353](https://tracker.ceph.com/issues/81353) | 14 | `userDeleteNeedsNoGroups` | `tcNoGroupsInGroup`, `tcDelUserVsAddGuarded`, `tcFixDelUserVsAdd`, `tcFixNoGroupsInGroup` |
| [81354](https://tracker.ceph.com/issues/81354) | 15 | `retriesOutServiceFailure`, or `maxRetries = -1` | `tcRoleFailsAfterRetries`, `tcRoleHotUnlimited`, `tcFixRoleHot` |

For 81353, the group check alone is not enough under a race
(`tcDelUserVsAddNoGroups`). An AddUserToGroup can land between the check
and the removal, as in finding 5. With `userDeleteGuarded` too, it holds.

For 81354, a request loses a race only when another request's write
succeeds. So n concurrent writers of one entity need n - 1 retries at
most. Three writers with one retry run out (`tcRoleHotOneRetry`), and
with two retries they do not (`tcRoleHotTwoRetries`). Ten retries run
out only with more than ten concurrent writers of one entity.

The model check of the fixes found one gap in `groupLinkGuarded` as first
written. AddUserToGroup wrote the member entry first. If a DeleteUser
then removed the user before the user write, the entry stayed
(`tcFixDelUserVsAdd`). The fix now takes the entry back in that case.

## What the model finds on main

The test cases named first reproduce each finding. The case with the fix
flag holds.

1. DeleteRole can delete a role that has policies
   (`tcDelRoleVsPutMain`, `tcDelRoleVsAttachMain`,
   `tcDelRoleRecreateMain`, fixed by `roleDeleteById`).
   - `RGWDeleteRole::execute` checks the role that `init_processing`
     loaded for policies (`rgw_rest_role.cc:380-388`). Then
     `role->delete_obj` resolves the name again and removes the role it
     finds, under the version it reads then (`driver/rados/role.cc:449-513`).
   - So a PutRolePolicy or AttachRolePolicy that lands between the two
     reads is deleted with the role.
   - The same gap lets DeleteRole remove a different role of the same name.
     For this, the name is deleted and created again in between, and the
     new role gets policies.
2. A role update that races DeleteRole is answered EntityAlreadyExists
   (`tcDelRoleVsPutAns`, fixed by `roleWriteNeedsRole`).
   - `role::write` reads the old info. When the info is gone, it treats the
     role as new and writes its name object and account entry (`role.cc:338-427`).
   - If DeleteRole has not yet removed one of them, that write fails with
     EEXIST. The request then answers it as EntityAlreadyExists.
   - PutRolePolicy, AttachRolePolicy and UpdateAssumeRolePolicy do not list
     EntityAlreadyExists. When the info write is the step that fails
     instead, the retry answers NoSuchEntity, which is correct.
3. The account's limits on roles, users and groups can be exceeded
   (`tcRolesLimitMain`, `tcUsersLimitMain`, `tcGroupsLimitMain`, fixed by
   `limitAtomic`).
   - Each create checks a count it read first (`rgw_rest_role.cc:218`,
     `rgw_rest_iam_user.cc:177`, `rgw_rest_iam_group.cc:177`).
   - The add to the account index passes no limit (`role.cc:271`,
     `svc_user_rados.cc:352`, `group.cc:273`), so concurrent creates all
     pass. `cls_account_resource_add` can enforce the limit atomically.
4. Two users or two groups can get the same name (`tcSameUserMain`,
   `tcRenameSameMain`, `tcSameGroupMain`, fixed by `limitAtomic`).
   - A new user's name is checked by a read in `PutOperation::prepare`
     (`svc_user_rados.cc:255-273`), and its account entry is written
     non-exclusively (`:352`). CreateUser and UpdateUser rename both take
     this path.
   - A group's name is also checked by a read. The failure of its exclusive
     name write is ignored, and its account entry is written
     non-exclusively (`group.cc:212-279`).
   - Both requests succeed, and the indexes name only one of the two
     entities. Roles do not have the problem, because their name object is
     created exclusively before the info.
5. DeleteUser can answer success while the user survives
   (`tcDelUserVsKeyMain`, `tcDelUserVsPolicyMain`, fixed by
   `userDeleteGuarded`).
   - `check_empty` looks at the user that `init_processing` loaded
     (`rgw_rest_iam_user.cc:562-608`).
   - `remove_user_info` removes the key index, the account entry and the
     group member entries before the user object
     (`svc_user_rados.cc:533-620`). Its version-checked removal treats
     ECANCELED as success (`:630`).
   - A CreateAccessKey or PutUserPolicy in between leaves the user object
     in place. No name reaches it, but its new key, which is in the key
     index, still authenticates.
6. A deactivated access key can authenticate (`tcDeactivateMain`,
   `tcDeactivateIndex`, fixed by `authChecksActive`, and the key index by
   `policyOpsPassOld`).
   - The user policy ops store the user with no old info
     (`rgw_rest_user_policy.cc:215`, `:401`). `PutOperation::complete` then
     writes the key index entry of every active key (`svc_user_rados.cc:316-326`).
   - The ops that write in `complete` are not ordered by the user object's
     version. An UpdateAccessKey to Inactive can remove the entry, and then
     a PutUserPolicy that read the key as active writes it back.
   - Authentication looks the key up in the user info but does not check
     that it is active (`rgw_rest_s3.cc:7320`).
7. DeleteGroup can delete a group that has a member
   (`tcDelGroupVsAddMain`, `tcDelGroupVsAddIndex`, fixed by
   `groupLinkGuarded`).
   - AddUserToGroup reads the group, writes only the user, and writes the
     member entry after that (`rgw_rest_iam_group.cc:902-916`).
   - DeleteGroup checks the member entries and removes the group under the
     group's version (`:600-645`), which AddUserToGroup never changes.
8. A renamed member's entry keeps its old name (`tcRenameMemberMain`, fixed by `renameMovesMembers`). This needs no race.
   - The member entries are keyed by display name, and an UpdateUser rename
     skips the groups the user stays in (`svc_user_rados.cc:365`, `:444`).
     RemoveUserFromGroup and DeleteUser then remove the new name, and the
     old entry stays.
9. DeleteGroup can delete a group that has members (`tcStaleMemberMain`, fixed by `groupLinkGuarded`). This also needs no race.
   - The check lists one member entry (`rgw_rest_iam_group.cc:608`), and
     `list_group_users` drops an entry whose user is gone
     (`rgw_sal_rados.cc:2293-2296`).
   - After finding 8, a renamed and deleted member leaves such an entry
     first in the list, so the group's other members are not seen.
10. AssumeRole can issue credentials for a role whose trust policy it never
    checked (`tcAssumeVsRecreateMain`, fixed by `assumeRoleOneRead`).
    - `verify_permission` reads the role and evaluates its trust policy
      (`rgw_rest_sts.cc:1100-1142`). `STSService::assumeRole` reads the
      role by name again and puts that read's ID into the token
      (`rgw_sts.cc:394-402`).
    - A role deleted and created again under the same name in between gets
      the session, whatever its trust policy says.
11. AssumeRole does not require the trust policy to allow the caller
    (`tcAssumeIdentityOnlyMain`, fixed by `trustRequired`).
    - In the same account, an identity policy that allows `sts:AssumeRole`
      is enough (`rgw_common.cc:1266`). AWS requires the role's trust
      policy to allow the caller in every case.
12. AssumeRole answers NoSuchEntity for a role that does not exist
    (`tcAssumeNoRoleMain`, fixed by `assumeDeniesMissing`).
    - `getRoleInfo` returns `ERR_NO_ROLE_FOUND` (`rgw_sts.cc:298`). The STS
      model lists no NoSuchEntity, and AWS answers AccessDenied.
13. GetSessionToken accepts temporary credentials, and the credentials it
    issues to a role session outlive the role (`tcGstFromRoleAns`,
    `tcGstFromRoleMain`, fixed by `gstLongTermOnly`).
    - Only AssumeRole* refuse role sessions (`rgw_rest_sts.cc:1092`).
      GetSessionToken's `verify_permission` checks only a policy
      (`:864-877`).
    - The token it issues carries the account type of a role but no role
      ID. If the token names no role, `STSEngine` reads no role
      (`rgw_rest_s3.cc:7549`). So the new credentials still authenticate
      after the role is deleted.
14. DeleteUser deletes a user that is in groups (`tcDelUserInGroup`).
    - `check_empty` does not look at group memberships, and
      `remove_user_info` unlinks them. IAM requires the caller to remove
      them first. This looks deliberate, so no fix flag is offered.
15. A read-modify-write that loses its race eleven times in a row is
    answered ConcurrentModification (`tcRoleRetriesOut`, with no retries).
    - Inline policy and attach operations do not list that error. With ten
      retries, the model's scenarios never run out.
16. An RGW that dies between two RADOS ops leaves the indexes inconsistent
    (`tcRoleCrash`, `tcUserCrash`).
    - Examples are a role name object that names no role, and a user
      unlinked from its account but not deleted. None of the writes is
      transactional, and nothing repairs them later. No fix is offered.

These hold on main:

- Concurrent updates of one role, user or group do not lose a write, because
  of the version check and the retry (`tcRoleUpdates`, `tcUserUpdates`,
  `tcMemberships`).
- The access key limit holds (`tcKeys`).
- Two roles never share a name (`tcSameRole`).
- AssumeRole racing a trust policy update gets an answer that some order
  of the two requests gives (`tcAssumeVsTrust`).
- A role session stops working once its role is deleted
  (`tcSessionAfterDelete`).

## Not modelled

- Multisite. The model has one zone, the metadata master. On a secondary,
  every write is forwarded to the master first and then applied locally.
  The limit checks run on the secondary's own counts, and DeleteRole skips
  its policy check there.
- The metadata cache and its notifications, which can serve a stale role or
  user to other RGWs.
- AssumeRoleWithWebIdentity and OIDC providers, session policies, session
  tags, permission evaluation beyond the trust policy, and the encryption
  of session tokens.
- Swift users and subusers, emails, and buckets owned by users.
- Inputs that the handlers examine before they touch RADOS: names, paths,
  policy documents, durations.
