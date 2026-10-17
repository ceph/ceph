/*
 * RGW's IAM and STS front ends over one account: roles, users and their
 * access keys, groups, inline and managed policies, and the STS
 * operations that issue and accept temporary credentials.
 *
 * The RADOS state is one Store machine: the metadata objects (each
 * entity's info object, guarded by its RGWObjVersionTracker; the name
 * objects; the access key index) and the cls_user omap indexes (the
 * account's users, roles and groups, and each group's members). Each
 * Store handler is one atomic RADOS op. Each Rgw machine serves one
 * request, one op at a time, as RGW does.
 */

type tCfg = (
  // proposed fixes (false is main)
  roleDeleteById: bool,     // DeleteRole removes the role it loaded and checked, by ID, under
                            // the version it read; a raced removal reloads and checks again.
                            // Its name object and account entry are removed only while they
                            // still name that ID
  roleWriteNeedsRole: bool, // a role update that finds its info object gone fails with ENOENT,
                            // and writes no name object or account entry
  limitAtomic: bool,        // the account's user, role and group entries are added by
                            // cls_account_resource_add with the account's limit, exclusively,
                            // before the info object is written (rolled back on failure),
                            // rather than checked against a count read first
  userDeleteGuarded: bool,  // DeleteUser removes the user object first, under the version it
                            // read; a raced removal reloads and checks again, and only then are
                            // the indexes removed
  authChecksActive: bool,   // authentication refuses an access key the user info marks inactive
  groupLinkGuarded: bool,   // AddUserToGroup writes the group's member entry, then rewrites the
                            // group under the version it read, and only then the user; a group
                            // that is gone takes the entry back. DeleteGroup checks every member
                            // entry, not the first
  assumeRoleOneRead: bool,  // AssumeRole issues its token for the role it checked the trust
                            // policy of, not for a second read of the name
  trustRequired: bool,      // AssumeRole requires the role's trust policy to allow the caller, as
                            // AWS does; an identity policy alone is not enough
  gstLongTermOnly: bool,    // GetSessionToken refuses temporary credentials, as AWS does
  assumeDeniesMissing: bool, // AssumeRole answers AccessDenied for a role that does not exist, as
                            // AWS does, not NoSuchEntity (ERR_NO_ROLE_FOUND), which STS does not list
  policyOpsPassOld: bool,   // the user policy ops store the user with its old info, as the other
                            // user ops do, so they do not rewrite its key index and links
  renameMovesMembers: bool, // a renamed user's group member entries move to its new name
  userDeleteNeedsNoGroups: bool, // DeleteUser answers DeleteConflict for a user in a group, as IAM
                            // does (check_empty also looks at group_ids)
  retriesOutServiceFailure: bool, // once retry_raced_*_write runs out of retries, an operation whose
                            // API does not list ConcurrentModification answers ServiceFailure
  existingOpsOnly: bool,    // the fixes use only the ops RADOS and cls_user offer today: a removal
                            // only while an object or entry names an ID is a read, a check, and a
                            // removal (under the object's version; an omap entry has none)
  // environment
  maxRoles: int,            // the account's limits; -1 is none
  maxUsers: int,
  maxGroups: int,
  maxKeys: int,             // access keys per user (the account's max_access_keys)
  maxRetries: int,          // retry_raced_*_write's retries (10 on main); -1 is no limit
  mayCrash: bool            // an RGW may die between any two RADOS ops
);

// metadata objects: info objects by ID, name objects by name, and the access
// key index (user_keys_pool) by key ID
enum tOk { OK_ROLE, OK_ROLE_NAME, OK_USER, OK_KEY, OK_GROUP, OK_GROUP_NAME }
type tOid = (k: tOk, id: int);

// cls_user omap indexes: the account's users (by display name), roles and
// groups (by name), and a group's members (users.<group id>, by display name)
enum tIxk { IX_USERS, IX_ROLES, IX_GROUPS, IX_MEMBERS }
type tIx = (k: tIxk, id: int);

// one record for every entity's info. trust is a role's trust policy: TRUST_NONE(),
// TRUST_ACCOUNT() (the account principal) or the user ID it names
type tInfo = (id: int, name: int, policies: set[int], managed: set[int], akeys: map[int, bool],
              groups: set[int], trust: int);
// ref: the ID a name object or a key index object points to; ver: its
// RGWObjVersionTracker version
type tObj = (ver: int, ref: int, info: tInfo);

fun TRUST_NONE(): int { return 0; }
fun TRUST_ACCOUNT(): int { return -1; }
// an identity policy that allows sts:AssumeRole on any role
fun POL_ASSUME(): int { return 90; }
// a role policy that allows every action, sts:GetSessionToken among them
fun POL_ALL(): int { return 91; }
fun NOLIMIT(): int { return 1000000; }

fun NoInfo(): tInfo {
  return (id = 0, name = 0, policies = default(set[int]), managed = default(set[int]),
          akeys = default(map[int, bool]), groups = default(set[int]), trust = 0);
}
fun NamedInfo(id: int, name: int): tInfo {
  var i: tInfo;
  i = NoInfo();
  i.id = id;
  i.name = name;
  return i;
}

// the error codes of the IAM and STS APIs (the Smithy models' awsQueryError
// codes), and how RGW answers
enum tCode {
  E_OK,
  E_NO_SUCH_ENTITY,          // 404
  E_ENTITY_ALREADY_EXISTS,   // 409
  E_DELETE_CONFLICT,         // 409
  E_LIMIT_EXCEEDED,          // 409
  E_CONCURRENT_MODIFICATION, // 409
  E_INVALID_INPUT,           // 400
  E_ACCESS_DENIED,           // 403, the protocol's
  E_INVALID_ACCESS_KEY,      // 403 InvalidAccessKeyId
  E_SERVICE_FAILURE          // 500
}

enum tKind {
  R_CREATE_ROLE,        // name, trust (arg)
  R_DELETE_ROLE,        // name
  R_PUT_ROLE_POLICY,    // name, policy (arg)
  R_DELETE_ROLE_POLICY, // name, policy
  R_ATTACH_ROLE_POLICY, // name, policy ARN (arg)
  R_UPDATE_TRUST,       // name, trust (arg): UpdateAssumeRolePolicy
  R_CREATE_USER,        // name
  R_DELETE_USER,        // name
  R_RENAME_USER,        // name, new name (arg): UpdateUser NewUserName
  R_CREATE_KEY,         // name
  R_UPDATE_KEY,         // name, key (arg), active (arg2 != 0)
  R_DELETE_KEY,         // name, key
  R_PUT_USER_POLICY,    // name, policy
  R_DELETE_USER_POLICY, // name, policy
  R_ATTACH_USER_POLICY, // name, policy ARN
  R_CREATE_GROUP,       // name
  R_DELETE_GROUP,       // name
  R_ADD_TO_GROUP,       // group name, user name (arg)
  R_REMOVE_FROM_GROUP,  // group name, user name
  R_PUT_GROUP_POLICY,   // group name, policy
  R_AUTH,               // an S3 request signed with access key (arg)
  R_ASSUME_ROLE,        // caller user name (arg), role name; the token goes to slot
  R_GET_SESSION_TOKEN,  // caller user name (arg), or the token in slot arg2; the token goes to slot
  R_USE_TOKEN           // an S3 request signed with the token in slot (arg2)
}

type tReq = (kind: tKind, name: int, arg: int, arg2: int, slot: int);

// temporary credentials, as the session token carries them: the role ID (0
// for none), the user, and whether the session is a role's (acct_type
// TYPE_ROLE). origin is the role the session descends from, for the specs
type tToken = (present: bool, roleId: int, user: int, isRole: bool, origin: int);

// an answer. entity: the info object the request acted on (0 if none);
// token: what an STS operation issued
type tAnswer = (rid: int, kind: tKind, code: tCode, entity: int, token: tToken);

// the class of each kind's entity
enum tClass { CL_ROLE, CL_USER, CL_GROUP, CL_NONE }

fun ClassOf(k: tKind): tClass {
  if (k == R_CREATE_ROLE || k == R_DELETE_ROLE || k == R_PUT_ROLE_POLICY || k == R_DELETE_ROLE_POLICY ||
      k == R_ATTACH_ROLE_POLICY || k == R_UPDATE_TRUST) {
    return CL_ROLE;
  }
  if (k == R_CREATE_USER || k == R_DELETE_USER || k == R_RENAME_USER || k == R_CREATE_KEY ||
      k == R_UPDATE_KEY || k == R_DELETE_KEY || k == R_PUT_USER_POLICY || k == R_DELETE_USER_POLICY ||
      k == R_ATTACH_USER_POLICY) {
    return CL_USER;
  }
  if (k == R_CREATE_GROUP || k == R_DELETE_GROUP || k == R_ADD_TO_GROUP || k == R_REMOVE_FROM_GROUP ||
      k == R_PUT_GROUP_POLICY) {
    return CL_GROUP;
  }
  return CL_NONE;
}

// the state the specs see after every change
type tState = (objs: map[tOid, tObj], ixs: map[tIx, tOmap]);

// requests to the Store, each one atomic op
event eGet: (from: machine, oid: tOid);
event eGot: (present: bool, obj: tObj);
// a write: exclusive create, or a write checked against ver (none if ver is 0)
event ePut: (from: machine, rid: int, oid: tOid, excl: bool, ver: int, ref: int, info: tInfo);
// a removal, checked against ver (none if ver is 0), or only while it points to ref (if ref is not 0)
event eRm: (from: machine, rid: int, oid: tOid, ver: int, ref: int);
event eRc: tRc;
// cls_account_resource_add: exclusive or not, with a limit on the count
event eIxAdd: (from: machine, rid: int, ix: tIx, name: int, val: int, excl: bool, limit: int);
// cls_account_resource_rm, only while it maps to val (if val is not 0); or the whole object
event eIxRm: (from: machine, rid: int, ix: tIx, name: int, val: int);
event eIxDrop: (from: machine, rid: int, ix: tIx);
event eIxGet: (from: machine, ix: tIx, name: int);
event eIxGot: (present: bool, val: int);
event eIxCount: (from: machine, ix: tIx);
event eIxCounted: int;
event eIxList: (from: machine, ix: tIx);
event eIxListed: map[int, int];

// to the specs
event eConfig: tCfg;
event eState: (rid: int, st: tState);
event eRemoved: (rid: int, oid: tOid, obj: tObj, st: tState);
event eAnswer: tAnswer;
event eFinal: tState;
event eCrash: int;

// for common/Common.p: the Store starts from a state; a request's input
// and output are session credentials, handed on in slots
type tInit = tState;
type tOut = tToken;
fun InSlot(r: tReq): int {
  if (r.kind == R_USE_TOKEN || r.kind == R_GET_SESSION_TOKEN) {
    return r.arg2;
  }
  return 0;
}
fun OutSlot(r: tReq): int { return r.slot; }
fun FirstRid(): int { return 100; }
