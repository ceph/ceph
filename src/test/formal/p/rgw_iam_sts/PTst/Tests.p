// the code on main
fun Main(): tCfg {
  return (roleDeleteById = false, roleWriteNeedsRole = false, limitAtomic = false, userDeleteGuarded = false,
          authChecksActive = false, groupLinkGuarded = false, assumeRoleOneRead = false, trustRequired = false,
          gstLongTermOnly = false, assumeDeniesMissing = false, policyOpsPassOld = false,
          renameMovesMembers = false, userDeleteNeedsNoGroups = false, retriesOutServiceFailure = false,
          existingOpsOnly = false, maxRoles = -1, maxUsers = -1, maxGroups = -1, maxKeys = 2, maxRetries = 10,
          mayCrash = false);
}
// every proposed fix
fun Fixed(): tCfg {
  var c: tCfg;
  c = Main();
  c.roleDeleteById = true;
  c.roleWriteNeedsRole = true;
  c.limitAtomic = true;
  c.userDeleteGuarded = true;
  c.authChecksActive = true;
  c.groupLinkGuarded = true;
  c.assumeRoleOneRead = true;
  c.trustRequired = true;
  c.gstLongTermOnly = true;
  c.assumeDeniesMissing = true;
  c.policyOpsPassOld = true;
  c.renameMovesMembers = true;
  return c;
}

// requests
fun Q(kind: tKind, name: int, arg: int, arg2: int, slot: int): tReq {
  return (kind = kind, name = name, arg = arg, arg2 = arg2, slot = slot);
}
fun CreateRole(n: int, trust: int): tReq { return Q(R_CREATE_ROLE, n, trust, 0, 0); }
fun DeleteRole(n: int): tReq { return Q(R_DELETE_ROLE, n, 0, 0, 0); }
fun PutRolePolicy(n: int, p: int): tReq { return Q(R_PUT_ROLE_POLICY, n, p, 0, 0); }
fun DeleteRolePolicy(n: int, p: int): tReq { return Q(R_DELETE_ROLE_POLICY, n, p, 0, 0); }
fun AttachRolePolicy(n: int, p: int): tReq { return Q(R_ATTACH_ROLE_POLICY, n, p, 0, 0); }
fun UpdateTrust(n: int, t: int): tReq { return Q(R_UPDATE_TRUST, n, t, 0, 0); }
fun CreateUser(n: int): tReq { return Q(R_CREATE_USER, n, 0, 0, 0); }
fun DeleteUser(n: int): tReq { return Q(R_DELETE_USER, n, 0, 0, 0); }
fun RenameUser(n: int, dst: int): tReq { return Q(R_RENAME_USER, n, dst, 0, 0); }
fun CreateKey(n: int): tReq { return Q(R_CREATE_KEY, n, 0, 0, 0); }
fun DeactivateKey(n: int, key: int): tReq { return Q(R_UPDATE_KEY, n, key, 0, 0); }
fun DeleteKey(n: int, key: int): tReq { return Q(R_DELETE_KEY, n, key, 0, 0); }
fun PutUserPolicy(n: int, p: int): tReq { return Q(R_PUT_USER_POLICY, n, p, 0, 0); }
fun AttachUserPolicy(n: int, p: int): tReq { return Q(R_ATTACH_USER_POLICY, n, p, 0, 0); }
fun CreateGroup(n: int): tReq { return Q(R_CREATE_GROUP, n, 0, 0, 0); }
fun DeleteGroup(n: int): tReq { return Q(R_DELETE_GROUP, n, 0, 0, 0); }
fun AddToGroup(g: int, u: int): tReq { return Q(R_ADD_TO_GROUP, g, u, 0, 0); }
fun RemoveFromGroup(g: int, u: int): tReq { return Q(R_REMOVE_FROM_GROUP, g, u, 0, 0); }
fun PutGroupPolicy(g: int, p: int): tReq { return Q(R_PUT_GROUP_POLICY, g, p, 0, 0); }
fun Auth(key: int): tReq { return Q(R_AUTH, 0, key, 0, 0); }
fun AssumeRole(caller: int, role: int, slot: int): tReq { return Q(R_ASSUME_ROLE, role, caller, 0, slot); }
fun SessionTokenFrom(from: int, slot: int): tReq { return Q(R_GET_SESSION_TOKEN, 0, 0, from, slot); }
fun UseToken(slot: int): tReq { return Q(R_USE_TOKEN, 0, 0, slot, 0); }

// the starting state: entities as the code would have left them
fun Obj(ver: int, ref: int, info: tInfo): tObj { return (ver = ver, ref = ref, info = info); }
fun IxPut(st: tState, ix: tIx, name: int, val: int): tState {
  var m: tOmap;
  if (ix in st.ixs) {
    m = st.ixs[ix];
  }
  st.ixs[ix] = OmapAdd(m, name, val, true, NOLIMIT()).m;
  return st;
}
fun AddRole(st: tState, id: int, name: int, trust: int, policy: int): tState {
  var i: tInfo;
  i = NamedInfo(id, name);
  i.trust = trust;
  if (policy != 0) {
    i.policies += (policy);
  }
  st.objs[(k = OK_ROLE, id = id)] = Obj(id, 0, i);
  st.objs[(k = OK_ROLE_NAME, id = name)] = Obj(id, id, NoInfo());
  return IxPut(st, (k = IX_ROLES, id = 0), name, id);
}
// a user, with an active access key (if key is not 0), in a group (if
// group is not 0), with an inline policy (if policy is not 0)
fun AddUser(st: tState, id: int, name: int, key: int, group: int, policy: int): tState {
  var i: tInfo;
  i = NamedInfo(id, name);
  if (key != 0) {
    i.akeys[key] = true;
    st.objs[(k = OK_KEY, id = key)] = Obj(id, id, NoInfo());
  }
  if (group != 0) {
    i.groups += (group);
    st = IxPut(st, (k = IX_MEMBERS, id = group), name, id);
  }
  if (policy != 0) {
    i.policies += (policy);
  }
  st.objs[(k = OK_USER, id = id)] = Obj(id, 0, i);
  return IxPut(st, (k = IX_USERS, id = 0), name, id);
}
fun AddGroup(st: tState, id: int, name: int): tState {
  st.objs[(k = OK_GROUP, id = id)] = Obj(id, 0, NamedInfo(id, name));
  st.objs[(k = OK_GROUP_NAME, id = name)] = Obj(id, id, NoInfo());
  return IxPut(st, (k = IX_GROUPS, id = 0), name, id);
}

// IDs: roles 10.., users 20.., groups 30.., access keys 50..; entities and
// keys created by a request take its request ID (100..). Names are small
// numbers, one namespace per class. Policies 1..9, POL_ASSUME() and POL_ALL()
enum tScenario {
  SC_DEL_ROLE_VS_PUT,      // DeleteRole of an empty role, and PutRolePolicy on it
  SC_DEL_ROLE_VS_ATTACH,   // DeleteRole of an empty role, and AttachRolePolicy on it
  SC_DEL_ROLE_RECREATE,    // two DeleteRoles, a CreateRole and an AttachRolePolicy of one name
  SC_ROLE_UPDATES,         // two PutRolePolicys and an AttachRolePolicy on one role
  SC_ROLES_LIMIT,          // two CreateRoles in an account with room for one more
  SC_SAME_ROLE,            // two CreateRoles of one name
  SC_ROLE_CRASH,           // CreateRole and DeleteRole, either RGW may die
  SC_DEL_USER_VS_KEY,      // DeleteUser of an empty user, and CreateAccessKey for it
  SC_DEL_USER_VS_POLICY,   // DeleteUser of an empty user, and PutUserPolicy on it
  SC_USERS_LIMIT,          // two CreateUsers in an account with room for one more
  SC_SAME_USER,            // two CreateUsers of one name
  SC_RENAME_SAME,          // two users renamed to one new name
  SC_KEYS,                 // two CreateAccessKeys for a user with one key
  SC_DEACTIVATE_VS_POLICY, // UpdateAccessKey Inactive, and PutUserPolicy, then the key is used
  SC_USER_UPDATES,         // PutUserPolicy, AttachUserPolicy and CreateAccessKey on one user
  SC_USER_CRASH,           // CreateUser, CreateAccessKey and DeleteUser, any RGW may die
  SC_DEL_USER_IN_GROUP,    // DeleteUser of a user in a group
  SC_DEL_GROUP_VS_ADD,     // DeleteGroup of an empty group, and AddUserToGroup to it
  SC_SAME_GROUP,           // two CreateGroups of one name
  SC_GROUPS_LIMIT,         // two CreateGroups in an account with room for one more
  SC_RENAME_MEMBER,        // a group member renamed
  SC_STALE_MEMBER,         // a member renamed and deleted, then its group deleted with a member left
  SC_MEMBERSHIPS,          // AddUserToGroup to two groups, and PutGroupPolicy
  SC_ASSUME_VS_RECREATE,   // AssumeRole, while the role is deleted and its name re-created
  SC_ASSUME_VS_TRUST,      // AssumeRole, while the trust policy is replaced
  SC_ASSUME_IDENTITY_ONLY, // AssumeRole of a role whose trust policy does not name the caller
  SC_ASSUME_NO_ROLE,       // AssumeRole of a role that does not exist
  SC_SESSION_AFTER_DELETE, // a role session used after the role is deleted
  SC_GST_FROM_ROLE,        // GetSessionToken with a role session, used after the role is deleted
  SC_ROLE_RETRIES,         // two PutRolePolicys on one role, with no retries left
  SC_DEL_USER_VS_ADD,      // DeleteUser of an empty user, and AddUserToGroup of it
  SC_ROLE_HOT              // three PutRolePolicys on one role
}

fun Setup(sc: tScenario): (init: tState, script: seq[seq[tReq]]) {
  var st: tState;
  var s: seq[seq[tReq]];
  if (sc == SC_DEL_ROLE_VS_PUT) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Two(DeleteRole(1), PutRolePolicy(1, 5)));
  } else if (sc == SC_DEL_ROLE_VS_ATTACH) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Two(DeleteRole(1), AttachRolePolicy(1, 6)));
  } else if (sc == SC_DEL_ROLE_RECREATE) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Two(DeleteRole(1), DeleteRole(1)));
    s[0] += (2, CreateRole(1, TRUST_NONE()));
    s[0] += (3, AttachRolePolicy(1, 6));
  } else if (sc == SC_ROLE_UPDATES) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Three(PutRolePolicy(1, 1), PutRolePolicy(1, 2), AttachRolePolicy(1, 3)));
  } else if (sc == SC_ROLES_LIMIT) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Two(CreateRole(2, TRUST_NONE()), CreateRole(3, TRUST_NONE())));
  } else if (sc == SC_SAME_ROLE) {
    s += (0, Two(CreateRole(1, TRUST_NONE()), CreateRole(1, TRUST_NONE())));
  } else if (sc == SC_ROLE_CRASH) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Two(CreateRole(2, TRUST_NONE()), DeleteRole(1)));
  } else if (sc == SC_DEL_USER_VS_KEY) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Two(DeleteUser(1), CreateKey(1)));
  } else if (sc == SC_DEL_USER_VS_POLICY) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Two(DeleteUser(1), PutUserPolicy(1, 5)));
  } else if (sc == SC_USERS_LIMIT) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Two(CreateUser(2), CreateUser(3)));
  } else if (sc == SC_SAME_USER) {
    s += (0, Two(CreateUser(1), CreateUser(1)));
  } else if (sc == SC_RENAME_SAME) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    st = AddUser(st, 21, 2, 0, 0, 0);
    s += (0, Two(RenameUser(1, 3), RenameUser(2, 3)));
  } else if (sc == SC_KEYS) {
    st = AddUser(st, 20, 1, 50, 0, 0);
    s += (0, Two(CreateKey(1), CreateKey(1)));
  } else if (sc == SC_DEACTIVATE_VS_POLICY) {
    st = AddUser(st, 20, 1, 50, 0, 0);
    s += (0, Two(DeactivateKey(1, 50), PutUserPolicy(1, 5)));
    s += (1, One(Auth(50)));
  } else if (sc == SC_USER_UPDATES) {
    st = AddUser(st, 20, 1, 50, 0, 0);
    s += (0, Three(PutUserPolicy(1, 1), AttachUserPolicy(1, 2), CreateKey(1)));
  } else if (sc == SC_USER_CRASH) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Three(CreateUser(2), CreateKey(1), DeleteUser(1)));
  } else if (sc == SC_DEL_USER_IN_GROUP) {
    st = AddGroup(st, 30, 1);
    st = AddUser(st, 20, 1, 0, 30, 0);
    s += (0, One(DeleteUser(1)));
  } else if (sc == SC_DEL_GROUP_VS_ADD) {
    st = AddGroup(st, 30, 1);
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Two(DeleteGroup(1), AddToGroup(1, 1)));
  } else if (sc == SC_SAME_GROUP) {
    s += (0, Two(CreateGroup(1), CreateGroup(1)));
  } else if (sc == SC_GROUPS_LIMIT) {
    st = AddGroup(st, 30, 1);
    s += (0, Two(CreateGroup(2), CreateGroup(3)));
  } else if (sc == SC_RENAME_MEMBER) {
    st = AddGroup(st, 30, 1);
    st = AddUser(st, 20, 1, 0, 30, 0);
    s += (0, One(RenameUser(1, 5)));
  } else if (sc == SC_STALE_MEMBER) {
    // user 20 (name 1) sorts first among the group's member entries
    st = AddGroup(st, 30, 1);
    st = AddUser(st, 20, 1, 0, 30, 0);
    st = AddUser(st, 21, 2, 0, 30, 0);
    s += (0, One(RenameUser(1, 5)));
    s += (1, One(DeleteUser(5)));
    s += (2, One(DeleteGroup(1)));
  } else if (sc == SC_MEMBERSHIPS) {
    st = AddGroup(st, 30, 1);
    st = AddGroup(st, 31, 2);
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Three(AddToGroup(1, 1), AddToGroup(2, 1), PutGroupPolicy(1, 4)));
  } else if (sc == SC_ASSUME_VS_RECREATE) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    st = AddRole(st, 10, 1, 20, 0);
    s += (0, Two(AssumeRole(1, 1, 1), DeleteRole(1)));
    s[0] += (2, CreateRole(1, TRUST_NONE()));
  } else if (sc == SC_ASSUME_VS_TRUST) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    st = AddRole(st, 10, 1, 20, 0);
    s += (0, Two(AssumeRole(1, 1, 1), UpdateTrust(1, TRUST_NONE())));
  } else if (sc == SC_ASSUME_IDENTITY_ONLY) {
    st = AddUser(st, 20, 1, 0, 0, POL_ASSUME());
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, One(AssumeRole(1, 1, 1)));
  } else if (sc == SC_ASSUME_NO_ROLE) {
    st = AddUser(st, 20, 1, 0, 0, POL_ASSUME());
    s += (0, One(AssumeRole(1, 1, 1)));
  } else if (sc == SC_SESSION_AFTER_DELETE) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    st = AddRole(st, 10, 1, 20, 0);
    s += (0, One(AssumeRole(1, 1, 1)));
    s += (1, Two(DeleteRole(1), UseToken(1)));
    s += (2, One(UseToken(1)));
  } else if (sc == SC_GST_FROM_ROLE) {
    st = AddUser(st, 20, 1, 0, 0, 0);
    st = AddRole(st, 10, 1, 20, POL_ALL());
    s += (0, One(AssumeRole(1, 1, 1)));
    s += (1, One(SessionTokenFrom(1, 2)));
    s += (2, One(DeleteRolePolicy(1, POL_ALL())));
    s += (3, One(DeleteRole(1)));
    s += (4, Two(UseToken(1), UseToken(2)));
  } else if (sc == SC_ROLE_RETRIES) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Two(PutRolePolicy(1, 1), PutRolePolicy(1, 2)));
  } else if (sc == SC_DEL_USER_VS_ADD) {
    st = AddGroup(st, 30, 1);
    st = AddUser(st, 20, 1, 0, 0, 0);
    s += (0, Two(DeleteUser(1), AddToGroup(1, 1)));
  } else if (sc == SC_ROLE_HOT) {
    st = AddRole(st, 10, 1, TRUST_NONE(), 0);
    s += (0, Three(PutRolePolicy(1, 1), PutRolePolicy(1, 2), PutRolePolicy(1, 3)));
  }
  return (init = st, script = s);
}

machine Scenario {
  start state Init {
    entry (p: (sc: tScenario, cfg: tCfg)) {
      var x: (init: tState, script: seq[seq[tReq]]);
      x = Setup(p.sc);
      announce eConfig, p.cfg;
      announce eState, (rid = 0, st = x.init);
      new Driver((cfg = p.cfg, init = x.init, script = x.script));
    }
  }
}
