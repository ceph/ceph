/*
 * One RGW serving one IAM, STS or S3 request, one RADOS op at a time, as
 * the code on main does. The caller of the IAM operations is the
 * account's root user; the metadata master is the only zone.
 *
 * An internal ECANCELED is carried as E_CONCURRENT_MODIFICATION, which is
 * what retry_raced_*_write answers once its retries run out.
 */

type tLoaded = (code: tCode, id: int, ver: int, info: tInfo);

machine Rgw {
  var cfg: tCfg;
  var store: machine;
  var driver: machine;
  var rid: int;
  var req: tReq;
  var token: tToken;    // the credentials a request presents (R_USE_TOKEN, R_GET_SESSION_TOKEN)
  var issued: tToken;   // the credentials an STS request issues
  var entity: int;      // the info object the request acted on
  var dead: bool;

  start state Run {
    entry (p: (cfg: tCfg, store: machine, driver: machine, rid: int, req: tReq, input: tOut)) {
      var code: tCode;
      cfg = p.cfg;
      store = p.store;
      driver = p.driver;
      rid = p.rid;
      req = p.req;
      token = p.input;
      code = Serve();
      if (dead) {
        announce eCrash, rid;
        send driver, eDone, (rid = rid, crashed = true, out = default(tToken));
      } else {
        announce eAnswer, (rid = rid, kind = req.kind, code = code, entity = entity, token = issued);
        send driver, eDone, (rid = rid, crashed = false, out = issued);
      }
    }
  }

  fun Serve(): tCode {
    var k: tKind;
    k = req.kind;
    if (k == R_CREATE_ROLE) { return CreateRole(); }
    if (k == R_DELETE_ROLE) { return DeleteRole(); }
    if (k == R_PUT_ROLE_POLICY || k == R_DELETE_ROLE_POLICY || k == R_ATTACH_ROLE_POLICY || k == R_UPDATE_TRUST) {
      return UpdateRole();
    }
    if (k == R_CREATE_USER) { return CreateUser(); }
    if (k == R_DELETE_USER) { return DeleteUser(); }
    if (k == R_RENAME_USER || k == R_CREATE_KEY || k == R_UPDATE_KEY || k == R_DELETE_KEY ||
        k == R_PUT_USER_POLICY || k == R_DELETE_USER_POLICY || k == R_ATTACH_USER_POLICY) {
      return UpdateUser();
    }
    if (k == R_CREATE_GROUP) { return CreateGroup(); }
    if (k == R_DELETE_GROUP) { return DeleteGroup(); }
    if (k == R_ADD_TO_GROUP || k == R_REMOVE_FROM_GROUP) { return Membership(); }
    if (k == R_PUT_GROUP_POLICY) { return PutGroupPolicy(); }
    if (k == R_AUTH) { return Authenticate(); }
    if (k == R_ASSUME_ROLE) { return AssumeRole(); }
    if (k == R_GET_SESSION_TOKEN) { return GetSessionToken(); }
    return UseToken();
  }

  fun Code(rc: tRc): tCode {
    if (rc == OK) { return E_OK; }
    if (rc == ENOENT) { return E_NO_SUCH_ENTITY; }
    if (rc == EEXIST) { return E_ENTITY_ALREADY_EXISTS; }
    if (rc == ECANCELED) { return E_CONCURRENT_MODIFICATION; }
    return E_LIMIT_EXCEEDED;  // EUSERS, from a limited cls_account_resource_add (proposed)
  }

  fun MoreRetries(i: int): bool {
    return cfg.maxRetries < 0 || i < cfg.maxRetries;
  }

  // what a request answers once retry_raced_*_write has run out of retries
  fun RetriesOut(c: tCode): tCode {
    var k: tKind;
    k = req.kind;
    if (c == E_CONCURRENT_MODIFICATION && cfg.retriesOutServiceFailure &&
        !(k == R_CREATE_ROLE || k == R_DELETE_ROLE || k == R_CREATE_USER || k == R_DELETE_USER ||
          k == R_RENAME_USER)) {
      return E_SERVICE_FAILURE;
    }
    return c;
  }

  fun Limit(max: int): int {
    if (max < 0) { return NOLIMIT(); }
    return max;
  }

  /*
   * Roles (rgw_rest_role.cc, driver/rados/role.cc)
   */

  // load_role: the name object, then the info object by ID
  fun LoadRoleByName(name: int): tLoaded {
    var o: (present: bool, obj: tObj);
    o = Get((k = OK_ROLE_NAME, id = name));
    if (!o.present) {
      return (code = E_NO_SUCH_ENTITY, id = 0, ver = 0, info = NoInfo());
    }
    return LoadById(OK_ROLE, o.obj.ref);
  }

  fun LoadById(k: tOk, id: int): tLoaded {
    var o: (present: bool, obj: tObj);
    o = Get((k = k, id = id));
    if (!o.present) {
      return (code = E_NO_SUCH_ENTITY, id = id, ver = 0, info = NoInfo());
    }
    return (code = E_OK, id = id, ver = o.obj.ver, info = o.obj.info);
  }

  // rgwrados::role::write. A write that finds no info object writes the name
  // object and the account entry as if the role were new; the info write's
  // version check then fails, and both are rolled back
  fun RoleWrite(info: tInfo, ver: int, excl: bool): tCode {
    var o: (present: bool, obj: tObj);
    var sameName: bool;
    var wroteName: bool;
    var wroteIx: bool;
    var rc: tRc;
    if (!excl) {
      o = Get((k = OK_ROLE, id = info.id));
      sameName = o.present && o.obj.info.name == info.name;
      if (!o.present && cfg.roleWriteNeedsRole) {
        return E_NO_SUCH_ENTITY;
      }
    }
    if (!sameName) {
      rc = Put((k = OK_ROLE_NAME, id = info.name), true, 0, info.id, NoInfo());
      if (rc != OK) {
        return Code(rc);
      }
      wroteName = true;
      // write_path: roles::add, exclusive, with no limit on main
      if (excl && cfg.limitAtomic) {
        rc = IxAdd((k = IX_ROLES, id = 0), info.name, info.id, true, Limit(cfg.maxRoles));
      } else {
        rc = IxAdd((k = IX_ROLES, id = 0), info.name, info.id, true, NOLIMIT());
      }
      if (rc != OK) {
        Rm((k = OK_ROLE_NAME, id = info.name), 0, 0);
        return Code(rc);
      }
      wroteIx = true;
    }
    rc = Put((k = OK_ROLE, id = info.id), excl, ver, 0, info);
    if (rc != OK) {
      if (wroteName) { Rm((k = OK_ROLE_NAME, id = info.name), 0, 0); }
      if (wroteIx) { IxRm((k = IX_ROLES, id = 0), info.name, 0); }
      return Code(rc);
    }
    return E_OK;
  }

  // RGWCreateRole: check_role_limit in init_processing, then an exclusive write
  fun CreateRole(): tCode {
    var info: tInfo;
    if (!cfg.limitAtomic && cfg.maxRoles >= 0) {
      if (IxCount((k = IX_ROLES, id = 0)) >= cfg.maxRoles) {
        return E_LIMIT_EXCEEDED;
      }
    }
    info = NamedInfo(rid, req.name);
    info.trust = req.arg;
    entity = rid;
    return RoleWrite(info, 0, true);
  }

  // retry_raced_role_write over one of the role's read-modify-writes
  fun UpdateRole(): tCode {
    var l: tLoaded;
    var c: tCode;
    var i: int;
    l = LoadRoleByName(req.name);
    if (l.code != E_OK) {
      return l.code;
    }
    entity = l.id;
    c = RoleRmw(l);
    while (c == E_CONCURRENT_MODIFICATION && MoreRetries(i)) {
      l = LoadById(OK_ROLE, l.id);
      if (l.code != E_OK) {
        return l.code;
      }
      c = RoleRmw(l);
      i = i + 1;
    }
    return RetriesOut(c);
  }

  fun RoleRmw(l: tLoaded): tCode {
    var info: tInfo;
    info = l.info;
    if (req.kind == R_PUT_ROLE_POLICY) {
      info.policies += (req.arg);
    } else if (req.kind == R_DELETE_ROLE_POLICY) {
      if (!(req.arg in info.policies)) {
        return E_NO_SUCH_ENTITY;
      }
      info.policies -= (req.arg);
    } else if (req.kind == R_ATTACH_ROLE_POLICY) {
      if (req.arg in info.managed) {
        return E_OK;  // already attached: nothing to write
      }
      info.managed += (req.arg);
    } else {
      info.trust = req.arg;
    }
    return RoleWrite(info, l.ver, false);
  }

  // RGWDeleteRole: the role loaded in init_processing is checked for
  // policies; role->delete_obj then resolves the name again, and removes
  // what it finds under the version it reads then (role::remove,
  // remove_by_id)
  fun DeleteRole(): tCode {
    var l: tLoaded;
    var c: tCode;
    var i: int;
    l = LoadRoleByName(req.name);
    if (l.code != E_OK) {
      return l.code;
    }
    c = DeleteRoleOnce(l);
    while (c == E_CONCURRENT_MODIFICATION && MoreRetries(i)) {
      l = LoadById(OK_ROLE, l.id);
      if (l.code != E_OK) {
        return l.code;
      }
      c = DeleteRoleOnce(l);
      i = i + 1;
    }
    return RetriesOut(c);
  }

  fun DeleteRoleOnce(l: tLoaded): tCode {
    var n: (present: bool, obj: tObj);
    var r: tLoaded;
    var rc: tRc;
    if (sizeof(l.info.policies) > 0 || sizeof(l.info.managed) > 0) {
      return E_DELETE_CONFLICT;
    }
    if (cfg.roleDeleteById) {
      rc = Rm((k = OK_ROLE, id = l.id), l.ver, 0);
      if (rc != OK) {
        return Code(rc);
      }
      entity = l.id;
      Rm((k = OK_ROLE_NAME, id = l.info.name), 0, l.id);
      IxRm((k = IX_ROLES, id = 0), l.info.name, l.id);
      return E_OK;
    }
    n = Get((k = OK_ROLE_NAME, id = req.name));
    if (!n.present) {
      return E_NO_SUCH_ENTITY;
    }
    r = LoadById(OK_ROLE, n.obj.ref);
    if (r.code != E_OK) {
      return r.code;
    }
    rc = Rm((k = OK_ROLE, id = r.id), r.ver, 0);
    if (rc != OK) {
      return Code(rc);
    }
    entity = r.id;
    Rm((k = OK_ROLE_NAME, id = r.info.name), 0, 0);
    IxRm((k = IX_ROLES, id = 0), r.info.name, 0);
    return E_OK;
  }

  /*
   * Users (rgw_rest_iam_user.cc, rgw_rest_user_policy.cc,
   * services/svc_user_rados.cc)
   */

  // the account's users index by display name, then the user object
  fun LoadUserByName(name: int): tLoaded {
    var x: (present: bool, val: int);
    x = IxGet((k = IX_USERS, id = 0), name);
    if (!x.present) {
      return (code = E_NO_SUCH_ENTITY, id = 0, ver = 0, info = NoInfo());
    }
    return LoadById(OK_USER, x.val);
  }

  fun KeyActive(i: tInfo, k: int): bool {
    return k in i.akeys && i.akeys[k];
  }

  // RGWSI_User_RADOS PutOperation: prepare (a read of the name's entry),
  // put (the user object, exclusive or version-checked), complete (the
  // access key index, the old indexes, the account entry, the group member
  // entries). Only put is checked against a version
  fun StoreUser(info: tInfo, ver: int, excl: bool, hasOld: bool, old: tInfo): tCode {
    var x: (present: bool, val: int);
    var newName: bool;
    var linked: bool;
    var rc: tRc;
    var k: int;
    var g: int;
    newName = !hasOld || old.name != info.name;
    // prepare
    if (cfg.limitAtomic && newName && (excl || hasOld)) {
      if (excl) {
        rc = IxAdd((k = IX_USERS, id = 0), info.name, info.id, true, Limit(cfg.maxUsers));
      } else {
        rc = IxAdd((k = IX_USERS, id = 0), info.name, info.id, true, NOLIMIT());
      }
      if (rc != OK) {
        return Code(rc);
      }
      linked = true;
    } else if (newName) {
      x = IxGet((k = IX_USERS, id = 0), info.name);
      if (x.present && x.val != info.id) {
        return E_ENTITY_ALREADY_EXISTS;
      }
    }
    // put
    rc = Put((k = OK_USER, id = info.id), excl, ver, 0, info);
    if (rc != OK) {
      if (linked) { IxRm((k = IX_USERS, id = 0), info.name, info.id); }
      return Code(rc);
    }
    // complete
    foreach (k in keys(info.akeys)) {
      if (info.akeys[k] && !(hasOld && KeyActive(old, k))) {
        Put((k = OK_KEY, id = k), excl, 0, info.id, NoInfo());
      }
    }
    if (hasOld) {
      foreach (k in keys(old.akeys)) {
        if (old.akeys[k] && !KeyActive(info, k)) {
          Rm((k = OK_KEY, id = k), 0, 0);
        }
      }
      if (old.name != info.name) {
        IxRm((k = IX_USERS, id = 0), old.name, 0);
      }
      foreach (g in old.groups) {
        if (!(g in info.groups) || (cfg.renameMovesMembers && old.name != info.name)) {
          IxRm((k = IX_MEMBERS, id = g), old.name, 0);
        }
      }
    }
    if (newName && !linked) {
      IxAdd((k = IX_USERS, id = 0), info.name, info.id, false, NOLIMIT());
    }
    foreach (g in info.groups) {
      if (!(hasOld && g in old.groups) || (cfg.renameMovesMembers && hasOld && old.name != info.name)) {
        IxAdd((k = IX_MEMBERS, id = g), info.name, info.id, false, NOLIMIT());
      }
    }
    return E_OK;
  }

  // RGWCreateUser_IAM: the limit is checked against the count read first
  fun CreateUser(): tCode {
    if (!cfg.limitAtomic && cfg.maxUsers >= 0) {
      if (IxCount((k = IX_USERS, id = 0)) >= cfg.maxUsers) {
        return E_LIMIT_EXCEEDED;
      }
    }
    entity = rid;
    return StoreUser(NamedInfo(rid, req.name), 0, true, false, NoInfo());
  }

  // retry_raced_user_write over one of the user's read-modify-writes
  fun UpdateUser(): tCode {
    var l: tLoaded;
    l = LoadUserByName(req.name);
    if (l.code != E_OK) {
      return l.code;
    }
    entity = l.id;
    return RetryUser(l, 0);
  }

  fun RetryUser(l: tLoaded, group: int): tCode {
    var c: tCode;
    var i: int;
    c = UserRmw(l, group);
    while (c == E_CONCURRENT_MODIFICATION && MoreRetries(i)) {
      l = LoadById(OK_USER, l.id);
      if (l.code != E_OK) {
        return l.code;
      }
      c = UserRmw(l, group);
      i = i + 1;
    }
    return RetriesOut(c);
  }

  fun UserRmw(l: tLoaded, group: int): tCode {
    var info: tInfo;
    var k: tKind;
    k = req.kind;
    info = l.info;
    if (k == R_RENAME_USER) {
      info.name = req.arg;
      return StoreUser(info, l.ver, false, true, l.info);
    }
    if (k == R_CREATE_KEY) {
      if (sizeof(info.akeys) >= cfg.maxKeys) {
        return E_LIMIT_EXCEEDED;
      }
      info.akeys[rid] = true;
      return StoreUser(info, l.ver, false, true, l.info);
    }
    if (k == R_UPDATE_KEY) {
      if (!(req.arg in info.akeys)) {
        return E_NO_SUCH_ENTITY;
      }
      if (info.akeys[req.arg] == (req.arg2 != 0)) {
        return E_OK;
      }
      info.akeys[req.arg] = req.arg2 != 0;
      return StoreUser(info, l.ver, false, true, l.info);
    }
    if (k == R_DELETE_KEY) {
      if (!(req.arg in info.akeys)) {
        return E_NO_SUCH_ENTITY;
      }
      info.akeys -= (req.arg);
      return StoreUser(info, l.ver, false, true, l.info);
    }
    if (k == R_ADD_TO_GROUP) {
      if (group in info.groups) {
        return E_OK;
      }
      info.groups += (group);
      return StoreUser(info, l.ver, false, true, l.info);
    }
    if (k == R_REMOVE_FROM_GROUP) {
      if (!(group in info.groups)) {
        return E_OK;
      }
      info.groups -= (group);
      return StoreUser(info, l.ver, false, true, l.info);
    }
    // the user policy ops store the user with no old info (store_user(s, y, false))
    if (k == R_PUT_USER_POLICY) {
      info.policies += (req.arg);
    } else if (k == R_DELETE_USER_POLICY) {
      if (!(req.arg in info.policies)) {
        return E_NO_SUCH_ENTITY;
      }
      info.policies -= (req.arg);
    } else {
      info.managed += (req.arg);
    }
    if (cfg.policyOpsPassOld) {
      return StoreUser(info, l.ver, false, true, l.info);
    }
    return StoreUser(info, l.ver, false, false, NoInfo());
  }

  // RGWDeleteUser_IAM: check_empty on the user loaded in init_processing,
  // then remove_user_info: the key index, the account entry, the group
  // member entries, and last the user object under the version read
  // (remove_uid_index, which answers success on ECANCELED)
  fun DeleteUser(): tCode {
    var l: tLoaded;
    var rc: tRc;
    var i: int;
    l = LoadUserByName(req.name);
    if (l.code != E_OK) {
      return l.code;
    }
    entity = l.id;
    if (cfg.userDeleteGuarded) {
      while (true) {
        if (!UserEmpty(l.info)) {
          return E_DELETE_CONFLICT;
        }
        rc = Rm((k = OK_USER, id = l.id), l.ver, 0);
        if (rc == OK) {
          break;
        }
        if (rc != ECANCELED || !MoreRetries(i)) {
          return RetriesOut(Code(rc));
        }
        l = LoadById(OK_USER, l.id);
        if (l.code != E_OK) {
          return l.code;
        }
        i = i + 1;
      }
      RemoveUserIndexes(l.info, l.id);
      return E_OK;
    }
    if (!UserEmpty(l.info)) {
      return E_DELETE_CONFLICT;
    }
    rc = RemoveUserIndexes(l.info, 0);
    if (rc != OK) {
      return Code(rc);
    }
    Rm((k = OK_USER, id = l.id), l.ver, 0);
    return E_OK;
  }

  // the indexes remove_user_info removes; the account entry only while it
  // names the user (if id is not 0)
  fun RemoveUserIndexes(info: tInfo, id: int): tRc {
    var k: int;
    var g: int;
    var rc: tRc;
    foreach (k in keys(info.akeys)) {
      if (info.akeys[k]) {
        Rm((k = OK_KEY, id = k), 0, 0);
      }
    }
    rc = IxRm((k = IX_USERS, id = 0), info.name, id);
    if (rc != OK) {
      return rc;
    }
    foreach (g in info.groups) {
      IxRm((k = IX_MEMBERS, id = g), info.name, 0);
    }
    return OK;
  }

  // check_empty: access keys, inline and managed policies (and, proposed,
  // group memberships)
  fun UserEmpty(i: tInfo): bool {
    if (cfg.userDeleteNeedsNoGroups && sizeof(i.groups) > 0) {
      return false;
    }
    return sizeof(i.akeys) == 0 && sizeof(i.policies) == 0 && sizeof(i.managed) == 0;
  }

  /*
   * Groups (rgw_rest_iam_group.cc, driver/rados/group.cc)
   */

  fun LoadGroupByName(name: int): tLoaded {
    var o: (present: bool, obj: tObj);
    o = Get((k = OK_GROUP_NAME, id = name));
    if (!o.present) {
      return (code = E_NO_SUCH_ENTITY, id = 0, ver = 0, info = NoInfo());
    }
    return LoadById(OK_GROUP, o.obj.ref);
  }

  // rgwrados::group::write: a read of the new name object, the info object,
  // then the name object and account entry, whose failures are not fatal
  fun GroupWrite(info: tInfo, ver: int, excl: bool, hasOld: bool, old: tInfo): tCode {
    var o: (present: bool, obj: tObj);
    var sameName: bool;
    var removeName: bool;
    var oldNameVer: int;
    var linked: bool;
    var rc: tRc;
    sameName = hasOld && old.name == info.name;
    if (hasOld && !sameName) {
      o = Get((k = OK_GROUP_NAME, id = old.name));
      if (o.present && o.obj.ref == info.id) {
        removeName = true;
        oldNameVer = o.obj.ver;
      }
    }
    if (!sameName) {
      o = Get((k = OK_GROUP_NAME, id = info.name));
      if (o.present) {
        return E_ENTITY_ALREADY_EXISTS;
      }
      if (cfg.limitAtomic) {
        if (excl) {
          rc = IxAdd((k = IX_GROUPS, id = 0), info.name, info.id, true, Limit(cfg.maxGroups));
        } else {
          rc = IxAdd((k = IX_GROUPS, id = 0), info.name, info.id, true, NOLIMIT());
        }
        if (rc != OK) {
          return Code(rc);
        }
        linked = true;
      }
    }
    rc = Put((k = OK_GROUP, id = info.id), excl, ver, 0, info);
    if (rc != OK) {
      if (linked) { IxRm((k = IX_GROUPS, id = 0), info.name, info.id); }
      return Code(rc);
    }
    if (removeName) {
      Rm((k = OK_GROUP_NAME, id = old.name), oldNameVer, 0);
      IxRm((k = IX_GROUPS, id = 0), old.name, 0);
    }
    if (!sameName) {
      Put((k = OK_GROUP_NAME, id = info.name), true, 0, info.id, NoInfo());
      if (!linked) {
        IxAdd((k = IX_GROUPS, id = 0), info.name, info.id, false, NOLIMIT());
      }
    }
    return E_OK;
  }

  // RGWCreateGroup_IAM: the limit is checked against the count read first
  fun CreateGroup(): tCode {
    if (!cfg.limitAtomic && cfg.maxGroups >= 0) {
      if (IxCount((k = IX_GROUPS, id = 0)) >= cfg.maxGroups) {
        return E_LIMIT_EXCEEDED;
      }
    }
    entity = rid;
    return GroupWrite(NamedInfo(rid, req.name), 0, true, false, NoInfo());
  }

  // retry_raced_group_write: check_empty, then rgwrados::group::remove
  fun DeleteGroup(): tCode {
    var l: tLoaded;
    var c: tCode;
    var i: int;
    l = LoadGroupByName(req.name);
    if (l.code != E_OK) {
      return l.code;
    }
    entity = l.id;
    c = DeleteGroupOnce(l);
    while (c == E_CONCURRENT_MODIFICATION && MoreRetries(i)) {
      l = LoadById(OK_GROUP, l.id);
      if (l.code != E_OK) {
        return l.code;
      }
      c = DeleteGroupOnce(l);
      i = i + 1;
    }
    return RetriesOut(c);
  }

  fun DeleteGroupOnce(l: tLoaded): tCode {
    var members: map[int, int];
    var n: int;
    var first: int;
    var rc: tRc;
    if (sizeof(l.info.policies) > 0 || sizeof(l.info.managed) > 0) {
      return E_DELETE_CONFLICT;
    }
    // list_group_users(..., max_items = 1): the first member entry, dropped
    // if its user is gone
    members = IxList((k = IX_MEMBERS, id = l.id));
    first = -1;
    foreach (n in keys(members)) {
      if (cfg.groupLinkGuarded) {
        if (LoadById(OK_USER, members[n]).code == E_OK) {
          return E_DELETE_CONFLICT;
        }
      } else if (first == -1 || n < first) {
        first = n;
      }
    }
    if (first != -1 && LoadById(OK_USER, members[first]).code == E_OK) {
      return E_DELETE_CONFLICT;
    }
    rc = Rm((k = OK_GROUP, id = l.id), l.ver, 0);
    if (rc != OK) {
      return Code(rc);
    }
    Rm((k = OK_GROUP_NAME, id = l.info.name), 0, 0);
    IxDrop((k = IX_MEMBERS, id = l.id));
    IxRm((k = IX_GROUPS, id = 0), l.info.name, 0);
    return E_OK;
  }

  // RGWAddUserToGroup_IAM, RGWRemoveUserFromGroup_IAM: the group is read in
  // init_processing; only the user is written
  fun Membership(): tCode {
    var g: tLoaded;
    var u: tLoaded;
    var c: tCode;
    g = LoadGroupByName(req.name);
    if (g.code != E_OK) {
      return g.code;
    }
    u = LoadUserByName(req.arg);
    if (u.code != E_OK) {
      return u.code;
    }
    entity = u.id;
    if (req.kind == R_ADD_TO_GROUP && cfg.groupLinkGuarded) {
      // the member entry first, then the group rewritten under the version
      // read: a DeleteGroup that checked the members before the entry was
      // written then fails its version check and checks again
      IxAdd((k = IX_MEMBERS, id = g.id), u.info.name, u.id, false, NOLIMIT());
      while (Put((k = OK_GROUP, id = g.id), false, g.ver, 0, g.info) != OK) {
        g = LoadById(OK_GROUP, g.id);
        if (g.code != E_OK) {
          IxRm((k = IX_MEMBERS, id = g.id), u.info.name, u.id);
          return E_NO_SUCH_ENTITY;
        }
      }
    }
    c = RetryUser(u, g.id);
    if (req.kind == R_ADD_TO_GROUP && cfg.groupLinkGuarded && c != E_OK) {
      // the user is gone or the write failed: take the member entry back
      IxRm((k = IX_MEMBERS, id = g.id), u.info.name, u.id);
    }
    return c;
  }

  fun PutGroupPolicy(): tCode {
    var l: tLoaded;
    var c: tCode;
    var i: int;
    var info: tInfo;
    l = LoadGroupByName(req.name);
    if (l.code != E_OK) {
      return l.code;
    }
    entity = l.id;
    info = l.info;
    info.policies += (req.arg);
    c = GroupWrite(info, l.ver, false, true, l.info);
    while (c == E_CONCURRENT_MODIFICATION && MoreRetries(i)) {
      l = LoadById(OK_GROUP, l.id);
      if (l.code != E_OK) {
        return l.code;
      }
      info = l.info;
      info.policies += (req.arg);
      c = GroupWrite(info, l.ver, false, true, l.info);
      i = i + 1;
    }
    return RetriesOut(c);
  }

  /*
   * S3 authentication (rgw_rest_s3.cc LocalEngine::authenticate)
   */

  // the access key index, the user it names, and the key in the user info
  fun Authenticate(): tCode {
    var x: (present: bool, obj: tObj);
    var u: tLoaded;
    x = Get((k = OK_KEY, id = req.arg));
    if (!x.present) {
      return E_INVALID_ACCESS_KEY;
    }
    u = LoadById(OK_USER, x.obj.ref);
    if (u.code != E_OK) {
      return E_INVALID_ACCESS_KEY;
    }
    if (!(req.arg in u.info.akeys)) {
      return E_ACCESS_DENIED;
    }
    if (cfg.authChecksActive && !u.info.akeys[req.arg]) {
      return E_INVALID_ACCESS_KEY;
    }
    entity = u.id;
    return E_OK;
  }

  /*
   * STS (rgw_rest_sts.cc, rgw_sts.cc) and session tokens (STSEngine)
   */

  // RGWSTSAssumeRole: verify_permission reads the role and evaluates its
  // trust policy; execute (STSService::assumeRole) reads the role by name
  // again and issues the token for that read's role ID
  fun AssumeRole(): tCode {
    var caller: tLoaded;
    var l: tLoaded;
    caller = LoadUserByName(req.arg);
    if (caller.code != E_OK) {
      return E_INVALID_ACCESS_KEY;
    }
    l = LoadRoleByName(req.name);
    if (l.code != E_OK) {
      return StsMissing(l.code);  // ERR_NO_ROLE_FOUND
    }
    if (!Trusts(l.info.trust, caller)) {
      return E_ACCESS_DENIED;
    }
    if (!cfg.assumeRoleOneRead) {
      l = LoadRoleByName(req.name);
      if (l.code != E_OK) {
        return StsMissing(l.code);
      }
    }
    entity = l.id;
    issued = (present = true, roleId = l.id, user = caller.id, isRole = true, origin = l.id);
    return E_OK;
  }

  fun StsMissing(c: tCode): tCode {
    if (cfg.assumeDeniesMissing) {
      return E_ACCESS_DENIED;
    }
    return c;
  }

  // the same-account evaluation (evaluate_iam_policies): an Allow from the
  // trust policy for the user principal, or from the caller's identity
  // policies, is enough. AWS requires the trust policy to allow the caller,
  // through its account principal only together with an identity policy
  fun Trusts(trust: int, caller: tLoaded): bool {
    var identity: bool;
    identity = POL_ASSUME() in caller.info.policies;
    if (trust == caller.id) {
      return true;
    }
    if (cfg.trustRequired) {
      return trust == TRUST_ACCOUNT() && identity;
    }
    return identity;
  }

  // RGWSTSGetSessionToken: with an access key, or with session credentials.
  // A session token carries the role ID only from AssumeRole*: one issued to
  // a role session has acct_type TYPE_ROLE and no role ID
  fun GetSessionToken(): tCode {
    var caller: tLoaded;
    var c: tCode;
    if (req.arg2 == 0) {
      caller = LoadUserByName(req.arg);
      if (caller.code != E_OK) {
        return E_INVALID_ACCESS_KEY;
      }
      issued = (present = true, roleId = 0, user = caller.id, isRole = false, origin = 0);
      return E_OK;
    }
    c = AuthToken();
    if (c != E_OK) {
      return c;
    }
    if (cfg.gstLongTermOnly) {
      return E_ACCESS_DENIED;
    }
    issued = (present = true, roleId = 0, user = token.user, isRole = token.isRole, origin = token.origin);
    return E_OK;
  }

  // STSEngine::authenticate: a role session's role is read by ID if the
  // token names one; a user session's user is read. For GetSessionToken a
  // role session also needs a role policy that allows it
  fun AuthToken(): tCode {
    var l: tLoaded;
    if (!token.present) {
      return E_ACCESS_DENIED;
    }
    if (token.isRole) {
      if (token.roleId != 0) {
        l = LoadById(OK_ROLE, token.roleId);
        if (l.code != E_OK) {
          return E_ACCESS_DENIED;
        }
        if (req.kind == R_GET_SESSION_TOKEN && !(POL_ALL() in l.info.policies)) {
          return E_ACCESS_DENIED;
        }
      }
      return E_OK;
    }
    l = LoadById(OK_USER, token.user);
    if (l.code != E_OK) {
      return E_ACCESS_DENIED;
    }
    return E_OK;
  }

  fun UseToken(): tCode {
    entity = token.origin;
    return AuthToken();
  }

  /*
   * RADOS ops. Each first gives the RGW a chance to die; a dead RGW's ops
   * do nothing
   */

  fun Dies(): bool {
    if (!dead && cfg.mayCrash && $) {
      dead = true;
    }
    return dead;
  }

  fun Get(oid: tOid): (present: bool, obj: tObj) {
    var r: (present: bool, obj: tObj);
    if (Dies()) {
      return (present = false, obj = (ver = 0, ref = 0, info = NoInfo()));
    }
    send store, eGet, (from = this, oid = oid);
    receive {
      case eGot: (x: (present: bool, obj: tObj)) { r = x; }
    }
    return r;
  }

  fun Put(oid: tOid, excl: bool, ver: int, ref: int, info: tInfo): tRc {
    var rc: tRc;
    if (Dies()) {
      return ECANCELED;
    }
    send store, ePut, (from = this, rid = rid, oid = oid, excl = excl, ver = ver, ref = ref, info = info);
    receive {
      case eRc: (x: tRc) { rc = x; }
    }
    return rc;
  }

  fun Rm(oid: tOid, ver: int, ref: int): tRc {
    var rc: tRc;
    var o: (present: bool, obj: tObj);
    if (ref != 0 && cfg.existingOpsOnly) {
      // read the object, check what it names, remove it under its version
      o = Get(oid);
      if (!o.present) {
        return ENOENT;
      }
      if (o.obj.ref != ref || (ver != 0 && o.obj.ver != ver)) {
        return ECANCELED;
      }
      return Rm(oid, o.obj.ver, 0);
    }
    if (Dies()) {
      return ECANCELED;
    }
    send store, eRm, (from = this, rid = rid, oid = oid, ver = ver, ref = ref);
    receive {
      case eRc: (x: tRc) { rc = x; }
    }
    return rc;
  }

  fun IxAdd(ix: tIx, name: int, val: int, excl: bool, limit: int): tRc {
    var rc: tRc;
    if (Dies()) {
      return ECANCELED;
    }
    send store, eIxAdd, (from = this, rid = rid, ix = ix, name = name, val = val, excl = excl, limit = limit);
    receive {
      case eRc: (x: tRc) { rc = x; }
    }
    return rc;
  }

  fun IxRm(ix: tIx, name: int, val: int): tRc {
    var rc: tRc;
    var x: (present: bool, val: int);
    if (val != 0 && cfg.existingOpsOnly) {
      // cls_account_resource_rm takes only a name: read the entry, check
      // it, then remove it, with no guard in between
      x = IxGet(ix, name);
      if (!x.present) {
        return ENOENT;
      }
      if (x.val != val) {
        return ECANCELED;
      }
      return IxRm(ix, name, 0);
    }
    if (Dies()) {
      return ECANCELED;
    }
    send store, eIxRm, (from = this, rid = rid, ix = ix, name = name, val = val);
    receive {
      case eRc: (x: tRc) { rc = x; }
    }
    return rc;
  }

  fun IxDrop(ix: tIx): tRc {
    var rc: tRc;
    if (Dies()) {
      return ECANCELED;
    }
    send store, eIxDrop, (from = this, rid = rid, ix = ix);
    receive {
      case eRc: (x: tRc) { rc = x; }
    }
    return rc;
  }

  fun IxGet(ix: tIx, name: int): (present: bool, val: int) {
    var r: (present: bool, val: int);
    if (Dies()) {
      return (present = false, val = 0);
    }
    send store, eIxGet, (from = this, ix = ix, name = name);
    receive {
      case eIxGot: (x: (present: bool, val: int)) { r = x; }
    }
    return r;
  }

  fun IxCount(ix: tIx): int {
    var n: int;
    if (Dies()) {
      return 0;
    }
    send store, eIxCount, (from = this, ix = ix);
    receive {
      case eIxCounted: (x: int) { n = x; }
    }
    return n;
  }

  fun IxList(ix: tIx): map[int, int] {
    var m: map[int, int];
    if (Dies()) {
      return m;
    }
    send store, eIxList, (from = this, ix = ix);
    receive {
      case eIxListed: (x: map[int, int]) { m = x; }
    }
    return m;
  }
}
