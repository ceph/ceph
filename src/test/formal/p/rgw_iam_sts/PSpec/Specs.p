/*
 * The properties. The contract is AWS's Smithy models of IAM and STS
 * (aws/api-models-aws at 69ae840c: models/iam/service/2010-05-08,
 * models/sts/service/2011-06-15): each operation's errors, and what its
 * documentation says it does. A request is checked as if it ran at one
 * point between its start and its answer.
 */

fun CodeName(c: tCode): string {
  if (c == E_OK) { return "success"; }
  if (c == E_NO_SUCH_ENTITY) { return "NoSuchEntity"; }
  if (c == E_ENTITY_ALREADY_EXISTS) { return "EntityAlreadyExists"; }
  if (c == E_DELETE_CONFLICT) { return "DeleteConflict"; }
  if (c == E_LIMIT_EXCEEDED) { return "LimitExceeded"; }
  if (c == E_CONCURRENT_MODIFICATION) { return "ConcurrentModification"; }
  if (c == E_INVALID_INPUT) { return "InvalidInput"; }
  if (c == E_ACCESS_DENIED) { return "AccessDenied"; }
  if (c == E_INVALID_ACCESS_KEY) { return "InvalidAccessKeyId"; }
  return "ServiceFailure";
}
fun KindName(k: tKind): string {
  if (k == R_CREATE_ROLE) { return "CreateRole"; }
  if (k == R_DELETE_ROLE) { return "DeleteRole"; }
  if (k == R_PUT_ROLE_POLICY) { return "PutRolePolicy"; }
  if (k == R_DELETE_ROLE_POLICY) { return "DeleteRolePolicy"; }
  if (k == R_ATTACH_ROLE_POLICY) { return "AttachRolePolicy"; }
  if (k == R_UPDATE_TRUST) { return "UpdateAssumeRolePolicy"; }
  if (k == R_CREATE_USER) { return "CreateUser"; }
  if (k == R_DELETE_USER) { return "DeleteUser"; }
  if (k == R_RENAME_USER) { return "UpdateUser"; }
  if (k == R_CREATE_KEY) { return "CreateAccessKey"; }
  if (k == R_UPDATE_KEY) { return "UpdateAccessKey"; }
  if (k == R_DELETE_KEY) { return "DeleteAccessKey"; }
  if (k == R_PUT_USER_POLICY) { return "PutUserPolicy"; }
  if (k == R_DELETE_USER_POLICY) { return "DeleteUserPolicy"; }
  if (k == R_ATTACH_USER_POLICY) { return "AttachUserPolicy"; }
  if (k == R_CREATE_GROUP) { return "CreateGroup"; }
  if (k == R_DELETE_GROUP) { return "DeleteGroup"; }
  if (k == R_ADD_TO_GROUP) { return "AddUserToGroup"; }
  if (k == R_REMOVE_FROM_GROUP) { return "RemoveUserFromGroup"; }
  if (k == R_PUT_GROUP_POLICY) { return "PutGroupPolicy"; }
  if (k == R_AUTH) { return "an S3 request"; }
  if (k == R_ASSUME_ROLE) { return "AssumeRole"; }
  if (k == R_GET_SESSION_TOKEN) { return "GetSessionToken"; }
  return "an S3 request with session credentials";
}
fun OkName(k: tOk): string {
  if (k == OK_ROLE) { return "roles"; }
  if (k == OK_USER) { return "users"; }
  return "groups";
}

// what the state says of names and entities

// a role named n: its name object, and the info object it names
fun RoleByName(st: tState, n: int): int {
  var o: tOid;
  o = (k = OK_ROLE_NAME, id = n);
  if (o in st.objs && (k = OK_ROLE, id = st.objs[o].ref) in st.objs) {
    return st.objs[o].ref;
  }
  return 0;
}
// a user named n: the account's users entry, and the user object it names
fun UserByName(st: tState, n: int): int {
  var ix: tIx;
  ix = (k = IX_USERS, id = 0);
  if (ix in st.ixs && n in st.ixs[ix].entries && (k = OK_USER, id = st.ixs[ix].entries[n]) in st.objs) {
    return st.ixs[ix].entries[n];
  }
  return 0;
}
fun GroupByName(st: tState, n: int): int {
  var o: tOid;
  o = (k = OK_GROUP_NAME, id = n);
  if (o in st.objs && (k = OK_GROUP, id = st.objs[o].ref) in st.objs) {
    return st.objs[o].ref;
  }
  return 0;
}
fun ByName(st: tState, c: tClass, n: int): int {
  if (c == CL_ROLE) { return RoleByName(st, n); }
  if (c == CL_USER) { return UserByName(st, n); }
  return GroupByName(st, n);
}
fun InfoKind(c: tClass): tOk {
  if (c == CL_ROLE) { return OK_ROLE; }
  if (c == CL_USER) { return OK_USER; }
  return OK_GROUP;
}
fun IsDelete(k: tKind): bool {
  return k == R_DELETE_ROLE || k == R_DELETE_USER || k == R_DELETE_GROUP;
}
fun Count(st: tState, k: tOk): int {
  var o: tOid;
  var n: int;
  foreach (o in keys(st.objs)) {
    if (o.k == k) { n = n + 1; }
  }
  return n;
}

// DeleteRole, DeleteUser and DeleteGroup answer DeleteConflict while the
// entity has what must be removed first: a role its inline and attached
// policies; a user its access keys and policies; a group its policies and
// members. So a delete never removes an entity that has them
spec DeleteNeedsEmpty observes eLaunch, eRemoved {
  var kinds: map[int, tKind];

  start state Watch {
    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) { kinds[p.rid] = p.req.kind; }

    on eRemoved do (p: (rid: int, oid: tOid, obj: tObj, st: tState)) {
      var i: tInfo;
      var o: tOid;
      if (!(p.rid in kinds) || !IsDelete(kinds[p.rid])) {
        return;
      }
      i = p.obj.info;
      if (p.oid.k == OK_ROLE) {
        assert sizeof(i.policies) == 0 && sizeof(i.managed) == 0,
          format("DeleteRole {0} deleted role {1}, which has policies", p.rid, p.oid.id);
      } else if (p.oid.k == OK_USER) {
        assert sizeof(i.policies) == 0 && sizeof(i.managed) == 0,
          format("DeleteUser {0} deleted user {1}, which has policies", p.rid, p.oid.id);
        assert sizeof(i.akeys) == 0,
          format("DeleteUser {0} deleted user {1}, which has access keys", p.rid, p.oid.id);
      } else if (p.oid.k == OK_GROUP) {
        assert sizeof(i.policies) == 0 && sizeof(i.managed) == 0,
          format("DeleteGroup {0} deleted group {1}, which has policies", p.rid, p.oid.id);
        foreach (o in keys(p.st.objs)) {
          assert !(o.k == OK_USER && p.oid.id in p.st.objs[o].info.groups),
            format("DeleteGroup {0} deleted group {1}, which has member {2}", p.rid, p.oid.id, o.id);
        }
      }
    }
  }
}

// DeleteUser also answers DeleteConflict for a user in a group (the IAM
// API reference: "Removes the user from any groups" is a step the caller
// must take first)
spec DeleteUserNeedsNoGroups observes eLaunch, eRemoved {
  var kinds: map[int, tKind];

  start state Watch {
    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) { kinds[p.rid] = p.req.kind; }

    on eRemoved do (p: (rid: int, oid: tOid, obj: tObj, st: tState)) {
      if (p.rid in kinds && kinds[p.rid] == R_DELETE_USER && p.oid.k == OK_USER) {
        assert sizeof(p.obj.info.groups) == 0,
          format("DeleteUser {0} deleted user {1}, which is in group(s) {2}", p.rid, p.oid.id, p.obj.info.groups);
      }
    }
  }
}

// a delete answered success removed the entity it acted on
spec DeletesTakeEffect observes eState, eAnswer {
  var st: tState;

  start state Watch {
    on eState do (p: (rid: int, st: tState)) { st = p.st; }

    on eAnswer do (a: tAnswer) {
      if (IsDelete(a.kind) && a.code == E_OK) {
        assert !((k = InfoKind(ClassOf(a.kind)), id = a.entity) in st.objs),
          format("{0} {1} answered success, but entity {2} remains: {3}",
                 KindName(a.kind), a.rid, a.entity, st.objs[(k = InfoKind(ClassOf(a.kind)), id = a.entity)].info);
      }
    }
  }
}

// once every request is done, the indexes match the info objects: each
// entity has its name object or account entry, and they name nothing else;
// the account indexes count their entries; each active access key, and
// only those, is in the key index; a group's member entries are its users'
// memberships, and a user is only in groups that exist
spec IndexesMatch observes eFinal {
  start state Watch {
    on eFinal do (st: tState) {
      var o: tOid;
      var ix: tIx;
      var n: int;
      var k: int;
      var g: int;
      var i: tInfo;
      foreach (ix in keys(st.ixs)) {
        assert st.ixs[ix].count == sizeof(st.ixs[ix].entries),
          format("index {0} counts {1} entries but holds {2}", ix, st.ixs[ix].count, sizeof(st.ixs[ix].entries));
      }
      foreach (o in keys(st.objs)) {
        i = st.objs[o].info;
        if (o.k == OK_ROLE_NAME) {
          assert RoleByName(st, o.id) != 0 && st.objs[(k = OK_ROLE, id = st.objs[o].ref)].info.name == o.id,
            format("role name object {0} names role {1}, which is gone", o.id, st.objs[o].ref);
        } else if (o.k == OK_ROLE) {
          assert RoleByName(st, i.name) == o.id,
            format("role {0} is not reachable by its name {1}", o.id, i.name);
          assert (k = IX_ROLES, id = 0) in st.ixs && i.name in st.ixs[(k = IX_ROLES, id = 0)].entries &&
                 st.ixs[(k = IX_ROLES, id = 0)].entries[i.name] == o.id,
            format("role {0} has no account entry", o.id);
        } else if (o.k == OK_USER) {
          assert UserByName(st, i.name) == o.id,
            format("user {0} is not reachable by its name {1}", o.id, i.name);
          foreach (k in keys(i.akeys)) {
            assert !i.akeys[k] || ((k = OK_KEY, id = k) in st.objs && st.objs[(k = OK_KEY, id = k)].ref == o.id),
              format("user {0}'s active key {1} is not in the key index", o.id, k);
          }
          foreach (g in i.groups) {
            assert (k = OK_GROUP, id = g) in st.objs,
              format("user {0} is in group {1}, which is gone", o.id, g);
            assert (k = IX_MEMBERS, id = g) in st.ixs && i.name in st.ixs[(k = IX_MEMBERS, id = g)].entries &&
                   st.ixs[(k = IX_MEMBERS, id = g)].entries[i.name] == o.id,
              format("user {0} is in group {1}, which does not list it", o.id, g);
          }
        } else if (o.k == OK_KEY) {
          assert (k = OK_USER, id = st.objs[o].ref) in st.objs &&
                 o.id in st.objs[(k = OK_USER, id = st.objs[o].ref)].info.akeys &&
                 st.objs[(k = OK_USER, id = st.objs[o].ref)].info.akeys[o.id],
            format("the key index lists key {0}, which is not an active key of user {1}", o.id, st.objs[o].ref);
        } else if (o.k == OK_GROUP_NAME) {
          assert GroupByName(st, o.id) != 0 && st.objs[(k = OK_GROUP, id = st.objs[o].ref)].info.name == o.id,
            format("group name object {0} names group {1}, which is gone", o.id, st.objs[o].ref);
        } else if (o.k == OK_GROUP) {
          assert GroupByName(st, i.name) == o.id,
            format("group {0} is not reachable by its name {1}", o.id, i.name);
          assert (k = IX_GROUPS, id = 0) in st.ixs && i.name in st.ixs[(k = IX_GROUPS, id = 0)].entries &&
                 st.ixs[(k = IX_GROUPS, id = 0)].entries[i.name] == o.id,
            format("group {0} has no account entry, or one naming another group", o.id);
        }
      }
      foreach (ix in keys(st.ixs)) {
        foreach (n in keys(st.ixs[ix].entries)) {
          k = st.ixs[ix].entries[n];
          if (ix.k == IX_USERS) {
            assert (k = OK_USER, id = k) in st.objs && st.objs[(k = OK_USER, id = k)].info.name == n,
              format("the account's users entry {0} names user {1}, which is gone or renamed", n, k);
          } else if (ix.k == IX_ROLES) {
            assert (k = OK_ROLE, id = k) in st.objs,
              format("the account's roles entry {0} names role {1}, which is gone", n, k);
          } else if (ix.k == IX_GROUPS) {
            assert (k = OK_GROUP, id = k) in st.objs,
              format("the account's groups entry {0} names group {1}, which is gone", n, k);
          } else {
            assert (k = OK_USER, id = k) in st.objs && ix.id in st.objs[(k = OK_USER, id = k)].info.groups &&
                   st.objs[(k = OK_USER, id = k)].info.name == n,
              format("group {0} lists member {1} (user {2}), which is not in the group", ix.id, n, k);
          }
        }
      }
    }
  }
}

// the account's limits on users, roles and groups, and on each user's
// access keys (LimitExceeded), hold
spec LimitsHold observes eConfig, eState {
  var cfg: tCfg;

  start state Watch {
    on eConfig do (c: tCfg) { cfg = c; }

    on eState do (p: (rid: int, st: tState)) {
      var o: tOid;
      assert cfg.maxRoles < 0 || Count(p.st, OK_ROLE) <= cfg.maxRoles,
        format("the account has {0} roles, over its limit of {1}", Count(p.st, OK_ROLE), cfg.maxRoles);
      assert cfg.maxUsers < 0 || Count(p.st, OK_USER) <= cfg.maxUsers,
        format("the account has {0} users, over its limit of {1}", Count(p.st, OK_USER), cfg.maxUsers);
      assert cfg.maxGroups < 0 || Count(p.st, OK_GROUP) <= cfg.maxGroups,
        format("the account has {0} groups, over its limit of {1}", Count(p.st, OK_GROUP), cfg.maxGroups);
      foreach (o in keys(p.st.objs)) {
        assert o.k != OK_USER || sizeof(p.st.objs[o].info.akeys) <= cfg.maxKeys,
          format("user {0} has {1} access keys, over the limit of {2}", o.id, sizeof(p.st.objs[o].info.akeys), cfg.maxKeys);
      }
    }
  }
}

// no two entities of a class share a name ("Names are not distinguished by
// case"; CreateUser, CreateGroup and UpdateUser answer EntityAlreadyExists)
spec NamesUnique observes eState {
  start state Watch {
    on eState do (p: (rid: int, st: tState)) {
      var a: tOid;
      var b: tOid;
      foreach (a in keys(p.st.objs)) {
        foreach (b in keys(p.st.objs)) {
          if (a.k == b.k && a.id < b.id && (a.k == OK_USER || a.k == OK_ROLE || a.k == OK_GROUP)) {
            assert p.st.objs[a].info.name != p.st.objs[b].info.name,
              format("{0} {1} and {2} share the name {3}", OkName(a.k), a.id, b.id, p.st.objs[a].info.name);
          }
        }
      }
    }
  }
}

// an addition answered success (an inline policy, a managed policy, an
// access key, a group membership) is not lost: at the end it is still
// there, unless a request removed it or the entity is gone
spec NoLostUpdate observes eLaunch, eState, eAnswer, eFinal {
  var reqs: map[int, tReq];
  var st: tState;
  var added: map[int, (oid: tOid, what: int, item: int)];
  var removed: set[(oid: tOid, what: int, item: int)];

  start state Watch {
    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) { reqs[p.rid] = p.req; }
    on eState do (p: (rid: int, st: tState)) { st = p.st; }

    on eAnswer do (a: tAnswer) {
      var r: tReq;
      var c: tClass;
      r = reqs[a.rid];
      c = ClassOf(a.kind);
      if (a.code != E_OK) {
        return;
      }
      if (a.kind == R_PUT_ROLE_POLICY || a.kind == R_PUT_USER_POLICY || a.kind == R_PUT_GROUP_POLICY) {
        added[a.rid] = (oid = (k = InfoKind(c), id = a.entity), what = 0, item = r.arg);
      } else if (a.kind == R_ATTACH_ROLE_POLICY || a.kind == R_ATTACH_USER_POLICY) {
        added[a.rid] = (oid = (k = InfoKind(c), id = a.entity), what = 1, item = r.arg);
      } else if (a.kind == R_CREATE_KEY) {
        added[a.rid] = (oid = (k = OK_USER, id = a.entity), what = 2, item = a.rid);
      } else if (a.kind == R_ADD_TO_GROUP) {
        added[a.rid] = (oid = (k = OK_USER, id = a.entity), what = 3, item = GroupByName(st, r.name));
      } else if (a.kind == R_DELETE_ROLE_POLICY || a.kind == R_DELETE_USER_POLICY) {
        removed += ((oid = (k = InfoKind(c), id = a.entity), what = 0, item = r.arg));
      } else if (a.kind == R_DELETE_KEY) {
        removed += ((oid = (k = OK_USER, id = a.entity), what = 2, item = r.arg));
      } else if (a.kind == R_REMOVE_FROM_GROUP) {
        removed += ((oid = (k = OK_USER, id = a.entity), what = 3, item = GroupByName(st, r.name)));
      }
    }

    on eFinal do (fin: tState) {
      var rid: int;
      var x: (oid: tOid, what: int, item: int);
      var i: tInfo;
      foreach (rid in keys(added)) {
        x = added[rid];
        if (x in removed || !(x.oid in fin.objs)) {
          continue;
        }
        i = fin.objs[x.oid].info;
        if (x.what == 0) {
          assert x.item in i.policies, format("request {0}'s policy {1} on {2} is lost", rid, x.item, x.oid);
        } else if (x.what == 1) {
          assert x.item in i.managed, format("request {0}'s managed policy {1} on {2} is lost", rid, x.item, x.oid);
        } else if (x.what == 2) {
          assert x.item in i.akeys, format("request {0}'s access key on {1} is lost", rid, x.oid);
        } else {
          assert x.item in i.groups, format("request {0}'s membership of {1} for {2} is lost", rid, x.item, x.oid);
        }
      }
    }
  }
}

// an S3 request is authenticated only with an access key that is active
// (UpdateAccessKey: "Inactive ... cannot be used for API calls")
spec KeysHonored observes eLaunch, eState, eAnswer {
  var reqs: map[int, tReq];
  var st: tState;

  start state Watch {
    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) { reqs[p.rid] = p.req; }
    on eState do (p: (rid: int, st: tState)) { st = p.st; }

    on eAnswer do (a: tAnswer) {
      var key: int;
      var u: tOid;
      if (a.kind != R_AUTH || a.code != E_OK) {
        return;
      }
      key = reqs[a.rid].arg;
      u = (k = OK_USER, id = a.entity);
      assert u in st.objs && key in st.objs[u].info.akeys && st.objs[u].info.akeys[key],
        format("request {0} was authenticated with access key {1}, which user {2} does not hold active", a.rid, key, a.entity);
    }
  }
}

// AssumeRole issues credentials for a role whose trust policy allowed the
// caller at some point while it ran. As AWS evaluates it, the trust policy
// must allow the caller: naming the user, or the account together with an
// identity policy that allows sts:AssumeRole
fun AwsTrusts(st: tState, caller: int, trust: int): bool {
  var u: tOid;
  if (caller == 0) {
    return false;
  }
  if (trust == caller) {
    return true;
  }
  u = (k = OK_USER, id = caller);
  return trust == TRUST_ACCOUNT() && u in st.objs && POL_ASSUME() in st.objs[u].info.policies;
}

spec TrustHonored observes eLaunch, eState, eAnswer {
  var st: tState;
  var callers: map[int, int];        // in-flight AssumeRole: the caller's user name
  var allowed: map[int, set[int]];   // the roles whose trust allowed the caller so far

  start state Watch {
    on eState do (p: (rid: int, st: tState)) {
      st = p.st;
      Note();
    }

    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) {
      if (p.req.kind == R_ASSUME_ROLE) {
        callers[p.rid] = p.req.arg;
        allowed[p.rid] = default(set[int]);
        Note();
      }
    }

    on eAnswer do (a: tAnswer) {
      if (a.kind == R_ASSUME_ROLE && a.code == E_OK) {
        assert a.token.roleId in allowed[a.rid],
          format("AssumeRole {0} issued credentials for role {1}, whose trust policy did not allow the caller while it ran",
                 a.rid, a.token.roleId);
      }
      if (a.rid in callers) {
        callers -= (a.rid);
      }
    }
  }

  fun Note() {
    var rid: int;
    var o: tOid;
    var caller: int;
    foreach (rid in keys(callers)) {
      caller = UserByName(st, callers[rid]);
      foreach (o in keys(st.objs)) {
        if (o.k == OK_ROLE && AwsTrusts(st, caller, st.objs[o].info.trust)) {
          allowed[rid] += (o.id);
        }
      }
    }
  }
}

// a role session's credentials stop working once the role is deleted, and
// so do credentials obtained with them: a request authenticated with them
// found the role at some point while it ran
spec SessionsDieWithRole observes eLaunch, eState, eAnswer {
  var st: tState;
  var origins: map[int, int];   // in-flight requests with session credentials: their role
  var found: set[int];          // those that found it

  start state Watch {
    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) {
      if (p.req.kind == R_USE_TOKEN && p.input.origin != 0) {
        origins[p.rid] = p.input.origin;
        if ((k = OK_ROLE, id = p.input.origin) in st.objs) {
          found += (p.rid);
        }
      }
    }

    on eState do (p: (rid: int, st: tState)) {
      var rid: int;
      st = p.st;
      foreach (rid in keys(origins)) {
        if ((k = OK_ROLE, id = origins[rid]) in st.objs) {
          found += (rid);
        }
      }
    }

    on eAnswer do (a: tAnswer) {
      if (a.kind == R_USE_TOKEN && a.code == E_OK && a.entity != 0) {
        assert a.rid in found || (k = OK_ROLE, id = a.entity) in st.objs,
          format("request {0} was authenticated with a session of role {1}, which is deleted", a.rid, a.entity);
      }
      if (a.rid in origins) {
        origins -= (a.rid);
      }
    }
  }
}

// the errors each operation's Smithy model lists. Any request may also be
// answered AccessDenied, or InvalidClientTokenId for unknown credentials
fun Allowed(k: tKind): set[tCode] {
  var s: set[tCode];
  s += (E_ACCESS_DENIED);
  s += (E_INVALID_ACCESS_KEY);
  s += (E_SERVICE_FAILURE);
  if (k == R_CREATE_ROLE) {
    s += (E_CONCURRENT_MODIFICATION); s += (E_ENTITY_ALREADY_EXISTS); s += (E_INVALID_INPUT);
    s += (E_LIMIT_EXCEEDED);
  } else if (k == R_DELETE_ROLE || k == R_DELETE_USER) {
    s += (E_CONCURRENT_MODIFICATION); s += (E_DELETE_CONFLICT); s += (E_LIMIT_EXCEEDED);
    s += (E_NO_SUCH_ENTITY);
  } else if (k == R_PUT_ROLE_POLICY || k == R_UPDATE_TRUST || k == R_DELETE_ROLE_POLICY ||
             k == R_PUT_USER_POLICY || k == R_DELETE_USER_POLICY || k == R_PUT_GROUP_POLICY ||
             k == R_CREATE_KEY || k == R_DELETE_KEY || k == R_ADD_TO_GROUP || k == R_REMOVE_FROM_GROUP) {
    s += (E_LIMIT_EXCEEDED); s += (E_NO_SUCH_ENTITY);
  } else if (k == R_ATTACH_ROLE_POLICY || k == R_ATTACH_USER_POLICY || k == R_UPDATE_KEY) {
    s += (E_INVALID_INPUT); s += (E_LIMIT_EXCEEDED); s += (E_NO_SUCH_ENTITY);
  } else if (k == R_CREATE_USER) {
    s += (E_CONCURRENT_MODIFICATION); s += (E_ENTITY_ALREADY_EXISTS); s += (E_INVALID_INPUT);
    s += (E_LIMIT_EXCEEDED); s += (E_NO_SUCH_ENTITY);
  } else if (k == R_RENAME_USER) {
    s += (E_CONCURRENT_MODIFICATION); s += (E_ENTITY_ALREADY_EXISTS); s += (E_LIMIT_EXCEEDED);
    s += (E_NO_SUCH_ENTITY);
  } else if (k == R_CREATE_GROUP) {
    s += (E_ENTITY_ALREADY_EXISTS); s += (E_LIMIT_EXCEEDED); s += (E_NO_SUCH_ENTITY);
  } else if (k == R_DELETE_GROUP) {
    s += (E_DELETE_CONFLICT); s += (E_LIMIT_EXCEEDED); s += (E_NO_SUCH_ENTITY);
  }
  // AssumeRole and GetSessionToken list no error the model can reach: a
  // missing or untrusting role is AccessDenied
  return s;
}

// each request is answered as its operation's contract allows: an error
// its model lists; EntityAlreadyExists only while an entity of the name
// existed, and NoSuchEntity for a name only while none did; a create's
// success only while none did. GetSessionToken must be called with
// long-term credentials
spec IamAnswers observes eLaunch, eState, eAnswer {
  var reqs: map[int, tReq];
  var st: tState;
  var sawName: map[int, bool];     // the name the request is about was taken at some point
  var sawNoName: map[int, bool];   // and free at some point

  start state Watch {
    on eLaunch do (p: (rid: int, req: tReq, input: tToken)) {
      reqs[p.rid] = p.req;
      sawName[p.rid] = false;
      sawNoName[p.rid] = false;
      NoteOne(p.rid);
    }

    on eState do (p: (rid: int, st: tState)) {
      var rid: int;
      st = p.st;
      foreach (rid in keys(sawName)) {
        NoteOne(rid);
      }
    }

    on eAnswer do (a: tAnswer) {
      var r: tReq;
      r = reqs[a.rid];
      if (r.kind == R_AUTH || r.kind == R_USE_TOKEN) {
        Forget(a.rid);
        return;
      }
      assert a.code == E_OK || a.code in Allowed(a.kind),
        format("{0} {1} was answered {2}, which its operation does not list", KindName(a.kind), a.rid, CodeName(a.code));
      if (a.code == E_ENTITY_ALREADY_EXISTS) {
        assert sawName[a.rid],
          format("{0} {1} was answered EntityAlreadyExists, but nothing had or was creating the name {2} while it ran",
                 KindName(a.kind), a.rid, NameOf(r));
      }
      if (a.code == E_NO_SUCH_ENTITY && NamesTarget(r.kind)) {
        assert sawNoName[a.rid],
          format("{0} {1} was answered NoSuchEntity, but {2} existed all the while", KindName(a.kind), a.rid, NameOf(r));
      }
      if (a.code == E_OK && (r.kind == R_CREATE_ROLE || r.kind == R_CREATE_USER || r.kind == R_CREATE_GROUP)) {
        assert sawNoName[a.rid],
          format("{0} {1} succeeded, but the name {2} was taken all the while", KindName(a.kind), a.rid, r.name);
      }
      if (r.kind == R_GET_SESSION_TOKEN && r.arg2 != 0) {
        assert a.code != E_OK,
          format("GetSessionToken {0} accepted temporary credentials", a.rid);
      }
      Forget(a.rid);
    }

  }

  fun Forget(rid: int) {
    sawName -= (rid);
    sawNoName -= (rid);
  }

  // the name a request's EntityAlreadyExists or NoSuchEntity is about
  fun NameOf(r: tReq): int {
    if (r.kind == R_RENAME_USER) {
      return r.arg;
    }
    return r.name;
  }

  // the requests whose only NoSuchEntity is for the name they address
  fun NamesTarget(k: tKind): bool {
    return k == R_DELETE_ROLE || k == R_PUT_ROLE_POLICY || k == R_ATTACH_ROLE_POLICY || k == R_UPDATE_TRUST ||
           k == R_DELETE_USER || k == R_PUT_USER_POLICY || k == R_ATTACH_USER_POLICY || k == R_CREATE_KEY ||
           k == R_DELETE_GROUP || k == R_PUT_GROUP_POLICY;
  }

  // a create, delete or rename of the name in flight may hold it: a
  // concurrent request that sees its name object or entry may be ordered
  // after the create or rename, or before the delete
  fun NoteOne(rid: int) {
    var r: tReq;
    var c: tClass;
    var o: int;
    r = reqs[rid];
    c = ClassOf(r.kind);
    if (c == CL_NONE) {
      return;
    }
    if (ByName(st, c, NameOf(r)) != 0) {
      sawName[rid] = true;
    } else {
      sawNoName[rid] = true;
    }
    foreach (o in keys(sawName)) {
      if (o != rid && ClassOf(reqs[o].kind) == c && (NameOf(reqs[o]) == NameOf(r) || reqs[o].name == NameOf(r)) &&
          (reqs[o].kind == R_CREATE_ROLE || reqs[o].kind == R_CREATE_USER || reqs[o].kind == R_CREATE_GROUP ||
           reqs[o].kind == R_RENAME_USER || IsDelete(reqs[o].kind))) {
        sawName[rid] = true;
      }
    }
  }
}
