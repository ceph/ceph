/*
 * RADOS semantics shared by the models' Store machines, as pure functions
 * over state values. A Store calls them from its handlers, so each of its
 * handlers stays one atomic op.
 *
 * A model that includes this file defines tOid, the key of its metadata
 * objects, and tObj, a metadata object with a field ver: its
 * RGWObjVersionTracker version.
 */

/*
 * Metadata objects (rgw_put_system_obj, rgw_delete_system_obj). An
 * exclusive create fails with EEXIST on an object that exists. A write or
 * removal with a tracker that read version ver carries
 * cls_version_check(EQ), and fails with ECANCELED on another version or on
 * no object; a tracker that read nothing (ver 0) adds no check. A removal
 * of no object fails with ENOENT. These return whether the op applies; the
 * Store applies it, with a fresh version for a write.
 */
fun SysPutRc(objs: map[tOid, tObj], oid: tOid, excl: bool, ver: int): tRc {
  if (excl && oid in objs) {
    return EEXIST;
  }
  if (!excl && ver != 0 && (!(oid in objs) || objs[oid].ver != ver)) {
    return ECANCELED;
  }
  return OK;
}

fun SysRmRc(objs: map[tOid, tObj], oid: tOid, ver: int): tRc {
  if (!(oid in objs)) {
    return ENOENT;
  }
  if (ver != 0 && objs[oid].ver != ver) {
    return ECANCELED;
  }
  return OK;
}

/*
 * cls_user account resource omaps (cls_account_resource_add, _rm, _get,
 * _list), as for an account's users, roles and groups, or a group's
 * members: entries by name, and a header that counts them. An add of a
 * name already present fails with EEXIST if exclusive, and otherwise
 * replaces the entry without counting it; a new entry fails with EUSERS
 * once the count reaches the limit. A removal of no entry fails with
 * ENOENT. A removal with val not 0 is a proposed op: it removes the entry
 * only while it maps to val, and otherwise fails with ECANCELED.
 */
type tOmap = (entries: map[int, int], count: int);

fun OmapAdd(m: tOmap, name: int, val: int, excl: bool, limit: int): (rc: tRc, m: tOmap) {
  if (name in m.entries) {
    if (excl) {
      return (rc = EEXIST, m = m);
    }
    m.entries[name] = val;
    return (rc = OK, m = m);
  }
  if (m.count >= limit) {
    return (rc = EUSERS, m = m);
  }
  m.entries[name] = val;
  m.count = m.count + 1;
  return (rc = OK, m = m);
}

fun OmapRm(m: tOmap, name: int, val: int): (rc: tRc, m: tOmap) {
  if (!(name in m.entries)) {
    return (rc = ENOENT, m = m);
  }
  if (val != 0 && m.entries[name] != val) {
    return (rc = ECANCELED, m = m);
  }
  m.entries -= (name);
  m.count = m.count - 1;
  return (rc = OK, m = m);
}
