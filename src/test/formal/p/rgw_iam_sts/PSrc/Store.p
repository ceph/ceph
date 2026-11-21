/*
 * The RADOS state. Each handler is one atomic op, with the semantics of
 * common/Rados.p: metadata objects written and removed under their
 * RGWObjVersionTracker, or created exclusively, and the cls_user account
 * resource omaps. A conditional removal of a name object (ref not 0) is a
 * proposed op: it removes the object only while it names the expected ID,
 * as a cmpxattr guard could.
 */
machine Store {
  var st: tState;
  var clock: int;

  start state Serve {
    entry (p: (cfg: tCfg, init: tInit)) {
      st = p.init;
      clock = 1000;
    }

    on eGet do (p: (from: machine, oid: tOid)) {
      if (p.oid in st.objs) {
        send p.from, eGot, (present = true, obj = st.objs[p.oid]);
      } else {
        send p.from, eGot, (present = false, obj = (ver = 0, ref = 0, info = NoInfo()));
      }
    }

    on ePut do (p: (from: machine, rid: int, oid: tOid, excl: bool, ver: int, ref: int, info: tInfo)) {
      var rc: tRc;
      rc = SysPutRc(st.objs, p.oid, p.excl, p.ver);
      if (rc == OK) {
        clock = clock + 1;
        st.objs[p.oid] = (ver = clock, ref = p.ref, info = p.info);
        announce eState, (rid = p.rid, st = st);
      }
      send p.from, eRc, rc;
    }

    on eRm do (p: (from: machine, rid: int, oid: tOid, ver: int, ref: int)) {
      var rc: tRc;
      var old: tObj;
      rc = SysRmRc(st.objs, p.oid, p.ver);
      if (rc == OK && p.ref != 0 && st.objs[p.oid].ref != p.ref) {
        rc = ECANCELED;
      }
      if (rc == OK) {
        old = st.objs[p.oid];
        st.objs -= (p.oid);
        announce eRemoved, (rid = p.rid, oid = p.oid, obj = old, st = st);
        announce eState, (rid = p.rid, st = st);
      }
      send p.from, eRc, rc;
    }

    on eIxAdd do (p: (from: machine, rid: int, ix: tIx, name: int, val: int, excl: bool, limit: int)) {
      var r: (rc: tRc, m: tOmap);
      r = OmapAdd(Omap(p.ix), p.name, p.val, p.excl, p.limit);
      if (r.rc == OK) {
        st.ixs[p.ix] = r.m;
        announce eState, (rid = p.rid, st = st);
      }
      send p.from, eRc, r.rc;
    }

    on eIxRm do (p: (from: machine, rid: int, ix: tIx, name: int, val: int)) {
      var r: (rc: tRc, m: tOmap);
      r = OmapRm(Omap(p.ix), p.name, p.val);
      if (r.rc == OK) {
        st.ixs[p.ix] = r.m;
        announce eState, (rid = p.rid, st = st);
      }
      send p.from, eRc, r.rc;
    }

    // the removal of a whole omap object
    on eIxDrop do (p: (from: machine, rid: int, ix: tIx)) {
      if (!(p.ix in st.ixs)) {
        send p.from, eRc, ENOENT;
        return;
      }
      st.ixs -= (p.ix);
      announce eState, (rid = p.rid, st = st);
      send p.from, eRc, OK;
    }

    on eIxGet do (p: (from: machine, ix: tIx, name: int)) {
      if (p.name in Omap(p.ix).entries) {
        send p.from, eIxGot, (present = true, val = st.ixs[p.ix].entries[p.name]);
      } else {
        send p.from, eIxGot, (present = false, val = 0);
      }
    }

    on eIxCount do (p: (from: machine, ix: tIx)) {
      send p.from, eIxCounted, Omap(p.ix).count;
    }

    on eIxList do (p: (from: machine, ix: tIx)) {
      send p.from, eIxListed, Omap(p.ix).entries;
    }

    on eQuiesce do (d: machine) {
      announce eFinal, st;
      send d, eQuiesced;
    }
  }

  fun Omap(ix: tIx): tOmap {
    if (ix in st.ixs) {
      return st.ixs[ix];
    }
    return default(tOmap);
  }
}
