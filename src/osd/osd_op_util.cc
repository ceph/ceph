// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/osd_op_util.h"

#include "osd/ClassHandler.h"
#include "messages/MOSDOp.h"

using std::ostream;
using std::string;
using std::vector;

using ceph::bufferlist;

bool OpInfo::check_rmw(int flag) const {
  ceph_assert(rmw_flags != 0);
  return rmw_flags & flag;
}
// Returns true if op performs a read (including of the object_info).
bool OpInfo::may_read() const {
  return need_read_cap() || check_rmw(CEPH_OSD_RMW_FLAG_CLASS_READ);
}
bool OpInfo::may_write() const {
  return need_write_cap() || check_rmw(CEPH_OSD_RMW_FLAG_CLASS_WRITE);
}
bool OpInfo::may_cache() const { return check_rmw(CEPH_OSD_RMW_FLAG_CACHE); }
bool OpInfo::rwordered_forced() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_RWORDERED);
}
bool OpInfo::rwordered() const {
  return may_write() || may_cache() || rwordered_forced();
}

bool OpInfo::includes_pg_op() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_PGOP);
}
bool OpInfo::need_read_cap() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_READ);
}
bool OpInfo::need_write_cap() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_WRITE);
}
bool OpInfo::need_promote() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_FORCE_PROMOTE);
}
bool OpInfo::need_skip_handle_cache() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_SKIP_HANDLE_CACHE);
}
bool OpInfo::need_skip_promote() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_SKIP_PROMOTE);
}
bool OpInfo::allows_returnvec() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_RETURNVEC);
}
/**
 * may_read_data()
 * 
 * Returns true if op reads information other than the object_info. Requires that the
 * osd flush any prior writes prior to servicing this op. Includes any information not
 * cached by the osd in the object_info or snapset.
 */
bool OpInfo::may_read_data() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_READ_DATA);
}

void OpInfo::set_rmw_flags(int flags) {
  rmw_flags |= flags;
}

void OpInfo::set_read() { set_rmw_flags(CEPH_OSD_RMW_FLAG_READ); }
void OpInfo::set_write() { set_rmw_flags(CEPH_OSD_RMW_FLAG_WRITE); }
void OpInfo::set_class_read() { set_rmw_flags(CEPH_OSD_RMW_FLAG_CLASS_READ); }
void OpInfo::set_class_write() { set_rmw_flags(CEPH_OSD_RMW_FLAG_CLASS_WRITE); }
void OpInfo::set_pg_op() { set_rmw_flags(CEPH_OSD_RMW_FLAG_PGOP); }
void OpInfo::set_cache() { set_rmw_flags(CEPH_OSD_RMW_FLAG_CACHE); }
void OpInfo::set_promote() { set_rmw_flags(CEPH_OSD_RMW_FLAG_FORCE_PROMOTE); }
void OpInfo::set_skip_handle_cache() { set_rmw_flags(CEPH_OSD_RMW_FLAG_SKIP_HANDLE_CACHE); }
void OpInfo::set_skip_promote() { set_rmw_flags(CEPH_OSD_RMW_FLAG_SKIP_PROMOTE); }
void OpInfo::set_force_rwordered() { set_rmw_flags(CEPH_OSD_RMW_FLAG_RWORDERED); }
void OpInfo::set_returnvec() { set_rmw_flags(CEPH_OSD_RMW_FLAG_RETURNVEC); }
void OpInfo::set_read_data() { set_rmw_flags(CEPH_OSD_RMW_FLAG_READ_DATA); }


int OpInfo::set_from_op(
  const MOSDOp *m,
  const OSDMap &osdmap)
{
  // client flags have no bearing on whether an op is a read, write, etc.
  clear();

  if (m->has_flag(CEPH_OSD_FLAG_RWORDERED)) {
    set_force_rwordered();
  }
  if (m->has_flag(CEPH_OSD_FLAG_RETURNVEC)) {
    set_returnvec();
  }
  return set_from_op(m->ops, m->get_pg(), osdmap);
}

int OpInfo::set_from_op(
  const std::vector<OSDOp>& ops,
  const pg_t& pg,
  const OSDMap &osdmap)
{
  vector<OSDOp>::const_iterator iter;

  // set bits based on op codes, called methods.
  for (iter = ops.begin(); iter != ops.end(); ++iter) {
    if ((iter->op.op == CEPH_OSD_OP_WATCH &&
	 iter->op.watch.op == CEPH_OSD_WATCH_OP_PING)) {
      /* This a bit odd.  PING isn't actually a write.  It can't
       * result in an update to the object_info.  PINGs also aren't
       * resent, so there's no reason to write out a log entry.
       *
       * However, we pipeline them behind writes, so let's force
       * the write_ordered flag.
       */
      set_force_rwordered();
    } else {
      if (ceph_osd_op_mode_modify(iter->op.op))
	set_write();
    }
    if (ceph_osd_op_mode_read(iter->op.op)) {
      set_read();
      if (iter->op.op != CEPH_OSD_OP_STAT) {
        set_read_data();
      }
    }

    // set PGOP flag if there are PG ops
    if (ceph_osd_op_type_pg(iter->op.op))
      set_pg_op();

    if (ceph_osd_op_mode_cache(iter->op.op))
      set_cache();

    // check for ec base pool
    int64_t poolid = pg.pool();
    const pg_pool_t *pool = osdmap.get_pg_pool(poolid);
    if (pool && pool->is_tier()) {
      const pg_pool_t *base_pool = osdmap.get_pg_pool(pool->tier_of);
      if (base_pool && base_pool->require_rollback()) {
        if ((iter->op.op != CEPH_OSD_OP_READ) &&
            (iter->op.op != CEPH_OSD_OP_CHECKSUM) &&
            (iter->op.op != CEPH_OSD_OP_CMPEXT) &&
            (iter->op.op != CEPH_OSD_OP_STAT) &&
            (iter->op.op != CEPH_OSD_OP_ISDIRTY) &&
            (iter->op.op != CEPH_OSD_OP_UNDIRTY) &&
            (iter->op.op != CEPH_OSD_OP_GETXATTR) &&
            (iter->op.op != CEPH_OSD_OP_GETXATTRS) &&
            (iter->op.op != CEPH_OSD_OP_CMPXATTR) &&
            (iter->op.op != CEPH_OSD_OP_ASSERT_VER) &&
            (iter->op.op != CEPH_OSD_OP_LIST_WATCHERS) &&
            (iter->op.op != CEPH_OSD_OP_LIST_SNAPS) &&
            (iter->op.op != CEPH_OSD_OP_SETALLOCHINT) &&
            (iter->op.op != CEPH_OSD_OP_WRITEFULL) &&
            (iter->op.op != CEPH_OSD_OP_ROLLBACK) &&
            (iter->op.op != CEPH_OSD_OP_CREATE) &&
            (iter->op.op != CEPH_OSD_OP_DELETE) &&
            (iter->op.op != CEPH_OSD_OP_SETXATTR) &&
            (iter->op.op != CEPH_OSD_OP_RMXATTR) &&
            (iter->op.op != CEPH_OSD_OP_STARTSYNC) &&
            (iter->op.op != CEPH_OSD_OP_COPY_GET) &&
            (iter->op.op != CEPH_OSD_OP_COPY_FROM) &&
            (iter->op.op != CEPH_OSD_OP_COPY_FROM2)) {
          set_promote();
        }
      }
    }

    switch (iter->op.op) {
    case CEPH_OSD_OP_CALL:
      {
	bufferlist::iterator bp = const_cast<bufferlist&>(iter->indata).begin();
	int is_write, is_read;
	string cname, mname;
	bp.copy(iter->op.cls.class_len, cname);
	bp.copy(iter->op.cls.method_len, mname);

	ClassHandler::ClassData *cls;
	int r = ClassHandler::get_instance().open_class(cname, &cls);
	if (r) {
	  if (r == -ENOENT)
	    r = -EOPNOTSUPP;
	  else if (r != -EPERM) // propagate permission errors
	    r = -EIO;
	  return r;
	}
	int flags = cls->get_method_flags(mname);
	if (flags < 0) {
	  if (flags == -ENOENT)
	    r = -EOPNOTSUPP;
	  else
	    r = flags;
	  return r;
	}
	is_read = flags & CLS_METHOD_RD;
	is_write = flags & CLS_METHOD_WR;
        bool is_promote = flags & CLS_METHOD_PROMOTE;

	if (is_read)
	  set_class_read();
	if (is_write)
	  set_class_write();
        if (is_promote)
          set_promote();
        add_class(std::move(cname), std::move(mname), is_read, is_write,
                      cls->allowed);
	break;
      }

    case CEPH_OSD_OP_WATCH:
      // force the read bit for watch since it is depends on previous
      // watch state (and may return early if the watch exists) or, in
      // the case of ping, is simply a read op.
      set_read();
      set_read_data();
      // fall through
    case CEPH_OSD_OP_NOTIFY:
    case CEPH_OSD_OP_NOTIFY_ACK:
      {
        set_promote();
        break;
      }

    case CEPH_OSD_OP_DELETE:
      // if we get a delete with FAILOK we can skip handle cache. without
      // FAILOK we still need to promote (or do something smarter) to
      // determine whether to return ENOENT or 0.
      if (iter == ops.begin() &&
	  iter->op.flags == CEPH_OSD_OP_FLAG_FAILOK) {
	set_skip_handle_cache();
      }
      // skip promotion when proxying a delete op
      if (ops.size() == 1) {
	set_skip_promote();
      }
      break;

    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
    case CEPH_OSD_OP_CACHE_FLUSH:
    case CEPH_OSD_OP_CACHE_EVICT:
      // If try_flush/flush/evict is the only op, can skip handle cache.
      if (ops.size() == 1) {
	set_skip_handle_cache();
      }
      break;

    case CEPH_OSD_OP_READ:
    case CEPH_OSD_OP_SYNC_READ:
    case CEPH_OSD_OP_SPARSE_READ:
    case CEPH_OSD_OP_CHECKSUM:
    case CEPH_OSD_OP_WRITEFULL:
      if (ops.size() == 1 &&
          (iter->op.flags & CEPH_OSD_OP_FLAG_FADVISE_NOCACHE ||
           iter->op.flags & CEPH_OSD_OP_FLAG_FADVISE_DONTNEED)) {
        set_skip_promote();
      }
      break;

    // force promotion when pin an object in cache tier
    case CEPH_OSD_OP_CACHE_PIN:
      set_promote();
      break;

    default:
      break;
    }
  }

  if (rmw_flags == 0)
    return -EINVAL;

  return 0;

}

ostream& operator<<(ostream& out, const OpInfo::ClassInfo& i)
{
  out << "class " << i.class_name << " method " << i.method_name
      << " rd " << i.read << " wr " << i.write << " allowed " << i.allowed;
  return out;
}
