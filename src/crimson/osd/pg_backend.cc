// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_backend.h"

seastar::future<> PGBackend::handle_message(OpRef op)
{
  const Message *m = op->get_req();
  switch (m->get_type()) {
  case MSG_OSD_PG_RECOVERY_DELETE:
  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    return seastar::make_ready_future<>();
  case MSG_OSD_PG_PUSH:
  case MSG_OSD_PG_PULL:
  case MSG_OSD_PG_PUSH_REPLY:
  case MSG_OSD_REPOP:
  case MSG_OSD_REPOPREPLY:
    return seastar::make_ready_future<>();
  case CEPH_MSG_OSD_OP:
    if (!is_active()) {
      waiting_for_active.push_back(op);
      return seastar::now();
    }
    return do_msg_osd_op(op);
  case CEPH_MSG_OSD_BACKOFF:
  case MSG_OSD_PG_SCAN:
  case MSG_OSD_PG_BACKFILL:
  case MSG_OSD_PG_BACKFILL_REMOVE:
  case MSG_OSD_SCRUB_RESERVE:
  case MSG_OSD_REP_SCRUB:
  case MSG_OSD_REP_SCRUBMAP:
  case MSG_OSD_PG_UPDATE_LOG_MISSING:
  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    return seastar::make_ready_future<>();
  default:
    return seastar::make_ready_future<>();
  }
}
seastar::future<> PGBackend::do_msg_osd_op(OpRef op)
{
  return seastar::now();
}
seastar::future<> PGBackend::do_osd_op(Ref<MOSDOp> m)
{
  return seastar::do_for_each(m.ops, [this](OSDOp& op) {
    return do_op(op).then([&op](int result) {
      op.rval = result;
      return seastar::now();
    });
}

seastar::future<int> PGBackend::do_op(OSDOp& op)
{
  switch (op.op) {
  // --- READS ---
  case CEPH_OSD_OP_CMPEXT:
    // compare an extend with given buffer
    return seastar::make_ready_future<int>(-EOPNOTSUPP);
  case CEPH_OSD_OP_SYNC_READ:
    if (pool.info.is_erasure()) {
      return seastar::make_ready_future<int>(-EOPNOTSUPP);
    }
    // fall through
  case CEPH_OSD_OP_READ:
    return do_read(op);
  case CEPH_OSD_OP_CHECKSUM:
  case CEPH_OSD_OP_MAPEXT:
  case CEPH_OSD_OP_SPARSE_READ:
  case CEPH_OSD_OP_CALL:
  case CEPH_OSD_OP_STAT:
  case CEPH_OSD_OP_UNDIRTY:
  case CEPH_OSD_OP_CACHE_TRY_FLUSH:
  case CEPH_OSD_OP_CACHE_FLUSH:
  case CEPH_OSD_OP_CACHE_EVICT:
  case CEPH_OSD_OP_GETXATTR:
  case CEPH_OSD_OP_GETXATTRS:
  case CEPH_OSD_OP_CMPXATTR:
  case CEPH_OSD_OP_ASSERT_VER:
  case CEPH_OSD_OP_LIST_WATCHERS:
  case CEPH_OSD_OP_LIST_SNAPS:
  case CEPH_OSD_OP_NOTIFY:
  case CEPH_OSD_OP_NOTIFY_ACK:
  case CEPH_OSD_OP_SETALLOCHINT:
  // --- WRITES ---
  // -- object data --
  case CEPH_OSD_OP_WRITE:
  case CEPH_OSD_OP_WRITEFULL:
  case CEPH_OSD_OP_WRITESAME:
  case CEPH_OSD_OP_ROLLBACK:
  case CEPH_OSD_OP_ZERO:
  case CEPH_OSD_OP_CREATE:
  case CEPH_OSD_OP_TRIMTRUNC:
  case CEPH_OSD_OP_TRUNCATE:
  case CEPH_OSD_OP_DELETE:
  case CEPH_OSD_OP_WATCH:
  case CEPH_OSD_OP_CACHE_PIN:
  case CEPH_OSD_OP_CACHE_UNPIN:
  case CEPH_OSD_OP_SET_REDIRECT:
  case CEPH_OSD_OP_SET_CHUNK:
  case CEPH_OSD_OP_TIER_PROMOTE:
  case CEPH_OSD_OP_UNSET_MANIFEST:
  // -- object attrs --
  case CEPH_OSD_OP_SETXATTR:
  case CEPH_OSD_OP_RMXATTR:
  // -- fancy writers --
  case CEPH_OSD_OP_APPEND:
  case CEPH_OSD_OP_STARTSYNC:
  // -- trivial map --
  case CEPH_OSD_OP_TMAPGET:
  case CEPH_OSD_OP_TMAPPUT:
  case CEPH_OSD_OP_TMAPUP:
  case CEPH_OSD_OP_TMAP2OMAP:
  // OMAP Read ops
  case CEPH_OSD_OP_OMAPGETKEYS:
  case CEPH_OSD_OP_OMAPGETVALS:
  case CEPH_OSD_OP_OMAPGETHEADER:
  case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
  case CEPH_OSD_OP_OMAP_CMP:
  // OMAP Write ops
  case CEPH_OSD_OP_OMAPSETVALS:
  case CEPH_OSD_OP_OMAPSETHEADER:
  case CEPH_OSD_OP_OMAPCLEAR:
  case CEPH_OSD_OP_OMAPRMKEYS:
  case CEPH_OSD_OP_COPY_GET:
  case CEPH_OSD_OP_COPY_FROM:
  default:
    return seastar::make_ready_future<int>(-EOPNOTSUPP);
  }
}

seastar::future<int> PGBackend::do_read(OSDOp& op)
{
  
  return store->read(op.soid,
                     op.extent.offset,
                     op.extent.length,
                     op.flags).then([](int retval, bufferlist& outdata) {
    if (retval >= 0) {
      op.outdata = std::move(outdata);
    }
    return make_ready_future<int>(retval);
  });
}
