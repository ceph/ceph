// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <cerrno>

#include "Objecter.h"
#include "osd/OSDMap.h"
#include "osd/error_code.h"
#include "Filer.h"

#include "mon/MonClient.h"
#include "mon/error_code.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDBackoff.h"
#include "messages/MOSDMap.h"

#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"

#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"
#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MMonCommand.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MWatchNotify.h"


#include "common/Cond.h"
#include "common/config.h"
#include "common/perf_counters.h"
#include "common/scrub_types.h"
#include "include/str_list.h"
#include "common/errno.h"
#include "common/EventTrace.h"
#include "common/async/waiter.h"
#include "error_code.h"


using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

using ceph::decode;
using ceph::encode;
using ceph::Formatter;

using std::defer_lock;
using std::scoped_lock;
using std::shared_lock;
using std::unique_lock;

using ceph::real_time;
using ceph::real_clock;

using ceph::mono_clock;
using ceph::mono_time;

using ceph::timespan;

using ceph::shunique_lock;
using ceph::acquire_shared;
using ceph::acquire_unique;

namespace bc = boost::container;
namespace bs = boost::system;
namespace ca = ceph::async;
namespace cb = ceph::buffer;
namespace asio = boost::asio;

#define dout_subsys ceph_subsys_objecter
#undef dout_prefix
#define dout_prefix *_dout << messenger->get_myname() << ".objecter "


enum {
  l_osdc_first = 123200,
  l_osdc_op_active,
  l_osdc_op_laggy,
  l_osdc_op_send,
  l_osdc_op_send_bytes,
  l_osdc_op_resend,
  l_osdc_op_reply,
  l_osdc_op_latency,
  l_osdc_op_inflight,
  l_osdc_oplen_avg,

  l_osdc_op,
  l_osdc_op_r,
  l_osdc_op_w,
  l_osdc_op_rmw,
  l_osdc_op_pg,

  l_osdc_osdop_stat,
  l_osdc_osdop_create,
  l_osdc_osdop_read,
  l_osdc_osdop_write,
  l_osdc_osdop_writefull,
  l_osdc_osdop_writesame,
  l_osdc_osdop_append,
  l_osdc_osdop_zero,
  l_osdc_osdop_truncate,
  l_osdc_osdop_delete,
  l_osdc_osdop_mapext,
  l_osdc_osdop_sparse_read,
  l_osdc_osdop_clonerange,
  l_osdc_osdop_getxattr,
  l_osdc_osdop_setxattr,
  l_osdc_osdop_cmpxattr,
  l_osdc_osdop_rmxattr,
  l_osdc_osdop_resetxattrs,
  l_osdc_osdop_call,
  l_osdc_osdop_watch,
  l_osdc_osdop_notify,
  l_osdc_osdop_src_cmpxattr,
  l_osdc_osdop_pgls,
  l_osdc_osdop_pgls_filter,
  l_osdc_osdop_other,

  l_osdc_linger_active,
  l_osdc_linger_send,
  l_osdc_linger_resend,
  l_osdc_linger_ping,

  l_osdc_poolop_active,
  l_osdc_poolop_send,
  l_osdc_poolop_resend,

  l_osdc_poolstat_active,
  l_osdc_poolstat_send,
  l_osdc_poolstat_resend,

  l_osdc_statfs_active,
  l_osdc_statfs_send,
  l_osdc_statfs_resend,

  l_osdc_command_active,
  l_osdc_command_send,
  l_osdc_command_resend,

  l_osdc_map_epoch,
  l_osdc_map_full,
  l_osdc_map_inc,

  l_osdc_osd_sessions,
  l_osdc_osd_session_open,
  l_osdc_osd_session_close,
  l_osdc_osd_laggy,

  l_osdc_osdop_omap_wr,
  l_osdc_osdop_omap_rd,
  l_osdc_osdop_omap_del,

  l_osdc_replica_read_sent,
  l_osdc_replica_read_bounced,
  l_osdc_replica_read_completed,

  l_osdc_last,
};

namespace {
inline bs::error_code osdcode(int r) {
  return (r < 0) ? bs::error_code(-r, osd_category()) : bs::error_code();
}
}

// config obs ----------------------------

class Objecter::RequestStateHook : public AdminSocketHook {
  Objecter *m_objecter;
public:
  explicit RequestStateHook(Objecter *objecter);
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   cb::list& out) override;
};

std::unique_lock<std::mutex> Objecter::OSDSession::get_lock(object_t& oid)
{
  if (oid.name.empty())
    return {};

  static constexpr uint32_t HASH_PRIME = 1021;
  uint32_t h = ceph_str_hash_linux(oid.name.c_str(), oid.name.size())
    % HASH_PRIME;

  return {completion_locks[h % num_locks], std::defer_lock};
}

const char** Objecter::get_tracked_conf_keys() const
{
  static const char *config_keys[] = {
    "crush_location",
    "rados_mon_op_timeout",
    "rados_osd_op_timeout",
    NULL
  };
  return config_keys;
}


void Objecter::handle_conf_change(const ConfigProxy& conf,
				  const std::set <std::string> &changed)
{
  if (changed.count("crush_location")) {
    update_crush_location();
  }
  if (changed.count("rados_mon_op_timeout")) {
    mon_timeout = conf.get_val<std::chrono::seconds>("rados_mon_op_timeout");
  }
  if (changed.count("rados_osd_op_timeout")) {
    osd_timeout = conf.get_val<std::chrono::seconds>("rados_osd_op_timeout");
  }
}

void Objecter::update_crush_location()
{
  unique_lock wl(rwlock);
  crush_location = cct->crush_location.get_location();
}

// messages ------------------------------

/*
 * initialize only internal data structures, don't initiate cluster interaction
 */
void Objecter::init()
{
  ceph_assert(!initialized);

  if (!logger) {
    PerfCountersBuilder pcb(cct, "objecter", l_osdc_first, l_osdc_last);

    pcb.add_u64(l_osdc_op_active, "op_active", "Operations active", "actv",
		PerfCountersBuilder::PRIO_CRITICAL);
    pcb.add_u64(l_osdc_op_laggy, "op_laggy", "Laggy operations");
    pcb.add_u64_counter(l_osdc_op_send, "op_send", "Sent operations");
    pcb.add_u64_counter(l_osdc_op_send_bytes, "op_send_bytes", "Sent data", NULL, 0, unit_t(UNIT_BYTES));
    pcb.add_u64_counter(l_osdc_op_resend, "op_resend", "Resent operations");
    pcb.add_u64_counter(l_osdc_op_reply, "op_reply", "Operation reply");
    pcb.add_time_avg(l_osdc_op_latency, "op_latency", "Operation latency");
    pcb.add_u64(l_osdc_op_inflight, "op_inflight", "Operations in flight");
    pcb.add_u64_avg(l_osdc_oplen_avg, "oplen_avg", "Average length of operation vector");

    pcb.add_u64_counter(l_osdc_op, "op", "Operations");
    pcb.add_u64_counter(l_osdc_op_r, "op_r", "Read operations", "rd",
			PerfCountersBuilder::PRIO_CRITICAL);
    pcb.add_u64_counter(l_osdc_op_w, "op_w", "Write operations", "wr",
			PerfCountersBuilder::PRIO_CRITICAL);
    pcb.add_u64_counter(l_osdc_op_rmw, "op_rmw", "Read-modify-write operations",
			"rdwr", PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64_counter(l_osdc_op_pg, "op_pg", "PG operation");

    pcb.add_u64_counter(l_osdc_osdop_stat, "osdop_stat", "Stat operations");
    pcb.add_u64_counter(l_osdc_osdop_create, "osdop_create",
			"Create object operations");
    pcb.add_u64_counter(l_osdc_osdop_read, "osdop_read", "Read operations");
    pcb.add_u64_counter(l_osdc_osdop_write, "osdop_write", "Write operations");
    pcb.add_u64_counter(l_osdc_osdop_writefull, "osdop_writefull",
			"Write full object operations");
    pcb.add_u64_counter(l_osdc_osdop_writesame, "osdop_writesame",
                        "Write same operations");
    pcb.add_u64_counter(l_osdc_osdop_append, "osdop_append",
			"Append operation");
    pcb.add_u64_counter(l_osdc_osdop_zero, "osdop_zero",
			"Set object to zero operations");
    pcb.add_u64_counter(l_osdc_osdop_truncate, "osdop_truncate",
			"Truncate object operations");
    pcb.add_u64_counter(l_osdc_osdop_delete, "osdop_delete",
			"Delete object operations");
    pcb.add_u64_counter(l_osdc_osdop_mapext, "osdop_mapext",
			"Map extent operations");
    pcb.add_u64_counter(l_osdc_osdop_sparse_read, "osdop_sparse_read",
			"Sparse read operations");
    pcb.add_u64_counter(l_osdc_osdop_clonerange, "osdop_clonerange",
			"Clone range operations");
    pcb.add_u64_counter(l_osdc_osdop_getxattr, "osdop_getxattr",
			"Get xattr operations");
    pcb.add_u64_counter(l_osdc_osdop_setxattr, "osdop_setxattr",
			"Set xattr operations");
    pcb.add_u64_counter(l_osdc_osdop_cmpxattr, "osdop_cmpxattr",
			"Xattr comparison operations");
    pcb.add_u64_counter(l_osdc_osdop_rmxattr, "osdop_rmxattr",
			"Remove xattr operations");
    pcb.add_u64_counter(l_osdc_osdop_resetxattrs, "osdop_resetxattrs",
			"Reset xattr operations");
    pcb.add_u64_counter(l_osdc_osdop_call, "osdop_call",
			"Call (execute) operations");
    pcb.add_u64_counter(l_osdc_osdop_watch, "osdop_watch",
			"Watch by object operations");
    pcb.add_u64_counter(l_osdc_osdop_notify, "osdop_notify",
			"Notify about object operations");
    pcb.add_u64_counter(l_osdc_osdop_src_cmpxattr, "osdop_src_cmpxattr",
			"Extended attribute comparison in multi operations");
    pcb.add_u64_counter(l_osdc_osdop_pgls, "osdop_pgls");
    pcb.add_u64_counter(l_osdc_osdop_pgls_filter, "osdop_pgls_filter");
    pcb.add_u64_counter(l_osdc_osdop_other, "osdop_other", "Other operations");

    pcb.add_u64(l_osdc_linger_active, "linger_active",
		"Active lingering operations");
    pcb.add_u64_counter(l_osdc_linger_send, "linger_send",
			"Sent lingering operations");
    pcb.add_u64_counter(l_osdc_linger_resend, "linger_resend",
			"Resent lingering operations");
    pcb.add_u64_counter(l_osdc_linger_ping, "linger_ping",
			"Sent pings to lingering operations");

    pcb.add_u64(l_osdc_poolop_active, "poolop_active",
		"Active pool operations");
    pcb.add_u64_counter(l_osdc_poolop_send, "poolop_send",
			"Sent pool operations");
    pcb.add_u64_counter(l_osdc_poolop_resend, "poolop_resend",
			"Resent pool operations");

    pcb.add_u64(l_osdc_poolstat_active, "poolstat_active",
		"Active get pool stat operations");
    pcb.add_u64_counter(l_osdc_poolstat_send, "poolstat_send",
			"Pool stat operations sent");
    pcb.add_u64_counter(l_osdc_poolstat_resend, "poolstat_resend",
			"Resent pool stats");

    pcb.add_u64(l_osdc_statfs_active, "statfs_active", "Statfs operations");
    pcb.add_u64_counter(l_osdc_statfs_send, "statfs_send", "Sent FS stats");
    pcb.add_u64_counter(l_osdc_statfs_resend, "statfs_resend",
			"Resent FS stats");

    pcb.add_u64(l_osdc_command_active, "command_active", "Active commands");
    pcb.add_u64_counter(l_osdc_command_send, "command_send",
			"Sent commands");
    pcb.add_u64_counter(l_osdc_command_resend, "command_resend",
			"Resent commands");

    pcb.add_u64(l_osdc_map_epoch, "map_epoch", "OSD map epoch");
    pcb.add_u64_counter(l_osdc_map_full, "map_full",
			"Full OSD maps received");
    pcb.add_u64_counter(l_osdc_map_inc, "map_inc",
			"Incremental OSD maps received");

    pcb.add_u64(l_osdc_osd_sessions, "osd_sessions",
		"Open sessions");  // open sessions
    pcb.add_u64_counter(l_osdc_osd_session_open, "osd_session_open",
			"Sessions opened");
    pcb.add_u64_counter(l_osdc_osd_session_close, "osd_session_close",
			"Sessions closed");
    pcb.add_u64(l_osdc_osd_laggy, "osd_laggy", "Laggy OSD sessions");

    pcb.add_u64_counter(l_osdc_osdop_omap_wr, "omap_wr",
			"OSD OMAP write operations");
    pcb.add_u64_counter(l_osdc_osdop_omap_rd, "omap_rd",
			"OSD OMAP read operations");
    pcb.add_u64_counter(l_osdc_osdop_omap_del, "omap_del",
			"OSD OMAP delete operations");

    pcb.add_u64_counter(l_osdc_replica_read_sent, "replica_read_sent",
			"Operations sent to replica");
    pcb.add_u64_counter(l_osdc_replica_read_bounced, "replica_read_bounced",
			"Operations bounced by replica to be resent to primary");
    pcb.add_u64_counter(l_osdc_replica_read_completed, "replica_read_completed",
			"Operations completed by replica");

    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  m_request_state_hook = new RequestStateHook(this);
  auto admin_socket = cct->get_admin_socket();
  int ret = admin_socket->register_command("objecter_requests",
					   m_request_state_hook,
					   "show in-progress osd requests");

  /* Don't warn on EEXIST, happens if multiple ceph clients
   * are instantiated from one process */
  if (ret < 0 && ret != -EEXIST) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(ret) << dendl;
  }

  update_crush_location();

  cct->_conf.add_observer(this);

  initialized = true;
}

/*
 * ok, cluster interaction can happen
 */
void Objecter::start(const OSDMap* o)
{
  shared_lock rl(rwlock);

  start_tick();
  if (o) {
    osdmap->deepish_copy_from(*o);
    prune_pg_mapping(osdmap->get_pools());
  } else if (osdmap->get_epoch() == 0) {
    _maybe_request_map();
  }
}

void Objecter::shutdown()
{
  ceph_assert(initialized);

  unique_lock wl(rwlock);

  initialized = false;

  wl.unlock();
  cct->_conf.remove_observer(this);
  wl.lock();

  while (!osd_sessions.empty()) {
    auto p = osd_sessions.begin();
    close_session(p->second);
  }

  while(!check_latest_map_lingers.empty()) {
    auto i = check_latest_map_lingers.begin();
    i->second->put();
    check_latest_map_lingers.erase(i->first);
  }

  while(!check_latest_map_ops.empty()) {
    auto i = check_latest_map_ops.begin();
    i->second->put();
    check_latest_map_ops.erase(i->first);
  }

  while(!check_latest_map_commands.empty()) {
    auto i = check_latest_map_commands.begin();
    i->second->put();
    check_latest_map_commands.erase(i->first);
  }

  while(!poolstat_ops.empty()) {
    auto i = poolstat_ops.begin();
    delete i->second;
    poolstat_ops.erase(i->first);
  }

  while(!statfs_ops.empty()) {
    auto i = statfs_ops.begin();
    delete i->second;
    statfs_ops.erase(i->first);
  }

  while(!pool_ops.empty()) {
    auto i = pool_ops.begin();
    delete i->second;
    pool_ops.erase(i->first);
  }

  ldout(cct, 20) << __func__ << " clearing up homeless session..." << dendl;
  while(!homeless_session->linger_ops.empty()) {
    auto i = homeless_session->linger_ops.begin();
    ldout(cct, 10) << " linger_op " << i->first << dendl;
    LingerOp *lop = i->second;
    {
      std::unique_lock swl(homeless_session->lock);
      _session_linger_op_remove(homeless_session, lop);
    }
    linger_ops.erase(lop->linger_id);
    linger_ops_set.erase(lop);
    lop->put();
  }

  while(!homeless_session->ops.empty()) {
    auto i = homeless_session->ops.begin();
    ldout(cct, 10) << " op " << i->first << dendl;
    auto op = i->second;
    {
      std::unique_lock swl(homeless_session->lock);
      _session_op_remove(homeless_session, op);
    }
    op->put();
  }

  while(!homeless_session->command_ops.empty()) {
    auto i = homeless_session->command_ops.begin();
    ldout(cct, 10) << " command_op " << i->first << dendl;
    auto cop = i->second;
    {
      std::unique_lock swl(homeless_session->lock);
      _session_command_op_remove(homeless_session, cop);
    }
    cop->put();
  }

  if (tick_event) {
    if (timer.cancel_event(tick_event)) {
      ldout(cct, 10) <<  " successfully canceled tick" << dendl;
    }
    tick_event = 0;
  }

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }

  // Let go of Objecter write lock so timer thread can shutdown
  wl.unlock();

  // Outside of lock to avoid cycle WRT calls to RequestStateHook
  // This is safe because we guarantee no concurrent calls to
  // shutdown() with the ::initialized check at start.
  if (m_request_state_hook) {
    auto admin_socket = cct->get_admin_socket();
    admin_socket->unregister_commands(m_request_state_hook);
    delete m_request_state_hook;
    m_request_state_hook = NULL;
  }
}

void Objecter::_send_linger(LingerOp *info,
			    ceph::shunique_lock<ceph::shared_mutex>& sul)
{
  ceph_assert(sul.owns_lock() && sul.mutex() == &rwlock);

  fu2::unique_function<Op::OpSig> oncommit;
  osdc_opvec opv;
  std::shared_lock watchl(info->watch_lock);
  cb::list *poutbl = nullptr;
  if (info->registered && info->is_watch) {
    ldout(cct, 15) << "send_linger " << info->linger_id << " reconnect"
		   << dendl;
    opv.push_back(OSDOp());
    opv.back().op.op = CEPH_OSD_OP_WATCH;
    opv.back().op.watch.cookie = info->get_cookie();
    opv.back().op.watch.op = CEPH_OSD_WATCH_OP_RECONNECT;
    opv.back().op.watch.gen = ++info->register_gen;
    oncommit = CB_Linger_Reconnect(this, info);
  } else {
    ldout(cct, 15) << "send_linger " << info->linger_id << " register"
		   << dendl;
    opv = info->ops;
    // TODO Augment ca::Completion with an equivalent of
    // target so we can handle these cases better.
    auto c = std::make_unique<CB_Linger_Commit>(this, info);
    if (!info->is_watch) {
      info->notify_id = 0;
      poutbl = &c->outbl;
    }
    oncommit = [c = std::move(c)](bs::error_code ec) mutable {
		 std::move(*c)(ec);
	       };
  }
  watchl.unlock();
  auto o = new Op(info->target.base_oid, info->target.base_oloc,
		  std::move(opv), info->target.flags | CEPH_OSD_FLAG_READ,
		  std::move(oncommit), info->pobjver);
  o->outbl = poutbl;
  o->snapid = info->snap;
  o->snapc = info->snapc;
  o->mtime = info->mtime;

  o->target = info->target;
  o->tid = ++last_tid;

  // do not resend this; we will send a new op to reregister
  o->should_resend = false;
  o->ctx_budgeted = true;

  if (info->register_tid) {
    // repeat send.  cancel old registration op, if any.
    std::unique_lock sl(info->session->lock);
    if (info->session->ops.count(info->register_tid)) {
      auto o = info->session->ops[info->register_tid];
      _op_cancel_map_check(o);
      _cancel_linger_op(o);
    }
    sl.unlock();
  }

  _op_submit_with_budget(o, sul, &info->register_tid, &info->ctx_budget);

  logger->inc(l_osdc_linger_send);
}

void Objecter::_linger_commit(LingerOp *info, bs::error_code ec,
			      cb::list& outbl)
{
  std::unique_lock wl(info->watch_lock);
  ldout(cct, 10) << "_linger_commit " << info->linger_id << dendl;
  if (info->on_reg_commit) {
    asio::defer(service.get_executor(),
		asio::append(std::move(info->on_reg_commit),
			     ec, cb::list{}));
  }
  if (ec && info->on_notify_finish) {
    asio::defer(service.get_executor(),
		asio::append(std::move(info->on_notify_finish),
			     ec, cb::list{}));
  }

  // only tell the user the first time we do this
  info->registered = true;
  info->pobjver = NULL;

  if (!info->is_watch) {
    // make note of the notify_id
    auto p = outbl.cbegin();
    try {
      decode(info->notify_id, p);
      ldout(cct, 10) << "_linger_commit  notify_id=" << info->notify_id
		     << dendl;
    }
    catch (cb::error& e) {
    }
  }
}

class CB_DoWatchError {
  Objecter *objecter;
  boost::intrusive_ptr<Objecter::LingerOp> info;
  bs::error_code ec;
public:
  CB_DoWatchError(Objecter *o, Objecter::LingerOp *i,
		  bs::error_code ec)
    : objecter(o), info(i), ec(ec) {
    info->_queued_async();
  }
  void operator()() {
    std::unique_lock wl(objecter->rwlock);
    bool canceled = info->canceled;
    wl.unlock();

    if (!canceled) {
      info->handle(ec, 0, info->get_cookie(), 0, {});
    }

    info->finished_async();
  }
};

bs::error_code Objecter::_normalize_watch_error(bs::error_code ec)
{
  // translate ENOENT -> ENOTCONN so that a delete->disconnection
  // notification and a failure to reconnect because we raced with
  // the delete appear the same to the user.
  if (ec == bs::errc::no_such_file_or_directory)
    ec = bs::error_code(ENOTCONN, osd_category());
  return ec;
}

void Objecter::_linger_reconnect(LingerOp *info, bs::error_code ec)
{
  ldout(cct, 10) << __func__ << " " << info->linger_id << " = " << ec 
		 << " (last_error " << info->last_error << ")" << dendl;
  std::unique_lock wl(info->watch_lock);
  if (ec) {
    ec = _normalize_watch_error(ec);
    if (!info->last_error) {
      if (info->handle) {
	asio::defer(finish_strand, CB_DoWatchError(this, info, ec));
      }
    }
  }

  info->last_error = ec;
}

void Objecter::_send_linger_ping(LingerOp *info)
{
  // rwlock is locked unique
  // info->session->lock is locked

  if (cct->_conf->objecter_inject_no_watch_ping) {
    ldout(cct, 10) << __func__ << " " << info->linger_id << " SKIPPING"
		   << dendl;
    return;
  }
  if (osdmap->test_flag(CEPH_OSDMAP_PAUSERD)) {
    ldout(cct, 10) << __func__ << " PAUSERD" << dendl;
    return;
  }

  ceph::coarse_mono_time now = ceph::coarse_mono_clock::now();
  ldout(cct, 10) << __func__ << " " << info->linger_id << " now " << now
		 << dendl;

  osdc_opvec opv(1);
  opv[0].op.op = CEPH_OSD_OP_WATCH;
  opv[0].op.watch.cookie = info->get_cookie();
  opv[0].op.watch.op = CEPH_OSD_WATCH_OP_PING;
  opv[0].op.watch.gen = info->register_gen;

  Op *o = new Op(info->target.base_oid, info->target.base_oloc,
		 std::move(opv), info->target.flags | CEPH_OSD_FLAG_READ,
		 fu2::unique_function<Op::OpSig>{CB_Linger_Ping(this, info, now)},
		 nullptr, nullptr);
  o->target = info->target;
  o->should_resend = false;
  _send_op_account(o);
  o->tid = ++last_tid;
  _session_op_assign(info->session, o);
  _send_op(o);
  info->ping_tid = o->tid;

  logger->inc(l_osdc_linger_ping);
}

void Objecter::_linger_ping(LingerOp *info, bs::error_code ec, ceph::coarse_mono_time sent,
			    uint32_t register_gen)
{
  std::unique_lock l(info->watch_lock);
  ldout(cct, 10) << __func__ << " " << info->linger_id
		 << " sent " << sent << " gen " << register_gen << " = " << ec
		 << " (last_error " << info->last_error
		 << " register_gen " << info->register_gen << ")" << dendl;
  if (info->register_gen == register_gen) {
    if (!ec) {
      info->watch_valid_thru = sent;
    } else if (ec && !info->last_error) {
      ec = _normalize_watch_error(ec);
      info->last_error = ec;
      if (info->handle) {
	asio::defer(finish_strand, CB_DoWatchError(this, info, ec));
      }
    }
  } else {
    ldout(cct, 20) << " ignoring old gen" << dendl;
  }
}

tl::expected<ceph::timespan,
	     bs::error_code> Objecter::linger_check(LingerOp *info)
{
  std::shared_lock l(info->watch_lock);

  ceph::coarse_mono_time stamp = info->watch_valid_thru;
  if (!info->watch_pending_async.empty())
    stamp = std::min(info->watch_valid_thru, info->watch_pending_async.front());
  auto age = ceph::coarse_mono_clock::now() - stamp;

  ldout(cct, 10) << __func__ << " " << info->linger_id
		 << " err " << info->last_error
		 << " age " << age << dendl;
  if (info->last_error)
    return tl::unexpected(info->last_error);
  // return a safe upper bound (we are truncating to ms)
  return age;
}

void Objecter::linger_cancel(LingerOp *info)
{
  unique_lock wl(rwlock);
  _linger_cancel(info);
  info->put();
}

void Objecter::_linger_cancel(LingerOp *info)
{
  // rwlock is locked unique
  ldout(cct, 20) << __func__ << " linger_id=" << info->linger_id << dendl;
  if (!info->canceled) {
    OSDSession *s = info->session;
    std::unique_lock sl(s->lock);
    _session_linger_op_remove(s, info);
    sl.unlock();

    linger_ops.erase(info->linger_id);
    linger_ops_set.erase(info);
    ceph_assert(linger_ops.size() == linger_ops_set.size());

    info->canceled = true;
    info->put();

    logger->dec(l_osdc_linger_active);
  }
}



Objecter::LingerOp *Objecter::linger_register(const object_t& oid,
					      const object_locator_t& oloc,
					      int flags)
{
  unique_lock l(rwlock);
  // Acquire linger ID
  auto info = new LingerOp(this, ++max_linger_id);
  info->target.base_oid = oid;
  info->target.base_oloc = oloc;
  if (info->target.base_oloc.key == oid)
    info->target.base_oloc.key.clear();
  info->target.flags = flags;
  info->watch_valid_thru = ceph::coarse_mono_clock::now();
  ldout(cct, 10) << __func__ << " info " << info
		 << " linger_id " << info->linger_id
		 << " cookie " << info->get_cookie()
		 << dendl;
  linger_ops[info->linger_id] = info;
  linger_ops_set.insert(info);
  ceph_assert(linger_ops.size() == linger_ops_set.size());

  info->get(); // for the caller
  return info;
}

ceph_tid_t Objecter::linger_watch(LingerOp *info,
				  ObjectOperation& op,
				  const SnapContext& snapc,
				  real_time mtime,
				  cb::list& inbl,
				  decltype(info->on_reg_commit)&& oncommit,
				  version_t *objver)
{
  info->is_watch = true;
  info->snapc = snapc;
  info->mtime = mtime;
  info->target.flags |= CEPH_OSD_FLAG_WRITE;
  info->ops = op.ops;
  info->inbl = inbl;
  info->pobjver = objver;
  info->on_reg_commit = std::move(oncommit);

  info->ctx_budget = take_linger_budget(info);

  shunique_lock sul(rwlock, ceph::acquire_unique);
  _linger_submit(info, sul);
  logger->inc(l_osdc_linger_active);

  op.clear();
  return info->linger_id;
}

ceph_tid_t Objecter::linger_notify(LingerOp *info,
				   ObjectOperation& op,
				   snapid_t snap, cb::list& inbl,
				   decltype(LingerOp::on_reg_commit)&& onfinish,
				   version_t *objver)
{
  info->snap = snap;
  info->target.flags |= CEPH_OSD_FLAG_READ;
  info->ops = op.ops;
  info->inbl = inbl;
  info->pobjver = objver;
  info->on_reg_commit = std::move(onfinish);
  info->ctx_budget = take_linger_budget(info);

  shunique_lock sul(rwlock, ceph::acquire_unique);
  _linger_submit(info, sul);
  logger->inc(l_osdc_linger_active);

  op.clear();
  return info->linger_id;
}

void Objecter::_linger_submit(LingerOp *info,
			      ceph::shunique_lock<ceph::shared_mutex>& sul)
{
  ceph_assert(sul.owns_lock() && sul.mutex() == &rwlock);
  ceph_assert(info->linger_id);
  ceph_assert(info->ctx_budget != -1); // caller needs to have taken budget already!

  // Populate Op::target
  OSDSession *s = NULL;
  int r = _calc_target(&info->target, nullptr);
  switch (r) {
  case RECALC_OP_TARGET_POOL_EIO:
    _check_linger_pool_eio(info);
    return;
  }

  // Create LingerOp<->OSDSession relation
  r = _get_session(info->target.osd, &s, sul);
  ceph_assert(r == 0);
  unique_lock sl(s->lock);
  _session_linger_op_assign(s, info);
  sl.unlock();
  put_session(s);

  _send_linger(info, sul);
}

struct CB_DoWatchNotify {
  Objecter *objecter;
  boost::intrusive_ptr<Objecter::LingerOp> info;
  boost::intrusive_ptr<MWatchNotify> msg;
  CB_DoWatchNotify(Objecter *o, Objecter::LingerOp *i, MWatchNotify *m)
    : objecter(o), info(i), msg(m) {
    info->_queued_async();
  }
  void operator()() {
    objecter->_do_watch_notify(std::move(info), std::move(msg));
  }
};

void Objecter::handle_watch_notify(MWatchNotify *m)
{
  shared_lock l(rwlock);
  if (!initialized) {
    return;
  }

  LingerOp *info = reinterpret_cast<LingerOp*>(m->cookie);
  if (linger_ops_set.count(info) == 0) {
    ldout(cct, 7) << __func__ << " cookie " << m->cookie << " dne" << dendl;
    return;
  }
  std::unique_lock wl(info->watch_lock);
  if (m->opcode == CEPH_WATCH_EVENT_DISCONNECT) {
    if (!info->last_error) {
      info->last_error = bs::error_code(ENOTCONN, osd_category());
      if (info->handle) {
	asio::defer(finish_strand, CB_DoWatchError(this, info,
							  info->last_error));
      }
    }
  } else if (!info->is_watch) {
    // we have CEPH_WATCH_EVENT_NOTIFY_COMPLETE; we can do this inline
    // since we know the only user (librados) is safe to call in
    // fast-dispatch context
    if (info->notify_id &&
	info->notify_id != m->notify_id) {
      ldout(cct, 10) << __func__ << " reply notify " << m->notify_id
		     << " != " << info->notify_id << ", ignoring" << dendl;
    } else if (info->on_notify_finish) {
      asio::defer(service.get_executor(),
		  asio::append(std::move(info->on_notify_finish),
			       osdcode(m->return_code),
			       std::move(m->get_data())));
      // if we race with reconnect we might get a second notify; only
      // notify the caller once!
      info->on_notify_finish = nullptr;
    }
  } else {
    asio::defer(finish_strand, CB_DoWatchNotify(this, info, m));
  }
}

void Objecter::_do_watch_notify(boost::intrusive_ptr<LingerOp> info,
                                boost::intrusive_ptr<MWatchNotify> m)
{
  ldout(cct, 10) << __func__ << " " << *m << dendl;

  shared_lock l(rwlock);
  ceph_assert(initialized);

  if (info->canceled) {
    l.unlock();
    goto out;
  }

  // notify completion?
  ceph_assert(info->is_watch);
  ceph_assert(info->handle);
  ceph_assert(m->opcode != CEPH_WATCH_EVENT_DISCONNECT);

  l.unlock();

  switch (m->opcode) {
  case CEPH_WATCH_EVENT_NOTIFY:
    info->handle({}, m->notify_id, m->cookie, m->notifier_gid, std::move(m->bl));
    break;
  }

 out:
  info->finished_async();
}

bool Objecter::ms_dispatch(Message *m)
{
  ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;
  switch (m->get_type()) {
    // these we exlusively handle
  case CEPH_MSG_OSD_OPREPLY:
    handle_osd_op_reply(static_cast<MOSDOpReply*>(m));
    return true;

  case CEPH_MSG_OSD_BACKOFF:
    handle_osd_backoff(static_cast<MOSDBackoff*>(m));
    return true;

  case CEPH_MSG_WATCH_NOTIFY:
    handle_watch_notify(static_cast<MWatchNotify*>(m));
    m->put();
    return true;

  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_OSD) {
      handle_command_reply(static_cast<MCommandReply*>(m));
      return true;
    } else {
      return false;
    }

  case MSG_GETPOOLSTATSREPLY:
    handle_get_pool_stats_reply(static_cast<MGetPoolStatsReply*>(m));
    return true;

  case CEPH_MSG_POOLOP_REPLY:
    handle_pool_op_reply(static_cast<MPoolOpReply*>(m));
    return true;

  case CEPH_MSG_STATFS_REPLY:
    handle_fs_stats_reply(static_cast<MStatfsReply*>(m));
    return true;

    // these we give others a chance to inspect

    // MDS, OSD
  case CEPH_MSG_OSD_MAP:
    handle_osd_map(static_cast<MOSDMap*>(m));
    return false;
  }
  return false;
}

void Objecter::_scan_requests(
  OSDSession *s,
  bool skipped_map,
  bool cluster_full,
  map<int64_t, bool> *pool_full_map,
  map<ceph_tid_t, Op*>& need_resend,
  list<LingerOp*>& need_resend_linger,
  map<ceph_tid_t, CommandOp*>& need_resend_command,
  ceph::shunique_lock<ceph::shared_mutex>& sul)
{
  ceph_assert(sul.owns_lock() && sul.mutex() == &rwlock);

  list<LingerOp*> unregister_lingers;

  std::unique_lock sl(s->lock);

  // check for changed linger mappings (_before_ regular ops)
  auto lp = s->linger_ops.begin();
  while (lp != s->linger_ops.end()) {
    auto op = lp->second;
    ceph_assert(op->session == s);
    // check_linger_pool_dne() may touch linger_ops; prevent iterator
    // invalidation
    ++lp;
    ldout(cct, 10) << " checking linger op " << op->linger_id << dendl;
    bool unregister, force_resend_writes = cluster_full;
    int r = _recalc_linger_op_target(op, sul);
    if (pool_full_map)
      force_resend_writes = force_resend_writes ||
	(*pool_full_map)[op->target.base_oloc.pool];
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!skipped_map && !force_resend_writes)
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend_linger.push_back(op);
      _linger_cancel_map_check(op);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
      _check_linger_pool_dne(op, &unregister);
      if (unregister) {
	ldout(cct, 10) << " need to unregister linger op "
		       << op->linger_id << dendl;
	op->get();
	unregister_lingers.push_back(op);
      }
      break;
    case RECALC_OP_TARGET_POOL_EIO:
      _check_linger_pool_eio(op);
      ldout(cct, 10) << " need to unregister linger op "
		     << op->linger_id << dendl;
      op->get();
      unregister_lingers.push_back(op);
      break;
    }
  }

  // check for changed request mappings
  auto p = s->ops.begin();
  while (p != s->ops.end()) {
    Op *op = p->second;
    ++p;   // check_op_pool_dne() may touch ops; prevent iterator invalidation
    ldout(cct, 10) << " checking op " << op->tid << dendl;
    _prune_snapc(osdmap->get_new_removed_snaps(), op);
    bool force_resend_writes = cluster_full;
    if (pool_full_map)
      force_resend_writes = force_resend_writes ||
	(*pool_full_map)[op->target.base_oloc.pool];
    int r = _calc_target(&op->target,
			 op->session ? op->session->con.get() : nullptr);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      if (!skipped_map && !(force_resend_writes && op->target.respects_full()))
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      _session_op_remove(op->session, op);
      need_resend[op->tid] = op;
      _op_cancel_map_check(op);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
      _check_op_pool_dne(op, &sl);
      break;
    case RECALC_OP_TARGET_POOL_EIO:
      _check_op_pool_eio(op, &sl);
      break;
    }
  }

  // commands
  auto cp = s->command_ops.begin();
  while (cp != s->command_ops.end()) {
    auto c = cp->second;
    ++cp;
    ldout(cct, 10) << " checking command " << c->tid << dendl;
    bool force_resend_writes = cluster_full;
    if (pool_full_map)
      force_resend_writes = force_resend_writes ||
	(*pool_full_map)[c->target_pg.pool()];
    int r = _calc_command_target(c, sul);
    switch (r) {
    case RECALC_OP_TARGET_NO_ACTION:
      // resend if skipped map; otherwise do nothing.
      if (!skipped_map && !force_resend_writes)
	break;
      // -- fall-thru --
    case RECALC_OP_TARGET_NEED_RESEND:
      need_resend_command[c->tid] = c;
      _session_command_op_remove(c->session, c);
      _command_cancel_map_check(c);
      break;
    case RECALC_OP_TARGET_POOL_DNE:
    case RECALC_OP_TARGET_OSD_DNE:
    case RECALC_OP_TARGET_OSD_DOWN:
      _check_command_map_dne(c);
      break;
    }
  }

  sl.unlock();

  for (auto iter = unregister_lingers.begin();
       iter != unregister_lingers.end();
       ++iter) {
    _linger_cancel(*iter);
    (*iter)->put();
  }
}

void Objecter::handle_osd_map(MOSDMap *m)
{
  ceph::shunique_lock sul(rwlock, acquire_unique);
  if (!initialized)
    return;

  ceph_assert(osdmap);

  if (m->fsid != monc->get_fsid()) {
    ldout(cct, 0) << "handle_osd_map fsid " << m->fsid
		  << " != " << monc->get_fsid() << dendl;
    return;
  }

  bool was_pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool cluster_full = _osdmap_full_flag();
  bool was_pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || cluster_full ||
    _osdmap_has_pool_full();
  map<int64_t, bool> pool_full_map;
  for (auto it = osdmap->get_pools().begin();
       it != osdmap->get_pools().end(); ++it)
    pool_full_map[it->first] = _osdmap_pool_full(it->second);


  list<LingerOp*> need_resend_linger;
  map<ceph_tid_t, Op*> need_resend;
  map<ceph_tid_t, CommandOp*> need_resend_command;

  if (m->get_last() <= osdmap->get_epoch()) {
    ldout(cct, 3) << "handle_osd_map ignoring epochs ["
		  << m->get_first() << "," << m->get_last()
		  << "] <= " << osdmap->get_epoch() << dendl;
  } else {
    ldout(cct, 3) << "handle_osd_map got epochs ["
		  << m->get_first() << "," << m->get_last()
		  << "] > " << osdmap->get_epoch() << dendl;

    if (osdmap->get_epoch()) {
      bool skipped_map = false;
      // we want incrementals
      for (epoch_t e = osdmap->get_epoch() + 1;
	   e <= m->get_last();
	   e++) {

	if (osdmap->get_epoch() == e-1 &&
	    m->incremental_maps.count(e)) {
	  ldout(cct, 3) << "handle_osd_map decoding incremental epoch " << e
			<< dendl;
	  OSDMap::Incremental inc(m->incremental_maps[e]);
	  osdmap->apply_incremental(inc);

          emit_blocklist_events(inc);

	  logger->inc(l_osdc_map_inc);
	}
	else if (m->maps.count(e)) {
	  ldout(cct, 3) << "handle_osd_map decoding full epoch " << e << dendl;
          auto new_osdmap = std::make_unique<OSDMap>();
          new_osdmap->decode(m->maps[e]);

          emit_blocklist_events(*osdmap, *new_osdmap);
          osdmap = std::move(new_osdmap);

	  logger->inc(l_osdc_map_full);
	}
	else {
	  if (e >= m->cluster_osdmap_trim_lower_bound) {
	    ldout(cct, 3) << "handle_osd_map requesting missing epoch "
			  << osdmap->get_epoch()+1 << dendl;
	    _maybe_request_map();
	    break;
	  }
	  ldout(cct, 3) << "handle_osd_map missing epoch "
			<< osdmap->get_epoch()+1
			<< ", jumping to "
			<< m->cluster_osdmap_trim_lower_bound << dendl;
	  e = m->cluster_osdmap_trim_lower_bound - 1;
	  skipped_map = true;
	  continue;
	}
	logger->set(l_osdc_map_epoch, osdmap->get_epoch());

        prune_pg_mapping(osdmap->get_pools());
	cluster_full = cluster_full || _osdmap_full_flag();
	update_pool_full_map(pool_full_map);

	// check all outstanding requests on every epoch
	for (auto& i : need_resend) {
	  _prune_snapc(osdmap->get_new_removed_snaps(), i.second);
	}
	_scan_requests(homeless_session, skipped_map, cluster_full,
		       &pool_full_map, need_resend,
		       need_resend_linger, need_resend_command, sul);
	for (auto p = osd_sessions.begin();
	     p != osd_sessions.end(); ) {
	  auto s = p->second;
	  _scan_requests(s, skipped_map, cluster_full,
			 &pool_full_map, need_resend,
			 need_resend_linger, need_resend_command, sul);
	  ++p;
	  // osd down or addr change?
	  if (!osdmap->is_up(s->osd) ||
	      (s->con &&
	       s->con->get_peer_addrs() != osdmap->get_addrs(s->osd))) {
	    close_session(s);
	  }
	}

	ceph_assert(e == osdmap->get_epoch());
      }

    } else {
      // first map.  we want the full thing.
      if (m->maps.count(m->get_last())) {
	for (auto p = osd_sessions.begin();
	     p != osd_sessions.end(); ++p) {
	  OSDSession *s = p->second;
	  _scan_requests(s, false, false, NULL, need_resend,
			 need_resend_linger, need_resend_command, sul);
	}
	ldout(cct, 3) << "handle_osd_map decoding full epoch "
		      << m->get_last() << dendl;
	osdmap->decode(m->maps[m->get_last()]);
        prune_pg_mapping(osdmap->get_pools());

	_scan_requests(homeless_session, false, false, NULL,
		       need_resend, need_resend_linger,
		       need_resend_command, sul);
      } else {
	ldout(cct, 3) << "handle_osd_map hmm, i want a full map, requesting"
		      << dendl;
	monc->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
	monc->renew_subs();
      }
    }
  }

  // make sure need_resend targets reflect latest map
  for (auto p = need_resend.begin(); p != need_resend.end(); ) {
    Op *op = p->second;
    if (op->target.epoch < osdmap->get_epoch()) {
      ldout(cct, 10) << __func__ << "  checking op " << p->first << dendl;
      int r = _calc_target(&op->target, nullptr);
      if (r == RECALC_OP_TARGET_POOL_DNE) {
	p = need_resend.erase(p);
	_check_op_pool_dne(op, nullptr);
      } else {
	++p;
      }
    } else {
      ++p;
    }
  }

  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) || _osdmap_full_flag()
    || _osdmap_has_pool_full();

  // was/is paused?
  if (was_pauserd || was_pausewr || pauserd || pausewr ||
      osdmap->get_epoch() < epoch_barrier) {
    _maybe_request_map();
  }

  // resend requests
  for (auto p = need_resend.begin();
       p != need_resend.end(); ++p) {
    auto op = p->second;
    auto s = op->session;
    bool mapped_session = false;
    if (!s) {
      int r = _map_session(&op->target, &s, sul);
      ceph_assert(r == 0);
      mapped_session = true;
    } else {
      get_session(s);
    }
    std::unique_lock sl(s->lock);
    if (mapped_session) {
      _session_op_assign(s, op);
    }
    if (op->should_resend) {
      if (!op->session->is_homeless() && !op->target.paused) {
	logger->inc(l_osdc_op_resend);
	_send_op(op);
      }
    } else {
      _op_cancel_map_check(op);
      _cancel_linger_op(op);
    }
    sl.unlock();
    put_session(s);
  }
  for (auto p = need_resend_linger.begin();
       p != need_resend_linger.end(); ++p) {
    LingerOp *op = *p;
    ceph_assert(op->session);
    if (!op->session->is_homeless()) {
      logger->inc(l_osdc_linger_resend);
      _send_linger(op, sul);
    }
  }
  for (auto p = need_resend_command.begin();
       p != need_resend_command.end(); ++p) {
    auto c = p->second;
    if (c->target.osd >= 0) {
      _assign_command_session(c, sul);
      if (c->session && !c->session->is_homeless()) {
	_send_command(c);
      }
    }
  }

  _dump_active();

  // finish any Contexts that were waiting on a map update
  auto p = waiting_for_map.begin();
  while (p != waiting_for_map.end() &&
	 p->first <= osdmap->get_epoch()) {
    //go through the list and call the onfinish methods
    for (auto& [c, ec] : p->second) {
      asio::post(service.get_executor(), asio::append(std::move(c), ec));
    }
    waiting_for_map.erase(p++);
  }

  monc->sub_got("osdmap", osdmap->get_epoch());

  if (!waiting_for_map.empty()) {
    _maybe_request_map();
  }
}

void Objecter::enable_blocklist_events()
{
  unique_lock wl(rwlock);

  blocklist_events_enabled = true;
}

void Objecter::consume_blocklist_events(std::set<entity_addr_t> *events)
{
  unique_lock wl(rwlock);

  if (events->empty()) {
    events->swap(blocklist_events);
  } else {
    for (const auto &i : blocklist_events) {
      events->insert(i);
    }
    blocklist_events.clear();
  }
}

void Objecter::emit_blocklist_events(const OSDMap::Incremental &inc)
{
  if (!blocklist_events_enabled) {
    return;
  }

  for (const auto &i : inc.new_blocklist) {
    blocklist_events.insert(i.first);
  }
}

void Objecter::emit_blocklist_events(const OSDMap &old_osd_map,
                                     const OSDMap &new_osd_map)
{
  if (!blocklist_events_enabled) {
    return;
  }

  std::set<entity_addr_t> old_set;
  std::set<entity_addr_t> new_set;
  std::set<entity_addr_t> old_range_set;
  std::set<entity_addr_t> new_range_set;

  old_osd_map.get_blocklist(&old_set, &old_range_set);
  new_osd_map.get_blocklist(&new_set, &new_range_set);

  std::set<entity_addr_t> delta_set;
  std::set_difference(
      new_set.begin(), new_set.end(), old_set.begin(), old_set.end(),
      std::inserter(delta_set, delta_set.begin()));
  std::set_difference(
      new_range_set.begin(), new_range_set.end(),
      old_range_set.begin(), old_range_set.end(),
      std::inserter(delta_set, delta_set.begin()));
  blocklist_events.insert(delta_set.begin(), delta_set.end());
}

// op pool check

void Objecter::CB_Op_Map_Latest::operator()(bs::error_code e,
					    version_t latest, version_t)
{
  if (e == bs::errc::resource_unavailable_try_again ||
      e == bs::errc::operation_canceled)
    return;

  lgeneric_subdout(objecter->cct, objecter, 10)
    << "op_map_latest r=" << e << " tid=" << tid
    << " latest " << latest << dendl;

  unique_lock wl(objecter->rwlock);

  auto iter = objecter->check_latest_map_ops.find(tid);
  if (iter == objecter->check_latest_map_ops.end()) {
    lgeneric_subdout(objecter->cct, objecter, 10)
      << "op_map_latest op "<< tid << " not found" << dendl;
    return;
  }

  Op *op = iter->second;
  objecter->check_latest_map_ops.erase(iter);

  lgeneric_subdout(objecter->cct, objecter, 20)
    << "op_map_latest op "<< op << dendl;

  if (op->map_dne_bound == 0)
    op->map_dne_bound = latest;

  unique_lock sl(op->session->lock, defer_lock);
  objecter->_check_op_pool_dne(op, &sl);

  op->put();
}

int Objecter::pool_snap_by_name(int64_t poolid, const char *snap_name,
				snapid_t *snap) const
{
  shared_lock rl(rwlock);

  auto& pools = osdmap->get_pools();
  auto iter = pools.find(poolid);
  if (iter == pools.end()) {
    return -ENOENT;
  }
  const pg_pool_t& pg_pool = iter->second;
  for (auto p = pg_pool.snaps.begin();
       p != pg_pool.snaps.end();
       ++p) {
    if (p->second.name == snap_name) {
      *snap = p->first;
      return 0;
    }
  }
  return -ENOENT;
}

int Objecter::pool_snap_get_info(int64_t poolid, snapid_t snap,
				 pool_snap_info_t *info) const
{
  shared_lock rl(rwlock);

  auto& pools = osdmap->get_pools();
  auto iter = pools.find(poolid);
  if (iter == pools.end()) {
    return -ENOENT;
  }
  const pg_pool_t& pg_pool = iter->second;
  auto p = pg_pool.snaps.find(snap);
  if (p == pg_pool.snaps.end())
    return -ENOENT;
  *info = p->second;

  return 0;
}

int Objecter::pool_snap_list(int64_t poolid, vector<uint64_t> *snaps)
{
  shared_lock rl(rwlock);

  const pg_pool_t *pi = osdmap->get_pg_pool(poolid);
  if (!pi)
    return -ENOENT;
  for (auto p = pi->snaps.begin();
       p != pi->snaps.end();
       ++p) {
    snaps->push_back(p->first);
  }
  return 0;
}

// sl may be unlocked.
void Objecter::_check_op_pool_dne(Op *op, std::unique_lock<std::shared_mutex> *sl)
{
  // rwlock is locked unique

  if (op->target.pool_ever_existed) {
    // the pool previously existed and now it does not, which means it
    // was deleted.
    op->map_dne_bound = osdmap->get_epoch();
    ldout(cct, 10) << "check_op_pool_dne tid " << op->tid
		   << " pool previously exists but now does not"
		   << dendl;
  } else {
    ldout(cct, 10) << "check_op_pool_dne tid " << op->tid
		   << " current " << osdmap->get_epoch()
		   << " map_dne_bound " << op->map_dne_bound
		   << dendl;
  }
  if (op->map_dne_bound > 0) {
    if (osdmap->get_epoch() >= op->map_dne_bound) {
      // we had a new enough map
      ldout(cct, 10) << "check_op_pool_dne tid " << op->tid
		     << " concluding pool " << op->target.base_pgid.pool()
		     << " dne" << dendl;
      if (op->has_completion()) {
	num_in_flight--;
	op->complete(osdc_errc::pool_dne, -ENOENT, service.get_executor());
      }

      OSDSession *s = op->session;
      if (s) {
	ceph_assert(s != NULL);
	ceph_assert(sl->mutex() == &s->lock);
	bool session_locked = sl->owns_lock();
	if (!session_locked) {
	  sl->lock();
	}
	_finish_op(op, 0);
	if (!session_locked) {
	  sl->unlock();
	}
      } else {
	_finish_op(op, 0);	// no session
      }
    }
  } else {
    _send_op_map_check(op);
  }
}

// sl may be unlocked.
void Objecter::_check_op_pool_eio(Op *op, std::unique_lock<std::shared_mutex> *sl)
{
  // rwlock is locked unique

  // we had a new enough map
  ldout(cct, 10) << "check_op_pool_eio tid " << op->tid
		 << " concluding pool " << op->target.base_pgid.pool()
		 << " has eio" << dendl;
  if (op->has_completion()) {
    num_in_flight--;
    op->complete(osdc_errc::pool_eio, -EIO, service.get_executor());
  }

  OSDSession *s = op->session;
  if (s) {
    ceph_assert(s != NULL);
    ceph_assert(sl->mutex() == &s->lock);
    bool session_locked = sl->owns_lock();
    if (!session_locked) {
      sl->lock();
    }
    _finish_op(op, 0);
    if (!session_locked) {
      sl->unlock();
    }
  } else {
    _finish_op(op, 0);	// no session
  }
}

void Objecter::_send_op_map_check(Op *op)
{
  // rwlock is locked unique
  // ask the monitor
  if (check_latest_map_ops.count(op->tid) == 0) {
    op->get();
    check_latest_map_ops[op->tid] = op;
    monc->get_version("osdmap", CB_Op_Map_Latest(this, op->tid));
  }
}

void Objecter::_op_cancel_map_check(Op *op)
{
  // rwlock is locked unique
  auto iter = check_latest_map_ops.find(op->tid);
  if (iter != check_latest_map_ops.end()) {
    Op *op = iter->second;
    op->put();
    check_latest_map_ops.erase(iter);
  }
}

// linger pool check

void Objecter::CB_Linger_Map_Latest::operator()(bs::error_code e,
						version_t latest,
						version_t)
{
  if (e == bs::errc::resource_unavailable_try_again ||
      e == bs::errc::operation_canceled) {
    // ignore callback; we will retry in resend_mon_ops()
    return;
  }

  unique_lock wl(objecter->rwlock);

  auto iter = objecter->check_latest_map_lingers.find(linger_id);
  if (iter == objecter->check_latest_map_lingers.end()) {
    return;
  }

  auto op = iter->second;
  objecter->check_latest_map_lingers.erase(iter);

  if (op->map_dne_bound == 0)
    op->map_dne_bound = latest;

  bool unregister;
  objecter->_check_linger_pool_dne(op, &unregister);

  if (unregister) {
    objecter->_linger_cancel(op);
  }

  op->put();
}

void Objecter::_check_linger_pool_dne(LingerOp *op, bool *need_unregister)
{
  // rwlock is locked unique

  *need_unregister = false;

  if (op->register_gen > 0) {
    ldout(cct, 10) << "_check_linger_pool_dne linger_id " << op->linger_id
		   << " pool previously existed but now does not"
		   << dendl;
    op->map_dne_bound = osdmap->get_epoch();
  } else {
    ldout(cct, 10) << "_check_linger_pool_dne linger_id " << op->linger_id
		   << " current " << osdmap->get_epoch()
		   << " map_dne_bound " << op->map_dne_bound
		   << dendl;
  }
  if (op->map_dne_bound > 0) {
    if (osdmap->get_epoch() >= op->map_dne_bound) {
      std::unique_lock wl{op->watch_lock};
      if (op->on_reg_commit) {
	asio::defer(service.get_executor(),
		    asio::append(std::move(op->on_reg_commit),
				 osdc_errc::pool_dne, cb::list{}));
	op->on_reg_commit = nullptr;
      }
      if (op->on_notify_finish) {
	asio::defer(service.get_executor(),
		    asio::append(std::move(op->on_notify_finish),
				 osdc_errc::pool_dne, cb::list{}));
        op->on_notify_finish = nullptr;
      }
      *need_unregister = true;
    }
  } else {
    _send_linger_map_check(op);
  }
}

void Objecter::_check_linger_pool_eio(LingerOp *op)
{
  // rwlock is locked unique

  std::unique_lock wl{op->watch_lock};
  if (op->on_reg_commit) {
    asio::defer(service.get_executor(),
		asio::append(std::move(op->on_reg_commit),
			     osdc_errc::pool_dne, cb::list{}));
  }
  if (op->on_notify_finish) {
    asio::defer(service.get_executor(),
		asio::append(std::move(op->on_notify_finish),
			     osdc_errc::pool_dne, cb::list{}));
  }
}

void Objecter::_send_linger_map_check(LingerOp *op)
{
  // ask the monitor
  if (check_latest_map_lingers.count(op->linger_id) == 0) {
    op->get();
    check_latest_map_lingers[op->linger_id] = op;
    monc->get_version("osdmap", CB_Linger_Map_Latest(this, op->linger_id));
  }
}

void Objecter::_linger_cancel_map_check(LingerOp *op)
{
  // rwlock is locked unique

  auto iter = check_latest_map_lingers.find(op->linger_id);
  if (iter != check_latest_map_lingers.end()) {
    LingerOp *op = iter->second;
    op->put();
    check_latest_map_lingers.erase(iter);
  }
}

// command pool check

void Objecter::CB_Command_Map_Latest::operator()(bs::error_code e,
						 version_t latest, version_t)
{
  if (e == bs::errc::resource_unavailable_try_again ||
      e == bs::errc::operation_canceled) {
    // ignore callback; we will retry in resend_mon_ops()
    return;
  }

  unique_lock wl(objecter->rwlock);

  auto iter = objecter->check_latest_map_commands.find(tid);
  if (iter == objecter->check_latest_map_commands.end()) {
    return;
  }

  auto c = iter->second;
  objecter->check_latest_map_commands.erase(iter);

  if (c->map_dne_bound == 0)
    c->map_dne_bound = latest;

  unique_lock sul(c->session->lock);
  objecter->_check_command_map_dne(c);
  sul.unlock();

  c->put();
}

void Objecter::_check_command_map_dne(CommandOp *c)
{
  // rwlock is locked unique
  // session is locked unique

  ldout(cct, 10) << "_check_command_map_dne tid " << c->tid
		 << " current " << osdmap->get_epoch()
		 << " map_dne_bound " << c->map_dne_bound
		 << dendl;
  if (c->map_dne_bound > 0) {
    if (osdmap->get_epoch() >= c->map_dne_bound) {
      _finish_command(c, osdcode(c->map_check_error),
		      std::move(c->map_check_error_str), {});
    }
  } else {
    _send_command_map_check(c);
  }
}

void Objecter::_send_command_map_check(CommandOp *c)
{
  // rwlock is locked unique
  // session is locked unique

  // ask the monitor
  if (check_latest_map_commands.count(c->tid) == 0) {
    c->get();
    check_latest_map_commands[c->tid] = c;
    monc->get_version("osdmap", CB_Command_Map_Latest(this, c->tid));
  }
}

void Objecter::_command_cancel_map_check(CommandOp *c)
{
  // rwlock is locked uniqe

  auto iter = check_latest_map_commands.find(c->tid);
  if (iter != check_latest_map_commands.end()) {
    auto c = iter->second;
    c->put();
    check_latest_map_commands.erase(iter);
  }
}


/**
 * Look up OSDSession by OSD id.
 *
 * @returns 0 on success, or -EAGAIN if the lock context requires
 * promotion to write.
 */
int Objecter::_get_session(int osd, OSDSession **session,
			   shunique_lock<ceph::shared_mutex>& sul)
{
  ceph_assert(sul && sul.mutex() == &rwlock);

  if (osd < 0) {
    *session = homeless_session;
    ldout(cct, 20) << __func__ << " osd=" << osd << " returning homeless"
		   << dendl;
    return 0;
  }

  auto p = osd_sessions.find(osd);
  if (p != osd_sessions.end()) {
    auto s = p->second;
    s->get();
    *session = s;
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		   << s->get_nref() << dendl;
    return 0;
  }
  if (!sul.owns_lock()) {
    return -EAGAIN;
  }
  auto s = new OSDSession(cct, osd);
  osd_sessions[osd] = s;
  s->con = messenger->connect_to_osd(osdmap->get_addrs(osd));
  s->con->set_priv(RefCountedPtr{s});
  logger->inc(l_osdc_osd_session_open);
  logger->set(l_osdc_osd_sessions, osd_sessions.size());
  s->get();
  *session = s;
  ldout(cct, 20) << __func__ << " s=" << s << " osd=" << osd << " "
		 << s->get_nref() << dendl;
  return 0;
}

void Objecter::put_session(Objecter::OSDSession *s)
{
  if (s && !s->is_homeless()) {
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << s->osd << " "
		   << s->get_nref() << dendl;
    s->put();
  }
}

void Objecter::get_session(Objecter::OSDSession *s)
{
  ceph_assert(s != NULL);

  if (!s->is_homeless()) {
    ldout(cct, 20) << __func__ << " s=" << s << " osd=" << s->osd << " "
		   << s->get_nref() << dendl;
    s->get();
  }
}

void Objecter::_reopen_session(OSDSession *s)
{
  // rwlock is locked unique
  // s->lock is locked

  auto addrs = osdmap->get_addrs(s->osd);
  ldout(cct, 10) << "reopen_session osd." << s->osd << " session, addr now "
		 << addrs << dendl;
  if (s->con) {
    s->con->set_priv(NULL);
    s->con->mark_down();
    logger->inc(l_osdc_osd_session_close);
  }
  s->con = messenger->connect_to_osd(addrs);
  s->con->set_priv(RefCountedPtr{s});
  s->incarnation++;
  logger->inc(l_osdc_osd_session_open);
}

void Objecter::close_session(OSDSession *s)
{
  // rwlock is locked unique

  ldout(cct, 10) << "close_session for osd." << s->osd << dendl;
  if (s->con) {
    s->con->set_priv(NULL);
    s->con->mark_down();
    logger->inc(l_osdc_osd_session_close);
  }
  unique_lock sl(s->lock);

  std::list<LingerOp*> homeless_lingers;
  std::list<CommandOp*> homeless_commands;
  std::list<Op*> homeless_ops;

  while (!s->linger_ops.empty()) {
    auto i = s->linger_ops.begin();
    ldout(cct, 10) << " linger_op " << i->first << dendl;
    homeless_lingers.push_back(i->second);
    _session_linger_op_remove(s, i->second);
  }

  while (!s->ops.empty()) {
    auto i = s->ops.begin();
    ldout(cct, 10) << " op " << i->first << dendl;
    homeless_ops.push_back(i->second);
    _session_op_remove(s, i->second);
  }

  while (!s->command_ops.empty()) {
    auto i = s->command_ops.begin();
    ldout(cct, 10) << " command_op " << i->first << dendl;
    homeless_commands.push_back(i->second);
    _session_command_op_remove(s, i->second);
  }

  osd_sessions.erase(s->osd);
  sl.unlock();
  put_session(s);

  // Assign any leftover ops to the homeless session
  {
    unique_lock hsl(homeless_session->lock);
    for (auto i = homeless_lingers.begin();
	 i != homeless_lingers.end(); ++i) {
      _session_linger_op_assign(homeless_session, *i);
    }
    for (auto i = homeless_ops.begin();
	 i != homeless_ops.end(); ++i) {
      _session_op_assign(homeless_session, *i);
    }
    for (auto i = homeless_commands.begin();
	 i != homeless_commands.end(); ++i) {
      _session_command_op_assign(homeless_session, *i);
    }
  }

  logger->set(l_osdc_osd_sessions, osd_sessions.size());
}

void Objecter::wait_for_osd_map(epoch_t e)
{
  unique_lock l(rwlock);
  if (osdmap->get_epoch() >= e) {
    l.unlock();
    return;
  }

  ca::waiter<bs::error_code> w;
  auto ex = boost::asio::prefer(
    service.get_executor(),
    boost::asio::execution::outstanding_work.tracked);
  waiting_for_map[e].emplace_back(asio::bind_executor(
				    service.get_executor(),
				    w.ref()),
				  bs::error_code{});
  l.unlock();
  w.wait();
}

void Objecter::_get_latest_version(epoch_t oldest, epoch_t newest,
				   OpCompletion fin,
				   std::unique_lock<ceph::shared_mutex>&& l)
{
  ceph_assert(fin);
  if (osdmap->get_epoch() >= newest) {
    ldout(cct, 10) << __func__ << " latest " << newest << ", have it" << dendl;
    l.unlock();
    asio::defer(service.get_executor(),
		asio::append(std::move(fin), bs::error_code{}));
  } else {
    ldout(cct, 10) << __func__ << " latest " << newest << ", waiting" << dendl;
    _wait_for_new_map(std::move(fin), newest, bs::error_code{});
    l.unlock();
  }
}

void Objecter::maybe_request_map()
{
  shared_lock rl(rwlock);
  _maybe_request_map();
}

void Objecter::_maybe_request_map()
{
  // rwlock is locked
  int flag = 0;
  if (_osdmap_full_flag()
      || osdmap->test_flag(CEPH_OSDMAP_PAUSERD)
      || osdmap->test_flag(CEPH_OSDMAP_PAUSEWR)) {
    ldout(cct, 10) << "_maybe_request_map subscribing (continuous) to next "
      "osd map (FULL flag is set)" << dendl;
  } else {
    ldout(cct, 10)
      << "_maybe_request_map subscribing (onetime) to next osd map" << dendl;
    flag = CEPH_SUBSCRIBE_ONETIME;
  }
  epoch_t epoch = osdmap->get_epoch() ? osdmap->get_epoch()+1 : 0;
  if (monc->sub_want("osdmap", epoch, flag)) {
    monc->renew_subs();
  }
}

void Objecter::_wait_for_new_map(OpCompletion c, epoch_t epoch,
				 bs::error_code ec)
{
  // rwlock is locked unique
  waiting_for_map[epoch].emplace_back(std::move(c), ec);
  _maybe_request_map();
}


/**
 * Use this together with wait_for_map: this is a pre-check to avoid
 * allocating a Context for wait_for_map if we can see that we
 * definitely already have the epoch.
 *
 * This does *not* replace the need to handle the return value of
 * wait_for_map: just because we don't have it in this pre-check
 * doesn't mean we won't have it when calling back into wait_for_map,
 * since the objecter lock is dropped in between.
 */
bool Objecter::have_map(const epoch_t epoch)
{
  shared_lock rl(rwlock);
  if (osdmap->get_epoch() >= epoch) {
    return true;
  } else {
    return false;
  }
}

void Objecter::_kick_requests(OSDSession *session,
			      map<uint64_t, LingerOp *>& lresend)
{
  // rwlock is locked unique

  // clear backoffs
  session->backoffs.clear();
  session->backoffs_by_id.clear();

  // resend ops
  map<ceph_tid_t,Op*> resend;  // resend in tid order
  for (auto p = session->ops.begin(); p != session->ops.end();) {
    Op *op = p->second;
    ++p;
    if (op->should_resend) {
      if (!op->target.paused)
	resend[op->tid] = op;
    } else {
      _op_cancel_map_check(op);
      _cancel_linger_op(op);
    }
  }

  logger->inc(l_osdc_op_resend, resend.size());
  while (!resend.empty()) {
    _send_op(resend.begin()->second);
    resend.erase(resend.begin());
  }

  // resend lingers
  logger->inc(l_osdc_linger_resend, session->linger_ops.size());
  for (auto j = session->linger_ops.begin();
       j != session->linger_ops.end(); ++j) {
    LingerOp *op = j->second;
    op->get();
    ceph_assert(lresend.count(j->first) == 0);
    lresend[j->first] = op;
  }

  // resend commands
  logger->inc(l_osdc_command_resend, session->command_ops.size());
  map<uint64_t,CommandOp*> cresend;  // resend in order
  for (auto k = session->command_ops.begin();
       k != session->command_ops.end(); ++k) {
    cresend[k->first] = k->second;
  }
  while (!cresend.empty()) {
    _send_command(cresend.begin()->second);
    cresend.erase(cresend.begin());
  }
}

void Objecter::_linger_ops_resend(map<uint64_t, LingerOp *>& lresend,
				  unique_lock<ceph::shared_mutex>& ul)
{
  ceph_assert(ul.owns_lock());
  shunique_lock sul(std::move(ul));
  while (!lresend.empty()) {
    LingerOp *op = lresend.begin()->second;
    if (!op->canceled) {
      _send_linger(op, sul);
    }
    op->put();
    lresend.erase(lresend.begin());
  }
  ul = sul.release_to_unique();
}

void Objecter::start_tick()
{
  ceph_assert(tick_event == 0);
  tick_event =
    timer.add_event(ceph::make_timespan(cct->_conf->objecter_tick_interval),
		    &Objecter::tick, this);
}

void Objecter::tick()
{
  shared_lock rl(rwlock);

  ldout(cct, 10) << "tick" << dendl;

  // we are only called by C_Tick
  tick_event = 0;

  if (!initialized) {
    // we raced with shutdown
    ldout(cct, 10) << __func__ << " raced with shutdown" << dendl;
    return;
  }

  set<OSDSession*> toping;


  // look for laggy requests
  auto cutoff = ceph::coarse_mono_clock::now();
  cutoff -= ceph::make_timespan(cct->_conf->objecter_timeout);  // timeout

  unsigned laggy_ops = 0;

  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    auto s = siter->second;
    scoped_lock l(s->lock);
    bool found = false;
    for (auto p = s->ops.begin(); p != s->ops.end(); ++p) {
      auto op = p->second;
      ceph_assert(op->session);
      if (op->stamp < cutoff) {
	ldout(cct, 2) << " tid " << p->first << " on osd." << op->session->osd
		      << " is laggy" << dendl;
	found = true;
	++laggy_ops;
      }
    }
    for (auto p = s->linger_ops.begin();
	p != s->linger_ops.end();
	++p) {
      auto op = p->second;
      std::unique_lock wl(op->watch_lock);
      ceph_assert(op->session);
      ldout(cct, 10) << " pinging osd that serves lingering tid " << p->first
		     << " (osd." << op->session->osd << ")" << dendl;
      found = true;
      if (op->is_watch && op->registered && !op->last_error)
	_send_linger_ping(op);
    }
    for (auto p = s->command_ops.begin();
	p != s->command_ops.end();
	++p) {
      auto op = p->second;
      ceph_assert(op->session);
      ldout(cct, 10) << " pinging osd that serves command tid " << p->first
		     << " (osd." << op->session->osd << ")" << dendl;
      found = true;
    }
    if (found)
      toping.insert(s);
  }
  if (num_homeless_ops || !toping.empty()) {
    _maybe_request_map();
  }

  logger->set(l_osdc_op_laggy, laggy_ops);
  logger->set(l_osdc_osd_laggy, toping.size());

  if (!toping.empty()) {
    // send a ping to these osds, to ensure we detect any session resets
    // (osd reply message policy is lossy)
    for (auto i = toping.begin(); i != toping.end(); ++i) {
      (*i)->con->send_message(new MPing);
    }
  }

  // Make sure we don't reschedule if we wake up after shutdown
  if (initialized) {
    tick_event = timer.reschedule_me(ceph::make_timespan(
				       cct->_conf->objecter_tick_interval));
  }
}

void Objecter::resend_mon_ops()
{
  unique_lock wl(rwlock);

  ldout(cct, 10) << "resend_mon_ops" << dendl;

  for (auto p = poolstat_ops.begin(); p != poolstat_ops.end(); ++p) {
    _poolstat_submit(p->second);
    logger->inc(l_osdc_poolstat_resend);
  }

  for (auto p = statfs_ops.begin(); p != statfs_ops.end(); ++p) {
    _fs_stats_submit(p->second);
    logger->inc(l_osdc_statfs_resend);
  }

  for (auto p = pool_ops.begin(); p != pool_ops.end(); ++p) {
    _pool_op_submit(p->second);
    logger->inc(l_osdc_poolop_resend);
  }

  for (auto p = check_latest_map_ops.begin();
       p != check_latest_map_ops.end();
       ++p) {
    monc->get_version("osdmap", CB_Op_Map_Latest(this, p->second->tid));
  }

  for (auto p = check_latest_map_lingers.begin();
       p != check_latest_map_lingers.end();
       ++p) {
    monc->get_version("osdmap", CB_Linger_Map_Latest(this, p->second->linger_id));
  }

  for (auto p = check_latest_map_commands.begin();
       p != check_latest_map_commands.end();
       ++p) {
    monc->get_version("osdmap", CB_Command_Map_Latest(this, p->second->tid));
  }
}

// read | write ---------------------------

void Objecter::op_submit(Op *op, ceph_tid_t *ptid, int *ctx_budget)
{
  shunique_lock rl(rwlock, ceph::acquire_shared);
  ceph_tid_t tid = 0;
  if (!ptid)
    ptid = &tid;
  op->trace.event("op submit");
  _op_submit_with_budget(op, rl, ptid, ctx_budget);
}

void Objecter::_op_submit_with_budget(Op *op,
				      shunique_lock<ceph::shared_mutex>& sul,
				      ceph_tid_t *ptid,
				      int *ctx_budget)
{
  ceph_assert(initialized);

  ceph_assert(op->ops.size() == op->out_bl.size());
  ceph_assert(op->ops.size() == op->out_rval.size());
  ceph_assert(op->ops.size() == op->out_handler.size());

  // throttle.  before we look at any state, because
  // _take_op_budget() may drop our lock while it blocks.
  if (!op->ctx_budgeted || (ctx_budget && (*ctx_budget == -1))) {
    int op_budget = _take_op_budget(op, sul);
    // take and pass out the budget for the first OP
    // in the context session
    if (ctx_budget && (*ctx_budget == -1)) {
      *ctx_budget = op_budget;
    }
  }

  if (osd_timeout > timespan(0)) {
    if (op->tid == 0)
      op->tid = ++last_tid;
    auto tid = op->tid;
    op->ontimeout = timer.add_event(osd_timeout,
				    [this, tid]() {
				      op_cancel(tid, -ETIMEDOUT); });
  }

  _op_submit(op, sul, ptid);
}

void Objecter::_send_op_account(Op *op)
{
  inflight_ops++;

  // add to gather set(s)
  if (op->has_completion()) {
    num_in_flight++;
  } else {
    ldout(cct, 20) << " note: not requesting reply" << dendl;
  }

  if (op->target.used_replica) {
    logger->inc(l_osdc_replica_read_sent);
  }

  logger->inc(l_osdc_op_active);
  logger->inc(l_osdc_op);
  logger->inc(l_osdc_oplen_avg, op->ops.size());

  if ((op->target.flags & (CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_WRITE)) ==
      (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE))
    logger->inc(l_osdc_op_rmw);
  else if (op->target.flags & CEPH_OSD_FLAG_WRITE)
    logger->inc(l_osdc_op_w);
  else if (op->target.flags & CEPH_OSD_FLAG_READ)
    logger->inc(l_osdc_op_r);

  if (op->target.flags & CEPH_OSD_FLAG_PGOP)
    logger->inc(l_osdc_op_pg);

  for (auto p = op->ops.begin(); p != op->ops.end(); ++p) {
    int code = l_osdc_osdop_other;
    switch (p->op.op) {
    case CEPH_OSD_OP_STAT: code = l_osdc_osdop_stat; break;
    case CEPH_OSD_OP_CREATE: code = l_osdc_osdop_create; break;
    case CEPH_OSD_OP_READ: code = l_osdc_osdop_read; break;
    case CEPH_OSD_OP_WRITE: code = l_osdc_osdop_write; break;
    case CEPH_OSD_OP_WRITEFULL: code = l_osdc_osdop_writefull; break;
    case CEPH_OSD_OP_WRITESAME: code = l_osdc_osdop_writesame; break;
    case CEPH_OSD_OP_APPEND: code = l_osdc_osdop_append; break;
    case CEPH_OSD_OP_ZERO: code = l_osdc_osdop_zero; break;
    case CEPH_OSD_OP_TRUNCATE: code = l_osdc_osdop_truncate; break;
    case CEPH_OSD_OP_DELETE: code = l_osdc_osdop_delete; break;
    case CEPH_OSD_OP_MAPEXT: code = l_osdc_osdop_mapext; break;
    case CEPH_OSD_OP_SPARSE_READ: code = l_osdc_osdop_sparse_read; break;
    case CEPH_OSD_OP_GETXATTR: code = l_osdc_osdop_getxattr; break;
    case CEPH_OSD_OP_SETXATTR: code = l_osdc_osdop_setxattr; break;
    case CEPH_OSD_OP_CMPXATTR: code = l_osdc_osdop_cmpxattr; break;
    case CEPH_OSD_OP_RMXATTR: code = l_osdc_osdop_rmxattr; break;
    case CEPH_OSD_OP_RESETXATTRS: code = l_osdc_osdop_resetxattrs; break;

    // OMAP read operations
    case CEPH_OSD_OP_OMAPGETVALS:
    case CEPH_OSD_OP_OMAPGETKEYS:
    case CEPH_OSD_OP_OMAPGETHEADER:
    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
    case CEPH_OSD_OP_OMAP_CMP: code = l_osdc_osdop_omap_rd; break;

    // OMAP write operations
    case CEPH_OSD_OP_OMAPSETVALS:
    case CEPH_OSD_OP_OMAPSETHEADER: code = l_osdc_osdop_omap_wr; break;

    // OMAP del operations
    case CEPH_OSD_OP_OMAPCLEAR:
    case CEPH_OSD_OP_OMAPRMKEYS: code = l_osdc_osdop_omap_del; break;

    case CEPH_OSD_OP_CALL: code = l_osdc_osdop_call; break;
    case CEPH_OSD_OP_WATCH: code = l_osdc_osdop_watch; break;
    case CEPH_OSD_OP_NOTIFY: code = l_osdc_osdop_notify; break;
    }
    if (code)
      logger->inc(code);
  }
}

void Objecter::_op_submit(Op *op, shunique_lock<ceph::shared_mutex>& sul, ceph_tid_t *ptid)
{
  // rwlock is locked

  ldout(cct, 10) << __func__ << " op " << op << dendl;

  // pick target
  ceph_assert(op->session == NULL);
  OSDSession *s = NULL;

  bool check_for_latest_map = false;
  int r = _calc_target(&op->target, nullptr);
  switch(r) {
  case RECALC_OP_TARGET_POOL_DNE:
    check_for_latest_map = true;
    break;
  case RECALC_OP_TARGET_POOL_EIO:
    if (op->has_completion()) {
      op->complete(osdc_errc::pool_eio, -EIO, service.get_executor());
    }
    return;
  }

  // Try to get a session, including a retry if we need to take write lock
  r = _get_session(op->target.osd, &s, sul);
  if (r == -EAGAIN ||
      (check_for_latest_map && sul.owns_lock_shared()) ||
      cct->_conf->objecter_debug_inject_relock_delay) {
    epoch_t orig_epoch = osdmap->get_epoch();
    sul.unlock();
    if (cct->_conf->objecter_debug_inject_relock_delay) {
      sleep(1);
    }
    sul.lock();
    if (orig_epoch != osdmap->get_epoch()) {
      // map changed; recalculate mapping
      ldout(cct, 10) << __func__ << " relock raced with osdmap, recalc target"
		     << dendl;
      check_for_latest_map = _calc_target(&op->target, nullptr)
	== RECALC_OP_TARGET_POOL_DNE;
      if (s) {
	put_session(s);
	s = NULL;
	r = -EAGAIN;
      }
    }
  }
  if (r == -EAGAIN) {
    ceph_assert(s == NULL);
    r = _get_session(op->target.osd, &s, sul);
  }
  ceph_assert(r == 0);
  ceph_assert(s);  // may be homeless

  _send_op_account(op);

  // send?

  ceph_assert(op->target.flags & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE));

  bool need_send = false;
  if (op->target.paused) {
    ldout(cct, 10) << " tid " << op->tid << " op " << op << " is paused"
		   << dendl;
    _maybe_request_map();
  } else if (!s->is_homeless()) {
    need_send = true;
  } else {
    _maybe_request_map();
  }

  unique_lock sl(s->lock);
  if (op->tid == 0)
    op->tid = ++last_tid;

  ldout(cct, 10) << "_op_submit oid " << op->target.base_oid
		 << " '" << op->target.base_oloc << "' '"
		 << op->target.target_oloc << "' " << op->ops << " tid "
		 << op->tid << " osd." << (!s->is_homeless() ? s->osd : -1)
		 << dendl;

  _session_op_assign(s, op);

  if (need_send) {
    _send_op(op);
  }

  // Last chance to touch Op here, after giving up session lock it can
  // be freed at any time by response handler.
  ceph_tid_t tid = op->tid;
  if (check_for_latest_map) {
    _send_op_map_check(op);
  }
  if (ptid)
    *ptid = tid;
  op = NULL;

  sl.unlock();
  put_session(s);

  ldout(cct, 5) << num_in_flight << " in flight" << dendl;
}

int Objecter::op_cancel(OSDSession *s, ceph_tid_t tid, int r)
{
  ceph_assert(initialized);

  unique_lock sl(s->lock);

  auto p = s->ops.find(tid);
  if (p == s->ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne in session "
		   << s->osd << dendl;
    return -ENOENT;
  }

#if 0
  if (s->con) {
    ldout(cct, 20) << " revoking rx ceph::buffer for " << tid
		   << " on " << s->con << dendl;
    s->con->revoke_rx_buffer(tid);
  }
#endif

  ldout(cct, 10) << __func__ << " tid " << tid << " in session " << s->osd
		 << dendl;
  Op *op = p->second;
  if (op->has_completion()) {
    num_in_flight--;
    op->complete(osdcode(r), r, service.get_executor());
  }
  _op_cancel_map_check(op);
  _finish_op(op, r);
  sl.unlock();

  return 0;
}

int Objecter::op_cancel(ceph_tid_t tid, int r)
{
  int ret = 0;

  unique_lock wl(rwlock);
  ret = _op_cancel(tid, r);

  return ret;
}

int Objecter::op_cancel(const vector<ceph_tid_t>& tids, int r)
{
  unique_lock wl(rwlock);
  ldout(cct,10) << __func__ << " " << tids << dendl;
  for (auto tid : tids) {
    _op_cancel(tid, r);
  }
  return 0;
}

int Objecter::_op_cancel(ceph_tid_t tid, int r)
{
  int ret = 0;

  ldout(cct, 5) << __func__ << ": cancelling tid " << tid << " r=" << r
		<< dendl;

start:

  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    shared_lock sl(s->lock);
    if (s->ops.find(tid) != s->ops.end()) {
      sl.unlock();
      ret = op_cancel(s, tid, r);
      if (ret == -ENOENT) {
	/* oh no! raced, maybe tid moved to another session, restarting */
	goto start;
      }
      return ret;
    }
  }

  ldout(cct, 5) << __func__ << ": tid " << tid
		<< " not found in live sessions" << dendl;

  // Handle case where the op is in homeless session
  shared_lock sl(homeless_session->lock);
  if (homeless_session->ops.find(tid) != homeless_session->ops.end()) {
    sl.unlock();
    ret = op_cancel(homeless_session, tid, r);
    if (ret == -ENOENT) {
      /* oh no! raced, maybe tid moved to another session, restarting */
      goto start;
    } else {
      return ret;
    }
  } else {
    sl.unlock();
  }

  ldout(cct, 5) << __func__ << ": tid " << tid
		<< " not found in homeless session" << dendl;

  return ret;
}


epoch_t Objecter::op_cancel_writes(int r, int64_t pool)
{
  unique_lock wl(rwlock);

  std::vector<ceph_tid_t> to_cancel;
  bool found = false;

  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    shared_lock sl(s->lock);
    for (auto op_i = s->ops.begin();
	 op_i != s->ops.end(); ++op_i) {
      if (op_i->second->target.flags & CEPH_OSD_FLAG_WRITE
	  && (pool == -1 || op_i->second->target.target_oloc.pool == pool)) {
	to_cancel.push_back(op_i->first);
      }
    }
    sl.unlock();

    for (auto titer = to_cancel.begin(); titer != to_cancel.end(); ++titer) {
      int cancel_result = op_cancel(s, *titer, r);
      // We hold rwlock across search and cancellation, so cancels
      // should always succeed
      ceph_assert(cancel_result == 0);
    }
    if (!found && to_cancel.size())
      found = true;
    to_cancel.clear();
  }

  const epoch_t epoch = osdmap->get_epoch();

  wl.unlock();

  if (found) {
    return epoch;
  } else {
    return -1;
  }
}

bool Objecter::is_pg_changed(
  int oldprimary,
  const vector<int>& oldacting,
  int newprimary,
  const vector<int>& newacting,
  bool any_change)
{
  if (OSDMap::primary_changed_broken( // https://tracker.ceph.com/issues/43213
	oldprimary,
	oldacting,
	newprimary,
	newacting))
    return true;
  if (any_change && oldacting != newacting)
    return true;
  return false;      // same primary (tho replicas may have changed)
}

bool Objecter::target_should_be_paused(op_target_t *t)
{
  const pg_pool_t *pi = osdmap->get_pg_pool(t->base_oloc.pool);
  bool pauserd = osdmap->test_flag(CEPH_OSDMAP_PAUSERD);
  bool pausewr = osdmap->test_flag(CEPH_OSDMAP_PAUSEWR) ||
    (t->respects_full() && (_osdmap_full_flag() || _osdmap_pool_full(*pi)));

  return (t->flags & CEPH_OSD_FLAG_READ && pauserd) ||
    (t->flags & CEPH_OSD_FLAG_WRITE && pausewr) ||
    (osdmap->get_epoch() < epoch_barrier);
}

/**
 * Locking public accessor for _osdmap_full_flag
 */
bool Objecter::osdmap_full_flag() const
{
  shared_lock rl(rwlock);

  return _osdmap_full_flag();
}

bool Objecter::osdmap_pool_full(const int64_t pool_id) const
{
  shared_lock rl(rwlock);

  if (_osdmap_full_flag()) {
    return true;
  }

  return _osdmap_pool_full(pool_id);
}

bool Objecter::_osdmap_pool_full(const int64_t pool_id) const
{
  const pg_pool_t *pool = osdmap->get_pg_pool(pool_id);
  if (pool == NULL) {
    ldout(cct, 4) << __func__ << ": DNE pool " << pool_id << dendl;
    return false;
  }

  return _osdmap_pool_full(*pool);
}

bool Objecter::_osdmap_has_pool_full() const
{
  for (auto it = osdmap->get_pools().begin();
       it != osdmap->get_pools().end(); ++it) {
    if (_osdmap_pool_full(it->second))
      return true;
  }
  return false;
}

/**
 * Wrapper around osdmap->test_flag for special handling of the FULL flag.
 */
bool Objecter::_osdmap_full_flag() const
{
  // Ignore the FULL flag if the caller does not have honor_osdmap_full
  return osdmap->test_flag(CEPH_OSDMAP_FULL) && honor_pool_full;
}

void Objecter::update_pool_full_map(map<int64_t, bool>& pool_full_map)
{
  for (map<int64_t, pg_pool_t>::const_iterator it
	 = osdmap->get_pools().begin();
       it != osdmap->get_pools().end(); ++it) {
    if (pool_full_map.find(it->first) == pool_full_map.end()) {
      pool_full_map[it->first] = _osdmap_pool_full(it->second);
    } else {
      pool_full_map[it->first] = _osdmap_pool_full(it->second) ||
	pool_full_map[it->first];
    }
  }
}

int64_t Objecter::get_object_hash_position(int64_t pool, const string& key,
					   const string& ns)
{
  shared_lock rl(rwlock);
  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -ENOENT;
  return p->hash_key(key, ns);
}

int64_t Objecter::get_object_pg_hash_position(int64_t pool, const string& key,
					      const string& ns)
{
  shared_lock rl(rwlock);
  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p)
    return -ENOENT;
  return p->raw_hash_to_pg(p->hash_key(key, ns));
}

void Objecter::_prune_snapc(
  const mempool::osdmap::map<int64_t,
  snap_interval_set_t>& new_removed_snaps,
  Op *op)
{
  bool match = false;
  auto i = new_removed_snaps.find(op->target.base_pgid.pool());
  if (i != new_removed_snaps.end()) {
    for (auto s : op->snapc.snaps) {
      if (i->second.contains(s)) {
	match = true;
	break;
      }
    }
    if (match) {
      vector<snapid_t> new_snaps;
      for (auto s : op->snapc.snaps) {
	if (!i->second.contains(s)) {
	  new_snaps.push_back(s);
	}
      }
      op->snapc.snaps.swap(new_snaps);
      ldout(cct,10) << __func__ << " op " << op->tid << " snapc " << op->snapc
		    << " (was " << new_snaps << ")" << dendl;
    }
  }
}

int Objecter::_calc_target(op_target_t *t, Connection *con, bool any_change)
{
  // rwlock is locked
  bool is_read = t->flags & CEPH_OSD_FLAG_READ;
  bool is_write = t->flags & CEPH_OSD_FLAG_WRITE;
  t->epoch = osdmap->get_epoch();
  ldout(cct,20) << __func__ << " epoch " << t->epoch
		<< " base " << t->base_oid << " " << t->base_oloc
		<< " precalc_pgid " << (int)t->precalc_pgid
		<< " pgid " << t->base_pgid
		<< (is_read ? " is_read" : "")
		<< (is_write ? " is_write" : "")
		<< dendl;

  const pg_pool_t *pi = osdmap->get_pg_pool(t->base_oloc.pool);
  if (!pi) {
    t->osd = -1;
    return RECALC_OP_TARGET_POOL_DNE;
  }

  if (pi->has_flag(pg_pool_t::FLAG_EIO)) {
    return RECALC_OP_TARGET_POOL_EIO;
  }

  ldout(cct,30) << __func__ << "  base pi " << pi
		<< " pg_num " << pi->get_pg_num() << dendl;

  bool force_resend = false;
  if (osdmap->get_epoch() == pi->last_force_op_resend) {
    if (t->last_force_resend < pi->last_force_op_resend) {
      t->last_force_resend = pi->last_force_op_resend;
      force_resend = true;
    } else if (t->last_force_resend == 0) {
      force_resend = true;
    }
  }

  // apply tiering
  t->target_oid = t->base_oid;
  t->target_oloc = t->base_oloc;
  if ((t->flags & CEPH_OSD_FLAG_IGNORE_OVERLAY) == 0) {
    if (is_read && pi->has_read_tier())
      t->target_oloc.pool = pi->read_tier;
    if (is_write && pi->has_write_tier())
      t->target_oloc.pool = pi->write_tier;
    pi = osdmap->get_pg_pool(t->target_oloc.pool);
    if (!pi) {
      t->osd = -1;
      return RECALC_OP_TARGET_POOL_DNE;
    }
  }

  pg_t pgid;
  if (t->precalc_pgid) {
    ceph_assert(t->flags & CEPH_OSD_FLAG_IGNORE_OVERLAY);
    ceph_assert(t->base_oid.name.empty()); // make sure this is a pg op
    ceph_assert(t->base_oloc.pool == (int64_t)t->base_pgid.pool());
    pgid = t->base_pgid;
  } else {
    int ret = osdmap->object_locator_to_pg(t->target_oid, t->target_oloc,
					   pgid);
    if (ret == -ENOENT) {
      t->osd = -1;
      return RECALC_OP_TARGET_POOL_DNE;
    }
  }
  ldout(cct,20) << __func__ << " target " << t->target_oid << " "
		<< t->target_oloc << " -> pgid " << pgid << dendl;
  ldout(cct,30) << __func__ << "  target pi " << pi
		<< " pg_num " << pi->get_pg_num() << dendl;
  t->pool_ever_existed = true;

  int size = pi->size;
  int min_size = pi->min_size;
  unsigned pg_num = pi->get_pg_num();
  unsigned pg_num_mask = pi->get_pg_num_mask();
  unsigned pg_num_pending = pi->get_pg_num_pending();
  int up_primary, acting_primary;
  vector<int> up, acting;
  ps_t actual_ps = ceph_stable_mod(pgid.ps(), pg_num, pg_num_mask);
  pg_t actual_pgid(actual_ps, pgid.pool());
  if (!lookup_pg_mapping(actual_pgid, osdmap->get_epoch(), &up, &up_primary,
                         &acting, &acting_primary)) {
    osdmap->pg_to_up_acting_osds(actual_pgid, &up, &up_primary,
                                 &acting, &acting_primary);
    pg_mapping_t pg_mapping(osdmap->get_epoch(),
                            up, up_primary, acting, acting_primary);
    update_pg_mapping(actual_pgid, std::move(pg_mapping));
  }
  bool sort_bitwise = osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE);
  bool recovery_deletes = osdmap->test_flag(CEPH_OSDMAP_RECOVERY_DELETES);
  unsigned prev_seed = ceph_stable_mod(pgid.ps(), t->pg_num, t->pg_num_mask);
  pg_t prev_pgid(prev_seed, pgid.pool());
  if (any_change && PastIntervals::is_new_interval(
	t->acting_primary,
	acting_primary,
	t->acting,
	acting,
	t->up_primary,
	up_primary,
	t->up,
	up,
	t->size,
	size,
	t->min_size,
	min_size,
	t->pg_num,
	pg_num,
	t->pg_num_pending,
	pg_num_pending,
	t->sort_bitwise,
	sort_bitwise,
	t->recovery_deletes,
	recovery_deletes,
	t->peering_crush_bucket_count,
	pi->peering_crush_bucket_count,
	t->peering_crush_bucket_target,
	pi->peering_crush_bucket_target,
	t->peering_crush_bucket_barrier,
	pi->peering_crush_bucket_barrier,
	t->peering_crush_mandatory_member,
	pi->peering_crush_mandatory_member,
	prev_pgid)) {
    force_resend = true;
  }

  bool unpaused = false;
  bool should_be_paused = target_should_be_paused(t);
  if (t->paused && !should_be_paused) {
    unpaused = true;
  }
  if (t->paused != should_be_paused) {
    ldout(cct, 10) << __func__ << " paused " << t->paused
		   << " -> " << should_be_paused << dendl;
    t->paused = should_be_paused;
  }

  bool legacy_change =
    t->pgid != pgid ||
      is_pg_changed(
	t->acting_primary, t->acting, acting_primary, acting,
	t->used_replica || any_change);
  bool split_or_merge = false;
  if (t->pg_num) {
    split_or_merge =
      prev_pgid.is_split(t->pg_num, pg_num, nullptr) ||
      prev_pgid.is_merge_source(t->pg_num, pg_num, nullptr) ||
      prev_pgid.is_merge_target(t->pg_num, pg_num);
  }

  if (legacy_change || split_or_merge || force_resend) {
    t->pgid = pgid;
    t->acting = std::move(acting);
    t->acting_primary = acting_primary;
    t->up_primary = up_primary;
    t->up = std::move(up);
    t->size = size;
    t->min_size = min_size;
    t->pg_num = pg_num;
    t->pg_num_mask = pg_num_mask;
    t->pg_num_pending = pg_num_pending;
    spg_t spgid(actual_pgid);
    if (pi->is_erasure()) {
      for (uint8_t i = 0; i < t->acting.size(); ++i) {
        if (t->acting[i] == acting_primary) {
          spgid.reset_shard(shard_id_t(i));
          break;
        }
      }
    }
    t->actual_pgid = spgid;
    t->sort_bitwise = sort_bitwise;
    t->recovery_deletes = recovery_deletes;
    t->peering_crush_bucket_count = pi->peering_crush_bucket_count;
    t->peering_crush_bucket_target = pi->peering_crush_bucket_target;
    t->peering_crush_bucket_barrier = pi->peering_crush_bucket_barrier;
    t->peering_crush_mandatory_member = pi->peering_crush_mandatory_member;
    ldout(cct, 10) << __func__ << " "
		   << " raw pgid " << pgid << " -> actual " << t->actual_pgid
		   << " acting " << t->acting
		   << " primary " << acting_primary << dendl;
    t->used_replica = false;
    if ((t->flags & (CEPH_OSD_FLAG_BALANCE_READS |
                     CEPH_OSD_FLAG_LOCALIZE_READS)) &&
        !is_write && pi->is_replicated() && t->acting.size() > 1) {
      int osd;
      ceph_assert(is_read && t->acting[0] == acting_primary);
      if (t->flags & CEPH_OSD_FLAG_BALANCE_READS) {
	int p = rand() % t->acting.size();
	if (p)
	  t->used_replica = true;
	osd = t->acting[p];
	ldout(cct, 10) << " chose random osd." << osd << " of " << t->acting
		       << dendl;
      } else {
	// look for a local replica.  prefer the primary if the
	// distance is the same.
	int best = -1;
	int best_locality = 0;
	for (unsigned i = 0; i < t->acting.size(); ++i) {
	  int locality = osdmap->crush->get_common_ancestor_distance(
		 cct, t->acting[i], crush_location);
	  ldout(cct, 20) << __func__ << " localize: rank " << i
			 << " osd." << t->acting[i]
			 << " locality " << locality << dendl;
	  if (i == 0 ||
	      (locality >= 0 && best_locality >= 0 &&
	       locality < best_locality) ||
	      (best_locality < 0 && locality >= 0)) {
	    best = i;
	    best_locality = locality;
	    if (i)
	      t->used_replica = true;
	  }
	}
	ceph_assert(best >= 0);
	osd = t->acting[best];
      }
      t->osd = osd;
    } else {
      t->osd = acting_primary;
    }
  }
  if (legacy_change || unpaused || force_resend) {
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  if (split_or_merge &&
      (osdmap->require_osd_release >= ceph_release_t::luminous ||
       HAVE_FEATURE(osdmap->get_xinfo(acting_primary).features,
		    RESEND_ON_SPLIT))) {
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  return RECALC_OP_TARGET_NO_ACTION;
}

int Objecter::_map_session(op_target_t *target, OSDSession **s,
			   shunique_lock<ceph::shared_mutex>& sul)
{
  _calc_target(target, nullptr);
  return _get_session(target->osd, s, sul);
}

void Objecter::_session_op_assign(OSDSession *to, Op *op)
{
  // to->lock is locked
  ceph_assert(op->session == NULL);
  ceph_assert(op->tid);

  get_session(to);
  op->session = to;
  to->ops[op->tid] = op;

  if (to->is_homeless()) {
    num_homeless_ops++;
  }

  ldout(cct, 15) << __func__ << " " << to->osd << " " << op->tid << dendl;
}

void Objecter::_session_op_remove(OSDSession *from, Op *op)
{
  ceph_assert(op->session == from);
  // from->lock is locked

  if (from->is_homeless()) {
    num_homeless_ops--;
  }

  from->ops.erase(op->tid);
  put_session(from);
  op->session = NULL;

  ldout(cct, 15) << __func__ << " " << from->osd << " " << op->tid << dendl;
}

void Objecter::_session_linger_op_assign(OSDSession *to, LingerOp *op)
{
  // to lock is locked unique
  ceph_assert(op->session == NULL);

  if (to->is_homeless()) {
    num_homeless_ops++;
  }

  get_session(to);
  op->session = to;
  to->linger_ops[op->linger_id] = op;

  ldout(cct, 15) << __func__ << " " << to->osd << " " << op->linger_id
		 << dendl;
}

void Objecter::_session_linger_op_remove(OSDSession *from, LingerOp *op)
{
  ceph_assert(from == op->session);
  // from->lock is locked unique

  if (from->is_homeless()) {
    num_homeless_ops--;
  }

  from->linger_ops.erase(op->linger_id);
  put_session(from);
  op->session = NULL;

  ldout(cct, 15) << __func__ << " " << from->osd << " " << op->linger_id
		 << dendl;
}

void Objecter::_session_command_op_remove(OSDSession *from, CommandOp *op)
{
  ceph_assert(from == op->session);
  // from->lock is locked

  if (from->is_homeless()) {
    num_homeless_ops--;
  }

  from->command_ops.erase(op->tid);
  put_session(from);
  op->session = NULL;

  ldout(cct, 15) << __func__ << " " << from->osd << " " << op->tid << dendl;
}

void Objecter::_session_command_op_assign(OSDSession *to, CommandOp *op)
{
  // to->lock is locked
  ceph_assert(op->session == NULL);
  ceph_assert(op->tid);

  if (to->is_homeless()) {
    num_homeless_ops++;
  }

  get_session(to);
  op->session = to;
  to->command_ops[op->tid] = op;

  ldout(cct, 15) << __func__ << " " << to->osd << " " << op->tid << dendl;
}

int Objecter::_recalc_linger_op_target(LingerOp *linger_op,
				       shunique_lock<ceph::shared_mutex>& sul)
{
  // rwlock is locked unique

  int r = _calc_target(&linger_op->target, nullptr, true);
  if (r == RECALC_OP_TARGET_NEED_RESEND) {
    ldout(cct, 10) << "recalc_linger_op_target tid " << linger_op->linger_id
		   << " pgid " << linger_op->target.pgid
		   << " acting " << linger_op->target.acting << dendl;

    OSDSession *s = NULL;
    r = _get_session(linger_op->target.osd, &s, sul);
    ceph_assert(r == 0);

    if (linger_op->session != s) {
      // NB locking two sessions (s and linger_op->session) at the
      // same time here is only safe because we are the only one that
      // takes two, and we are holding rwlock for write.  We use
      // std::shared_mutex in OSDSession because lockdep doesn't know
      // that.
      unique_lock sl(s->lock);
      _session_linger_op_remove(linger_op->session, linger_op);
      _session_linger_op_assign(s, linger_op);
    }

    put_session(s);
    return RECALC_OP_TARGET_NEED_RESEND;
  }
  return r;
}

void Objecter::_cancel_linger_op(Op *op)
{
  ldout(cct, 15) << "cancel_op " << op->tid << dendl;

  ceph_assert(!op->should_resend);
  if (op->has_completion()) {
    op->onfinish = nullptr;
    num_in_flight--;
  }

  _finish_op(op, 0);
}

void Objecter::_finish_op(Op *op, int r)
{
  ldout(cct, 15) << __func__ << " " << op->tid << dendl;

  // op->session->lock is locked unique or op->session is null

  if (!op->ctx_budgeted && op->budget >= 0) {
    put_op_budget_bytes(op->budget);
    op->budget = -1;
  }

  if (op->ontimeout && r != -ETIMEDOUT)
    timer.cancel_event(op->ontimeout);

  if (op->session) {
    _session_op_remove(op->session, op);
  }

  logger->dec(l_osdc_op_active);

  ceph_assert(check_latest_map_ops.find(op->tid) == check_latest_map_ops.end());

  inflight_ops--;

  op->put();
}

Objecter::MOSDOp *Objecter::_prepare_osd_op(Op *op)
{
  // rwlock is locked

  int flags = op->target.flags;
  flags |= CEPH_OSD_FLAG_KNOWN_REDIR;
  flags |= CEPH_OSD_FLAG_SUPPORTSPOOLEIO;

  // Nothing checks this any longer, but needed for compatibility with
  // pre-luminous osds
  flags |= CEPH_OSD_FLAG_ONDISK;

  if (!honor_pool_full)
    flags |= CEPH_OSD_FLAG_FULL_FORCE;

  op->target.paused = false;
  op->stamp = ceph::coarse_mono_clock::now();

  hobject_t hobj = op->target.get_hobj();
  auto m = new MOSDOp(client_inc, op->tid,
		      hobj, op->target.actual_pgid,
		      osdmap->get_epoch(),
		      flags, op->features);

  m->set_snapid(op->snapid);
  m->set_snap_seq(op->snapc.seq);
  m->set_snaps(op->snapc.snaps);

  m->ops = op->ops;
  m->set_mtime(op->mtime);
  m->set_retry_attempt(op->attempts++);

  if (!op->trace.valid() && cct->_conf->osdc_blkin_trace_all) {
    op->trace.init("op", &trace_endpoint);
  }

  if (op->priority)
    m->set_priority(op->priority);
  else
    m->set_priority(cct->_conf->osd_client_op_priority);

  if (op->reqid != osd_reqid_t()) {
    m->set_reqid(op->reqid);
  }

  if (op->otel_trace && op->otel_trace->IsValid()) {
     m->otel_trace = jspan_context(*op->otel_trace);
  }

  logger->inc(l_osdc_op_send);
  ssize_t sum = 0;
  for (unsigned i = 0; i < m->ops.size(); i++) {
    sum += m->ops[i].indata.length();
  }
  logger->inc(l_osdc_op_send_bytes, sum);

  return m;
}

void Objecter::_send_op(Op *op)
{
  // rwlock is locked
  // op->session->lock is locked

  // backoff?
  auto p = op->session->backoffs.find(op->target.actual_pgid);
  if (p != op->session->backoffs.end()) {
    hobject_t hoid = op->target.get_hobj();
    auto q = p->second.lower_bound(hoid);
    if (q != p->second.begin()) {
      --q;
      if (hoid >= q->second.end) {
	++q;
      }
    }
    if (q != p->second.end()) {
      ldout(cct, 20) << __func__ << " ? " << q->first << " [" << q->second.begin
		     << "," << q->second.end << ")" << dendl;
      int r = cmp(hoid, q->second.begin);
      if (r == 0 || (r > 0 && hoid < q->second.end)) {
	ldout(cct, 10) << __func__ << " backoff " << op->target.actual_pgid
		       << " id " << q->second.id << " on " << hoid
		       << ", queuing " << op << " tid " << op->tid << dendl;
	return;
      }
    }
  }

  ceph_assert(op->tid > 0);
  MOSDOp *m = _prepare_osd_op(op);

  if (op->target.actual_pgid != m->get_spg()) {
    ldout(cct, 10) << __func__ << " " << op->tid << " pgid change from "
		   << m->get_spg() << " to " << op->target.actual_pgid
		   << ", updating and reencoding" << dendl;
    m->set_spg(op->target.actual_pgid);
    m->clear_payload();  // reencode
  }

  ldout(cct, 15) << "_send_op " << op->tid << " to "
		 << op->target.actual_pgid << " on osd." << op->session->osd
		 << dendl;

  ConnectionRef con = op->session->con;
  ceph_assert(con);

#if 0
  // preallocated rx ceph::buffer?
  if (op->con) {
    ldout(cct, 20) << " revoking rx ceph::buffer for " << op->tid << " on "
		   << op->con << dendl;
    op->con->revoke_rx_buffer(op->tid);
  }
  if (op->outbl &&
      op->ontimeout == 0 &&  // only post rx_buffer if no timeout; see #9582
      op->outbl->length()) {
    op->outbl->invalidate_crc();  // messenger writes through c_str()
    ldout(cct, 20) << " posting rx ceph::buffer for " << op->tid << " on " << con
		   << dendl;
    op->con = con;
    op->con->post_rx_buffer(op->tid, *op->outbl);
  }
#endif

  op->incarnation = op->session->incarnation;

  if (op->trace.valid()) {
    m->trace.init("op msg", nullptr, &op->trace);
  }
  op->session->con->send_message(m);
}

int Objecter::calc_op_budget(const bc::small_vector_base<OSDOp>& ops)
{
  int op_budget = 0;
  for (auto i = ops.begin(); i != ops.end(); ++i) {
    if (i->op.op & CEPH_OSD_OP_MODE_WR) {
      op_budget += i->indata.length();
    } else if (ceph_osd_op_mode_read(i->op.op)) {
      if (ceph_osd_op_uses_extent(i->op.op)) {
        if ((int64_t)i->op.extent.length > 0)
          op_budget += (int64_t)i->op.extent.length;
      } else if (ceph_osd_op_type_attr(i->op.op)) {
        op_budget += i->op.xattr.name_len + i->op.xattr.value_len;
      }
    }
  }
  return op_budget;
}

void Objecter::_throttle_op(Op *op,
			    shunique_lock<ceph::shared_mutex>& sul,
			    int op_budget)
{
  ceph_assert(sul && sul.mutex() == &rwlock);
  bool locked_for_write = sul.owns_lock();

  if (!op_budget)
    op_budget = calc_op_budget(op->ops);
  if (!op_throttle_bytes.get_or_fail(op_budget)) { //couldn't take right now
    sul.unlock();
    op_throttle_bytes.get(op_budget);
    if (locked_for_write)
      sul.lock();
    else
      sul.lock_shared();
  }
  if (!op_throttle_ops.get_or_fail(1)) { //couldn't take right now
    sul.unlock();
    op_throttle_ops.get(1);
    if (locked_for_write)
      sul.lock();
    else
      sul.lock_shared();
  }
}

int Objecter::take_linger_budget(LingerOp *info)
{
  return 1;
}

/* This function DOES put the passed message before returning */
void Objecter::handle_osd_op_reply(MOSDOpReply *m)
{
  ldout(cct, 10) << "in handle_osd_op_reply" << dendl;

  // get pio
  ceph_tid_t tid = m->get_tid();

  shunique_lock sul(rwlock, ceph::acquire_shared);
  if (!initialized) {
    m->put();
    return;
  }

  ConnectionRef con = m->get_connection();
  auto priv = con->get_priv();
  auto s = static_cast<OSDSession*>(priv.get());
  if (!s || s->con != con) {
    ldout(cct, 7) << __func__ << " no session on con " << con << dendl;
    m->put();
    return;
  }

  unique_lock sl(s->lock);

  map<ceph_tid_t, Op *>::iterator iter = s->ops.find(tid);
  if (iter == s->ops.end()) {
    ldout(cct, 7) << "handle_osd_op_reply " << tid
		  << (m->is_ondisk() ? " ondisk" : (m->is_onnvram() ?
						    " onnvram" : " ack"))
		  << " ... stray" << dendl;
    sl.unlock();
    m->put();
    return;
  }

  ldout(cct, 7) << "handle_osd_op_reply " << tid
		<< (m->is_ondisk() ? " ondisk" :
		    (m->is_onnvram() ? " onnvram" : " ack"))
		<< " uv " << m->get_user_version()
		<< " in " << m->get_pg()
		<< " attempt " << m->get_retry_attempt()
		<< dendl;
  Op *op = iter->second;
  op->trace.event("osd op reply");

  if (retry_writes_after_first_reply && op->attempts == 1 &&
      (op->target.flags & CEPH_OSD_FLAG_WRITE)) {
    ldout(cct, 7) << "retrying write after first reply: " << tid << dendl;
    if (op->has_completion()) {
      num_in_flight--;
    }
    _session_op_remove(s, op);
    sl.unlock();

    _op_submit(op, sul, NULL);
    m->put();
    return;
  }

  if (m->get_retry_attempt() >= 0) {
    if (m->get_retry_attempt() != (op->attempts - 1)) {
      ldout(cct, 7) << " ignoring reply from attempt "
		    << m->get_retry_attempt()
		    << " from " << m->get_source_inst()
		    << "; last attempt " << (op->attempts - 1) << " sent to "
		    << op->session->con->get_peer_addr() << dendl;
      m->put();
      sl.unlock();
      return;
    }
  } else {
    // we don't know the request attempt because the server is old, so
    // just accept this one.  we may do ACK callbacks we shouldn't
    // have, but that is better than doing callbacks out of order.
  }

  decltype(op->onfinish) onfinish;

  int rc = m->get_result();

  if (m->is_redirect_reply()) {
    ldout(cct, 5) << " got redirect reply; redirecting" << dendl;
    if (op->has_completion())
      num_in_flight--;
    _session_op_remove(s, op);
    sl.unlock();

    // FIXME: two redirects could race and reorder

    op->tid = 0;
    m->get_redirect().combine_with_locator(op->target.target_oloc,
					   op->target.target_oid.name);
    op->target.flags |= (CEPH_OSD_FLAG_REDIRECTED |
			 CEPH_OSD_FLAG_IGNORE_CACHE |
			 CEPH_OSD_FLAG_IGNORE_OVERLAY);
    _op_submit(op, sul, NULL);
    m->put();
    return;
  }

  if (op->target.flags & (CEPH_OSD_FLAG_BALANCE_READS |
			  CEPH_OSD_FLAG_LOCALIZE_READS)) {
    if (rc == -EAGAIN) {
      logger->inc(l_osdc_replica_read_bounced);
    } else {
      logger->inc(l_osdc_replica_read_completed);
    }
  }

  if (rc == -EAGAIN) {
    ldout(cct, 7) << " got -EAGAIN, resubmitting" << dendl;
    if (op->has_completion())
      num_in_flight--;
    _session_op_remove(s, op);
    sl.unlock();

    op->tid = 0;
    op->target.flags &= ~(CEPH_OSD_FLAG_BALANCE_READS |
			  CEPH_OSD_FLAG_LOCALIZE_READS);
    op->target.pgid = pg_t();
    _op_submit(op, sul, NULL);
    m->put();
    return;
  }

  sul.unlock();

  if (op->objver)
    *op->objver = m->get_user_version();
  if (op->reply_epoch)
    *op->reply_epoch = m->get_map_epoch();
  if (op->data_offset)
    *op->data_offset = m->get_header().data_off;

  // got data?
  if (op->outbl) {
#if 0
    if (op->con)
      op->con->revoke_rx_buffer(op->tid);
#endif
    auto& bl = m->get_data();
    if (op->outbl->length() == bl.length() &&
	bl.get_num_buffers() <= 1) {
      // this is here to keep previous users to *relied* on getting data
      // read into existing buffers happy.  Notably,
      // libradosstriper::RadosStriperImpl::aio_read().
      ldout(cct,10) << __func__ << " copying resulting " << bl.length()
		    << " into existing ceph::buffer of length " << op->outbl->length()
		    << dendl;
      cb::list t;
      t = std::move(*op->outbl);
      t.invalidate_crc();  // we're overwriting the raw buffers via c_str()
      bl.begin().copy(bl.length(), t.c_str());
      op->outbl->substr_of(t, 0, bl.length());
    } else {
      m->claim_data(*op->outbl);
    }
    op->outbl = 0;
  }

  // per-op result demuxing
  vector<OSDOp> out_ops;
  m->claim_ops(out_ops);

  if (out_ops.size() != op->ops.size())
    ldout(cct, 0) << "WARNING: tid " << op->tid << " reply ops " << out_ops
		  << " != request ops " << op->ops
		  << " from " << m->get_source_inst() << dendl;

  ceph_assert(op->ops.size() == op->out_bl.size());
  ceph_assert(op->ops.size() == op->out_rval.size());
  ceph_assert(op->ops.size() == op->out_ec.size());
  ceph_assert(op->ops.size() == op->out_handler.size());
  auto pb = op->out_bl.begin();
  auto pr = op->out_rval.begin();
  auto pe = op->out_ec.begin();
  auto ph = op->out_handler.begin();
  ceph_assert(op->out_bl.size() == op->out_rval.size());
  ceph_assert(op->out_bl.size() == op->out_handler.size());
  auto p = out_ops.begin();
  // Propagates handler error to Op::completion. In the event of
  // multiple handler errors, the most recent wins.
  bs::error_code handler_error;
  // Holds OSD error code, so handlers downstream of a failing op are
  // made aware of it.
  bs::error_code first_osd_error;
  for (unsigned i = 0;
       p != out_ops.end() && pb != op->out_bl.end();
       ++i, ++p, ++pb, ++pr, ++pe, ++ph) {
    ldout(cct, 10) << " op " << i << " rval " << p->rval
		   << " len " << p->outdata.length() << dendl;
    // Track when we get an OSD error and supply it to subsequent
    // handlers so they won't attempt to operate on data that isn't
    // there.
    if (!first_osd_error && (p->rval < 0)) {
      first_osd_error = bs::error_code(-p->rval, osd_category());
    }
    if (*pb)
      **pb = p->outdata;
    // set rval before running handlers so that handlers
    // can change it if e.g. decoding fails
    if (*pr)
      **pr = ceph_to_hostos_errno(p->rval);
    if (*pe)
      **pe = p->rval < 0 ? bs::error_code(-p->rval, osd_category()) :
	bs::error_code();
    if (*ph) {
      try {
	bs::error_code e;
	if (first_osd_error) {
	  e = first_osd_error;
	} else if (p->rval < 0) {
	  e = bs::error_code(-p->rval, osd_category());
	}
	std::move((*ph))(e, p->rval, p->outdata);
      } catch (const bs::system_error& e) {
	ldout(cct, 10) << "ERROR: tid " << op->tid << ": handler function threw "
		       << e.what() << dendl;
	handler_error = e.code();
	if (*pe) {
	  **pe = e.code();
	}
	if (*pr && **pr == 0) {
	  **pr = ceph::from_error_code(e.code());
	}
      } catch (const std::exception& e) {
	ldout(cct, 0) << "ERROR: tid " << op->tid << ": handler function threw "
		      << e.what() << dendl;
	handler_error = osdc_errc::handler_failed;
	if (*pe) {
	  **pe = osdc_errc::handler_failed;
	}
	if (*pr && **pr == 0) {
	  **pr = -EIO;
	}
      }
    }
  }

  // NOTE: we assume that since we only request ONDISK ever we will
  // only ever get back one (type of) ack ever.

  if (op->has_completion()) {
    num_in_flight--;
    onfinish = std::move(op->onfinish);
    op->onfinish = nullptr;
  }
  logger->inc(l_osdc_op_reply);
  logger->tinc(l_osdc_op_latency, ceph::coarse_mono_time::clock::now() - op->stamp);
  logger->set(l_osdc_op_inflight, num_in_flight);

  /* get it before we call _finish_op() */
  auto completion_lock = s->get_lock(op->target.base_oid);

  ldout(cct, 15) << "handle_osd_op_reply completed tid " << tid << dendl;
  _finish_op(op, 0);

  ldout(cct, 5) << num_in_flight << " in flight" << dendl;

  // serialize completions
  if (completion_lock.mutex()) {
    completion_lock.lock();
  }
  sl.unlock();

  // do callbacks
  if (Op::has_completion(onfinish)) {
    if (rc == 0 && handler_error) {
      Op::complete(std::move(onfinish), handler_error, -EIO, service.get_executor());
    } else if (handler_error) {
      Op::complete(std::move(onfinish), handler_error, rc, service.get_executor());
    } else {
      Op::complete(std::move(onfinish), osdcode(rc), rc, service.get_executor());
    }
  }
  if (completion_lock.mutex()) {
    completion_lock.unlock();
  }

  m->put();
}

void Objecter::handle_osd_backoff(MOSDBackoff *m)
{
  ldout(cct, 10) << __func__ << " " << *m << dendl;
  shunique_lock sul(rwlock, ceph::acquire_shared);
  if (!initialized) {
    m->put();
    return;
  }

  ConnectionRef con = m->get_connection();
  auto priv = con->get_priv();
  auto s = static_cast<OSDSession*>(priv.get());
  if (!s || s->con != con) {
    ldout(cct, 7) << __func__ << " no session on con " << con << dendl;
    m->put();
    return;
  }

  get_session(s);

  unique_lock sl(s->lock);

  switch (m->op) {
  case CEPH_OSD_BACKOFF_OP_BLOCK:
    {
      // register
      OSDBackoff& b = s->backoffs[m->pgid][m->begin];
      s->backoffs_by_id.insert(make_pair(m->id, &b));
      b.pgid = m->pgid;
      b.id = m->id;
      b.begin = m->begin;
      b.end = m->end;

      // ack with original backoff's epoch so that the osd can discard this if
      // there was a pg split.
      auto r = new MOSDBackoff(m->pgid, m->map_epoch,
			       CEPH_OSD_BACKOFF_OP_ACK_BLOCK,
			       m->id, m->begin, m->end);
      // this priority must match the MOSDOps from _prepare_osd_op
      r->set_priority(cct->_conf->osd_client_op_priority);
      con->send_message(r);
    }
    break;

  case CEPH_OSD_BACKOFF_OP_UNBLOCK:
    {
      auto p = s->backoffs_by_id.find(m->id);
      if (p != s->backoffs_by_id.end()) {
	OSDBackoff *b = p->second;
	if (b->begin != m->begin &&
	    b->end != m->end) {
	  lderr(cct) << __func__ << " got " << m->pgid << " id " << m->id
		     << " unblock on ["
		     << m->begin << "," << m->end << ") but backoff is ["
		     << b->begin << "," << b->end << ")" << dendl;
	  // hrmpf, unblock it anyway.
	}
	ldout(cct, 10) << __func__ << " unblock backoff " << b->pgid
		       << " id " << b->id
		       << " [" << b->begin << "," << b->end
		       << ")" << dendl;
	auto spgp = s->backoffs.find(b->pgid);
	ceph_assert(spgp != s->backoffs.end());
	spgp->second.erase(b->begin);
	if (spgp->second.empty()) {
	  s->backoffs.erase(spgp);
	}
	s->backoffs_by_id.erase(p);

	// check for any ops to resend
	for (auto& q : s->ops) {
	  if (q.second->target.actual_pgid == m->pgid) {
	    int r = q.second->target.contained_by(m->begin, m->end);
	    ldout(cct, 20) << __func__ <<  " contained_by " << r << " on "
			   << q.second->target.get_hobj() << dendl;
	    if (r) {
	      _send_op(q.second);
	    }
	  }
	}
      } else {
	lderr(cct) << __func__ << " " << m->pgid << " id " << m->id
		   << " unblock on ["
		   << m->begin << "," << m->end << ") but backoff dne" << dendl;
      }
    }
    break;

  default:
    ldout(cct, 10) << __func__ << " unrecognized op " << (int)m->op << dendl;
  }

  sul.unlock();
  sl.unlock();

  m->put();
  put_session(s);
}

uint32_t Objecter::list_nobjects_seek(NListContext *list_context,
				      uint32_t pos)
{
  shared_lock rl(rwlock);
  list_context->pos = hobject_t(object_t(), string(), CEPH_NOSNAP,
				pos, list_context->pool_id, string());
  ldout(cct, 10) << __func__ << " " << list_context
		 << " pos " << pos << " -> " << list_context->pos << dendl;
  pg_t actual = osdmap->raw_pg_to_pg(pg_t(pos, list_context->pool_id));
  list_context->current_pg = actual.ps();
  list_context->at_end_of_pool = false;
  return pos;
}

uint32_t Objecter::list_nobjects_seek(NListContext *list_context,
				      const hobject_t& cursor)
{
  shared_lock rl(rwlock);
  ldout(cct, 10) << "list_nobjects_seek " << list_context << dendl;
  list_context->pos = cursor;
  list_context->at_end_of_pool = false;
  pg_t actual = osdmap->raw_pg_to_pg(pg_t(cursor.get_hash(), list_context->pool_id));
  list_context->current_pg = actual.ps();
  list_context->sort_bitwise = true;
  return list_context->current_pg;
}

void Objecter::list_nobjects_get_cursor(NListContext *list_context,
                                        hobject_t *cursor)
{
  shared_lock rl(rwlock);
  if (list_context->list.empty()) {
    *cursor = list_context->pos;
  } else {
    const librados::ListObjectImpl& entry = list_context->list.front();
    const string *key = (entry.locator.empty() ? &entry.oid : &entry.locator);
    uint32_t h = osdmap->get_pg_pool(list_context->pool_id)->hash_key(*key, entry.nspace);
    *cursor = hobject_t(entry.oid, entry.locator, list_context->pool_snap_seq, h, list_context->pool_id, entry.nspace);
  }
}

void Objecter::list_nobjects(NListContext *list_context, Context *onfinish)
{
  ldout(cct, 10) << __func__ << " pool_id " << list_context->pool_id
		 << " pool_snap_seq " << list_context->pool_snap_seq
		 << " max_entries " << list_context->max_entries
		 << " list_context " << list_context
		 << " onfinish " << onfinish
		 << " current_pg " << list_context->current_pg
		 << " pos " << list_context->pos << dendl;

  shared_lock rl(rwlock);
  const pg_pool_t *pool = osdmap->get_pg_pool(list_context->pool_id);
  if (!pool) { // pool is gone
    rl.unlock();
    put_nlist_context_budget(list_context);
    onfinish->complete(-ENOENT);
    return;
  }
  int pg_num = pool->get_pg_num();
  bool sort_bitwise = osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE);

  if (list_context->pos.is_min()) {
    list_context->starting_pg_num = 0;
    list_context->sort_bitwise = sort_bitwise;
    list_context->starting_pg_num = pg_num;
  }
  if (list_context->sort_bitwise != sort_bitwise) {
    list_context->pos = hobject_t(
      object_t(), string(), CEPH_NOSNAP,
      list_context->current_pg, list_context->pool_id, string());
    list_context->sort_bitwise = sort_bitwise;
    ldout(cct, 10) << " hobject sort order changed, restarting this pg at "
		   << list_context->pos << dendl;
  }
  if (list_context->starting_pg_num != pg_num) {
    if (!sort_bitwise) {
      // start reading from the beginning; the pgs have changed
      ldout(cct, 10) << " pg_num changed; restarting with " << pg_num << dendl;
      list_context->pos = collection_list_handle_t();
    }
    list_context->starting_pg_num = pg_num;
  }

  if (list_context->pos.is_max()) {
    ldout(cct, 20) << __func__ << " end of pool, list "
		   << list_context->list << dendl;
    if (list_context->list.empty()) {
      list_context->at_end_of_pool = true;
    }
    // release the listing context's budget once all
    // OPs (in the session) are finished
    put_nlist_context_budget(list_context);
    onfinish->complete(0);
    return;
  }

  ObjectOperation op;
  op.pg_nls(list_context->max_entries, list_context->filter,
	    list_context->pos, osdmap->get_epoch());
  list_context->bl.clear();
  auto onack = new C_NList(list_context, onfinish, this);
  object_locator_t oloc(list_context->pool_id, list_context->nspace);

  // note current_pg in case we don't have (or lose) SORTBITWISE
  list_context->current_pg = pool->raw_hash_to_pg(list_context->pos.get_hash());
  rl.unlock();

  pg_read(list_context->current_pg, oloc, op,
	  &list_context->bl, 0, onack, &onack->epoch,
	  &list_context->ctx_budget);
}

void Objecter::_nlist_reply(NListContext *list_context, int r,
			    Context *final_finish, epoch_t reply_epoch)
{
  ldout(cct, 10) << __func__ << " " << list_context << dendl;

  auto iter = list_context->bl.cbegin();
  pg_nls_response_t response;
  decode(response, iter);
  if (!iter.end()) {
    // we do this as legacy.
    cb::list legacy_extra_info;
    decode(legacy_extra_info, iter);
  }

  // if the osd returns 1 (newer code), or handle MAX, it means we
  // hit the end of the pg.
  if ((response.handle.is_max() || r == 1) &&
      !list_context->sort_bitwise) {
    // legacy OSD and !sortbitwise, figure out the next PG on our own
    ++list_context->current_pg;
    if (list_context->current_pg == list_context->starting_pg_num) {
      // end of pool
      list_context->pos = hobject_t::get_max();
    } else {
      // next pg
      list_context->pos = hobject_t(object_t(), string(), CEPH_NOSNAP,
				    list_context->current_pg,
				    list_context->pool_id, string());
    }
  } else {
    list_context->pos = response.handle;
  }

  int response_size = response.entries.size();
  ldout(cct, 20) << " response.entries.size " << response_size
		 << ", response.entries " << response.entries
		 << ", handle " << response.handle
		 << ", tentative new pos " << list_context->pos << dendl;
  if (response_size) {
    std::move(response.entries.begin(), response.entries.end(),
	      std::back_inserter(list_context->list));
    response.entries.clear();
  }

  if (list_context->list.size() >= list_context->max_entries) {
    ldout(cct, 20) << " hit max, returning results so far, "
		   << list_context->list << dendl;
    // release the listing context's budget once all
    // OPs (in the session) are finished
    put_nlist_context_budget(list_context);
    final_finish->complete(0);
    return;
  }

  // continue!
  list_nobjects(list_context, final_finish);
}

void Objecter::put_nlist_context_budget(NListContext *list_context)
{
  if (list_context->ctx_budget >= 0) {
    ldout(cct, 10) << " release listing context's budget " <<
      list_context->ctx_budget << dendl;
    put_op_budget_bytes(list_context->ctx_budget);
    list_context->ctx_budget = -1;
  }
}

// snapshots

void Objecter::create_pool_snap(int64_t pool, std::string_view snap_name,
				decltype(PoolOp::onfinish)&& onfinish)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "create_pool_snap; pool: " << pool << "; snap: "
		 << snap_name << dendl;

  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p) {
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::pool_dne, cb::list{}));
    return;
  }
  if (p->snap_exists(snap_name)) {
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::snapshot_exists,
			     cb::list{}));
    return;
  }

  auto op = new PoolOp;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snap_name;
  op->onfinish = std::move(onfinish);
  op->pool_op = POOL_OP_CREATE_SNAP;
  pool_ops[op->tid] = op;

  pool_op_submit(op);
}

struct CB_SelfmanagedSnap {
  asio::any_completion_handler<void(bs::error_code, snapid_t)> fin;
  CB_SelfmanagedSnap(decltype(fin)&& fin)
    : fin(std::move(fin)) {}
  void operator()(bs::error_code ec, const cb::list& bl) {
    snapid_t snapid = 0;
    if (!ec) {
      try {
	auto p = bl.cbegin();
	decode(snapid, p);
      } catch (const cb::error& e) {
        ec = e.code();
      }
    }
    asio::dispatch(asio::append(std::move(fin), ec, snapid));
  }
};

void Objecter::allocate_selfmanaged_snap(
  int64_t pool,
  asio::any_completion_handler<void(bs::error_code, snapid_t)> onfinish)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "allocate_selfmanaged_snap; pool: " << pool << dendl;
  auto op = new PoolOp;
  op->tid = ++last_tid;
  op->pool = pool;
  auto e = boost::asio::prefer(
    service.get_executor(),
    boost::asio::execution::outstanding_work.tracked);
  op->onfinish = asio::bind_executor(e, CB_SelfmanagedSnap(std::move(onfinish)));
  op->pool_op = POOL_OP_CREATE_UNMANAGED_SNAP;
  pool_ops[op->tid] = op;

  pool_op_submit(op);
}

void Objecter::delete_pool_snap(
  int64_t pool, std::string_view snap_name,
  decltype(PoolOp::onfinish)&& onfinish)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "delete_pool_snap; pool: " << pool << "; snap: "
		 << snap_name << dendl;

  const pg_pool_t *p = osdmap->get_pg_pool(pool);
  if (!p) {
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::pool_dne,
			     cb::list{}));
    return;
  }

  if (!p->snap_exists(snap_name)) {
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::snapshot_dne, cb::list{}));
    return;
  }

  auto op = new PoolOp;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = snap_name;
  op->onfinish = std::move(onfinish);
  op->pool_op = POOL_OP_DELETE_SNAP;
  pool_ops[op->tid] = op;

  pool_op_submit(op);
}

void Objecter::delete_selfmanaged_snap(int64_t pool, snapid_t snap,
				       decltype(PoolOp::onfinish)&& onfinish)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "delete_selfmanaged_snap; pool: " << pool << "; snap: "
		 << snap << dendl;
  auto op = new PoolOp;
  op->tid = ++last_tid;
  op->pool = pool;
  op->onfinish = std::move(onfinish);
  op->pool_op = POOL_OP_DELETE_UNMANAGED_SNAP;
  op->snapid = snap;
  pool_ops[op->tid] = op;

  pool_op_submit(op);
}

void Objecter::create_pool(std::string_view name,
			   decltype(PoolOp::onfinish)&& onfinish,
			   int crush_rule)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "create_pool name=" << name << dendl;

  if (osdmap->lookup_pg_pool_name(name) >= 0) {
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::pool_exists,
			     cb::list{}));
    return;
  }

  auto op = new PoolOp;
  op->tid = ++last_tid;
  op->pool = 0;
  op->name = name;
  op->onfinish = std::move(onfinish);
  op->pool_op = POOL_OP_CREATE;
  pool_ops[op->tid] = op;
  op->crush_rule = crush_rule;

  pool_op_submit(op);
}

void Objecter::delete_pool(int64_t pool,
			   decltype(PoolOp::onfinish)&& onfinish)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "delete_pool " << pool << dendl;

  if (!osdmap->have_pg_pool(pool))
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::pool_dne,
			     cb::list{}));
  else
    _do_delete_pool(pool, std::move(onfinish));
}

void Objecter::delete_pool(std::string_view pool_name,
			   decltype(PoolOp::onfinish)&& onfinish)
{
  unique_lock wl(rwlock);
  ldout(cct, 10) << "delete_pool " << pool_name << dendl;

  int64_t pool = osdmap->lookup_pg_pool_name(pool_name);
  if (pool < 0)
    // This only returns one error: -ENOENT.
    asio::defer(service.get_executor(),
		asio::append(std::move(onfinish), osdc_errc::pool_dne,
			     cb::list{}));
  else
    _do_delete_pool(pool, std::move(onfinish));
}

void Objecter::_do_delete_pool(int64_t pool,
			       decltype(PoolOp::onfinish)&& onfinish)

{
  auto op = new PoolOp;
  op->tid = ++last_tid;
  op->pool = pool;
  op->name = "delete";
  op->onfinish = std::move(onfinish);
  op->pool_op = POOL_OP_DELETE;
  pool_ops[op->tid] = op;
  pool_op_submit(op);
}

void Objecter::pool_op_submit(PoolOp *op)
{
  // rwlock is locked
  if (mon_timeout > timespan(0)) {
    op->ontimeout = timer.add_event(mon_timeout,
				    [this, op]() {
				      pool_op_cancel(op->tid, -ETIMEDOUT); });
  }
  _pool_op_submit(op);
}

void Objecter::_pool_op_submit(PoolOp *op)
{
  // rwlock is locked unique

  ldout(cct, 10) << "pool_op_submit " << op->tid << dendl;
  auto m = new MPoolOp(monc->get_fsid(), op->tid, op->pool,
		       op->name, op->pool_op,
		       last_seen_osdmap_version);
  if (op->snapid) m->snapid = op->snapid;
  if (op->crush_rule) m->crush_rule = op->crush_rule;
  monc->send_mon_message(m);
  op->last_submit = ceph::coarse_mono_clock::now();

  logger->inc(l_osdc_poolop_send);
}

/**
 * Handle a reply to a PoolOp message. Check that we sent the message
 * and give the caller responsibility for the returned cb::list.
 * Then either call the finisher or stash the PoolOp, depending on if we
 * have a new enough map.
 * Lastly, clean up the message and PoolOp.
 */
void Objecter::handle_pool_op_reply(MPoolOpReply *m)
{
  int rc = m->replyCode;
  auto ec = rc < 0 ? bs::error_code(-rc, mon_category()) : bs::error_code();
  FUNCTRACE(cct);
  shunique_lock sul(rwlock, acquire_shared);
  if (!initialized) {
    sul.unlock();
    m->put();
    return;
  }

  ldout(cct, 10) << "handle_pool_op_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();
  auto iter = pool_ops.find(tid);
  if (iter != pool_ops.end()) {
    PoolOp *op = iter->second;
    ldout(cct, 10) << "have request " << tid << " at " << op << " Op: "
		   << ceph_pool_op_name(op->pool_op) << dendl;
    cb::list bl{std::move(m->response_data)};
    if (m->version > last_seen_osdmap_version)
      last_seen_osdmap_version = m->version;
    if (osdmap->get_epoch() < m->epoch) {
      sul.unlock();
      sul.lock();
      // recheck op existence since we have let go of rwlock
      // (for promotion) above.
      iter = pool_ops.find(tid);
      if (iter == pool_ops.end())
	goto done; // op is gone.
      if (osdmap->get_epoch() < m->epoch) {
	ldout(cct, 20) << "waiting for client to reach epoch " << m->epoch
		       << " before calling back" << dendl;
	auto e = boost::asio::prefer(
	  service.get_executor(),
	  boost::asio::execution::outstanding_work.tracked);
	_wait_for_new_map(asio::bind_executor(
			    e,
			    [o = std::move(op->onfinish),
			     bl = std::move(bl),
			     e = service.get_executor()](
			      bs::error_code ec) mutable {
			      asio::defer(e, asio::append(std::move(o), ec, bl));
			    }),
			  m->epoch,
			  ec);
      } else {
	// map epoch changed, probably because a MOSDMap message
	// sneaked in. Do caller-specified callback now or else
	// we lose it forever.
	ceph_assert(op->onfinish);
	asio::defer(service.get_executor(), asio::append(std::move(op->onfinish), ec, std::move(bl)));
      }
    } else {
      ceph_assert(op->onfinish);
      asio::defer(service.get_executor(), asio::append(std::move(op->onfinish), ec, std::move(bl)));
    }
    op->onfinish = nullptr;
    if (!sul.owns_lock()) {
      sul.unlock();
      sul.lock();
    }
    iter = pool_ops.find(tid);
    if (iter != pool_ops.end()) {
      _finish_pool_op(op, 0);
    }
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }

done:
  // Not strictly necessary, since we'll release it on return.
  sul.unlock();

  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::pool_op_cancel(ceph_tid_t tid, int r)
{
  ceph_assert(initialized);

  unique_lock wl(rwlock);

  auto it = pool_ops.find(tid);
  if (it == pool_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  PoolOp *op = it->second;
  if (op->onfinish)
    asio::defer(service.get_executor(), asio::append(std::move(op->onfinish),
						     osdcode(r), cb::list{}));

  _finish_pool_op(op, r);
  return 0;
}

void Objecter::_finish_pool_op(PoolOp *op, int r)
{
  // rwlock is locked unique
  pool_ops.erase(op->tid);
  logger->set(l_osdc_poolop_active, pool_ops.size());

  if (op->ontimeout && r != -ETIMEDOUT) {
    timer.cancel_event(op->ontimeout);
  }

  delete op;
}

// pool stats

void Objecter::get_pool_stats_(
  const std::vector<std::string>& pools,
  decltype(PoolStatOp::onfinish)&& onfinish)
{
  ldout(cct, 10) << "get_pool_stats " << pools << dendl;

  auto op = new PoolStatOp;
  op->tid = ++last_tid;
  op->pools = pools;
  op->onfinish = std::move(onfinish);
  if (mon_timeout > timespan(0)) {
    op->ontimeout = timer.add_event(mon_timeout,
				    [this, op]() {
				      pool_stat_op_cancel(op->tid,
							  -ETIMEDOUT); });
  } else {
    op->ontimeout = 0;
  }

  unique_lock wl(rwlock);

  poolstat_ops[op->tid] = op;

  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  _poolstat_submit(op);
}

void Objecter::_poolstat_submit(PoolStatOp *op)
{
  ldout(cct, 10) << "_poolstat_submit " << op->tid << dendl;
  monc->send_mon_message(new MGetPoolStats(monc->get_fsid(), op->tid,
					   op->pools,
					   last_seen_pgmap_version));
  op->last_submit = ceph::coarse_mono_clock::now();

  logger->inc(l_osdc_poolstat_send);
}

void Objecter::handle_get_pool_stats_reply(MGetPoolStatsReply *m)
{
  ldout(cct, 10) << "handle_get_pool_stats_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();

  unique_lock wl(rwlock);
  if (!initialized) {
    m->put();
    return;
  }

  auto iter = poolstat_ops.find(tid);
  if (iter != poolstat_ops.end()) {
    PoolStatOp *op = poolstat_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    if (m->version > last_seen_pgmap_version) {
      last_seen_pgmap_version = m->version;
    }
    asio::defer(service.get_executor(),
		asio::append(std::move(op->onfinish), bs::error_code{},
			     std::move(m->pool_stats), m->per_pool));
    _finish_pool_stat_op(op, 0);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  ldout(cct, 10) << "done" << dendl;
  m->put();
}

int Objecter::pool_stat_op_cancel(ceph_tid_t tid, int r)
{
  ceph_assert(initialized);

  unique_lock wl(rwlock);

  auto it = poolstat_ops.find(tid);
  if (it == poolstat_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  auto op = it->second;
  if (op->onfinish)
    asio::defer(service.get_executor(),
		asio::append(std::move(op->onfinish), osdcode(r),
			     bc::flat_map<std::string, pool_stat_t>{}, false));
  _finish_pool_stat_op(op, r);
  return 0;
}

void Objecter::_finish_pool_stat_op(PoolStatOp *op, int r)
{
  // rwlock is locked unique

  poolstat_ops.erase(op->tid);
  logger->set(l_osdc_poolstat_active, poolstat_ops.size());

  if (op->ontimeout && r != -ETIMEDOUT)
    timer.cancel_event(op->ontimeout);

  delete op;
}

void Objecter::get_fs_stats_(std::optional<int64_t> poolid,
			     decltype(StatfsOp::onfinish)&& onfinish)
{
  ldout(cct, 10) << "get_fs_stats" << dendl;
  unique_lock l(rwlock);

  auto op = new StatfsOp;
  op->tid = ++last_tid;
  op->data_pool = poolid;
  op->onfinish = std::move(onfinish);
  if (mon_timeout > timespan(0)) {
    op->ontimeout = timer.add_event(mon_timeout,
				    [this, op]() {
				      statfs_op_cancel(op->tid,
						       -ETIMEDOUT); });
  } else {
    op->ontimeout = 0;
  }
  statfs_ops[op->tid] = op;

  logger->set(l_osdc_statfs_active, statfs_ops.size());

  _fs_stats_submit(op);
}

void Objecter::_fs_stats_submit(StatfsOp *op)
{
  // rwlock is locked unique

  ldout(cct, 10) << "fs_stats_submit" << op->tid << dendl;
  monc->send_mon_message(new MStatfs(monc->get_fsid(), op->tid,
				     op->data_pool,
				     last_seen_pgmap_version));
  op->last_submit = ceph::coarse_mono_clock::now();

  logger->inc(l_osdc_statfs_send);
}

void Objecter::handle_fs_stats_reply(MStatfsReply *m)
{
  unique_lock wl(rwlock);
  if (!initialized) {
    m->put();
    return;
  }

  ldout(cct, 10) << "handle_fs_stats_reply " << *m << dendl;
  ceph_tid_t tid = m->get_tid();

  if (statfs_ops.count(tid)) {
    StatfsOp *op = statfs_ops[tid];
    ldout(cct, 10) << "have request " << tid << " at " << op << dendl;
    if (m->h.version > last_seen_pgmap_version)
      last_seen_pgmap_version = m->h.version;
    asio::defer(service.get_executor(), asio::append(std::move(op->onfinish),
						     bs::error_code{}, m->h.st));
    _finish_statfs_op(op, 0);
  } else {
    ldout(cct, 10) << "unknown request " << tid << dendl;
  }
  m->put();
  ldout(cct, 10) << "done" << dendl;
}

int Objecter::statfs_op_cancel(ceph_tid_t tid, int r)
{
  ceph_assert(initialized);

  unique_lock wl(rwlock);

  auto it = statfs_ops.find(tid);
  if (it == statfs_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  auto op = it->second;
  if (op->onfinish)
    asio::defer(service.get_executor(),
		asio::append(std::move(op->onfinish),
			     osdcode(r), ceph_statfs{}));
  _finish_statfs_op(op, r);
  return 0;
}

void Objecter::_finish_statfs_op(StatfsOp *op, int r)
{
  // rwlock is locked unique

  statfs_ops.erase(op->tid);
  logger->set(l_osdc_statfs_active, statfs_ops.size());

  if (op->ontimeout && r != -ETIMEDOUT)
    timer.cancel_event(op->ontimeout);

  delete op;
}

// scatter/gather

void Objecter::_sg_read_finish(vector<ObjectExtent>& extents,
			       vector<cb::list>& resultbl,
			       cb::list *bl, Context *onfinish)
{
  // all done
  ldout(cct, 15) << "_sg_read_finish" << dendl;

  if (extents.size() > 1) {
    Striper::StripedReadResult r;
    auto bit = resultbl.begin();
    for (auto eit = extents.begin();
	 eit != extents.end();
	 ++eit, ++bit) {
      r.add_partial_result(cct, *bit, eit->buffer_extents);
    }
    bl->clear();
    r.assemble_result(cct, *bl, false);
  } else {
    ldout(cct, 15) << "  only one frag" << dendl;
    *bl = std::move(resultbl[0]);
  }

  // done
  uint64_t bytes_read = bl->length();
  ldout(cct, 7) << "_sg_read_finish " << bytes_read << " bytes" << dendl;

  if (onfinish) {
    onfinish->complete(bytes_read);// > 0 ? bytes_read:m->get_result());
  }
}


void Objecter::ms_handle_connect(Connection *con)
{
  ldout(cct, 10) << "ms_handle_connect " << con << dendl;
  if (!initialized)
    return;

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    resend_mon_ops();
}

bool Objecter::ms_handle_reset(Connection *con)
{
  if (!initialized)
    return false;
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD) {
    unique_lock wl(rwlock);

    auto priv = con->get_priv();
    auto session = static_cast<OSDSession*>(priv.get());
    if (session) {
      ldout(cct, 1) << "ms_handle_reset " << con << " session " << session
		    << " osd." << session->osd << dendl;
      // the session maybe had been closed if new osdmap just handled
      // says the osd down
      if (!(initialized && osdmap->is_up(session->osd))) {
	ldout(cct, 1) << "ms_handle_reset aborted,initialized=" << initialized << dendl;
	wl.unlock();
	return false;
      }
      map<uint64_t, LingerOp *> lresend;
      unique_lock sl(session->lock);
      _reopen_session(session);
      _kick_requests(session, lresend);
      sl.unlock();
      _linger_ops_resend(lresend, wl);
      wl.unlock();
      maybe_request_map();
    }
    return true;
  }
  return false;
}

void Objecter::ms_handle_remote_reset(Connection *con)
{
  /*
   * treat these the same.
   */
  ms_handle_reset(con);
}

bool Objecter::ms_handle_refused(Connection *con)
{
  // just log for now
  if (osdmap && (con->get_peer_type() == CEPH_ENTITY_TYPE_OSD)) {
    int osd = osdmap->identify_osd(con->get_peer_addr());
    if (osd >= 0) {
      ldout(cct, 1) << "ms_handle_refused on osd." << osd << dendl;
    }
  }
  return false;
}

void Objecter::op_target_t::dump(Formatter *f) const
{
  f->dump_stream("pg") << pgid;
  f->dump_int("osd", osd);
  f->dump_stream("object_id") << base_oid;
  f->dump_stream("object_locator") << base_oloc;
  f->dump_stream("target_object_id") << target_oid;
  f->dump_stream("target_object_locator") << target_oloc;
  f->dump_int("paused", (int)paused);
  f->dump_int("used_replica", (int)used_replica);
  f->dump_int("precalc_pgid", (int)precalc_pgid);
}

void Objecter::_dump_active(OSDSession *s)
{
  for (auto p = s->ops.begin(); p != s->ops.end(); ++p) {
    Op *op = p->second;
    ldout(cct, 20) << op->tid << "\t" << op->target.pgid
		   << "\tosd." << (op->session ? op->session->osd : -1)
		   << "\t" << op->target.base_oid
		   << "\t" << op->ops << dendl;
  }
}

void Objecter::_dump_active()
{
  ldout(cct, 20) << "dump_active .. " << num_homeless_ops << " homeless"
		 << dendl;
  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    auto s = siter->second;
    shared_lock sl(s->lock);
    _dump_active(s);
    sl.unlock();
  }
  _dump_active(homeless_session);
}

void Objecter::dump_active()
{
  shared_lock rl(rwlock);
  _dump_active();
  rl.unlock();
}

void Objecter::dump_requests(Formatter *fmt)
{
  // Read-lock on Objecter held here
  fmt->open_object_section("requests");
  dump_ops(fmt);
  dump_linger_ops(fmt);
  dump_pool_ops(fmt);
  dump_pool_stat_ops(fmt);
  dump_statfs_ops(fmt);
  dump_command_ops(fmt);
  fmt->close_section(); // requests object
}

void Objecter::_dump_ops(const OSDSession *s, Formatter *fmt)
{
  for (auto p = s->ops.begin(); p != s->ops.end(); ++p) {
    Op *op = p->second;
    auto age = std::chrono::duration<double>(ceph::coarse_mono_clock::now() - op->stamp);
    fmt->open_object_section("op");
    fmt->dump_unsigned("tid", op->tid);
    op->target.dump(fmt);
    fmt->dump_stream("last_sent") << op->stamp;
    fmt->dump_float("age", age.count());
    fmt->dump_int("attempts", op->attempts);
    fmt->dump_stream("snapid") << op->snapid;
    fmt->dump_stream("snap_context") << op->snapc;
    fmt->dump_stream("mtime") << op->mtime;

    fmt->open_array_section("osd_ops");
    for (auto it = op->ops.begin(); it != op->ops.end(); ++it) {
      fmt->dump_stream("osd_op") << *it;
    }
    fmt->close_section(); // osd_ops array

    fmt->close_section(); // op object
  }
}

void Objecter::dump_ops(Formatter *fmt)
{
  // Read-lock on Objecter held
  fmt->open_array_section("ops");
  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    OSDSession *s = siter->second;
    shared_lock sl(s->lock);
    _dump_ops(s, fmt);
    sl.unlock();
  }
  _dump_ops(homeless_session, fmt);
  fmt->close_section(); // ops array
}

void Objecter::_dump_linger_ops(const OSDSession *s, Formatter *fmt)
{
  for (auto p = s->linger_ops.begin(); p != s->linger_ops.end(); ++p) {
    auto op = p->second;
    fmt->open_object_section("linger_op");
    fmt->dump_unsigned("linger_id", op->linger_id);
    op->target.dump(fmt);
    fmt->dump_stream("snapid") << op->snap;
    fmt->dump_stream("registered") << op->registered;
    fmt->close_section(); // linger_op object
  }
}

void Objecter::dump_linger_ops(Formatter *fmt)
{
  // We have a read-lock on the objecter
  fmt->open_array_section("linger_ops");
  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    auto s = siter->second;
    shared_lock sl(s->lock);
    _dump_linger_ops(s, fmt);
    sl.unlock();
  }
  _dump_linger_ops(homeless_session, fmt);
  fmt->close_section(); // linger_ops array
}

void Objecter::_dump_command_ops(const OSDSession *s, Formatter *fmt)
{
  for (auto p = s->command_ops.begin(); p != s->command_ops.end(); ++p) {
    auto op = p->second;
    fmt->open_object_section("command_op");
    fmt->dump_unsigned("command_id", op->tid);
    fmt->dump_int("osd", op->session ? op->session->osd : -1);
    fmt->open_array_section("command");
    for (auto q = op->cmd.begin(); q != op->cmd.end(); ++q)
      fmt->dump_string("word", *q);
    fmt->close_section();
    if (op->target_osd >= 0)
      fmt->dump_int("target_osd", op->target_osd);
    else
      fmt->dump_stream("target_pg") << op->target_pg;
    fmt->close_section(); // command_op object
  }
}

void Objecter::dump_command_ops(Formatter *fmt)
{
  // We have a read-lock on the Objecter here
  fmt->open_array_section("command_ops");
  for (auto siter = osd_sessions.begin();
       siter != osd_sessions.end(); ++siter) {
    auto s = siter->second;
    shared_lock sl(s->lock);
    _dump_command_ops(s, fmt);
    sl.unlock();
  }
  _dump_command_ops(homeless_session, fmt);
  fmt->close_section(); // command_ops array
}

void Objecter::dump_pool_ops(Formatter *fmt) const
{
  fmt->open_array_section("pool_ops");
  for (auto p = pool_ops.begin(); p != pool_ops.end(); ++p) {
    auto op = p->second;
    fmt->open_object_section("pool_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_int("pool", op->pool);
    fmt->dump_string("name", op->name);
    fmt->dump_int("operation_type", op->pool_op);
    fmt->dump_unsigned("crush_rule", op->crush_rule);
    fmt->dump_stream("snapid") << op->snapid;
    fmt->dump_stream("last_sent") << op->last_submit;
    fmt->close_section(); // pool_op object
  }
  fmt->close_section(); // pool_ops array
}

void Objecter::dump_pool_stat_ops(Formatter *fmt) const
{
  fmt->open_array_section("pool_stat_ops");
  for (auto p = poolstat_ops.begin();
       p != poolstat_ops.end();
       ++p) {
    PoolStatOp *op = p->second;
    fmt->open_object_section("pool_stat_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_stream("last_sent") << op->last_submit;

    fmt->open_array_section("pools");
    for (const auto& it : op->pools) {
      fmt->dump_string("pool", it);
    }
    fmt->close_section(); // pools array

    fmt->close_section(); // pool_stat_op object
  }
  fmt->close_section(); // pool_stat_ops array
}

void Objecter::dump_statfs_ops(Formatter *fmt) const
{
  fmt->open_array_section("statfs_ops");
  for (auto p = statfs_ops.begin(); p != statfs_ops.end(); ++p) {
    auto op = p->second;
    fmt->open_object_section("statfs_op");
    fmt->dump_unsigned("tid", op->tid);
    fmt->dump_stream("last_sent") << op->last_submit;
    fmt->close_section(); // statfs_op object
  }
  fmt->close_section(); // statfs_ops array
}

Objecter::RequestStateHook::RequestStateHook(Objecter *objecter) :
  m_objecter(objecter)
{
}

int Objecter::RequestStateHook::call(std::string_view command,
				     const cmdmap_t& cmdmap,
				     const bufferlist&,
				     Formatter *f,
				     std::ostream& ss,
				     cb::list& out)
{
  shared_lock rl(m_objecter->rwlock);
  m_objecter->dump_requests(f);
  return 0;
}

void Objecter::blocklist_self(bool set)
{
  ldout(cct, 10) << "blocklist_self " << (set ? "add" : "rm") << dendl;

  vector<string> cmd;
  cmd.push_back("{\"prefix\":\"osd blocklist\", ");
  if (set)
    cmd.push_back("\"blocklistop\":\"add\",");
  else
    cmd.push_back("\"blocklistop\":\"rm\",");
  stringstream ss;
  // this is somewhat imprecise in that we are blocklisting our first addr only
  ss << messenger->get_myaddrs().front().get_legacy_str();
  cmd.push_back("\"addr\":\"" + ss.str() + "\"");

  auto m = new MMonCommand(monc->get_fsid());
  m->cmd = cmd;

  // NOTE: no fallback to legacy blacklist command implemented here
  // since this is only used for test code.

  monc->send_mon_message(m);
}

// commands

void Objecter::handle_command_reply(MCommandReply *m)
{
  unique_lock wl(rwlock);
  if (!initialized) {
    m->put();
    return;
  }

  ConnectionRef con = m->get_connection();
  auto priv = con->get_priv();
  auto s = static_cast<OSDSession*>(priv.get());
  if (!s || s->con != con) {
    ldout(cct, 7) << __func__ << " no session on con " << con << dendl;
    m->put();
    return;
  }

  shared_lock sl(s->lock);
  auto p = s->command_ops.find(m->get_tid());
  if (p == s->command_ops.end()) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid()
		   << " not found" << dendl;
    m->put();
    sl.unlock();
    return;
  }

  CommandOp *c = p->second;
  if (!c->session ||
      m->get_connection() != c->session->con) {
    ldout(cct, 10) << "handle_command_reply tid " << m->get_tid()
		   << " got reply from wrong connection "
		   << m->get_connection() << " " << m->get_source_inst()
		   << dendl;
    m->put();
    sl.unlock();
    return;
  }

  if (m->r == -EAGAIN) {
    ldout(cct,10) << __func__ << " tid " << m->get_tid()
		  << " got EAGAIN, requesting map and resending" << dendl;
    // NOTE: This might resend twice... once now, and once again when
    // we get an updated osdmap and the PG is found to have moved.
    _maybe_request_map();
    _send_command(c);
    m->put();
    sl.unlock();
    return;
  }

  sl.unlock();

  unique_lock sul(s->lock);
  _finish_command(c, m->r < 0 ? bs::error_code(-m->r, osd_category()) :
		  bs::error_code(), std::move(m->rs),
		  std::move(m->get_data()));
  sul.unlock();

  m->put();
}

Objecter::LingerOp::LingerOp(Objecter *o, uint64_t linger_id)
  : objecter(o),
    linger_id(linger_id),
    watch_lock(ceph::make_shared_mutex(
		 fmt::format("LingerOp::watch_lock #{}", linger_id)))
{}

void Objecter::submit_command(CommandOp *c, ceph_tid_t *ptid)
{
  shunique_lock sul(rwlock, ceph::acquire_unique);

  ceph_tid_t tid = ++last_tid;
  ldout(cct, 10) << "_submit_command " << tid << " " << c->cmd << dendl;
  c->tid = tid;

  {
    unique_lock hs_wl(homeless_session->lock);
    _session_command_op_assign(homeless_session, c);
  }

  _calc_command_target(c, sul);
  _assign_command_session(c, sul);
  if (osd_timeout > timespan(0)) {
    c->ontimeout = timer.add_event(osd_timeout,
				   [this, c, tid]() {
				     command_op_cancel(
				       c->session, tid,
				       osdc_errc::timed_out); });
  }

  if (!c->session->is_homeless()) {
    _send_command(c);
  } else {
    _maybe_request_map();
  }
  if (c->map_check_error)
    _send_command_map_check(c);
  if (ptid)
    *ptid = tid;

  logger->inc(l_osdc_command_active);
}

int Objecter::_calc_command_target(CommandOp *c,
				   shunique_lock<ceph::shared_mutex>& sul)
{
  ceph_assert(sul.owns_lock() && sul.mutex() == &rwlock);

  c->map_check_error = 0;

  // ignore overlays, just like we do with pg ops
  c->target.flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;

  if (c->target_osd >= 0) {
    if (!osdmap->exists(c->target_osd)) {
      c->map_check_error = -ENOENT;
      c->map_check_error_str = "osd dne";
      c->target.osd = -1;
      return RECALC_OP_TARGET_OSD_DNE;
    }
    if (osdmap->is_down(c->target_osd)) {
      c->map_check_error = -ENXIO;
      c->map_check_error_str = "osd down";
      c->target.osd = -1;
      return RECALC_OP_TARGET_OSD_DOWN;
    }
    c->target.osd = c->target_osd;
  } else {
    int ret = _calc_target(&(c->target), nullptr, true);
    if (ret == RECALC_OP_TARGET_POOL_DNE) {
      c->map_check_error = -ENOENT;
      c->map_check_error_str = "pool dne";
      c->target.osd = -1;
      return ret;
    } else if (ret == RECALC_OP_TARGET_OSD_DOWN) {
      c->map_check_error = -ENXIO;
      c->map_check_error_str = "osd down";
      c->target.osd = -1;
      return ret;
    }
  }

  OSDSession *s;
  int r = _get_session(c->target.osd, &s, sul);
  ceph_assert(r != -EAGAIN); /* shouldn't happen as we're holding the write lock */

  if (c->session != s) {
    put_session(s);
    return RECALC_OP_TARGET_NEED_RESEND;
  }

  put_session(s);

  ldout(cct, 20) << "_recalc_command_target " << c->tid << " no change, "
		 << c->session << dendl;

  return RECALC_OP_TARGET_NO_ACTION;
}

void Objecter::_assign_command_session(CommandOp *c,
				       shunique_lock<ceph::shared_mutex>& sul)
{
  ceph_assert(sul.owns_lock() && sul.mutex() == &rwlock);

  OSDSession *s;
  int r = _get_session(c->target.osd, &s, sul);
  ceph_assert(r != -EAGAIN); /* shouldn't happen as we're holding the write lock */

  if (c->session != s) {
    if (c->session) {
      OSDSession *cs = c->session;
      unique_lock csl(cs->lock);
      _session_command_op_remove(c->session, c);
      csl.unlock();
    }
    unique_lock sl(s->lock);
    _session_command_op_assign(s, c);
  }

  put_session(s);
}

void Objecter::_send_command(CommandOp *c)
{
  ldout(cct, 10) << "_send_command " << c->tid << dendl;
  ceph_assert(c->session);
  ceph_assert(c->session->con);
  auto m = new MCommand(monc->monmap.fsid);
  m->cmd = c->cmd;
  m->set_data(c->inbl);
  m->set_tid(c->tid);
  c->session->con->send_message(m);
  logger->inc(l_osdc_command_send);
}

int Objecter::command_op_cancel(OSDSession *s, ceph_tid_t tid,
				bs::error_code ec)
{
  ceph_assert(initialized);

  unique_lock wl(rwlock);

  auto it = s->command_ops.find(tid);
  if (it == s->command_ops.end()) {
    ldout(cct, 10) << __func__ << " tid " << tid << " dne" << dendl;
    return -ENOENT;
  }

  ldout(cct, 10) << __func__ << " tid " << tid << dendl;

  CommandOp *op = it->second;
  _command_cancel_map_check(op);
  unique_lock sl(op->session->lock);
  _finish_command(op, ec, {}, {});
  sl.unlock();
  return 0;
}

void Objecter::_finish_command(CommandOp *c, bs::error_code ec,
			       string&& rs, cb::list&& bl)
{
  // rwlock is locked unique
  // session lock is locked

  ldout(cct, 10) << "_finish_command " << c->tid << " = " << ec << " "
		 << rs << dendl;

  if (c->onfinish)
    asio::defer(service.get_executor(),
		asio::append(std::move(c->onfinish), ec, std::move(rs),
			     std::move(bl)));

  if (c->ontimeout && ec != bs::errc::timed_out)
    timer.cancel_event(c->ontimeout);

  _session_command_op_remove(c->session, c);

  c->put();

  logger->dec(l_osdc_command_active);
}

Objecter::OSDSession::~OSDSession()
{
  // Caller is responsible for re-assigning or
  // destroying any ops that were assigned to us
  ceph_assert(ops.empty());
  ceph_assert(linger_ops.empty());
  ceph_assert(command_ops.empty());
}

Objecter::Objecter(CephContext *cct,
		   Messenger *m, MonClient *mc,
		   asio::io_context& service) :
  Dispatcher(cct), messenger(m), monc(mc), service(service)
{
  mon_timeout = cct->_conf.get_val<std::chrono::seconds>("rados_mon_op_timeout");
  osd_timeout = cct->_conf.get_val<std::chrono::seconds>("rados_osd_op_timeout");
}

Objecter::~Objecter()
{
  ceph_assert(homeless_session->get_nref() == 1);
  ceph_assert(num_homeless_ops == 0);
  homeless_session->put();

  ceph_assert(osd_sessions.empty());
  ceph_assert(poolstat_ops.empty());
  ceph_assert(statfs_ops.empty());
  ceph_assert(pool_ops.empty());
  ceph_assert(waiting_for_map.empty());
  ceph_assert(linger_ops.empty());
  ceph_assert(check_latest_map_lingers.empty());
  ceph_assert(check_latest_map_ops.empty());
  ceph_assert(check_latest_map_commands.empty());

  ceph_assert(!m_request_state_hook);
  ceph_assert(!logger);
}

/**
 * Wait until this OSD map epoch is received before
 * sending any more operations to OSDs.  Use this
 * when it is known that the client can't trust
 * anything from before this epoch (e.g. due to
 * client blocklist at this epoch).
 */
void Objecter::set_epoch_barrier(epoch_t epoch)
{
  unique_lock wl(rwlock);

  ldout(cct, 7) << __func__ << ": barrier " << epoch << " (was "
		<< epoch_barrier << ") current epoch " << osdmap->get_epoch()
		<< dendl;
  if (epoch > epoch_barrier) {
    epoch_barrier = epoch;
    _maybe_request_map();
  }
}



hobject_t Objecter::enumerate_objects_begin()
{
  return hobject_t();
}

hobject_t Objecter::enumerate_objects_end()
{
  return hobject_t::get_max();
}

template<typename T>
struct EnumerationContext {
  Objecter* objecter;
  const hobject_t end;
  const cb::list filter;
  uint32_t max;
  const object_locator_t oloc;
  std::vector<T> ls;
private:
  fu2::unique_function<void(bs::error_code,
			    std::vector<T>,
			    hobject_t) &&> on_finish;
public:
  epoch_t epoch = 0;
  int budget = -1;

  EnumerationContext(Objecter* objecter,
		     hobject_t end, cb::list filter,
		     uint32_t max, object_locator_t oloc,
		     decltype(on_finish) on_finish)
    : objecter(objecter), end(std::move(end)), filter(std::move(filter)),
      max(max), oloc(std::move(oloc)), on_finish(std::move(on_finish)) {}

  void operator()(bs::error_code ec,
		  std::vector<T> v,
		  hobject_t h) && {
    if (budget >= 0) {
      objecter->put_op_budget_bytes(budget);
      budget = -1;
    }

    std::move(on_finish)(ec, std::move(v), std::move(h));
  }
};

template<typename T>
struct CB_EnumerateReply {
  cb::list bl;

  Objecter* objecter;
  std::unique_ptr<EnumerationContext<T>> ctx;

  CB_EnumerateReply(Objecter* objecter,
		    std::unique_ptr<EnumerationContext<T>>&& ctx) :
    objecter(objecter), ctx(std::move(ctx)) {}

  void operator()(bs::error_code ec) {
    objecter->_enumerate_reply(std::move(bl), ec, std::move(ctx));
  }
};

template<typename T>
void Objecter::enumerate_objects(
  int64_t pool_id,
  std::string_view ns,
  hobject_t start,
  hobject_t end,
  const uint32_t max,
  const cb::list& filter_bl,
  fu2::unique_function<void(bs::error_code,
			    std::vector<T>,
			    hobject_t) &&> on_finish) {
  if (!end.is_max() && start > end) {
    lderr(cct) << __func__ << ": start " << start << " > end " << end << dendl;
    std::move(on_finish)(osdc_errc::precondition_violated, {}, {});
    return;
  }

  if (max < 1) {
    lderr(cct) << __func__ << ": result size may not be zero" << dendl;
    std::move(on_finish)(osdc_errc::precondition_violated, {}, {});
    return;
  }

  if (start.is_max()) {
    std::move(on_finish)({}, {}, {});
    return;
  }

  shared_lock rl(rwlock);
  ceph_assert(osdmap->get_epoch());
  if (!osdmap->test_flag(CEPH_OSDMAP_SORTBITWISE)) {
    rl.unlock();
    lderr(cct) << __func__ << ": SORTBITWISE cluster flag not set" << dendl;
    std::move(on_finish)(osdc_errc::not_supported, {}, {});
    return;
  }
  const pg_pool_t* p = osdmap->get_pg_pool(pool_id);
  if (!p) {
    lderr(cct) << __func__ << ": pool " << pool_id << " DNE in osd epoch "
	       << osdmap->get_epoch() << dendl;
    rl.unlock();
    std::move(on_finish)(osdc_errc::pool_dne, {}, {});
    return;
  } else {
    rl.unlock();
  }

  _issue_enumerate(start,
		   std::make_unique<EnumerationContext<T>>(
		     this, std::move(end), filter_bl,
		     max, object_locator_t{pool_id, ns},
		     std::move(on_finish)));
}

template
void Objecter::enumerate_objects<librados::ListObjectImpl>(
  int64_t pool_id,
  std::string_view ns,
  hobject_t start,
  hobject_t end,
  const uint32_t max,
  const cb::list& filter_bl,
  fu2::unique_function<void(bs::error_code,
			    std::vector<librados::ListObjectImpl>,
			    hobject_t) &&> on_finish);

template
void Objecter::enumerate_objects<neorados::Entry>(
  int64_t pool_id,
  std::string_view ns,
  hobject_t start,
  hobject_t end,
  const uint32_t max,
  const cb::list& filter_bl,
  fu2::unique_function<void(bs::error_code,
			    std::vector<neorados::Entry>,
			    hobject_t) &&> on_finish);



template<typename T>
void Objecter::_issue_enumerate(hobject_t start,
				std::unique_ptr<EnumerationContext<T>> ctx) {
  ObjectOperation op;
  auto c = ctx.get();
  op.pg_nls(c->max, c->filter, start, osdmap->get_epoch());
  auto on_ack = std::make_unique<CB_EnumerateReply<T>>(this, std::move(ctx));
  // I hate having to do this. Try to find a cleaner way
  // later.
  auto epoch = &c->epoch;
  auto budget = &c->budget;
  auto pbl = &on_ack->bl;

  // Issue.  See you later in _enumerate_reply
  auto e = boost::asio::prefer(
    service.get_executor(),
    boost::asio::execution::outstanding_work.tracked);
  pg_read(start.get_hash(),
	  c->oloc, op, pbl, 0,
	  asio::bind_executor(e,
			     [c = std::move(on_ack)]
			     (bs::error_code ec) mutable {
			       (*c)(ec);
			     }), epoch, budget);
}

template
void Objecter::_issue_enumerate<librados::ListObjectImpl>(
  hobject_t start,
  std::unique_ptr<EnumerationContext<librados::ListObjectImpl>> ctx);
template
void Objecter::_issue_enumerate<neorados::Entry>(
  hobject_t start, std::unique_ptr<EnumerationContext<neorados::Entry>> ctx);

template<typename T>
void Objecter::_enumerate_reply(
  cb::list&& bl,
  bs::error_code ec,
  std::unique_ptr<EnumerationContext<T>>&& ctx)
{
  if (ec) {
    std::move(*ctx)(ec, {}, {});
    return;
  }

  // Decode the results
  auto iter = bl.cbegin();
  pg_nls_response_template<T> response;

  try {
    response.decode(iter);
    if (!iter.end()) {
      // extra_info isn't used anywhere. We do this solely to preserve
      // backward compatibility
      cb::list legacy_extra_info;
      decode(legacy_extra_info, iter);
    }
  } catch (const bs::system_error& e) {
    std::move(*ctx)(e.code(), {}, {});
    return;
  }

  shared_lock rl(rwlock);
  auto pool = osdmap->get_pg_pool(ctx->oloc.get_pool());
  rl.unlock();
  if (!pool) {
    // pool is gone, drop any results which are now meaningless.
    std::move(*ctx)(osdc_errc::pool_dne, {}, {});
    return;
  }

  hobject_t next;
  if ((response.handle <= ctx->end)) {
    next = response.handle;
  } else {
    next = ctx->end;

    // drop anything after 'end'
    while (!response.entries.empty()) {
      uint32_t hash = response.entries.back().locator.empty() ?
	pool->hash_key(response.entries.back().oid,
		       response.entries.back().nspace) :
	pool->hash_key(response.entries.back().locator,
		       response.entries.back().nspace);
      hobject_t last(response.entries.back().oid,
		     response.entries.back().locator,
		     CEPH_NOSNAP,
		     hash,
		     ctx->oloc.get_pool(),
		     response.entries.back().nspace);
      if (last < ctx->end)
	break;
      response.entries.pop_back();
    }
  }

  if (response.entries.size() <= ctx->max) {
    ctx->max -= response.entries.size();
    std::move(response.entries.begin(), response.entries.end(),
	      std::back_inserter(ctx->ls));
  } else {
    auto i = response.entries.begin();
    while (ctx->max > 0) {
      ctx->ls.push_back(std::move(*i));
      --(ctx->max);
      ++i;
    }
    uint32_t hash =
      i->locator.empty() ?
      pool->hash_key(i->oid, i->nspace) :
      pool->hash_key(i->locator, i->nspace);

    next = hobject_t{i->oid, i->locator,
		     CEPH_NOSNAP,
		     hash,
		     ctx->oloc.get_pool(),
		     i->nspace};
  }

  if (next == ctx->end || ctx->max == 0) {
    std::move(*ctx)(ec, std::move(ctx->ls), std::move(next));
  } else {
    _issue_enumerate(next, std::move(ctx));
  }
}

template
void Objecter::_enumerate_reply<librados::ListObjectImpl>(
  cb::list&& bl,
  bs::error_code ec,
  std::unique_ptr<EnumerationContext<librados::ListObjectImpl>>&& ctx);

template
void Objecter::_enumerate_reply<neorados::Entry>(
  cb::list&& bl,
  bs::error_code ec,
  std::unique_ptr<EnumerationContext<neorados::Entry>>&& ctx);

namespace {
  using namespace librados;

  template <typename T>
  void do_decode(std::vector<T>& items, std::vector<cb::list>& bls)
  {
    for (auto bl : bls) {
      auto p = bl.cbegin();
      T t;
      decode(t, p);
      items.push_back(t);
    }
  }

  struct C_ObjectOperation_scrub_ls : public Context {
    cb::list bl;
    uint32_t* interval;
    std::vector<inconsistent_obj_t> *objects = nullptr;
    std::vector<inconsistent_snapset_t> *snapsets = nullptr;
    int* rval;

    C_ObjectOperation_scrub_ls(uint32_t* interval,
			       std::vector<inconsistent_obj_t>* objects,
			       int* rval)
      : interval(interval), objects(objects), rval(rval) {}
    C_ObjectOperation_scrub_ls(uint32_t* interval,
			       std::vector<inconsistent_snapset_t>* snapsets,
			       int* rval)
      : interval(interval), snapsets(snapsets), rval(rval) {}
    void finish(int r) override {
      if (r < 0 && r != -EAGAIN) {
        if (rval)
          *rval = r;
	return;
      }

      if (rval)
        *rval = 0;

      try {
	decode();
      } catch (cb::error&) {
	if (rval)
	  *rval = -EIO;
      }
    }
  private:
    void decode() {
      scrub_ls_result_t result;
      auto p = bl.cbegin();
      result.decode(p);
      *interval = result.interval;
      if (objects) {
	do_decode(*objects, result.vals);
      } else {
	do_decode(*snapsets, result.vals);
      }
    }
  };

  template <typename T>
  void do_scrub_ls(::ObjectOperation* op,
		   const scrub_ls_arg_t& arg,
		   std::vector<T> *items,
		   uint32_t* interval,
		   int* rval)
  {
    OSDOp& osd_op = op->add_op(CEPH_OSD_OP_SCRUBLS);
    op->flags |= CEPH_OSD_FLAG_PGOP;
    ceph_assert(interval);
    arg.encode(osd_op.indata);
    unsigned p = op->ops.size() - 1;
    auto h = new C_ObjectOperation_scrub_ls{interval, items, rval};
    op->set_handler(h);
    op->out_bl[p] = &h->bl;
    op->out_rval[p] = rval;
  }
}

void ::ObjectOperation::scrub_ls(const librados::object_id_t& start_after,
				 uint64_t max_to_get,
				 std::vector<librados::inconsistent_obj_t>* objects,
				 uint32_t* interval,
				 int* rval)
{
  scrub_ls_arg_t arg = {*interval, 0, start_after, max_to_get};
  do_scrub_ls(this, arg, objects, interval, rval);
}

void ::ObjectOperation::scrub_ls(const librados::object_id_t& start_after,
				 uint64_t max_to_get,
				 std::vector<librados::inconsistent_snapset_t> *snapsets,
				 uint32_t *interval,
				 int *rval)
{
  scrub_ls_arg_t arg = {*interval, 1, start_after, max_to_get};
  do_scrub_ls(this, arg, snapsets, interval, rval);
}
