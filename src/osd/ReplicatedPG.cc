// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "boost/tuple/tuple.hpp"
#include "PG.h"
#include "ReplicatedPG.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/errno.h"
#include "common/perf_counters.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"

#include "messages/MOSDPing.h"
#include "messages/MWatchNotify.h"

#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"

#include "Watch.h"

#include "mds/inode_backtrace.h" // Ugh

#include "common/config.h"
#include "include/compat.h"
#include "common/cmdparse.h"

#include "mon/MonClient.h"
#include "osdc/Objecter.h"

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"
#include "include/assert.h"  // json_spirit clobbers it

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
template <typename T>
static ostream& _prefix(std::ostream *_dout, T *pg) {
  return *_dout << pg->gen_prefix();
}


#include <sstream>
#include <utility>

#include <errno.h>

PGLSFilter::PGLSFilter()
{
}

PGLSFilter::~PGLSFilter()
{
}

static void log_subop_stats(
  PerfCounters *logger,
  OpRequestRef op, int tag_inb, int tag_lat)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->get_req()->get_recv_stamp();

  uint64_t inb = op->get_req()->get_data().length();

  logger->inc(l_osd_sop);

  logger->inc(l_osd_sop_inb, inb);
  logger->tinc(l_osd_sop_lat, latency);

  if (tag_inb)
    logger->inc(tag_inb, inb);
  logger->tinc(tag_lat, latency);
}

struct OnReadComplete : public Context {
  ReplicatedPG *pg;
  ReplicatedPG::OpContext *opcontext;
  OnReadComplete(
    ReplicatedPG *pg,
    ReplicatedPG::OpContext *ctx) : pg(pg), opcontext(ctx) {}
  void finish(int r) {
    if (r < 0)
      opcontext->async_read_result = r;
    opcontext->finish_read(pg);
  }
  ~OnReadComplete() {}
};

// OpContext
void ReplicatedPG::OpContext::start_async_reads(ReplicatedPG *pg)
{
  inflightreads = 1;
  pg->pgbackend->objects_read_async(
    obc->obs.oi.soid,
    pending_async_reads,
    new OnReadComplete(pg, this));
  pending_async_reads.clear();
}
void ReplicatedPG::OpContext::finish_read(ReplicatedPG *pg)
{
  assert(inflightreads > 0);
  --inflightreads;
  if (async_reads_complete()) {
    assert(pg->in_progress_async_reads.size());
    assert(pg->in_progress_async_reads.front().second == this);
    pg->in_progress_async_reads.pop_front();
    pg->complete_read_ctx(async_read_result, this);
  }
}

class CopyFromCallback: public ReplicatedPG::CopyCallback {
public:
  ReplicatedPG::CopyResults *results;
  int retval;
  ReplicatedPG::OpContext *ctx;
  CopyFromCallback(ReplicatedPG::OpContext *ctx_)
    : results(NULL),
      retval(0),
      ctx(ctx_) {}
  ~CopyFromCallback() {}

  virtual void finish(ReplicatedPG::CopyCallbackResults results_) {
    results = results_.get<1>();
    int r = results_.get<0>();
    retval = r;

    // for finish_copyfrom
    ctx->user_at_version = results->user_version;

    if (r >= 0) {
      ctx->pg->execute_ctx(ctx);
    }
    ctx->copy_cb = NULL;
    if (r < 0) {
      if (r != -ECANCELED) { // on cancel just toss it out; client resends
	if (ctx->op)
	  ctx->pg->osd->reply_op_error(ctx->op, r);
      } else if (results->should_requeue) {
	if (ctx->op)
	  ctx->pg->requeue_op(ctx->op);
      }
      ctx->pg->close_op_ctx(ctx, r);
    }
  }

  bool is_temp_obj_used() {
    return results->started_temp_obj;
  }
  uint64_t get_data_size() {
    return results->object_size;
  }
  int get_result() {
    return retval;
  }
};

// ======================
// PGBackend::Listener


void ReplicatedPG::on_local_recover_start(
  const hobject_t &oid,
  ObjectStore::Transaction *t)
{
  pg_log.revise_have(oid, eversion_t());
  remove_snap_mapped_object(*t, oid);
}

void ReplicatedPG::on_local_recover(
  const hobject_t &hoid,
  const object_stat_sum_t &stat_diff,
  const ObjectRecoveryInfo &_recovery_info,
  ObjectContextRef obc,
  ObjectStore::Transaction *t
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;
  ObjectRecoveryInfo recovery_info(_recovery_info);
  if (recovery_info.soid.snap < CEPH_NOSNAP) {
    assert(recovery_info.oi.snaps.size());
    OSDriver::OSTransaction _t(osdriver.get_transaction(t));
    set<snapid_t> snaps(
      recovery_info.oi.snaps.begin(),
      recovery_info.oi.snaps.end());
    snap_mapper.add_oid(
      recovery_info.soid,
      snaps,
      &_t);
  }

  if (pg_log.get_missing().is_missing(recovery_info.soid) &&
      pg_log.get_missing().missing.find(recovery_info.soid)->second.need > recovery_info.version) {
    assert(is_primary());
    const pg_log_entry_t *latest = pg_log.get_log().objects.find(recovery_info.soid)->second;
    if (latest->op == pg_log_entry_t::LOST_REVERT &&
	latest->reverting_to == recovery_info.version) {
      dout(10) << " got old revert version " << recovery_info.version
	       << " for " << *latest << dendl;
      recovery_info.version = latest->version;
      // update the attr to the revert event version
      recovery_info.oi.prior_version = recovery_info.oi.version;
      recovery_info.oi.version = latest->version;
      bufferlist bl;
      ::encode(recovery_info.oi, bl);
      t->setattr(coll, recovery_info.soid, OI_ATTR, bl);
      if (obc)
	obc->attr_cache[OI_ATTR] = bl;
    }
  }

  // keep track of active pushes for scrub
  ++active_pushes;

  recover_got(recovery_info.soid, recovery_info.version);

  if (is_primary()) {
    info.stats.stats.sum.add(stat_diff);

    assert(obc);
    obc->obs.exists = true;
    obc->ondisk_write_lock();
    obc->obs.oi = recovery_info.oi;  // may have been updated above


    t->register_on_applied(new C_OSD_AppliedRecoveredObject(this, obc));
    t->register_on_applied_sync(new C_OSD_OndiskWriteUnlock(obc));

    publish_stats_to_osd();
    assert(missing_loc.needs_recovery(hoid));
    missing_loc.add_location(hoid, pg_whoami);
    if (!is_unreadable_object(hoid) &&
        waiting_for_unreadable_object.count(hoid)) {
      dout(20) << " kicking unreadable waiters on " << hoid << dendl;
      requeue_ops(waiting_for_unreadable_object[hoid]);
      waiting_for_unreadable_object.erase(hoid);
    }
    if (pg_log.get_missing().missing.size() == 0) {
      requeue_ops(waiting_for_all_missing);
      waiting_for_all_missing.clear();
    }
  } else {
    t->register_on_applied(
      new C_OSD_AppliedRecoveredObjectReplica(this));

  }

  t->register_on_commit(
    new C_OSD_CommittedPushedObject(
      this,
      get_osdmap()->get_epoch(),
      info.last_complete));

  // update pg
  dirty_info = true;
  write_if_dirty(*t);

}

void ReplicatedPG::on_global_recover(
  const hobject_t &soid)
{
  missing_loc.recovered(soid);
  publish_stats_to_osd();
  dout(10) << "pushed " << soid << " to all replicas" << dendl;
  map<hobject_t, ObjectContextRef>::iterator i = recovering.find(soid);
  assert(i != recovering.end());
  if (backfills_in_flight.count(soid)) {
    list<OpRequestRef> requeue_list;
    i->second->drop_backfill_read(&requeue_list);
    requeue_ops(requeue_list);
    backfills_in_flight.erase(soid);
  }
  recovering.erase(i);
  finish_recovery_op(soid);
  if (waiting_for_degraded_object.count(soid)) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(waiting_for_degraded_object[soid]);
    waiting_for_degraded_object.erase(soid);
  }
  if (waiting_for_unreadable_object.count(soid)) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(waiting_for_unreadable_object[soid]);
    waiting_for_unreadable_object.erase(soid);
  }
  finish_degraded_object(soid);
}

void ReplicatedPG::on_peer_recover(
  pg_shard_t peer,
  const hobject_t &soid,
  const ObjectRecoveryInfo &recovery_info,
  const object_stat_sum_t &stat)
{
  info.stats.stats.sum.add(stat);
  publish_stats_to_osd();
  // done!
  peer_missing[peer].got(soid, recovery_info.version);
}

void ReplicatedPG::begin_peer_recover(
  pg_shard_t peer,
  const hobject_t soid)
{
  peer_missing[peer].revise_have(soid, eversion_t());
}

void ReplicatedPG::schedule_work(
  GenContext<ThreadPool::TPHandle&> *c)
{
  osd->gen_wq.queue(c);
}

void ReplicatedPG::send_message_osd_cluster(
  int peer, Message *m, epoch_t from_epoch)
{
  osd->send_message_osd_cluster(peer, m, from_epoch);
}

void ReplicatedPG::send_message_osd_cluster(
  Message *m, Connection *con)
{
  osd->send_message_osd_cluster(m, con);
}

void ReplicatedPG::send_message_osd_cluster(
  Message *m, const ConnectionRef& con)
{
  osd->send_message_osd_cluster(m, con);
}

ConnectionRef ReplicatedPG::get_con_osd_cluster(
  int peer, epoch_t from_epoch)
{
  return osd->get_con_osd_cluster(peer, from_epoch);
}

PerfCounters *ReplicatedPG::get_logger()
{
  return osd->logger;
}


// ====================
// missing objects

bool ReplicatedPG::is_missing_object(const hobject_t& soid) const
{
  return pg_log.get_missing().missing.count(soid);
}

void ReplicatedPG::wait_for_unreadable_object(
  const hobject_t& soid, OpRequestRef op)
{
  assert(is_unreadable_object(soid));

  eversion_t v;
  bool needs_recovery = missing_loc.needs_recovery(soid, &v);
  assert(needs_recovery);

  map<hobject_t, ObjectContextRef>::const_iterator p = recovering.find(soid);
  if (p != recovering.end()) {
    dout(7) << "missing " << soid << " v " << v << ", already recovering." << dendl;
  } else if (missing_loc.is_unfound(soid)) {
    dout(7) << "missing " << soid << " v " << v << ", is unfound." << dendl;
  } else {
    dout(7) << "missing " << soid << " v " << v << ", recovering." << dendl;
    PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
    recover_missing(soid, v, cct->_conf->osd_client_op_priority, h);
    pgbackend->run_recovery_op(h, cct->_conf->osd_client_op_priority);
  }
  waiting_for_unreadable_object[soid].push_back(op);
  op->mark_delayed("waiting for missing object");
}

void ReplicatedPG::wait_for_all_missing(OpRequestRef op)
{
  waiting_for_all_missing.push_back(op);
}

bool ReplicatedPG::is_degraded_object(const hobject_t& soid)
{
  if (pg_log.get_missing().missing.count(soid))
    return true;
  assert(actingbackfill.size() > 0);
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    if (peer_missing.count(peer) &&
	peer_missing[peer].missing.count(soid))
      return true;

    // Object is degraded if after last_backfill AND
    // we are backfilling it
    if (is_backfill_targets(peer) &&
	peer_info[peer].last_backfill <= soid &&
	last_backfill_started >= soid &&
	backfills_in_flight.count(soid))
      return true;
  }
  return false;
}

void ReplicatedPG::wait_for_degraded_object(const hobject_t& soid, OpRequestRef op)
{
  assert(is_degraded_object(soid));

  // we don't have it (yet).
  if (recovering.count(soid)) {
    dout(7) << "degraded "
	    << soid 
	    << ", already recovering"
	    << dendl;
  } else if (missing_loc.is_unfound(soid)) {
    dout(7) << "degraded "
	    << soid
	    << ", still unfound, waiting"
	    << dendl;
  } else {
    dout(7) << "degraded " 
	    << soid 
	    << ", recovering"
	    << dendl;
    eversion_t v;
    assert(actingbackfill.size() > 0);
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      pg_shard_t peer = *i;
      if (peer_missing.count(peer) &&
	  peer_missing[peer].missing.count(soid)) {
	v = peer_missing[peer].missing[soid].need;
	break;
      }
    }
    PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
    prep_object_replica_pushes(soid, v, h);
    pgbackend->run_recovery_op(h, cct->_conf->osd_client_op_priority);
  }
  waiting_for_degraded_object[soid].push_back(op);
  op->mark_delayed("waiting for degraded object");
}

bool ReplicatedPG::maybe_await_blocked_snapset(
  const hobject_t &hoid,
  OpRequestRef op)
{
  ObjectContextRef obc;
  if (obc = object_contexts.lookup(hoid.get_head())) {
    if (obc->is_blocked()) {
      wait_for_blocked_object(obc->obs.oi.soid, op);
      return true;
    } else {
      return false;
    }
  }
  if (obc = object_contexts.lookup(hoid.get_snapdir())) {
    if (obc->is_blocked()) {
      wait_for_blocked_object(obc->obs.oi.soid, op);
      return true;
    } else {
      return false;
    }
  }
  return false;
}

void ReplicatedPG::wait_for_blocked_object(const hobject_t& soid, OpRequestRef op)
{
  dout(10) << __func__ << " " << soid << " " << op << dendl;
  waiting_for_blocked_object[soid].push_back(op);
  op->mark_delayed("waiting for blocked object");
}

bool PGLSParentFilter::filter(bufferlist& xattr_data, bufferlist& outdata)
{
  bufferlist::iterator iter = xattr_data.begin();
  inode_backtrace_t bt;

  generic_dout(0) << "PGLSParentFilter::filter" << dendl;

  ::decode(bt, iter);

  vector<inode_backpointer_t>::iterator vi;
  for (vi = bt.ancestors.begin(); vi != bt.ancestors.end(); ++vi) {
    generic_dout(0) << "vi->dirino=" << vi->dirino << " parent_ino=" << parent_ino << dendl;
    if ( vi->dirino == parent_ino) {
      ::encode(*vi, outdata);
      return true;
    }
  }

  return false;
}

bool PGLSPlainFilter::filter(bufferlist& xattr_data, bufferlist& outdata)
{
  if (val.size() != xattr_data.length())
    return false;

  if (memcmp(val.c_str(), xattr_data.c_str(), val.size()))
    return false;

  return true;
}

bool ReplicatedPG::pgls_filter(PGLSFilter *filter, hobject_t& sobj, bufferlist& outdata)
{
  bufferlist bl;
  int ret = pgbackend->objects_get_attr(
    sobj,
    filter->get_xattr(),
    &bl);
  dout(0) << "getattr (sobj=" << sobj << ", attr=" << filter->get_xattr() << ") returned " << ret << dendl;
  if (ret < 0)
    return false;

  return filter->filter(bl, outdata);
}

int ReplicatedPG::get_pgls_filter(bufferlist::iterator& iter, PGLSFilter **pfilter)
{
  string type;
  PGLSFilter *filter;

  try {
    ::decode(type, iter);
  }
  catch (buffer::error& e) {
    return -EINVAL;
  }

  if (type.compare("parent") == 0) {
    filter = new PGLSParentFilter(iter);
  } else if (type.compare("plain") == 0) {
    filter = new PGLSPlainFilter(iter);
  } else {
    return -EINVAL;
  }

  *pfilter = filter;

  return  0;
}


// ==========================================================

int ReplicatedPG::do_command(cmdmap_t cmdmap, ostream& ss,
			     bufferlist& idata, bufferlist& odata)
{
  const pg_missing_t &missing = pg_log.get_missing();
  string prefix;
  string format;

  cmd_getval(cct, cmdmap, "format", format);
  boost::scoped_ptr<Formatter> f(new_formatter(format));
  // demand that we have a formatter
  if (!f)
    f.reset(new_formatter("json"));

  string command;
  cmd_getval(cct, cmdmap, "cmd", command);
  if (command == "query") {
    f->open_object_section("pg");
    f->dump_string("state", pg_state_string(get_state()));
    f->dump_stream("snap_trimq") << snap_trimq;
    f->dump_unsigned("epoch", get_osdmap()->get_epoch());
    f->open_array_section("up");
    for (vector<int>::iterator p = up.begin(); p != up.end(); ++p)
      f->dump_unsigned("osd", *p);
    f->close_section();
    f->open_array_section("acting");
    for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p)
      f->dump_unsigned("osd", *p);
    f->close_section();
    if (backfill_targets.size() > 0) {
      f->open_array_section("backfill_targets");
      for (set<pg_shard_t>::iterator p = backfill_targets.begin();
	   p != backfill_targets.end();
	   ++p)
        f->dump_stream("shard") << *p;
      f->close_section();
    }
    if (actingbackfill.size() > 0) {
      f->open_array_section("actingbackfill");
      for (set<pg_shard_t>::iterator p = actingbackfill.begin();
	   p != actingbackfill.end();
	   ++p)
        f->dump_stream("shard") << *p;
      f->close_section();
    }
    f->open_object_section("info");
    _update_calc_stats();
    info.dump(f.get());
    f->close_section();

    f->open_array_section("peer_info");
    for (map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
	 p != peer_info.end();
	 ++p) {
      f->open_object_section("info");
      f->dump_stream("peer") << p->first;
      p->second.dump(f.get());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("recovery_state");
    handle_query_state(f.get());
    f->close_section();

    f->open_object_section("agent_state");
    if (agent_state)
      agent_state->dump(f.get());
    f->close_section();

    f->close_section();
    f->flush(odata);
    return 0;
  }
  else if (command == "mark_unfound_lost") {
    string mulcmd;
    cmd_getval(cct, cmdmap, "mulcmd", mulcmd);
    int mode = -1;
    if (mulcmd == "revert") {
      if (pool.info.ec_pool()) {
	ss << "mode must be 'delete' for ec pool";
	return -EINVAL;
      }
      mode = pg_log_entry_t::LOST_REVERT;
    } else if (mulcmd == "delete") {
      mode = pg_log_entry_t::LOST_DELETE;
    } else {
      ss << "mode must be 'revert' or 'delete'; mark not yet implemented";
      return -EINVAL;
    }
    assert(mode == pg_log_entry_t::LOST_REVERT ||
	   mode == pg_log_entry_t::LOST_DELETE);

    if (!is_primary()) {
      ss << "not primary";
      return -EROFS;
    }

    int unfound = missing_loc.num_unfound();
    if (!unfound) {
      ss << "pg has no unfound objects";
      return 0;  // make command idempotent
    }

    if (!all_unfound_are_queried_or_lost(get_osdmap())) {
      ss << "pg has " << unfound
	 << " unfound objects but we haven't probed all sources, not marking lost";
      return -EINVAL;
    }

    ss << "pg has " << unfound
       << " objects unfound and apparently lost, marking";
    mark_all_unfound_lost(mode);
    return 0;
  }
  else if (command == "list_missing") {
    hobject_t offset;
    string offset_json;
    if (cmd_getval(cct, cmdmap, "offset", offset_json)) {
      json_spirit::Value v;
      try {
	if (!json_spirit::read(offset_json, v))
	  throw std::runtime_error("bad json");
	offset.decode(v);
      } catch (std::runtime_error& e) {
	ss << "error parsing offset: " << e.what();
	return -EINVAL;
      }
    }
    f->open_object_section("missing");
    {
      f->open_object_section("offset");
      offset.dump(f.get());
      f->close_section();
    }
    f->dump_int("num_missing", missing.num_missing());
    f->dump_int("num_unfound", get_num_unfound());
    map<hobject_t,pg_missing_t::item>::const_iterator p = missing.missing.upper_bound(offset);
    {
      f->open_array_section("objects");
      int32_t num = 0;
      bufferlist bl;
      while (p != missing.missing.end() && num < cct->_conf->osd_command_max_records) {
	f->open_object_section("object");
	{
	  f->open_object_section("oid");
	  p->first.dump(f.get());
	  f->close_section();
	}
	p->second.dump(f.get());  // have, need keys
	{
	  f->open_array_section("locations");
	  if (missing_loc.needs_recovery(p->first)) {
	    for (set<pg_shard_t>::iterator r =
		   missing_loc.get_locations(p->first).begin();
		 r != missing_loc.get_locations(p->first).end();
		 ++r)
	      f->dump_stream("shard") << *r;
	  }
	  f->close_section();
	}
	f->close_section();
	++p;
	num++;
      }
      f->close_section();
    }
    f->dump_int("more", p != missing.missing.end());
    f->close_section();
    f->flush(odata);
    return 0;
  };

  ss << "unknown pg command " << prefix;
  return -EINVAL;
}

// ==========================================================

bool ReplicatedPG::pg_op_must_wait(MOSDOp *op)
{
  if (pg_log.get_missing().missing.empty())
    return false;
  for (vector<OSDOp>::iterator p = op->ops.begin(); p != op->ops.end(); ++p) {
    if (p->op.op == CEPH_OSD_OP_PGLS) {
      if (op->get_snapid() != CEPH_NOSNAP) {
	return true;
      }
    }
  }
  return false;
}

void ReplicatedPG::do_pg_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp *>(op->get_req());
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  dout(10) << "do_pg_op " << *m << dendl;

  op->mark_started();

  int result = 0;
  string cname, mname;
  PGLSFilter *filter = NULL;
  bufferlist filter_out;

  snapid_t snapid = m->get_snapid();

  vector<OSDOp> ops = m->ops;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    OSDOp& osd_op = *p;
    bufferlist::iterator bp = p->indata.begin();
    switch (p->op.op) {
    case CEPH_OSD_OP_PGLS_FILTER:
      try {
	::decode(cname, bp);
	::decode(mname, bp);
      }
      catch (const buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      result = get_pgls_filter(bp, &filter);
      if (result < 0)
        break;

      assert(filter);

      // fall through

    case CEPH_OSD_OP_PGLS:
      if (m->get_pg() != info.pgid.pgid) {
        dout(10) << " pgls pg=" << m->get_pg() << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = MIN(cct->_conf->osd_max_pgls, p->op.pgls.count);

        dout(10) << " pgls pg=" << m->get_pg() << " count " << list_size << dendl;
	// read into a buffer
        vector<hobject_t> sentries;
        pg_ls_response_t response;
	try {
	  ::decode(response.handle, bp);
	}
	catch (const buffer::error& e) {
	  dout(0) << "unable to decode PGLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t current = response.handle;
	osr->flush();
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  snapid,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	assert(snapid == CEPH_NOSNAP || pg_log.get_missing().missing.empty());
	map<hobject_t, pg_missing_t::item>::const_iterator missing_iter =
	  pg_log.get_missing().missing.lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t &mcand =
	    missing_iter == pg_log.get_missing().missing.end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t &lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    assert(!lcand.is_max());
	    ++ls_iter;
	  }

	  if (candidate >= next) {
	    break;
	  }
	    
	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  // skip snapdir objects
	  if (candidate.snap == CEPH_SNAPDIR)
	    continue;

	  if (candidate.snap < snapid)
	    continue;

	  if (snapid != CEPH_NOSNAP) {
	    bufferlist bl;
	    if (candidate.snap == CEPH_NOSNAP) {
	      pgbackend->objects_get_attr(
		candidate,
		SS_ATTR,
		&bl);
	      SnapSet snapset(bl);
	      if (snapid <= snapset.seq)
		continue;
	    } else {
	      bufferlist attr_bl;
	      pgbackend->objects_get_attr(
		candidate, OI_ATTR, &attr_bl);
	      object_info_t oi(attr_bl);
	      vector<snapid_t>::iterator i = find(oi.snaps.begin(),
						  oi.snaps.end(),
						  snapid);
	      if (i == oi.snaps.end())
		continue;
	    }
	  }

	  // skip wrong namespace
	  if (candidate.get_namespace() != m->get_object_locator().nspace)
	    continue;

	  if (filter && !pgls_filter(filter, candidate, filter_out))
	    continue;

	  response.entries.push_back(make_pair(candidate.oid,
					       candidate.get_key()));
	}
	if (next.is_max() &&
	    missing_iter == pg_log.get_missing().missing.end() &&
	    ls_iter == sentries.end()) {
	  result = 1;
	}
	response.handle = next;
	::encode(response, osd_op.outdata);
	if (filter)
	  ::encode(filter_out, osd_op.outdata);
	dout(10) << " pgls result=" << result << " outdata.length()="
		 << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_LS:
      {
	list< pair<utime_t,utime_t> > ls;
	for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	     p != info.hit_set.history.end();
	     ++p)
	  ls.push_back(make_pair(p->begin, p->end));
	if (info.hit_set.current_info.begin)
	  ls.push_back(make_pair(info.hit_set.current_info.begin, utime_t()));
	else if (hit_set)
	  ls.push_back(make_pair(hit_set_start_stamp, utime_t()));
	::encode(ls, osd_op.outdata);
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_GET:
      {
	utime_t stamp(osd_op.op.hit_set_get.stamp);
	if ((info.hit_set.current_info.begin &&
	     stamp >= info.hit_set.current_info.begin) ||
	    stamp >= hit_set_start_stamp) {
	  // read the current in-memory HitSet, not the version we've
	  // checkpointed.
	  if (!hit_set) {
	    result= -ENOENT;
	    break;
	  }
	  ::encode(*hit_set, osd_op.outdata);
	  result = osd_op.outdata.length();
	} else {
	  // read an archived HitSet.
	  hobject_t oid;
	  for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	       p != info.hit_set.history.end();
	       ++p) {
	    if (stamp >= p->begin && stamp <= p->end) {
	      oid = get_hit_set_archive_object(p->begin, p->end);
	      break;
	    }
	  }
	  if (oid == hobject_t()) {
	    result = -ENOENT;
	    break;
	  }
	  if (!pool.info.is_replicated()) {
	    // FIXME: EC not supported yet
	    result = -EOPNOTSUPP;
	    break;
	  }
	  if (is_unreadable_object(oid)) {
	    wait_for_unreadable_object(oid, op);
	    return;
	  }
	  result = osd->store->read(coll, oid, 0, 0, osd_op.outdata);
	}
      }
      break;


    default:
      result = -EINVAL;
      break;
    }
  }

  // reply
  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(),
				       CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
				       false);
  reply->claim_op_out_data(ops);
  reply->set_result(result);
  reply->set_reply_versions(info.last_update, info.last_user_version);
  osd->send_message_osd_client(reply, m->get_connection());
  delete filter;
}

void ReplicatedPG::calc_trim_to()
{
  if (is_scrubbing() && scrubber.classic) {
    dout(10) << "calc_trim_to no trim during classic scrub" << dendl;
    pg_trim_to = eversion_t();
    return;
  }

  size_t target = cct->_conf->osd_min_pg_log_entries;
  if (is_degraded() ||
      state_test(PG_STATE_RECOVERING |
		 PG_STATE_RECOVERY_WAIT |
		 PG_STATE_BACKFILL |
		 PG_STATE_BACKFILL_WAIT |
		 PG_STATE_BACKFILL_TOOFULL)) {
    target = cct->_conf->osd_max_pg_log_entries;
  }

  if (min_last_complete_ondisk != eversion_t() &&
      min_last_complete_ondisk != pg_trim_to &&
      pg_log.get_log().approx_size() > target) {
    size_t num_to_trim = pg_log.get_log().approx_size() - target;
    list<pg_log_entry_t>::const_iterator it = pg_log.get_log().log.begin();
    eversion_t new_trim_to;
    for (size_t i = 0; i < num_to_trim; ++i) {
      new_trim_to = it->version;
      ++it;
      if (new_trim_to > min_last_complete_ondisk) {
	new_trim_to = min_last_complete_ondisk;
	dout(10) << "calc_trim_to trimming to min_last_complete_ondisk" << dendl;
	break;
      }
    }
    dout(10) << "calc_trim_to " << pg_trim_to << " -> " << new_trim_to << dendl;
    pg_trim_to = new_trim_to;
    assert(pg_trim_to <= pg_log.get_head());
    assert(pg_trim_to <= min_last_complete_ondisk);
  }
}

ReplicatedPG::ReplicatedPG(OSDService *o, OSDMapRef curmap,
			   const PGPool &_pool, spg_t p, const hobject_t& oid,
			   const hobject_t& ioid) :
  PG(o, curmap, _pool, p, oid, ioid),
  pgbackend(
    PGBackend::build_pg_backend(
      _pool.info, curmap, this, coll_t(p), coll_t::make_temp_coll(p), o->store, cct)),
  snapset_contexts_lock("ReplicatedPG::snapset_contexts"),
  temp_seq(0),
  snap_trimmer_machine(this)
{ 
  missing_loc.set_backend_predicates(
    pgbackend->get_is_readable_predicate(),
    pgbackend->get_is_recoverable_predicate());
  snap_trimmer_machine.initiate();
}

void ReplicatedPG::get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc)
{
  src_oloc = oloc;
  if (oloc.key.empty())
    src_oloc.key = oid.name;
}

void ReplicatedPG::do_request(
  OpRequestRef op,
  ThreadPool::TPHandle &handle)
{
  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }
  assert(!op_must_wait_for_map(get_osdmap(), op));
  if (can_discard_request(op)) {
    return;
  }
  if (flushes_in_progress > 0) {
    dout(20) << flushes_in_progress
	     << " flushes_in_progress pending "
	     << "waiting for active on " << op << dendl;
    waiting_for_active.push_back(op);
    return;
  }

  if (!is_active()) {
    // Delay unless PGBackend says it's ok
    if (pgbackend->can_handle_while_inactive(op)) {
      bool handled = pgbackend->handle_message(op);
      assert(handled);
      return;
    } else {
      waiting_for_active.push_back(op);
      return;
    }
  }

  assert(is_active() && flushes_in_progress == 0);
  if (pgbackend->handle_message(op))
    return;

  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (is_replay()) {
      dout(20) << " replay, waiting for active on " << op << dendl;
      waiting_for_active.push_back(op);
      return;
    }
    // verify client features
    if ((pool.info.has_tiers() || pool.info.is_tier()) &&
	!op->has_feature(CEPH_FEATURE_OSD_CACHEPOOL)) {
      osd->reply_op_error(op, -EOPNOTSUPP);
      return;
    }
    do_op(op); // do it now
    break;

  case MSG_OSD_SUBOP:
    do_sub_op(op);
    break;

  case MSG_OSD_SUBOPREPLY:
    do_sub_op_reply(op);
    break;

  case MSG_OSD_PG_SCAN:
    do_scan(op, handle);
    break;

  case MSG_OSD_PG_BACKFILL:
    do_backfill(op);
    break;

  default:
    assert(0 == "bad message type in do_request");
  }
}

hobject_t ReplicatedPG::earliest_backfill() const
{
  hobject_t e = hobject_t::get_max();
  for (set<pg_shard_t>::iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    pg_shard_t bt = *i;
    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(bt);
    assert(iter != peer_info.end());
    if (iter->second.last_backfill < e)
      e = iter->second.last_backfill;
  }
  return e;
}

// if we have src_oids, we need to be careful of the target being
// before and a src being after the last_backfill line, or else the
// operation won't apply properly on the backfill_target.  (the
// opposite is not a problem; if the target is after the line, we
// don't apply on the backfill_target and it doesn't matter.)
// With multi-backfill some backfill targets can be ahead of
// last_backfill_started.  We consider each replica individually and
// take the larger of last_backfill_started and the replicas last_backfill.
bool ReplicatedPG::check_src_targ(const hobject_t& soid, const hobject_t& toid) const
{
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t bt = *i;
    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(bt);
    assert(iter != peer_info.end());

    if (toid <= MAX(last_backfill_started, iter->second.last_backfill) &&
	soid > MAX(last_backfill_started, iter->second.last_backfill))
      return true;
  }
  return false;
}

/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void ReplicatedPG::do_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  if (op->includes_pg_op()) {
    if (pg_op_must_wait(m)) {
      wait_for_all_missing(op);
      return;
    }
    return do_pg_op(op);
  }

  if (get_osdmap()->is_blacklisted(m->get_source_addr())) {
    dout(10) << "do_op " << m->get_source_addr() << " is blacklisted" << dendl;
    osd->reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // order this op as a write?
  bool write_ordered =
    op->may_write() ||
    op->may_cache() ||
    (m->get_flags() & CEPH_OSD_FLAG_RWORDERED);

  dout(10) << "do_op " << *m
	   << (op->may_write() ? " may_write" : "")
	   << (op->may_read() ? " may_read" : "")
	   << (op->may_cache() ? " may_cache" : "")
	   << " -> " << (write_ordered ? "write-ordered" : "read-ordered")
	   << " flags " << ceph_osd_flag_string(m->get_flags())
	   << dendl;

  hobject_t head(m->get_oid(), m->get_object_locator().key,
		 CEPH_NOSNAP, m->get_pg().ps(),
		 info.pgid.pool(), m->get_object_locator().nspace);


  if (write_ordered && scrubber.write_blocked_by_scrub(head)) {
    dout(20) << __func__ << ": waiting for scrub" << dendl;
    waiting_for_active.push_back(op);
    op->mark_delayed("waiting for scrub");
    return;
  }

  // missing object?
  if (is_unreadable_object(head)) {
    wait_for_unreadable_object(head, op);
    return;
  }

  // degraded object?
  if (write_ordered && is_degraded_object(head)) {
    wait_for_degraded_object(head, op);
    return;
  }

  // missing snapdir?
  hobject_t snapdir(m->get_oid(), m->get_object_locator().key,
		    CEPH_SNAPDIR, m->get_pg().ps(), info.pgid.pool(),
		    m->get_object_locator().nspace);
  if (is_unreadable_object(snapdir)) {
    wait_for_unreadable_object(snapdir, op);
    return;
  }

  // degraded object?
  if (write_ordered && is_degraded_object(snapdir)) {
    wait_for_degraded_object(snapdir, op);
    return;
  }
 
  // asking for SNAPDIR is only ok for reads
  if (m->get_snapid() == CEPH_SNAPDIR && op->may_write()) {
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  // dup/replay?
  if (op->may_write() || op->may_cache()) {
    const pg_log_entry_t *entry = pg_log.get_log().get_request(m->get_reqid());
    if (entry) {
      const eversion_t& oldv = entry->version;
      dout(3) << __func__ << " dup " << m->get_reqid()
	      << " was " << oldv << dendl;
      if (already_complete(oldv)) {
	osd->reply_op_error(op, 0, oldv, entry->user_version);
      } else {
	if (m->wants_ack()) {
	  if (already_ack(oldv)) {
	    MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0, false);
	    reply->add_flags(CEPH_OSD_FLAG_ACK);
	    reply->set_reply_versions(oldv, entry->user_version);
	    osd->send_message_osd_client(reply, m->get_connection());
	  } else {
	    dout(10) << " waiting for " << oldv << " to ack" << dendl;
	    waiting_for_ack[oldv].push_back(op);
	  }
	}
	dout(10) << " waiting for " << oldv << " to commit" << dendl;
	waiting_for_ondisk[oldv].push_back(op);  // always queue ondisk waiters, so that we can requeue if needed
	op->mark_delayed("waiting for ondisk");
      }
      return;
    }
  }

  ObjectContextRef obc;
  bool can_create = op->may_write() || op->may_cache();
  hobject_t missing_oid;
  hobject_t oid(m->get_oid(),
		m->get_object_locator().key,
		m->get_snapid(),
		m->get_pg().ps(),
		m->get_object_locator().get_pool(),
		m->get_object_locator().nspace);

  // io blocked on obc?
  if (((m->get_flags() & CEPH_OSD_FLAG_FLUSH) == 0) &&
      maybe_await_blocked_snapset(oid, op)) {
    return;
  }

  int r = find_object_context(
    oid, &obc, can_create,
    m->get_flags() & CEPH_OSD_FLAG_MAP_SNAP_CLONE,
    &missing_oid);

  if (r == -EAGAIN) {
    // If we're not the primary of this OSD, and we have
    // CEPH_OSD_FLAG_LOCALIZE_READS set, we just return -EAGAIN. Otherwise,
    // we have to wait for the object.
    if (is_primary() ||
	(!(m->get_flags() & CEPH_OSD_FLAG_BALANCE_READS) &&
	 !(m->get_flags() & CEPH_OSD_FLAG_LOCALIZE_READS))) {
      // missing the specific snap we need; requeue and wait.
      assert(!op->may_write()); // only happens on a read/cache
      wait_for_unreadable_object(missing_oid, op);
      return;
    }
  } else if (r == 0) {
    if (is_unreadable_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
	       << " is unreadable, waiting" << dendl;
      wait_for_unreadable_object(obc->obs.oi.soid, op);
      return;
    }

    // degraded object?  (the check above was for head; this could be a clone)
    if (write_ordered &&
	obc->obs.oi.soid.snap != CEPH_NOSNAP &&
	is_degraded_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
	       << " is degraded, waiting" << dendl;
      wait_for_degraded_object(obc->obs.oi.soid, op);
      return;
    }
  }

  bool in_hit_set = false;
  if (hit_set) {
    if (missing_oid != hobject_t() && hit_set->contains(missing_oid))
      in_hit_set = true;
    hit_set->insert(oid);
    if (hit_set->is_full() ||
	hit_set_start_stamp + pool.info.hit_set_period <= m->get_recv_stamp()) {
      hit_set_persist();
    }
  }

  if (agent_state) {
    agent_choose_mode();
  }

  if ((m->get_flags() & CEPH_OSD_FLAG_IGNORE_CACHE) == 0 &&
      maybe_handle_cache(op, write_ordered, obc, r, missing_oid, false, in_hit_set))
    return;

  if (r) {
    osd->reply_op_error(op, r);
    return;
  }

  // make sure locator is consistent
  object_locator_t oloc(obc->obs.oi.soid);
  if (m->get_object_locator() != oloc) {
    dout(10) << " provided locator " << m->get_object_locator() 
	     << " != object's " << obc->obs.oi.soid << dendl;
    osd->clog.warn() << "bad locator " << m->get_object_locator() 
		     << " on object " << oloc
		     << " op " << *m << "\n";
  }

  // io blocked on obc?
  if (obc->is_blocked() &&
      (m->get_flags() & CEPH_OSD_FLAG_FLUSH) == 0) {
    wait_for_blocked_object(obc->obs.oi.soid, op);
    return;
  }

  dout(25) << __func__ << " oi " << obc->obs.oi << dendl;

  // are writes blocked by another object?
  if (obc->blocked_by) {
    dout(10) << "do_op writes for " << obc->obs.oi.soid << " blocked by "
	     << obc->blocked_by->obs.oi.soid << dendl;
    wait_for_degraded_object(obc->blocked_by->obs.oi.soid, op);
    return;
  }

  // src_oids
  map<hobject_t,ObjectContextRef> src_obc;
  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); ++p) {
    OSDOp& osd_op = *p;

    // make sure LIST_SNAPS is on CEPH_SNAPDIR and nothing else
    if (osd_op.op.op == CEPH_OSD_OP_LIST_SNAPS &&
	m->get_snapid() != CEPH_SNAPDIR) {
      dout(10) << "LIST_SNAPS with incorrect context" << dendl;
      osd->reply_op_error(op, -EINVAL);
      return;
    }

    if (!ceph_osd_op_type_multi(osd_op.op.op))
      continue;
    if (osd_op.soid.oid.name.length()) {
      object_locator_t src_oloc;
      get_src_oloc(m->get_oid(), m->get_object_locator(), src_oloc);
      hobject_t src_oid(osd_op.soid, src_oloc.key, m->get_pg().ps(),
			info.pgid.pool(), m->get_object_locator().nspace);
      if (!src_obc.count(src_oid)) {
	ObjectContextRef sobc;
	hobject_t wait_oid;
	int r;

	if (src_oid.is_head() && is_missing_object(src_oid)) {
	  wait_for_unreadable_object(src_oid, op);
	} else if ((r = find_object_context(
		      src_oid, &sobc, false, false,
		      &wait_oid)) == -EAGAIN) {
	  // missing the specific snap we need; requeue and wait.
	  wait_for_unreadable_object(wait_oid, op);
	} else if (r) {
	  if (!maybe_handle_cache(op, write_ordered, sobc, r, wait_oid, true))
	    osd->reply_op_error(op, r);
	} else if (sobc->obs.oi.is_whiteout()) {
	  osd->reply_op_error(op, -ENOENT);
	} else {
	  if (sobc->obs.oi.soid.get_key() != obc->obs.oi.soid.get_key() &&
		   sobc->obs.oi.soid.get_key() != obc->obs.oi.soid.oid.name &&
		   sobc->obs.oi.soid.oid.name != obc->obs.oi.soid.get_key()) {
	    dout(1) << " src_oid " << sobc->obs.oi.soid << " != "
		  << obc->obs.oi.soid << dendl;
	    osd->reply_op_error(op, -EINVAL);
	  } else if (is_degraded_object(sobc->obs.oi.soid) ||
		   (check_src_targ(sobc->obs.oi.soid, obc->obs.oi.soid))) {
	    if (is_degraded_object(sobc->obs.oi.soid)) {
	      wait_for_degraded_object(sobc->obs.oi.soid, op);
	    } else {
	      waiting_for_degraded_object[sobc->obs.oi.soid].push_back(op);
	      op->mark_delayed("waiting for degraded object");
	    }
	    dout(10) << " writes for " << obc->obs.oi.soid << " now blocked by "
		     << sobc->obs.oi.soid << dendl;
	    obc->blocked_by = sobc;
	    sobc->blocking.insert(obc);
	  } else {
	    dout(10) << " src_oid " << src_oid << " obc " << src_obc << dendl;
	    src_obc[src_oid] = sobc;
	    continue;
	  }
	}
	// Error cleanup below
      } else {
	continue;
      }
      // Error cleanup below
    } else {
      dout(10) << "no src oid specified for multi op " << osd_op << dendl;
      osd->reply_op_error(op, -EINVAL);
    }
    return;
  }

  // any SNAPDIR op needs to have all clones present.  treat them as
  // src_obc's so that we track references properly and clean up later.
  if (m->get_snapid() == CEPH_SNAPDIR) {
    for (vector<snapid_t>::iterator p = obc->ssc->snapset.clones.begin();
	 p != obc->ssc->snapset.clones.end();
	 ++p) {
      hobject_t clone_oid = obc->obs.oi.soid;
      clone_oid.snap = *p;
      if (!src_obc.count(clone_oid)) {
	if (is_unreadable_object(clone_oid)) {
	  wait_for_unreadable_object(clone_oid, op);
	  return;
	}

	ObjectContextRef sobc = get_object_context(clone_oid, false);
	if (!sobc) {
	  if (!maybe_handle_cache(op, write_ordered, sobc, -ENOENT, clone_oid, true))
	    osd->reply_op_error(op, -ENOENT);
	  return;
	} else {
	  dout(10) << " clone_oid " << clone_oid << " obc " << sobc << dendl;
	  src_obc[clone_oid] = sobc;
	  continue;
	}
	assert(0); // unreachable
      } else {
	continue;
      }
    }
  }

  OpContext *ctx = new OpContext(op, m->get_reqid(), m->ops,
				 &obc->obs, obc->ssc, 
				 this);
  ctx->op_t = pgbackend->get_transaction();
  ctx->obc = obc;

  if (!obc->obs.exists)
    ctx->snapset_obc = get_object_context(obc->obs.oi.soid.get_snapdir(), false);

  if (m->get_flags() & CEPH_OSD_FLAG_SKIPRWLOCKS) {
    dout(20) << __func__ << ": skipping rw locks" << dendl;
  } else if (m->get_flags() & CEPH_OSD_FLAG_FLUSH) {
    dout(20) << __func__ << ": part of flush, will ignore write lock" << dendl;

    // verify there is in fact a flush in progress
    // FIXME: we could make this a stronger test.
    map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(obc->obs.oi.soid);
    if (p == flush_ops.end()) {
      dout(10) << __func__ << " no flush in progress, aborting" << dendl;
      reply_ctx(ctx, -EINVAL);
      return;
    }
  } else if (!get_rw_locks(ctx)) {
    dout(20) << __func__ << " waiting for rw locks " << dendl;
    op->mark_delayed("waiting for rw locks");
    close_op_ctx(ctx, -EBUSY);
    return;
  }

  if ((op->may_read()) && (obc->obs.oi.is_lost())) {
    // This object is lost. Reading from it returns an error.
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
	     << " is lost" << dendl;
    reply_ctx(ctx, -ENFILE);
    return;
  }
  if (!op->may_write() &&
      !op->may_cache() &&
      (!obc->obs.exists ||
       ((m->get_snapid() != CEPH_SNAPDIR) &&
	obc->obs.oi.is_whiteout()))) {
    reply_ctx(ctx, -ENOENT);
    return;
  }

  op->mark_started();
  ctx->src_obc = src_obc;

  execute_ctx(ctx);
}

bool ReplicatedPG::maybe_handle_cache(OpRequestRef op,
				      bool write_ordered,
				      ObjectContextRef obc,
                                      int r, const hobject_t& missing_oid,
				      bool must_promote,
				      bool in_hit_set)
{
  if (obc)
    dout(25) << __func__ << " " << obc->obs.oi << " "
	     << (obc->obs.exists ? "exists" : "DNE")
	     << " missing_oid " << missing_oid
	     << dendl;
  else
    dout(25) << __func__ << " (no obc)"
	     << " missing_oid " << missing_oid
	     << dendl;

  if (obc.get() && obc->is_blocked()) {
    // we're already doing something with this object
    dout(20) << __func__ << " blocked on " << obc->obs.oi.soid << dendl;
    return false;
  }

  if (r == -ENOENT && missing_oid == hobject_t()) {
    // we know this object is logically absent (e.g., an undefined clone)
    return false;
  }

  switch (pool.info.cache_mode) {
  case pg_pool_t::CACHEMODE_NONE:
    return false;

  case pg_pool_t::CACHEMODE_WRITEBACK:
    if (obc.get() && obc->obs.exists) {
      return false;
    }
    if (agent_state &&
	agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
      if (!op->may_write() && !op->may_cache() && !write_ordered) {
	dout(20) << __func__ << " cache pool full, redirecting read" << dendl;
	do_cache_redirect(op, obc);
	return true;
      }
      dout(20) << __func__ << " cache pool full, waiting" << dendl;
      waiting_for_cache_not_full.push_back(op);
      return true;
    }
    if (!must_promote && can_skip_promote(op, obc)) {
      return false;
    }
    if (op->may_write() || write_ordered || must_promote || !hit_set) {
      promote_object(op, obc, missing_oid);
    } else {
      switch (pool.info.min_read_recency_for_promote) {
      case 0:
        promote_object(op, obc, missing_oid);
        break;
      case 1:
        // Check if in the current hit set
        if (in_hit_set) {
          promote_object(op, obc, missing_oid);
        } else {
          do_cache_redirect(op, obc);
        }
        break;
      default:
        if (in_hit_set) {
          promote_object(op, obc, missing_oid);
        } else {
          // Check if in other hit sets
          map<time_t,HitSetRef>::iterator itor;
          bool in_other_hit_sets = false;
          for (itor = agent_state->hit_set_map.begin(); itor != agent_state->hit_set_map.end(); itor++) {
            if (itor->second->contains(missing_oid)) {
              in_other_hit_sets = true;
              break;
            }
          }
          if (in_other_hit_sets) {
            promote_object(op, obc, missing_oid);
          } else {
            do_cache_redirect(op, obc);
          }
        }
        break;
      }
    }
    return true;

  case pg_pool_t::CACHEMODE_FORWARD:
    if (obc.get() && obc->obs.exists) {
      return false;
    }
    if (must_promote)
      promote_object(op, obc, missing_oid);
    else
      do_cache_redirect(op, obc);
    return true;

  case pg_pool_t::CACHEMODE_READONLY:
    // TODO: clean this case up
    if (obc.get() && obc->obs.exists) {
      return false;
    }
    if (!obc.get() && r == -ENOENT) {
      // we don't have the object and op's a read
      promote_object(op, obc, missing_oid);
      return true;
    }
    if (!r) { // it must be a write
      do_cache_redirect(op, obc);
      return true;
    }
    // crap, there was a failure of some kind
    return false;

  default:
    assert(0 == "unrecognized cache_mode");
  }
  return false;
}

bool ReplicatedPG::can_skip_promote(OpRequestRef op, ObjectContextRef obc)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  if (m->ops.empty())
    return false;
  // if we get a delete with FAILOK we can skip promote.  without
  // FAILOK we still need to promote (or do something smarter) to
  // determine whether to return ENOENT or 0.
  if (m->ops[0].op.op == CEPH_OSD_OP_DELETE &&
      (m->ops[0].op.flags & CEPH_OSD_OP_FLAG_FAILOK))
    return true;
  return false;
}

void ReplicatedPG::do_cache_redirect(OpRequestRef op, ObjectContextRef obc)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  int flags = m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);
  MOSDOpReply *reply = new MOSDOpReply(m, -ENOENT,
				       get_osdmap()->get_epoch(), flags, false);
  request_redirect_t redir(m->get_object_locator(), pool.info.tier_of);
  reply->set_redirect(redir);
  dout(10) << "sending redirect to pool " << pool.info.tier_of << " for op "
	   << op << dendl;
  m->get_connection()->get_messenger()->send_message(reply, m->get_connection());
  return;
}

class PromoteCallback: public ReplicatedPG::CopyCallback {
  OpRequestRef op;
  ObjectContextRef obc;
  ReplicatedPG *pg;
public:
  PromoteCallback(OpRequestRef op_, ObjectContextRef obc_,
		  ReplicatedPG *pg_)
    : op(op_),
      obc(obc_),
      pg(pg_) {}

  virtual void finish(ReplicatedPG::CopyCallbackResults results) {
    ReplicatedPG::CopyResults *results_data = results.get<1>();
    int r = results.get<0>();
    pg->finish_promote(r, op, results_data, obc);
  }
};

void ReplicatedPG::promote_object(OpRequestRef op, ObjectContextRef obc,
				  const hobject_t& missing_oid)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  if (!obc) { // we need to create an ObjectContext
    assert(missing_oid != hobject_t());
    obc = get_object_context(missing_oid, true);
  }
  dout(10) << __func__ << " " << obc->obs.oi.soid << dendl;
  if (scrubber.write_blocked_by_scrub(obc->obs.oi.soid)) {
    dout(10) << __func__ << " " << obc->obs.oi.soid
	     << " blocked by scrub" << dendl;
    if (op) {
      waiting_for_active.push_back(op);
      dout(10) << __func__ << " " << obc->obs.oi.soid
	       << " placing op in waiting_for_active" << dendl;
    } else {
      dout(10) << __func__ << " " << obc->obs.oi.soid
	       << " no op, dropping on the floor" << dendl;
    }
    return;
  }

  PromoteCallback *cb = new PromoteCallback(op, obc, this);
  object_locator_t oloc(m->get_object_locator());
  oloc.pool = pool.info.tier_of;
  start_copy(cb, obc, obc->obs.oi.soid, oloc, 0,
	     CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
	     CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
	     CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE,
	     obc->obs.oi.soid.snap == CEPH_NOSNAP);

  assert(obc->is_blocked());
  wait_for_blocked_object(obc->obs.oi.soid, op);
}

void ReplicatedPG::execute_ctx(OpContext *ctx)
{
  dout(10) << __func__ << " " << ctx << dendl;
  ctx->reset_obs(ctx->obc);
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  ObjectContextRef obc = ctx->obc;
  const hobject_t& soid = obc->obs.oi.soid;
  map<hobject_t,ObjectContextRef>& src_obc = ctx->src_obc;

  // this method must be idempotent since we may call it several times
  // before we finally apply the resulting transaction.
  delete ctx->op_t;
  ctx->op_t = pgbackend->get_transaction();

  if (op->may_write() || op->may_cache()) {
    op->mark_started();

    // snap
    if (!(m->get_flags() & CEPH_OSD_FLAG_ENFORCE_SNAPC) &&
	pool.info.is_pool_snaps_mode()) {
      // use pool's snapc
      ctx->snapc = pool.snapc;
    } else {
      // client specified snapc
      ctx->snapc.seq = m->get_snap_seq();
      ctx->snapc.snaps = m->get_snaps();
    }
    if ((m->get_flags() & CEPH_OSD_FLAG_ORDERSNAP) &&
	ctx->snapc.seq < obc->ssc->snapset.seq) {
      dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	       << " < snapset seq " << obc->ssc->snapset.seq
	       << " on " << obc->obs.oi.soid << dendl;
      reply_ctx(ctx, -EOLDSNAPC);
      return;
    }

    // version
    ctx->at_version = get_next_version();
    ctx->mtime = m->get_mtime();

    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version 
	     << " snapc " << ctx->snapc
	     << " snapset " << obc->ssc->snapset
	     << dendl;  
  } else {
    dout(10) << "do_op " << soid << " " << ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;  
  }

  if (!ctx->user_at_version)
    ctx->user_at_version = obc->obs.oi.user_version;
  dout(30) << __func__ << " user_at_version " << ctx->user_at_version << dendl;

  // note my stats
  utime_t now = ceph_clock_now(cct);

  if (op->may_read()) {
    dout(10) << " taking ondisk_read_lock" << dendl;
    obc->ondisk_read_lock();
  }
  for (map<hobject_t,ObjectContextRef>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " taking ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_lock();
  }

  int result = prepare_transaction(ctx);

  if (op->may_read()) {
    dout(10) << " dropping ondisk_read_lock" << dendl;
    obc->ondisk_read_unlock();
  }
  for (map<hobject_t,ObjectContextRef>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " dropping ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_unlock();
  }

  if (result == -EINPROGRESS) {
    // come back later.
    return;
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    close_op_ctx(ctx, result);
    return;
  }

  // check for full
  if (ctx->delta_stats.num_bytes > 0 &&
      pool.info.get_flags() & pg_pool_t::FLAG_FULL) {
    reply_ctx(ctx, -ENOSPC);
    return;
  }

  bool successful_write = !ctx->op_t->empty() && op->may_write() && result >= 0;
  // prepare the reply
  ctx->reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0,
			       successful_write);

  // Write operations aren't allowed to return a data payload because
  // we can't do so reliably. If the client has to resend the request
  // and it has already been applied, we will return 0 with no
  // payload.  Non-deterministic behavior is no good.  However, it is
  // possible to construct an operation that does a read, does a guard
  // check (e.g., CMPXATTR), and then a write.  Then we either succeed
  // with the write, or return a CMPXATTR and the read value.
  if (successful_write) {
    // write.  normalize the result code.
    dout(20) << " zeroing write result code " << result << dendl;
    result = 0;
  }
  ctx->reply->set_result(result);

  // read or error?
  if (ctx->op_t->empty() || result < 0) {
    if (ctx->pending_async_reads.empty()) {
      complete_read_ctx(result, ctx);
    } else {
      in_progress_async_reads.push_back(make_pair(op, ctx));
      ctx->start_async_reads(this);
    }
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  assert(op->may_write() || op->may_cache());

  // trim log?
  calc_trim_to();

  // verify that we are doing this in order?
  if (cct->_conf->osd_debug_op_order && m->get_source().is_client() &&
      !pool.info.is_tier() && !pool.info.has_tiers()) {
    map<client_t,ceph_tid_t>& cm = debug_op_order[obc->obs.oi.soid];
    ceph_tid_t t = m->get_tid();
    client_t n = m->get_source().num();
    map<client_t,ceph_tid_t>::iterator p = cm.find(n);
    if (p == cm.end()) {
      dout(20) << " op order client." << n << " tid " << t << " (first)" << dendl;
      cm[n] = t;
    } else {
      dout(20) << " op order client." << n << " tid " << t << " last was " << p->second << dendl;
      if (p->second > t) {
	derr << "bad op order, already applied " << p->second << " > this " << t << dendl;;
	assert(0 == "out of order op");
      }
      p->second = t;
    }
  }

  // issue replica writes
  ceph_tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(ctx, obc, rep_tid);  // new repop claims our obc, src_obc refs
  // note: repop now owns ctx AND ctx->op

  repop->src_obc.swap(src_obc); // and src_obc.

  issue_repop(repop, now);

  eval_repop(repop);
  repop->put();
}

void ReplicatedPG::reply_ctx(OpContext *ctx, int r)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r);
  close_op_ctx(ctx, r);
}

void ReplicatedPG::reply_ctx(OpContext *ctx, int r, eversion_t v, version_t uv)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r, v, uv);
  close_op_ctx(ctx, r);
}

void ReplicatedPG::log_op_stats(OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());

  utime_t now = ceph_clock_now(cct);
  utime_t latency = now;
  latency -= ctx->op->get_req()->get_recv_stamp();
  utime_t process_latency = now;
  process_latency -= ctx->op->get_dequeued_time();

  utime_t rlatency;
  if (ctx->readable_stamp != utime_t()) {
    rlatency = ctx->readable_stamp;
    rlatency -= ctx->op->get_req()->get_recv_stamp();
  }

  uint64_t inb = ctx->bytes_written;
  uint64_t outb = ctx->bytes_read;

  osd->logger->inc(l_osd_op);

  osd->logger->inc(l_osd_op_outb, outb);
  osd->logger->inc(l_osd_op_inb, inb);
  osd->logger->tinc(l_osd_op_lat, latency);
  osd->logger->tinc(l_osd_op_process_lat, process_latency);

  if (op->may_read() && op->may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->tinc(l_osd_op_rw_rlat, rlatency);
    osd->logger->tinc(l_osd_op_rw_lat, latency);
    osd->logger->tinc(l_osd_op_rw_process_lat, process_latency);
  } else if (op->may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->tinc(l_osd_op_r_lat, latency);
    osd->logger->tinc(l_osd_op_r_process_lat, process_latency);
  } else if (op->may_write() || op->may_cache()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->tinc(l_osd_op_w_rlat, rlatency);
    osd->logger->tinc(l_osd_op_w_lat, latency);
    osd->logger->tinc(l_osd_op_w_process_lat, process_latency);
  } else
    assert(0);

  dout(15) << "log_op_stats " << *m
	   << " inb " << inb
	   << " outb " << outb
	   << " rlat " << rlatency
	   << " lat " << latency << dendl;
}

void ReplicatedPG::do_sub_op(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(have_same_or_newer_map(m->map_epoch));
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(15) << "do_sub_op " << *op->get_req() << dendl;

  OSDOp *first = NULL;
  if (m->ops.size() >= 1) {
    first = &m->ops[0];
  }

  if (!is_active()) {
    waiting_for_active.push_back(op);
    op->mark_delayed("waiting for active");
    return;
  }

  if (first) {
    switch (first->op.op) {
    case CEPH_OSD_OP_DELETE:
      sub_op_remove(op);
      return;
    case CEPH_OSD_OP_SCRUB_RESERVE:
      sub_op_scrub_reserve(op);
      return;
    case CEPH_OSD_OP_SCRUB_UNRESERVE:
      sub_op_scrub_unreserve(op);
      return;
    case CEPH_OSD_OP_SCRUB_STOP:
      sub_op_scrub_stop(op);
      return;
    case CEPH_OSD_OP_SCRUB_MAP:
      sub_op_scrub_map(op);
      return;
    }
  }
}

void ReplicatedPG::do_sub_op_reply(OpRequestRef op)
{
  MOSDSubOpReply *r = static_cast<MOSDSubOpReply *>(op->get_req());
  assert(r->get_header().type == MSG_OSD_SUBOPREPLY);
  if (r->ops.size() >= 1) {
    OSDOp& first = r->ops[0];
    switch (first.op.op) {
    case CEPH_OSD_OP_SCRUB_RESERVE:
      sub_op_scrub_reserve_reply(op);
      return;
    }
  }
}

void ReplicatedPG::do_scan(
  OpRequestRef op,
  ThreadPool::TPHandle &handle)
{
  MOSDPGScan *m = static_cast<MOSDPGScan*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_SCAN);
  dout(10) << "do_scan " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGScan::OP_SCAN_GET_DIGEST:
    {
      double ratio, full_ratio;
      if (osd->too_full_for_backfill(&ratio, &full_ratio)) {
	dout(1) << __func__ << ": Canceling backfill, current usage is "
		<< ratio << ", which exceeds " << full_ratio << dendl;
	queue_peering_event(
	  CephPeeringEvtRef(
	    new CephPeeringEvt(
	      get_osdmap()->get_epoch(),
	      get_osdmap()->get_epoch(),
	      BackfillTooFull())));
	return;
      }

      BackfillInterval bi;
      bi.begin = m->begin;
      // No need to flush, there won't be any in progress writes occuring
      // past m->begin
      scan_range(
	cct->_conf->osd_backfill_scan_min,
	cct->_conf->osd_backfill_scan_max,
	&bi,
	handle);
      MOSDPGScan *reply = new MOSDPGScan(
	MOSDPGScan::OP_SCAN_DIGEST,
	pg_whoami,
	get_osdmap()->get_epoch(), m->query_epoch,
	spg_t(info.pgid.pgid, get_primary().shard), bi.begin, bi.end);
      ::encode(bi.objects, reply->get_data());
      osd->send_message_osd_cluster(reply, m->get_connection());
    }
    break;

  case MOSDPGScan::OP_SCAN_DIGEST:
    {
      pg_shard_t from = m->from;

      // Check that from is in backfill_targets vector
      assert(is_backfill_targets(from));

      BackfillInterval bi;
      bi.begin = m->begin;
      bi.end = m->end;
      bufferlist::iterator p = m->get_data().begin();
      ::decode(bi.objects, p);

      // handle hobject_t encoding change
      if (bi.objects.size() && bi.objects.begin()->first.pool == -1) {
	map<hobject_t, eversion_t> tmp;
	tmp.swap(bi.objects);
	for (map<hobject_t, eversion_t>::iterator i = tmp.begin();
	     i != tmp.end();
	     ++i) {
	  hobject_t first(i->first);
	  if (!first.is_max() && first.pool == -1)
	    first.pool = info.pgid.pool();
	  bi.objects[first] = i->second;
	}
      }
      peer_backfill_info[from] = bi;

      if (waiting_on_backfill.find(from) != waiting_on_backfill.end()) {
	waiting_on_backfill.erase(from);

	if (waiting_on_backfill.empty()) {
	  assert(peer_backfill_info.size() == backfill_targets.size());
	  finish_recovery_op(hobject_t::get_max());
	}
      } else {
	// we canceled backfill for a while due to a too full, and this
	// is an extra response from a non-too-full peer
      }
    }
    break;
  }
}

void ReplicatedBackend::_do_push(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  vector<PushReplyOp> replies;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());
    handle_push(from, *i, &(replies.back()), t);
  }

  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->from = get_parent()->whoami_shard();
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->replies.swap(replies);
  reply->compute_cost(cct);

  t->register_on_complete(
    new PG_SendMessageOnConn(
      get_parent(), reply, m->get_connection()));

  t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(t));
  get_parent()->queue_transaction(t);
}

struct C_ReplicatedBackend_OnPullComplete : GenContext<ThreadPool::TPHandle&> {
  ReplicatedBackend *bc;
  list<hobject_t> to_continue;
  int priority;
  C_ReplicatedBackend_OnPullComplete(ReplicatedBackend *bc, int priority)
    : bc(bc), priority(priority) {}

  void finish(ThreadPool::TPHandle &handle) {
    ReplicatedBackend::RPGHandle *h = bc->_open_recovery_op();
    for (list<hobject_t>::iterator i =
	   to_continue.begin();
	 i != to_continue.end();
	 ++i) {
      map<hobject_t, ReplicatedBackend::PullInfo>::iterator j =
	bc->pulling.find(*i);
      assert(j != bc->pulling.end());
      if (!bc->start_pushes(*i, j->second.obc, h)) {
	bc->get_parent()->on_global_recover(
	  *i);
      }
      bc->pulling.erase(*i);
      handle.reset_tp_timeout();
    }
    bc->run_recovery_op(h, priority);
  }
};

void ReplicatedBackend::_do_pull_response(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_PUSH);
  pg_shard_t from = m->from;

  vector<PullOp> replies(1);
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  list<hobject_t> to_continue;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    bool more = handle_pull_response(from, *i, &(replies.back()), &to_continue, t);
    if (more)
      replies.push_back(PullOp());
  }
  if (!to_continue.empty()) {
    C_ReplicatedBackend_OnPullComplete *c =
      new C_ReplicatedBackend_OnPullComplete(
	this,
	m->get_priority());
    c->to_continue.swap(to_continue);
    t->register_on_complete(
      new PG_QueueAsync(
	get_parent(),
	get_parent()->bless_gencontext(c)));
  }
  replies.erase(replies.end() - 1);

  if (replies.size()) {
    MOSDPGPull *reply = new MOSDPGPull;
    reply->from = parent->whoami_shard();
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->pulls.swap(replies);
    reply->compute_cost(cct);

    t->register_on_complete(
      new PG_SendMessageOnConn(
	get_parent(), reply, m->get_connection()));
  }

  t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(t));
  get_parent()->queue_transaction(t);
}

void ReplicatedBackend::do_pull(OpRequestRef op)
{
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_PULL);
  pg_shard_t from = m->from;

  map<pg_shard_t, vector<PushOp> > replies;
  for (vector<PullOp>::iterator i = m->pulls.begin();
       i != m->pulls.end();
       ++i) {
    replies[from].push_back(PushOp());
    handle_pull(from, *i, &(replies[from].back()));
  }
  send_pushes(m->get_priority(), replies);
}

void ReplicatedBackend::do_push_reply(OpRequestRef op)
{
  MOSDPGPushReply *m = static_cast<MOSDPGPushReply *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_PUSH_REPLY);
  pg_shard_t from = m->from;

  vector<PushOp> replies(1);
  for (vector<PushReplyOp>::iterator i = m->replies.begin();
       i != m->replies.end();
       ++i) {
    bool more = handle_push_reply(from, *i, &(replies.back()));
    if (more)
      replies.push_back(PushOp());
  }
  replies.erase(replies.end() - 1);

  map<pg_shard_t, vector<PushOp> > _replies;
  _replies[from].swap(replies);
  send_pushes(m->get_priority(), _replies);
}

void ReplicatedPG::do_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = static_cast<MOSDPGBackfill*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);
  dout(10) << "do_backfill " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGBackfill::OP_BACKFILL_FINISH:
    {
      assert(cct->_conf->osd_kill_backfill_at != 1);

      MOSDPGBackfill *reply = new MOSDPGBackfill(
	MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
	get_osdmap()->get_epoch(),
	m->query_epoch,
	spg_t(info.pgid.pgid, primary.shard));
      reply->set_priority(cct->_conf->osd_recovery_op_priority);
      osd->send_message_osd_cluster(reply, m->get_connection());
      queue_peering_event(
	CephPeeringEvtRef(
	  new CephPeeringEvt(
	    get_osdmap()->get_epoch(),
	    get_osdmap()->get_epoch(),
	    RecoveryDone())));
    }
    // fall-thru

  case MOSDPGBackfill::OP_BACKFILL_PROGRESS:
    {
      assert(cct->_conf->osd_kill_backfill_at != 2);

      info.last_backfill = m->last_backfill;
      if (m->compat_stat_sum) {
	info.stats.stats = m->stats.stats; // Previously, we only sent sum
      } else {
	info.stats = m->stats;
      }

      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      dirty_info = true;
      write_if_dirty(*t);
      int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
      assert(tr == 0);
    }
    break;

  case MOSDPGBackfill::OP_BACKFILL_FINISH_ACK:
    {
      assert(is_primary());
      assert(cct->_conf->osd_kill_backfill_at != 3);
      finish_recovery_op(hobject_t::get_max());
    }
    break;
  }
}

ReplicatedPG::RepGather *ReplicatedPG::trim_object(const hobject_t &coid)
{
  // load clone info
  bufferlist bl;
  ObjectContextRef obc = get_object_context(coid, false, NULL);
  if (!obc) {
    derr << __func__ << "could not find coid " << coid << dendl;
    assert(0);
  }
  assert(obc->ssc);

  if (!obc->get_snaptrimmer_write()) {
    dout(10) << __func__ << ": Unable to get a wlock on " << coid << dendl;
    return NULL;
  }

  hobject_t snapoid(
    coid.oid, coid.get_key(),
    obc->ssc->snapset.head_exists ? CEPH_NOSNAP:CEPH_SNAPDIR, coid.hash,
    info.pgid.pool(), coid.get_namespace());
  ObjectContextRef snapset_obc = get_object_context(snapoid, false);

  if (!snapset_obc->get_snaptrimmer_write()) {
    dout(10) << __func__ << ": Unable to get a wlock on " << snapoid << dendl;
    list<OpRequestRef> to_wake;
    bool requeue_recovery = false;
    bool requeue_snaptrimmer = false;
    obc->put_write(&to_wake, &requeue_recovery, &requeue_snaptrimmer);
    assert(to_wake.empty());
    assert(!requeue_recovery);
    return NULL;
  }

  object_info_t &coi = obc->obs.oi;
  set<snapid_t> old_snaps(coi.snaps.begin(), coi.snaps.end());
  assert(old_snaps.size());

  SnapSet& snapset = obc->ssc->snapset;

  dout(10) << coid << " old_snaps " << old_snaps
	   << " old snapset " << snapset << dendl;
  assert(snapset.seq);

  RepGather *repop = simple_repop_create(obc);
  OpContext *ctx = repop->ctx;
  ctx->snapset_obc = snapset_obc;
  ctx->lock_to_release = OpContext::W_LOCK;
  ctx->release_snapset_obc = true;
  ctx->at_version = get_next_version();

  PGBackend::PGTransaction *t = ctx->op_t;
  set<snapid_t> new_snaps;
  for (set<snapid_t>::iterator i = old_snaps.begin();
       i != old_snaps.end();
       ++i) {
    if (!pool.info.is_removed_snap(*i))
      new_snaps.insert(*i);
  }

  if (new_snaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << old_snaps << " -> "
	     << new_snaps << " ... deleting" << dendl;

    // ...from snapset
    snapid_t last = coid.snap;
    vector<snapid_t>::iterator p;
    for (p = snapset.clones.begin(); p != snapset.clones.end(); ++p)
      if (*p == last)
	break;
    assert(p != snapset.clones.end());
    object_stat_sum_t delta;
    delta.num_bytes -= snapset.get_clone_bytes(last);

    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      hobject_t prev_coid = coid;
      prev_coid.snap = *n;
      bool adjust_prev_bytes = is_present_clone(prev_coid);

      if (adjust_prev_bytes)
	delta.num_bytes -= snapset.get_clone_bytes(*n);

      snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);

      if (adjust_prev_bytes)
	delta.num_bytes += snapset.get_clone_bytes(*n);
    }
    delta.num_objects--;
    if (coi.is_dirty())
      delta.num_objects_dirty--;
    if (coi.is_omap())
      delta.num_objects_omap--;
    if (coi.is_whiteout()) {
      dout(20) << __func__ << " trimming whiteout on " << coid << dendl;
      delta.num_whiteouts--;
    }
    delta.num_object_clones--;
    info.stats.stats.add(delta, obc->obs.oi.category);
    obc->obs.exists = false;

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);
	
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::DELETE,
	coid,
	ctx->at_version,
	ctx->obs->oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime)
      );
    if (pool.info.require_rollback()) {
      set<snapid_t> snaps(
	ctx->obc->obs.oi.snaps.begin(),
	ctx->obc->obs.oi.snaps.end());
      ctx->log.back().mod_desc.update_snaps(snaps);
      if (ctx->log.back().mod_desc.rmobject(ctx->at_version.version)) {
	t->stash(coid, ctx->at_version.version);
      } else {
	t->remove(coid);
      }
    } else {
      t->remove(coid);
      ctx->log.back().mod_desc.mark_unrollbackable();
    }
    ctx->at_version.version++;
  } else {
    // save adjusted snaps for this object
    dout(10) << coid << " snaps " << old_snaps
	     << " -> " << new_snaps << dendl;
    coi.snaps = vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());

    coi.prior_version = coi.version;
    coi.version = ctx->at_version;
    bl.clear();
    ::encode(coi, bl);
    setattr_maybe_cache(ctx->obc, ctx, t, OI_ATTR, bl);

    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	coid,
	coi.version,
	coi.prior_version,
	0,
	osd_reqid_t(),
	ctx->mtime)
      );
    if (pool.info.require_rollback()) {
      set<string> changing;
      changing.insert(OI_ATTR);
      ctx->obc->fill_in_setattrs(changing, &(ctx->log.back().mod_desc));
      set<snapid_t> snaps(
	ctx->obc->obs.oi.snaps.begin(),
	ctx->obc->obs.oi.snaps.end());
      ctx->log.back().mod_desc.update_snaps(old_snaps);
    } else {
      ctx->log.back().mod_desc.mark_unrollbackable();
    }
    
    ::encode(coi.snaps, ctx->log.back().snaps);
    ctx->at_version.version++;
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << dendl;

  if (snapset.clones.empty() && !snapset.head_exists) {
    dout(10) << coid << " removing " << snapoid << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::DELETE,
	snapoid,
	ctx->at_version,
	ctx->snapset_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime)
      );

    ctx->snapset_obc->obs.exists = false;
    
    if (pool.info.require_rollback()) {
      if (ctx->log.back().mod_desc.rmobject(ctx->at_version.version)) {
	t->stash(snapoid, ctx->at_version.version);
      } else {
	t->remove(snapoid);
      }
    } else {
      t->remove(snapoid);
      ctx->log.back().mod_desc.mark_unrollbackable();
    }
  } else {
    dout(10) << coid << " updating snapset on " << snapoid << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	snapoid,
	ctx->at_version,
	ctx->snapset_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime)
      );

    ctx->snapset_obc->obs.oi.prior_version =
      ctx->snapset_obc->obs.oi.version;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;

    bl.clear();
    ::encode(snapset, bl);
    setattr_maybe_cache(ctx->snapset_obc, ctx, t, SS_ATTR, bl);

    bl.clear();
    ::encode(ctx->snapset_obc->obs.oi, bl);
    setattr_maybe_cache(ctx->snapset_obc, ctx, t, OI_ATTR, bl);

    if (pool.info.require_rollback()) {
      set<string> changing;
      changing.insert(OI_ATTR);
      changing.insert(SS_ATTR);
      ctx->snapset_obc->fill_in_setattrs(changing, &(ctx->log.back().mod_desc));
    } else {
      ctx->log.back().mod_desc.mark_unrollbackable();
    }
  }

  return repop;
}

void ReplicatedPG::snap_trimmer()
{
  if (g_conf->osd_snap_trim_sleep > 0) {
    utime_t t;
    t.set_from_double(g_conf->osd_snap_trim_sleep);
    t.sleep();
    lock();
    dout(20) << __func__ << " slept for " << t << dendl;
  } else {
    lock();
  }
  if (deleting) {
    unlock();
    return;
  }
  dout(10) << "snap_trimmer entry" << dendl;
  if (is_primary()) {
    entity_inst_t nobody;
    if (scrubber.active) {
      dout(10) << " scrubbing, will requeue snap_trimmer after" << dendl;
      scrubber.queue_snap_trim = true;
      unlock();
      return;
    }

    dout(10) << "snap_trimmer posting" << dendl;
    snap_trimmer_machine.process_event(SnapTrim());

    if (snap_trimmer_machine.need_share_pg_info) {
      dout(10) << "snap_trimmer share_pg_info" << dendl;
      snap_trimmer_machine.need_share_pg_info = false;
      share_pg_info();
    }
  } else if (is_active() && 
	     last_complete_ondisk.epoch > info.history.last_epoch_started) {
    // replica collection trimming
    snap_trimmer_machine.process_event(SnapTrim());
  }
  unlock();
  return;
}

int ReplicatedPG::do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr)
{
  __u64 v2;
  if (xattr.length())
    v2 = atoll(xattr.c_str());
  else
    v2 = 0;

  dout(20) << "do_xattr_cmp_u64 '" << v1 << "' vs '" << v2 << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1 == v2);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1 != v2);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1 > v2);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1 >= v2);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1 < v2);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1 <= v2);
  default:
    return -EINVAL;
  }
}

int ReplicatedPG::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
{
  string v2s(xattr.c_str(), xattr.length());

  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2s << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1s.compare(v2s) == 0);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1s.compare(v2s) != 0);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1s.compare(v2s) > 0);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1s.compare(v2s) >= 0);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1s.compare(v2s) < 0);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1s.compare(v2s) <= 0);
  default:
    return -EINVAL;
  }
}

// ========================================================================
// low level osd ops

int ReplicatedPG::do_tmap2omap(OpContext *ctx, unsigned flags)
{
  dout(20) << " convert tmap to omap for " << ctx->new_obs.oi.soid << dendl;
  bufferlist header, vals;
  int r = _get_tmap(ctx, &header, &vals);
  if (r < 0) {
    if (r == -ENODATA && (flags & CEPH_OSD_TMAP2OMAP_NULLOK))
      r = 0;
    return r;
  }

  vector<OSDOp> ops(3);

  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = 0;

  ops[1].op.op = CEPH_OSD_OP_OMAPSETHEADER;
  ops[1].indata.claim(header);

  ops[2].op.op = CEPH_OSD_OP_OMAPSETVALS;
  ops[2].indata.claim(vals);

  return do_osd_ops(ctx, ops);
}

int ReplicatedPG::do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op,
				    bufferlist& bl)
{
  // decode
  bufferlist header;
  map<string, bufferlist> m;
  if (bl.length()) {
    bufferlist::iterator p = bl.begin();
    ::decode(header, p);
    ::decode(m, p);
    assert(p.end());
  }

  // do the update(s)
  while (!bp.end()) {
    __u8 op;
    string key;
    ::decode(op, bp);

    switch (op) {
    case CEPH_OSD_TMAP_SET: // insert key
      {
	::decode(key, bp);
	bufferlist data;
	::decode(data, bp);
	m[key] = data;
      }
      break;
    case CEPH_OSD_TMAP_RM: // remove key
      ::decode(key, bp);
      if (!m.count(key)) {
	return -ENOENT;
      }
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_RMSLOPPY: // remove key
      ::decode(key, bp);
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_HDR: // update header
      {
	::decode(header, bp);
      }
      break;
    default:
      return -EINVAL;
    }
  }

  // reencode
  bufferlist obl;
  ::encode(header, obl);
  ::encode(m, obl);

  // write it out
  vector<OSDOp> nops(1);
  OSDOp& newop = nops[0];
  newop.op.op = CEPH_OSD_OP_WRITEFULL;
  newop.op.extent.offset = 0;
  newop.op.extent.length = obl.length();
  newop.indata = obl;
  do_osd_ops(ctx, nops);
  osd_op.outdata.claim(newop.outdata);
  return 0;
}

int ReplicatedPG::do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op)
{
  bufferlist::iterator orig_bp = bp;
  int result = 0;
  if (bp.end()) {
    dout(10) << "tmapup is a no-op" << dendl;
  } else {
    // read the whole object
    vector<OSDOp> nops(1);
    OSDOp& newop = nops[0];
    newop.op.op = CEPH_OSD_OP_READ;
    newop.op.extent.offset = 0;
    newop.op.extent.length = 0;
    do_osd_ops(ctx, nops);

    dout(10) << "tmapup read " << newop.outdata.length() << dendl;

    dout(30) << " starting is \n";
    newop.outdata.hexdump(*_dout);
    *_dout << dendl;

    bufferlist::iterator ip = newop.outdata.begin();
    bufferlist obl;

    dout(30) << "the update command is: \n";
    osd_op.indata.hexdump(*_dout);
    *_dout << dendl;

    // header
    bufferlist header;
    __u32 nkeys = 0;
    if (newop.outdata.length()) {
      ::decode(header, ip);
      ::decode(nkeys, ip);
    }
    dout(10) << "tmapup header " << header.length() << dendl;

    if (!bp.end() && *bp == CEPH_OSD_TMAP_HDR) {
      ++bp;
      ::decode(header, bp);
      dout(10) << "tmapup new header " << header.length() << dendl;
    }

    ::encode(header, obl);

    dout(20) << "tmapup initial nkeys " << nkeys << dendl;

    // update keys
    bufferlist newkeydata;
    string nextkey, last_in_key;
    bufferlist nextval;
    bool have_next = false;
    string last_disk_key;
    if (!ip.end()) {
      have_next = true;
      ::decode(nextkey, ip);
      ::decode(nextval, ip);
      if (nextkey < last_disk_key) {
	dout(5) << "tmapup warning: key '" << nextkey << "' < previous key '" << last_disk_key
		<< "', falling back to an inefficient (unsorted) update" << dendl;
	bp = orig_bp;
	return do_tmapup_slow(ctx, bp, osd_op, newop.outdata);
      }
      last_disk_key = nextkey;
    }
    result = 0;
    while (!bp.end() && !result) {
      __u8 op;
      string key;
      try {
	::decode(op, bp);
	::decode(key, bp);
      }
      catch (buffer::error& e) {
	return -EINVAL;
      }
      if (key < last_in_key) {
	dout(5) << "tmapup warning: key '" << key << "' < previous key '" << last_in_key
		<< "', falling back to an inefficient (unsorted) update" << dendl;
	bp = orig_bp;
	return do_tmapup_slow(ctx, bp, osd_op, newop.outdata);
      }
      last_in_key = key;

      dout(10) << "tmapup op " << (int)op << " key " << key << dendl;
	  
      // skip existing intervening keys
      bool key_exists = false;
      while (have_next && !key_exists) {
	dout(20) << "  (have_next=" << have_next << " nextkey=" << nextkey << ")" << dendl;
	if (nextkey > key)
	  break;
	if (nextkey < key) {
	  // copy untouched.
	  ::encode(nextkey, newkeydata);
	  ::encode(nextval, newkeydata);
	  dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	} else {
	  // don't copy; discard old value.  and stop.
	  dout(20) << "  drop " << nextkey << " " << nextval.length() << dendl;
	  key_exists = true;
	  nkeys--;
	}
	if (!ip.end()) {
	  ::decode(nextkey, ip);
	  ::decode(nextval, ip);
	} else {
	  have_next = false;
	}
      }

      if (op == CEPH_OSD_TMAP_SET) {
	bufferlist val;
	try {
	  ::decode(val, bp);
	}
	catch (buffer::error& e) {
	  return -EINVAL;
	}
	::encode(key, newkeydata);
	::encode(val, newkeydata);
	dout(20) << "   set " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_CREATE) {
	if (key_exists) {
	  return -EEXIST;
	}
	bufferlist val;
	try {
	  ::decode(val, bp);
	}
	catch (buffer::error& e) {
	  return -EINVAL;
	}
	::encode(key, newkeydata);
	::encode(val, newkeydata);
	dout(20) << "   create " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_RM) {
	// do nothing.
	if (!key_exists) {
	  return -ENOENT;
	}
      } else if (op == CEPH_OSD_TMAP_RMSLOPPY) {
	// do nothing
      } else {
	dout(10) << "  invalid tmap op " << (int)op << dendl;
	return -EINVAL;
      }
    }

    // copy remaining
    if (have_next) {
      ::encode(nextkey, newkeydata);
      ::encode(nextval, newkeydata);
      dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
    }
    if (!ip.end()) {
      bufferlist rest;
      rest.substr_of(newop.outdata, ip.get_off(), newop.outdata.length() - ip.get_off());
      dout(20) << "  keep trailing " << rest.length()
	       << " at " << newkeydata.length() << dendl;
      newkeydata.claim_append(rest);
    }

    // encode final key count + key data
    dout(20) << "tmapup final nkeys " << nkeys << dendl;
    ::encode(nkeys, obl);
    obl.claim_append(newkeydata);

    if (0) {
      dout(30) << " final is \n";
      obl.hexdump(*_dout);
      *_dout << dendl;

      // sanity check
      bufferlist::iterator tp = obl.begin();
      bufferlist h;
      ::decode(h, tp);
      map<string,bufferlist> d;
      ::decode(d, tp);
      assert(tp.end());
      dout(0) << " **** debug sanity check, looks ok ****" << dendl;
    }

    // write it out
    if (!result) {
      dout(20) << "tmapput write " << obl.length() << dendl;
      newop.op.op = CEPH_OSD_OP_WRITEFULL;
      newop.op.extent.offset = 0;
      newop.op.extent.length = obl.length();
      newop.indata = obl;
      do_osd_ops(ctx, nops);
      osd_op.outdata.claim(newop.outdata);
    }
  }
  return result;
}

static int check_offset_and_length(uint64_t offset, uint64_t length, uint64_t max)
{
  if (offset >= max ||
      length > max ||
      offset + length > max)
    return -EFBIG;

  return 0;
}

struct FillInExtent : public Context {
  ceph_le64 *r;
  FillInExtent(ceph_le64 *r) : r(r) {}
  void finish(int _r) {
    if (_r >= 0) {
      *r = _r;
    }
  }
};

int ReplicatedPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
{
  int result = 0;
  SnapSetContext *ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;

  bool first_read = true;

  PGBackend::PGTransaction* t = ctx->op_t;

  dout(10) << "do_osd_op " << soid << " " << ops << dendl;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p, ctx->current_osd_subop_num++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;
 
    dout(10) << "do_osd_op  " << osd_op << dendl;

    bufferlist::iterator bp = osd_op.indata.begin();

    // user-visible modifcation?
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
    case CEPH_OSD_OP_CACHE_EVICT:
    case CEPH_OSD_OP_CACHE_FLUSH:
    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
    case CEPH_OSD_OP_UNDIRTY:
    case CEPH_OSD_OP_COPY_FROM:  // we handle user_version update explicitly
      break;
    default:
      if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;
    }

    ObjectContextRef src_obc;
    if (ceph_osd_op_type_multi(op.op)) {
      MOSDOp *m = static_cast<MOSDOp *>(ctx->op->get_req());
      object_locator_t src_oloc;
      get_src_oloc(soid.oid, m->get_object_locator(), src_oloc);
      hobject_t src_oid(osd_op.soid, src_oloc.key, soid.hash,
			info.pgid.pool(), src_oloc.nspace);
      src_obc = ctx->src_obc[src_oid];
      dout(10) << " src_oid " << src_oid << " obc " << src_obc << dendl;
      assert(src_obc);
    }

    // munge -1 truncate to 0 truncate
    if (op.extent.truncate_seq == 1 && op.extent.truncate_size == (-1ULL)) {
      op.extent.truncate_size = 0;
      op.extent.truncate_seq = 0;
    }

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
	obs.exists &&
	op.extent.offset < cct->_conf->osd_max_object_size &&
	op.extent.length >= 1 &&
	op.extent.length <= cct->_conf->osd_max_object_size &&
	op.extent.offset + op.extent.length >= oi.size) {
      if (op.extent.offset >= oi.size) {
        // no-op
	goto fail;
      }
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      
      // --- READS ---

    case CEPH_OSD_OP_SYNC_READ:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      // fall through
    case CEPH_OSD_OP_READ:
      ++ctx->num_read;
      {
	__u32 seq = oi.truncate_seq;
	uint64_t size = oi.size;
	bool trimmed_read = false;
	// are we beyond truncate_size?
	if ( (seq < op.extent.truncate_seq) &&
	     (op.extent.offset + op.extent.length > op.extent.truncate_size) )
	  size = op.extent.truncate_size;

	if (op.extent.offset >= size) {
	  op.extent.length = 0;
	  trimmed_read = true;
	} else if (op.extent.offset + op.extent.length > size) {
	  op.extent.length = size - op.extent.offset;
	  trimmed_read = true;
	}

	// read into a buffer
	bufferlist bl;
	if (trimmed_read && op.extent.length == 0) {
	  // read size was trimmed to zero and it is expected to do nothing
	  // a read operation of 0 bytes does *not* do nothing, this is why
	  // the trimmed_read boolean is needed
	} else if (pool.info.require_rollback()) {
	  ctx->pending_async_reads.push_back(
	    make_pair(
	      make_pair(op.extent.offset, op.extent.length),
	      make_pair(&osd_op.outdata, new FillInExtent(&op.extent.length))));
	  dout(10) << " async_read noted for " << soid << dendl;
	} else {
	  int r = pgbackend->objects_read_sync(
	    soid, op.extent.offset, op.extent.length, &osd_op.outdata);
	  if (r >= 0)
	    op.extent.length = r;
	  else {
	    result = r;
	    op.extent.length = 0;
	  }
	  dout(10) << " read got " << r << " / " << op.extent.length
		   << " bytes from obj " << soid << dendl;
	}
	if (first_read) {
	  first_read = false;
	  ctx->data_off = op.extent.offset;
	}
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;

      }
      break;

    /* map extents */
    case CEPH_OSD_OP_MAPEXT:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->fiemap(coll, soid, op.extent.offset, op.extent.length, bl);
	osd_op.outdata.claim(bl);
	if (r < 0)
	  result = r;
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " map_extents done on object " << soid << dendl;
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      {
        if (op.extent.truncate_seq) {
          dout(0) << "sparse_read does not support truncation sequence " << dendl;
          result = -EINVAL;
          break;
        }
	// read into a buffer
	bufferlist bl;
        int total_read = 0;
	int r = osd->store->fiemap(coll, soid, op.extent.offset, op.extent.length, bl);
	if (r < 0)  {
	  result = r;
          break;
	}
        map<uint64_t, uint64_t> m;
        bufferlist::iterator iter = bl.begin();
        ::decode(m, iter);
        map<uint64_t, uint64_t>::iterator miter;
        bufferlist data_bl;
	uint64_t last = op.extent.offset;
        for (miter = m.begin(); miter != m.end(); ++miter) {
	  // verify hole?
	  if (cct->_conf->osd_verify_sparse_read_holes &&
	      last < miter->first) {
	    bufferlist t;
	    uint64_t len = miter->first - last;
	    r = pgbackend->objects_read_sync(
	      soid, last, len, &t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid << " sparse-read found data in hole "
				<< last << "~" << len << "\n";
	    }
	  }

          bufferlist tmpbl;
	  r = pgbackend->objects_read_sync(
	    soid, miter->first, miter->second, &tmpbl);
          if (r < 0)
            break;

          if (r < (int)miter->second) /* this is usually happen when we get extent that exceeds the actual file size */
            miter->second = r;
          total_read += r;
          dout(10) << "sparse-read " << miter->first << "@" << miter->second << dendl;
	  data_bl.claim_append(tmpbl);
	  last = miter->first + r;
        }

	// verify trailing hole?
	if (cct->_conf->osd_verify_sparse_read_holes) {
	  uint64_t end = MIN(op.extent.offset + op.extent.length, oi.size);
	  if (last < end) {
	    bufferlist t;
	    uint64_t len = end - last;
	    r = pgbackend->objects_read_sync(
	      soid, last, len, &t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid << " sparse-read found data in hole "
				<< last << "~" << len << "\n";
	    }
	  }
	}

        if (r < 0) {
          result = r;
          break;
        }

        op.extent.length = total_read;

        ::encode(m, osd_op.outdata);
        ::encode(data_bl, osd_op.outdata);

	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;

	dout(10) << " sparse_read got " << total_read << " bytes from object " << soid << dendl;
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	string cname, mname;
	bufferlist indata;
	try {
	  bp.copy(op.cls.class_len, cname);
	  bp.copy(op.cls.method_len, mname);
	  bp.copy(op.cls.indata_len, indata);
	} catch (buffer::error& e) {
	  dout(10) << "call unable to decode class + method + indata" << dendl;
	  dout(30) << "in dump: ";
	  osd_op.indata.hexdump(*_dout);
	  *_dout << dendl;
	  result = -EINVAL;
	  break;
	}

	ClassHandler::ClassData *cls;
	result = osd->class_handler->open_class(cname, &cls);
	assert(result == 0);   // init_op_flags() already verified this works.

	ClassHandler::ClassMethod *method = cls->get_method(mname.c_str());
	if (!method) {
	  dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	  result = -EOPNOTSUPP;
	  break;
	}

	int flags = method->get_flags();
	if (flags & CLS_METHOD_WR)
	  ctx->user_modify = true;

	bufferlist outdata;
	dout(10) << "call method " << cname << "." << mname << dendl;
	int prev_rd = ctx->num_read;
	int prev_wr = ctx->num_write;
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);

	if (ctx->num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
	  derr << "method " << cname << "." << mname << " tried to read object but is not marked RD" << dendl;
	  result = -EIO;
	  break;
	}
	if (ctx->num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
	  derr << "method " << cname << "." << mname << " tried to update object but is not marked WR" << dendl;
	  result = -EIO;
	  break;
	}

	dout(10) << "method called response length=" << outdata.length() << dendl;
	op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
	dout(30) << "out dump: ";
	osd_op.outdata.hexdump(*_dout);
	*_dout << dendl;
      }
      break;

    case CEPH_OSD_OP_STAT:
      // note: stat does not require RD
      {
	if (obs.exists && !oi.is_whiteout()) {
	  ::encode(oi.size, osd_op.outdata);
	  ::encode(oi.mtime, osd_op.outdata);
	  dout(10) << "stat oi has " << oi.size << " " << oi.mtime << dendl;
	} else {
	  result = -ENOENT;
	  dout(10) << "stat oi object does not exist" << dendl;
	}

	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_ISDIRTY:
      ++ctx->num_read;
      {
	bool is_dirty = obs.oi.is_dirty();
	::encode(is_dirty, osd_op.outdata);
	ctx->delta_stats.num_rd++;
	result = 0;
      }
      break;

    case CEPH_OSD_OP_UNDIRTY:
      ++ctx->num_write;
      {
	if (oi.is_dirty()) {
	  ctx->undirty = true;  // see make_writeable()
	  ctx->modify = true;
	  ctx->delta_stats.num_wr++;
	}
	result = 0;
      }
      break;

    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
      ++ctx->num_write;
      {
	if (ctx->lock_to_release != OpContext::NONE) {
	  dout(10) << "cache-try-flush without SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, false, NULL, NULL);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_FLUSH:
      ++ctx->num_write;
      {
	if (ctx->lock_to_release == OpContext::NONE) {
	  dout(10) << "cache-flush with SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	hobject_t missing;
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, true, &missing, NULL);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
	// Check special return value which has set missing_return
        if (result == -ENOENT) {
          dout(10) << __func__ << " CEPH_OSD_OP_CACHE_FLUSH got ENOENT" << dendl;
	  assert(!missing.is_min());
	  wait_for_unreadable_object(missing, ctx->op);
	  // Error code which is used elsewhere when wait_for_unreadable_object() is used
	  result = -EAGAIN;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_EVICT:
      ++ctx->num_write;
      {
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE) {
	  result = -EINVAL;
	  break;
	}
	if (oi.is_dirty()) {
	  result = -EBUSY;
	  break;
	}
	if (!oi.watchers.empty()) {
	  result = -EBUSY;
	  break;
	}
	if (soid.snap == CEPH_NOSNAP) {
	  result = _verify_no_head_clones(soid, ssc->snapset);
	  if (result < 0)
	    break;
	}
	result = _delete_oid(ctx, true);
	if (result >= 0) {
	  // mark that this is a cache eviction to avoid triggering normal
	  // make_writeable() clone or snapdir object creation in finish_ctx()
	  ctx->cache_evict = true;
	}
	osd->logger->inc(l_osd_tier_evict);
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &(osd_op.outdata));
	if (r >= 0) {
	  op.xattr.value_len = r;
	  result = 0;
	  ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(r, 10);
	  ctx->delta_stats.num_rd++;
	} else
	  result = r;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
      ++ctx->num_read;
      {
	map<string, bufferlist> out;
	result = getattrs_maybe_cache(
	  ctx->obc,
	  &out,
	  true);
        
        bufferlist bl;
        ::encode(out, bl);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(bl.length(), 10);
        ctx->delta_stats.num_rd++;
        osd_op.outdata.claim_append(bl);
      }
      break;
      
    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_SRC_CMPXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;
	
	bufferlist xattr;
	if (op.op == CEPH_OSD_OP_CMPXATTR)
	  result = getattr_maybe_cache(
	    ctx->obc,
	    name,
	    &xattr);
	else
	  result = getattr_maybe_cache(
	    src_obc,
	    name,
	    &xattr);
	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;
	
	ctx->delta_stats.num_rd++;
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(xattr.length(), 10);

	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	  {
	    string val;
	    bp.copy(op.xattr.value_len, val);
	    val[op.xattr.value_len] = 0;
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	  }
	  break;

        case CEPH_OSD_CMPXATTR_MODE_U64:
	  {
	    uint64_t u64val;
	    try {
	      ::decode(u64val, bp);
	    }
	    catch (buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << u64val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_u64(op.xattr.cmp_op, u64val, xattr);
	  }
	  break;

	default:
	  dout(10) << "bad cmp mode " << (int)op.xattr.cmp_mode << dendl;
	  result = -EINVAL;
	}

	if (!result) {
	  dout(10) << "comparison returned false" << dendl;
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  dout(10) << "comparison returned " << result << " " << cpp_strerror(-result) << dendl;
	  break;
	}

	dout(10) << "comparison returned true" << dendl;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      ++ctx->num_read;
      {
	uint64_t ver = op.watch.ver;
	if (!ver)
	  result = -EINVAL;
        else if (ver < oi.user_version)
	  result = -ERANGE;
	else if (ver > oi.user_version)
	  result = -EOVERFLOW;
      }
      break;

    case CEPH_OSD_OP_LIST_WATCHERS:
      ++ctx->num_read;
      {
        obj_list_watch_response_t resp;

        map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator oi_iter;
        for (oi_iter = oi.watchers.begin(); oi_iter != oi.watchers.end();
                                       ++oi_iter) {
          dout(20) << "key cookie=" << oi_iter->first.first
               << " entity=" << oi_iter->first.second << " "
               << oi_iter->second << dendl;
          assert(oi_iter->first.first == oi_iter->second.cookie);
          assert(oi_iter->first.second.is_client());

          watch_item_t wi(oi_iter->first.second, oi_iter->second.cookie,
		 oi_iter->second.timeout_seconds, oi_iter->second.addr);
          resp.entries.push_back(wi);
        }

        resp.encode(osd_op.outdata);
        result = 0;

        ctx->delta_stats.num_rd++;
        break;
      }

    case CEPH_OSD_OP_LIST_SNAPS:
      ++ctx->num_read;
      {
        obj_list_snap_response_t resp;

        if (!ssc) {
	  ssc = ctx->obc->ssc = get_snapset_context(soid, false);
        }
        assert(ssc);

        int clonecount = ssc->snapset.clones.size();
        if (ssc->snapset.head_exists)
          clonecount++;
        resp.clones.reserve(clonecount);
        for (vector<snapid_t>::const_iterator clone_iter = ssc->snapset.clones.begin();
	     clone_iter != ssc->snapset.clones.end(); ++clone_iter) {
          clone_info ci;
          ci.cloneid = *clone_iter;

	  hobject_t clone_oid = soid;
	  clone_oid.snap = *clone_iter;
	  ObjectContextRef clone_obc = ctx->src_obc[clone_oid];
          assert(clone_obc);
	  for (vector<snapid_t>::reverse_iterator p = clone_obc->obs.oi.snaps.rbegin();
	       p != clone_obc->obs.oi.snaps.rend();
	       ++p) {
	    ci.snaps.push_back(*p);
	  }

          dout(20) << " clone " << *clone_iter << " snaps " << ci.snaps << dendl;

          map<snapid_t, interval_set<uint64_t> >::const_iterator coi;
          coi = ssc->snapset.clone_overlap.find(ci.cloneid);
          if (coi == ssc->snapset.clone_overlap.end()) {
            osd->clog.error() << "osd." << osd->whoami << ": inconsistent clone_overlap found for oid "
			      << soid << " clone " << *clone_iter;
            result = -EINVAL;
            break;
          }
          const interval_set<uint64_t> &o = coi->second;
          ci.overlap.reserve(o.num_intervals());
          for (interval_set<uint64_t>::const_iterator r = o.begin();
               r != o.end(); ++r) {
            ci.overlap.push_back(pair<uint64_t,uint64_t>(r.get_start(), r.get_len()));
          }

          map<snapid_t, uint64_t>::const_iterator si;
          si = ssc->snapset.clone_size.find(ci.cloneid);
          if (si == ssc->snapset.clone_size.end()) {
            osd->clog.error() << "osd." << osd->whoami << ": inconsistent clone_size found for oid "
			      << soid << " clone " << *clone_iter;
            result = -EINVAL;
            break;
          }
          ci.size = si->second;

          resp.clones.push_back(ci);
        }
        if (ssc->snapset.head_exists &&
	    !ctx->obc->obs.oi.is_whiteout()) {
          assert(obs.exists);
          clone_info ci;
          ci.cloneid = CEPH_NOSNAP;

          //Size for HEAD is oi.size
          ci.size = oi.size;

          resp.clones.push_back(ci);
        }
	resp.seq = ssc->snapset.seq;

        resp.encode(osd_op.outdata);
        result = 0;

        ctx->delta_stats.num_rd++;
        break;
      }

    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      ++ctx->num_read;
      {
	uint64_t ver = op.assert_ver.ver;
	if (!ver)
	  result = -EINVAL;
        else if (ver < src_obc->obs.oi.user_version)
	  result = -ERANGE;
	else if (ver > src_obc->obs.oi.user_version)
	  result = -EOVERFLOW;
	break;
      }

   case CEPH_OSD_OP_NOTIFY:
      ++ctx->num_read;
      {
	uint32_t ver;
	uint32_t timeout;
        bufferlist bl;

	try {
          ::decode(ver, bp);
	  ::decode(timeout, bp);
          ::decode(bl, bp);
	} catch (const buffer::error &e) {
	  timeout = 0;
	}
	if (!timeout)
	  timeout = cct->_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.cookie = op.watch.cookie;
        n.bl = bl;
	ctx->notifies.push_back(n);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      ++ctx->num_read;
      {
	try {
	  uint64_t notify_id = 0;
	  uint64_t watch_cookie = 0;
	  ::decode(notify_id, bp);
	  ::decode(watch_cookie, bp);
	  OpContext::NotifyAck ack(notify_id, watch_cookie);
	  ctx->notify_acks.push_back(ack);
	} catch (const buffer::error &e) {
	  OpContext::NotifyAck ack(
	    // op.watch.cookie is actually the notify_id for historical reasons
	    op.watch.cookie
	    );
	  ctx->notify_acks.push_back(ack);
	}
      }
      break;

    case CEPH_OSD_OP_SETALLOCHINT:
      ++ctx->num_write;
      {
        if (!(get_min_peer_features() & CEPH_FEATURE_OSD_SET_ALLOC_HINT)) { 
          result = -EOPNOTSUPP;
          break;
        }
        if (!obs.exists) {
          ctx->mod_desc.create();
          t->touch(soid);
          ctx->delta_stats.num_objects++;
          obs.exists = true;
        }
        t->set_alloc_hint(soid, op.alloc_hint.expected_object_size,
                          op.alloc_hint.expected_write_size);
        ctx->delta_stats.num_wr++;
        result = 0;
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      ++ctx->num_write;
      { // write
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}

	if (pool.info.requires_aligned_append() &&
	    (op.extent.offset % pool.info.required_alignment() != 0)) {
	  result = -EOPNOTSUPP;
	  break;
	}

	if (!obs.exists) {
	  if (pool.info.require_rollback() && op.extent.offset) {
	    result = -EOPNOTSUPP;
	    break;
	  }
	  ctx->mod_desc.create();
	} else if (op.extent.offset == oi.size) {
	  ctx->mod_desc.append(oi.size);
	} else {
	  ctx->mod_desc.mark_unrollbackable();
	  if (pool.info.require_rollback()) {
	    result = -EOPNOTSUPP;
	    break;
	  }
	}

        __u32 seq = oi.truncate_seq;
        if (seq && (seq > op.extent.truncate_seq) &&
            (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
	  bufferlist t;
	  t.substr_of(osd_op.indata, 0, op.extent.length);
	  osd_op.indata.swap(t);
        }
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  if (obs.exists && !oi.is_whiteout()) {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", truncating to " << op.extent.truncate_size << dendl;
	    t->truncate(soid, op.extent.truncate_size);
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	    if (op.extent.truncate_size != oi.size) {
	      ctx->delta_stats.num_bytes -= oi.size;
	      ctx->delta_stats.num_bytes += op.extent.truncate_size;
	      oi.size = op.extent.truncate_size;
	    }
	  } else {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", but object is new" << dendl;
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	  }
	}
	result = check_offset_and_length(op.extent.offset, op.extent.length, cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;
	if (pool.info.require_rollback()) {
	  t->append(soid, op.extent.offset, op.extent.length, osd_op.indata);
	} else {
	  t->write(soid, op.extent.offset, op.extent.length, osd_op.indata);
	}
	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.extent.offset, op.extent.length, true);
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      ++ctx->num_write;
      { // write full object
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}
	result = check_offset_and_length(op.extent.offset, op.extent.length, cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;

	if (pool.info.require_rollback()) {
	  if (obs.exists) {
	    if (ctx->mod_desc.rmobject(ctx->at_version.version)) {
	      t->stash(soid, ctx->at_version.version);
	    } else {
	      t->remove(soid);
	    }
	  }
	  ctx->mod_desc.create();
	  t->append(soid, op.extent.offset, op.extent.length, osd_op.indata);
	  if (obs.exists) {
	    map<string, bufferlist> to_set = ctx->obc->attr_cache;
	    map<string, boost::optional<bufferlist> > &overlay =
	      ctx->pending_attrs[ctx->obc];
	    for (map<string, boost::optional<bufferlist> >::iterator i =
		   overlay.begin();
		 i != overlay.end();
		 ++i) {
	      if (i->second) {
		to_set[i->first] = *(i->second);
	      } else {
		to_set.erase(i->first);
	      }
	    }
	    t->setattrs(soid, to_set);
	  }
	} else {
	  ctx->mod_desc.mark_unrollbackable();
	  if (obs.exists) {
	    t->truncate(soid, 0);
	  }
	  t->write(soid, op.extent.offset, op.extent.length, osd_op.indata);
	}
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	interval_set<uint64_t> ch;
	if (oi.size > 0)
	  ch.insert(0, oi.size);
	ctx->modified_ranges.union_of(ch);
	if (op.extent.length + op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  oi.size = op.extent.length + op.extent.offset;
	  ctx->delta_stats.num_bytes += oi.size;
	}
	ctx->delta_stats.num_wr++;
	ctx->delta_stats.num_wr_kb += SHIFT_ROUND_UP(op.extent.length, 10);
      }
      break;

    case CEPH_OSD_OP_ROLLBACK :
      ++ctx->num_write;
      result = _rollback_to(ctx, op);
      break;

    case CEPH_OSD_OP_ZERO:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      { // zero
	result = check_offset_and_length(op.extent.offset, op.extent.length, cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;
	assert(op.extent.length);
	if (obs.exists && !oi.is_whiteout()) {
	  ctx->mod_desc.mark_unrollbackable();
	  t->zero(soid, op.extent.offset, op.extent.length);
	  interval_set<uint64_t> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->delta_stats.num_wr++;
	} else {
	  // no-op
	}
      }
      break;
    case CEPH_OSD_OP_CREATE:
      ++ctx->num_write;
      {
        int flags = le32_to_cpu(op.flags);
	if (obs.exists && !oi.is_whiteout() &&
	    (flags & CEPH_OSD_OP_FLAG_EXCL)) {
          result = -EEXIST; /* this is an exclusive create */
	} else {
	  if (osd_op.indata.length()) {
	    bufferlist::iterator p = osd_op.indata.begin();
	    string category;
	    try {
	      ::decode(category, p);
	    }
	    catch (buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    if (category.size()) {
	      if (obs.exists && !oi.is_whiteout()) {
		if (obs.oi.category != category)
		  result = -EEXIST;  // category cannot be reset
	      } else {
		obs.oi.category = category;
	      }
	    }
	  }
          if (result >= 0) {
            if (!obs.exists)
              ctx->mod_desc.create();
            t->touch(soid);
            if (!obs.exists) {
              ctx->delta_stats.num_objects++;
              obs.exists = true;
            }
          }
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      ctx->mod_desc.mark_unrollbackable();
      {
	// truncate
	if (!obs.exists || oi.is_whiteout()) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}

	if (op.extent.offset > cct->_conf->osd_max_object_size) {
	  result = -EFBIG;
	  break;
	}

	if (op.extent.truncate_seq) {
	  assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
		     << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
		   << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	t->truncate(soid, op.extent.offset);
	if (oi.size > op.extent.offset) {
	  interval_set<uint64_t> trim;
	  trim.insert(op.extent.offset, oi.size-op.extent.offset);
	  ctx->modified_ranges.union_of(trim);
	}
	if (op.extent.offset != oi.size) {
	  ctx->delta_stats.num_bytes -= oi.size;
	  ctx->delta_stats.num_bytes += op.extent.offset;
	  oi.size = op.extent.offset;
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.
      }
      break;
    
    case CEPH_OSD_OP_DELETE:
      ++ctx->num_write;
      if (ctx->obc->obs.oi.watchers.size()) {
	// Cannot delete an object with watchers
	result = -EBUSY;
      } else {
	result = _delete_oid(ctx, false);
      }
      break;

    case CEPH_OSD_OP_CLONERANGE:
      ctx->mod_desc.mark_unrollbackable();
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  t->touch(obs.oi.soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	if (op.clonerange.src_offset + op.clonerange.length > src_obc->obs.oi.size) {
	  dout(10) << " clonerange source " << osd_op.soid << " "
		   << op.clonerange.src_offset << "~" << op.clonerange.length
		   << " extends past size " << src_obc->obs.oi.size << dendl;
	  result = -EINVAL;
	  break;
	}
	t->clone_range(src_obc->obs.oi.soid,
		      obs.oi.soid, op.clonerange.src_offset,
		      op.clonerange.length, op.clonerange.offset);
		      

	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.clonerange.offset, op.clonerange.length, false);
      }
      break;
      
    case CEPH_OSD_OP_WATCH:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
        uint64_t cookie = op.watch.cookie;
	bool do_watch = op.watch.flag & 1;
        entity_name_t entity = ctx->reqid.name;
	ObjectContextRef obc = ctx->obc;

	dout(10) << "watch: ctx->obc=" << (void *)obc.get() << " cookie=" << cookie
		 << " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(10) << "watch: oi.user_version=" << oi.user_version<< dendl;
	dout(10) << "watch: peer_addr="
	  << ctx->op->get_req()->get_connection()->get_peer_addr() << dendl;

	watch_info_t w(cookie, cct->_conf->osd_client_watch_timeout,
	  ctx->op->get_req()->get_connection()->get_peer_addr());
	if (do_watch) {
	  if (oi.watchers.count(make_pair(cookie, entity))) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[make_pair(cookie, entity)] = w;
	    t->nop();  // make sure update the object_info on disk!
	  }
	  ctx->watch_connects.push_back(w);
        } else {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by "
		     << entity << dendl;
            oi.watchers.erase(oi_iter);
	    t->nop();  // update oi on disk
	    ctx->watch_disconnects.push_back(w);
	  } else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
        }
      }
      break;


      // -- object attrs --
      
    case CEPH_OSD_OP_SETXATTR:
      ++ctx->num_write;
      {
	if (cct->_conf->osd_max_attr_size > 0 &&
	    op.xattr.value_len > cct->_conf->osd_max_attr_size) {
	  result = -EFBIG;
	  break;
	}
	if (!obs.exists) {
	  ctx->mod_desc.create();
	  t->touch(soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	if (pool.info.require_rollback()) {
	  map<string, boost::optional<bufferlist> > to_set;
	  bufferlist old;
	  int r = getattr_maybe_cache(ctx->obc, name, &old);
	  if (r == 0) {
	    to_set[name] = old;
	  } else {
	    to_set[name];
	  }
	  ctx->mod_desc.setattrs(to_set);
	} else {
	  ctx->mod_desc.mark_unrollbackable();
	}
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	setattr_maybe_cache(ctx->obc, ctx, t, name, bl);
 	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      ++ctx->num_write;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	if (pool.info.require_rollback()) {
	  map<string, boost::optional<bufferlist> > to_set;
	  bufferlist old;
	  int r = getattr_maybe_cache(ctx->obc, name, &old);
	  if (r == 0) {
	    to_set[name] = old;
	  } else {
	    to_set[name];
	  }
	  ctx->mod_desc.setattrs(to_set);
	} else {
	  ctx->mod_desc.mark_unrollbackable();
	}
	rmattr_maybe_cache(ctx->obc, ctx, t, name);
 	ctx->delta_stats.num_wr++;
      }
      break;
    

      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
      {
	// just do it inline; this works because we are happy to execute
	// fancy op on replicas as well.
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITE;
	newop.op.extent.offset = oi.size;
	newop.op.extent.length = op.extent.length;
	newop.op.extent.truncate_seq = oi.truncate_seq;
        newop.indata = osd_op.indata;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      t->nop();
      break;


      // -- trivial map --
    case CEPH_OSD_OP_TMAPGET:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_SYNC_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      {
	//_dout_lock.Lock();
	//osd_op.data.hexdump(*_dout);
	//_dout_lock.Unlock();

	// verify sort order
	bool unsorted = false;
	if (true) {
	  bufferlist header;
	  ::decode(header, bp);
	  uint32_t n;
	  ::decode(n, bp);
	  string last_key;
	  while (n--) {
	    string key;
	    ::decode(key, bp);
	    dout(10) << "tmapput key " << key << dendl;
	    bufferlist val;
	    ::decode(val, bp);
	    if (key < last_key) {
	      dout(10) << "TMAPPUT is unordered; resorting" << dendl;
	      unsorted = true;
	      break;
	    }
	    last_key = key;
	  }
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.indata.length();
	newop.indata = osd_op.indata;

	if (unsorted) {
	  bp = osd_op.indata.begin();
	  bufferlist header;
	  map<string, bufferlist> m;
	  ::decode(header, bp);
	  ::decode(m, bp);
	  assert(bp.end());
	  bufferlist newbl;
	  ::encode(header, newbl);
	  ::encode(m, newbl);
	  newop.indata = newbl;
	}
	do_osd_ops(ctx, nops);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = do_tmapup(ctx, bp, osd_op);
      break;

    case CEPH_OSD_OP_TMAP2OMAP:
      ++ctx->num_write;
      result = do_tmap2omap(ctx, op.tmap2omap.flags);
      break;

      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	try {
	  ::decode(start_after, bp);
	  ::decode(max_return, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	set<string> out_set;

	if (!pool.info.require_rollback()) {
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    coll, soid
	    );
	  assert(iter);
	  iter->upper_bound(start_after);
	  for (uint64_t i = 0;
	       i < max_return && iter->valid();
	       ++i, iter->next()) {
	    out_set.insert(iter->key());
	  }
	} // else return empty out_set
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	string filter_prefix;
	try {
	  ::decode(start_after, bp);
	  ::decode(max_return, bp);
	  ::decode(filter_prefix, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	map<string, bufferlist> out_set;

	if (!pool.info.require_rollback()) {
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    coll, soid
	    );
          if (!iter) {
            result = -ENOENT;
            goto fail;
          }
	  iter->upper_bound(start_after);
	  if (filter_prefix >= start_after) iter->lower_bound(filter_prefix);
	  for (uint64_t i = 0;
	       i < max_return && iter->valid() &&
		 iter->key().substr(0, filter_prefix.size()) == filter_prefix;
	       ++i, iter->next()) {
	    dout(20) << "Found key " << iter->key() << dendl;
	    out_set.insert(make_pair(iter->key(), iter->value()));
	  }
	} // else return empty out_set
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETHEADER:
      if (pool.info.require_rollback()) {
	// return empty header
	break;
      }
      ++ctx->num_read;
      {
	osd->store->omap_get_header(coll, soid, &osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
      ++ctx->num_read;
      {
	set<string> keys_to_get;
	try {
	  ::decode(keys_to_get, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	map<string, bufferlist> out;
	if (!pool.info.require_rollback()) {
	  osd->store->omap_get_values(coll, soid, keys_to_get, &out);
	} // else return empty omap entries
	::encode(out, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAP_CMP:
      ++ctx->num_read;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	map<string, pair<bufferlist, int> > assertions;
	try {
	  ::decode(assertions, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	
	map<string, bufferlist> out;

	if (!pool.info.require_rollback()) {
	  set<string> to_get;
	  for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	       i != assertions.end();
	       ++i)
	    to_get.insert(i->first);
	  int r = osd->store->omap_get_values(coll, soid, to_get, &out);
	  if (r < 0) {
	    result = r;
	    break;
	  }
	} // else leave out empty

	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	int r = 0;
	bufferlist empty;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i) {
	  bufferlist &bl = out.count(i->first) ? 
	    out[i->first] : empty;
	  switch (i->second.second) {
	  case CEPH_OSD_CMPXATTR_OP_EQ:
	    if (!(bl == i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_LT:
	    if (!(bl < i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_GT:
	    if (!(bl > i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  default:
	    r = -EINVAL;
	    break;
	  }
	  if (r < 0)
	    break;
	}
	if (r < 0) {
	  result = r;
	}
      }
      break;

      // OMAP Write ops
    case CEPH_OSD_OP_OMAPSETVALS:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ctx->mod_desc.mark_unrollbackable();
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t->touch(soid);
	map<string, bufferlist> to_set;
	try {
	  ::decode(to_set, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	dout(20) << "setting vals: " << dendl;
	for (map<string, bufferlist>::iterator i = to_set.begin();
	     i != to_set.end();
	     ++i) {
	  dout(20) << "\t" << i->first << dendl;
	}
	t->omap_setkeys(soid, to_set);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_OMAPSETHEADER:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ctx->mod_desc.mark_unrollbackable();
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t->touch(soid);
	t->omap_setheader(soid, osd_op.indata);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_OMAPCLEAR:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ctx->mod_desc.mark_unrollbackable();
      ++ctx->num_write;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	t->touch(soid);
	t->omap_clear(soid);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_OMAPRMKEYS:
      if (pool.info.require_rollback()) {
	result = -EOPNOTSUPP;
	break;
      }
      ctx->mod_desc.mark_unrollbackable();
      ++ctx->num_write;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	t->touch(soid);
	set<string> to_rm;
	try {
	  ::decode(to_rm, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t->omap_rmkeys(soid, to_rm);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      break;

    case CEPH_OSD_OP_COPY_GET_CLASSIC:
      ++ctx->num_read;
      result = fill_in_copy_get(ctx, bp, osd_op, ctx->obc, true);
      if (result == -EINVAL)
	goto fail;
      break;

    case CEPH_OSD_OP_COPY_GET:
      ++ctx->num_read;
      result = fill_in_copy_get(ctx, bp, osd_op, ctx->obc, false);
      if (result == -EINVAL)
	goto fail;
      break;

    case CEPH_OSD_OP_COPY_FROM:
      ++ctx->num_write;
      {
	object_t src_name;
	object_locator_t src_oloc;
	snapid_t src_snapid = (uint64_t)op.copy_from.snapid;
	version_t src_version = op.copy_from.src_version;
	try {
	  ::decode(src_name, bp);
	  ::decode(src_oloc, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	if (!ctx->copy_cb) {
	  // start
	  pg_t raw_pg;
	  get_osdmap()->object_locator_to_pg(src_name, src_oloc, raw_pg);
	  hobject_t src(src_name, src_oloc.key, src_snapid,
			raw_pg.ps(), raw_pg.pool(),
			src_oloc.nspace);
	  if (src == soid) {
	    dout(20) << " copy from self is invalid" << dendl;
	    result = -EINVAL;
	    break;
	  }
	  CopyFromCallback *cb = new CopyFromCallback(ctx);
	  ctx->copy_cb = cb;
	  start_copy(cb, ctx->obc, src, src_oloc, src_version,
		     op.copy_from.flags,
		     false);
	  result = -EINPROGRESS;
	} else {
	  // finish
	  assert(ctx->copy_cb->get_result() >= 0);
	  finish_copyfrom(ctx);
	  result = 0;
	}
      }
      break;

    default:
      dout(1) << "unrecognized osd op " << op.op
	      << " " << ceph_osd_op_name(op.op)
	      << dendl;
      result = -EOPNOTSUPP;
    }

    ctx->bytes_read += osd_op.outdata.length();

  fail:
    osd_op.rval = result;
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK))
      result = 0;

    if (result < 0)
      break;
  }
  return result;
}

int ReplicatedPG::_get_tmap(OpContext *ctx, bufferlist *header, bufferlist *vals)
{
  if (ctx->new_obs.oi.size == 0) {
    dout(20) << "unable to get tmap for zero sized " << ctx->new_obs.oi.soid << dendl;
    return -ENODATA;
  }
  vector<OSDOp> nops(1);
  OSDOp &newop = nops[0];
  newop.op.op = CEPH_OSD_OP_TMAPGET;
  do_osd_ops(ctx, nops);
  try {
    bufferlist::iterator i = newop.outdata.begin();
    ::decode(*header, i);
    (*vals).substr_of(newop.outdata, i.get_off(), i.get_remaining());
  } catch (...) {
    dout(20) << "unsuccessful at decoding tmap for " << ctx->new_obs.oi.soid
	     << dendl;
    return -EINVAL;
  }
  dout(20) << "successful at decoding tmap for " << ctx->new_obs.oi.soid
	   << dendl;
  return 0;
}

int ReplicatedPG::_verify_no_head_clones(const hobject_t& soid,
					const SnapSet& ss)
{
  // verify that all clones have been evicted
  dout(20) << __func__ << " verifying clones are absent "
	   << ss << dendl;
  for (vector<snapid_t>::const_iterator p = ss.clones.begin();
       p != ss.clones.end();
       ++p) {
    hobject_t clone_oid = soid;
    clone_oid.snap = *p;
    if (is_missing_object(clone_oid))
      return -EBUSY;
    ObjectContextRef clone_obc = get_object_context(clone_oid, false);
    if (clone_obc && clone_obc->obs.exists) {
      dout(10) << __func__ << " cannot evict head before clone "
	       << clone_oid << dendl;
      return -EBUSY;
    }
    if (copy_ops.count(clone_oid)) {
      dout(10) << __func__ << " cannot evict head, pending promote on clone "
	       << clone_oid << dendl;
      return -EBUSY;
    }
  }
  return 0;
}

inline int ReplicatedPG::_delete_oid(OpContext *ctx, bool no_whiteout)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGBackend::PGTransaction* t = ctx->op_t;

  if (!obs.exists || (obs.oi.is_whiteout() && !no_whiteout))
    return -ENOENT;

  if (pool.info.require_rollback()) {
    if (ctx->mod_desc.rmobject(ctx->at_version.version)) {
      t->stash(soid, ctx->at_version.version);
    } else {
      t->remove(soid);
    }
    map<string, bufferlist> new_attrs;
    replace_cached_attrs(ctx, ctx->obc, new_attrs);
  } else {
    ctx->mod_desc.mark_unrollbackable();
    t->remove(soid);
  }

  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
  }

  ctx->delta_stats.num_wr++;
  if (soid.is_snap()) {
    assert(ctx->obc->ssc->snapset.clone_overlap.count(soid.snap));
    ctx->delta_stats.num_bytes -= ctx->obc->ssc->snapset.get_clone_bytes(soid.snap);
  } else {
    ctx->delta_stats.num_bytes -= oi.size;
  }
  oi.size = 0;

  // cache: cache: set whiteout on delete?
  if (pool.info.cache_mode != pg_pool_t::CACHEMODE_NONE && !no_whiteout) {
    dout(20) << __func__ << " setting whiteout on " << soid << dendl;
    oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ctx->delta_stats.num_whiteouts++;
    t->touch(soid);
    osd->logger->inc(l_osd_tier_whiteout);
    return 0;
  }

  ctx->delta_stats.num_objects--;
  if (soid.is_snap())
    ctx->delta_stats.num_object_clones--;
  if (oi.is_whiteout()) {
    dout(20) << __func__ << " deleting whiteout on " << soid << dendl;
    ctx->delta_stats.num_whiteouts--;
  }
  if (soid.is_head())
    snapset.head_exists = false;
  obs.exists = false;
  return 0;
}

int ReplicatedPG::_rollback_to(OpContext *ctx, ceph_osd_op& op)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGBackend::PGTransaction* t = ctx->op_t;
  snapid_t snapid = (uint64_t)op.snap.snapid;
  hobject_t missing_oid;

  dout(10) << "_rollback_to " << soid << " snapid " << snapid << dendl;

  ObjectContextRef rollback_to;
  int ret = find_object_context(
    hobject_t(soid.oid, soid.get_key(), snapid, soid.hash, info.pgid.pool(),
	      soid.get_namespace()),
    &rollback_to, false, false, &missing_oid);
  if (ret == -EAGAIN) {
    /* clone must be missing */
    assert(is_missing_object(missing_oid));
    dout(20) << "_rollback_to attempted to roll back to a missing object "
	     << missing_oid << " (requested snapid: ) " << snapid << dendl;
    wait_for_unreadable_object(missing_oid, ctx->op);
    return ret;
  }
  if (maybe_handle_cache(ctx->op, true, rollback_to, ret, missing_oid, true)) {
    // promoting the rollback src, presumably
    return -EAGAIN;
  }
  if (ret == -ENOENT || (rollback_to && rollback_to->obs.oi.is_whiteout())) {
    // there's no snapshot here, or there's no object.
    // if there's no snapshot, we delete the object; otherwise, do nothing.
    dout(20) << "_rollback_to deleting head on " << soid.oid
	     << " because got ENOENT|whiteout on find_object_context" << dendl;
    if (ctx->obc->obs.oi.watchers.size()) {
      // Cannot delete an object with watchers
      ret = -EBUSY;
    } else {
      _delete_oid(ctx, false);
      ret = 0;
    }
  } else if (ret) {
    // ummm....huh? It *can't* return anything else at time of writing.
    assert(0 == "unexpected error code in _rollback_to");
  } else { //we got our context, let's use it to do the rollback!
    hobject_t& rollback_to_sobject = rollback_to->obs.oi.soid;
    if (is_degraded_object(rollback_to_sobject)) {
      dout(20) << "_rollback_to attempted to roll back to a degraded object "
	       << rollback_to_sobject << " (requested snapid: ) " << snapid << dendl;
      wait_for_degraded_object(rollback_to_sobject, ctx->op);
      ret = -EAGAIN;
    } else if (rollback_to->obs.oi.soid.snap == CEPH_NOSNAP) {
      // rolling back to the head; we just need to clone it.
      ctx->modify = true;
    } else {
      /* 1) Delete current head
       * 2) Clone correct snapshot into head
       * 3) Calculate clone_overlaps by following overlaps
       *    forward from rollback snapshot */
      dout(10) << "_rollback_to deleting " << soid.oid
	       << " and rolling back to old snap" << dendl;

      if (pool.info.require_rollback()) {
	if (obs.exists) {
	  if (ctx->mod_desc.rmobject(ctx->at_version.version)) {
	    t->stash(soid, ctx->at_version.version);
	  } else {
	    t->remove(soid);
	  }
	}
	replace_cached_attrs(ctx, ctx->obc, rollback_to->attr_cache);
      } else {
	if (obs.exists) {
	  ctx->mod_desc.mark_unrollbackable();
	  t->remove(soid);
	}
      }
      ctx->mod_desc.create();
      t->clone(rollback_to_sobject, soid);
      snapset.head_exists = true;

      map<snapid_t, interval_set<uint64_t> >::iterator iter =
	snapset.clone_overlap.lower_bound(snapid);
      interval_set<uint64_t> overlaps = iter->second;
      assert(iter != snapset.clone_overlap.end());
      for ( ;
	    iter != snapset.clone_overlap.end();
	    ++iter)
	overlaps.intersection_of(iter->second);

      if (obs.oi.size > 0) {
	interval_set<uint64_t> modified;
	modified.insert(0, obs.oi.size);
	overlaps.intersection_of(modified);
	modified.subtract(overlaps);
	ctx->modified_ranges.union_of(modified);
      }

      // Adjust the cached objectcontext
      if (!obs.exists) {
	obs.exists = true; //we're about to recreate it
	ctx->delta_stats.num_objects++;
      }
      ctx->delta_stats.num_bytes -= obs.oi.size;
      ctx->delta_stats.num_bytes += rollback_to->obs.oi.size;
      obs.oi.size = rollback_to->obs.oi.size;
      snapset.head_exists = true;
    }
  }
  return ret;
}

void ReplicatedPG::_make_clone(
  OpContext *ctx,
  PGBackend::PGTransaction* t,
  ObjectContextRef obc,
  const hobject_t& head, const hobject_t& coid,
  object_info_t *poi)
{
  bufferlist bv;
  ::encode(*poi, bv);

  t->clone(head, coid);
  setattr_maybe_cache(obc, ctx, t, OI_ATTR, bv);
  rmattr_maybe_cache(obc, ctx, t, SS_ATTR);
}

void ReplicatedPG::make_writeable(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  SnapContext& snapc = ctx->snapc;
  PGBackend::PGTransaction *t = pgbackend->get_transaction();

  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ctx->snapset
	   << "  snapc=" << snapc << dendl;;
  
  bool was_dirty = ctx->obc->obs.oi.is_dirty();
  if (ctx->new_obs.exists) {
    // we will mark the object dirty
    if (ctx->undirty && was_dirty) {
      dout(20) << " clearing DIRTY flag" << dendl;
      assert(ctx->new_obs.oi.is_dirty());
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
      --ctx->delta_stats.num_objects_dirty;
      osd->logger->inc(l_osd_tier_clean);
    } else if (!was_dirty && !ctx->undirty) {
      dout(20) << " setting DIRTY flag" << dendl;
      ctx->new_obs.oi.set_flag(object_info_t::FLAG_DIRTY);
      ++ctx->delta_stats.num_objects_dirty;
      osd->logger->inc(l_osd_tier_dirty);
    }
  } else {
    if (was_dirty) {
      dout(20) << " deletion, decrementing num_dirty and clearing flag" << dendl;
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
      --ctx->delta_stats.num_objects_dirty;
    }
  }

  if ((ctx->new_obs.exists &&
       ctx->new_obs.oi.is_omap()) &&
      (!ctx->obc->obs.exists ||
       !ctx->obc->obs.oi.is_omap())) {
    ++ctx->delta_stats.num_objects_omap;
  }
  if ((!ctx->new_obs.exists ||
       !ctx->new_obs.oi.is_omap()) &&
      (ctx->obc->obs.exists &&
       ctx->obc->obs.oi.is_omap())) {
    --ctx->delta_stats.num_objects_omap;
  }

  // use newer snapc?
  if (ctx->new_snapset.seq > snapc.seq) {
    snapc.seq = ctx->new_snapset.seq;
    snapc.snaps = ctx->new_snapset.snaps;
    dout(10) << " using newer snapc " << snapc << dendl;
  }

  if (ctx->obs->exists)
    filter_snapc(snapc.snaps);
  
  if ((ctx->obs->exists && !ctx->obs->oi.is_whiteout()) && // head exist(ed)
      snapc.snaps.size() &&                 // there are snaps
      !ctx->cache_evict &&
      snapc.snaps[0] > ctx->new_snapset.seq) {  // existing object is old
    // clone
    hobject_t coid = soid;
    coid.snap = snapc.seq;
    
    unsigned l;
    for (l=1; l<snapc.snaps.size() && snapc.snaps[l] > ctx->new_snapset.seq; l++) ;
    
    vector<snapid_t> snaps(l);
    for (unsigned i=0; i<l; i++)
      snaps[i] = snapc.snaps[i];
    
    // prepare clone
    object_info_t static_snap_oi(coid);
    object_info_t *snap_oi;
    if (is_primary()) {
      ctx->clone_obc = object_contexts.lookup_or_create(static_snap_oi.soid);
      ctx->clone_obc->destructor_callback = new C_PG_ObjectContext(this, ctx->clone_obc.get());
      ctx->clone_obc->obs.oi = static_snap_oi;
      ctx->clone_obc->obs.exists = true;
      ctx->clone_obc->ssc = ctx->obc->ssc;
      ctx->clone_obc->ssc->ref++;
      if (pool.info.require_rollback())
	ctx->clone_obc->attr_cache = ctx->obc->attr_cache;
      snap_oi = &ctx->clone_obc->obs.oi;
      bool got = ctx->clone_obc->get_write_greedy(ctx->op);
      assert(got);
      dout(20) << " got greedy write on clone_obc " << *ctx->clone_obc << dendl;
    } else {
      snap_oi = &static_snap_oi;
    }
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);
    snap_oi->snaps = snaps;
    if (was_dirty)
      snap_oi->set_flag(object_info_t::FLAG_DIRTY);
    _make_clone(ctx, t, ctx->clone_obc, soid, coid, snap_oi);
    
    ctx->delta_stats.num_objects++;
    if (snap_oi->is_dirty())
      ctx->delta_stats.num_objects_dirty++;
    if (snap_oi->is_whiteout()) {
      dout(20) << __func__ << " cloning whiteout on " << soid << " to " << coid << dendl;
      ctx->delta_stats.num_whiteouts++;
    }
    if (snap_oi->is_omap())
      ctx->delta_stats.num_objects_omap++;
    ctx->delta_stats.num_object_clones++;
    ctx->new_snapset.clones.push_back(coid.snap);
    ctx->new_snapset.clone_size[coid.snap] = ctx->obs->oi.size;

    // clone_overlap should contain an entry for each clone 
    // (an empty interval_set if there is no overlap)
    ctx->new_snapset.clone_overlap[coid.snap];
    if (ctx->obs->oi.size)
      ctx->new_snapset.clone_overlap[coid.snap].insert(0, ctx->obs->oi.size);
    
    // log clone
    dout(10) << " cloning v " << ctx->obs->oi.version
	     << " to " << coid << " v " << ctx->at_version
	     << " snaps=" << snaps << dendl;
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::CLONE, coid, ctx->at_version,
				      ctx->obs->oi.version,
				      ctx->obs->oi.user_version,
				      osd_reqid_t(), ctx->new_obs.oi.mtime));
    ::encode(snaps, ctx->log.back().snaps);
    ctx->log.back().mod_desc.create();

    ctx->at_version.version++;
  }

  // update most recent clone_overlap and usage stats
  if (ctx->new_snapset.clones.size() > 0) {
    /* we need to check whether the most recent clone exists, if it's been evicted,
     * it's not included in the stats */
    hobject_t last_clone_oid = soid;
    last_clone_oid.snap = ctx->new_snapset.clone_overlap.rbegin()->first;
    if (is_present_clone(last_clone_oid)) {
      interval_set<uint64_t> &newest_overlap = ctx->new_snapset.clone_overlap.rbegin()->second;
      ctx->modified_ranges.intersection_of(newest_overlap);
      // modified_ranges is still in use by the clone
      add_interval_usage(ctx->modified_ranges, ctx->delta_stats);
      newest_overlap.subtract(ctx->modified_ranges);
    }
  }
  
  // prepend transaction to op_t
  t->append(ctx->op_t);
  delete ctx->op_t;
  ctx->op_t = t;

  // update snapset with latest snap context
  ctx->new_snapset.seq = snapc.seq;
  ctx->new_snapset.snaps = snapc.snaps;
  ctx->new_snapset.head_exists = ctx->new_obs.exists;
  dout(20) << "make_writeable " << soid << " done, snapset=" << ctx->new_snapset << dendl;
}


void ReplicatedPG::write_update_size_and_usage(object_stat_sum_t& delta_stats, object_info_t& oi,
					       SnapSet& ss, interval_set<uint64_t>& modified,
					       uint64_t offset, uint64_t length, bool count_bytes)
{
  interval_set<uint64_t> ch;
  if (length)
    ch.insert(offset, length);
  modified.union_of(ch);
  if (length && (offset + length > oi.size)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes += new_size - oi.size;
    oi.size = new_size;
  }
  delta_stats.num_wr++;
  if (count_bytes)
    delta_stats.num_wr_kb += SHIFT_ROUND_UP(length, 10);
}

void ReplicatedPG::add_interval_usage(interval_set<uint64_t>& s, object_stat_sum_t& delta_stats)
{
  for (interval_set<uint64_t>::const_iterator p = s.begin(); p != s.end(); ++p) {
    delta_stats.num_bytes += p.get_len();
  }
}

void ReplicatedPG::do_osd_op_effects(OpContext *ctx)
{
  ConnectionRef conn(ctx->op->get_req()->get_connection());
  boost::intrusive_ptr<OSD::Session> session(
    (OSD::Session *)conn->get_priv());
  session->put();  // get_priv() takes a ref, and so does the intrusive_ptr
  entity_name_t entity = ctx->reqid.name;

  dout(15) << "do_osd_op_effects on session " << session.get() << dendl;

  for (list<watch_info_t>::iterator i = ctx->watch_connects.begin();
       i != ctx->watch_connects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, entity);
    dout(15) << "do_osd_op_effects applying watch connect on session "
	     << session.get() << " watcher " << watcher << dendl;
    WatchRef watch;
    if (ctx->obc->watchers.count(watcher)) {
      dout(15) << "do_osd_op_effects found existing watch watcher " << watcher
	       << dendl;
      watch = ctx->obc->watchers[watcher];
    } else {
      dout(15) << "do_osd_op_effects new watcher " << watcher
	       << dendl;
      watch = Watch::makeWatchRef(
	this, osd, ctx->obc, i->timeout_seconds,
	i->cookie, entity, conn->get_peer_addr());
      ctx->obc->watchers.insert(
	make_pair(
	  watcher,
	  watch));
    }
    watch->connect(conn);
  }

  for (list<watch_info_t>::iterator i = ctx->watch_disconnects.begin();
       i != ctx->watch_disconnects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, entity);
    dout(15) << "do_osd_op_effects applying watch disconnect on session "
	     << session.get() << " and watcher " << watcher << dendl;
    if (ctx->obc->watchers.count(watcher)) {
      WatchRef watch = ctx->obc->watchers[watcher];
      dout(10) << "do_osd_op_effects applying disconnect found watcher "
	       << watcher << dendl;
      ctx->obc->watchers.erase(watcher);
      watch->remove();
    } else {
      dout(10) << "do_osd_op_effects failed to find watcher "
	       << watcher << dendl;
    }
  }

  for (list<notify_info_t>::iterator p = ctx->notifies.begin();
       p != ctx->notifies.end();
       ++p) {
    dout(10) << "do_osd_op_effects, notify " << *p << dendl;
    NotifyRef notif(
      Notify::makeNotifyRef(
	conn,
	ctx->obc->watchers.size(),
	p->bl,
	p->timeout,
	p->cookie,
	osd->get_next_id(get_osdmap()->get_epoch()),
	ctx->obc->obs.oi.user_version,
	osd));
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      dout(10) << "starting notify on watch " << i->first << dendl;
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    if (p->watch_cookie)
      dout(10) << "notify_ack " << make_pair(p->watch_cookie.get(), p->notify_id) << dendl;
    else
      dout(10) << "notify_ack " << make_pair("NULL", p->notify_id) << dendl;
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
	  p->watch_cookie.get() != i->first.first) continue;
      dout(10) << "acking notify on watch " << i->first << dendl;
      i->second->notify_ack(p->notify_id);
    }
  }
}

coll_t ReplicatedPG::get_temp_coll(ObjectStore::Transaction *t)
{
  return pgbackend->get_temp_coll(t);
}

hobject_t ReplicatedPG::generate_temp_object()
{
  ostringstream ss;
  ss << "temp_" << info.pgid << "_" << get_role() << "_" << osd->monc->get_global_id() << "_" << (++temp_seq);
  hobject_t hoid = hobject_t::make_temp(ss.str());
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

int ReplicatedPG::prepare_transaction(OpContext *ctx)
{
  assert(!ctx->ops.empty());
  
  const hobject_t& soid = ctx->obs->oi.soid;

  // valid snap context?
  if (!ctx->snapc.is_valid()) {
    dout(10) << " invalid snapc " << ctx->snapc << dendl;
    return -EINVAL;
  }

  // prepare the actual mutation
  int result = do_osd_ops(ctx, ctx->ops);
  if (result < 0)
    return result;

  // finish side-effects
  if (result == 0)
    do_osd_op_effects(ctx);

  // read-op?  done?
  if (ctx->op_t->empty() && !ctx->modify) {
    unstable_stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
    return result;
  }

  // cache: clear whiteout?
  if (pool.info.cache_mode != pg_pool_t::CACHEMODE_NONE) {
    if (ctx->user_modify &&
	ctx->obc->obs.oi.is_whiteout()) {
      dout(10) << __func__ << " clearing whiteout on " << soid << dendl;
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
      --ctx->delta_stats.num_whiteouts;
    }
  }

  // clone, if necessary
  if (soid.snap == CEPH_NOSNAP)
    make_writeable(ctx);

  finish_ctx(ctx,
	     ctx->new_obs.exists ? pg_log_entry_t::MODIFY :
	     pg_log_entry_t::DELETE);

  return result;
}

void ReplicatedPG::finish_ctx(OpContext *ctx, int log_op_type, bool maintain_ssc)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  dout(20) << __func__ << " " << soid << " " << ctx
	   << " op " << pg_log_entry_t::get_op_name(log_op_type)
	   << dendl;
  utime_t now = ceph_clock_now(cct);

  // snapset
  bufferlist bss;

  if (soid.snap == CEPH_NOSNAP && maintain_ssc) {
    ::encode(ctx->new_snapset, bss);
    assert(ctx->new_obs.exists == ctx->new_snapset.head_exists);

    if (ctx->new_obs.exists) {
      if (!ctx->obs->exists) {
	if (ctx->snapset_obc && ctx->snapset_obc->obs.exists) {
	  hobject_t snapoid = soid.get_snapdir();
	  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::DELETE, snapoid,
	      ctx->at_version,
	      ctx->snapset_obc->obs.oi.version,
	      0, osd_reqid_t(), ctx->mtime));
	  if (pool.info.require_rollback()) {
	    if (ctx->log.back().mod_desc.rmobject(ctx->at_version.version)) {
	      ctx->op_t->stash(snapoid, ctx->at_version.version);
	    } else {
	      ctx->op_t->remove(snapoid);
	    }
	  } else {
	    ctx->op_t->remove(snapoid);
	    ctx->log.back().mod_desc.mark_unrollbackable();
	  }
	  dout(10) << " removing old " << snapoid << dendl;

	  ctx->at_version.version++;

	  ctx->snapset_obc->obs.exists = false;
	}
      }
    } else if (ctx->new_snapset.clones.size() &&
	       !ctx->cache_evict &&
	       (!ctx->snapset_obc || !ctx->snapset_obc->obs.exists)) {
      // save snapset on _snap
      hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
			info.pgid.pool(), soid.get_namespace());
      dout(10) << " final snapset " << ctx->new_snapset
	       << " in " << snapoid << dendl;
      ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, snapoid,
					ctx->at_version,
	                                eversion_t(),
					0, osd_reqid_t(), ctx->mtime));

      if (!ctx->snapset_obc)
	ctx->snapset_obc = get_object_context(snapoid, true);
      bool got = ctx->snapset_obc->get_write_greedy(ctx->op);
      assert(got);
      dout(20) << " got greedy write on snapset_obc " << *ctx->snapset_obc << dendl;
      ctx->release_snapset_obc = true;
      if (pool.info.require_rollback() && !ctx->snapset_obc->obs.exists) {
	ctx->log.back().mod_desc.create();
      } else if (!pool.info.require_rollback()) {
	ctx->log.back().mod_desc.mark_unrollbackable();
      }
      ctx->snapset_obc->obs.exists = true;
      ctx->snapset_obc->obs.oi.version = ctx->at_version;
      ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
      ctx->snapset_obc->obs.oi.mtime = ctx->mtime;
      ctx->snapset_obc->obs.oi.local_mtime = now;

      bufferlist bv(sizeof(ctx->new_obs.oi));
      ::encode(ctx->snapset_obc->obs.oi, bv);
      ctx->op_t->touch(snapoid);
      setattr_maybe_cache(ctx->snapset_obc, ctx, ctx->op_t, OI_ATTR, bv);
      setattr_maybe_cache(ctx->snapset_obc, ctx, ctx->op_t, SS_ATTR, bss);
      if (pool.info.require_rollback()) {
	map<string, boost::optional<bufferlist> > to_set;
	to_set[SS_ATTR];
	to_set[OI_ATTR];
	ctx->log.back().mod_desc.setattrs(to_set);
      } else {
	ctx->log.back().mod_desc.mark_unrollbackable();
      }
      ctx->at_version.version++;
    }
  }

  // finish and log the op.
  if (ctx->user_modify) {
    // update the user_version for any modify ops, except for the watch op
    ctx->user_at_version = MAX(info.last_user_version, ctx->new_obs.oi.user_version) + 1;
    /* In order for new clients and old clients to interoperate properly
     * when exchanging versions, we need to lower bound the user_version
     * (which our new clients pay proper attention to)
     * by the at_version (which is all the old clients can ever see). */
    if (ctx->at_version.version > ctx->user_at_version)
      ctx->user_at_version = ctx->at_version.version;
    ctx->new_obs.oi.user_version = ctx->user_at_version;
  }
  ctx->bytes_written = ctx->op_t->get_bytes_written();
 
  if (ctx->new_obs.exists) {
    // on the head object
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = ctx->obs->oi.version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
      ctx->new_obs.oi.local_mtime = now;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->new_obs.oi, bv);
    setattr_maybe_cache(ctx->obc, ctx, ctx->op_t, OI_ATTR, bv);

    if (soid.snap == CEPH_NOSNAP) {
      dout(10) << " final snapset " << ctx->new_snapset
	       << " in " << soid << dendl;
      setattr_maybe_cache(ctx->obc, ctx, ctx->op_t, SS_ATTR, bss);

      if (pool.info.require_rollback()) {
	set<string> changing;
	changing.insert(OI_ATTR);
	changing.insert(SS_ATTR);
	ctx->obc->fill_in_setattrs(changing, &(ctx->mod_desc));
      } else {
	// replicated pools are never rollbackable in this case
	ctx->mod_desc.mark_unrollbackable();
      }
    } else {
      dout(10) << " no snapset (this is a clone)" << dendl;
    }
  } else {
    ctx->new_obs.oi = object_info_t(ctx->obc->obs.oi.soid);
  }

  // append to log
  ctx->log.push_back(pg_log_entry_t(log_op_type, soid, ctx->at_version,
				    ctx->obs->oi.version,
				    ctx->user_at_version, ctx->reqid,
				    ctx->mtime));
  if (soid.snap < CEPH_NOSNAP) {
    set<snapid_t> _snaps(ctx->new_obs.oi.snaps.begin(),
			 ctx->new_obs.oi.snaps.end());
    switch (log_op_type) {
    case pg_log_entry_t::MODIFY:
    case pg_log_entry_t::PROMOTE:
      dout(20) << __func__ << " encoding snaps " << ctx->new_obs.oi.snaps
	       << dendl;
      ::encode(ctx->new_obs.oi.snaps, ctx->log.back().snaps);
      break;
    case pg_log_entry_t::CLEAN:
      dout(20) << __func__ << " encoding snaps " << ctx->new_obs.oi.snaps
	       << dendl;
      ::encode(ctx->new_obs.oi.snaps, ctx->log.back().snaps);
      break;
    default:
      break;
    }
  }

  ctx->log.back().mod_desc.claim(ctx->mod_desc);

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;

  if (!maintain_ssc && soid.is_head()) {
    ctx->obc->ssc->exists = false;
    ctx->obc->ssc->snapset = SnapSet();
  } else {
    ctx->obc->ssc->exists = true;
    ctx->obc->ssc->snapset = ctx->new_snapset;
  }

  info.stats.stats.add(ctx->delta_stats, ctx->obs->oi.category);

  for (set<pg_shard_t>::iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    pg_shard_t bt = *i;
    pg_info_t& pinfo = peer_info[bt];
    if (soid <= pinfo.last_backfill)
      pinfo.stats.stats.add(ctx->delta_stats, ctx->obs->oi.category);
    else if (soid <= last_backfill_started)
      pending_backfill_updates[soid].stats.add(ctx->delta_stats,
					       ctx->obs->oi.category);
  }

  if (scrubber.active && scrubber.is_chunky) {
    assert(soid < scrubber.start || soid >= scrubber.end);
    if (soid < scrubber.start)
      scrub_cstat.add(ctx->delta_stats, ctx->obs->oi.category);
  }
}

void ReplicatedPG::complete_read_ctx(int result, OpContext *ctx)
{
  MOSDOp *m = static_cast<MOSDOp*>(ctx->op->get_req());
  assert(ctx->async_reads_complete());
  ctx->reply->claim_op_out_data(ctx->ops);
  ctx->reply->get_header().data_off = ctx->data_off;

  MOSDOpReply *reply = ctx->reply;
  ctx->reply = NULL;

  if (result >= 0) {
    log_op_stats(ctx);
    publish_stats_to_osd();

    // on read, return the current object version
    reply->set_reply_versions(eversion_t(), ctx->obs->oi.user_version);
  } else if (result == -ENOENT) {
    // on ENOENT, set a floor for what the next user version will be.
    reply->set_enoent_reply_versions(info.last_update, info.last_user_version);
  }

  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
  close_op_ctx(ctx, 0);
}

// ========================================================================
// copyfrom

struct C_Copyfrom : public Context {
  ReplicatedPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  ReplicatedPG::CopyOpRef cop;
  C_Copyfrom(ReplicatedPG *p, hobject_t o, epoch_t lpr,
	     const ReplicatedPG::CopyOpRef& c)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), cop(c)
  {}
  void finish(int r) {
    if (r == -ECANCELED)
      return;
    pg->lock();
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->process_copy_chunk(oid, tid, r);
    }
    pg->unlock();
  }
};

struct C_CopyFrom_AsyncReadCb : public Context {
  OSDOp *osd_op;
  object_copy_data_t reply_obj;
  bool classic;
  size_t len;
  C_CopyFrom_AsyncReadCb(OSDOp *osd_op, bool classic) :
    osd_op(osd_op), classic(classic), len(0) {}
  void finish(int r) {
    assert(len > 0);
    assert(len <= reply_obj.data.length());
    bufferlist bl;
    bl.substr_of(reply_obj.data, 0, len);
    reply_obj.data.swap(bl);
    if (classic) {
      reply_obj.encode_classic(osd_op->outdata);
    } else {
      ::encode(reply_obj, osd_op->outdata);
    }
  }
};

int ReplicatedPG::fill_in_copy_get(
  OpContext *ctx,
  bufferlist::iterator& bp,
  OSDOp& osd_op,
  ObjectContextRef &obc,
  bool classic)
{
  object_info_t& oi = obc->obs.oi;
  hobject_t& soid = oi.soid;
  int result = 0;
  object_copy_cursor_t cursor;
  uint64_t out_max;
  try {
    ::decode(cursor, bp);
    ::decode(out_max, bp);
  }
  catch (buffer::error& e) {
    result = -EINVAL;
    return result;
  }

  bool async_read_started = false;
  object_copy_data_t _reply_obj;
  C_CopyFrom_AsyncReadCb *cb = NULL;
  if (pool.info.require_rollback()) {
    cb = new C_CopyFrom_AsyncReadCb(&osd_op, classic);
  }
  object_copy_data_t &reply_obj = cb ? cb->reply_obj : _reply_obj;
  // size, mtime
  reply_obj.size = oi.size;
  reply_obj.mtime = oi.mtime;
  reply_obj.category = oi.category;
  if (soid.snap < CEPH_NOSNAP) {
    reply_obj.snaps = oi.snaps;
  } else {
    assert(obc->ssc);
    reply_obj.snap_seq = obc->ssc->snapset.seq;
  }

  // attrs
  map<string,bufferlist>& out_attrs = reply_obj.attrs;
  if (!cursor.attr_complete) {
    result = getattrs_maybe_cache(
      ctx->obc,
      &out_attrs,
      true);
    if (result < 0) {
      if (cb) {
        delete cb;
      }
      return result;
    }
    cursor.attr_complete = true;
    dout(20) << " got attrs" << dendl;
  }

  int64_t left = out_max - osd_op.outdata.length();

  // data
  bufferlist& bl = reply_obj.data;
  if (left > 0 && !cursor.data_complete) {
    if (cursor.data_offset < oi.size) {
      if (cb) {
	async_read_started = true;
	ctx->pending_async_reads.push_back(
	  make_pair(
	    make_pair(cursor.data_offset, left),
	    make_pair(&bl, cb)));
	result = MIN(oi.size - cursor.data_offset, (uint64_t)left);
	cb->len = result;
      } else {
	result = pgbackend->objects_read_sync(
	  oi.soid, cursor.data_offset, left, &bl);
	if (result < 0)
	  return result;
      }
      assert(result <= left);
      left -= result;
      cursor.data_offset += result;
    }
    if (cursor.data_offset == oi.size) {
      cursor.data_complete = true;
      dout(20) << " got data" << dendl;
    }
    assert(cursor.data_offset <= oi.size);
  }

  // omap
  std::map<std::string,bufferlist>& out_omap = reply_obj.omap;
  if (pool.info.require_rollback()) {
    cursor.omap_complete = true;
  } else {
    if (left > 0 && !cursor.omap_complete) {
      assert(cursor.data_complete);
      if (cursor.omap_offset.empty()) {
	osd->store->omap_get_header(coll, oi.soid, &reply_obj.omap_header);
      }
      ObjectMap::ObjectMapIterator iter =
	osd->store->get_omap_iterator(coll, oi.soid);
      assert(iter);
      iter->upper_bound(cursor.omap_offset);
      for (; iter->valid(); iter->next()) {
	out_omap.insert(make_pair(iter->key(), iter->value()));
	left -= iter->key().length() + 4 + iter->value().length() + 4;
	if (left <= 0)
	  break;
      }
      if (iter->valid()) {
	cursor.omap_offset = iter->key();
      } else {
	cursor.omap_complete = true;
	dout(20) << " got omap" << dendl;
      }
    }
  }

  dout(20) << " cursor.is_complete=" << cursor.is_complete()
	   << " " << out_attrs.size() << " attrs"
	   << " " << bl.length() << " bytes"
	   << " " << reply_obj.omap_header.length() << " omap header bytes"
	   << " " << out_omap.size() << " keys"
	   << dendl;
  reply_obj.cursor = cursor;
  if (!async_read_started) {
    if (classic) {
      reply_obj.encode_classic(osd_op.outdata);
    } else {
      ::encode(reply_obj, osd_op.outdata);
    }
  }
  if (cb && !async_read_started) {
    delete cb;
  }
  result = 0;
  return result;
}

void ReplicatedPG::start_copy(CopyCallback *cb, ObjectContextRef obc,
			      hobject_t src, object_locator_t oloc,
			      version_t version, unsigned flags,
			      bool mirror_snapset)
{
  const hobject_t& dest = obc->obs.oi.soid;
  dout(10) << __func__ << " " << dest
	   << " from " << src << " " << oloc << " v" << version
	   << " flags " << flags
	   << (mirror_snapset ? " mirror_snapset" : "")
	   << dendl;

  assert(!mirror_snapset || (src.snap == CEPH_NOSNAP ||
			     src.snap == CEPH_SNAPDIR));

  // cancel a previous in-progress copy?
  if (copy_ops.count(dest)) {
    // FIXME: if the src etc match, we could avoid restarting from the
    // beginning.
    CopyOpRef cop = copy_ops[dest];
    cancel_copy(cop, false);
  }

  CopyOpRef cop(new CopyOp(cb, obc, src, oloc, version, flags,
			   mirror_snapset));
  copy_ops[dest] = cop;
  obc->start_block();

  _copy_some(obc, cop);
}

void ReplicatedPG::_copy_some(ObjectContextRef obc, CopyOpRef cop)
{
  dout(10) << __func__ << " " << obc << " " << cop << dendl;

  unsigned flags = 0;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_FLUSH)
    flags |= CEPH_OSD_FLAG_FLUSH;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE)
    flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY)
    flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE)
    flags |= CEPH_OSD_FLAG_MAP_SNAP_CLONE;

  C_GatherBuilder gather(g_ceph_context);

  if (cop->cursor.is_initial() && cop->mirror_snapset) {
    // list snaps too.
    assert(cop->src.snap == CEPH_NOSNAP);
    ObjectOperation op;
    op.list_snaps(&cop->results.snapset, NULL);
    osd->objecter_lock.Lock();
    ceph_tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				    CEPH_SNAPDIR, NULL,
				    flags, gather.new_sub(), NULL);
    cop->objecter_tid2 = tid;
    osd->objecter_lock.Unlock();
  }

  ObjectOperation op;
  if (cop->results.user_version) {
    op.assert_version(cop->results.user_version);
  } else {
    // we should learn the version after the first chunk, if we didn't know
    // it already!
    assert(cop->cursor.is_initial());
  }
  op.copy_get(&cop->cursor, get_copy_chunk_size(),
	      &cop->results.object_size, &cop->results.mtime,
	      &cop->results.category,
	      &cop->attrs, &cop->data, &cop->omap_header, &cop->omap,
	      &cop->results.snaps, &cop->results.snap_seq,
	      &cop->rval);

  C_Copyfrom *fin = new C_Copyfrom(this, obc->obs.oi.soid,
				   get_last_peering_reset(), cop);
  gather.set_finisher(new C_OnFinisher(fin,
				       &osd->objecter_finisher));

  osd->objecter_lock.Lock();
  ceph_tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				  cop->src.snap, NULL,
				  flags,
				  gather.new_sub(),
				  // discover the object version if we don't know it yet
				  cop->results.user_version ? NULL : &cop->results.user_version);
  fin->tid = tid;
  cop->objecter_tid = tid;
  gather.activate();
  osd->objecter_lock.Unlock();
}

void ReplicatedPG::process_copy_chunk(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.find(oid);
  if (p == copy_ops.end()) {
    dout(10) << __func__ << " no copy_op found" << dendl;
    return;
  }
  CopyOpRef cop = p->second;
  if (tid != cop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != cop " << cop
	     << " tid " << cop->objecter_tid << dendl;
    return;
  }

  if (cop->omap.size())
    cop->results.has_omap = true;

  if (r >= 0 && pool.info.require_rollback() && cop->omap.size()) {
    r = -EOPNOTSUPP;
  }
  cop->objecter_tid = 0;
  cop->objecter_tid2 = 0;  // assume this ordered before us (if it happened)
  ObjectContextRef& cobc = cop->obc;

  if (r < 0)
    goto out;

  assert(cop->rval >= 0);

  if (oid.snap < CEPH_NOSNAP && !cop->results.snaps.empty()) {
    // verify snap hasn't been deleted
    vector<snapid_t>::iterator p = cop->results.snaps.begin();
    while (p != cop->results.snaps.end()) {
      if (pool.info.is_removed_snap(*p)) {
	dout(10) << __func__ << " clone snap " << *p << " has been deleted"
		 << dendl;
	for (vector<snapid_t>::iterator q = p + 1;
	     q != cop->results.snaps.end();
	     ++q)
	  *(q - 1) = *q;
	cop->results.snaps.resize(cop->results.snaps.size() - 1);
      } else {
	++p;
      }
    }
    if (cop->results.snaps.empty()) {
      dout(10) << __func__ << " no more snaps for " << oid << dendl;
      r = -ENOENT;
      goto out;
    }
  }

  assert(cop->rval >= 0);

  if (!cop->cursor.is_complete()) {
    // write out what we have so far
    if (cop->temp_cursor.is_initial()) {
      assert(!cop->results.started_temp_obj);
      cop->results.started_temp_obj = true;
      cop->results.temp_oid = generate_temp_object();
      dout(20) << __func__ << " using temp " << cop->results.temp_oid << dendl;
    }
    ObjectContextRef tempobc = get_object_context(cop->results.temp_oid, true);
    RepGather *repop = simple_repop_create(tempobc);
    if (cop->temp_cursor.is_initial()) {
      repop->ctx->new_temp_oid = cop->results.temp_oid;
    }
    _write_copy_chunk(cop, repop->ctx->op_t);
    simple_repop_submit(repop);
    dout(10) << __func__ << " fetching more" << dendl;
    _copy_some(cobc, cop);
    return;
  }

  dout(20) << __func__ << " success; committing" << dendl;
  cop->results.final_tx = pgbackend->get_transaction();
  _build_finish_copy_transaction(cop, cop->results.final_tx);

 out:
  dout(20) << __func__ << " complete r = " << cpp_strerror(r) << dendl;
  CopyCallbackResults results(r, &cop->results);
  cop->cb->complete(results);

  copy_ops.erase(cobc->obs.oi.soid);
  cobc->stop_block();
  kick_object_context_blocked(cobc);
}

void ReplicatedPG::_write_copy_chunk(CopyOpRef cop, PGBackend::PGTransaction *t)
{
  dout(20) << __func__ << " " << cop
	   << " " << cop->attrs.size() << " attrs"
	   << " " << cop->data.length() << " bytes"
	   << " " << cop->omap.size() << " keys"
	   << dendl;
  if (!cop->temp_cursor.attr_complete) {
    t->touch(cop->results.temp_oid);
    for (map<string,bufferlist>::iterator p = cop->attrs.begin();
	 p != cop->attrs.end();
	 ++p) {
      cop->results.attrs[string("_") + p->first] = p->second;
      t->setattr(
	cop->results.temp_oid,
	string("_") + p->first, p->second);
    }
    cop->attrs.clear();
  }
  if (!cop->temp_cursor.data_complete) {
    assert(cop->data.length() + cop->temp_cursor.data_offset ==
	   cop->cursor.data_offset);
    if (pool.info.requires_aligned_append() &&
	!cop->cursor.data_complete) {
      /**
       * Trim off the unaligned bit at the end, we'll adjust cursor.data_offset
       * to pick it up on the next pass.
       */
      assert(cop->temp_cursor.data_offset %
	     pool.info.required_alignment() == 0);
      if (cop->data.length() % pool.info.required_alignment() != 0) {
	uint64_t to_trim =
	  cop->data.length() % pool.info.required_alignment();
	bufferlist bl;
	bl.substr_of(cop->data, 0, cop->data.length() - to_trim);
	cop->data.swap(bl);
	cop->cursor.data_offset -= to_trim;
	assert(cop->data.length() + cop->temp_cursor.data_offset ==
	       cop->cursor.data_offset);
      }
    }
    t->append(
      cop->results.temp_oid,
      cop->temp_cursor.data_offset,
      cop->data.length(),
      cop->data);
    cop->data.clear();
  }
  if (!pool.info.require_rollback()) {
    if (!cop->temp_cursor.omap_complete) {
      if (cop->omap_header.length()) {
	t->omap_setheader(
	  cop->results.temp_oid,
	  cop->omap_header);
	cop->omap_header.clear();
      }
      t->omap_setkeys(cop->results.temp_oid, cop->omap);
      cop->omap.clear();
    }
  } else {
    assert(cop->omap_header.length() == 0);
    assert(cop->omap.empty());
  }
  cop->temp_cursor = cop->cursor;
}

void ReplicatedPG::_build_finish_copy_transaction(CopyOpRef cop,
                                                  PGBackend::PGTransaction* t)
{
  ObjectState& obs = cop->obc->obs;
  if (cop->temp_cursor.is_initial()) {
    // write directly to final object
    cop->results.temp_oid = obs.oi.soid;
    _write_copy_chunk(cop, t);
  } else {
    // finish writing to temp object, then move into place
    _write_copy_chunk(cop, t);
    t->rename(cop->results.temp_oid, obs.oi.soid);
  }
}

void ReplicatedPG::finish_copyfrom(OpContext *ctx)
{
  dout(20) << "finish_copyfrom on " << ctx->obs->oi.soid << dendl;
  ObjectState& obs = ctx->new_obs;
  CopyFromCallback *cb = static_cast<CopyFromCallback*>(ctx->copy_cb);

  if (pool.info.require_rollback()) {
    if (obs.exists) {
      if (ctx->mod_desc.rmobject(ctx->at_version.version)) {
	ctx->op_t->stash(obs.oi.soid, ctx->at_version.version);
      } else {
	ctx->op_t->remove(obs.oi.soid);
      }
    }
    ctx->mod_desc.create();
    replace_cached_attrs(ctx, ctx->obc, cb->results->attrs);
  } else {
    if (obs.exists) {
      ctx->op_t->remove(obs.oi.soid);
    }
    ctx->mod_desc.mark_unrollbackable();
  }

  if (!obs.exists) {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
  }
  if (cb->is_temp_obj_used()) {
    ctx->discard_temp_oid = cb->results->temp_oid;
  }
  ctx->op_t->append(cb->results->final_tx);
  delete cb->results->final_tx;
  cb->results->final_tx = NULL;

  // CopyFromCallback fills this in for us
  obs.oi.user_version = ctx->user_at_version;

  // cache: clear whiteout?
  if (obs.oi.is_whiteout()) {
    dout(10) << __func__ << " clearing whiteout on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    --ctx->delta_stats.num_whiteouts;
  }

  if (cb->results->has_omap) {
    dout(10) << __func__ << " setting omap flag on " << obs.oi.soid << dendl;
    obs.oi.set_flag(object_info_t::FLAG_OMAP);
  } else {
    dout(10) << __func__ << " clearing omap flag on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
  }

  interval_set<uint64_t> ch;
  if (obs.oi.size > 0)
    ch.insert(0, obs.oi.size);
  ctx->modified_ranges.union_of(ch);

  if (cb->get_data_size() != obs.oi.size) {
    ctx->delta_stats.num_bytes -= obs.oi.size;
    obs.oi.size = cb->get_data_size();
    ctx->delta_stats.num_bytes += obs.oi.size;
  }
  ctx->delta_stats.num_wr++;
  ctx->delta_stats.num_wr_kb += SHIFT_ROUND_UP(obs.oi.size, 10);

  osd->logger->inc(l_osd_copyfrom);
}

void ReplicatedPG::finish_promote(int r, OpRequestRef op,
				  CopyResults *results, ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  dout(10) << __func__ << " " << soid << " r=" << r
	   << " uv" << results->user_version << dendl;

  if (r == -ECANCELED) {
    return;
  }

  if (r == -ENOENT && results->started_temp_obj) {
    dout(10) << __func__ << " abort; will clean up partial work" << dendl;
    ObjectContextRef tempobc = get_object_context(results->temp_oid, true);
    RepGather *repop = simple_repop_create(tempobc);
    repop->ctx->op_t->remove(results->temp_oid);
    simple_repop_submit(repop);
    results->started_temp_obj = false;
  }

  if (r == -ENOENT && soid.is_snap()) {
    dout(10) << __func__
	     << ": enoent while trying to promote clone, " << soid
	     << " must have been trimmed, removing from snapset"
	     << dendl;
    hobject_t head(soid.get_head());
    ObjectContextRef obc = get_object_context(head, false);
    assert(obc);
    RepGather *repop = simple_repop_create(obc);
    OpContext *tctx = repop->ctx;
    tctx->at_version = get_next_version();
    filter_snapc(tctx->new_snapset.snaps);
    vector<snapid_t> new_clones(tctx->new_snapset.clones.size());
    for (vector<snapid_t>::iterator i = tctx->new_snapset.clones.begin();
	 i != tctx->new_snapset.clones.end();
	 ++i) {
      if (*i != soid.snap)
	new_clones.push_back(*i);
    }
    tctx->new_snapset.clones.swap(new_clones);
    tctx->new_snapset.clone_overlap.erase(soid.snap);
    tctx->new_snapset.clone_size.erase(soid.snap);

    // take RWWRITE lock for duration of our local write.  ignore starvation.
    if (!obc->rwstate.take_write_lock()) {
      assert(0 == "problem!");
    }
    tctx->lock_to_release = OpContext::W_LOCK;
    dout(20) << __func__ << " took lock on obc, " << obc->rwstate << dendl;

    finish_ctx(tctx, pg_log_entry_t::PROMOTE);

    simple_repop_submit(repop);
    return;
  }

  bool whiteout = false;
  if (r == -ENOENT &&
      soid.snap == CEPH_NOSNAP &&
      (pool.info.cache_mode == pg_pool_t::CACHEMODE_WRITEBACK ||
       pool.info.cache_mode == pg_pool_t::CACHEMODE_READONLY)) {
    dout(10) << __func__ << " whiteout " << soid << dendl;
    whiteout = true;
  }

  if (r < 0 && !whiteout) {
    // we need to get rid of the op in the blocked queue
    map<hobject_t,list<OpRequestRef> >::iterator blocked_iter =
      waiting_for_blocked_object.find(soid);
    assert(blocked_iter != waiting_for_blocked_object.end());
    assert(blocked_iter->second.begin()->get() == op.get());
    blocked_iter->second.pop_front();
    if (blocked_iter->second.empty()) {
      waiting_for_blocked_object.erase(blocked_iter);
    }
    osd->reply_op_error(op, r);
    return;
  }

  RepGather *repop = simple_repop_create(obc);
  OpContext *tctx = repop->ctx;
  tctx->at_version = get_next_version();

  ++tctx->delta_stats.num_objects;
  if (soid.snap < CEPH_NOSNAP)
    ++tctx->delta_stats.num_object_clones;
  tctx->new_obs.exists = true;

  if (whiteout) {
    // create a whiteout
    tctx->op_t->touch(soid);
    tctx->new_obs.oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ++tctx->delta_stats.num_whiteouts;
    dout(20) << __func__ << " creating whiteout on " << soid << dendl;
    osd->logger->inc(l_osd_tier_whiteout);
  } else {
    if (results->has_omap) {
      dout(10) << __func__ << " setting omap flag on " << soid << dendl;
      tctx->new_obs.oi.set_flag(object_info_t::FLAG_OMAP);
      ++tctx->delta_stats.num_objects_omap;
    }

    tctx->op_t->append(results->final_tx);
    delete results->final_tx;
    results->final_tx = NULL;
    if (results->started_temp_obj) {
      tctx->discard_temp_oid = results->temp_oid;
    }
    tctx->new_obs.oi.size = results->object_size;
    tctx->new_obs.oi.category = results->category;
    tctx->new_obs.oi.user_version = results->user_version;

    if (soid.snap != CEPH_NOSNAP) {
      if (!results->snaps.empty()) {
	tctx->new_obs.oi.snaps = results->snaps;
      } else {
	// we must have read "snap" content from the head object in
	// the base pool.  use snap_seq to construct what snaps should
	// be for this clone (what is was before we evicted the clean
	// clone from this pool, and what it will be when we flush and
	// the clone eventually happens in the base pool).
	SnapSet& snapset = obc->ssc->snapset;
	vector<snapid_t>::iterator p = snapset.snaps.begin();
	while (p != snapset.snaps.end() && *p > soid.snap)
	  ++p;
	assert(p != snapset.snaps.end());
	do {
	  tctx->new_obs.oi.snaps.push_back(*p);
	  ++p;
	} while (p != snapset.snaps.end() && *p > results->snap_seq);
      }
      dout(20) << __func__ << " snaps " << tctx->new_obs.oi.snaps << dendl;
      assert(!tctx->new_obs.oi.snaps.empty());
      assert(obc->ssc->snapset.clone_size.count(soid.snap));
      assert(obc->ssc->snapset.clone_size[soid.snap] ==
	     results->object_size);
      assert(obc->ssc->snapset.clone_overlap.count(soid.snap));

      tctx->delta_stats.num_bytes += obc->ssc->snapset.get_clone_bytes(soid.snap);
    } else {
      tctx->delta_stats.num_bytes += results->object_size;
    }
  }

  if (results->mirror_snapset) {
    assert(tctx->new_obs.oi.soid.snap == CEPH_NOSNAP);
    tctx->new_snapset.from_snap_set(results->snapset);
  }
  tctx->new_snapset.head_exists = true;
  dout(20) << __func__ << " new_snapset " << tctx->new_snapset << dendl;

  // take RWWRITE lock for duration of our local write.  ignore starvation.
  if (!obc->rwstate.take_write_lock()) {
    assert(0 == "problem!");
  }
  tctx->lock_to_release = OpContext::W_LOCK;
  dout(20) << __func__ << " took lock on obc, " << obc->rwstate << dendl;

  finish_ctx(tctx, pg_log_entry_t::PROMOTE);

  simple_repop_submit(repop);

  osd->logger->inc(l_osd_tier_promote);
}

void ReplicatedPG::cancel_copy(CopyOpRef cop, bool requeue)
{
  dout(10) << __func__ << " " << cop->obc->obs.oi.soid
	   << " from " << cop->src << " " << cop->oloc
	   << " v" << cop->results.user_version << dendl;

  // cancel objecter op, if we can
  if (cop->objecter_tid) {
    Mutex::Locker l(osd->objecter_lock);
    osd->objecter->op_cancel(cop->objecter_tid, -ECANCELED);
    cop->objecter_tid = 0;
    if (cop->objecter_tid2) {
      osd->objecter->op_cancel(cop->objecter_tid2, -ECANCELED);
      cop->objecter_tid2 = 0;
    }
  }

  copy_ops.erase(cop->obc->obs.oi.soid);
  cop->obc->stop_block();

  kick_object_context_blocked(cop->obc);
  cop->results.should_requeue = requeue;
  CopyCallbackResults result(-ECANCELED, &cop->results);
  cop->cb->complete(result);

  // There may still be an objecter callback referencing this copy op.
  // That callback will not need the obc since it's been canceled, and
  // we need the obc reference to go away prior to flush.
  cop->obc = ObjectContextRef();
}

void ReplicatedPG::cancel_copy_ops(bool requeue)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.begin();
  while (p != copy_ops.end()) {
    // requeue this op? can I queue up all of them?
    cancel_copy((p++)->second, requeue);
  }
}


// ========================================================================
// flush
//
// Flush a dirty object in the cache tier by writing it back to the
// base tier.  The sequence looks like:
//
//  * send a copy-from operation to the base tier to copy the current
//    version of the object
//  * base tier will pull the object via (perhaps multiple) copy-get(s)
//  * on completion, we check if the object has been modified.  if so,
//    just reply with -EAGAIN.
//  * try to take a write lock so we can clear the dirty flag.  if this
//    fails, wait and retry
//  * start a repop that clears the bit.
//
// If we have to wait, we will retry by coming back through the
// start_flush method.  We check if a flush is already in progress
// and, if so, try to finish it by rechecking the version and trying
// to clear the dirty bit.
//
// In order for the cache-flush (a write op) to not block the copy-get
// from reading the object, the client *must* set the SKIPRWLOCKS
// flag.
//
// NOTE: normally writes are strictly ordered for the client, but
// flushes are special in that they can be reordered with respect to
// other writes.  In particular, we can't have a flush request block
// an update to the cache pool object!

struct C_Flush : public Context {
  ReplicatedPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  C_Flush(ReplicatedPG *p, hobject_t o, epoch_t lpr)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0)
  {}
  void finish(int r) {
    if (r == -ECANCELED)
      return;
    pg->lock();
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_flush(oid, tid, r);
    }
    pg->unlock();
  }
};

int ReplicatedPG::start_flush(
  OpRequestRef op, ObjectContextRef obc,
  bool blocking, hobject_t *pmissing,
  Context *on_flush)
{
  const object_info_t& oi = obc->obs.oi;
  const hobject_t& soid = oi.soid;
  dout(10) << __func__ << " " << soid
	   << " v" << oi.version
	   << " uv" << oi.user_version
	   << " " << (blocking ? "blocking" : "non-blocking/best-effort")
	   << dendl;

  // verify there are no (older) check for dirty clones
  SnapSet& snapset = obc->ssc->snapset;
  {
    dout(20) << " snapset " << snapset << dendl;
    vector<snapid_t>::reverse_iterator p = snapset.clones.rbegin();
    while (p != snapset.clones.rend() && *p >= soid.snap)
      ++p;
    if (p != snapset.clones.rend()) {
      hobject_t next = soid;
      next.snap = *p;
      assert(next.snap < soid.snap);
      if (pg_log.get_missing().is_missing(next)) {
	dout(10) << __func__ << " missing clone is " << next << dendl;
	if (pmissing)
	  *pmissing = next;
	return -ENOENT;
      }
      ObjectContextRef older_obc = get_object_context(next, false);
      if (older_obc) {
	dout(20) << __func__ << " next oldest clone is " << older_obc->obs.oi
		 << dendl;
	if (older_obc->obs.oi.is_dirty()) {
	  dout(10) << __func__ << " next oldest clone is dirty: "
		   << older_obc->obs.oi << dendl;
	  return -EBUSY;
	}
      } else {
	dout(20) << __func__ << " next oldest clone " << next
		 << " is not present; implicitly clean" << dendl;
      }
    } else {
      dout(20) << __func__ << " no older clones" << dendl;
    }
  }

  if (blocking)
    obc->start_block();

  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(soid);
  if (p != flush_ops.end()) {
    FlushOpRef fop = p->second;
    if (fop->op == op) {
      // we couldn't take the write lock on a cache-try-flush before;
      // now we are trying again for the lock.
      return try_flush_mark_clean(fop);
    }
    if (fop->flushed_version == obc->obs.oi.user_version &&
	(fop->blocking || !blocking)) {
      // nonblocking can join anything
      // blocking can only join a blocking flush
      dout(20) << __func__ << " piggybacking on existing flush " << dendl;
      if (op)
	fop->dup_ops.push_back(op);
      return -EAGAIN;   // clean up this ctx; op will retry later
    }

    // cancel current flush since it will fail anyway, or because we
    // are blocking and the existing flush is nonblocking.
    dout(20) << __func__ << " canceling previous flush; it will fail" << dendl;
    if (fop->op)
      osd->reply_op_error(fop->op, -EBUSY);
    while (!fop->dup_ops.empty()) {
      osd->reply_op_error(fop->dup_ops.front(), -EBUSY);
      fop->dup_ops.pop_front();
    }
    cancel_flush(fop, false);
  }

  /**
   * In general, we need to send two deletes and a copyfrom.
   * Consider snapc 10:[10, 9, 8, 4, 3, 2]:[10(10, 9), 4(4,3,2)]
   * where 4 is marked as clean.  To flush 10, we have to:
   * 1) delete 4:[4,3,2] -- ensure head is created at cloneid 4
   * 2) delete (8-1):[4,3,2] -- ensure that the object does not exist at 8
   * 3) copyfrom 8:[8,4,3,2] -- flush object excluding snap 8
   *
   * The second delete is required in case at some point in the past
   * there had been a clone 7(7,6), which we had flushed.  Without
   * the second delete, the object would appear in the base pool to
   * have existed.
   */

  SnapContext snapc, dsnapc, dsnapc2;
  if (snapset.seq != 0) {
    if (soid.snap == CEPH_NOSNAP) {
      snapc.seq = snapset.seq;
      snapc.snaps = snapset.snaps;
    } else {
      snapid_t min_included_snap = oi.snaps.back();
      snapc = snapset.get_ssc_as_of(min_included_snap - 1);
    }

    snapid_t prev_snapc = 0;
    for (vector<snapid_t>::reverse_iterator citer = snapset.clones.rbegin();
	 citer != snapset.clones.rend();
	 ++citer) {
      if (*citer < soid.snap) {
	prev_snapc = *citer;
	break;
      }
    }

    if (prev_snapc != snapc.seq) {
      dsnapc = snapset.get_ssc_as_of(prev_snapc);
      snapid_t first_snap_after_prev_snapc =
	snapset.get_first_snap_after(prev_snapc, snapc.seq);
      dsnapc2 = snapset.get_ssc_as_of(
	first_snap_after_prev_snapc - 1);
    }
  }

  object_locator_t base_oloc(soid);
  base_oloc.pool = pool.info.tier_of;

  if (dsnapc.seq > 0 && dsnapc.seq < snapc.seq) {
    ObjectOperation o;
    o.remove();
    osd->objecter_lock.Lock();
    osd->objecter->mutate(
      soid.oid,
      base_oloc,
      o,
      dsnapc,
      oi.mtime,
      (CEPH_OSD_FLAG_IGNORE_OVERLAY |
       CEPH_OSD_FLAG_ORDERSNAP |
       CEPH_OSD_FLAG_ENFORCE_SNAPC),
      NULL,
      NULL /* no callback, we'll rely on the ordering w.r.t the next op */);
    osd->objecter_lock.Unlock();
  }

  if (dsnapc2.seq > dsnapc.seq && dsnapc2.seq < snapc.seq) {
    ObjectOperation o;
    o.remove();
    osd->objecter_lock.Lock();
    osd->objecter->mutate(
      soid.oid,
      base_oloc,
      o,
      dsnapc2,
      oi.mtime,
      (CEPH_OSD_FLAG_IGNORE_OVERLAY |
       CEPH_OSD_FLAG_ORDERSNAP |
       CEPH_OSD_FLAG_ENFORCE_SNAPC),
      NULL,
      NULL /* no callback, we'll rely on the ordering w.r.t the next op */);
    osd->objecter_lock.Unlock();
  }

  FlushOpRef fop(new FlushOp);
  fop->obc = obc;
  fop->flushed_version = oi.user_version;
  fop->blocking = blocking;
  fop->on_flush = on_flush;
  fop->op = op;

  ObjectOperation o;
  if (oi.is_whiteout()) {
    fop->removal = true;
    o.remove();
  } else {
    object_locator_t oloc(soid);
    o.copy_from(soid.oid.name, soid.snap, oloc, oi.user_version,
		CEPH_OSD_COPY_FROM_FLAG_FLUSH |
		CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
		CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
		CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE);
  }
  C_Flush *fin = new C_Flush(this, soid, get_last_peering_reset());

  osd->objecter_lock.Lock();
  ceph_tid_t tid = osd->objecter->mutate(
    soid.oid, base_oloc, o, snapc, oi.mtime,
    CEPH_OSD_FLAG_IGNORE_OVERLAY | CEPH_OSD_FLAG_ENFORCE_SNAPC,
    NULL,
    new C_OnFinisher(fin,
		     &osd->objecter_finisher));
  fin->tid = tid;
  fop->objecter_tid = tid;
  osd->objecter_lock.Unlock();

  flush_ops[soid] = fop;
  return -EINPROGRESS;
}

void ReplicatedPG::finish_flush(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(oid);
  if (p == flush_ops.end()) {
    dout(10) << __func__ << " no flush_op found" << dendl;
    return;
  }
  FlushOpRef fop = p->second;
  if (tid != fop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != fop " << fop
	     << " tid " << fop->objecter_tid << dendl;
    return;
  }
  ObjectContextRef obc = fop->obc;
  fop->objecter_tid = 0;

  if (r < 0 && !(r == -ENOENT && fop->removal)) {
    if (fop->op)
      osd->reply_op_error(fop->op, -EBUSY);
    if (!fop->dup_ops.empty()) {
      dout(20) << __func__ << " requeueing dups" << dendl;
      requeue_ops(fop->dup_ops);
    }
    if (fop->on_flush) {
      Context *on_flush = fop->on_flush;
      fop->on_flush = NULL;
      on_flush->complete(-EBUSY);
    }
    flush_ops.erase(oid);
    return;
  }

  r = try_flush_mark_clean(fop);
  if (r == -EBUSY && fop->op) {
    osd->reply_op_error(fop->op, r);
  }
}

int ReplicatedPG::try_flush_mark_clean(FlushOpRef fop)
{
  ObjectContextRef obc = fop->obc;
  const hobject_t& oid = obc->obs.oi.soid;

  if (fop->blocking) {
    obc->stop_block();
    kick_object_context_blocked(obc);
  }

  if (fop->flushed_version != obc->obs.oi.user_version ||
      !obc->obs.exists) {
    if (obc->obs.exists)
      dout(10) << __func__ << " flushed_version " << fop->flushed_version
	       << " != current " << obc->obs.oi.user_version
	       << dendl;
    else
      dout(10) << __func__ << " object no longer exists" << dendl;

    if (!fop->dup_ops.empty()) {
      dout(20) << __func__ << " requeueing dups" << dendl;
      requeue_ops(fop->dup_ops);
    }
    if (fop->on_flush) {
      Context *on_flush = fop->on_flush;
      fop->on_flush = NULL;
      on_flush->complete(-EBUSY);
    }
    flush_ops.erase(oid);
    if (fop->blocking)
      osd->logger->inc(l_osd_tier_flush_fail);
    else
      osd->logger->inc(l_osd_tier_try_flush_fail);
    return -EBUSY;
  }

  if (!fop->blocking && scrubber.write_blocked_by_scrub(oid)) {
    if (fop->op) {
      dout(10) << __func__ << " blocked by scrub" << dendl;
      requeue_op(fop->op);
      requeue_ops(fop->dup_ops);
      return -EAGAIN;    // will retry
    } else {
      osd->logger->inc(l_osd_tier_try_flush_fail);
      cancel_flush(fop, false);
      return -ECANCELED;
    }
  }

  // successfully flushed; can we clear the dirty bit?
  // try to take the lock manually, since we don't
  // have a ctx yet.
  if (obc->get_write(fop->op)) {
    dout(20) << __func__ << " took write lock" << dendl;
  } else if (fop->op) {
    dout(10) << __func__ << " waiting on write lock" << dendl;
    requeue_op(fop->op);
    requeue_ops(fop->dup_ops);
    return -EAGAIN;    // will retry
  } else {
    dout(10) << __func__ << " failed write lock, no op; failing" << dendl;
    osd->logger->inc(l_osd_tier_try_flush_fail);
    cancel_flush(fop, false);
    return -ECANCELED;
  }

  dout(10) << __func__ << " clearing DIRTY flag for " << oid << dendl;
  RepGather *repop = simple_repop_create(fop->obc);
  OpContext *ctx = repop->ctx;

  ctx->on_finish = fop->on_flush;
  fop->on_flush = NULL;

  ctx->lock_to_release = OpContext::W_LOCK;  // we took it above
  ctx->at_version = get_next_version();

  ctx->new_obs = obc->obs;
  ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
  --ctx->delta_stats.num_objects_dirty;

  finish_ctx(ctx, pg_log_entry_t::CLEAN);

  osd->logger->inc(l_osd_tier_clean);

  if (!fop->dup_ops.empty() || fop->op) {
    dout(20) << __func__ << " requeueing for " << ctx->at_version << dendl;
    list<OpRequestRef> ls;
    if (fop->op)
      ls.push_back(fop->op);
    ls.splice(ls.end(), fop->dup_ops);
    requeue_ops(ls);
  }

  simple_repop_submit(repop);

  flush_ops.erase(oid);

  if (fop->blocking)
    osd->logger->inc(l_osd_tier_flush);
  else
    osd->logger->inc(l_osd_tier_try_flush);

  return -EINPROGRESS;
}

void ReplicatedPG::cancel_flush(FlushOpRef fop, bool requeue)
{
  dout(10) << __func__ << " " << fop->obc->obs.oi.soid << " tid "
	   << fop->objecter_tid << dendl;
  if (fop->objecter_tid) {
    Mutex::Locker l(osd->objecter_lock);
    osd->objecter->op_cancel(fop->objecter_tid, -ECANCELED);
    fop->objecter_tid = 0;
  }
  if (fop->blocking) {
    fop->obc->stop_block();
    kick_object_context_blocked(fop->obc);
  }
  if (requeue) {
    if (fop->op)
      requeue_op(fop->op);
    requeue_ops(fop->dup_ops);
  }
  if (fop->on_flush) {
    Context *on_flush = fop->on_flush;
    fop->on_flush = NULL;
    on_flush->complete(-ECANCELED);
  }
  flush_ops.erase(fop->obc->obs.oi.soid);
}

void ReplicatedPG::cancel_flush_ops(bool requeue)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.begin();
  while (p != flush_ops.end()) {
    cancel_flush((p++)->second, requeue);
  }
}

bool ReplicatedPG::is_present_clone(hobject_t coid)
{
  if (!pool.info.allow_incomplete_clones())
    return true;
  if (is_missing_object(coid))
    return true;
  ObjectContextRef obc = get_object_context(coid, false);
  return obc && obc->obs.exists;
}

// ========================================================================
// rep op gather

class C_OSD_RepopApplied : public Context {
  ReplicatedPGRef pg;
  boost::intrusive_ptr<ReplicatedPG::RepGather> repop;
public:
  C_OSD_RepopApplied(ReplicatedPG *pg, ReplicatedPG::RepGather *repop)
  : pg(pg), repop(repop) {}
  void finish(int) {
    pg->repop_all_applied(repop.get());
  }
};


void ReplicatedPG::repop_all_applied(RepGather *repop)
{
  dout(10) << __func__ << ": repop tid " << repop->rep_tid << " all applied "
	   << dendl;
  repop->all_applied = true;
  if (!repop->rep_aborted) {
    eval_repop(repop);
    if (repop->on_applied) {
     repop->on_applied->complete(0);
     repop->on_applied = NULL;
    }
  }
}

class C_OSD_RepopCommit : public Context {
  ReplicatedPGRef pg;
  boost::intrusive_ptr<ReplicatedPG::RepGather> repop;
public:
  C_OSD_RepopCommit(ReplicatedPG *pg, ReplicatedPG::RepGather *repop)
    : pg(pg), repop(repop) {}
  void finish(int) {
    pg->repop_all_committed(repop.get());
  }
};

void ReplicatedPG::repop_all_committed(RepGather *repop)
{
  dout(10) << __func__ << ": repop tid " << repop->rep_tid << " all committed "
	   << dendl;
  repop->all_committed = true;

  if (!repop->rep_aborted) {
    if (repop->v != eversion_t()) {
      last_update_ondisk = repop->v;
      last_complete_ondisk = repop->pg_local_last_complete;
    }
    eval_repop(repop);
  }
}

void ReplicatedPG::op_applied(const eversion_t &applied_version)
{
  dout(10) << "op_applied on primary on version " << applied_version << dendl;
  if (applied_version == eversion_t())
    return;
  assert(applied_version > last_update_applied);
  assert(applied_version <= info.last_update);
  last_update_applied = applied_version;
  if (is_primary()) {
    if (scrubber.active && scrubber.is_chunky) {
      if (last_update_applied == scrubber.subset_last_update) {
        osd->scrub_wq.queue(this);
      }
    } else if (last_update_applied == info.last_update && scrubber.block_writes) {
      dout(10) << "requeueing scrub for cleanup" << dendl;
      scrubber.finalizing = true;
      scrub_gather_replica_maps();
      ++scrubber.waiting_on;
      scrubber.waiting_on_whom.insert(pg_whoami);
      osd->scrub_wq.queue(this);
    }
  } else {
    dout(10) << "op_applied on replica on version " << applied_version << dendl;
    if (scrubber.active_rep_scrub) {
      if (last_update_applied == scrubber.active_rep_scrub->scrub_to) {
	osd->rep_scrub_wq.queue(scrubber.active_rep_scrub);
	scrubber.active_rep_scrub = 0;
      }
    }
  }
}

void ReplicatedPG::eval_repop(RepGather *repop)
{
  MOSDOp *m = NULL;
  if (repop->ctx->op)
    m = static_cast<MOSDOp *>(repop->ctx->op->get_req());

  if (m)
    dout(10) << "eval_repop " << *repop
	     << " wants=" << (m->wants_ack() ? "a":"") << (m->wants_ondisk() ? "d":"")
	     << (repop->rep_done ? " DONE" : "")
	     << dendl;
  else
    dout(10) << "eval_repop " << *repop << " (no op)"
	     << (repop->rep_done ? " DONE" : "")
	     << dendl;

  if (repop->rep_done)
    return;

  if (m) {

    // an 'ondisk' reply implies 'ack'. so, prefer to send just one
    // ondisk instead of ack followed by ondisk.

    // ondisk?
    if (repop->all_committed) {

      log_op_stats(repop->ctx);
      publish_stats_to_osd();

      // send dup commits, in order
      if (waiting_for_ondisk.count(repop->v)) {
	assert(waiting_for_ondisk.begin()->first == repop->v);
	for (list<OpRequestRef>::iterator i = waiting_for_ondisk[repop->v].begin();
	     i != waiting_for_ondisk[repop->v].end();
	     ++i) {
	  osd->reply_op_error(*i, 0, repop->ctx->at_version,
			      repop->ctx->user_at_version);
	}
	waiting_for_ondisk.erase(repop->v);
      }

      // clear out acks, we sent the commits above
      if (waiting_for_ack.count(repop->v)) {
	assert(waiting_for_ack.begin()->first == repop->v);
	waiting_for_ack.erase(repop->v);
      }

      if (m->wants_ondisk() && !repop->sent_disk) {
	// send commit.
	MOSDOpReply *reply = repop->ctx->reply;
	if (reply)
	  repop->ctx->reply = NULL;
	else {
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0, true);
	  reply->set_reply_versions(repop->ctx->at_version,
	                            repop->ctx->user_at_version);
	}
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending commit on " << *repop << " " << reply << dendl;
	osd->send_message_osd_client(reply, m->get_connection());
	repop->sent_disk = true;
	repop->ctx->op->mark_commit_sent();
      }
    }

    // applied?
    if (repop->all_applied) {

      // send dup acks, in order
      if (waiting_for_ack.count(repop->v)) {
	assert(waiting_for_ack.begin()->first == repop->v);
	for (list<OpRequestRef>::iterator i = waiting_for_ack[repop->v].begin();
	     i != waiting_for_ack[repop->v].end();
	     ++i) {
	  MOSDOp *m = (MOSDOp*)(*i)->get_req();
	  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0, true);
	  reply->set_reply_versions(repop->ctx->at_version,
	                            repop->ctx->user_at_version);
	  reply->add_flags(CEPH_OSD_FLAG_ACK);
	  osd->send_message_osd_client(reply, m->get_connection());
	}
	waiting_for_ack.erase(repop->v);
      }

      if (m->wants_ack() && !repop->sent_ack && !repop->sent_disk) {
	// send ack
	MOSDOpReply *reply = repop->ctx->reply;
	if (reply)
	  repop->ctx->reply = NULL;
	else {
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0, true);
	  reply->set_reply_versions(repop->ctx->at_version,
	                            repop->ctx->user_at_version);
	}
	reply->add_flags(CEPH_OSD_FLAG_ACK);
	dout(10) << " sending ack on " << *repop << " " << reply << dendl;
        assert(entity_name_t::TYPE_OSD != m->get_connection()->peer_type);
	osd->send_message_osd_client(reply, m->get_connection());
	repop->sent_ack = true;
      }

      // note the write is now readable (for rlatency calc).  note
      // that this will only be defined if the write is readable
      // _prior_ to being committed; it will not get set with
      // writeahead journaling, for instance.
      if (repop->ctx->readable_stamp == utime_t())
	repop->ctx->readable_stamp = ceph_clock_now(cct);
    }
  }

  // done.
  if (repop->all_applied && repop->all_committed) {
    repop->rep_done = true;

    release_op_ctx_locks(repop->ctx);

    calc_min_last_complete_ondisk();

    // kick snap_trimmer if necessary
    if (repop->queue_snap_trimmer) {
      queue_snap_trim();
    }

    dout(10) << " removing " << *repop << dendl;
    assert(!repop_queue.empty());
    dout(20) << "   q front is " << *repop_queue.front() << dendl; 
    if (repop_queue.front() != repop) {
      dout(0) << " removing " << *repop << dendl;
      dout(0) << "   q front is " << *repop_queue.front() << dendl; 
      assert(repop_queue.front() == repop);
    }
    repop_queue.pop_front();
    remove_repop(repop);
  }
}

void ReplicatedPG::issue_repop(RepGather *repop, utime_t now)
{
  OpContext *ctx = repop->ctx;
  const hobject_t& soid = ctx->obs->oi.soid;
  if (ctx->op &&
    ((static_cast<MOSDOp *>(
	ctx->op->get_req()))->get_flags() & CEPH_OSD_FLAG_PARALLELEXEC)) {
    // replicate original op for parallel execution on replica
    assert(0 == "broken implementation, do not use");
  }
  dout(7) << "issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << dendl;

  repop->v = ctx->at_version;
  if (ctx->at_version > eversion_t()) {
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      pg_info_t &pinfo = peer_info[*i];
      // keep peer_info up to date
      if (pinfo.last_complete == pinfo.last_update)
	pinfo.last_complete = ctx->at_version;
      pinfo.last_update = ctx->at_version;
    }
  }

  repop->obc->ondisk_write_lock();
  if (repop->ctx->clone_obc)
    repop->ctx->clone_obc->ondisk_write_lock();

  bool unlock_snapset_obc = false;
  if (repop->ctx->snapset_obc && repop->ctx->snapset_obc->obs.oi.soid !=
      repop->obc->obs.oi.soid) {
    repop->ctx->snapset_obc->ondisk_write_lock();
    unlock_snapset_obc = true;
  }

  repop->ctx->apply_pending_attrs();

  if (pool.info.require_rollback()) {
    for (vector<pg_log_entry_t>::iterator i = repop->ctx->log.begin();
	 i != repop->ctx->log.end();
	 ++i) {
      assert(i->mod_desc.can_rollback());
      assert(!i->mod_desc.empty());
    }
  }

  Context *on_all_commit = new C_OSD_RepopCommit(this, repop);
  Context *on_all_applied = new C_OSD_RepopApplied(this, repop);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(
    repop->obc,
    repop->ctx->clone_obc,
    unlock_snapset_obc ? repop->ctx->snapset_obc : ObjectContextRef());
  pgbackend->submit_transaction(
    soid,
    repop->ctx->at_version,
    repop->ctx->op_t,
    pg_trim_to,
    min_last_complete_ondisk,
    repop->ctx->log,
    repop->ctx->updated_hset_history,
    onapplied_sync,
    on_all_applied,
    on_all_commit,
    repop->rep_tid,
    repop->ctx->reqid,
    repop->ctx->op);
  repop->ctx->op_t = NULL;
}
    
void ReplicatedBackend::issue_op(
  const hobject_t &soid,
  const eversion_t &at_version,
  ceph_tid_t tid,
  osd_reqid_t reqid,
  eversion_t pg_trim_to,
  eversion_t pg_trim_rollback_to,
  hobject_t new_temp_oid,
  hobject_t discard_temp_oid,
  vector<pg_log_entry_t> &log_entries,
  boost::optional<pg_hit_set_history_t> &hset_hist,
  InProgressOp *op,
  ObjectStore::Transaction *op_t)
{
  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

  if (parent->get_actingbackfill_shards().size() > 1) {
    ostringstream ss;
    set<pg_shard_t> replicas = parent->get_actingbackfill_shards();
    replicas.erase(parent->whoami_shard());
    ss << "waiting for subops from " << replicas;
    if (op->op)
      op->op->mark_sub_op_sent(ss.str());
  }
  for (set<pg_shard_t>::const_iterator i =
	 parent->get_actingbackfill_shards().begin();
       i != parent->get_actingbackfill_shards().end();
       ++i) {
    if (*i == parent->whoami_shard()) continue;
    pg_shard_t peer = *i;
    const pg_info_t &pinfo = parent->get_shard_info().find(peer)->second;

    // forward the write/update/whatever
    MOSDSubOp *wr = new MOSDSubOp(
      reqid, parent->whoami_shard(),
      spg_t(get_info().pgid.pgid, i->shard),
      soid,
      false, acks_wanted,
      get_osdmap()->get_epoch(),
      tid, at_version);

    // ship resulting transaction, log entries, and pg_stats
    if (!parent->should_send_op(peer, soid)) {
      dout(10) << "issue_repop shipping empty opt to osd." << peer
	       <<", object " << soid
	       << " beyond MAX(last_backfill_started "
	       << ", pinfo.last_backfill "
	       << pinfo.last_backfill << ")" << dendl;
      ObjectStore::Transaction t;
      ::encode(t, wr->get_data());
    } else {
      ::encode(*op_t, wr->get_data());
    }

    ::encode(log_entries, wr->logbl);

    if (pinfo.is_incomplete())
      wr->pg_stats = pinfo.stats;  // reflects backfill progress
    else
      wr->pg_stats = get_info().stats;
    
    wr->pg_trim_to = pg_trim_to;
    wr->pg_trim_rollback_to = pg_trim_rollback_to;

    wr->new_temp_oid = new_temp_oid;
    wr->discard_temp_oid = discard_temp_oid;
    wr->updated_hit_set_history = hset_hist;

    get_parent()->send_message_osd_cluster(
      peer.osd, wr, get_osdmap()->get_epoch());
  }
}

ReplicatedPG::RepGather *ReplicatedPG::new_repop(OpContext *ctx, ObjectContextRef obc,
						 ceph_tid_t rep_tid)
{
  if (ctx->op)
    dout(10) << "new_repop rep_tid " << rep_tid << " on " << *ctx->op->get_req() << dendl;
  else
    dout(10) << "new_repop rep_tid " << rep_tid << " (no op)" << dendl;

  RepGather *repop = new RepGather(ctx, obc, rep_tid, info.last_complete);

  repop->start = ceph_clock_now(cct);

  repop_queue.push_back(&repop->queue_item);
  repop_map[repop->rep_tid] = repop;
  repop->get();

  osd->logger->set(l_osd_op_wip, repop_map.size());

  return repop;
}
 
void ReplicatedPG::remove_repop(RepGather *repop)
{
  dout(20) << __func__ << " " << *repop << dendl;
  if (repop->ctx->obc)
    dout(20) << " obc " << *repop->ctx->obc << dendl;
  if (repop->ctx->clone_obc)
    dout(20) << " clone_obc " << *repop->ctx->clone_obc << dendl;
  if (repop->ctx->snapset_obc)
    dout(20) << " snapset_obc " << *repop->ctx->snapset_obc << dendl;
  release_op_ctx_locks(repop->ctx);
  repop->ctx->finish(0);  // FIXME: return value here is sloppy
  repop_map.erase(repop->rep_tid);
  repop->put();

  osd->logger->set(l_osd_op_wip, repop_map.size());
}

ReplicatedPG::RepGather *ReplicatedPG::simple_repop_create(ObjectContextRef obc)
{
  dout(20) << __func__ << " " << obc->obs.oi.soid << dendl;
  vector<OSDOp> ops;
  ceph_tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, obc->ssc, this);
  ctx->op_t = pgbackend->get_transaction();
  ctx->mtime = ceph_clock_now(g_ceph_context);
  ctx->obc = obc;
  RepGather *repop = new_repop(ctx, obc, rep_tid);
  return repop;
}

void ReplicatedPG::simple_repop_submit(RepGather *repop)
{
  dout(20) << __func__ << " " << repop << dendl;
  issue_repop(repop, repop->ctx->mtime);
  eval_repop(repop);
  repop->put();
}

// -------------------------------------------------------

void ReplicatedPG::get_watchers(list<obj_watch_item_t> &pg_watchers)
{
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    get_obc_watchers(obc, pg_watchers);
  }
}

void ReplicatedPG::get_obc_watchers(ObjectContextRef obc, list<obj_watch_item_t> &pg_watchers)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	 obc->watchers.begin();
	j != obc->watchers.end();
	++j) {
    obj_watch_item_t owi;

    owi.obj = obc->obs.oi.soid;
    owi.wi.addr = j->second->get_peer_addr();
    owi.wi.name = j->second->get_entity();
    owi.wi.cookie = j->second->get_cookie();
    owi.wi.timeout_seconds = j->second->get_timeout();

    dout(30) << "watch: Found oid=" << owi.obj << " addr=" << owi.wi.addr
      << " name=" << owi.wi.name << " cookie=" << owi.wi.cookie << dendl;

    pg_watchers.push_back(owi);
  }
}

void ReplicatedPG::check_blacklisted_watchers()
{
  dout(20) << "ReplicatedPG::check_blacklisted_watchers for pg " << get_pgid() << dendl;
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i))
    check_blacklisted_obc_watchers(i.second);
}

void ReplicatedPG::check_blacklisted_obc_watchers(ObjectContextRef obc)
{
  dout(20) << "ReplicatedPG::check_blacklisted_obc_watchers for obc " << obc->obs.oi.soid << dendl;
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator k =
	 obc->watchers.begin();
	k != obc->watchers.end();
	) {
    //Advance iterator now so handle_watch_timeout() can erase element
    map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j = k++;
    dout(30) << "watch: Found " << j->second->get_entity() << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (get_osdmap()->is_blacklisted(ea)) {
      dout(10) << "watch: Found blacklisted watcher for " << ea << dendl;
      assert(j->second->get_pg() == this);
      j->second->unregister_cb();
      handle_watch_timeout(j->second);
    }
  }
}

void ReplicatedPG::populate_obc_watchers(ObjectContextRef obc)
{
  assert(is_active());
  assert((recovering.count(obc->obs.oi.soid) ||
	  !is_missing_object(obc->obs.oi.soid)) ||
	 (pg_log.get_log().objects.count(obc->obs.oi.soid) && // or this is a revert... see recover_primary()
	  pg_log.get_log().objects.find(obc->obs.oi.soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT &&
	  pg_log.get_log().objects.find(obc->obs.oi.soid)->second->reverting_to ==
	    obc->obs.oi.version));

  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	obc->obs.oi.watchers.begin();
       p != obc->obs.oi.watchers.end();
       ++p) {
    utime_t expire = info.stats.last_became_active;
    expire += p->second.timeout_seconds;
    dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
	this, osd, obc, p->second.timeout_seconds, p->first.first,
	p->first.second, p->second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
	make_pair(p->first.first, p->first.second),
	watch));
  }
  // Look for watchers from blacklisted clients and drop
  check_blacklisted_obc_watchers(obc);
}

void ReplicatedPG::handle_watch_timeout(WatchRef watch)
{
  ObjectContextRef obc = watch->get_obc(); // handle_watch_timeout owns this ref
  dout(10) << "handle_watch_timeout obc " << obc << dendl;

  if (is_degraded_object(obc->obs.oi.soid)) {
    callbacks_for_degraded_object[obc->obs.oi.soid].push_back(
      watch->get_delayed_cb()
      );
    dout(10) << "handle_watch_timeout waiting for degraded on obj "
	     << obc->obs.oi.soid
	     << dendl;
    return;
  }

  if (scrubber.write_blocked_by_scrub(obc->obs.oi.soid)) {
    dout(10) << "handle_watch_timeout waiting for scrub on obj "
	     << obc->obs.oi.soid
	     << dendl;
    scrubber.add_callback(
      watch->get_delayed_cb() // This callback!
      );
    return;
  }

  obc->watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  obc->obs.oi.watchers.erase(make_pair(watch->get_cookie(), watch->get_entity()));
  watch->remove();

  vector<OSDOp> ops;
  ceph_tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, obc->ssc, this);
  ctx->op_t = pgbackend->get_transaction();
  ctx->mtime = ceph_clock_now(cct);
  ctx->at_version = get_next_version();

  entity_inst_t nobody;

  RepGather *repop = new_repop(ctx, obc, rep_tid);

  PGBackend::PGTransaction *t = ctx->op_t;

  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, obc->obs.oi.soid,
				    ctx->at_version,
				    obc->obs.oi.version,
				    0,
				    osd_reqid_t(), ctx->mtime));

  obc->obs.oi.prior_version = repop->obc->obs.oi.version;
  obc->obs.oi.version = ctx->at_version;
  bufferlist bl;
  ::encode(obc->obs.oi, bl);
  setattr_maybe_cache(obc, repop->ctx, t, OI_ATTR, bl);

  if (pool.info.require_rollback()) {
    map<string, boost::optional<bufferlist> > to_set;
    to_set[OI_ATTR] = bl;
    ctx->log.back().mod_desc.setattrs(to_set);
  } else {
    ctx->log.back().mod_desc.mark_unrollbackable();
  }

  // obc ref swallowed by repop!
  issue_repop(repop, repop->ctx->mtime);
  eval_repop(repop);
  repop->put();
}

ObjectContextRef ReplicatedPG::create_object_context(const object_info_t& oi,
						     SnapSetContext *ssc)
{
  ObjectContextRef obc(object_contexts.lookup_or_create(oi.soid));
  assert(obc->destructor_callback == NULL);
  obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());  
  obc->obs.oi = oi;
  obc->obs.exists = false;
  obc->ssc = ssc;
  if (ssc)
    register_snapset_context(ssc);
  dout(10) << "create_object_context " << (void*)obc.get() << " " << oi.soid << " " << dendl;
  populate_obc_watchers(obc);
  return obc;
}

ObjectContextRef ReplicatedPG::get_object_context(const hobject_t& soid,
						  bool can_create,
						  map<string, bufferlist> *attrs)
{
  assert(
    attrs || !pg_log.get_missing().is_missing(soid) ||
    // or this is a revert... see recover_primary()
    (pg_log.get_log().objects.count(soid) &&
      pg_log.get_log().objects.find(soid)->second->op ==
      pg_log_entry_t::LOST_REVERT));
  ObjectContextRef obc = object_contexts.lookup(soid);
  if (obc) {
    dout(10) << __func__ << ": found obc in cache: " << obc
	     << dendl;
  } else {
    // check disk
    bufferlist bv;
    if (attrs) {
      assert(attrs->count(OI_ATTR));
      bv = attrs->find(OI_ATTR)->second;
    } else {
      int r = pgbackend->objects_get_attr(soid, OI_ATTR, &bv);
      if (r < 0) {
	if (!can_create) {
	  dout(10) << __func__ << ": no obc for soid "
		   << soid << " and !can_create"
		   << dendl;
	  return ObjectContextRef();   // -ENOENT!
	}

	dout(10) << __func__ << ": no obc for soid "
		 << soid << " but can_create"
		 << dendl;
	// new object.
	object_info_t oi(soid);
	SnapSetContext *ssc = get_snapset_context(
	  soid, true,
	  soid.has_snapset() ? attrs : 0);
	obc = create_object_context(oi, ssc);
	dout(10) << __func__ << ": " << obc << " " << soid
		 << " " << obc->rwstate
		 << " oi: " << obc->obs.oi
		 << " ssc: " << obc->ssc
		 << " snapset: " << obc->ssc->snapset << dendl;
	return obc;
      }
    }

    object_info_t oi(bv);

    assert(oi.soid.pool == (int64_t)info.pgid.pool());

    obc = object_contexts.lookup_or_create(oi.soid);
    obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
    obc->obs.oi = oi;
    obc->obs.exists = true;

    obc->ssc = get_snapset_context(
      soid, true,
      soid.has_snapset() ? attrs : 0);
    register_snapset_context(obc->ssc);

    populate_obc_watchers(obc);

    if (pool.info.require_rollback()) {
      if (attrs) {
	obc->attr_cache = *attrs;
      } else {
	int r = pgbackend->objects_get_attrs(
	  soid,
	  &obc->attr_cache);
	assert(r == 0);
      }
    }

    dout(10) << __func__ << ": creating obc from disk: " << obc
	     << dendl;
  }
  assert(obc->ssc);
  dout(10) << __func__ << ": " << obc << " " << soid
	   << " " << obc->rwstate
	   << " oi: " << obc->obs.oi
	   << " ssc: " << obc->ssc
	   << " snapset: " << obc->ssc->snapset << dendl;
  return obc;
}

void ReplicatedPG::context_registry_on_change()
{
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    if (obc) {
      for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	     obc->watchers.begin();
	   j != obc->watchers.end();
	   obc->watchers.erase(j++)) {
	j->second->discard();
      }
    }
  }
}


/*
 * If we return an error, and set *pmissing, then promoting that
 * object may help.
 *
 * If we return -EAGAIN, we will always set *pmissing to the missing
 * object to wait for.
 *
 * If we return an error but do not set *pmissing, then we know the
 * object does not exist.
 */
int ReplicatedPG::find_object_context(const hobject_t& oid,
				      ObjectContextRef *pobc,
				      bool can_create,
				      bool map_snapid_to_clone,
				      hobject_t *pmissing)
{
  hobject_t head(oid.oid, oid.get_key(), CEPH_NOSNAP, oid.hash,
		 info.pgid.pool(), oid.get_namespace());
  hobject_t snapdir(oid.oid, oid.get_key(), CEPH_SNAPDIR, oid.hash,
		    info.pgid.pool(), oid.get_namespace());

  // want the snapdir?
  if (oid.snap == CEPH_SNAPDIR) {
    // return head or snapdir, whichever exists.
    ObjectContextRef obc = get_object_context(head, can_create);
    if (!obc || !obc->obs.exists)
      obc = get_object_context(snapdir, can_create);
    if (!obc || !obc->obs.exists) {
      // if we have neither, we would want to promote the head.
      if (pmissing)
	*pmissing = head;
      return -ENOENT;
    }
    dout(10) << "find_object_context " << oid
	     << " @" << oid.snap
	     << " oi=" << obc->obs.oi
	     << dendl;
    *pobc = obc;

    // always populate ssc for SNAPDIR...
    if (!obc->ssc)
      obc->ssc = get_snapset_context(
	oid, true);
    return 0;
  }

  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContextRef obc = get_object_context(head, can_create);
    if (!obc) {
      if (pmissing)
	*pmissing = head;
      return -ENOENT;
    }
    dout(10) << "find_object_context " << oid
	     << " @" << oid.snap
	     << " oi=" << obc->obs.oi
	     << dendl;
    *pobc = obc;

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(oid, true);

    return 0;
  }

  // we want a snap
  if (!map_snapid_to_clone && pool.info.is_removed_snap(oid.snap)) {
    dout(10) << __func__ << " snap " << oid.snap << " is removed" << dendl;
    return -ENOENT;
  }

  SnapSetContext *ssc = get_snapset_context(oid, can_create);
  if (!ssc || !(ssc->exists)) {
    dout(20) << __func__ << " " << oid << " no snapset" << dendl;
    if (pmissing)
      *pmissing = head;  // start by getting the head
    if (ssc)
      put_snapset_context(ssc);
    return -ENOENT;
  }

  if (map_snapid_to_clone) {
    dout(10) << "find_object_context " << oid << " @" << oid.snap
	     << " snapset " << ssc->snapset
	     << " map_snapid_to_clone=true" << dendl;
    if (oid.snap > ssc->snapset.seq) {
      // already must be readable
      ObjectContextRef obc = get_object_context(head, false);
      dout(10) << "find_object_context " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " maps to head" << dendl;
      *pobc = obc;
      put_snapset_context(ssc);
      return (obc && obc->obs.exists) ? 0 : -ENOENT;
    } else {
      vector<snapid_t>::const_iterator citer = std::find(
	ssc->snapset.clones.begin(),
	ssc->snapset.clones.end(),
	oid.snap);
      if (citer == ssc->snapset.clones.end()) {
	dout(10) << "find_object_context " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " maps to nothing" << dendl;
	put_snapset_context(ssc);
	return -ENOENT;
      }

      dout(10) << "find_object_context " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " maps to " << oid << dendl;

      if (pg_log.get_missing().is_missing(oid)) {
	dout(10) << "find_object_context " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " " << oid << " is missing" << dendl;
	if (pmissing)
	  *pmissing = oid;
	put_snapset_context(ssc);
	return -EAGAIN;
      }

      ObjectContextRef obc = get_object_context(oid, false);
      if (!obc || !obc->obs.exists) {
	dout(10) << "find_object_context " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " " << oid << " is not present" << dendl;
	if (pmissing)
	  *pmissing = oid;
	put_snapset_context(ssc);
	return -ENOENT;
      }
      dout(10) << "find_object_context " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " " << oid << " HIT" << dendl;
      *pobc = obc;
      put_snapset_context(ssc);
      return 0;
    }
    assert(0); //unreachable
  }

  dout(10) << "find_object_context " << oid << " @" << oid.snap
	   << " snapset " << ssc->snapset << dendl;
 
  // head?
  if (oid.snap > ssc->snapset.seq) {
    if (ssc->snapset.head_exists) {
      ObjectContextRef obc = get_object_context(head, false);
      dout(10) << "find_object_context  " << head
	       << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	       << " -- HIT " << obc->obs
	       << dendl;
      if (!obc->ssc)
	obc->ssc = ssc;
      else {
	assert(ssc == obc->ssc);
	put_snapset_context(ssc);
      }
      *pobc = obc;
      return 0;
    }
    dout(10) << "find_object_context  " << head
	     << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	     << " but head dne -- DNE"
	     << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < oid.snap)
    k++;
  if (k == ssc->snapset.clones.size()) {
    dout(10) << "find_object_context  no clones with last >= oid.snap "
	     << oid.snap << " -- DNE" << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }
  hobject_t soid(oid.oid, oid.get_key(), ssc->snapset.clones[k], oid.hash,
		 info.pgid.pool(), oid.get_namespace());

  if (pg_log.get_missing().is_missing(soid)) {
    dout(20) << "find_object_context  " << soid << " missing, try again later"
	     << dendl;
    if (pmissing)
      *pmissing = soid;
    put_snapset_context(ssc);
    return -EAGAIN;
  }

  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc || !obc->obs.exists) {
    dout(20) << __func__ << " missing clone " << soid << dendl;
    if (pmissing)
      *pmissing = soid;
    put_snapset_context(ssc);
    return -ENOENT;
  }

  if (!obc->ssc) {
    obc->ssc = ssc;
  } else {
    assert(obc->ssc == ssc);
    put_snapset_context(ssc);
  }
  ssc = 0;

  // clone
  dout(20) << "find_object_context  " << soid << " snaps " << obc->obs.oi.snaps
	   << dendl;
  snapid_t first = obc->obs.oi.snaps[obc->obs.oi.snaps.size()-1];
  snapid_t last = obc->obs.oi.snaps[0];
  if (first <= oid.snap) {
    dout(20) << "find_object_context  " << soid << " [" << first << "," << last
	     << "] contains " << oid.snap << " -- HIT " << obc->obs << dendl;
    *pobc = obc;
    return 0;
  } else {
    dout(20) << "find_object_context  " << soid << " [" << first << "," << last
	     << "] does not contain " << oid.snap << " -- DNE" << dendl;
    return -ENOENT;
  }
}

void ReplicatedPG::object_context_destructor_callback(ObjectContext *obc)
{
  if (obc->ssc)
    put_snapset_context(obc->ssc);
}

void ReplicatedPG::add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *pgstat)
{
  object_info_t& oi = obc->obs.oi;

  dout(10) << "add_object_context_to_pg_stat " << oi.soid << dendl;
  object_stat_sum_t stat;

  stat.num_bytes += oi.size;

  if (oi.soid.snap != CEPH_SNAPDIR)
    stat.num_objects++;
  if (oi.is_dirty())
    stat.num_objects_dirty++;
  if (oi.is_whiteout())
    stat.num_whiteouts++;
  if (oi.is_omap())
    stat.num_objects_omap++;

  if (oi.soid.snap && oi.soid.snap != CEPH_NOSNAP && oi.soid.snap != CEPH_SNAPDIR) {
    stat.num_object_clones++;

    if (!obc->ssc)
      obc->ssc = get_snapset_context(oi.soid, false);
    assert(obc->ssc);

    // subtract off clone overlap
    if (obc->ssc->snapset.clone_overlap.count(oi.soid.snap)) {
      interval_set<uint64_t>& o = obc->ssc->snapset.clone_overlap[oi.soid.snap];
      for (interval_set<uint64_t>::const_iterator r = o.begin();
	   r != o.end();
	   ++r) {
	stat.num_bytes -= r.get_len();
      }	  
    }
  }

  // add it in
  pgstat->stats.sum.add(stat);
  if (oi.category.length())
    pgstat->stats.cat_sum[oi.category].add(stat);
}

void ReplicatedPG::kick_object_context_blocked(ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  map<hobject_t, list<OpRequestRef> >::iterator p = waiting_for_blocked_object.find(soid);
  if (p == waiting_for_blocked_object.end())
    return;

  if (obc->is_blocked()) {
    dout(10) << __func__ << " " << soid << " still blocked" << dendl;
    return;
  }

  list<OpRequestRef>& ls = p->second;
  dout(10) << __func__ << " " << soid << " requeuing " << ls.size() << " requests" << dendl;
  requeue_ops(ls);
  waiting_for_blocked_object.erase(p);

  if (obc->requeue_scrub_on_unblock)
    osd->queue_for_scrub(this);
}

SnapSetContext *ReplicatedPG::create_snapset_context(const hobject_t& oid)
{
  Mutex::Locker l(snapset_contexts_lock);
  SnapSetContext *ssc = new SnapSetContext(oid.get_snapdir());
  _register_snapset_context(ssc);
  ssc->ref++;
  return ssc;
}

SnapSetContext *ReplicatedPG::get_snapset_context(
  const hobject_t& oid,
  bool can_create,
  map<string, bufferlist> *attrs)
{
  Mutex::Locker l(snapset_contexts_lock);
  SnapSetContext *ssc;
  map<hobject_t, SnapSetContext*>::iterator p = snapset_contexts.find(
    oid.get_snapdir());
  if (p != snapset_contexts.end()) {
    if (can_create || p->second->exists) {
      ssc = p->second;
      ssc->exists = true;
    } else {
      return NULL;
    }
  } else {
    bufferlist bv;
    if (!attrs) {
      int r = pgbackend->objects_get_attr(oid.get_head(), SS_ATTR, &bv);
      if (r < 0) {
	// try _snapset
	r = pgbackend->objects_get_attr(oid.get_snapdir(), SS_ATTR, &bv);
	if (r < 0 && !can_create)
	  return NULL;
      }
    } else {
      assert(attrs->count(SS_ATTR));
      bv = attrs->find(SS_ATTR)->second;
    }
    ssc = new SnapSetContext(oid.get_snapdir());
    _register_snapset_context(ssc);
    if (bv.length()) {
      bufferlist::iterator bvp = bv.begin();
      ssc->snapset.decode(bvp);
    }
  }
  assert(ssc);
  ssc->ref++;
  return ssc;
}

void ReplicatedPG::put_snapset_context(SnapSetContext *ssc)
{
  Mutex::Locker l(snapset_contexts_lock);
  --ssc->ref;
  if (ssc->ref == 0) {
    if (ssc->registered)
      snapset_contexts.erase(ssc->oid);
    delete ssc;
  }
}

// sub op modify

void ReplicatedBackend::sub_op_modify(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);

  const hobject_t& soid = m->poid;

  const char *opname;
  if (m->noop)
    opname = "no-op";
  else if (m->ops.size())
    opname = ceph_osd_op_name(m->ops[0].op.op);
  else
    opname = "trans";

  dout(10) << "sub_op_modify " << opname 
           << " " << soid 
           << " v " << m->version
	   << (m->noop ? " NOOP" : "")
	   << (m->logbl.length() ? " (transaction)" : " (parallel exec")
	   << " " << m->logbl.length()
	   << dendl;  

  // sanity checks
  assert(m->map_epoch >= get_info().history.same_interval_since);
  
  // we better not be missing this.
  assert(!parent->get_log().get_missing().is_missing(soid));

  int ackerosd = m->get_source().num();
  
  op->mark_started();

  RepModifyRef rm(new RepModify);
  rm->op = op;
  rm->ackerosd = ackerosd;
  rm->last_complete = get_info().last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  if (!m->noop) {
    assert(m->logbl.length());
    // shipped transaction and log entries
    vector<pg_log_entry_t> log;

    bufferlist::iterator p = m->get_data().begin();
    ::decode(rm->opt, p);
    if (!(m->get_connection()->get_features() & CEPH_FEATURE_OSD_SNAPMAPPER))
      rm->opt.set_tolerate_collection_add_enoent();

    if (m->new_temp_oid != hobject_t()) {
      dout(20) << __func__ << " start tracking temp " << m->new_temp_oid << dendl;
      add_temp_obj(m->new_temp_oid);
      get_temp_coll(&rm->localt);
    }
    if (m->discard_temp_oid != hobject_t()) {
      dout(20) << __func__ << " stop tracking temp " << m->discard_temp_oid << dendl;
      if (rm->opt.empty()) {
	dout(10) << __func__ << ": removing object " << m->discard_temp_oid
		 << " since we won't get the transaction" << dendl;
	rm->localt.remove(temp_coll, m->discard_temp_oid);
      }
      clear_temp_obj(m->discard_temp_oid);
    }

    p = m->logbl.begin();
    ::decode(log, p);
    if (m->hobject_incorrect_pool) {
      for (vector<pg_log_entry_t>::iterator i = log.begin();
	  i != log.end();
	  ++i) {
	if (!i->soid.is_max() && i->soid.pool == -1)
	  i->soid.pool = get_info().pgid.pool();
      }
      rm->opt.set_pool_override(get_info().pgid.pool());
    }
    rm->opt.set_replica();

    bool update_snaps = false;
    if (!rm->opt.empty()) {
      // If the opt is non-empty, we infer we are before
      // last_backfill (according to the primary, not our
      // not-quite-accurate value), and should update the
      // collections now.  Otherwise, we do it later on push.
      update_snaps = true;
    }
    parent->update_stats(m->pg_stats);
    parent->log_operation(
      log,
      m->updated_hit_set_history,
      m->pg_trim_to,
      m->pg_trim_rollback_to,
      update_snaps,
      &(rm->localt));
      
    rm->bytes_written = rm->opt.get_encoded_bytes();

  } else {
    assert(0);
    #if 0
    // just trim the log
    if (m->pg_trim_to != eversion_t()) {
      pg_log.trim(m->pg_trim_to, info);
      dirty_info = true;
      write_if_dirty(rm->localt);
    }
    #endif
  }
  
  op->mark_started();

  rm->localt.append(rm->opt);
  rm->localt.register_on_commit(
    parent->bless_context(
      new C_OSD_RepModifyCommit(this, rm)));
  rm->localt.register_on_applied(
    parent->bless_context(
      new C_OSD_RepModifyApply(this, rm)));
  parent->queue_transaction(&(rm->localt), op);
  // op is cleaned up by oncommit/onapply when both are executed
}

void ReplicatedBackend::sub_op_modify_applied(RepModifyRef rm)
{
  rm->op->mark_event("sub_op_applied");
  rm->applied = true;

  dout(10) << "sub_op_modify_applied on " << rm << " op "
	   << *rm->op->get_req() << dendl;
  MOSDSubOp *m = static_cast<MOSDSubOp*>(rm->op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);
  
  if (!rm->committed) {
    // send ack to acker only if we haven't sent a commit already
    MOSDSubOpReply *ack = new MOSDSubOpReply(
      m, parent->whoami_shard(),
      0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
    ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
    get_parent()->send_message_osd_cluster(
      rm->ackerosd, ack, get_osdmap()->get_epoch());
  }
  
  parent->op_applied(m->version);
}

void ReplicatedBackend::sub_op_modify_commit(RepModifyRef rm)
{
  rm->op->mark_commit_sent();
  rm->committed = true;

  // send commit.
  dout(10) << "sub_op_modify_commit on op " << *rm->op->get_req()
	   << ", sending commit to osd." << rm->ackerosd
	   << dendl;
  
  assert(get_osdmap()->is_up(rm->ackerosd));
  get_parent()->update_last_complete_ondisk(rm->last_complete);
  MOSDSubOpReply *commit = new MOSDSubOpReply(
    static_cast<MOSDSubOp*>(rm->op->get_req()),
    get_parent()->whoami_shard(),
    0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
  commit->set_last_complete_ondisk(rm->last_complete);
  commit->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
  get_parent()->send_message_osd_cluster(
    rm->ackerosd, commit, get_osdmap()->get_epoch());
  
  log_subop_stats(get_parent()->get_logger(), rm->op,
		  l_osd_sop_w_inb, l_osd_sop_w_lat);
}


// ===========================================================

void ReplicatedBackend::calc_head_subsets(
  ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_head_subsets " << head << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }


  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);    
  
  for (int j=snapset.clones.size()-1; j>=0; j--) {
    hobject_t c = head;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) && c < last_backfill) {
      dout(10) << "calc_head_subsets " << head << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_head_subsets " << head << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }


  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;
    clone_subsets.clear();
    cloning.clear();
  }

  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedBackend::calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill,
  interval_set<uint64_t>& data_subset,
  map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];
  if (size)
    data_subset.insert(0, size);

  if (get_parent()->get_pool().allow_incomplete_clones()) {
    dout(10) << __func__ << ": caching (was) enabled, skipping clone subsets" << dendl;
    return;
  }

  if (!cct->_conf->osd_recover_clone_overlap) {
    dout(10) << "calc_clone_subsets " << soid << " -- osd_recover_clone_overlap disabled" << dendl;
    return;
  }
  
  unsigned i;
  for (i=0; i < snapset.clones.size(); i++)
    if (snapset.clones[i] == soid.snap)
      break;

  // any overlap with next older clone?
  interval_set<uint64_t> cloning;
  interval_set<uint64_t> prev;
  if (size)
    prev.insert(0, size);    
  for (int j=i-1; j>=0; j--) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    prev.intersection_of(snapset.clone_overlap[snapset.clones[j]]);
    if (!missing.is_missing(c) && c < last_backfill) {
      dout(10) << "calc_clone_subsets " << soid << " has prev " << c
	       << " overlap " << prev << dendl;
      clone_subsets[c] = prev;
      cloning.union_of(prev);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have prev " << c
	     << " overlap " << prev << dendl;
  }
  
  // overlap with next newest?
  interval_set<uint64_t> next;
  if (size)
    next.insert(0, size);    
  for (unsigned j=i+1; j<snapset.clones.size(); j++) {
    hobject_t c = soid;
    c.snap = snapset.clones[j];
    next.intersection_of(snapset.clone_overlap[snapset.clones[j-1]]);
    if (!missing.is_missing(c) && c < last_backfill) {
      dout(10) << "calc_clone_subsets " << soid << " has next " << c
	       << " overlap " << next << dendl;
      clone_subsets[c] = next;
      cloning.union_of(next);
      break;
    }
    dout(10) << "calc_clone_subsets " << soid << " does not have next " << c
	     << " overlap " << next << dendl;
  }

  if (cloning.num_intervals() > cct->_conf->osd_recover_clone_overlap_limit) {
    dout(10) << "skipping clone, too many holes" << dendl;
    clone_subsets.clear();
    cloning.clear();
  }

  
  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_clone_subsets " << soid
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}


/** pull - request object from a peer
 */

/*
 * Return values:
 *  NONE  - didn't pull anything
 *  YES   - pulled what the caller wanted
 *  OTHER - needed to pull something else first (_head or _snapdir)
 */
enum { PULL_NONE, PULL_OTHER, PULL_YES };

void ReplicatedBackend::prepare_pull(
  eversion_t v,
  const hobject_t& soid,
  ObjectContextRef headctx,
  RPGHandle *h)
{
  assert(get_parent()->get_local_missing().missing.count(soid));
  eversion_t _v = get_parent()->get_local_missing().missing.find(
    soid)->second.need;
  assert(_v == v);
  const map<hobject_t, set<pg_shard_t> > &missing_loc(
    get_parent()->get_missing_loc_shards());
  const map<pg_shard_t, pg_missing_t > &peer_missing(
    get_parent()->get_shard_missing());
  map<hobject_t, set<pg_shard_t> >::const_iterator q = missing_loc.find(soid);
  assert(q != missing_loc.end());
  assert(!q->second.empty());

  // pick a pullee
  vector<pg_shard_t> shuffle(q->second.begin(), q->second.end());
  random_shuffle(shuffle.begin(), shuffle.end());
  vector<pg_shard_t>::iterator p = shuffle.begin();
  assert(get_osdmap()->is_up(p->osd));
  pg_shard_t fromshard = *p;

  dout(7) << "pull " << soid
	  << " v " << v
	  << " on osds " << *p
	  << " from osd." << fromshard
	  << dendl;

  assert(peer_missing.count(fromshard));
  const pg_missing_t &pmissing = peer_missing.find(fromshard)->second;
  if (pmissing.is_missing(soid, v)) {
    assert(pmissing.missing.find(soid)->second.have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromshard
	     << " at version " << pmissing.missing.find(soid)->second.have
	     << " rather than at version " << v << dendl;
    v = pmissing.missing.find(soid)->second.have;
    assert(get_parent()->get_log().get_log().objects.count(soid) &&
	   (get_parent()->get_log().get_log().objects.find(soid)->second->op ==
	    pg_log_entry_t::LOST_REVERT) &&
	   (get_parent()->get_log().get_log().objects.find(
	     soid)->second->reverting_to ==
	    v));
  }
  
  ObjectRecoveryInfo recovery_info;

  if (soid.is_snap()) {
    assert(!get_parent()->get_local_missing().is_missing(
	     soid.get_head()) ||
	   !get_parent()->get_local_missing().is_missing(
	     soid.get_snapdir()));
    assert(headctx);
    // check snapset
    SnapSetContext *ssc = headctx->ssc;
    assert(ssc);
    dout(10) << " snapset " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, get_parent()->get_local_missing(),
		       get_info().last_backfill,
		       recovery_info.copy_subset,
		       recovery_info.clone_subset);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    recovery_info.size = ((uint64_t)-1);
  }

  h->pulls[fromshard].push_back(PullOp());
  PullOp &op = h->pulls[fromshard].back();
  op.soid = soid;

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = false;
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  assert(!pulling.count(soid));
  pull_from_peer[fromshard].insert(soid);
  PullInfo &pi = pulling[soid];
  pi.head_ctx = headctx;
  pi.recovery_info = op.recovery_info;
  pi.recovery_progress = op.recovery_progress;
}

int ReplicatedPG::recover_missing(
  const hobject_t &soid, eversion_t v,
  int priority,
  PGBackend::RecoveryHandle *h)
{
  if (missing_loc.is_unfound(soid)) {
    dout(7) << "pull " << soid
	    << " v " << v 
	    << " but it is unfound" << dendl;
    return PULL_NONE;
  }

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  ObjectContextRef obc;
  ObjectContextRef head_obc;
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head and/or snapdir?
    hobject_t head = soid.get_head();
    if (pg_log.get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering head " << head << dendl;
	return PULL_NONE;
      } else {
	int r = recover_missing(
	  head, pg_log.get_missing().missing.find(head)->second.need, priority,
	  h);
	if (r != PULL_NONE)
	  return PULL_OTHER;
	return PULL_NONE;
      }
    }
    head = soid.get_snapdir();
    if (pg_log.get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering snapdir " << head << dendl;
	return PULL_NONE;
      } else {
	int r = recover_missing(
	  head, pg_log.get_missing().missing.find(head)->second.need, priority,
	  h);
	if (r != PULL_NONE)
	  return PULL_OTHER;
	return PULL_NONE;
      }
    }

    // we must have one or the other
    head_obc = get_object_context(
      soid.get_head(),
      false,
      0);
    if (!head_obc)
      head_obc = get_object_context(
	soid.get_snapdir(),
	false,
	0);
    assert(head_obc);
  }
  start_recovery_op(soid);
  assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));
  pgbackend->recover_object(
    soid,
    v,
    head_obc,
    obc,
    h);
  return PULL_YES;
}

void ReplicatedPG::send_remove_op(
  const hobject_t& oid, eversion_t v, pg_shard_t peer)
{
  ceph_tid_t tid = osd->get_tid();
  osd_reqid_t rid(osd->get_cluster_msgr_name(), 0, tid);

  dout(10) << "send_remove_op " << oid << " from osd." << peer
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(
    rid, pg_whoami, spg_t(info.pgid.pgid, peer.shard),
    oid, false, CEPH_OSD_FLAG_ACK,
    get_osdmap()->get_epoch(), tid, v);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_DELETE;

  osd->send_message_osd_cluster(peer.osd, subop, get_osdmap()->get_epoch());
}

/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
void ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
  PushOp *pop)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << __func__ << ": " << soid << " v" << oi.version
	   << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t> > clone_subsets;
  interval_set<uint64_t> data_subset;

  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {	
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (get_parent()->get_local_missing().is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop);
    }
    hobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (get_parent()->get_local_missing().is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir
	       << ", pushing raw clone" << dendl;
      return prep_push(obc, soid, peer, pop);
    }
    
    SnapSetContext *ssc = obc->ssc;
    assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    map<pg_shard_t, pg_missing_t>::const_iterator pm =
      get_parent()->get_shard_missing().find(peer);
    assert(pm != get_parent()->get_shard_missing().end());
    map<pg_shard_t, pg_info_t>::const_iterator pi =
      get_parent()->get_shard_info().find(peer);
    assert(pi != get_parent()->get_shard_info().end());
    calc_clone_subsets(ssc->snapset, soid,
		       pm->second,
		       pi->second.last_backfill,
		       data_subset, clone_subsets);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = obc->ssc;
    assert(ssc);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(
      obc,
      ssc->snapset, soid, get_parent()->get_shard_missing().find(peer)->second,
      get_parent()->get_shard_info().find(peer)->second.last_backfill,
      data_subset, clone_subsets);
  }

  prep_push(obc, soid, peer, oi.version, data_subset, clone_subsets, pop);
}

void ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, pg_shard_t peer,
			     PushOp *pop)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);
  map<hobject_t, interval_set<uint64_t> > clone_subsets;

  prep_push(obc, soid, peer,
	    obc->obs.oi.version, data_subset, clone_subsets,
	    pop);
}

void ReplicatedBackend::prep_push(
  ObjectContextRef obc,
  const hobject_t& soid, pg_shard_t peer,
  eversion_t version,
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t> >& clone_subsets,
  PushOp *pop)
{
  get_parent()->begin_peer_recover(peer, soid);
  // take note.
  PushInfo &pi = pushing[soid][peer];
  pi.obc = obc;
  pi.recovery_info.size = obc->obs.oi.size;
  pi.recovery_info.copy_subset = data_subset;
  pi.recovery_info.clone_subset = clone_subsets;
  pi.recovery_info.soid = soid;
  pi.recovery_info.oi = obc->obs.oi;
  pi.recovery_info.version = version;
  pi.recovery_progress.first = true;
  pi.recovery_progress.data_recovered_to = 0;
  pi.recovery_progress.data_complete = 0;
  pi.recovery_progress.omap_complete = 0;

  ObjectRecoveryProgress new_progress;
  int r = build_push_op(pi.recovery_info,
			pi.recovery_progress,
			&new_progress,
			pop,
			&(pi.stat));
  assert(r == 0);
  pi.recovery_progress = new_progress;
}

int ReplicatedBackend::send_pull_legacy(int prio, pg_shard_t peer,
					const ObjectRecoveryInfo &recovery_info,
					ObjectRecoveryProgress progress)
{
  // send op
  ceph_tid_t tid = get_parent()->get_tid();
  osd_reqid_t rid(get_parent()->get_cluster_msgr_name(), 0, tid);

  dout(10) << "send_pull_op " << recovery_info.soid << " "
	   << recovery_info.version
	   << " first=" << progress.first
	   << " data " << recovery_info.copy_subset
	   << " from osd." << peer
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(
    rid, parent->whoami_shard(),
    get_info().pgid, recovery_info.soid,
    false, CEPH_OSD_FLAG_ACK,
    get_osdmap()->get_epoch(), tid,
    recovery_info.version);
  subop->set_priority(prio);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->ops[0].op.extent.length = cct->_conf->osd_recovery_max_chunk;
  subop->recovery_info = recovery_info;
  subop->recovery_progress = progress;

  get_parent()->send_message_osd_cluster(
    peer.osd, subop, get_osdmap()->get_epoch());

  get_parent()->get_logger()->inc(l_osd_pull);
  return 0;
}

void ReplicatedBackend::submit_push_data(
  ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  map<string, bufferlist> &attrs,
  map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  coll_t target_coll;
  if (first && complete) {
    target_coll = coll;
  } else {
    dout(10) << __func__ << ": Creating oid "
	     << recovery_info.soid << " in the temp collection" << dendl;
    add_temp_obj(recovery_info.soid);
    target_coll = get_temp_coll(t);
  }

  if (first) {
    get_parent()->on_local_recover_start(recovery_info.soid, t);
    t->remove(get_temp_coll(t), recovery_info.soid);
    t->touch(target_coll, recovery_info.soid);
    t->omap_setheader(target_coll, recovery_info.soid, omap_header);
  }
  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
       p != intervals_included.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data_included, off, p.get_len());
    t->write(target_coll, recovery_info.soid,
	     p.get_start(), p.get_len(), bit);
    off += p.get_len();
  }

  t->omap_setkeys(target_coll, recovery_info.soid,
		  omap_entries);
  t->setattrs(target_coll, recovery_info.soid,
	      attrs);

  if (complete) {
    if (!first) {
      dout(10) << __func__ << ": Removing oid "
	       << recovery_info.soid << " from the temp collection" << dendl;
      clear_temp_obj(recovery_info.soid);
      t->collection_move(coll, target_coll, recovery_info.soid);
    }

    submit_push_complete(recovery_info, t);
  }
}

void ReplicatedBackend::submit_push_complete(ObjectRecoveryInfo &recovery_info,
					     ObjectStore::Transaction *t)
{
  for (map<hobject_t, interval_set<uint64_t> >::const_iterator p =
	 recovery_info.clone_subset.begin();
       p != recovery_info.clone_subset.end();
       ++p) {
    for (interval_set<uint64_t>::const_iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      dout(15) << " clone_range " << p->first << " "
	       << q.get_start() << "~" << q.get_len() << dendl;
      t->clone_range(coll, p->first, recovery_info.soid,
		     q.get_start(), q.get_len(), q.get_start());
    }
  }
}

ObjectRecoveryInfo ReplicatedBackend::recalc_subsets(
  const ObjectRecoveryInfo& recovery_info,
  SnapSetContext *ssc)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  assert(ssc);
  calc_clone_subsets(ssc->snapset, new_info.soid, get_parent()->get_local_missing(),
		     get_info().last_backfill,
		     new_info.copy_subset, new_info.clone_subset);
  return new_info;
}

bool ReplicatedBackend::handle_pull_response(
  pg_shard_t from, PushOp &pop, PullOp *response,
  list<hobject_t> *to_continue,
  ObjectStore::Transaction *t
  )
{
  interval_set<uint64_t> data_included = pop.data_included;
  bufferlist data;
  data.claim(pop.data);
  dout(10) << "handle_pull_response "
	   << pop.recovery_info
	   << pop.after_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;
  if (pop.version == eversion_t()) {
    // replica doesn't have it!
    _failed_push(from, pop.soid);
    return false;
  }

  hobject_t &hoid = pop.soid;
  assert((data_included.empty() && data.length() == 0) ||
	 (!data_included.empty() && data.length() > 0));

  if (!pulling.count(hoid)) {
    return false;
  }

  PullInfo &pi = pulling[hoid];
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = pop.recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
      pop.recovery_info.copy_subset);
  }

  bool first = pi.recovery_progress.first;
  if (first) {
    pi.obc = get_parent()->get_obc(pi.recovery_info.soid, pop.attrset);
    pi.recovery_info.oi = pi.obc->obs.oi;
    pi.recovery_info = recalc_subsets(pi.recovery_info, pi.obc->ssc);
  }


  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pi.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data.claim(usable_data);


  pi.recovery_progress = pop.after_progress;

  pi.stat.num_bytes_recovered += data.length();

  dout(10) << "new recovery_info " << pi.recovery_info
	   << ", new progress " << pi.recovery_progress
	   << dendl;

  bool complete = pi.is_complete();

  submit_push_data(pi.recovery_info, first,
		   complete,
		   data_included, data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  pi.stat.num_keys_recovered += pop.omap_entries.size();

  if (complete) {
    to_continue->push_back(hoid);
    pi.stat.num_objects_recovered++;
    get_parent()->on_local_recover(
      hoid, pi.stat, pi.recovery_info, pi.obc, t);
    pull_from_peer[from].erase(hoid);
    if (pull_from_peer[from].empty())
      pull_from_peer.erase(from);
    return false;
  } else {
    response->soid = pop.soid;
    response->recovery_info = pi.recovery_info;
    response->recovery_progress = pi.recovery_progress;
    return true;
  }
}

struct C_OnPushCommit : public Context {
  ReplicatedPG *pg;
  OpRequestRef op;
  C_OnPushCommit(ReplicatedPG *pg, OpRequestRef op) : pg(pg), op(op) {}
  void finish(int) {
    op->mark_event("committed");
    log_subop_stats(pg->osd->logger, op, l_osd_push_inb, l_osd_sop_push_lat);
  }
};

void ReplicatedBackend::handle_push(
  pg_shard_t from, PushOp &pop, PushReplyOp *response,
  ObjectStore::Transaction *t)
{
  dout(10) << "handle_push "
	   << pop.recovery_info
	   << pop.after_progress
	   << dendl;
  bufferlist data;
  data.claim(pop.data);
  bool first = pop.before_progress.first;
  bool complete = pop.after_progress.data_complete &&
    pop.after_progress.omap_complete;

  response->soid = pop.recovery_info.soid;
  submit_push_data(pop.recovery_info,
		   first,
		   complete,
		   pop.data_included,
		   data,
		   pop.omap_header,
		   pop.attrset,
		   pop.omap_entries,
		   t);

  if (complete)
    get_parent()->on_local_recover(
      pop.recovery_info.soid,
      object_stat_sum_t(),
      pop.recovery_info,
      ObjectContextRef(), // ok, is replica
      t);
}

void ReplicatedBackend::send_pushes(int prio, map<pg_shard_t, vector<PushOp> > &pushes)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;
    if (!(con->get_features() & CEPH_FEATURE_OSD_PACKED_RECOVERY)) {
      for (vector<PushOp>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	dout(20) << __func__ << ": sending push (legacy) " << *j
		 << " to osd." << i->first << dendl;
	send_push_op_legacy(prio, i->first, *j);
      }
    } else {
      vector<PushOp>::iterator j = i->second.begin();
      while (j != i->second.end()) {
	uint64_t cost = 0;
	uint64_t pushes = 0;
	MOSDPGPush *msg = new MOSDPGPush();
	msg->from = get_parent()->whoami_shard();
	msg->pgid = get_parent()->primary_spg_t();
	msg->map_epoch = get_osdmap()->get_epoch();
	msg->set_priority(prio);
	for (;
	     (j != i->second.end() &&
	      cost < cct->_conf->osd_max_push_cost &&
	      pushes < cct->_conf->osd_max_push_objects) ;
	     ++j) {
	  dout(20) << __func__ << ": sending push " << *j
		   << " to osd." << i->first << dendl;
	  cost += j->cost(cct);
	  pushes += 1;
	  msg->pushes.push_back(*j);
	}
	msg->compute_cost(cct);
	get_parent()->send_message_osd_cluster(msg, con);
      }
    }
  }
}

void ReplicatedBackend::send_pulls(int prio, map<pg_shard_t, vector<PullOp> > &pulls)
{
  for (map<pg_shard_t, vector<PullOp> >::iterator i = pulls.begin();
       i != pulls.end();
       ++i) {
    ConnectionRef con = get_parent()->get_con_osd_cluster(
      i->first.osd,
      get_osdmap()->get_epoch());
    if (!con)
      continue;
    if (!(con->get_features() & CEPH_FEATURE_OSD_PACKED_RECOVERY)) {
      for (vector<PullOp>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	dout(20) << __func__ << ": sending pull (legacy) " << *j
		 << " to osd." << i->first << dendl;
	send_pull_legacy(
	  prio,
	  i->first,
	  j->recovery_info,
	  j->recovery_progress);
      }
    } else {
      dout(20) << __func__ << ": sending pulls " << i->second
	       << " to osd." << i->first << dendl;
      MOSDPGPull *msg = new MOSDPGPull();
      msg->from = parent->whoami_shard();
      msg->set_priority(prio);
      msg->pgid = get_parent()->primary_spg_t();
      msg->map_epoch = get_osdmap()->get_epoch();
      msg->pulls.swap(i->second);
      msg->compute_cost(cct);
      get_parent()->send_message_osd_cluster(msg, con);
    }
  }
}

int ReplicatedBackend::build_push_op(const ObjectRecoveryInfo &recovery_info,
				     const ObjectRecoveryProgress &progress,
				     ObjectRecoveryProgress *out_progress,
				     PushOp *out_op,
				     object_stat_sum_t *stat)
{
  ObjectRecoveryProgress _new_progress;
  if (!out_progress)
    out_progress = &_new_progress;
  ObjectRecoveryProgress &new_progress = *out_progress;
  new_progress = progress;

  dout(7) << "send_push_op " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
	  << " recovery_info: " << recovery_info
          << dendl;

  if (progress.first) {
    store->omap_get_header(coll, recovery_info.soid, &out_op->omap_header);
    store->getattrs(coll, recovery_info.soid, out_op->attrset);

    // Debug
    bufferlist bv = out_op->attrset[OI_ATTR];
    object_info_t oi(bv);

    if (oi.version != recovery_info.version) {
      get_parent()->clog_error() << get_info().pgid << " push "
				 << recovery_info.soid << " v "
				 << recovery_info.version
				 << " failed because local copy is "
				 << oi.version << "\n";
      return -EINVAL;
    }

    new_progress.first = false;
  }

  uint64_t available = cct->_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {
    ObjectMap::ObjectMapIterator iter =
      store->get_omap_iterator(coll,
			       recovery_info.soid);
    for (iter->lower_bound(progress.omap_recovered_to);
	 iter->valid();
	 iter->next()) {
      if (!out_op->omap_entries.empty() &&
	  available <= (iter->key().size() + iter->value().length()))
	break;
      out_op->omap_entries.insert(make_pair(iter->key(), iter->value()));

      if ((iter->key().size() + iter->value().length()) <= available)
	available -= (iter->key().size() + iter->value().length());
      else
	available = 0;
    }
    if (!iter->valid())
      new_progress.omap_complete = true;
    else
      new_progress.omap_recovered_to = iter->key();
  }

  if (available > 0) {
    out_op->data_included.span_of(recovery_info.copy_subset,
				 progress.data_recovered_to,
				 available);
  } else {
    out_op->data_included.clear();
  }

  for (interval_set<uint64_t>::iterator p = out_op->data_included.begin();
       p != out_op->data_included.end();
       ++p) {
    bufferlist bit;
    store->read(coll, recovery_info.soid,
		     p.get_start(), p.get_len(), bit);
    if (p.get_len() != bit.length()) {
      dout(10) << " extent " << p.get_start() << "~" << p.get_len()
	       << " is actually " << p.get_start() << "~" << bit.length()
	       << dendl;
      interval_set<uint64_t>::iterator save = p++;
      if (bit.length() == 0)
        out_op->data_included.erase(save);     //Remove this empty interval
      else
        save.set_len(bit.length());
      // Remove any other intervals present
      while (p != out_op->data_included.end()) {
        interval_set<uint64_t>::iterator save = p++;
        out_op->data_included.erase(save);
      }
      new_progress.data_complete = true;
      out_op->data.claim_append(bit);
      break;
    }
    out_op->data.claim_append(bit);
  }

  if (!out_op->data_included.empty())
    new_progress.data_recovered_to = out_op->data_included.range_end();

  if (new_progress.is_complete(recovery_info)) {
    new_progress.data_complete = true;
    if (stat)
      stat->num_objects_recovered++;
  }

  if (stat) {
    stat->num_keys_recovered += out_op->omap_entries.size();
    stat->num_bytes_recovered += out_op->data.length();
  }

  get_parent()->get_logger()->inc(l_osd_push);
  get_parent()->get_logger()->inc(l_osd_push_outb, out_op->data.length());
  
  // send
  out_op->version = recovery_info.version;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;
  return 0;
}

int ReplicatedBackend::send_push_op_legacy(int prio, pg_shard_t peer, PushOp &pop)
{
  ceph_tid_t tid = get_parent()->get_tid();
  osd_reqid_t rid(get_parent()->get_cluster_msgr_name(), 0, tid);
  MOSDSubOp *subop = new MOSDSubOp(
    rid, parent->whoami_shard(),
    spg_t(get_info().pgid.pgid, peer.shard), pop.soid,
    false, 0, get_osdmap()->get_epoch(),
    tid, pop.recovery_info.version);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;

  subop->set_priority(prio);
  subop->version = pop.version;
  subop->ops[0].indata.claim(pop.data);
  subop->data_included.swap(pop.data_included);
  subop->omap_header.claim(pop.omap_header);
  subop->omap_entries.swap(pop.omap_entries);
  subop->attrset.swap(pop.attrset);
  subop->recovery_info = pop.recovery_info;
  subop->current_progress = pop.before_progress;
  subop->recovery_progress = pop.after_progress;

  get_parent()->send_message_osd_cluster(peer.osd, subop, get_osdmap()->get_epoch());
  return 0;
}

void ReplicatedBackend::prep_push_op_blank(const hobject_t& soid, PushOp *op)
{
  op->recovery_info.version = eversion_t();
  op->version = eversion_t();
  op->soid = soid;
}

void ReplicatedBackend::sub_op_push_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = static_cast<MOSDSubOpReply*>(op->get_req());
  const hobject_t& soid = reply->get_poid();
  assert(reply->get_header().type == MSG_OSD_SUBOPREPLY);
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;
  pg_shard_t peer = reply->from;

  op->mark_started();
  
  PushReplyOp rop;
  rop.soid = soid;
  PushOp pop;
  bool more = handle_push_reply(peer, rop, &pop);
  if (more)
    send_push_op_legacy(op->get_req()->get_priority(), peer, pop);
}

bool ReplicatedBackend::handle_push_reply(pg_shard_t peer, PushReplyOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;
    return false;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;
    return false;
  } else {
    PushInfo *pi = &pushing[soid][peer];

    if (!pi->recovery_progress.data_complete) {
      dout(10) << " pushing more from, "
	       << pi->recovery_progress.data_recovered_to
	       << " of " << pi->recovery_info.copy_subset << dendl;
      ObjectRecoveryProgress new_progress;
      int r = build_push_op(
	pi->recovery_info,
	pi->recovery_progress, &new_progress, reply,
	&(pi->stat));
      assert(r == 0);
      pi->recovery_progress = new_progress;
      return true;
    } else {
      // done!
      get_parent()->on_peer_recover(
	peer, soid, pi->recovery_info,
	pi->stat);
      
      pushing[soid].erase(peer);
      pi = NULL;
      
      
      if (pushing[soid].empty()) {
	get_parent()->on_global_recover(soid);
	pushing.erase(soid);
      } else {
	dout(10) << "pushed " << soid << ", still waiting for push ack from " 
		 << pushing[soid].size() << " others" << dendl;
      }
      return false;
    }
  }
}

void ReplicatedPG::finish_degraded_object(const hobject_t& oid)
{
  dout(10) << "finish_degraded_object " << oid << dendl;
  ObjectContextRef obc(object_contexts.lookup(oid));
  if (obc) {
    for (set<ObjectContextRef>::iterator j = obc->blocking.begin();
	 j != obc->blocking.end();
	 obc->blocking.erase(j++)) {
      dout(10) << " no longer blocking writes for " << (*j)->obs.oi.soid << dendl;
      (*j)->blocked_by = ObjectContextRef();
    }
  }
  if (callbacks_for_degraded_object.count(oid)) {
    list<Context*> contexts;
    contexts.swap(callbacks_for_degraded_object[oid]);
    callbacks_for_degraded_object.erase(oid);
    for (list<Context*>::iterator i = contexts.begin();
	 i != contexts.end();
	 ++i) {
      (*i)->complete(0);
    }
  }
}

/** op_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void ReplicatedBackend::sub_op_pull(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);

  op->mark_started();

  const hobject_t soid = m->poid;

  dout(7) << "pull" << soid << " v " << m->version
          << " from " << m->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  PullOp pop;
  pop.soid = soid;
  pop.recovery_info = m->recovery_info;
  pop.recovery_progress = m->recovery_progress;

  PushOp reply;
  handle_pull(m->from, pop, &reply);
  send_push_op_legacy(
    m->get_priority(),
    m->from,
    reply);

  log_subop_stats(get_parent()->get_logger(), op, 0, l_osd_sop_pull_lat);
}

void ReplicatedBackend::handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  struct stat st;
  int r = store->stat(coll, soid, &st);
  if (r != 0) {
    get_parent()->clog_error() << get_info().pgid << " "
			       << peer << " tried to pull " << soid
			       << " but got " << cpp_strerror(-r) << "\n";
    prep_push_op_blank(soid, reply);
  } else {
    ObjectRecoveryInfo &recovery_info = op.recovery_info;
    ObjectRecoveryProgress &progress = op.recovery_progress;
    if (progress.first && recovery_info.size == ((uint64_t)-1)) {
      // Adjust size and copy_subset
      recovery_info.size = st.st_size;
      recovery_info.copy_subset.clear();
      if (st.st_size)
	recovery_info.copy_subset.insert(0, st.st_size);
      assert(recovery_info.clone_subset.empty());
    }

    r = build_push_op(recovery_info, progress, 0, reply);
    if (r < 0)
      prep_push_op_blank(soid, reply);
  }
}


void ReplicatedPG::_committed_pushed_object(
  epoch_t epoch, eversion_t last_complete)
{
  lock();
  if (!pg_has_reset_since(epoch)) {
    dout(10) << "_committed_pushed_object last_complete " << last_complete << " now ondisk" << dendl;
    last_complete_ondisk = last_complete;

    if (last_complete_ondisk == info.last_update) {
      if (!is_primary()) {
        // Either we are a replica or backfill target.
	// we are fully up to date.  tell the primary!
	osd->send_message_osd_cluster(
	  get_primary().osd,
	  new MOSDPGTrim(
	    get_osdmap()->get_epoch(),
	    spg_t(info.pgid.pgid, primary.shard),
	    last_complete_ondisk),
	  get_osdmap()->get_epoch());
      } else {
	// we are the primary.  tell replicas to trim?
	if (calc_min_last_complete_ondisk())
	  trim_peers();
      }
    }

  } else {
    dout(10) << "_committed_pushed_object pg has changed, not touching last_complete_ondisk" << dendl;
  }

  unlock();
}

void ReplicatedPG::_applied_recovered_object(ObjectContextRef obc)
{
  lock();
  dout(10) << "_applied_recovered_object " << *obc << dendl;

  assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (!deleting && active_pushes == 0
      && scrubber.is_chunky_scrub_active()) {
    osd->scrub_wq.queue(this);
  }

  unlock();
}

void ReplicatedPG::_applied_recovered_object_replica()
{
  lock();
  dout(10) << "_applied_recovered_object_replica" << dendl;

  assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (!deleting && active_pushes == 0 &&
      scrubber.active_rep_scrub && scrubber.active_rep_scrub->chunky) {
    osd->rep_scrub_wq.queue(scrubber.active_rep_scrub);
    scrubber.active_rep_scrub = 0;
  }

  unlock();
}

void ReplicatedPG::recover_got(hobject_t oid, eversion_t v)
{
  dout(10) << "got missing " << oid << " v " << v << dendl;
  pg_log.recover_got(oid, v, info);
  if (pg_log.get_log().complete_to != pg_log.get_log().log.end()) {
    dout(10) << "last_complete now " << info.last_complete
	     << " log.complete_to " << pg_log.get_log().complete_to->version
	     << dendl;
  } else {
    dout(10) << "last_complete now " << info.last_complete
	     << " log.complete_to at end" << dendl;
    //below is not true in the repair case.
    //assert(missing.num_missing() == 0);  // otherwise, complete_to was wrong.
    assert(info.last_complete == info.last_update);
  }
}


/**
 * trim received data to remove what we don't want
 *
 * @param copy_subset intervals we want
 * @param data_included intervals we got
 * @param data_recieved data we got
 * @param intervals_usable intervals we want to keep
 * @param data_usable matching data we want to keep
 */
void ReplicatedBackend::trim_pushed_data(
  const interval_set<uint64_t> &copy_subset,
  const interval_set<uint64_t> &intervals_received,
  bufferlist data_received,
  interval_set<uint64_t> *intervals_usable,
  bufferlist *data_usable)
{
  if (intervals_received.subset_of(copy_subset)) {
    *intervals_usable = intervals_received;
    *data_usable = data_received;
    return;
  }

  intervals_usable->intersection_of(copy_subset,
				    intervals_received);

  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_received.begin();
       p != intervals_received.end();
       ++p) {
    interval_set<uint64_t> x;
    x.insert(p.get_start(), p.get_len());
    x.intersection_of(copy_subset);
    for (interval_set<uint64_t>::const_iterator q = x.begin();
	 q != x.end();
	 ++q) {
      bufferlist sub;
      uint64_t data_off = off + (q.get_start() - p.get_start());
      sub.substr_of(data_received, data_off, q.get_len());
      data_usable->claim_append(sub);
    }
    off += p.get_len();
  }
}

/** op_push
 * NOTE: called from opqueue.
 */
void ReplicatedBackend::sub_op_push(OpRequestRef op)
{
  op->mark_started();
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->get_req());

  PushOp pop;
  pop.soid = m->recovery_info.soid;
  pop.version = m->version;
  m->claim_data(pop.data);
  pop.data_included.swap(m->data_included);
  pop.omap_header.swap(m->omap_header);
  pop.omap_entries.swap(m->omap_entries);
  pop.attrset.swap(m->attrset);
  pop.recovery_info = m->recovery_info;
  pop.before_progress = m->current_progress;
  pop.after_progress = m->recovery_progress;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;

  if (is_primary()) {
    PullOp resp;
    RPGHandle *h = _open_recovery_op();
    list<hobject_t> to_continue;
    bool more = handle_pull_response(
      m->from, pop, &resp,
      &to_continue, t);
    if (more) {
      send_pull_legacy(
	m->get_priority(),
	m->from,
	resp.recovery_info,
	resp.recovery_progress);
    } else {
      C_ReplicatedBackend_OnPullComplete *c =
	new C_ReplicatedBackend_OnPullComplete(
	  this,
	  op->get_req()->get_priority());
      c->to_continue.swap(to_continue);
      t->register_on_complete(
	new PG_QueueAsync(
	  get_parent(),
	  get_parent()->bless_gencontext(c)));
    }
    run_recovery_op(h, op->get_req()->get_priority());
  } else {
    PushReplyOp resp;
    MOSDSubOpReply *reply = new MOSDSubOpReply(
      m, parent->whoami_shard(), 0,
      get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
    reply->set_priority(m->get_priority());
    assert(entity_name_t::TYPE_OSD == m->get_connection()->peer_type);
    handle_push(m->from, pop, &resp, t);
    t->register_on_complete(new PG_SendMessageOnConn(
			      get_parent(), reply, m->get_connection()));
  }
  t->register_on_applied(
    new ObjectStore::C_DeleteTransaction(t));
  get_parent()->queue_transaction(t);
  return;
}

void ReplicatedPG::failed_push(pg_shard_t from, const hobject_t &soid)
{
  assert(recovering.count(soid));
  recovering.erase(soid);
  missing_loc.remove_location(soid, from);
  dout(0) << "_failed_push " << soid << " from shard " << from
	  << ", reps on " << missing_loc.get_locations(soid)
	  << " unfound? " << missing_loc.is_unfound(soid) << dendl;
  finish_recovery_op(soid);  // close out this attempt,
}

void ReplicatedBackend::_failed_push(pg_shard_t from, const hobject_t &soid)
{
  get_parent()->failed_push(from, soid);
  pull_from_peer[from].erase(soid);
  if (pull_from_peer[from].empty())
    pull_from_peer.erase(from);
  pulling.erase(soid);
}

void ReplicatedPG::sub_op_remove(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_remove " << m->poid << dendl;

  op->mark_started();

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  remove_snap_mapped_object(*t, m->poid);
  int r = osd->store->queue_transaction_and_cleanup(osr.get(), t);
  assert(r == 0);
}


eversion_t ReplicatedPG::pick_newest_available(const hobject_t& oid)
{
  eversion_t v;

  assert(pg_log.get_missing().is_missing(oid));
  v = pg_log.get_missing().missing.find(oid)->second.have;
  dout(10) << "pick_newest_available " << oid << " " << v << " on osd." << osd->whoami << " (local)" << dendl;

  assert(actingbackfill.size() > 0);
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    if (!peer_missing[peer].is_missing(oid)) {
      assert(is_backfill_targets(peer));
      continue;
    }
    eversion_t h = peer_missing[peer].missing[oid].have;
    dout(10) << "pick_newest_available " << oid << " " << h << " on osd." << peer << dendl;
    if (h > v)
      v = h;
  }

  dout(10) << "pick_newest_available " << oid << " " << v << " (newest)" << dendl;
  return v;
}


/* Mark an object as lost
 */
ObjectContextRef ReplicatedPG::mark_object_lost(ObjectStore::Transaction *t,
							    const hobject_t &oid, eversion_t version,
							    utime_t mtime, int what)
{
  // Wake anyone waiting for this object. Now that it's been marked as lost,
  // we will just return an error code.
  map<hobject_t, list<OpRequestRef> >::iterator wmo =
    waiting_for_unreadable_object.find(oid);
  if (wmo != waiting_for_unreadable_object.end()) {
    requeue_ops(wmo->second);
  }

  // Add log entry
  ++info.last_update.version;
  pg_log_entry_t e(what, oid, info.last_update, version, 0, osd_reqid_t(), mtime);
  pg_log.add(e);
  
  ObjectContextRef obc = get_object_context(oid, true);

  obc->ondisk_write_lock();

  obc->obs.oi.set_flag(object_info_t::FLAG_LOST);
  obc->obs.oi.version = info.last_update;
  obc->obs.oi.prior_version = version;

  bufferlist b2;
  obc->obs.oi.encode(b2);
  t->setattr(coll, oid, OI_ATTR, b2);

  return obc;
}

struct C_PG_MarkUnfoundLost : public Context {
  ReplicatedPGRef pg;
  list<ObjectContextRef> obcs;
  C_PG_MarkUnfoundLost(ReplicatedPG *p) : pg(p) {}
  void finish(int r) {
    pg->_finish_mark_all_unfound_lost(obcs);
  }
};

/* Mark all unfound objects as lost.
 */
void ReplicatedPG::mark_all_unfound_lost(int what)
{
  dout(3) << __func__ << " " << pg_log_entry_t::get_op_name(what) << dendl;

  dout(30) << __func__ << ": log before:\n";
  pg_log.get_log().print(*_dout);
  *_dout << dendl;

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  C_PG_MarkUnfoundLost *c = new C_PG_MarkUnfoundLost(this);

  utime_t mtime = ceph_clock_now(cct);
  info.last_update.epoch = get_osdmap()->get_epoch();
  const pg_missing_t &missing = pg_log.get_missing();
  map<hobject_t, pg_missing_t::item>::const_iterator m =
    missing_loc.get_all_missing().begin();
  map<hobject_t, pg_missing_t::item>::const_iterator mend =
    missing_loc.get_all_missing().end();
  while (m != mend) {
    const hobject_t &oid(m->first);
    if (!missing_loc.is_unfound(oid)) {
      // We only care about unfound objects
      ++m;
      continue;
    }

    ObjectContextRef obc;
    eversion_t prev;

    switch (what) {
    case pg_log_entry_t::LOST_MARK:
      obc = mark_object_lost(t, oid, m->second.need, mtime, pg_log_entry_t::LOST_MARK);
      pg_log.missing_got(m++);
      assert(0 == "actually, not implemented yet!");
      // we need to be careful about how this is handled on the replica!
      break;

    case pg_log_entry_t::LOST_REVERT:
      prev = pick_newest_available(oid);
      if (prev > eversion_t()) {
	// log it
	++info.last_update.version;
	pg_log_entry_t e(
	  pg_log_entry_t::LOST_REVERT, oid, info.last_update,
	  m->second.need, 0, osd_reqid_t(), mtime);
	e.reverting_to = prev;
	pg_log.add(e);
	dout(10) << e << dendl;

	// we are now missing the new version; recovery code will sort it out.
	++m;
	pg_log.revise_need(oid, info.last_update);
	missing_loc.revise_need(oid, info.last_update);
	break;
      }
      /** fall-thru **/

    case pg_log_entry_t::LOST_DELETE:
      {
	// log it
      	++info.last_update.version;
	pg_log_entry_t e(pg_log_entry_t::LOST_DELETE, oid, info.last_update, m->second.need,
		     0, osd_reqid_t(), mtime);
	pg_log.add(e);
	dout(10) << e << dendl;

	t->remove(
	  coll,
	  ghobject_t(oid, ghobject_t::NO_GEN, pg_whoami.shard));
	pg_log.missing_add_event(e);
	++m;
	missing_loc.recovered(oid);
      }
      break;

    default:
      assert(0);
    }

    if (obc)
      c->obcs.push_back(obc);
  }

  dout(30) << __func__ << ": log after:\n";
  pg_log.get_log().print(*_dout);
  *_dout << dendl;

  info.stats.stats_invalid = true;

  if (missing.num_missing() == 0) {
    // advance last_complete since nothing else is missing!
    info.last_complete = info.last_update;
  }

  dirty_info = true;
  write_if_dirty(*t);

  t->register_on_complete(new ObjectStore::C_DeleteTransaction(t));

  osd->store->queue_transaction(osr.get(), t, c, NULL, new C_OSD_OndiskWriteUnlockList(&c->obcs));
	      
  // Send out the PG log to all replicas
  // So that they know what is lost
  share_pg_log();

  // queue ourselves so that we push the (now-lost) object_infos to replicas.
  osd->queue_for_recovery(this);
}

void ReplicatedPG::_finish_mark_all_unfound_lost(list<ObjectContextRef>& obcs)
{
  lock();
  dout(10) << "_finish_mark_all_unfound_lost " << dendl;

  if (!deleting)
    requeue_ops(waiting_for_all_missing);
  waiting_for_all_missing.clear();

  obcs.clear();
  unlock();
}

void ReplicatedPG::_split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  assert(repop_queue.empty());
}

/*
 * pg status change notification
 */

void ReplicatedPG::apply_and_flush_repops(bool requeue)
{
  list<OpRequestRef> rq;

  // apply all repops
  while (!repop_queue.empty()) {
    RepGather *repop = repop_queue.front();
    repop_queue.pop_front();
    dout(10) << " canceling repop tid " << repop->rep_tid << dendl;
    repop->rep_aborted = true;
    if (repop->on_applied) {
      delete repop->on_applied;
      repop->on_applied = NULL;
    }

    if (requeue) {
      if (repop->ctx->op) {
	dout(10) << " requeuing " << *repop->ctx->op->get_req() << dendl;
	rq.push_back(repop->ctx->op);
	repop->ctx->op = OpRequestRef();
      }

      // also requeue any dups, interleaved into position
      map<eversion_t, list<OpRequestRef> >::iterator p = waiting_for_ondisk.find(repop->v);
      if (p != waiting_for_ondisk.end()) {
	dout(10) << " also requeuing ondisk waiters " << p->second << dendl;
	rq.splice(rq.end(), p->second);
	waiting_for_ondisk.erase(p);
      }
    }

    remove_repop(repop);
  }

  if (requeue) {
    requeue_ops(rq);
    if (!waiting_for_ondisk.empty()) {
      for (map<eversion_t, list<OpRequestRef> >::iterator i =
	     waiting_for_ondisk.begin();
	   i != waiting_for_ondisk.end();
	   ++i) {
	for (list<OpRequestRef>::iterator j = i->second.begin();
	     j != i->second.end();
	     ++j) {
	  derr << __func__ << ": op " << *((*j)->get_req()) << " waiting on "
	       << i->first << dendl;
	}
      }
      assert(waiting_for_ondisk.empty());
    }
  }

  waiting_for_ondisk.clear();
  waiting_for_ack.clear();
}

void ReplicatedPG::on_flushed()
{
  assert(flushes_in_progress > 0);
  flushes_in_progress--;
  if (flushes_in_progress == 0) {
    requeue_ops(waiting_for_active);
  }
  if (!is_active() || !is_primary()) {
    pair<hobject_t, ObjectContextRef> i;
    while (object_contexts.get_next(i.first, &i)) {
      derr << "on_flushed: object " << i.first << " obc still alive" << dendl;
    }
    assert(object_contexts.empty());
  }
  pgbackend->on_flushed();
}

void ReplicatedPG::on_removal(ObjectStore::Transaction *t)
{
  dout(10) << "on_removal" << dendl;

  // adjust info to backfill
  info.last_backfill = hobject_t();
  dirty_info = true;


  // clear log
  PGLogEntryHandler rollbacker;
  pg_log.clear_can_rollback_to(&rollbacker);
  rollbacker.apply(this, t);

  write_if_dirty(*t);

  on_shutdown();
}

void ReplicatedPG::on_shutdown()
{
  dout(10) << "on_shutdown" << dendl;

  // remove from queues
  osd->recovery_wq.dequeue(this);
  osd->scrub_wq.dequeue(this);
  osd->scrub_finalize_wq.dequeue(this);
  osd->snap_trim_wq.dequeue(this);
  osd->pg_stat_queue_dequeue(this);
  osd->dequeue_pg(this, 0);
  osd->peering_wq.dequeue(this);

  // handles queue races
  deleting = true;

  unreg_next_scrub();
  cancel_copy_ops(false);
  cancel_flush_ops(false);
  apply_and_flush_repops(false);

  pgbackend->on_change();

  context_registry_on_change();

  osd->remote_reserver.cancel_reservation(info.pgid);
  osd->local_reserver.cancel_reservation(info.pgid);

  clear_primary_state();
  cancel_recovery();
}

void ReplicatedPG::on_activate()
{
  // all clean?
  if (needs_recovery()) {
    dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  DoRecovery())));
  } else if (needs_backfill()) {
    dout(10) << "activate queueing backfill" << dendl;
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  RequestBackfill())));
  } else {
    dout(10) << "activate all replicas clean, no recovery" << dendl;
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  AllReplicasRecovered())));
  }

  publish_stats_to_osd();

  if (!backfill_targets.empty()) {
    last_backfill_started = earliest_backfill();
    new_backfill = true;
    assert(!last_backfill_started.is_max());
    dout(5) << "on activate: bft=" << backfill_targets
	   << " from " << last_backfill_started << dendl;
    for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	 i != backfill_targets.end();
	 ++i) {
      dout(5) << "target shard " << *i
	     << " from " << peer_info[*i].last_backfill
	     << dendl;
    }
  }

  hit_set_setup();
  agent_setup();
}

void ReplicatedPG::on_change(ObjectStore::Transaction *t)
{
  dout(10) << "on_change" << dendl;

  if (hit_set && hit_set->insert_count() == 0) {
    dout(20) << " discarding empty hit_set" << dendl;
    hit_set_clear();
  }

  // requeue everything in the reverse order they should be
  // reexamined.

  clear_scrub_reserved();
  scrub_clear_state();

  context_registry_on_change();

  for (list<pair<OpRequestRef, OpContext*> >::iterator i =
         in_progress_async_reads.begin();
       i != in_progress_async_reads.end();
       in_progress_async_reads.erase(i++)) {
    close_op_ctx(i->second, -ECANCELED);
    requeue_op(i->first);
  }

  cancel_copy_ops(is_primary());
  cancel_flush_ops(is_primary());

  // requeue object waiters
  if (is_primary()) {
    requeue_object_waiters(waiting_for_unreadable_object);
  } else {
    waiting_for_unreadable_object.clear();
  }
  for (map<hobject_t,list<OpRequestRef> >::iterator p = waiting_for_degraded_object.begin();
       p != waiting_for_degraded_object.end();
       waiting_for_degraded_object.erase(p++)) {
    if (is_primary())
      requeue_ops(p->second);
    else
      p->second.clear();
    finish_degraded_object(p->first);
  }
  for (map<hobject_t,list<OpRequestRef> >::iterator p = waiting_for_blocked_object.begin();
       p != waiting_for_blocked_object.end();
       waiting_for_blocked_object.erase(p++)) {
    if (is_primary())
      requeue_ops(p->second);
    else
      p->second.clear();
  }
  for (map<hobject_t, list<Context*> >::iterator i =
	 callbacks_for_degraded_object.begin();
       i != callbacks_for_degraded_object.end();
    ) {
    finish_degraded_object((i++)->first);
  }
  assert(callbacks_for_degraded_object.empty());

  if (is_primary()) {
    requeue_ops(waiting_for_cache_not_full);
    requeue_ops(waiting_for_all_missing);
  } else {
    waiting_for_cache_not_full.clear();
    waiting_for_all_missing.clear();
  }

  // this will requeue ops we were working on but didn't finish, and
  // any dups
  apply_and_flush_repops(is_primary());

  pgbackend->on_change_cleanup(t);
  pgbackend->on_change();

  // clear snap_trimmer state
  snap_trimmer_machine.process_event(Reset());

  debug_op_order.clear();
  unstable_stats.clear();
}

void ReplicatedPG::on_role_change()
{
  dout(10) << "on_role_change" << dendl;
  if (get_role() != 0 && hit_set) {
    dout(10) << " clearing hit set" << dendl;
    hit_set_clear();
  }
}

void ReplicatedPG::on_pool_change()
{
  dout(10) << __func__ << dendl;
  // requeue cache full waiters just in case the cache_mode is
  // changing away from writeback mode.  note that if we are not
  // active the normal requeuing machinery is sufficient (and properly
  // ordered).
  if (is_active() &&
      pool.info.cache_mode != pg_pool_t::CACHEMODE_WRITEBACK &&
      !waiting_for_cache_not_full.empty()) {
    dout(10) << __func__ << " requeuing full waiters (not in writeback) "
	     << dendl;
    requeue_ops(waiting_for_cache_not_full);
  }
  hit_set_setup();
  agent_setup();
}

// clear state.  called on recovery completion AND cancellation.
void ReplicatedPG::_clear_recovery_state()
{
  missing_loc.clear();
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
  last_backfill_started = hobject_t();
  list<OpRequestRef> blocked_ops;
  set<hobject_t>::iterator i = backfills_in_flight.begin();
  while (i != backfills_in_flight.end()) {
    assert(recovering.count(*i));
    recovering[*i]->drop_backfill_read(&blocked_ops);
    requeue_ops(blocked_ops);
    backfills_in_flight.erase(i++);
  }
  assert(backfills_in_flight.empty());
  pending_backfill_updates.clear();
  recovering.clear();
  pgbackend->clear_state();
}

void ReplicatedPG::cancel_pull(const hobject_t &soid)
{
  dout(20) << __func__ << ": soid" << dendl;
  assert(recovering.count(soid));
  recovering.erase(soid);
  finish_recovery_op(soid);
  if (waiting_for_degraded_object.count(soid)) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(waiting_for_degraded_object[soid]);
    waiting_for_degraded_object.erase(soid);
  }
  if (waiting_for_unreadable_object.count(soid)) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(waiting_for_unreadable_object[soid]);
    waiting_for_unreadable_object.erase(soid);
  }
  if (is_missing_object(soid))
    pg_log.set_last_requested(0); // get recover_primary to start over
}

void ReplicatedPG::check_recovery_sources(const OSDMapRef osdmap)
{
  /*
   * check that any peers we are planning to (or currently) pulling
   * objects from are dealt with.
   */
  missing_loc.check_recovery_sources(osdmap);
  pgbackend->check_recovery_sources(osdmap);

  for (set<pg_shard_t>::iterator i = peer_log_requested.begin();
       i != peer_log_requested.end();
       ) {
    if (!osdmap->is_up(i->osd)) {
      dout(10) << "peer_log_requested removing " << *i << dendl;
      peer_log_requested.erase(i++);
    } else {
      ++i;
    }
  }

  for (set<pg_shard_t>::iterator i = peer_missing_requested.begin();
       i != peer_missing_requested.end();
       ) {
    if (!osdmap->is_up(i->osd)) {
      dout(10) << "peer_missing_requested removing " << *i << dendl;
      peer_missing_requested.erase(i++);
    } else {
      ++i;
    }
  }
}

void PG::MissingLoc::check_recovery_sources(const OSDMapRef osdmap)
{
  set<pg_shard_t> now_down;
  for (set<pg_shard_t>::iterator p = missing_loc_sources.begin();
       p != missing_loc_sources.end();
       ) {
    if (osdmap->is_up(p->osd)) {
      ++p;
      continue;
    }
    dout(10) << "check_recovery_sources source osd." << *p << " now down" << dendl;
    now_down.insert(*p);
    missing_loc_sources.erase(p++);
  }

  if (now_down.empty()) {
    dout(10) << "check_recovery_sources no source osds (" << missing_loc_sources << ") went down" << dendl;
  } else {
    dout(10) << "check_recovery_sources sources osds " << now_down << " now down, remaining sources are "
	     << missing_loc_sources << dendl;
    
    // filter missing_loc
    map<hobject_t, set<pg_shard_t> >::iterator p = missing_loc.begin();
    while (p != missing_loc.end()) {
      set<pg_shard_t>::iterator q = p->second.begin();
      while (q != p->second.end())
	if (now_down.count(*q)) {
	  p->second.erase(q++);
	} else {
	  ++q;
	}
      if (p->second.empty())
	missing_loc.erase(p++);
      else
	++p;
    }
  }
}
  

bool ReplicatedPG::start_recovery_ops(
  int max, RecoveryCtx *prctx,
  ThreadPool::TPHandle &handle,
  int *ops_started)
{
  int& started = *ops_started;
  started = 0;
  bool work_in_progress = false;
  assert(is_primary());

  if (!state_test(PG_STATE_RECOVERING) &&
      !state_test(PG_STATE_BACKFILL)) {
    /* TODO: I think this case is broken and will make do_recovery()
     * unhappy since we're returning false */
    dout(10) << "recovery raced and were queued twice, ignoring!" << dendl;
    return false;
  }

  const pg_missing_t &missing = pg_log.get_missing();

  int num_missing = missing.num_missing();
  int num_unfound = get_num_unfound();

  if (num_missing == 0) {
    info.last_complete = info.last_update;
  }

  if (num_missing == num_unfound) {
    // All of the missing objects we have are unfound.
    // Recover the replicas.
    started = recover_replicas(max, handle);
  }
  if (!started) {
    // We still have missing objects that we should grab from replicas.
    started += recover_primary(max, handle);
  }
  if (!started && num_unfound != get_num_unfound()) {
    // second chance to recovery replicas
    started = recover_replicas(max, handle);
  }

  if (started)
    work_in_progress = true;

  bool deferred_backfill = false;
  if (recovering.empty() &&
      state_test(PG_STATE_BACKFILL) &&
      !backfill_targets.empty() && started < max &&
      missing.num_missing() == 0 &&
      waiting_on_backfill.empty()) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
      dout(10) << "deferring backfill due to NOBACKFILL" << dendl;
      deferred_backfill = true;
    } else if (!backfill_reserved) {
      dout(10) << "deferring backfill due to !backfill_reserved" << dendl;
      if (!backfill_reserving) {
	dout(10) << "queueing RequestBackfill" << dendl;
	backfill_reserving = true;
	queue_peering_event(
	  CephPeeringEvtRef(
	    new CephPeeringEvt(
	      get_osdmap()->get_epoch(),
	      get_osdmap()->get_epoch(),
	      RequestBackfill())));
      }
      deferred_backfill = true;
    } else {
      started += recover_backfill(max - started, handle, &work_in_progress);
    }
  }

  dout(10) << " started " << started << dendl;
  osd->logger->inc(l_osd_rop, started);

  if (!recovering.empty() ||
      work_in_progress || recovery_ops_active > 0 || deferred_backfill)
    return work_in_progress;

  assert(recovering.empty());
  assert(recovery_ops_active == 0);

  dout(10) << __func__ << " needs_recovery: "
	   << missing_loc.get_needs_recovery()
	   << dendl;
  dout(10) << __func__ << " missing_loc: "
	   << missing_loc.get_missing_locs()
	   << dendl;
  int unfound = get_num_unfound();
  if (unfound) {
    dout(10) << " still have " << unfound << " unfound" << dendl;
    return work_in_progress;
  }

  if (missing.num_missing() > 0) {
    // this shouldn't happen!
    osd->clog.error() << info.pgid << " recovery ending with " << missing.num_missing()
		      << ": " << missing.missing << "\n";
    return work_in_progress;
  }

  if (needs_recovery()) {
    // this shouldn't happen!
    // We already checked num_missing() so we must have missing replicas
    osd->clog.error() << info.pgid << " recovery ending with missing replicas\n";
    return work_in_progress;
  }

  if (state_test(PG_STATE_RECOVERING)) {
    state_clear(PG_STATE_RECOVERING);
    if (needs_backfill()) {
      dout(10) << "recovery done, queuing backfill" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            RequestBackfill())));
    } else {
      dout(10) << "recovery done, no backfill" << dendl;
      queue_peering_event(
        CephPeeringEvtRef(
          new CephPeeringEvt(
            get_osdmap()->get_epoch(),
            get_osdmap()->get_epoch(),
            AllReplicasRecovered())));
    }
  } else { // backfilling
    state_clear(PG_STATE_BACKFILL);
    dout(10) << "recovery done, backfill done" << dendl;
    queue_peering_event(
      CephPeeringEvtRef(
        new CephPeeringEvt(
          get_osdmap()->get_epoch(),
          get_osdmap()->get_epoch(),
          Backfilled())));
  }

  return false;
}

/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
int ReplicatedPG::recover_primary(int max, ThreadPool::TPHandle &handle)
{
  assert(is_primary());

  const pg_missing_t &missing = pg_log.get_missing();

  dout(10) << "recover_primary recovering " << recovering.size()
	   << " in pg" << dendl;
  dout(10) << "recover_primary " << missing << dendl;
  dout(25) << "recover_primary " << missing.missing << dendl;

  // look at log!
  pg_log_entry_t *latest = 0;
  int started = 0;
  int skipped = 0;

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  map<version_t, hobject_t>::const_iterator p =
    missing.rmissing.lower_bound(pg_log.get_log().last_requested);
  while (p != missing.rmissing.end()) {
    handle.reset_tp_timeout();
    hobject_t soid;
    version_t v = p->first;

    if (pg_log.get_log().objects.count(p->second)) {
      latest = pg_log.get_log().objects.find(p->second)->second;
      assert(latest->is_update());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    const pg_missing_t::item& item = missing.missing.find(p->second)->second;
    ++p;

    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    eversion_t need = item.need;

    bool unfound = missing_loc.is_unfound(soid);

    dout(10) << "recover_primary "
             << soid << " " << item.need
	     << (unfound ? " (unfound)":"")
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (recovering.count(soid) ? " (recovering)":"")
	     << (recovering.count(head) ? " (recovering head)":"")
             << dendl;

    if (latest) {
      switch (latest->op) {
      case pg_log_entry_t::CLONE:
	/*
	 * Handling for this special case removed for now, until we
	 * can correctly construct an accurate SnapSet from the old
	 * one.
	 */
	break;

      case pg_log_entry_t::LOST_REVERT:
	{
	  if (item.have == latest->reverting_to) {
	    ObjectContextRef obc = get_object_context(soid, true);
	    
	    if (obc->obs.oi.version == latest->version) {
	      // I'm already reverting
	      dout(10) << " already reverting " << soid << dendl;
	    } else {
	      dout(10) << " reverting " << soid << " to " << latest->prior_version << dendl;
	      obc->ondisk_write_lock();
	      obc->obs.oi.version = latest->version;

	      ObjectStore::Transaction *t = new ObjectStore::Transaction;
	      t->register_on_applied(new ObjectStore::C_DeleteTransaction(t));
	      bufferlist b2;
	      obc->obs.oi.encode(b2);
	      t->setattr(coll, soid, OI_ATTR, b2);

	      recover_got(soid, latest->version);
	      missing_loc.add_location(soid, pg_whoami);

	      ++active_pushes;

	      osd->store->queue_transaction(osr.get(), t,
					    new C_OSD_AppliedRecoveredObject(this, obc),
					    new C_OSD_CommittedPushedObject(
					      this,
					      get_osdmap()->get_epoch(),
					      info.last_complete),
					    new C_OSD_OndiskWriteUnlock(obc));
	      continue;
	    }
	  } else {
	    /*
	     * Pull the old version of the object.  Update missing_loc here to have the location
	     * of the version we want.
	     *
	     * This doesn't use the usual missing_loc paths, but that's okay:
	     *  - if we have it locally, we hit the case above, and go from there.
	     *  - if we don't, we always pass through this case during recovery and set up the location
	     *    properly.
	     *  - this way we don't need to mangle the missing code to be general about needing an old
	     *    version...
	     */
	    eversion_t alternate_need = latest->reverting_to;
	    dout(10) << " need to pull prior_version " << alternate_need << " for revert " << item << dendl;

	    for (map<pg_shard_t, pg_missing_t>::iterator p = peer_missing.begin();
		 p != peer_missing.end();
		 ++p)
	      if (p->second.is_missing(soid, need) &&
		  p->second.missing[soid].have == alternate_need) {
		missing_loc.add_location(soid, p->first);
	      }
	    dout(10) << " will pull " << alternate_need << " or " << need
		     << " from one of " << missing_loc.get_locations(soid)
		     << dendl;
	    unfound = false;
	  }
	}
	break;
      }
    }
   
    if (!recovering.count(soid)) {
      if (recovering.count(head)) {
	++skipped;
      } else if (unfound) {
	++skipped;
      } else {
	int r = recover_missing(
	  soid, need, cct->_conf->osd_recovery_op_priority, h);
	switch (r) {
	case PULL_YES:
	  ++started;
	  break;
	case PULL_OTHER:
	  ++started;
	case PULL_NONE:
	  ++skipped;
	  break;
	default:
	  assert(0);
	}
	if (started >= max)
	  break;
      }
    }
    
    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      pg_log.set_last_requested(v);
  }
 
  pgbackend->run_recovery_op(h, cct->_conf->osd_recovery_op_priority);
  return started;
}

int ReplicatedPG::prep_object_replica_pushes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h)
{
  assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc) {
    pg_log.missing_add(soid, v, eversion_t());
    missing_loc.remove_location(soid, pg_whoami);
    bool uhoh = true;
    assert(actingbackfill.size() > 0);
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == get_primary()) continue;
      pg_shard_t peer = *i;
      if (!peer_missing[peer].is_missing(soid, v)) {
	missing_loc.add_location(soid, peer);
	dout(10) << info.pgid << " unexpectedly missing " << soid << " v" << v
		 << ", there should be a copy on shard " << peer << dendl;
	uhoh = false;
      }
    }
    if (uhoh)
      osd->clog.error() << info.pgid << " missing primary copy of " << soid << ", unfound\n";
    else
      osd->clog.error() << info.pgid << " missing primary copy of " << soid
			<< ", will try copies on " << missing_loc.get_locations(soid)
			<< "\n";
    return 0;
  }

  start_recovery_op(soid);
  assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));

  /* We need this in case there is an in progress write on the object.  In fact,
   * the only possible write is an update to the xattr due to a lost_revert --
   * a client write would be blocked since the object is degraded.
   * In almost all cases, therefore, this lock should be uncontended.
   */
  obc->ondisk_read_lock();
  pgbackend->recover_object(
    soid,
    v,
    ObjectContextRef(),
    obc, // has snapset context
    h);
  obc->ondisk_read_unlock();
  return 1;
}

int ReplicatedBackend::start_pushes(
  const hobject_t &soid,
  ObjectContextRef obc,
  RPGHandle *h)
{
  int pushes = 0;
  // who needs it?  
  assert(get_parent()->get_actingbackfill_shards().size() > 0);
  for (set<pg_shard_t>::iterator i =
	 get_parent()->get_actingbackfill_shards().begin();
       i != get_parent()->get_actingbackfill_shards().end();
       ++i) {
    if (*i == get_parent()->whoami_shard()) continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator j =
      get_parent()->get_shard_missing().find(peer);
    assert(j != get_parent()->get_shard_missing().end());
    if (j->second.is_missing(soid)) {
      ++pushes;
      h->pushes[peer].push_back(PushOp());
      prep_push_to_replica(obc, soid, peer,
			   &(h->pushes[peer].back())
	);
    }
  }
  return pushes;
}

int ReplicatedPG::recover_replicas(int max, ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << "(" << max << ")" << dendl;
  int started = 0;

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();

  // this is FAR from an optimal recovery order.  pretty lame, really.
  assert(actingbackfill.size() > 0);
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    map<pg_shard_t, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    assert(pm != peer_missing.end());
    map<pg_shard_t, pg_info_t>::const_iterator pi = peer_info.find(peer);
    assert(pi != peer_info.end());
    size_t m_sz = pm->second.num_missing();

    dout(10) << " peer osd." << peer << " missing " << m_sz << " objects." << dendl;
    dout(20) << " peer osd." << peer << " missing " << pm->second.missing << dendl;

    // oldest first!
    const pg_missing_t &m(pm->second);
    for (map<version_t, hobject_t>::const_iterator p = m.rmissing.begin();
	   p != m.rmissing.end() && started < max;
	   ++p) {
      handle.reset_tp_timeout();
      const hobject_t soid(p->second);

      if (soid > pi->second.last_backfill) {
	if (!recovering.count(soid)) {
	  derr << __func__ << ": object added to missing set for backfill, but "
	       << "is not in recovering, error!" << dendl;
	  assert(0);
	}
	continue;
      }

      if (recovering.count(soid)) {
	dout(10) << __func__ << ": already recovering " << soid << dendl;
	continue;
      }

      if (missing_loc.is_unfound(soid)) {
	dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	continue;
      }

      if (soid.is_snap() && pg_log.get_missing().is_missing(soid.get_head())) {
	dout(10) << __func__ << ": " << soid.get_head()
		 << " still missing on primary" << dendl;
	continue;
      }

      if (soid.is_snap() && pg_log.get_missing().is_missing(soid.get_snapdir())) {
	dout(10) << __func__ << ": " << soid.get_snapdir()
		 << " still missing on primary" << dendl;
	continue;
      }

      if (pg_log.get_missing().is_missing(soid)) {
	dout(10) << __func__ << ": " << soid << " still missing on primary" << dendl;
	continue;
      }

      dout(10) << __func__ << ": recover_object_replicas(" << soid << ")" << dendl;
      map<hobject_t,pg_missing_t::item>::const_iterator r = m.missing.find(soid);
      started += prep_object_replica_pushes(soid, r->second.need,
					    h);
    }
  }

  pgbackend->run_recovery_op(h, cct->_conf->osd_recovery_op_priority);
  return started;
}

hobject_t ReplicatedPG::earliest_peer_backfill() const
{
  hobject_t e = hobject_t::get_max();
  for (set<pg_shard_t>::const_iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    pg_shard_t peer = *i;
    map<pg_shard_t, BackfillInterval>::const_iterator iter =
      peer_backfill_info.find(peer);
    assert(iter != peer_backfill_info.end());
    if (iter->second.begin < e)
      e = iter->second.begin;
  }
  return e;
}

bool ReplicatedPG::all_peer_done() const
{
  // Primary hasn't got any more objects
  assert(backfill_info.empty());

  for (set<pg_shard_t>::const_iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    pg_shard_t bt = *i;
    map<pg_shard_t, BackfillInterval>::const_iterator piter =
      peer_backfill_info.find(bt);
    assert(piter != peer_backfill_info.end());
    const BackfillInterval& pbi = piter->second;
    // See if peer has more to process
    if (!pbi.extends_to_end() || !pbi.empty())
	return false;
  }
  return true;
}

/**
 * recover_backfill
 *
 * Invariants:
 *
 * backfilled: fully pushed to replica or present in replica's missing set (both
 * our copy and theirs).
 *
 * All objects on a backfill_target in
 * [MIN,peer_backfill_info[backfill_target].begin) are either
 * not present or backfilled (all removed objects have been removed).
 * There may be PG objects in this interval yet to be backfilled.
 *
 * All objects in PG in [MIN,backfill_info.begin) have been backfilled to all
 * backfill_targets.  There may be objects on backfill_target(s) yet to be deleted.
 *
 * For a backfill target, all objects < MIN(peer_backfill_info[target].begin,
 *     backfill_info.begin) in PG are backfilled.  No deleted objects in this
 * interval remain on the backfill target.
 *
 * For a backfill target, all objects <= peer_info[target].last_backfill
 * have been backfilled to target
 *
 * There *MAY* be objects between last_backfill_started and
 * MIN(peer_backfill_info[*].begin, backfill_info.begin) in the event that client
 * io created objects since the last scan.  For this reason, we call
 * update_range() again before continuing backfill.
 */
int ReplicatedPG::recover_backfill(
  int max,
  ThreadPool::TPHandle &handle, bool *work_started)
{
  dout(10) << "recover_backfill (" << max << ")"
           << " bft=" << backfill_targets
	   << " last_backfill_started " << last_backfill_started << dendl;
  assert(!backfill_targets.empty());

  // Initialize from prior backfill state
  if (new_backfill) {
    // on_activate() was called prior to getting here
    assert(last_backfill_started == earliest_backfill());
    new_backfill = false;
    for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	 i != backfill_targets.end();
	 ++i) {
      peer_backfill_info[*i].reset(peer_info[*i].last_backfill);
    }
    backfill_info.reset(last_backfill_started);
  }

  for (set<pg_shard_t>::iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    dout(10) << "peer osd." << *i
	   << " info " << peer_info[*i]
	   << " interval " << peer_backfill_info[*i].begin
	   << "-" << peer_backfill_info[*i].end
	   << " " << peer_backfill_info[*i].objects.size() << " objects"
	   << dendl;
  }

  // update our local interval to cope with recent changes
  backfill_info.begin = last_backfill_started;
  update_range(&backfill_info, handle);

  int ops = 0;
  vector<boost::tuple<hobject_t, eversion_t,
                      ObjectContextRef, vector<pg_shard_t> > > to_push;
  vector<boost::tuple<hobject_t, eversion_t, pg_shard_t> > to_remove;
  set<hobject_t> add_to_stat;

  for (set<pg_shard_t>::iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    peer_backfill_info[*i].trim_to(
      MAX(peer_info[*i].last_backfill, last_backfill_started));
  }
  backfill_info.trim_to(last_backfill_started);

  hobject_t backfill_pos = MIN(backfill_info.begin, earliest_peer_backfill());
  while (ops < max) {
    if (backfill_info.begin <= earliest_peer_backfill() &&
	!backfill_info.extends_to_end() && backfill_info.empty()) {
      hobject_t next = backfill_info.end;
      backfill_info.clear();
      backfill_info.begin = next;
      backfill_info.end = hobject_t::get_max();
      update_range(&backfill_info, handle);
      backfill_info.trim();
    }
    backfill_pos = MIN(backfill_info.begin, earliest_peer_backfill());

    dout(20) << "   my backfill interval " << backfill_info.begin << "-" << backfill_info.end
	     << " " << backfill_info.objects.size() << " objects"
	     << " " << backfill_info.objects
	     << dendl;

    bool sent_scan = false;
    for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	 i != backfill_targets.end();
	 ++i) {
      pg_shard_t bt = *i;
      BackfillInterval& pbi = peer_backfill_info[bt];

      dout(20) << " peer shard " << bt << " backfill " << pbi.begin << "-"
	       << pbi.end << " " << pbi.objects << dendl;
      if (pbi.begin <= backfill_info.begin &&
	  !pbi.extends_to_end() && pbi.empty()) {
	dout(10) << " scanning peer osd." << bt << " from " << pbi.end << dendl;
	epoch_t e = get_osdmap()->get_epoch();
	MOSDPGScan *m = new MOSDPGScan(
	  MOSDPGScan::OP_SCAN_GET_DIGEST, pg_whoami, e, e,
	  spg_t(info.pgid.pgid, bt.shard),
	  pbi.end, hobject_t());
	osd->send_message_osd_cluster(bt.osd, m, get_osdmap()->get_epoch());
	assert(waiting_on_backfill.find(bt) == waiting_on_backfill.end());
	waiting_on_backfill.insert(bt);
        sent_scan = true;
      }
    }

    // Count simultaneous scans as a single op and let those complete
    if (sent_scan) {
        ops++;
	start_recovery_op(hobject_t::get_max()); // XXX: was pbi.end
        break;
    }

    if (backfill_info.empty() && all_peer_done()) {
      dout(10) << " reached end for both local and all peers" << dendl;
      break;
    }

    // Get object within set of peers to operate on and
    // the set of targets for which that object applies.
    hobject_t check = earliest_peer_backfill();

    if (check < backfill_info.begin) {

      set<pg_shard_t> check_targets;
      for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	   i != backfill_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        if (pbi.begin == check)
          check_targets.insert(bt);
      }
      assert(!check_targets.empty());

      dout(20) << " BACKFILL removing " << check
	       << " from peers " << check_targets << dendl;
      for (set<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        assert(pbi.begin == check);

        to_remove.push_back(boost::make_tuple(check, pbi.objects.begin()->second, bt));
        pbi.pop_front();
      }
      last_backfill_started = check;
      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
    } else {
      eversion_t& obj_v = backfill_info.objects.begin()->second;

      vector<pg_shard_t> need_ver_targs, missing_targs, keep_ver_targs, skip_targs;
      for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	   i != backfill_targets.end();
	   ++i) {
	pg_shard_t bt = *i;
	BackfillInterval& pbi = peer_backfill_info[bt];
        // Find all check peers that have the wrong version
	if (check == backfill_info.begin && check == pbi.begin) {
	  if (pbi.objects.begin()->second != obj_v) {
	    need_ver_targs.push_back(bt);
	  } else {
	    keep_ver_targs.push_back(bt);
	  }
        } else {
	  pg_info_t& pinfo = peer_info[bt];

          // Only include peers that we've caught up to their backfill line
	  // otherwise, they only appear to be missing this object
	  // because their pbi.begin > backfill_info.begin.
          if (backfill_info.begin > pinfo.last_backfill)
	    missing_targs.push_back(bt);
	  else
	    skip_targs.push_back(bt);
	}
      }

      if (!keep_ver_targs.empty()) {
        // These peers have version obj_v
	dout(20) << " BACKFILL keeping " << check
		 << " with ver " << obj_v
		 << " on peers " << keep_ver_targs << dendl;
	//assert(!waiting_for_degraded_object.count(check));
      }
      if (!need_ver_targs.empty() || !missing_targs.empty()) {
	ObjectContextRef obc = get_object_context(backfill_info.begin, false);
	assert(obc);
	if (obc->get_backfill_read()) {
	  if (!need_ver_targs.empty()) {
	    dout(20) << " BACKFILL replacing " << check
		   << " with ver " << obj_v
		   << " to peers " << need_ver_targs << dendl;
	  }
	  if (!missing_targs.empty()) {
	    dout(20) << " BACKFILL pushing " << backfill_info.begin
	         << " with ver " << obj_v
	         << " to peers " << missing_targs << dendl;
	  }
	  vector<pg_shard_t> all_push = need_ver_targs;
	  all_push.insert(all_push.end(), missing_targs.begin(), missing_targs.end());

	  to_push.push_back(
	    boost::tuple<hobject_t, eversion_t, ObjectContextRef, vector<pg_shard_t> >
	    (backfill_info.begin, obj_v, obc, all_push));
	  // Count all simultaneous pushes of the same object as a single op
	  ops++;
	} else {
	  *work_started = true;
	  dout(20) << "backfill blocking on " << backfill_info.begin
		   << "; could not get rw_manager lock" << dendl;
	  break;
	}
      }
      dout(20) << "need_ver_targs=" << need_ver_targs
	       << " keep_ver_targs=" << keep_ver_targs << dendl;
      dout(20) << "backfill_targets=" << backfill_targets
	       << " missing_targs=" << missing_targs
	       << " skip_targs=" << skip_targs << dendl;

      last_backfill_started = backfill_info.begin;
      add_to_stat.insert(backfill_info.begin); // XXX: Only one for all pushes?
      backfill_info.pop_front();
      vector<pg_shard_t> check_targets = need_ver_targs;
      check_targets.insert(check_targets.end(), keep_ver_targs.begin(), keep_ver_targs.end());
      for (vector<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        pbi.pop_front();
      }
    }
  }
  backfill_pos = MIN(backfill_info.begin, earliest_peer_backfill());

  for (set<hobject_t>::iterator i = add_to_stat.begin();
       i != add_to_stat.end();
       ++i) {
    ObjectContextRef obc = get_object_context(*i, false);
    pg_stat_t stat;
    add_object_context_to_pg_stat(obc, &stat);
    pending_backfill_updates[*i] = stat;
  }
  for (unsigned i = 0; i < to_remove.size(); ++i) {
    handle.reset_tp_timeout();

    // ordered before any subsequent updates
    send_remove_op(to_remove[i].get<0>(), to_remove[i].get<1>(), to_remove[i].get<2>());

    pending_backfill_updates[to_remove[i].get<0>()]; // add empty stat!
  }

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  for (unsigned i = 0; i < to_push.size(); ++i) {
    handle.reset_tp_timeout();
    prep_backfill_object_push(to_push[i].get<0>(), to_push[i].get<1>(),
	    to_push[i].get<2>(), to_push[i].get<3>(), h);
  }
  pgbackend->run_recovery_op(h, cct->_conf->osd_recovery_op_priority);

  dout(5) << "backfill_pos is " << backfill_pos << dendl;
  for (set<hobject_t>::iterator i = backfills_in_flight.begin();
       i != backfills_in_flight.end();
       ++i) {
    dout(20) << *i << " is still in flight" << dendl;
  }

  hobject_t next_backfill_to_complete = backfills_in_flight.size() ?
    *(backfills_in_flight.begin()) : backfill_pos;
  hobject_t new_last_backfill = earliest_backfill();
  dout(10) << "starting new_last_backfill at " << new_last_backfill << dendl;
  for (map<hobject_t, pg_stat_t>::iterator i = pending_backfill_updates.begin();
       i != pending_backfill_updates.end() &&
	 i->first < next_backfill_to_complete;
       pending_backfill_updates.erase(i++)) {
    assert(i->first > new_last_backfill);
    for (set<pg_shard_t>::iterator j = backfill_targets.begin();
	 j != backfill_targets.end();
	 ++j) {
      pg_shard_t bt = *j;
      pg_info_t& pinfo = peer_info[bt];
      //Add stats to all peers that were missing object
      if (i->first > pinfo.last_backfill)
        pinfo.stats.add(i->second);
    }
    new_last_backfill = i->first;
  }
  dout(10) << "possible new_last_backfill at " << new_last_backfill << dendl;

  assert(!pending_backfill_updates.empty() ||
	 new_last_backfill == last_backfill_started);
  if (pending_backfill_updates.empty() &&
      backfill_pos.is_max()) {
    assert(backfills_in_flight.empty());
    new_last_backfill = backfill_pos;
    last_backfill_started = backfill_pos;
  }
  dout(10) << "final new_last_backfill at " << new_last_backfill << dendl;

  // If new_last_backfill == MAX, then we will send OP_BACKFILL_FINISH to
  // all the backfill targets.  Otherwise, we will move last_backfill up on
  // those targets need it and send OP_BACKFILL_PROGRESS to them.
  for (set<pg_shard_t>::iterator i = backfill_targets.begin();
       i != backfill_targets.end();
       ++i) {
    pg_shard_t bt = *i;
    pg_info_t& pinfo = peer_info[bt];

    if (new_last_backfill > pinfo.last_backfill) {
      pinfo.last_backfill = new_last_backfill;
      epoch_t e = get_osdmap()->get_epoch();
      MOSDPGBackfill *m = NULL;
      if (pinfo.last_backfill.is_max()) {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_FINISH,
	  e,
	  e,
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
        /* pinfo.stats might be wrong if we did log-based recovery on the
         * backfilled portion in addition to continuing backfill.
         */
        pinfo.stats = info.stats;
        start_recovery_op(hobject_t::get_max());
      } else {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_PROGRESS,
	  e,
	  e,
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
      }
      m->last_backfill = pinfo.last_backfill;
      m->stats = pinfo.stats;
      osd->send_message_osd_cluster(bt.osd, m, get_osdmap()->get_epoch());
      dout(10) << " peer " << bt
	       << " num_objects now " << pinfo.stats.stats.sum.num_objects
	       << " / " << info.stats.stats.sum.num_objects << dendl;
    }
  }

  if (ops)
    *work_started = true;
  return ops;
}

void ReplicatedPG::prep_backfill_object_push(
  hobject_t oid, eversion_t v,
  ObjectContextRef obc,
  vector<pg_shard_t> peers,
  PGBackend::RecoveryHandle *h)
{
  dout(10) << "push_backfill_object " << oid << " v " << v << " to peers " << peers << dendl;
  assert(!peers.empty());

  backfills_in_flight.insert(oid);
  for (unsigned int i = 0 ; i < peers.size(); ++i) {
    map<pg_shard_t, pg_missing_t>::iterator bpm = peer_missing.find(peers[i]);
    assert(bpm != peer_missing.end());
    bpm->second.add(oid, eversion_t(), eversion_t());
  }

  assert(!recovering.count(oid));

  start_recovery_op(oid);
  recovering.insert(make_pair(oid, obc));

  // We need to take the read_lock here in order to flush in-progress writes
  obc->ondisk_read_lock();
  pgbackend->recover_object(
    oid,
    v,
    ObjectContextRef(),
    obc,
    h);
  obc->ondisk_read_unlock();
}

void ReplicatedPG::update_range(
  BackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
  int local_min = cct->_conf->osd_backfill_scan_min;
  int local_max = cct->_conf->osd_backfill_scan_max;

  if (bi->version < info.log_tail) {
    dout(10) << __func__<< ": bi is old, rescanning local backfill_info"
	     << dendl;
    if (last_update_applied >= info.log_tail) {
      bi->version = last_update_applied;
    } else {
      osr->flush();
      bi->version = info.last_update;
    }
    scan_range(local_min, local_max, bi, handle);
  }

  if (bi->version >= info.last_update) {
    dout(10) << __func__<< ": bi is current " << dendl;
    assert(bi->version == info.last_update);
  } else if (bi->version >= info.log_tail) {
    if (pg_log.get_log().empty()) {
      /* Because we don't move log_tail on split, the log might be
       * empty even if log_tail != last_update.  However, the only
       * way to get here with an empty log is if log_tail is actually
       * eversion_t(), because otherwise the entry which changed
       * last_update since the last scan would have to be present.
       */
      assert(bi->version == eversion_t());
      return;
    }
    assert(!pg_log.get_log().empty());
    dout(10) << __func__<< ": bi is old, (" << bi->version
	     << ") can be updated with log" << dendl;
    list<pg_log_entry_t>::const_iterator i =
      pg_log.get_log().log.end();
    --i;
    while (i != pg_log.get_log().log.begin() &&
           i->version > bi->version) {
      --i;
    }
    if (i->version == bi->version)
      ++i;

    assert(i != pg_log.get_log().log.end());
    dout(10) << __func__ << ": updating from version " << i->version
	     << dendl;
    for (; i != pg_log.get_log().log.end(); ++i) {
      const hobject_t &soid = i->soid;
      if (soid >= bi->begin && soid < bi->end) {
	if (i->is_update()) {
	  dout(10) << __func__ << ": " << i->soid << " updated to version "
		   << i->version << dendl;
	  bi->objects.erase(i->soid);
	  bi->objects.insert(
	    make_pair(
	      i->soid,
	      i->version));
	} else if (i->is_delete()) {
	  dout(10) << __func__ << ": " << i->soid << " removed" << dendl;
	  bi->objects.erase(i->soid);
	}
      }
    }
    bi->version = info.last_update;
  } else {
    assert(0 == "scan_range should have raised bi->version past log_tail");
  }
}

void ReplicatedPG::scan_range(
  int min, int max, BackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
  assert(is_locked());
  dout(10) << "scan_range from " << bi->begin << dendl;
  bi->objects.clear();  // for good measure

  vector<hobject_t> ls;
  ls.reserve(max);
  int r = pgbackend->objects_list_partial(bi->begin, min, max, 0, &ls, &bi->end);
  assert(r >= 0);
  dout(10) << " got " << ls.size() << " items, next " << bi->end << dendl;
  dout(20) << ls << dendl;

  for (vector<hobject_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    handle.reset_tp_timeout();
    ObjectContextRef obc;
    if (is_primary())
      obc = object_contexts.lookup(*p);
    if (obc) {
      bi->objects[*p] = obc->obs.oi.version;
      dout(20) << "  " << *p << " " << obc->obs.oi.version << dendl;
    } else {
      bufferlist bl;
      int r = pgbackend->objects_get_attr(*p, OI_ATTR, &bl);

      /* If the object does not exist here, it must have been removed
	 * between the collection_list_partial and here.  This can happen
	 * for the first item in the range, which is usually last_backfill.
	 */
      if (r == -ENOENT)
	continue;

      assert(r >= 0);
      object_info_t oi(bl);
      bi->objects[*p] = oi.version;
      dout(20) << "  " << *p << " " << oi.version << dendl;
    }
  }
}


/** check_local
 * 
 * verifies that stray objects have been deleted
 */
void ReplicatedPG::check_local()
{
  dout(10) << __func__ << dendl;

  assert(info.last_update >= pg_log.get_tail());  // otherwise we need some help!

  if (!cct->_conf->osd_debug_verify_stray_on_activate)
    return;

  // just scan the log.
  set<hobject_t> did;
  for (list<pg_log_entry_t>::const_reverse_iterator p = pg_log.get_log().log.rbegin();
       p != pg_log.get_log().log.rend();
       ++p) {
    if (did.count(p->soid))
      continue;
    did.insert(p->soid);

    if (p->is_delete()) {
      dout(10) << " checking " << p->soid
	       << " at " << p->version << dendl;
      struct stat st;
      int r = osd->store->stat(
	coll,
	ghobject_t(p->soid, ghobject_t::NO_GEN, pg_whoami.shard),
	&st);
      if (r != -ENOENT) {
	derr << __func__ << " " << p->soid << " exists, but should have been "
	     << "deleted" << dendl;
	assert(0 == "erroneously present object");
      }
    } else {
      // ignore old(+missing) objects
    }
  }
}



// ===========================
// hit sets

hobject_t ReplicatedPG::get_hit_set_current_object(utime_t stamp)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid.pgid << "_current_" << stamp;
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

hobject_t ReplicatedPG::get_hit_set_archive_object(utime_t start, utime_t end)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid.pgid << "_archive_" << start << "_" << end;
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

void ReplicatedPG::hit_set_clear()
{
  dout(20) << __func__ << dendl;
  hit_set.reset();
  hit_set_start_stamp = utime_t();
  hit_set_flushing.clear();
}

void ReplicatedPG::hit_set_setup()
{
  if (!is_active() ||
      !is_primary() ||
      !pool.info.hit_set_count ||
      !pool.info.hit_set_period ||
      pool.info.hit_set_params.get_type() == HitSet::TYPE_NONE) {
    hit_set_clear();
    //hit_set_remove_all();  // FIXME: implement me soon
    return;
  }

  // FIXME: discard any previous data for now
  hit_set_create();

  // include any writes we know about from the pg log.  this doesn't
  // capture reads, but it is better than nothing!
  hit_set_apply_log();
}

void ReplicatedPG::hit_set_create()
{
  utime_t now = ceph_clock_now(NULL);
  // make a copy of the params to modify
  HitSet::Params params(pool.info.hit_set_params);

  dout(20) << __func__ << " " << params << dendl;
  if (pool.info.hit_set_params.get_type() == HitSet::TYPE_BLOOM) {
    BloomHitSet::Params *p =
      static_cast<BloomHitSet::Params*>(params.impl.get());

    // convert false positive rate so it holds up across the full period
    p->set_fpp(p->get_fpp() / pool.info.hit_set_count);
    if (p->get_fpp() <= 0.0)
      p->set_fpp(.01);  // fpp cannot be zero!

    // if we don't have specified size, estimate target size based on the
    // previous bin!
    if (p->target_size == 0 && hit_set) {
      utime_t dur = now - hit_set_start_stamp;
      unsigned unique = hit_set->approx_unique_insert_count();
      dout(20) << __func__ << " previous set had approx " << unique
	       << " unique items over " << dur << " seconds" << dendl;
      p->target_size = (double)unique * (double)pool.info.hit_set_period
		     / (double)dur;
    }
    if (p->target_size < static_cast<uint64_t>(g_conf->osd_hit_set_min_size))
      p->target_size = g_conf->osd_hit_set_min_size;

    if (p->target_size > static_cast<uint64_t>(g_conf->osd_hit_set_max_size))
      p->target_size = g_conf->osd_hit_set_max_size;

    p->seed = now.sec();

    dout(10) << __func__ << " target_size " << p->target_size
	     << " fpp " << p->get_fpp() << dendl;
  }
  hit_set.reset(new HitSet(params));
  hit_set_start_stamp = now;
}

/**
 * apply log entries to set
 *
 * this would only happen after peering, to at least capture writes
 * during an interval that was potentially lost.
 */
bool ReplicatedPG::hit_set_apply_log()
{
  if (!hit_set)
    return false;

  eversion_t to = info.last_update;
  eversion_t from = info.hit_set.current_last_update;
  if (to <= from) {
    dout(20) << __func__ << " no update" << dendl;
    return false;
  }

  dout(20) << __func__ << " " << to << " .. " << info.last_update << dendl;
  list<pg_log_entry_t>::const_reverse_iterator p = pg_log.get_log().log.rbegin();
  while (p != pg_log.get_log().log.rend() && p->version > to)
    ++p;
  while (p != pg_log.get_log().log.rend() && p->version > from) {
    hit_set->insert(p->soid);
    ++p;
  }

  return true;
}

struct C_HitSetFlushing : public Context {
  ReplicatedPGRef pg;
  time_t hit_set_name;
  C_HitSetFlushing(ReplicatedPG *p, time_t n) : pg(p), hit_set_name(n) { }
  void finish(int r) {
    pg->hit_set_flushing.erase(hit_set_name);
  }
};

void ReplicatedPG::hit_set_persist()
{
  dout(10) << __func__  << dendl;
  bufferlist bl;
  unsigned max = pool.info.hit_set_count;

  utime_t now = ceph_clock_now(cct);
  RepGather *repop;
  hobject_t oid;
  time_t flush_time = 0;

  // See what start is going to be used later
  utime_t start = info.hit_set.current_info.begin;
  if (!start)
     start = hit_set_start_stamp;

  // If any archives are degraded we skip this persist request
  // account for the additional entry being added below
  for (list<pg_hit_set_info_t>::iterator p = info.hit_set.history.begin();
       p != info.hit_set.history.end();
       ++p) {
    hobject_t aoid = get_hit_set_archive_object(p->begin, p->end);

    // Once we hit a degraded object just skip further trim
    if (is_degraded_object(aoid))
      return;
  }

  oid = get_hit_set_archive_object(start, now);
  // If the current object is degraded we skip this persist request
  if (is_degraded_object(oid))
    return;

  // If backfill is in progress and we could possibly overlap with the
  // hit_set_* objects, back off.  Since these all have
  // hobject_t::hash set to pgid.ps(), and those sort first, we can
  // look just at that.  This is necessary because our transactions
  // may include a modify of the new hit_set *and* a delete of the
  // old one, and this may span the backfill boundary.
  for (set<pg_shard_t>::iterator p = backfill_targets.begin();
       p != backfill_targets.end();
       ++p) {
    assert(peer_info.count(*p));
    const pg_info_t& pi = peer_info[*p];
    if (pi.last_backfill == hobject_t() ||
	pi.last_backfill.hash == info.pgid.ps()) {
      dout(10) << __func__ << " backfill target osd." << *p
	       << " last_backfill has not progressed past pgid ps"
	       << dendl;
      return;
    }
  }

  if (!info.hit_set.current_info.begin)
    info.hit_set.current_info.begin = hit_set_start_stamp;

  hit_set->seal();
  ::encode(*hit_set, bl);
  info.hit_set.current_info.end = now;
  dout(20) << __func__ << " archive " << oid << dendl;

  if (agent_state) {
    agent_state->add_hit_set(info.hit_set.current_info.begin, hit_set);
    hit_set_in_memory_trim();
  }

  // hold a ref until it is flushed to disk
  hit_set_flushing[info.hit_set.current_info.begin] = hit_set;
  flush_time = info.hit_set.current_info.begin;

  ObjectContextRef obc = get_object_context(oid, true);
  repop = simple_repop_create(obc);
  if (flush_time != 0)
    repop->on_applied = new C_HitSetFlushing(this, flush_time);
  OpContext *ctx = repop->ctx;
  ctx->at_version = get_next_version();
  ctx->updated_hset_history = info.hit_set;
  pg_hit_set_history_t &updated_hit_set_hist = *(ctx->updated_hset_history);

  if (updated_hit_set_hist.current_last_stamp != utime_t()) {
    // FIXME: we cheat slightly here by bundling in a remove on a object
    // other the RepGather object.  we aren't carrying an ObjectContext for
    // the deleted object over this period.
    hobject_t old_obj =
      get_hit_set_current_object(updated_hit_set_hist.current_last_stamp);
    ctx->log.push_back(
      pg_log_entry_t(pg_log_entry_t::DELETE,
		     old_obj,
		     ctx->at_version,
		     updated_hit_set_hist.current_last_update,
		     0,
		     osd_reqid_t(),
		     ctx->mtime));
    if (pool.info.require_rollback()) {
      if (ctx->log.back().mod_desc.rmobject(ctx->at_version.version)) {
	ctx->op_t->stash(old_obj, ctx->at_version.version);
      } else {
	ctx->op_t->remove(old_obj);
      }
    } else {
      ctx->op_t->remove(old_obj);
      ctx->log.back().mod_desc.mark_unrollbackable();
    }
    ++ctx->at_version.version;

    struct stat st;
    int r = osd->store->stat(
      coll,
      ghobject_t(old_obj, ghobject_t::NO_GEN, pg_whoami.shard),
      &st);
    assert(r == 0);
    --ctx->delta_stats.num_objects;
    ctx->delta_stats.num_bytes -= st.st_size;
  }

  updated_hit_set_hist.current_last_update = info.last_update; // *after* above remove!
  updated_hit_set_hist.current_info.version = ctx->at_version;

  updated_hit_set_hist.history.push_back(updated_hit_set_hist.current_info);
  hit_set_create();
  updated_hit_set_hist.current_info = pg_hit_set_info_t();
  updated_hit_set_hist.current_last_stamp = utime_t();

  // fabricate an object_info_t and SnapSet
  obc->obs.oi.version = ctx->at_version;
  obc->obs.oi.mtime = now;
  obc->obs.oi.size = bl.length();
  obc->obs.exists = true;

  ctx->new_obs = obc->obs;
  ctx->new_snapset.head_exists = true;

  ctx->delta_stats.num_objects++;
  ctx->delta_stats.num_objects_hit_set_archive++;
  ctx->delta_stats.num_bytes += bl.length();

  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  bufferlist boi(sizeof(ctx->new_obs.oi));
  ::encode(ctx->new_obs.oi, boi);

  ctx->op_t->append(oid, 0, bl.length(), bl);
  setattr_maybe_cache(ctx->obc, ctx, ctx->op_t, OI_ATTR, boi);
  setattr_maybe_cache(ctx->obc, ctx, ctx->op_t, SS_ATTR, bss);
  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::MODIFY,
      oid,
      ctx->at_version,
      eversion_t(),
      0,
      osd_reqid_t(),
      ctx->mtime)
    );
  if (pool.info.require_rollback()) {
    ctx->log.back().mod_desc.create();
  } else {
    ctx->log.back().mod_desc.mark_unrollbackable();
  }

  hit_set_trim(repop, max);

  info.stats.stats.add(ctx->delta_stats, string());

  simple_repop_submit(repop);
}

void ReplicatedPG::hit_set_trim(RepGather *repop, unsigned max)
{
  assert(repop->ctx->updated_hset_history);
  pg_hit_set_history_t &updated_hit_set_hist =
    *(repop->ctx->updated_hset_history);
  for (unsigned num = updated_hit_set_hist.history.size(); num > max; --num) {
    list<pg_hit_set_info_t>::iterator p = updated_hit_set_hist.history.begin();
    assert(p != updated_hit_set_hist.history.end());
    hobject_t oid = get_hit_set_archive_object(p->begin, p->end);

    assert(!is_degraded_object(oid));

    dout(20) << __func__ << " removing " << oid << dendl;
    ++repop->ctx->at_version.version;
    repop->ctx->log.push_back(
        pg_log_entry_t(pg_log_entry_t::DELETE,
		       oid,
		       repop->ctx->at_version,
		       p->version,
		       0,
		       osd_reqid_t(),
		       repop->ctx->mtime));
    if (pool.info.require_rollback()) {
      if (repop->ctx->log.back().mod_desc.rmobject(
	  repop->ctx->at_version.version)) {
	repop->ctx->op_t->stash(oid, repop->ctx->at_version.version);
      } else {
	repop->ctx->op_t->remove(oid);
      }
    } else {
      repop->ctx->op_t->remove(oid);
      repop->ctx->log.back().mod_desc.mark_unrollbackable();
    }
    updated_hit_set_hist.history.pop_front();

    ObjectContextRef obc = get_object_context(oid, false);
    assert(obc);
    --repop->ctx->delta_stats.num_objects;
    --repop->ctx->delta_stats.num_objects_hit_set_archive;
    repop->ctx->delta_stats.num_bytes -= obc->obs.oi.size;
  }
}

void ReplicatedPG::hit_set_in_memory_trim()
{
  unsigned max = pool.info.hit_set_count;
  unsigned max_in_memory = pool.info.min_read_recency_for_promote > 0 ? pool.info.min_read_recency_for_promote - 1 : 0;

  if (max_in_memory > max) {
    max_in_memory = max;
  }
  while (agent_state->hit_set_map.size() > max_in_memory) {
    agent_state->remove_oldest_hit_set();
  }
}


// =======================================
// cache agent

void ReplicatedPG::agent_setup()
{
  assert(is_locked());
  if (!is_active() ||
      !is_primary() ||
      pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE ||
      pool.info.tier_of < 0 ||
      !get_osdmap()->have_pg_pool(pool.info.tier_of)) {
    agent_clear();
    return;
  }
  if (!agent_state) {
    agent_state.reset(new TierAgentState);

    // choose random starting position
    agent_state->position = hobject_t();
    agent_state->position.pool = info.pgid.pool();
    agent_state->position.hash = pool.info.get_random_pg_position(
      info.pgid.pgid,
      rand());
    agent_state->start = agent_state->position;

    dout(10) << __func__ << " allocated new state, position "
	     << agent_state->position << dendl;
  } else {
    dout(10) << __func__ << " keeping existing state" << dendl;
  }

  if (info.stats.stats_invalid) {
    osd->clog.warn() << "pg " << info.pgid << " has invalid (post-split) stats; must scrub before tier agent can activate";
  }

  agent_choose_mode();
}

void ReplicatedPG::agent_clear()
{
  agent_stop();
  agent_state.reset(NULL);
}

// Return false if no objects operated on since start of object hash space
bool ReplicatedPG::agent_work(int start_max)
{
  lock();
  if (!agent_state) {
    dout(10) << __func__ << " no agent state, stopping" << dendl;
    unlock();
    return true;
  }

  assert(!deleting);

  if (agent_state->is_idle()) {
    dout(10) << __func__ << " idle, stopping" << dendl;
    unlock();
    return true;
  }

  osd->logger->inc(l_osd_agent_wake);

  dout(10) << __func__
	   << " max " << start_max
	   << ", flush " << agent_state->get_flush_mode_name()
	   << ", evict " << agent_state->get_evict_mode_name()
	   << ", pos " << agent_state->position
	   << dendl;
  assert(is_primary());
  assert(is_active());

  agent_load_hit_sets();

  const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
  assert(base_pool);

  int ls_min = 1;
  int ls_max = 10; // FIXME?

  // list some objects.  this conveniently lists clones (oldest to
  // newest) before heads... the same order we want to flush in.
  //
  // NOTE: do not flush the Sequencer.  we will assume that the
  // listing we get back is imprecise.
  vector<hobject_t> ls;
  hobject_t next;
  int r = pgbackend->objects_list_partial(agent_state->position, ls_min, ls_max,
					  0 /* no filtering by snapid */,
					  &ls, &next);
  assert(r >= 0);
  dout(20) << __func__ << " got " << ls.size() << " objects" << dendl;
  int started = 0;
  for (vector<hobject_t>::iterator p = ls.begin();
       p != ls.end();
       ++p) {
    if (p->nspace == cct->_conf->osd_hit_set_namespace) {
      dout(20) << __func__ << " skip (hit set) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (is_degraded_object(*p)) {
      dout(20) << __func__ << " skip (degraded) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (is_missing_object(p->get_head())) {
      dout(20) << __func__ << " skip (missing head) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    ObjectContextRef obc = get_object_context(*p, false, NULL);
    if (!obc) {
      // we didn't flush; we may miss something here.
      dout(20) << __func__ << " skip (no obc) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (!obc->obs.exists) {
      dout(20) << __func__ << " skip (dne) " << obc->obs.oi.soid << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (scrubber.write_blocked_by_scrub(obc->obs.oi.soid)) {
      dout(20) << __func__ << " skip (scrubbing) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (obc->is_blocked()) {
      dout(20) << __func__ << " skip (blocked) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }

    // be careful flushing omap to an EC pool.
    if (base_pool->is_erasure() &&
	obc->obs.oi.test_flag(object_info_t::FLAG_OMAP)) {
      dout(20) << __func__ << " skip (omap to EC) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }

    if (agent_state->flush_mode != TierAgentState::FLUSH_MODE_IDLE &&
	agent_maybe_flush(obc))
      ++started;
    if (agent_state->evict_mode != TierAgentState::EVICT_MODE_IDLE &&
	agent_maybe_evict(obc))
      ++started;
    if (started >= start_max) {
      // If finishing early, set "next" to the next object
      if (++p != ls.end())
	next = *p;
      break;
    }
  }

  if (++agent_state->hist_age > g_conf->osd_agent_hist_halflife) {
    dout(20) << __func__ << " resetting atime and temp histograms" << dendl;
    agent_state->hist_age = 0;
    agent_state->atime_hist.decay();
    agent_state->temp_hist.decay();
  }

  // Total objects operated on so far
  int total_started = agent_state->started + started;
  bool need_delay = false;

  dout(20) << __func__ << " start pos " << agent_state->position
    << " next start pos " << next
    << " started " << total_started << dendl;

  // See if we've made a full pass over the object hash space
  // This might check at most ls_max objects a second time to notice that
  // we've checked every objects at least once.
  if (agent_state->position < agent_state->start && next >= agent_state->start) {
    dout(20) << __func__ << " wrap around " << agent_state->start << dendl;
    if (total_started == 0)
      need_delay = true;
    else
      total_started = 0;
    agent_state->start = next;
  }
  agent_state->started = total_started;

  // See if we are starting from beginning
  if (next.is_max())
    agent_state->position = hobject_t();
  else
    agent_state->position = next;

  // Discard old in memory HitSets
  hit_set_in_memory_trim();

  if (need_delay) {
    assert(agent_state->delaying == false);
    agent_delay();
    unlock();
    return false;
  }
  agent_choose_mode();
  unlock();
  return true;
}

void ReplicatedPG::agent_load_hit_sets()
{
  if (agent_state->evict_mode == TierAgentState::EVICT_MODE_IDLE) {
    return;
  }

  if (agent_state->hit_set_map.size() < info.hit_set.history.size()) {
    dout(10) << __func__ << dendl;
    for (list<pg_hit_set_info_t>::iterator p = info.hit_set.history.begin();
	 p != info.hit_set.history.end(); ++p) {
      if (agent_state->hit_set_map.count(p->begin.sec()) == 0) {
	dout(10) << __func__ << " loading " << p->begin << "-"
		 << p->end << dendl;
	if (!pool.info.is_replicated()) {
	  // FIXME: EC not supported here yet
	  derr << __func__ << " on non-replicated pool" << dendl;
	  break;
	}

	// check if it's still in flight
	if (hit_set_flushing.count(p->begin)) {
	  agent_state->add_hit_set(p->begin.sec(), hit_set_flushing[p->begin]);
	  continue;
	}

	hobject_t oid = get_hit_set_archive_object(p->begin, p->end);
	if (is_unreadable_object(oid)) {
	  dout(10) << __func__ << " unreadable " << oid << ", waiting" << dendl;
	  break;
	}

	ObjectContextRef obc = get_object_context(oid, false);
	if (!obc) {
	  derr << __func__ << ": could not load hitset " << oid << dendl;
	  break;
	}

	bufferlist bl;
	{
	  obc->ondisk_read_lock();
	  int r = osd->store->read(coll, oid, 0, 0, bl);
	  assert(r >= 0);
	  obc->ondisk_read_unlock();
	}
	HitSetRef hs(new HitSet);
	bufferlist::iterator pbl = bl.begin();
	::decode(*hs, pbl);
	agent_state->add_hit_set(p->begin.sec(), hs);
      }
    }
  }
}

struct C_AgentFlushStartStop : public Context {
  ReplicatedPGRef pg;
  hobject_t oid;
  C_AgentFlushStartStop(ReplicatedPG *p, hobject_t o) : pg(p), oid(o) {
    pg->osd->agent_start_op(oid);
  }
  void finish(int r) {
    pg->osd->agent_finish_op(oid);
  }
};

bool ReplicatedPG::agent_maybe_flush(ObjectContextRef& obc)
{
  if (!obc->obs.oi.is_dirty()) {
    dout(20) << __func__ << " skip (clean) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  utime_t now = ceph_clock_now(NULL);
  utime_t ob_local_mtime;
  if (obc->obs.oi.local_mtime != utime_t()) {
    ob_local_mtime = obc->obs.oi.local_mtime;
  } else {
    ob_local_mtime = obc->obs.oi.mtime;
  }
  bool evict_mode_full = (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL);
  if (!evict_mode_full && (ob_local_mtime + utime_t(pool.info.cache_min_flush_age, 0) > now)) {
    dout(20) << __func__ << " skip (too young) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  if (osd->agent_is_active_oid(obc->obs.oi.soid)) {
    dout(20) << __func__ << " skip (flushing) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  dout(10) << __func__ << " flushing " << obc->obs.oi << dendl;

  // FIXME: flush anything dirty, regardless of what distribution of
  // ages we expect.

  Context *on_flush = new C_AgentFlushStartStop(this, obc->obs.oi.soid);
  int result = start_flush(
    OpRequestRef(), obc, false, NULL,
    on_flush);
  if (result != -EINPROGRESS) {
    on_flush->complete(result);
    dout(10) << __func__ << " start_flush() failed " << obc->obs.oi
      << " with " << result << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  osd->logger->inc(l_osd_agent_flush);
  return true;
}

bool ReplicatedPG::agent_maybe_evict(ObjectContextRef& obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  if (obc->obs.oi.is_dirty()) {
    dout(20) << __func__ << " skip (dirty) " << obc->obs.oi << dendl;
    return false;
  }
  if (!obc->obs.oi.watchers.empty()) {
    dout(20) << __func__ << " skip (watchers) " << obc->obs.oi << dendl;
    return false;
  }

  if (soid.snap == CEPH_NOSNAP) {
    int result = _verify_no_head_clones(soid, obc->ssc->snapset);
    if (result < 0) {
      dout(20) << __func__ << " skip (clones) " << obc->obs.oi << dendl;
      return false;
    }
  }

  if (agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL &&
      hit_set) {
    // is this object old and/or cold enough?
    int atime = -1, temp = 0;
    agent_estimate_atime_temp(soid, &atime, NULL /*FIXME &temp*/);

    uint64_t atime_upper = 0, atime_lower = 0;
    if (atime < 0 && obc->obs.oi.mtime != utime_t())
      atime = ceph_clock_now(NULL).sec() - obc->obs.oi.mtime;
    if (atime < 0)
      atime = pool.info.hit_set_period * pool.info.hit_set_count; // "infinite"
    if (atime >= 0) {
      agent_state->atime_hist.add(atime);
      agent_state->atime_hist.get_position_micro(atime, &atime_lower,
						 &atime_upper);
    }

    unsigned temp_upper = 0, temp_lower = 0;
    /*
    // FIXME: bound atime based on creation time?
    agent_state->temp_hist.add(atime);
    agent_state->temp_hist.get_position_micro(temp, &temp_lower, &temp_upper);
    */

    dout(20) << __func__
	     << " atime " << atime
	     << " pos " << atime_lower << "-" << atime_upper
	     << ", temp " << temp
	     << " pos " << temp_lower << "-" << temp_upper
	     << ", evict_effort " << agent_state->evict_effort
	     << dendl;
    dout(30) << "agent_state:\n";
    Formatter *f = new_formatter("");
    f->open_object_section("agent_state");
    agent_state->dump(f);
    f->close_section();
    f->flush(*_dout);
    delete f;
    *_dout << dendl;

    // FIXME: ignore temperature for now.

    if (1000000 - atime_upper >= agent_state->evict_effort)
      return false;
  }

  dout(10) << __func__ << " evicting " << obc->obs.oi << dendl;
  RepGather *repop = simple_repop_create(obc);
  OpContext *ctx = repop->ctx;
  ctx->at_version = get_next_version();
  assert(ctx->new_obs.exists);
  int r = _delete_oid(ctx, true);
  if (obc->obs.oi.is_omap())
    ctx->delta_stats.num_objects_omap--;
  assert(r == 0);
  finish_ctx(ctx, pg_log_entry_t::DELETE, false);
  simple_repop_submit(repop);
  osd->logger->inc(l_osd_tier_evict);
  osd->logger->inc(l_osd_agent_evict);
  return true;
}

void ReplicatedPG::agent_stop()
{
  dout(20) << __func__ << dendl;
  if (agent_state && !agent_state->is_idle()) {
    agent_state->evict_mode = TierAgentState::EVICT_MODE_IDLE;
    agent_state->flush_mode = TierAgentState::FLUSH_MODE_IDLE;
    osd->agent_disable_pg(this, agent_state->evict_effort);
  }
}

void ReplicatedPG::agent_delay()
{
  dout(20) << __func__ << dendl;
  if (agent_state && !agent_state->is_idle()) {
    assert(agent_state->delaying == false);
    agent_state->delaying = true;
    osd->agent_disable_pg(this, agent_state->evict_effort);
  }
}

void ReplicatedPG::agent_choose_mode_restart()
{
  dout(20) << __func__ << dendl;
  lock();
  if (agent_state && agent_state->delaying) {
    agent_state->delaying = false;
    agent_choose_mode(true);
  }
  unlock();
}

void ReplicatedPG::agent_choose_mode(bool restart)
{
  // Let delay play out
  if (agent_state->delaying) {
    dout(20) << __func__ << this << " delaying, ignored" << dendl;
    return;
  }

  uint64_t divisor = pool.info.get_pg_num_divisor(info.pgid.pgid);
  assert(divisor > 0);

  uint64_t num_user_objects = info.stats.stats.sum.num_objects;

  // adjust (effective) user objects down based on the number
  // of HitSet objects, which should not count toward our total since
  // they cannot be flushed.
  uint64_t unflushable = info.stats.stats.sum.num_objects_hit_set_archive;

  // also exclude omap objects if ec backing pool
  const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
  assert(base_pool);
  if (base_pool->is_erasure())
    unflushable += info.stats.stats.sum.num_objects_omap;


  if (num_user_objects > unflushable)
    num_user_objects -= unflushable;
  else
    num_user_objects = 0;

  // also reduce the num_dirty by num_objects_omap
  int64_t num_dirty = info.stats.stats.sum.num_objects_dirty;
  if (base_pool->is_erasure()) {
    if (num_dirty > info.stats.stats.sum.num_objects_omap)
      num_dirty -= info.stats.stats.sum.num_objects_omap;
    else
      num_dirty = 0;
  }

  dout(10) << __func__
	   << " flush_mode: "
	   << TierAgentState::get_flush_mode_name(agent_state->flush_mode)
	   << " evict_mode: "
	   << TierAgentState::get_evict_mode_name(agent_state->evict_mode)
	   << " num_objects: " << info.stats.stats.sum.num_objects
	   << " num_bytes: " << info.stats.stats.sum.num_bytes
	   << " num_objects_dirty: " << info.stats.stats.sum.num_objects_dirty
	   << " num_objects_omap: " << info.stats.stats.sum.num_objects_omap
	   << " num_dirty: " << num_dirty
	   << " num_user_objects: " << num_user_objects
	   << " pool.info.target_max_bytes: " << pool.info.target_max_bytes
	   << " pool.info.target_max_objects: " << pool.info.target_max_objects
	   << dendl;

  // get dirty, full ratios
  uint64_t dirty_micro = 0;
  uint64_t full_micro = 0;
  if (pool.info.target_max_bytes && info.stats.stats.sum.num_objects > 0) {
    uint64_t avg_size = info.stats.stats.sum.num_bytes /
      info.stats.stats.sum.num_objects;
    dirty_micro =
      num_dirty * avg_size * 1000000 /
      MAX(pool.info.target_max_bytes / divisor, 1);
    full_micro =
      num_user_objects * avg_size * 1000000 /
      MAX(pool.info.target_max_bytes / divisor, 1);
  }
  if (pool.info.target_max_objects > 0) {
    uint64_t dirty_objects_micro =
      num_dirty * 1000000 /
      MAX(pool.info.target_max_objects / divisor, 1);
    if (dirty_objects_micro > dirty_micro)
      dirty_micro = dirty_objects_micro;
    uint64_t full_objects_micro =
      num_user_objects * 1000000 /
      MAX(pool.info.target_max_objects / divisor, 1);
    if (full_objects_micro > full_micro)
      full_micro = full_objects_micro;
  }
  dout(20) << __func__ << " dirty " << ((float)dirty_micro / 1000000.0)
	   << " full " << ((float)full_micro / 1000000.0)
	   << dendl;

  // flush mode
  TierAgentState::flush_mode_t flush_mode = TierAgentState::FLUSH_MODE_IDLE;
  uint64_t flush_target = pool.info.cache_target_dirty_ratio_micro;
  uint64_t flush_slop = (float)flush_target * g_conf->osd_agent_slop;
  if (restart || agent_state->flush_mode == TierAgentState::FLUSH_MODE_IDLE)
    flush_target += flush_slop;
  else
    flush_target -= MIN(flush_target, flush_slop);

  if (info.stats.stats_invalid) {
    // idle; stats can't be trusted until we scrub.
    dout(20) << __func__ << " stats invalid (post-split), idle" << dendl;
  } else if (dirty_micro > flush_target) {
    flush_mode = TierAgentState::FLUSH_MODE_ACTIVE;
  }

  // evict mode
  TierAgentState::evict_mode_t evict_mode = TierAgentState::EVICT_MODE_IDLE;
  unsigned evict_effort = 0;
  uint64_t evict_target = pool.info.cache_target_full_ratio_micro;
  uint64_t evict_slop = (float)evict_target * g_conf->osd_agent_slop;
  if (restart || agent_state->evict_mode == TierAgentState::EVICT_MODE_IDLE)
    evict_target += evict_slop;
  else
    evict_target -= MIN(evict_target, evict_slop);

  if (info.stats.stats_invalid) {
    // idle; stats can't be trusted until we scrub.
  } else if (full_micro > 1000000) {
    // evict anything clean
    evict_mode = TierAgentState::EVICT_MODE_FULL;
    evict_effort = 1000000;
  } else if (full_micro > evict_target) {
    // set effort in [0..1] range based on where we are between
    evict_mode = TierAgentState::EVICT_MODE_SOME;
    uint64_t over = full_micro - evict_target;
    uint64_t span;
    if (evict_target >= 1000000)
      span = 1;
    else
      span = 1000000 - evict_target;
    evict_effort = MAX(over * 1000000 / span,
		       (unsigned)(1000000.0 * g_conf->osd_agent_min_evict_effort));

    // quantize effort to avoid too much reordering in the agent_queue.
    uint64_t inc = g_conf->osd_agent_quantize_effort * 1000000;
    assert(inc > 0);
    uint64_t was = evict_effort;
    evict_effort -= evict_effort % inc;
    if (evict_effort < inc)
      evict_effort = inc;
    assert(evict_effort >= inc && evict_effort <= 1000000);
    dout(30) << __func__ << " evict_effort " << was << " quantized by " << inc << " to " << evict_effort << dendl;
  }

  bool old_idle = agent_state->is_idle();
  if (flush_mode != agent_state->flush_mode) {
    dout(5) << __func__ << " flush_mode "
	    << TierAgentState::get_flush_mode_name(agent_state->flush_mode)
	    << " -> "
	    << TierAgentState::get_flush_mode_name(flush_mode)
	    << dendl;
    agent_state->flush_mode = flush_mode;
  }
  if (evict_mode != agent_state->evict_mode) {
    dout(5) << __func__ << " evict_mode "
	    << TierAgentState::get_evict_mode_name(agent_state->evict_mode)
	    << " -> "
	    << TierAgentState::get_evict_mode_name(evict_mode)
	    << dendl;
    if (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL &&
	is_active()) {
      requeue_ops(waiting_for_cache_not_full);
      requeue_ops(waiting_for_active);
    }
    agent_state->evict_mode = evict_mode;
  }
  uint64_t old_effort = agent_state->evict_effort;
  if (evict_effort != agent_state->evict_effort) {
    dout(5) << __func__ << " evict_effort "
	    << ((float)agent_state->evict_effort / 1000000.0)
	    << " -> "
	    << ((float)evict_effort / 1000000.0)
	    << dendl;
    agent_state->evict_effort = evict_effort;
  }

  // NOTE: we are using evict_effort as a proxy for *all* agent effort
  // (including flush).  This is probably fine (they should be
  // correlated) but it is not precisely correct.
  if (agent_state->is_idle()) {
    if (!restart && !old_idle) {
      osd->agent_disable_pg(this, old_effort);
    }
  } else {
    if (restart || old_idle) {
      osd->agent_enable_pg(this, agent_state->evict_effort);
    } else if (old_effort != agent_state->evict_effort) {
      osd->agent_adjust_pg(this, old_effort, agent_state->evict_effort);
    }
  }
}

void ReplicatedPG::agent_estimate_atime_temp(const hobject_t& oid,
					     int *atime, int *temp)
{
  assert(hit_set);
  *atime = -1;
  if (temp)
    *temp = 0;
  if (hit_set->contains(oid)) {
    *atime = 0;
    if (temp)
      ++(*temp);
    else
      return;
  }
  time_t now = ceph_clock_now(NULL).sec();
  for (map<time_t,HitSetRef>::reverse_iterator p =
	 agent_state->hit_set_map.rbegin();
       p != agent_state->hit_set_map.rend();
       ++p) {
    if (p->second->contains(oid)) {
      if (*atime < 0)
	*atime = now - p->first;
      if (temp)
	++(*temp);
      else
	return;
    }
  }
}


// ==========================================================================================
// SCRUB


bool ReplicatedPG::_range_available_for_scrub(
  const hobject_t &begin, const hobject_t &end)
{
  pair<hobject_t, ObjectContextRef> next;
  next.second = object_contexts.lookup(begin);
  next.first = begin;
  bool more = true;
  while (more && next.first < end) {
    if (next.second && next.second->is_blocked()) {
      next.second->requeue_scrub_on_unblock = true;
      dout(10) << __func__ << ": scrub delayed, "
	       << next.first << " is blocked"
	       << dendl;
      return false;
    }
    more = object_contexts.get_next(next.first, &next);
  }
  return true;
}

void ReplicatedPG::_scrub(ScrubMap& scrubmap)
{
  dout(10) << "_scrub" << dendl;

  coll_t c(info.pgid);
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  // traverse in reverse order.
  hobject_t head;
  SnapSet snapset;
  vector<snapid_t>::reverse_iterator curclone;
  hobject_t next_clone;

  bufferlist last_data;

  for (map<hobject_t,ScrubMap::object>::reverse_iterator p = scrubmap.objects.rbegin(); 
       p != scrubmap.objects.rend(); 
       ++p) {
    const hobject_t& soid = p->first;
    object_stat_sum_t stat;
    if (soid.snap != CEPH_SNAPDIR)
      stat.num_objects++;

    if (soid.nspace == cct->_conf->osd_hit_set_namespace)
      stat.num_objects_hit_set_archive++;

    // new snapset?
    if (soid.snap == CEPH_SNAPDIR ||
	soid.snap == CEPH_NOSNAP) {
      if (p->second.attrs.count(SS_ATTR) == 0) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " no '" << SS_ATTR << "' attr";
        ++scrubber.shallow_errors;
	continue;
      }
      bufferlist bl;
      bl.push_back(p->second.attrs[SS_ATTR]);
      bufferlist::iterator blp = bl.begin();
      ::decode(snapset, blp);

      // did we finish the last oid?
      if (head != hobject_t() &&
	  !pool.info.allow_incomplete_clones()) {
	osd->clog.error() << mode << " " << info.pgid << " " << head
			  << " missing clones";
        ++scrubber.shallow_errors;
      }
      
      // what will be next?
      if (snapset.clones.empty())
	head = hobject_t();  // no clones.
      else {
	curclone = snapset.clones.rbegin();
	head = p->first;
	next_clone = hobject_t();
	dout(20) << "  snapset " << snapset << dendl;
      }
    }

    // basic checks.
    if (p->second.attrs.count(OI_ATTR) == 0) {
      osd->clog.error() << mode << " " << info.pgid << " " << soid
			<< " no '" << OI_ATTR << "' attr";
      ++scrubber.shallow_errors;
      continue;
    }
    bufferlist bv;
    bv.push_back(p->second.attrs[OI_ATTR]);
    object_info_t oi(bv);

    if (pgbackend->be_get_ondisk_size(oi.size) != p->second.size) {
      osd->clog.error() << mode << " " << info.pgid << " " << soid
			<< " on disk size (" << p->second.size
			<< ") does not match object info size ("
			<< oi.size << ") ajusted for ondisk to ("
			<< pgbackend->be_get_ondisk_size(oi.size)
			<< ")";
      ++scrubber.shallow_errors;
    }

    dout(20) << mode << "  " << soid << " " << oi << dendl;

    if (soid.is_snap()) {
      stat.num_bytes += snapset.get_clone_bytes(soid.snap);
    } else {
      stat.num_bytes += oi.size;
    }

    if (!soid.is_snapdir()) {
      if (oi.is_dirty())
	++stat.num_objects_dirty;
      if (oi.is_whiteout())
	++stat.num_whiteouts;
      if (oi.is_omap())
	++stat.num_objects_omap;
    }

    //bufferlist data;
    //osd->store->read(c, poid, 0, 0, data);
    //assert(data.length() == p->size);
    //

    if (!next_clone.is_min() && next_clone != soid &&
	pool.info.allow_incomplete_clones()) {
      // it is okay to be missing one or more clones in a cache tier.
      // skip higher-numbered clones in the list.
      while (curclone != snapset.clones.rend() &&
	     soid.snap < *curclone)
	++curclone;
      if (curclone != snapset.clones.rend() &&
	  soid.snap == *curclone) {
	dout(20) << __func__ << " skipped some clones in cache tier" << dendl;
	next_clone.snap = *curclone;
      }
      if (curclone == snapset.clones.rend() ||
	  soid.snap == CEPH_NOSNAP) {
	dout(20) << __func__ << " skipped remaining clones in cache tier"
		 << dendl;
	next_clone = hobject_t();
	head = hobject_t();
      }
    }
    if (!next_clone.is_min() && next_clone != soid) {
      osd->clog.error() << mode << " " << info.pgid << " " << soid
			<< " expected clone " << next_clone;
      ++scrubber.shallow_errors;
    }

    if (soid.snap == CEPH_NOSNAP || soid.snap == CEPH_SNAPDIR) {
      if (soid.snap == CEPH_NOSNAP && !snapset.head_exists) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " snapset.head_exists=false, but head exists";
        ++scrubber.shallow_errors;
      }
      if (soid.snap == CEPH_SNAPDIR && snapset.head_exists) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " snapset.head_exists=true, but snapdir exists";
        ++scrubber.shallow_errors;
      }
      if (curclone == snapset.clones.rend()) {
	next_clone = hobject_t();
      } else {
	next_clone = soid;
	next_clone.snap = *curclone;
      }
    } else if (soid.snap) {
      // it's a clone
      stat.num_object_clones++;
      
      if (head == hobject_t()) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " found clone without head";
	++scrubber.shallow_errors;
	continue;
      }

      if (soid.snap != *curclone) {
	continue; // we warn above.  we could do better here...
      }

      if (oi.size != snapset.clone_size[*curclone]) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " size " << oi.size << " != clone_size "
			  << snapset.clone_size[*curclone];
	++scrubber.shallow_errors;
      }

      // verify overlap?
      // ...

      // what's next?
      if (curclone != snapset.clones.rend()) {
	++curclone;
      }
      if (curclone == snapset.clones.rend()) {
	head = hobject_t();
	next_clone = hobject_t();
      } else {
	next_clone.snap = *curclone;
      }

    } else {
      // it's unversioned.
      next_clone = hobject_t();
    }

    string cat; // fixme
    scrub_cstat.add(stat, cat);
  }

  if (!next_clone.is_min() &&
      !pool.info.allow_incomplete_clones()) {
    osd->clog.error() << mode << " " << info.pgid
		      << " expected clone " << next_clone;
    ++scrubber.shallow_errors;
  }
  
  dout(10) << "_scrub (" << mode << ") finish" << dendl;
}

void ReplicatedPG::_scrub_clear_state()
{
  scrub_cstat = object_stat_collection_t();
}

void ReplicatedPG::_scrub_finish()
{
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  if (info.stats.stats_invalid) {
    info.stats.stats = scrub_cstat;
    info.stats.stats_invalid = false;

    if (agent_state)
      agent_choose_mode();
  }

  dout(10) << mode << " got "
	   << scrub_cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
	   << scrub_cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
	   << scrub_cstat.sum.num_objects_dirty << "/" << info.stats.stats.sum.num_objects_dirty << " dirty, "
	   << scrub_cstat.sum.num_objects_omap << "/" << info.stats.stats.sum.num_objects_omap << " omap, "
	   << scrub_cstat.sum.num_objects_hit_set_archive << "/" << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
	   << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes."
	   << dendl;

  if (scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      scrub_cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      (scrub_cstat.sum.num_objects_dirty != info.stats.stats.sum.num_objects_dirty &&
       !info.stats.dirty_stats_invalid) ||
      (scrub_cstat.sum.num_objects_omap != info.stats.stats.sum.num_objects_omap &&
       !info.stats.omap_stats_invalid) ||
      (scrub_cstat.sum.num_objects_hit_set_archive != info.stats.stats.sum.num_objects_hit_set_archive &&
       !info.stats.hitset_stats_invalid) ||
      scrub_cstat.sum.num_whiteouts != info.stats.stats.sum.num_whiteouts ||
      scrub_cstat.sum.num_bytes != info.stats.stats.sum.num_bytes) {
    osd->clog.error() << info.pgid << " " << mode
		      << " stat mismatch, got "
		      << scrub_cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
		      << scrub_cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
		      << scrub_cstat.sum.num_objects_dirty << "/" << info.stats.stats.sum.num_objects_dirty << " dirty, "
		      << scrub_cstat.sum.num_objects_omap << "/" << info.stats.stats.sum.num_objects_omap << " omap, "
		      << scrub_cstat.sum.num_objects_hit_set_archive << "/" << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
		      << scrub_cstat.sum.num_whiteouts << "/" << info.stats.stats.sum.num_whiteouts << " whiteouts, "
		      << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes.\n";
    ++scrubber.shallow_errors;

    if (repair) {
      ++scrubber.fixed;
      info.stats.stats = scrub_cstat;
      info.stats.dirty_stats_invalid = false;
      info.stats.omap_stats_invalid = false;
      info.stats.hitset_stats_invalid = false;
      publish_stats_to_osd();
      share_pg_info();
    }
  }
}

/*---SnapTrimmer Logging---*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

ReplicatedPG::SnapTrimmer::~SnapTrimmer()
{
  while (!repops.empty()) {
    (*repops.begin())->put();
    repops.erase(repops.begin());
  }
}

void ReplicatedPG::SnapTrimmer::log_enter(const char *state_name)
{
  dout(20) << "enter " << state_name << dendl;
}

void ReplicatedPG::SnapTrimmer::log_exit(const char *state_name, utime_t enter_time)
{
  dout(20) << "exit " << state_name << dendl;
}

/*---SnapTrimmer states---*/
#undef dout_prefix
#define dout_prefix (*_dout << context< SnapTrimmer >().pg->gen_prefix() \
		     << "SnapTrimmer state<" << get_state_name() << ">: ")

/* NotTrimming */
ReplicatedPG::NotTrimming::NotTrimming(my_context ctx)
  : my_base(ctx), 
    NamedState(context< SnapTrimmer >().pg->cct, "NotTrimming")
{
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::NotTrimming::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result ReplicatedPG::NotTrimming::react(const SnapTrim&)
{
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  dout(10) << "NotTrimming react" << dendl;

  if (!pg->is_primary() || !pg->is_active() || !pg->is_clean()) {
    dout(10) << "NotTrimming not primary, active, clean" << dendl;
    return discard_event();
  } else if (pg->scrubber.active) {
    dout(10) << "NotTrimming finalizing scrub" << dendl;
    pg->queue_snap_trim();
    return discard_event();
  }

  // Primary trimming
  if (pg->snap_trimq.empty()) {
    return discard_event();
  } else {
    context<SnapTrimmer>().snap_to_trim = pg->snap_trimq.range_start();
    dout(10) << "NotTrimming: trimming "
	     << pg->snap_trimq.range_start()
	     << dendl;
    post_event(SnapTrim());
    return transit<TrimmingObjects>();
  }
}

/* TrimmingObjects */
ReplicatedPG::TrimmingObjects::TrimmingObjects(my_context ctx)
  : my_base(ctx),
    NamedState(context< SnapTrimmer >().pg->cct, "Trimming/TrimmingObjects")
{
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::TrimmingObjects::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
  // Clean up repops in case of reset
  set<RepGather *> &repops = context<SnapTrimmer>().repops;
  for (set<RepGather *>::iterator i = repops.begin();
       i != repops.end();
       repops.erase(i++)) {
    (*i)->put();
  }
}

boost::statechart::result ReplicatedPG::TrimmingObjects::react(const SnapTrim&)
{
  dout(10) << "TrimmingObjects react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  snapid_t snap_to_trim = context<SnapTrimmer>().snap_to_trim;
  set<RepGather *> &repops = context<SnapTrimmer>().repops;

  dout(10) << "TrimmingObjects: trimming snap " << snap_to_trim << dendl;

  for (set<RepGather *>::iterator i = repops.begin();
       i != repops.end(); 
       ) {
    if ((*i)->all_applied && (*i)->all_committed) {
      (*i)->put();
      repops.erase(i++);
    } else {
      ++i;
    }
  }

  while (repops.size() < g_conf->osd_pg_max_concurrent_snap_trims) {
    // Get next
    hobject_t old_pos = pos;
    int r = pg->snap_mapper.get_next_object_to_trim(snap_to_trim, &pos);
    if (r != 0 && r != -ENOENT) {
      derr << __func__ << ": get_next returned " << cpp_strerror(r) << dendl;
      assert(0);
    } else if (r == -ENOENT) {
      // Done!
      dout(10) << "TrimmingObjects: got ENOENT" << dendl;
      post_event(SnapTrim());
      return transit< WaitingOnReplicas >();
    }

    dout(10) << "TrimmingObjects react trimming " << pos << dendl;
    RepGather *repop = pg->trim_object(pos);
    if (!repop) {
      dout(10) << __func__ << " could not get write lock on obj "
	       << pos << dendl;
      pos = old_pos;
      return discard_event();
    }
    assert(repop);
    repop->queue_snap_trimmer = true;

    repops.insert(repop->get());
    pg->simple_repop_submit(repop);
  }
  return discard_event();
}
/* WaitingOnReplicasObjects */
ReplicatedPG::WaitingOnReplicas::WaitingOnReplicas(my_context ctx)
  : my_base(ctx),
    NamedState(context< SnapTrimmer >().pg->cct, "Trimming/WaitingOnReplicas")
{
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::WaitingOnReplicas::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);

  // Clean up repops in case of reset
  set<RepGather *> &repops = context<SnapTrimmer>().repops;
  for (set<RepGather *>::iterator i = repops.begin();
       i != repops.end();
       repops.erase(i++)) {
    (*i)->put();
  }
}

boost::statechart::result ReplicatedPG::WaitingOnReplicas::react(const SnapTrim&)
{
  // Have all the repops applied?
  dout(10) << "Waiting on Replicas react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  set<RepGather *> &repops = context<SnapTrimmer>().repops;
  for (set<RepGather *>::iterator i = repops.begin();
       i != repops.end();
       repops.erase(i++)) {
    if (!(*i)->all_applied || !(*i)->all_committed) {
      return discard_event();
    } else {
      (*i)->put();
    }
  }

  snapid_t &sn = context<SnapTrimmer>().snap_to_trim;
  dout(10) << "WaitingOnReplicas: adding snap " << sn << " to purged_snaps"
	   << dendl;

  pg->info.purged_snaps.insert(sn);
  pg->snap_trimq.erase(sn);
  dout(10) << "purged_snaps now " << pg->info.purged_snaps << ", snap_trimq now " 
	   << pg->snap_trimq << dendl;
  
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  pg->dirty_big_info = true;
  pg->write_if_dirty(*t);
  int tr = pg->osd->store->queue_transaction_and_cleanup(pg->osr.get(), t);
  assert(tr == 0);

  context<SnapTrimmer>().need_share_pg_info = true;

  // Back to the start
  pg->queue_snap_trim();
  return transit< NotTrimming >();
}

void ReplicatedPG::replace_cached_attrs(
  OpContext *ctx,
  ObjectContextRef obc,
  const map<string, bufferlist> &new_attrs)
{
  ctx->pending_attrs[obc].clear();
  for (map<string, bufferlist>::iterator i = obc->attr_cache.begin();
       i != obc->attr_cache.end();
       ++i) {
    ctx->pending_attrs[obc][i->first] = boost::optional<bufferlist>();
  }
  for (map<string, bufferlist>::const_iterator i = new_attrs.begin();
       i != new_attrs.end();
       ++i) {
    ctx->pending_attrs[obc][i->first] = i->second;
  }
}

void ReplicatedPG::setattr_maybe_cache(
  ObjectContextRef obc,
  OpContext *op,
  PGBackend::PGTransaction *t,
  const string &key,
  bufferlist &val)
{
  if (pool.info.require_rollback()) {
    op->pending_attrs[obc][key] = val;
  }
  t->setattr(obc->obs.oi.soid, key, val);
}

void ReplicatedPG::rmattr_maybe_cache(
  ObjectContextRef obc,
  OpContext *op,
  PGBackend::PGTransaction *t,
  const string &key)
{
  if (pool.info.require_rollback()) {
    op->pending_attrs[obc][key] = boost::optional<bufferlist>();
  }
  t->rmattr(obc->obs.oi.soid, key);
}

int ReplicatedPG::getattr_maybe_cache(
  ObjectContextRef obc,
  const string &key,
  bufferlist *val)
{
  if (pool.info.require_rollback()) {
    map<string, bufferlist>::iterator i = obc->attr_cache.find(key);
    if (i != obc->attr_cache.end()) {
      if (val)
	*val = i->second;
      return 0;
    } else {
      return -ENODATA;
    }
  }
  return pgbackend->objects_get_attr(obc->obs.oi.soid, key, val);
}

int ReplicatedPG::getattrs_maybe_cache(
  ObjectContextRef obc,
  map<string, bufferlist> *out,
  bool user_only)
{
  int r = 0;
  if (pool.info.require_rollback()) {
    if (out)
      *out = obc->attr_cache;
  } else {
    r = pgbackend->objects_get_attrs(obc->obs.oi.soid, out);
  }
  if (out && user_only) {
    map<string, bufferlist> tmp;
    for (map<string, bufferlist>::iterator i = out->begin();
	 i != out->end();
	 ++i) {
      if (i->first.size() > 1 && i->first[0] == '_')
	tmp[i->first.substr(1, i->first.size())].claim(i->second);
    }
    tmp.swap(*out);
  }
  return r;
}

void intrusive_ptr_add_ref(ReplicatedPG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(ReplicatedPG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
uint64_t get_with_id(ReplicatedPG *pg) { return pg->get_with_id(); }
void put_with_id(ReplicatedPG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif

void intrusive_ptr_add_ref(ReplicatedPG::RepGather *repop) { repop->get(); }
void intrusive_ptr_release(ReplicatedPG::RepGather *repop) { repop->put(); }
