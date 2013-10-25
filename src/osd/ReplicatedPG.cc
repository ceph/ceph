// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
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
  OSDService *osd,
  OpRequestRef op, int tag_inb, int tag_lat)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->get_req()->get_recv_stamp();

  uint64_t inb = op->get_req()->get_data().length();

  osd->logger->inc(l_osd_sop);

  osd->logger->inc(l_osd_sop_inb, inb);
  osd->logger->tinc(l_osd_sop_lat, latency);

  if (tag_inb)
    osd->logger->inc(tag_inb, inb);
  osd->logger->tinc(tag_lat, latency);
}

// ======================
// PGBackend::Listener


void ReplicatedPG::on_local_recover_start(
  const hobject_t &oid,
  ObjectStore::Transaction *t)
{
  pg_log.revise_have(oid, eversion_t());
  remove_snap_mapped_object(*t, oid);
  t->remove(coll, oid);
}

void ReplicatedPG::on_local_recover(
  const hobject_t &hoid,
  const object_stat_sum_t &stat_diff,
  const ObjectRecoveryInfo &_recovery_info,
  ObjectContextRef obc,
  ObjectStore::Transaction *t
  )
{
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
    if (waiting_for_missing_object.count(hoid)) {
      dout(20) << " kicking waiters on " << hoid << dendl;
      requeue_ops(waiting_for_missing_object[hoid]);
      waiting_for_missing_object.erase(hoid);
      if (pg_log.get_missing().missing.size() == 0) {
	requeue_ops(waiting_for_all_missing);
	waiting_for_all_missing.clear();
      }
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
  publish_stats_to_osd();
  dout(10) << "pushed " << soid << " to all replicas" << dendl;
  map<hobject_t, ObjectContextRef>::iterator i = recovering.find(soid);
  assert(i != recovering.end());
  recovering.erase(i);
  finish_recovery_op(soid);
  if (waiting_for_degraded_object.count(soid)) {
    requeue_ops(waiting_for_degraded_object[soid]);
    waiting_for_degraded_object.erase(soid);
  }
  finish_degraded_object(soid);
}

void ReplicatedPG::on_peer_recover(
  int peer,
  const hobject_t &soid,
  const ObjectRecoveryInfo &recovery_info,
  const object_stat_sum_t &stat)
{
  info.stats.stats.sum.add(stat);
  publish_stats_to_osd();
  // done!
  peer_missing[peer].got(soid, recovery_info.version);
  if (peer == backfill_target && backfills_in_flight.count(soid)) {
    map<hobject_t, ObjectContextRef>::iterator i = recovering.find(soid);
    assert(i != recovering.end());
    list<OpRequestRef> requeue_list;
    i->second->drop_backfill_read(&requeue_list);
    requeue_ops(requeue_list);
    backfills_in_flight.erase(soid);
  }
}

void ReplicatedPG::begin_peer_recover(
  int peer,
  const hobject_t soid)
{
  peer_missing[peer].revise_have(soid, eversion_t());
}

// =======================
// pg changes

bool ReplicatedPG::same_for_read_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_modify_since(epoch_t e)
{
  return (e >= info.history.same_primary_since);
}

bool ReplicatedPG::same_for_rep_modify_since(epoch_t e)
{
  // check osd map: same set, or primary+acker?
  return e >= info.history.same_primary_since;
}

// ====================
// missing objects

bool ReplicatedPG::is_missing_object(const hobject_t& soid)
{
  return pg_log.get_missing().missing.count(soid);
}

void ReplicatedPG::wait_for_missing_object(const hobject_t& soid, OpRequestRef op)
{
  assert(is_missing_object(soid));

  const pg_missing_t &missing = pg_log.get_missing();

  // we don't have it (yet).
  map<hobject_t, pg_missing_t::item>::const_iterator g = missing.missing.find(soid);
  assert(g != missing.missing.end());
  const eversion_t &v(g->second.need);

  map<hobject_t, ObjectContextRef>::const_iterator p = recovering.find(soid);
  if (p != recovering.end()) {
    dout(7) << "missing " << soid << " v " << v << ", already recovering." << dendl;
  }
  else if (missing_loc.find(soid) == missing_loc.end()) {
    dout(7) << "missing " << soid << " v " << v << ", is unfound." << dendl;
  }
  else {
    dout(7) << "missing " << soid << " v " << v << ", recovering." << dendl;
    PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
    recover_missing(soid, v, cct->_conf->osd_client_op_priority, h);
    pgbackend->run_recovery_op(h, cct->_conf->osd_client_op_priority);
  }
  waiting_for_missing_object[soid].push_back(op);
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
  for (unsigned i = 1; i < acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
	peer_missing[peer].missing.count(soid))
      return true;

    // Object is degraded if after last_backfill AND
    // we are backfilling it
    if (peer == backfill_target &&
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
  } else {
    dout(7) << "degraded " 
	    << soid 
	    << ", recovering"
	    << dendl;
    eversion_t v;
    for (unsigned i = 1; i < acting.size(); i++) {
      int peer = acting[i];
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
    f->dump_unsigned("epoch", get_osdmap()->get_epoch());
    f->open_array_section("up");
    for (vector<int>::iterator p = up.begin(); p != up.end(); ++p)
      f->dump_unsigned("osd", *p);
    f->close_section();
    f->open_array_section("acting");
    for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p)
      f->dump_unsigned("osd", *p);
    f->close_section();
    f->open_object_section("info");
    info.dump(f.get());
    f->close_section();

    f->open_array_section("recovery_state");
    handle_query_state(f.get());
    f->close_section();

    f->close_section();
    f->flush(odata);
    return 0;
  }
  else if (command == "mark_unfound_lost") {
    string mulcmd;
    cmd_getval(cct, cmdmap, "mulcmd", mulcmd);
    if (mulcmd != "revert") {
      ss << "mode must be 'revert'; mark and delete not yet implemented";
      return -EINVAL;
    }
    int mode = pg_log_entry_t::LOST_REVERT;

    if (!is_primary()) {
      ss << "not primary";
      return -EROFS;
    }

    int unfound = missing.num_missing() - missing_loc.size();
    if (!unfound) {
      ss << "pg has no unfound objects";
      return 0;  // make command idempotent
    }

    if (!all_unfound_are_queried_or_lost(get_osdmap())) {
      ss << "pg has " << unfound
	 << " unfound objects but we haven't probed all sources, not marking lost";
      return -EINVAL;
    }

    ss << "pg has " << unfound << " objects unfound and apparently lost, marking";
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
	  map<hobject_t,set<int> >::iterator q = missing_loc.find(p->first);
	  if (q != missing_loc.end())
	    for (set<int>::iterator r = q->second.begin(); r != q->second.end(); ++r)
	      f->dump_int("osd", *r);
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
      if (m->get_pg() != info.pgid) {
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
	      ls_iter++;
	      missing_iter++;
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
				       CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK); 
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
			   const PGPool &_pool, pg_t p, const hobject_t& oid,
			   const hobject_t& ioid) :
  PG(o, curmap, _pool, p, oid, ioid),
  pgbackend(new ReplicatedBackend(this, coll_t(p), o)),
  snapset_contexts_lock("ReplicatedPG::snapset_contexts"),
  temp_seq(0),
  snap_trimmer_machine(this)
{ 
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
  // do any pending flush
  do_pending_flush();

  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }
  assert(!op_must_wait_for_map(get_osdmap(), op));
  if (can_discard_request(op)) {
    return;
  }
  if (!flushed) {
    dout(20) << " !flushed, waiting for active on " << op << dendl;
    waiting_for_active.push_back(op);
    return;
  }

  if (pgbackend->handle_message(op))
    return;

  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (is_replay() || !is_active()) {
      dout(20) << " replay, waiting for active on " << op << dendl;
      waiting_for_active.push_back(op);
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

  // order this op as a write?
  bool write_ordered = op->may_write() || (m->get_flags() & CEPH_OSD_FLAG_RWORDERED);

  dout(10) << "do_op " << *m
	   << (op->may_write() ? " may_write" : "")
	   << (op->may_read() ? " may_read" : "")
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
  if (is_missing_object(head)) {
    wait_for_missing_object(head, op);
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
  if (is_missing_object(snapdir)) {
    wait_for_missing_object(snapdir, op);
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

  ObjectContextRef obc;
  bool can_create = op->may_write();
  snapid_t snapid;
  hobject_t oid(m->get_oid(),
		m->get_object_locator().key,
		m->get_snapid(),
		m->get_pg().ps(),
		m->get_object_locator().get_pool(),
		m->get_object_locator().nspace);
  int r = find_object_context(oid, &obc, can_create, &snapid);

  if (r == -EAGAIN) {
    // If we're not the primary of this OSD, and we have
    // CEPH_OSD_FLAG_LOCALIZE_READS set, we just return -EAGAIN. Otherwise,
    // we have to wait for the object.
    if (is_primary() ||
	(!(m->get_flags() & CEPH_OSD_FLAG_BALANCE_READS) &&
	 !(m->get_flags() & CEPH_OSD_FLAG_LOCALIZE_READS))) {
      // missing the specific snap we need; requeue and wait.
      assert(!can_create); // only happens on a read
      hobject_t soid(m->get_oid(), m->get_object_locator().key,
		     snapid, m->get_pg().ps(),
		     info.pgid.pool(), m->get_object_locator().nspace);
      wait_for_missing_object(soid, op);
      return;
    }
  }

  if (hit_set) {
    hit_set->insert(oid);
    if (hit_set->is_full() ||
	hit_set_start_stamp + pool.info.hit_set_period <= m->get_recv_stamp()) {
      hit_set_persist();
    }
  }

  if ((m->get_flags() & CEPH_OSD_FLAG_IGNORE_OVERLAY) == 0 &&
      maybe_handle_cache(op, obc, r))
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
  if (obc->is_blocked()) {
    wait_for_blocked_object(obc->obs.oi.soid, op);
    return;
  }

  dout(25) << __func__ << ": object " << obc->obs.oi.soid
	   << " has oi of " << obc->obs.oi << dendl;

  // are writes blocked by another object?
  if (obc->blocked_by) {
    dout(10) << "do_op writes for " << obc->obs.oi.soid << " blocked by "
	     << obc->blocked_by->obs.oi.soid << dendl;
    wait_for_degraded_object(obc->blocked_by->obs.oi.soid, op);
    return;
  }

  // if we have src_oids, we need to be careful of the target being
  // before and a src being after the last_backfill line, or else the
  // operation won't apply properly on the backfill_target.  (the
  // opposite is not a problem; if the target is after the line, we
  // don't apply on the backfill_target and it doesn't matter.)
  pg_info_t *backfill_target_info = NULL;
  bool before_backfill = false;
  if (backfill_target >= 0) {
    backfill_target_info = &peer_info[backfill_target];
    before_backfill = obc->obs.oi.soid < backfill_target_info->last_backfill;
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
	snapid_t ssnapid;

	int r = find_object_context(src_oid, &sobc, false, &ssnapid);
	if (r == -EAGAIN) {
	  // missing the specific snap we need; requeue and wait.
	  hobject_t wait_oid(osd_op.soid.oid, src_oloc.key, ssnapid, m->get_pg().ps(),
			     info.pgid.pool(), m->get_object_locator().nspace);
	  wait_for_missing_object(wait_oid, op);
	} else if (r) {
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
		   (before_backfill && sobc->obs.oi.soid > backfill_target_info->last_backfill)) {
	    wait_for_degraded_object(sobc->obs.oi.soid, op);
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

  // any SNAPDIR op needs to hvae all clones present.  treat them as
  // src_obc's so that we track references properly and clean up later.
  if (m->get_snapid() == CEPH_SNAPDIR) {
    for (vector<snapid_t>::iterator p = obc->ssc->snapset.clones.begin();
	 p != obc->ssc->snapset.clones.end();
	 ++p) {
      object_locator_t src_oloc;
      get_src_oloc(m->get_oid(), m->get_object_locator(), src_oloc);
      hobject_t clone_oid = obc->obs.oi.soid;
      clone_oid.snap = *p;
      if (!src_obc.count(clone_oid)) {
	ObjectContextRef sobc;
	snapid_t ssnapid;

	int r = find_object_context(clone_oid, &sobc, false, &ssnapid);
	if (r == -EAGAIN) {
	  // missing the specific snap we need; requeue and wait.
	  hobject_t wait_oid(clone_oid.oid, src_oloc.key, ssnapid, m->get_pg().ps(),
			     info.pgid.pool(), clone_oid.get_namespace());
	  wait_for_missing_object(wait_oid, op);
	} else if (r) {
	  osd->reply_op_error(op, r);
	} else if (sobc->obs.oi.is_whiteout()) {
	  osd->reply_op_error(op, -ENOENT);
	} else {
	  dout(10) << " clone_oid " << clone_oid << " obc " << sobc << dendl;
	  src_obc[clone_oid] = sobc;
	  continue;
	}
	return;
      } else {
	continue;
      }
    }
  }

  OpContext *ctx = new OpContext(op, m->get_reqid(), m->ops,
				 &obc->obs, obc->ssc, 
				 this);
  ctx->obc = obc;
  if (!get_rw_locks(ctx)) {
    op->mark_delayed("waiting for rw locks");
    close_op_ctx(ctx);
    return;
  }

  if ((op->may_read()) && (obc->obs.oi.is_lost())) {
    // This object is lost. Reading from it returns an error.
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
	     << " is lost" << dendl;
    close_op_ctx(ctx);
    osd->reply_op_error(op, -ENFILE);
    return;
  }
  if (!op->may_write() && (!obc->obs.exists ||
                           obc->obs.oi.is_whiteout())) {
    close_op_ctx(ctx);
    osd->reply_op_error(op, -ENOENT);
    return;
  }

  op->mark_started();
  ctx->src_obc = src_obc;

  execute_ctx(ctx);
}

bool ReplicatedPG::maybe_handle_cache(OpRequestRef op, ObjectContextRef obc,
                                      int r)
{
  if (obc.get() && obc->is_blocked()) {
    // we're already doing something with this object
    return false;
  }

  switch(pool.info.cache_mode) {
  case pg_pool_t::CACHEMODE_NONE:
    return false;

  case pg_pool_t::CACHEMODE_WRITEBACK:
    if (obc.get() && obc->obs.exists) {
      return false;
    }
    if (can_skip_promote(op, obc)) {
      return false;
    }
    promote_object(op, obc);
    return true;

  case pg_pool_t::CACHEMODE_INVALIDATE_FORWARD:
    do_cache_redirect(op, obc);
    return true;

  case pg_pool_t::CACHEMODE_READONLY: // TODO: clean this case up
    if (!obc.get() && r == -ENOENT) {
      // we don't have the object and op's a read
      promote_object(op, obc);
      return true;
    }
    if (obc.get() && obc->obs.exists) { // we have the object locally
      return false;
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
                                       get_osdmap()->get_epoch(), flags);
  request_redirect_t redir(m->get_object_locator(), pool.info.tier_of);
  reply->set_redirect(redir);
  dout(10) << "sending redirect to pool " << pool.info.tier_of << " for op "
	   << op << dendl;
  m->get_connection()->get_messenger()->send_message(reply, m->get_connection());
  return;
}

void ReplicatedPG::promote_object(OpRequestRef op, ObjectContextRef obc)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  if (!obc.get()) { // we need to create an ObjectContext
    int r = find_object_context(
      hobject_t(m->get_oid(),
	      m->get_object_locator().key,
	      m->get_snapid(),
	      m->get_pg().ps(),
	      m->get_object_locator().get_pool(),
	      m->get_object_locator().nspace),
      &obc, true, NULL);
    assert(r == 0); // a lookup that allows creates can't fail now
  }
dout(10) << __func__ << " " << obc->obs.oi.soid << dendl;

  hobject_t temp_target = generate_temp_object();
  PromoteCallback *cb = new PromoteCallback(op, obc, temp_target, this);
  object_locator_t oloc(m->get_object_locator());
  oloc.pool = pool.info.tier_of;
  start_copy(cb, obc, obc->obs.oi.soid, oloc, 0, temp_target);

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
  ctx->op_t = ObjectStore::Transaction();
  ctx->local_t = ObjectStore::Transaction();

  // dup/replay?
  if (op->may_write()) {
    const pg_log_entry_t *entry = pg_log.get_log().get_request(ctx->reqid);
    if (entry) {
      const eversion_t& oldv = entry->version;
      dout(3) << "do_op dup " << ctx->reqid << " was " << oldv << dendl;
      if (already_complete(oldv)) {
	reply_ctx(ctx, 0, oldv, entry->user_version);
      } else {
	close_op_ctx(ctx);

	if (m->wants_ack()) {
	  if (already_ack(oldv)) {
	    MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
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

    op->mark_started();

    // snap
    if (pool.info.is_pool_snaps_mode()) {
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

  ctx->user_at_version = obc->obs.oi.user_version;

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
    close_op_ctx(ctx);
    return;
  }

  // check for full
  if (ctx->delta_stats.num_bytes > 0 &&
      pool.info.get_flags() & pg_pool_t::FLAG_FULL) {
    reply_ctx(ctx, -ENOSPC);
    return;
  }

  // prepare the reply
  ctx->reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);

  // Write operations aren't allowed to return a data payload because
  // we can't do so reliably. If the client has to resend the request
  // and it has already been applied, we will return 0 with no
  // payload.  Non-deterministic behavior is no good.  However, it is
  // possible to construct an operation that does a read, does a guard
  // check (e.g., CMPXATTR), and then a write.  Then we either succeed
  // with the write, or return a CMPXATTR and the read value.
  if ((ctx->op_t.empty() && !ctx->modify) || result < 0) {
    // read.
    ctx->reply->claim_op_out_data(ctx->ops);
    ctx->reply->get_header().data_off = ctx->data_off;
  } else {
    // write.  normalize the result code.
    if (result > 0) {
      dout(20) << " zeroing write result code " << result << dendl;
      result = 0;
    }
  }
  ctx->reply->set_result(result);

  // read or error?
  if (ctx->op_t.empty() || result < 0) {
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
    close_op_ctx(ctx);
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  assert(op->may_write());

  // trim log?
  calc_trim_to();

  append_log(ctx->log, pg_trim_to, ctx->local_t);
  
  // verify that we are doing this in order?
  if (cct->_conf->osd_debug_op_order && m->get_source().is_client()) {
    map<client_t,tid_t>& cm = debug_op_order[obc->obs.oi.soid];
    tid_t t = m->get_tid();
    client_t n = m->get_source().num();
    map<client_t,tid_t>::iterator p = cm.find(n);
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
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(ctx, obc, rep_tid);  // new repop claims our obc, src_obc refs
  // note: repop now owns ctx AND ctx->op

  repop->src_obc.swap(src_obc); // and src_obc.

  issue_repop(repop, now);

  eval_repop(repop);
  repop->put();
}

void ReplicatedPG::reply_ctx(OpContext *ctx, int r)
{
  osd->reply_op_error(ctx->op, r);
  close_op_ctx(ctx);
}

void ReplicatedPG::reply_ctx(OpContext *ctx, int r, eversion_t v, version_t uv)
{
  osd->reply_op_error(ctx->op, r, v, uv);
  close_op_ctx(ctx);
}

void ReplicatedPG::log_op_stats(OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());

  utime_t now = ceph_clock_now(cct);
  utime_t latency = now;
  latency -= ctx->op->get_req()->get_recv_stamp();

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

  if (op->may_read() && op->may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->tinc(l_osd_op_rw_rlat, rlatency);
    osd->logger->tinc(l_osd_op_rw_lat, latency);
  } else if (op->may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->tinc(l_osd_op_r_lat, latency);
  } else if (op->may_write()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->tinc(l_osd_op_w_rlat, rlatency);
    osd->logger->tinc(l_osd_op_w_lat, latency);
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

  sub_op_modify(op);
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

  sub_op_modify_reply(op);
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
      MOSDPGScan *reply = new MOSDPGScan(MOSDPGScan::OP_SCAN_DIGEST,
					 get_osdmap()->get_epoch(), m->query_epoch,
					 info.pgid, bi.begin, bi.end);
      ::encode(bi.objects, reply->get_data());
      osd->send_message_osd_cluster(reply, m->get_connection());
    }
    break;

  case MOSDPGScan::OP_SCAN_DIGEST:
    {
      int from = m->get_source().num();
      assert(from == backfill_target);
      BackfillInterval& bi = peer_backfill_info;
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
	  if (first.pool == -1)
	    first.pool = info.pgid.pool();
	  bi.objects[first] = i->second;
	}
      }

      assert(waiting_on_backfill);
      waiting_on_backfill = false;
      finish_recovery_op(bi.begin);
    }
    break;
  }
}

void ReplicatedBackend::_do_push(OpRequestRef op)
{
  MOSDPGPush *m = static_cast<MOSDPGPush *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_PUSH);
  int from = m->get_source().num();

  vector<PushReplyOp> replies;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<PushOp>::iterator i = m->pushes.begin();
       i != m->pushes.end();
       ++i) {
    replies.push_back(PushReplyOp());
    handle_push(from, *i, &(replies.back()), t);
  }

  MOSDPGPushReply *reply = new MOSDPGPushReply;
  reply->set_priority(m->get_priority());
  reply->pgid = get_info().pgid;
  reply->map_epoch = m->map_epoch;
  reply->replies.swap(replies);
  reply->compute_cost(cct);

  t->register_on_complete(
    new C_OSD_SendMessageOnConn(
      osd, reply, m->get_connection()));

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
  int from = m->get_source().num();

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
      new C_QueueInWQ(
	&osd->push_wq,
	get_parent()->bless_gencontext(c)));
  }
  replies.erase(replies.end() - 1);

  if (replies.size()) {
    MOSDPGPull *reply = new MOSDPGPull;
    reply->set_priority(m->get_priority());
    reply->pgid = get_info().pgid;
    reply->map_epoch = m->map_epoch;
    reply->pulls.swap(replies);
    reply->compute_cost(cct);

    t->register_on_complete(
      new C_OSD_SendMessageOnConn(
	osd, reply, m->get_connection()));
  }

  get_parent()->queue_transaction(t);
}

void ReplicatedBackend::do_pull(OpRequestRef op)
{
  MOSDPGPull *m = static_cast<MOSDPGPull *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_PULL);
  int from = m->get_source().num();

  map<int, vector<PushOp> > replies;
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
  int from = m->get_source().num();

  vector<PushOp> replies(1);
  for (vector<PushReplyOp>::iterator i = m->replies.begin();
       i != m->replies.end();
       ++i) {
    bool more = handle_push_reply(from, *i, &(replies.back()));
    if (more)
      replies.push_back(PushOp());
  }
  replies.erase(replies.end() - 1);

  map<int, vector<PushOp> > _replies;
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
      assert(is_replica());
      assert(cct->_conf->osd_kill_backfill_at != 1);

      MOSDPGBackfill *reply = new MOSDPGBackfill(MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
						 get_osdmap()->get_epoch(), m->query_epoch,
						 info.pgid);
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
      assert(is_replica());
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
      int tr = osd->store->queue_transaction(osr.get(), t);
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
  ObjectContextRef obc;
  int r = find_object_context(coid, &obc, false, NULL);
  if (r == -ENOENT || coid.snap != obc->obs.oi.soid.snap) {
    derr << __func__ << "could not find coid " << coid << dendl;
    assert(0);
  }
  assert(r == 0);

  object_info_t &coi = obc->obs.oi;
  set<snapid_t> old_snaps(coi.snaps.begin(), coi.snaps.end());
  assert(old_snaps.size());

  // get snap set context
  if (!obc->ssc)
    obc->ssc = get_snapset_context(
      coid.oid,
      coid.get_key(),
      coid.hash,
      false,
      coid.get_namespace());

  assert(obc->ssc);
  SnapSet& snapset = obc->ssc->snapset;

  dout(10) << coid << " old_snaps " << old_snaps
	   << " old snapset " << snapset << dendl;
  assert(snapset.seq);

  RepGather *repop = simple_repop_create(obc);
  OpContext *ctx = repop->ctx;
  ctx->at_version = get_next_version();

  ObjectStore::Transaction *t = &ctx->op_t;
  OSDriver::OSTransaction os_t(osdriver.get_transaction(t));
    
  set<snapid_t> new_snaps;
  for (set<snapid_t>::iterator i = old_snaps.begin();
       i != old_snaps.end();
       ++i) {
    if (!pool.info.is_removed_snap(*i))
      new_snaps.insert(*i);
  }

  r = snap_mapper.update_snaps(
    coid,
    new_snaps,
    &old_snaps, // debug
    &os_t);
  if (r != 0) {
    derr << __func__ << ": snap_mapper.update_snap returned "
	 << cpp_strerror(r) << dendl;
    assert(0);
  }

  if (new_snaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << old_snaps << " -> "
	     << new_snaps << " ... deleting" << dendl;
    t->remove(coll, coid);

    // ...from snapset
    snapid_t last = coid.snap;
    vector<snapid_t>::iterator p;
    for (p = snapset.clones.begin(); p != snapset.clones.end(); ++p)
      if (*p == last)
	break;
    assert(p != snapset.clones.end());
    object_stat_sum_t delta;
    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      interval_set<uint64_t> keep;
      keep.union_of(
	snapset.clone_overlap[*n],
	snapset.clone_overlap[*p]);
      add_interval_usage(keep, delta);  // not deallocated
      snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);
    } else {
      add_interval_usage(
	snapset.clone_overlap[last],
	delta);  // not deallocated
    }
    delta.num_objects--;
    delta.num_object_clones--;
    delta.num_bytes -= snapset.clone_size[last];
    info.stats.stats.add(delta, obc->obs.oi.category);

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
    t->setattr(coll, coid, OI_ATTR, bl);

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
    ::encode(coi.snaps, ctx->log.back().snaps);
    ctx->at_version.version++;
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << dendl;

  hobject_t snapoid(
    coid.oid, coid.get_key(),
    snapset.head_exists ? CEPH_NOSNAP:CEPH_SNAPDIR, coid.hash,
    info.pgid.pool(), coid.get_namespace());
  ctx->snapset_obc = get_object_context(snapoid, false);

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

    t->remove(coll, snapoid);
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
    t->setattr(coll, snapoid, SS_ATTR, bl);

    bl.clear();
    ::encode(ctx->snapset_obc->obs.oi, bl);
    t->setattr(coll, snapoid, OI_ATTR, bl);
  }

  return repop;
}

void ReplicatedPG::snap_trimmer()
{
  lock();
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
  if (snap_trimmer_machine.requeue) {
    dout(10) << "snap_trimmer requeue" << dendl;
    queue_snap_trim();
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
  const char *v1, *v2;
  v1 = v1s.data();
  string v2s;
  if (xattr.length()) {
    v2s = string(xattr.c_str(), xattr.length());
    v2 = v2s.c_str();
  } else
    v2 = "";

  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2 << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (strcmp(v1, v2) == 0);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (strcmp(v1, v2) != 0);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (strcmp(v1, v2) > 0);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (strcmp(v1, v2) >= 0);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (strcmp(v1, v2) < 0);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (strcmp(v1, v2) <= 0);
  default:
    return -EINVAL;
  }
}

// ========================================================================
// low level osd ops

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

int ReplicatedPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
{
  int result = 0;
  SnapSetContext *ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;

  bool first_read = true;

  ObjectStore::Transaction& t = ctx->op_t;

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
    case CEPH_OSD_OP_UNDIRTY:
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

    case CEPH_OSD_OP_READ:
      ++ctx->num_read;
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->read(coll, soid, op.extent.offset, op.extent.length, bl);
	if (first_read) {
	  first_read = false;
	  ctx->data_off = op.extent.offset;
	}
	osd_op.outdata.claim_append(bl);
	if (r >= 0) 
	  op.extent.length = r;
	else {
	  result = r;
	  op.extent.length = 0;
	}
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(op.extent.length, 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " read got " << r << " / " << op.extent.length << " bytes from obj " << soid << dendl;

	__u32 seq = oi.truncate_seq;
	// are we beyond truncate_size?
	if ( (seq < op.extent.truncate_seq) &&
	     (op.extent.offset + op.extent.length > op.extent.truncate_size) ) {

	  // truncated portion of the read
	  unsigned from = MAX(op.extent.offset, op.extent.truncate_size);  // also end of data
	  unsigned to = op.extent.offset + op.extent.length;
	  unsigned trim = to-from;

	  op.extent.length = op.extent.length - trim;

	  bufferlist keep;

	  // keep first part of osd_op.outdata; trim at truncation point
	  dout(10) << " obj " << soid << " seq " << seq
	           << ": trimming overlap " << from << "~" << trim << dendl;
	  keep.substr_of(osd_op.outdata, 0, osd_op.outdata.length() - trim);
          osd_op.outdata.claim(keep);
	}
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_MAPEXT:
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
	    r = osd->store->read(coll, soid, last, len, t);
	    if (!t.is_zero()) {
	      osd->clog.error() << coll << " " << soid << " sparse-read found data in hole "
				<< last << "~" << len << "\n";
	    }
	  }

          bufferlist tmpbl;
          r = osd->store->read(coll, soid, miter->first, miter->second, tmpbl);
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
	    r = osd->store->read(coll, soid, last, len, t);
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
	ctx->undirty = true;  // see make_writeable()
	ctx->modify = true;
	ctx->delta_stats.num_wr++;
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
	result = _delete_head(ctx, true);
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = pgbackend->objects_get_attr(
	  soid,
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
	map<string,bufferptr> attrset;
        result = osd->store->getattrs(coll, soid, attrset, true);
        map<string, bufferptr>::iterator iter;
        map<string, bufferlist> newattrs;
        for (iter = attrset.begin(); iter != attrset.end(); ++iter) {
           bufferlist bl;
           bl.append(iter->second);
           newattrs[iter->first] = bl;
        }
        
        bufferlist bl;
        ::encode(newattrs, bl);
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
	  result = pgbackend->objects_get_attr(
	    soid,
	    name,
	    &xattr);
	else
	  result = pgbackend->objects_get_attr(
	    src_obc->obs.oi.soid,
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
	  ssc = ctx->obc->ssc = get_snapset_context(soid.oid,
		    soid.get_key(), soid.hash, false,  soid.get_namespace());
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
        if (ssc->snapset.head_exists) {
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


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      ++ctx->num_write;
      { // write
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
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
	    t.truncate(coll, soid, op.extent.truncate_size);
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
	t.write(coll, soid, op.extent.offset, op.extent.length, osd_op.indata);
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
	if (obs.exists) {
	  t.truncate(coll, soid, 0);
	} else {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.write(coll, soid, op.extent.offset, op.extent.length, osd_op.indata);
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
      ++ctx->num_write;
      { // zero
	result = check_offset_and_length(op.extent.offset, op.extent.length, cct->_conf->osd_max_object_size);
	if (result < 0)
	  break;
	assert(op.extent.length);
	if (obs.exists && !oi.is_whiteout()) {
	  t.zero(coll, soid, op.extent.offset, op.extent.length);
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
	  if (result >= 0 && !obs.exists) {
	    t.touch(coll, soid);
	    ctx->delta_stats.num_objects++;
	    obs.exists = true;
	  }
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      ++ctx->num_write;
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

	t.truncate(coll, soid, op.extent.offset);
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
	result = _delete_head(ctx, false);
      }
      break;

    case CEPH_OSD_OP_CLONERANGE:
      ++ctx->num_read;
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  t.touch(coll, obs.oi.soid);
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
	t.clone_range(coll, src_obc->obs.oi.soid,
		      obs.oi.soid, op.clonerange.src_offset,
		      op.clonerange.length, op.clonerange.offset);
		      

	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.clonerange.offset, op.clonerange.length, false);
      }
      break;
      
    case CEPH_OSD_OP_WATCH:
      ++ctx->num_write;
      {
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
	    t.nop();  // make sure update the object_info on disk!
	  }
	  ctx->watch_connects.push_back(w);
        } else {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by "
		     << entity << dendl;
            oi.watchers.erase(oi_iter);
	    t.nop();  // update oi on disk
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
	  t.touch(coll, soid);
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t.setattr(coll, soid, name, bl);
 	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      ++ctx->num_write;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	t.rmattr(coll, soid, name);
 	ctx->delta_stats.num_wr++;
      }
      break;
    

      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
      ++ctx->num_write;
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
	do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      t.start_sync();
      break;


      // -- trivial map --
    case CEPH_OSD_OP_TMAPGET:
      ++ctx->num_read;
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
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
      ++ctx->num_write;
      result = do_tmapup(ctx, bp, osd_op);
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

	{
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
	}
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

	{
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
	}
	::encode(out_set, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += SHIFT_ROUND_UP(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETHEADER:
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
	osd->store->omap_get_values(coll, soid, keys_to_get, &out);
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
	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	r = 0;
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
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.touch(coll, soid);
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
	t.omap_setkeys(coll, soid, to_set);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_OMAPSETHEADER:
      ++ctx->num_write;
      {
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.touch(coll, soid);
	t.omap_setheader(coll, soid, osd_op.indata);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_OMAPCLEAR:
      ++ctx->num_write;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	t.touch(coll, soid);
	t.omap_clear(coll, soid);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_OMAPRMKEYS:
      ++ctx->num_write;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	t.touch(coll, soid);
	set<string> to_rm;
	try {
	  ::decode(to_rm, bp);
	}
	catch (buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t.omap_rmkeys(coll, soid, to_rm);
	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_COPY_GET_CLASSIC:
      ++ctx->num_read;
      result = fill_in_copy_get(bp, osd_op, oi, true);
      if (result == -EINVAL)
	goto fail;
      break;

    case CEPH_OSD_OP_COPY_GET:
      ++ctx->num_read;
      result = fill_in_copy_get(bp, osd_op, oi, false);
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
	  hobject_t temp_target = generate_temp_object();
	  CopyFromCallback *cb = new CopyFromCallback(ctx, temp_target);
	  ctx->copy_cb = cb;
	  start_copy(cb, ctx->obc, src, src_oloc, src_version,
	                      temp_target);
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
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK))
      result = 0;

    if (result < 0)
      break;
  }
  return result;
}

int ReplicatedPG::_get_tmap(OpContext *ctx,
			    map<string, bufferlist> *out,
			    bufferlist *header)
{
  vector<OSDOp> nops(1);
  OSDOp &newop = nops[0];
  newop.op.op = CEPH_OSD_OP_TMAPGET;
  do_osd_ops(ctx, nops);
  try {
    bufferlist::iterator i = newop.outdata.begin();
    ::decode(*header, i);
    ::decode(*out, i);
  } catch (...) {
    dout(20) << "unsuccessful at decoding tmap for " << ctx->new_obs.oi.soid
	     << dendl;
    return -EINVAL;
  }
  dout(20) << "successful at decoding tmap for " << ctx->new_obs.oi.soid
	   << dendl;
  return 0;
}

inline int ReplicatedPG::_delete_head(OpContext *ctx, bool no_whiteout)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  ObjectStore::Transaction& t = ctx->op_t;

  if (!obs.exists || (obs.oi.is_whiteout() && !no_whiteout))
    return -ENOENT;
  
  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
  }

  ctx->delta_stats.num_wr++;
  ctx->delta_stats.num_bytes -= oi.size;
  oi.size = 0;

  // cache: writeback: set whiteout on delete?
  if (pool.info.cache_mode == pg_pool_t::CACHEMODE_WRITEBACK && !no_whiteout) {
    dout(20) << __func__ << " setting whiteout on " << soid << dendl;
    oi.set_flag(object_info_t::FLAG_WHITEOUT);
    t.truncate(coll, soid, 0);
    t.omap_clear(coll, soid);
    t.rmattrs(coll, soid);
    return 0;
  }

  t.remove(coll, soid);
  ctx->delta_stats.num_objects--;
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
  ObjectStore::Transaction& t = ctx->op_t;
  snapid_t snapid = (uint64_t)op.snap.snapid;
  snapid_t cloneid = 0;

  dout(10) << "_rollback_to " << soid << " snapid " << snapid << dendl;

  ObjectContextRef rollback_to;
  int ret = find_object_context(
    hobject_t(soid.oid, soid.get_key(), snapid, soid.hash, info.pgid.pool(), soid.get_namespace()),
    &rollback_to, false, &cloneid);
  if (ret == -ENOENT || (rollback_to && rollback_to->obs.oi.is_whiteout())) {
    // there's no snapshot here, or there's no object.
    // if there's no snapshot, we delete the object; otherwise, do nothing.
    dout(20) << "_rollback_to deleting head on " << soid.oid
	     << " because got ENOENT|whiteout on find_object_context" << dendl;
    if (ctx->obc->obs.oi.watchers.size()) {
      // Cannot delete an object with watchers
      ret = -EBUSY;
    } else {
      _delete_head(ctx, false);
      ret = 0;
    }
  } else if (-EAGAIN == ret) {
    /* a different problem, like degraded pool
     * with not-yet-restored object. We shouldn't have been able
     * to get here; recovery should have completed first! */
    hobject_t rollback_target(soid.oid, soid.get_key(), cloneid, soid.hash,
			      info.pgid.pool(), soid.get_namespace());
    assert(is_missing_object(rollback_target));
    dout(20) << "_rollback_to attempted to roll back to a missing object "
	     << rollback_target << " (requested snapid: ) " << snapid << dendl;
    wait_for_missing_object(rollback_target, ctx->op);
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

      if (obs.exists)
	t.remove(coll, soid);
      
      t.clone(coll,
	      rollback_to_sobject, soid);
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

void ReplicatedPG::_make_clone(ObjectStore::Transaction& t,
			       const hobject_t& head, const hobject_t& coid,
			       object_info_t *poi)
{
  bufferlist bv;
  ::encode(*poi, bv);

  t.clone(coll, head, coid);
  t.setattr(coll, coid, OI_ATTR, bv);
  t.rmattr(coll, coid, SS_ATTR);
}

void ReplicatedPG::make_writeable(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  SnapContext& snapc = ctx->snapc;
  ObjectStore::Transaction t;

  // clone?
  assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ctx->snapset
	   << "  snapc=" << snapc << dendl;;
  
  // we will mark the object dirty
  if (ctx->undirty) {
    dout(20) << " clearing DIRTY flag" << dendl;
    ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
  } else {
    dout(20) << " setting DIRTY flag" << dendl;
    ctx->new_obs.oi.set_flag(object_info_t::FLAG_DIRTY);
  }

  // use newer snapc?
  if (ctx->new_snapset.seq > snapc.seq) {
    snapc.seq = ctx->new_snapset.seq;
    snapc.snaps = ctx->new_snapset.snaps;
    dout(10) << " using newer snapc " << snapc << dendl;
  }

  if (ctx->obs->exists)
    filter_snapc(snapc);
  
  if (ctx->obs->exists &&               // head exist(ed)
      snapc.snaps.size() &&                 // there are snaps
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
      snap_oi = &ctx->clone_obc->obs.oi;
    } else {
      snap_oi = &static_snap_oi;
    }
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);
    snap_oi->snaps = snaps;
    _make_clone(t, soid, coid, snap_oi);
    
    OSDriver::OSTransaction _t(osdriver.get_transaction(&(ctx->local_t)));
    set<snapid_t> _snaps(snaps.begin(), snaps.end());
    snap_mapper.add_oid(coid, _snaps, &_t);
    
    ctx->delta_stats.num_objects++;
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

    ctx->at_version.version++;
  }

  // update most recent clone_overlap and usage stats
  if (ctx->new_snapset.clones.size() > 0) {
    interval_set<uint64_t> &newest_overlap = ctx->new_snapset.clone_overlap.rbegin()->second;
    ctx->modified_ranges.intersection_of(newest_overlap);
    // modified_ranges is still in use by the clone
    add_interval_usage(ctx->modified_ranges, ctx->delta_stats);
    newest_overlap.subtract(ctx->modified_ranges);
  }
  
  // prepend transaction to op_t
  t.append(ctx->op_t);
  t.swap(ctx->op_t);

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
    dout(10) << "notify_ack " << make_pair(p->watch_cookie, p->notify_id) << dendl;
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
  hobject_t hoid(object_t(ss.str()), "", CEPH_NOSNAP, 0, -1, "");
  pgbackend->add_temp_obj(hoid);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

int ReplicatedPG::prepare_transaction(OpContext *ctx)
{
  assert(!ctx->ops.empty());
  
  const hobject_t& soid = ctx->obs->oi.soid;

  // we'll need this to log
  eversion_t old_version = ctx->obs->oi.version;

  bool head_existed = ctx->obs->exists;

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
  if (ctx->op_t.empty() && !ctx->modify) {
    unstable_stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
    return result;
  }

  // cache: clear whiteout?
  if (pool.info.cache_mode == pg_pool_t::CACHEMODE_WRITEBACK) {
    if (ctx->user_modify &&
	ctx->obc->obs.oi.is_whiteout()) {
      dout(10) << __func__ << " clearing whiteout on " << soid << dendl;
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    }
  }

  // clone, if necessary
  make_writeable(ctx);

  // snapset
  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  assert(ctx->new_obs.exists == ctx->new_snapset.head_exists);

  if (ctx->new_obs.exists) {
    if (!head_existed) {
      // if we logically recreated the head, remove old _snapdir object
      hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
			info.pgid.pool(), soid.get_namespace());

      ctx->snapset_obc = get_object_context(snapoid, false);
      if (ctx->snapset_obc && ctx->snapset_obc->obs.exists) {
	ctx->op_t.remove(coll, snapoid);
	dout(10) << " removing old " << snapoid << dendl;

	ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::DELETE, snapoid, ctx->at_version, old_version,
					  0, osd_reqid_t(), ctx->mtime));
 	ctx->at_version.version++;

	ctx->snapset_obc->obs.exists = false;
      }
    }
  } else if (ctx->new_snapset.clones.size()) {
    // save snapset on _snap
    hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
		      info.pgid.pool(), soid.get_namespace());
    dout(10) << " final snapset " << ctx->new_snapset
	     << " in " << snapoid << dendl;
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, snapoid, ctx->at_version, old_version,
				      0, osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc = get_object_context(snapoid, true);
    ctx->snapset_obc->obs.exists = true;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;
    ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
    ctx->snapset_obc->obs.oi.mtime = ctx->mtime;

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->snapset_obc->obs.oi, bv);
    ctx->op_t.touch(coll, snapoid);
    ctx->op_t.setattr(coll, snapoid, OI_ATTR, bv);
    ctx->op_t.setattr(coll, snapoid, SS_ATTR, bss);
    ctx->at_version.version++;
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
  ctx->bytes_written = ctx->op_t.get_encoded_bytes();
 
  if (ctx->new_obs.exists) {
    // on the head object
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = old_version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->new_obs.oi, bv);
    ctx->op_t.setattr(coll, soid, OI_ATTR, bv);

    dout(10) << " final snapset " << ctx->new_snapset
	     << " in " << soid << dendl;
    ctx->op_t.setattr(coll, soid, SS_ATTR, bss);   
  }

  // append to log
  int logopcode = pg_log_entry_t::MODIFY;
  if (!ctx->new_obs.exists)
    logopcode = pg_log_entry_t::DELETE;
  ctx->log.push_back(pg_log_entry_t(logopcode, soid, ctx->at_version, old_version,
				ctx->user_at_version, ctx->reqid, ctx->mtime));

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;
  ctx->obc->ssc->snapset = ctx->new_snapset;
  info.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);

  if (backfill_target >= 0) {
    pg_info_t& pinfo = peer_info[backfill_target];
    if (soid <= pinfo.last_backfill)
      pinfo.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
    else if (soid <= last_backfill_started)
      pending_backfill_updates[soid].stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
  }

  if (scrubber.active && scrubber.is_chunky) {
    assert(soid < scrubber.start || soid >= scrubber.end);
    if (soid < scrubber.start)
      scrub_cstat.add(ctx->delta_stats, ctx->obc->obs.oi.category);
  }

  return result;
}

// ========================================================================
// copyfrom

struct C_Copyfrom : public Context {
  ReplicatedPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  tid_t tid;
  C_Copyfrom(ReplicatedPG *p, hobject_t o, epoch_t lpr)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0)
  {}
  void finish(int r) {
    pg->lock();
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->process_copy_chunk(oid, tid, r);
    }
    pg->unlock();
  }
};

int ReplicatedPG::fill_in_copy_get(bufferlist::iterator& bp, OSDOp& osd_op,
                                   object_info_t& oi, bool classic)
{
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

  object_copy_data_t reply_obj;
  // size, mtime
  reply_obj.size = oi.size;
  reply_obj.mtime = oi.mtime;
  reply_obj.category = oi.category;

  // attrs
  map<string,bufferlist>& out_attrs = reply_obj.attrs;
  if (!cursor.attr_complete) {
    result = osd->store->getattrs(coll, soid, out_attrs, true);
    if (result < 0)
      return result;
    cursor.attr_complete = true;
    dout(20) << " got attrs" << dendl;
  }

  int64_t left = out_max - osd_op.outdata.length();

  // data
  bufferlist& bl = reply_obj.data;
  if (left > 0 && !cursor.data_complete) {
    if (cursor.data_offset < oi.size) {
      result = osd->store->read(coll, oi.soid, cursor.data_offset, left, bl);
      if (result < 0)
	return result;
      assert(result <= left);
      left -= result;
      cursor.data_offset += result;
    }
    if (cursor.data_offset == oi.size) {
      cursor.data_complete = true;
      dout(20) << " got data" << dendl;
    }
  }

  // omap
  std::map<std::string,bufferlist>& out_omap = reply_obj.omap;
  if (left > 0 && !cursor.omap_complete) {
    ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(coll, oi.soid);
    assert(iter);
    if (iter->valid()) {
      iter->upper_bound(cursor.omap_offset);
      for (; left > 0 && iter->valid(); iter->next()) {
	out_omap.insert(make_pair(iter->key(), iter->value()));
	left -= iter->key().length() + 4 + iter->value().length() + 4;
      }
    }
    if (iter->valid()) {
      cursor.omap_offset = iter->key();
    } else {
      cursor.omap_complete = true;
      dout(20) << " got omap" << dendl;
    }
  }

  dout(20) << " cursor.is_complete=" << cursor.is_complete()
		     << " " << out_attrs.size() << " attrs"
		     << " " << bl.length() << " bytes"
		     << " " << out_omap.size() << " keys"
		     << dendl;
  reply_obj.cursor = cursor;
  if (classic) {
    reply_obj.encode_classic(osd_op.outdata);
  } else {
    ::encode(reply_obj, osd_op.outdata);
  }
  result = 0;
  return result;
}

void ReplicatedPG::start_copy(CopyCallback *cb, ObjectContextRef obc,
			     hobject_t src, object_locator_t oloc, version_t version,
			     const hobject_t& temp_dest_oid)
{
  const hobject_t& dest = obc->obs.oi.soid;
  dout(10) << __func__ << " " << dest
	   << " from " << src << " " << oloc << " v" << version
	   << dendl;

  // cancel a previous in-progress copy?
  if (copy_ops.count(dest)) {
    // FIXME: if the src etc match, we could avoid restarting from the
    // beginning.
    CopyOpRef cop = copy_ops[dest];
    cancel_copy(cop, false);
  }

  CopyOpRef cop(new CopyOp(cb, obc, src, oloc, version, temp_dest_oid));
  copy_ops[dest] = cop;
  ++obc->copyfrom_readside;

  _copy_some(obc, cop);
}

void ReplicatedPG::_copy_some(ObjectContextRef obc, CopyOpRef cop)
{
  dout(10) << __func__ << " " << obc << " " << cop << dendl;
  ObjectOperation op;
  if (cop->results->user_version) {
    op.assert_version(cop->results->user_version);
  } else {
    // we should learn the version after the first chunk, if we didn't know
    // it already!
    assert(cop->cursor.is_initial());
  }
  op.copy_get(&cop->cursor, cct->_conf->osd_copyfrom_max_chunk,
	      &cop->results->object_size, &cop->results->mtime,
	      &cop->results->category,
	      &cop->attrs, &cop->data, &cop->omap,
	      &cop->rval);

  C_Copyfrom *fin = new C_Copyfrom(this, obc->obs.oi.soid,
				   get_last_peering_reset());
  osd->objecter_lock.Lock();
  tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				  cop->src.snap, NULL, 0,
				  new C_OnFinisher(fin,
						   &osd->objecter_finisher),
				  // discover the object version if we don't know it yet
				  cop->results->user_version ? NULL : &cop->results->user_version);
  fin->tid = tid;
  cop->objecter_tid = tid;
  osd->objecter_lock.Unlock();
}

void ReplicatedPG::process_copy_chunk(hobject_t oid, tid_t tid, int r)
{
  dout(10) << __func__ << " tid " << tid << " " << cpp_strerror(r) << dendl;
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
  cop->objecter_tid = 0;
  ObjectContextRef& cobc = cop->obc;

  if (r >= 0) {
    assert(cop->rval >= 0);

    if (!cop->cursor.is_complete()) {
      // write out what we have so far
      ObjectContextRef tempobc = get_object_context(cop->temp_oid, true);
      RepGather *repop = simple_repop_create(tempobc);
      if (cop->temp_cursor.is_initial()) {
	cop->temp_coll = get_temp_coll(&repop->ctx->local_t);
	repop->ctx->new_temp_oid = cop->temp_oid;
      }
      _write_copy_chunk(cop, &repop->ctx->op_t);
      simple_repop_submit(repop);

      dout(10) << __func__ << " fetching more" << dendl;
      _copy_some(cobc, cop);
      return;
    }
    _build_finish_copy_transaction(cop, cop->results->final_tx);
  }

  dout(20) << __func__ << " complete; committing" << dendl;
  CopyCallbackResults results(r, cop->results);
  cop->cb->complete(results);

  copy_ops.erase(cobc->obs.oi.soid);
  --cobc->copyfrom_readside;
  kick_object_context_blocked(cobc);
}

void ReplicatedPG::_write_copy_chunk(CopyOpRef cop, ObjectStore::Transaction *t)
{
  dout(20) << __func__ << " " << cop
	   << " " << cop->attrs.size() << " attrs"
	   << " " << cop->data.length() << " bytes"
	   << " " << cop->omap.size() << " keys"
	   << dendl;
  if (!cop->temp_cursor.attr_complete) {
    t->touch(cop->temp_coll, cop->temp_oid);
    for (map<string,bufferlist>::iterator p = cop->attrs.begin(); p != cop->attrs.end(); ++p)
      t->setattr(cop->temp_coll, cop->temp_oid, string("_") + p->first, p->second);
    cop->attrs.clear();
  }
  if (!cop->temp_cursor.data_complete) {
    t->write(cop->temp_coll, cop->temp_oid, cop->temp_cursor.data_offset, cop->data.length(), cop->data);
    cop->data.clear();
  }
  if (!cop->temp_cursor.omap_complete) {
    t->omap_setkeys(cop->temp_coll, cop->temp_oid, cop->omap);
    cop->omap.clear();
  }
  cop->temp_cursor = cop->cursor;
}

void ReplicatedPG::_build_finish_copy_transaction(CopyOpRef cop,
                                                  ObjectStore::Transaction& t)
{
  ObjectState& obs = cop->obc->obs;

  if (obs.exists) {
    t.remove(coll, obs.oi.soid);
  }

  if (cop->temp_cursor.is_initial()) {
    // write directly to final object
    cop->temp_coll = coll;
    cop->temp_oid = obs.oi.soid;
    _write_copy_chunk(cop, &t);
  } else {
    // finish writing to temp object, then move into place
    _write_copy_chunk(cop, &t);
    t.collection_move_rename(cop->temp_coll, cop->temp_oid, coll, obs.oi.soid);
    pgbackend->clear_temp_obj(cop->temp_oid);
  }
}

void ReplicatedPG::finish_copyfrom(OpContext *ctx)
{
  dout(20) << "finish_copyfrom on " << ctx->obs->oi.soid << dendl;
  ObjectState& obs = ctx->new_obs;
  CopyFromCallback *cb = static_cast<CopyFromCallback*>(ctx->copy_cb);

  if (!ctx->obs->exists) {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
  }
  if (cb->is_temp_obj_used()) {
    ctx->discard_temp_oid = cb->temp_obj;
  }
  ctx->op_t.swap(cb->results->final_tx);
  ctx->op_t.append(cb->results->final_tx);

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
}

void ReplicatedPG::finish_promote(int r, OpRequestRef op,
				  CopyResults *results, ObjectContextRef obc,
                                  hobject_t& temp_obj)
{
  const hobject_t& soid = obc->obs.oi.soid;
  dout(10) << __func__ << " " << soid << " r=" << r << dendl;

  bool whiteout = false;
  if (r == -ENOENT &&
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
    if (r != -ECANCELED) { // on cancel the client will resend
      osd->reply_op_error(op, r);
    }
    return;
  }

  RepGather *repop = simple_repop_create(obc);
  OpContext *tctx = repop->ctx;

  object_stat_sum_t delta;
  ++delta.num_objects;
  obc->obs.exists = true;
  if (whiteout) {
    // create a whiteout
    tctx->op_t.touch(coll, soid);
    obc->obs.oi.set_flag(object_info_t::FLAG_WHITEOUT);
  } else {
    tctx->op_t.swap(results->final_tx);
    if (results->started_temp_obj) {
      tctx->discard_temp_oid = temp_obj;
    }
    delta.num_bytes += results->object_size;
    obc->obs.oi.category = results->category;
    tctx->user_at_version = results->user_version;
  }
  info.stats.stats.add(delta, obc->obs.oi.category);
  tctx->at_version.epoch = get_osdmap()->get_epoch();
  tctx->at_version.version = pg_log.get_head().version + 1;

  tctx->log.push_back(pg_log_entry_t(
	  pg_log_entry_t::MODIFY,
	  soid,
	  tctx->at_version,
	  tctx->obs->oi.version,
	  tctx->user_at_version,
	  osd_reqid_t(),
	  repop->ctx->mtime));

  // set object and snapset attrs
  bufferlist bv(sizeof(tctx->new_obs.oi));
  ::encode(tctx->new_obs.oi, bv);
  tctx->op_t.setattr(coll, soid, OI_ATTR, bv);

  bufferlist bss;
  ::encode(tctx->new_snapset, bss);
  tctx->op_t.setattr(coll, soid, SS_ATTR, bss);

  repop->ondone = new C_KickBlockedObject(obc, this);
  simple_repop_submit(repop);
}

void ReplicatedPG::cancel_copy(CopyOpRef cop, bool requeue)
{
  dout(10) << __func__ << " " << cop->obc->obs.oi.soid
	   << " from " << cop->src << " " << cop->oloc
	   << " v" << cop->results->user_version << dendl;

  // cancel objecter op, if we can
  if (cop->objecter_tid) {
    Mutex::Locker l(osd->objecter_lock);
    osd->objecter->op_cancel(cop->objecter_tid);
  }

  copy_ops.erase(cop->obc->obs.oi.soid);
  --cop->obc->copyfrom_readside;

  kick_object_context_blocked(cop->obc);
  cop->results->should_requeue = requeue;
  CopyCallbackResults result(-ECANCELED, cop->results);
  cop->cb->complete(result);
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
// rep op gather

class C_OSD_OpApplied : public Context {
public:
  ReplicatedPGRef pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpApplied(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
  }
  void finish(int r) {
    pg->op_applied(repop);
  }
};

class C_OSD_OpCommit : public Context {
public:
  ReplicatedPGRef pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpCommit(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
  }
  void finish(int r) {
    pg->op_commit(repop);
  }
};

void ReplicatedPG::apply_repop(RepGather *repop)
{
  dout(10) << "apply_repop  applying update on " << *repop << dendl;
  assert(!repop->applying);
  assert(!repop->applied);

  repop->applying = true;

  repop->tls.push_back(&repop->ctx->local_t);
  repop->tls.push_back(&repop->ctx->op_t);

  repop->obc->ondisk_write_lock();
  if (repop->ctx->clone_obc)
    repop->ctx->clone_obc->ondisk_write_lock();

  bool unlock_snapset_obc = false;
  if (repop->ctx->snapset_obc && repop->ctx->snapset_obc->obs.oi.soid !=
      repop->obc->obs.oi.soid) {
    repop->ctx->snapset_obc->ondisk_write_lock();
    unlock_snapset_obc = true;
  }

  Context *oncommit = new C_OSD_OpCommit(this, repop);
  Context *onapplied = new C_OSD_OpApplied(this, repop);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(
    repop->obc,
    repop->ctx->clone_obc,
    unlock_snapset_obc ? repop->ctx->snapset_obc : ObjectContextRef());
  int r = osd->store->queue_transactions(osr.get(), repop->tls, onapplied, oncommit, onapplied_sync, repop->ctx->op);
  if (r) {
    derr << "apply_repop  queue_transactions returned " << r << " on " << *repop << dendl;
    assert(0);
  }
}

void ReplicatedPG::op_applied(RepGather *repop)
{
  lock();
  dout(10) << "op_applied " << *repop << dendl;
  if (repop->ctx->op)
    repop->ctx->op->mark_event("op_applied");
  
  repop->applying = false;
  repop->applied = true;

  // (logical) local ack.
  int whoami = osd->get_nodeid();

  if (repop->ctx->clone_obc) {
    repop->ctx->clone_obc = ObjectContextRef();
  }
  if (repop->ctx->snapset_obc) {
    repop->ctx->snapset_obc = ObjectContextRef();
  }

  repop->src_obc.clear();

  if (!repop->aborted) {
    assert(repop->waitfor_ack.count(whoami) ||
	   repop->waitfor_disk.count(whoami) == 0);  // commit before ondisk
    repop->waitfor_ack.erase(whoami);

    if (repop->v != eversion_t()) {
      assert(info.last_update >= repop->v);
      assert(last_update_applied < repop->v);
      last_update_applied = repop->v;
    }

    // chunky scrub
    if (scrubber.active && scrubber.is_chunky) {
      if (last_update_applied == scrubber.subset_last_update) {
        osd->scrub_wq.queue(this);
      }

    // classic scrub
    } else if (last_update_applied == info.last_update && scrubber.block_writes) {
      dout(10) << "requeueing scrub for cleanup" << dendl;
      scrubber.finalizing = true;
      scrub_gather_replica_maps();
      ++scrubber.waiting_on;
      scrubber.waiting_on_whom.insert(osd->whoami);
      osd->scrub_wq.queue(this);
    }
  }

  if (!repop->aborted)
    eval_repop(repop);

  repop->put();
  unlock();
}

void ReplicatedPG::op_commit(RepGather *repop)
{
  lock();
  if (repop->ctx->op)
    repop->ctx->op->mark_event("op_commit");

  if (repop->aborted) {
    dout(10) << "op_commit " << *repop << " -- aborted" << dendl;
  } else if (repop->waitfor_disk.count(osd->get_nodeid()) == 0) {
    dout(10) << "op_commit " << *repop << " -- already marked ondisk" << dendl;
  } else {
    dout(10) << "op_commit " << *repop << dendl;
    int whoami = osd->get_nodeid();

    repop->waitfor_disk.erase(whoami);

    // remove from ack waitfor list too.  sub_op_modify_commit()
    // behaves the same in that the COMMIT implies and ACK and there
    // is no separate reply sent.
    repop->waitfor_ack.erase(whoami);
    
    if (repop->v != eversion_t()) {
      last_update_ondisk = repop->v;
      last_complete_ondisk = repop->pg_local_last_complete;
    }
    eval_repop(repop);
  }

  repop->put();
  unlock();
}



void ReplicatedPG::eval_repop(RepGather *repop)
{
  MOSDOp *m = NULL;
  if (repop->ctx->op)
    m = static_cast<MOSDOp *>(repop->ctx->op->get_req());

  if (m)
    dout(10) << "eval_repop " << *repop
	     << " wants=" << (m->wants_ack() ? "a":"") << (m->wants_ondisk() ? "d":"")
	     << (repop->done() ? " DONE" : "")
	     << dendl;
  else
    dout(10) << "eval_repop " << *repop << " (no op)"
	     << (repop->done() ? " DONE" : "")
	     << dendl;

  if (repop->done())
    return;

  // apply?
  if (!repop->applied && !repop->applying)
    apply_repop(repop);
  
  if (m) {

    // an 'ondisk' reply implies 'ack'. so, prefer to send just one
    // ondisk instead of ack followed by ondisk.

    // ondisk?
    if (repop->waitfor_disk.empty()) {

      release_op_ctx_locks(repop->ctx);

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
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
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
    if (repop->waitfor_ack.empty()) {

      // send dup acks, in order
      if (waiting_for_ack.count(repop->v)) {
	assert(waiting_for_ack.begin()->first == repop->v);
	for (list<OpRequestRef>::iterator i = waiting_for_ack[repop->v].begin();
	     i != waiting_for_ack[repop->v].end();
	     ++i) {
	  MOSDOp *m = (MOSDOp*)(*i)->get_req();
	  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
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
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
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
  if (repop->waitfor_ack.empty() && repop->waitfor_disk.empty() &&
      repop->applied) {
    repop->mark_done();

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

  dout(7) << "issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << dendl;

  repop->v = ctx->at_version;

  // add myself to gather set
  repop->waitfor_ack.insert(acting[0]);
  repop->waitfor_disk.insert(acting[0]);

  int acks_wanted = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

  if (ctx->op && acting.size() > 1) {
    ostringstream ss;
    ss << "waiting for subops from " << vector<int>(acting.begin() + 1, acting.end());
    ctx->op->mark_sub_op_sent(ss.str());
  }
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    pg_info_t &pinfo = peer_info[peer];

    repop->waitfor_ack.insert(peer);
    repop->waitfor_disk.insert(peer);

    // forward the write/update/whatever
    MOSDSubOp *wr = new MOSDSubOp(repop->ctx->reqid, info.pgid, soid,
				  false, acks_wanted,
				  get_osdmap()->get_epoch(),
				  repop->rep_tid, repop->ctx->at_version);
    if (ctx->op &&
	((static_cast<MOSDOp *>(ctx->op->get_req()))->get_flags() & CEPH_OSD_FLAG_PARALLELEXEC)) {
      // replicate original op for parallel execution on replica
      assert(0 == "broken implementation, do not use");
    }

    // ship resulting transaction, log entries, and pg_stats
    if (peer == backfill_target && soid > last_backfill_started &&
        // only skip normal (not temp pool=-1) objects
	soid.pool == (int64_t)info.pgid.pool()) {
      dout(10) << "issue_repop shipping empty opt to osd." << peer
	       <<", object beyond last_backfill_started"
	       << last_backfill_started << ", last_backfill is "
	       << pinfo.last_backfill << dendl;
      ObjectStore::Transaction t;
      ::encode(t, wr->get_data());
    } else {
      ::encode(repop->ctx->op_t, wr->get_data());
    }

    ::encode(repop->ctx->log, wr->logbl);

    if (backfill_target >= 0 && backfill_target == peer)
      wr->pg_stats = pinfo.stats;  // reflects backfill progress
    else
      wr->pg_stats = info.stats;
    
    wr->pg_trim_to = pg_trim_to;

    wr->new_temp_oid = repop->ctx->new_temp_oid;
    wr->discard_temp_oid = repop->ctx->discard_temp_oid;

    osd->send_message_osd_cluster(peer, wr, get_osdmap()->get_epoch());

    // keep peer_info up to date
    if (pinfo.last_complete == pinfo.last_update)
      pinfo.last_update = ctx->at_version;
    pinfo.last_update = ctx->at_version;
  }
}

ReplicatedPG::RepGather *ReplicatedPG::new_repop(OpContext *ctx, ObjectContextRef obc,
						 tid_t rep_tid)
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
  release_op_ctx_locks(repop->ctx);
  repop_map.erase(repop->rep_tid);
  repop->put();

  osd->logger->set(l_osd_op_wip, repop_map.size());
}

void ReplicatedPG::repop_ack(RepGather *repop, int result, int ack_type,
			     int fromosd, eversion_t peer_lcod)
{
  MOSDOp *m = NULL;

  if (repop->ctx->op)
    m = static_cast<MOSDOp *>(repop->ctx->op->get_req());

  if (m)
    dout(7) << "repop_ack rep_tid " << repop->rep_tid << " op " << *m
	    << " result " << result
	    << " ack_type " << ack_type
	    << " from osd." << fromosd
	    << dendl;
  else
    dout(7) << "repop_ack rep_tid " << repop->rep_tid << " (no op) "
	    << " result " << result
	    << " ack_type " << ack_type
	    << " from osd." << fromosd
	    << dendl;
  
  if (ack_type & CEPH_OSD_FLAG_ONDISK) {
    if (repop->ctx->op)
      repop->ctx->op->mark_event("sub_op_commit_rec");
    // disk
    if (repop->waitfor_disk.count(fromosd)) {
      repop->waitfor_disk.erase(fromosd);
      //repop->waitfor_nvram.erase(fromosd);
      repop->waitfor_ack.erase(fromosd);
      peer_last_complete_ondisk[fromosd] = peer_lcod;
    }
/*} else if (ack_type & CEPH_OSD_FLAG_ONNVRAM) {
    // nvram
    repop->waitfor_nvram.erase(fromosd);
    repop->waitfor_ack.erase(fromosd);*/
  } else {
    // ack
    if (repop->ctx->op)
      repop->ctx->op->mark_event("sub_op_applied_rec");
    repop->waitfor_ack.erase(fromosd);
  }

  if (!repop->aborted)
    eval_repop(repop);
}


ReplicatedPG::RepGather *ReplicatedPG::simple_repop_create(ObjectContextRef obc)
{
  dout(20) << __func__ << " " << obc->obs.oi.soid << dendl;
  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, obc->ssc, this);
  ctx->mtime = ceph_clock_now(g_ceph_context);
  RepGather *repop = new_repop(ctx, obc, rep_tid);
  return repop;
}

void ReplicatedPG::simple_repop_submit(RepGather *repop)
{
  dout(20) << __func__ << " " << repop << dendl;
  if (!repop->ctx->log.empty())
    append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);
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
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, obc->ssc, this);
  ctx->mtime = ceph_clock_now(cct);
  ctx->at_version = get_next_version();

  entity_inst_t nobody;

  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;

  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, obc->obs.oi.soid,
				    ctx->at_version,
				    obc->obs.oi.version,
				    0,
				    osd_reqid_t(), ctx->mtime));

  obc->obs.oi.prior_version = repop->obc->obs.oi.version;
  obc->obs.oi.version = ctx->at_version;
  bufferlist bl;
  ::encode(obc->obs.oi, bl);
  t->setattr(coll, obc->obs.oi.soid, OI_ATTR, bl);

  append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);

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
						  map<string, bufferptr> *attrs)
{
  assert(
    attrs || !pg_log.get_missing().is_missing(soid) ||
    // or this is a revert... see recover_primary()
    (pg_log.get_log().objects.count(soid) &&
      pg_log.get_log().objects.find(soid)->second->op ==
      pg_log_entry_t::LOST_REVERT));
  ObjectContextRef obc = object_contexts.lookup(soid);
  if (obc) {
    dout(10) << "get_object_context " << obc << " " << soid << dendl;
  } else {
    // check disk
    bufferlist bv;
    if (attrs) {
      assert(attrs->count(OI_ATTR));
      bv.push_back(attrs->find(OI_ATTR)->second);
    } else {
      int r = pgbackend->objects_get_attr(soid, OI_ATTR, &bv);
      if (r < 0) {
	if (!can_create)
	  return ObjectContextRef();   // -ENOENT!

	// new object.
	object_info_t oi(soid);
	SnapSetContext *ssc = get_snapset_context(
	  soid.oid, soid.get_key(), soid.hash, true, soid.get_namespace(),
	  soid.has_snapset() ? attrs : 0);
	return create_object_context(oi, ssc);
      }
    }

    object_info_t oi(bv);

    assert(oi.soid.pool == (int64_t)info.pgid.pool());

    obc = object_contexts.lookup_or_create(oi.soid);
    obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
    obc->obs.oi = oi;
    obc->obs.exists = true;

    obc->ssc = get_snapset_context(
      soid.oid, soid.get_key(), soid.hash,
      true, soid.get_namespace(),
      soid.has_snapset() ? attrs : 0);
    register_snapset_context(obc->ssc);

    populate_obc_watchers(obc);
    dout(10) << "get_object_context " << obc << " " << soid << " 0 -> 1 read " << obc->obs.oi << dendl;
  }
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


int ReplicatedPG::find_object_context(const hobject_t& oid,
				      ObjectContextRef *pobc,
				      bool can_create,
				      snapid_t *psnapid)
{
  hobject_t head(oid.oid, oid.get_key(), CEPH_NOSNAP, oid.hash,
		 info.pgid.pool(), oid.get_namespace());
  hobject_t snapdir(oid.oid, oid.get_key(), CEPH_SNAPDIR, oid.hash,
		    info.pgid.pool(), oid.get_namespace());

  // want the snapdir?
  if (oid.snap == CEPH_SNAPDIR) {
    // return head or snapdir, whichever exists.
    ObjectContextRef obc = get_object_context(head, can_create);
    if (obc && !obc->obs.exists) {
      // ignore it if the obc exists but the object doesn't
      obc = ObjectContextRef();
    }
    if (!obc) {
      obc = get_object_context(snapdir, can_create);
    }
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << oid.snap << dendl;
    *pobc = obc;

    // always populate ssc for SNAPDIR...
    if (!obc->ssc)
      obc->ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, true, oid.get_namespace());
    return 0;
  }

  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContextRef obc = get_object_context(head, can_create);
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << oid.snap << dendl;
    *pobc = obc;

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, true, oid.get_namespace());

    return 0;
  }

  // we want a snap
  SnapSetContext *ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, can_create, oid.get_namespace());
  if (!ssc)
    return -ENOENT;

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
    dout(10) << "find_object_context  no clones with last >= oid.snap " << oid.snap << " -- DNE" << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }
  hobject_t soid(oid.oid, oid.get_key(), ssc->snapset.clones[k], oid.hash,
		 info.pgid.pool(), oid.get_namespace());

  put_snapset_context(ssc); // we're done with ssc
  ssc = 0;

  if (pg_log.get_missing().is_missing(soid)) {
    dout(20) << "find_object_context  " << soid << " missing, try again later" << dendl;
    if (psnapid)
      *psnapid = soid.snap;
    return -EAGAIN;
  }

  ObjectContextRef obc = get_object_context(soid, false);
  assert(obc);

  // clone
  dout(20) << "find_object_context  " << soid << " snaps " << obc->obs.oi.snaps << dendl;
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

  if (oi.soid.snap && oi.soid.snap != CEPH_NOSNAP && oi.soid.snap != CEPH_SNAPDIR) {
    stat.num_object_clones++;

    if (!obc->ssc)
      obc->ssc = get_snapset_context(oi.soid.oid,
				     oi.soid.get_key(),
				     oi.soid.hash,
				     false,
				     oi.soid.get_namespace());
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
}

SnapSetContext *ReplicatedPG::create_snapset_context(const object_t& oid)
{
  Mutex::Locker l(snapset_contexts_lock);
  SnapSetContext *ssc = new SnapSetContext(oid);
  _register_snapset_context(ssc);
  ssc->ref++;
  return ssc;
}

SnapSetContext *ReplicatedPG::get_snapset_context(
  const object_t& oid,
  const string& key,
  ps_t seed,
  bool can_create,
  const string& nspace,
  map<string, bufferptr> *attrs)
{
  Mutex::Locker l(snapset_contexts_lock);
  SnapSetContext *ssc;
  map<object_t, SnapSetContext*>::iterator p = snapset_contexts.find(oid);
  if (p != snapset_contexts.end()) {
    ssc = p->second;
  } else {
    bufferlist bv;
    if (!attrs) {
      hobject_t head(oid, key, CEPH_NOSNAP, seed,
		     info.pgid.pool(), nspace);
      int r = pgbackend->objects_get_attr(head, SS_ATTR, &bv);
      if (r < 0) {
	// try _snapset
	hobject_t snapdir(oid, key, CEPH_SNAPDIR, seed,
			  info.pgid.pool(), nspace);
	r = pgbackend->objects_get_attr(snapdir, SS_ATTR, &bv);
	if (r < 0 && !can_create)
	  return NULL;
      }
    } else {
      assert(attrs->count(SS_ATTR));
      bv.push_back(attrs->find(SS_ATTR)->second);
    }
    ssc = new SnapSetContext(oid);
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

void ReplicatedPG::sub_op_modify(OpRequestRef op)
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
  assert(m->map_epoch >= info.history.same_interval_since);
  assert(is_active());
  assert(is_replica());
  
  // we better not be missing this.
  assert(!pg_log.get_missing().is_missing(soid));

  int ackerosd = acting[0];
  
  op->mark_started();

  RepModify *rm = new RepModify;
  rm->pg = this;
  get("RepModify");
  rm->op = op;
  rm->ctx = 0;
  rm->ackerosd = ackerosd;
  rm->last_complete = info.last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  if (!m->noop) {
    assert(m->logbl.length());
    // shipped transaction and log entries
    vector<pg_log_entry_t> log;

    bufferlist::iterator p = m->get_data().begin();

    if (m->new_temp_oid != hobject_t()) {
      dout(20) << __func__ << " start tracking temp " << m->new_temp_oid << dendl;
      pgbackend->add_temp_obj(m->new_temp_oid);
      get_temp_coll(&rm->localt);
    }
    if (m->discard_temp_oid != hobject_t()) {
      dout(20) << __func__ << " stop tracking temp " << m->discard_temp_oid << dendl;
      pgbackend->clear_temp_obj(m->discard_temp_oid);
    }

    ::decode(rm->opt, p);
    if (!(m->get_connection()->get_features() & CEPH_FEATURE_OSD_SNAPMAPPER))
      rm->opt.set_tolerate_collection_add_enoent();
    p = m->logbl.begin();
    ::decode(log, p);
    if (m->hobject_incorrect_pool) {
      for (vector<pg_log_entry_t>::iterator i = log.begin();
	  i != log.end();
	  ++i) {
	if (i->soid.pool == -1)
	  i->soid.pool = info.pgid.pool();
      }
      rm->opt.set_pool_override(info.pgid.pool());
    }
    rm->opt.set_replica();

    info.stats = m->pg_stats;
    if (!rm->opt.empty()) {
      // If the opt is non-empty, we infer we are before
      // last_backfill (according to the primary, not our
      // not-quite-accurate value), and should update the
      // collections now.  Otherwise, we do it later on push.
      update_snap_map(log, rm->localt);
    }
    append_log(log, m->pg_trim_to, rm->localt);

    rm->tls.push_back(&rm->localt);
    rm->tls.push_back(&rm->opt);
    
    rm->bytes_written = rm->opt.get_encoded_bytes();

  } else {
    // just trim the log
    if (m->pg_trim_to != eversion_t()) {
      pg_log.trim(m->pg_trim_to, info);
      dirty_info = true;
      write_if_dirty(rm->localt);
      rm->tls.push_back(&rm->localt);
    }
  }
  
  op->mark_started();

  Context *oncommit = new C_OSD_RepModifyCommit(rm);
  Context *onapply = new C_OSD_RepModifyApply(rm);
  int r = osd->store->queue_transactions(osr.get(), rm->tls, onapply, oncommit, 0, op);
  if (r) {
    dout(0) << "error applying transaction: r = " << r << dendl;
    assert(0);
  }
  // op is cleaned up by oncommit/onapply when both are executed
}

void ReplicatedPG::sub_op_modify_applied(RepModify *rm)
{
  lock();
  rm->op->mark_event("sub_op_applied");
  rm->applied = true;

  if (!pg_has_reset_since(rm->epoch_started)) {
    dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op->get_req() << dendl;
    MOSDSubOp *m = static_cast<MOSDSubOp*>(rm->op->get_req());
    assert(m->get_header().type == MSG_OSD_SUBOP);
    
    if (!rm->committed) {
      // send ack to acker only if we haven't sent a commit already
      MOSDSubOpReply *ack = new MOSDSubOpReply(m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
      ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
      osd->send_message_osd_cluster(rm->ackerosd, ack, get_osdmap()->get_epoch());
    }
    
    if (m->version != eversion_t()) {
      assert(info.last_update >= m->version);
      assert(last_update_applied < m->version);
      last_update_applied = m->version;
    }
    if (scrubber.active_rep_scrub) {
      if (last_update_applied == scrubber.active_rep_scrub->scrub_to) {
	osd->rep_scrub_wq.queue(scrubber.active_rep_scrub);
	scrubber.active_rep_scrub = 0;
      }
    }
  } else {
    dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op->get_req()
	     << " from epoch " << rm->epoch_started << " < last_peering_reset "
	     << last_peering_reset << dendl;
  }

  bool done = rm->applied && rm->committed;
  unlock();
  if (done) {
    delete rm->ctx;
    delete rm;
    put("RepModify");
  }
}

void ReplicatedPG::sub_op_modify_commit(RepModify *rm)
{
  lock();
  rm->op->mark_commit_sent();
  rm->committed = true;

  if (!pg_has_reset_since(rm->epoch_started)) {
    // send commit.
    dout(10) << "sub_op_modify_commit on op " << *rm->op->get_req()
	     << ", sending commit to osd." << rm->ackerosd
	     << dendl;
    
    if (get_osdmap()->is_up(rm->ackerosd)) {
      last_complete_ondisk = rm->last_complete;
      MOSDSubOpReply *commit = new MOSDSubOpReply(static_cast<MOSDSubOp*>(rm->op->get_req()), 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
      commit->set_last_complete_ondisk(rm->last_complete);
      commit->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
      osd->send_message_osd_cluster(rm->ackerosd, commit, get_osdmap()->get_epoch());
    }
  } else {
    dout(10) << "sub_op_modify_commit " << rm << " op " << *rm->op->get_req()
	     << " from epoch " << rm->epoch_started << " < last_peering_reset "
	     << last_peering_reset << dendl;
  }
  
  log_subop_stats(osd, rm->op, l_osd_sop_w_inb, l_osd_sop_w_lat);
  bool done = rm->applied && rm->committed;
  unlock();
  if (done) {
    delete rm->ctx;
    delete rm;
    put("RepModify");
  }
}

void ReplicatedPG::sub_op_modify_reply(OpRequestRef op)
{
  MOSDSubOpReply *r = static_cast<MOSDSubOpReply*>(op->get_req());
  assert(r->get_header().type == MSG_OSD_SUBOPREPLY);

  op->mark_started();

  // must be replication.
  tid_t rep_tid = r->get_tid();
  int fromosd = r->get_source().num();
  
  if (repop_map.count(rep_tid)) {
    // oh, good.
    repop_ack(repop_map[rep_tid], 
	      r->get_result(), r->ack_type,
	      fromosd, 
	      r->get_last_complete_ondisk());
  }
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
  const hobject_t& soid,
  ObjectContextRef headctx,
  RPGHandle *h)
{
  assert(get_parent()->get_local_missing().missing.count(soid));
  eversion_t v = get_parent()->get_local_missing().missing.find(
    soid)->second.need;
  const map<hobject_t, set<int> > &missing_loc(
    get_parent()->get_missing_loc());
  const map<int, pg_missing_t > &peer_missing(
    get_parent()->get_peer_missing());
  int fromosd = -1;
  map<hobject_t,set<int> >::const_iterator q = missing_loc.find(soid);
  assert(q != missing_loc.end());
  assert(!q->second.empty());

  // pick a pullee
  vector<int> shuffle(q->second.begin(), q->second.end());
  random_shuffle(shuffle.begin(), shuffle.end());
  vector<int>::iterator p = shuffle.begin();
  assert(get_osdmap()->is_up(*p));
  fromosd = *p;
  assert(fromosd >= 0);

  dout(7) << "pull " << soid
	  << "v " << v
	  << " on osds " << *p
	  << " from osd." << fromosd
	  << dendl;

  assert(peer_missing.count(fromosd));
  const pg_missing_t &pmissing = peer_missing.find(fromosd)->second;
  if (pmissing.is_missing(soid, v)) {
    assert(pmissing.missing.find(soid)->second.have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromosd
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

  h->pulls[fromosd].push_back(PullOp());
  PullOp &op = h->pulls[fromosd].back();
  op.soid = soid;

  op.recovery_info = recovery_info;
  op.recovery_info.soid = soid;
  op.recovery_info.version = v;
  op.recovery_progress.data_complete = false;
  op.recovery_progress.omap_complete = false;
  op.recovery_progress.data_recovered_to = 0;
  op.recovery_progress.first = true;

  assert(!pulling.count(soid));
  pull_from_peer[fromosd].insert(soid);
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
  map<hobject_t,set<int> >::iterator q = missing_loc.find(soid);
  if (q == missing_loc.end()) {
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
    head_obc,
    obc,
    h);
  return PULL_YES;
}

void ReplicatedPG::send_remove_op(const hobject_t& oid, eversion_t v, int peer)
{
  tid_t tid = osd->get_tid();
  osd_reqid_t rid(osd->get_cluster_msgr_name(), 0, tid);

  dout(10) << "send_remove_op " << oid << " from osd." << peer
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, oid, false, CEPH_OSD_FLAG_ACK,
				   get_osdmap()->get_epoch(), tid, v);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_DELETE;

  osd->send_message_osd_cluster(peer, subop, get_osdmap()->get_epoch());
}

/*
 * intelligently push an object to a replica.  make use of existing
 * clones/heads and dup data ranges where possible.
 */
void ReplicatedBackend::prep_push_to_replica(
  ObjectContextRef obc, const hobject_t& soid, int peer,
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
    map<int, pg_missing_t>::const_iterator pm =
      get_parent()->get_peer_missing().find(peer);
    assert(pm != get_parent()->get_peer_missing().end());
    map<int, pg_info_t>::const_iterator pi =
      get_parent()->get_peer_info().find(peer);
    assert(pi != get_parent()->get_peer_info().end());
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
      ssc->snapset, soid, get_parent()->get_peer_missing().find(peer)->second,
      get_parent()->get_peer_info().find(peer)->second.last_backfill,
      data_subset, clone_subsets);
  }

  prep_push(obc, soid, peer, oi.version, data_subset, clone_subsets, pop);
}

void ReplicatedBackend::prep_push(ObjectContextRef obc,
			     const hobject_t& soid, int peer,
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
  const hobject_t& soid, int peer,
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

int ReplicatedBackend::send_pull_legacy(int prio, int peer,
					const ObjectRecoveryInfo &recovery_info,
					ObjectRecoveryProgress progress)
{
  // send op
  tid_t tid = osd->get_tid();
  osd_reqid_t rid(osd->get_cluster_msgr_name(), 0, tid);

  dout(10) << "send_pull_op " << recovery_info.soid << " "
	   << recovery_info.version
	   << " first=" << progress.first
	   << " data " << recovery_info.copy_subset
	   << " from osd." << peer
	   << " tid " << tid << dendl;

  MOSDSubOp *subop = new MOSDSubOp(rid, get_info().pgid, recovery_info.soid,
				   false, CEPH_OSD_FLAG_ACK,
				   get_osdmap()->get_epoch(), tid,
				   recovery_info.version);
  subop->set_priority(prio);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->ops[0].op.extent.length = cct->_conf->osd_recovery_max_chunk;
  subop->recovery_info = recovery_info;
  subop->recovery_progress = progress;

  osd->send_message_osd_cluster(peer, subop, get_osdmap()->get_epoch());

  osd->logger->inc(l_osd_pull);
  return 0;
}

void ReplicatedBackend::submit_push_data(
  ObjectRecoveryInfo &recovery_info,
  bool first,
  bool complete,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  map<string, bufferptr> &attrs,
  map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  coll_t target_coll;
  if (first && complete) {
    target_coll = coll;
  } else {
    dout(10) << __func__ << ": Creating oid "
	     << recovery_info.soid << " in the temp collection" << dendl;
    temp_contents.insert(recovery_info.soid);
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
      assert(temp_contents.count(recovery_info.soid));
      dout(10) << __func__ << ": Removing oid "
	       << recovery_info.soid << " from the temp collection" << dendl;
      temp_contents.erase(recovery_info.soid);
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
  int from, PushOp &pop, PullOp *response,
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
    log_subop_stats(pg->osd, op, l_osd_push_inb, l_osd_sop_push_lat);
  }
};

void ReplicatedBackend::handle_push(
  int from, PushOp &pop, PushReplyOp *response,
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

void ReplicatedBackend::send_pushes(int prio, map<int, vector<PushOp> > &pushes)
{
  for (map<int, vector<PushOp> >::iterator i = pushes.begin();
       i != pushes.end();
       ++i) {
    ConnectionRef con = osd->get_con_osd_cluster(
      i->first,
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
	msg->pgid = get_info().pgid;
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
	osd->send_message_osd_cluster(msg, con);
      }
    }
  }
}

void ReplicatedBackend::send_pulls(int prio, map<int, vector<PullOp> > &pulls)
{
  for (map<int, vector<PullOp> >::iterator i = pulls.begin();
       i != pulls.end();
       ++i) {
    ConnectionRef con = osd->get_con_osd_cluster(
      i->first,
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
      msg->set_priority(prio);
      msg->pgid = get_info().pgid;
      msg->map_epoch = get_osdmap()->get_epoch();
      msg->pulls.swap(i->second);
      msg->compute_cost(cct);
      osd->send_message_osd_cluster(msg, con);
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
    osd->store->omap_get_header(coll, recovery_info.soid, &out_op->omap_header);
    osd->store->getattrs(coll, recovery_info.soid, out_op->attrset);

    // Debug
    bufferlist bv;
    bv.push_back(out_op->attrset[OI_ATTR]);
    object_info_t oi(bv);

    if (oi.version != recovery_info.version) {
      osd->clog.error() << get_info().pgid << " push "
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
      osd->store->get_omap_iterator(coll,
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
    osd->store->read(coll, recovery_info.soid,
		     p.get_start(), p.get_len(), bit);
    if (p.get_len() != bit.length()) {
      dout(10) << " extent " << p.get_start() << "~" << p.get_len()
	       << " is actually " << p.get_start() << "~" << bit.length()
	       << dendl;
      p.set_len(bit.length());
      new_progress.data_complete = true;
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

  osd->logger->inc(l_osd_push);
  osd->logger->inc(l_osd_push_outb, out_op->data.length());
  
  // send
  out_op->version = recovery_info.version;
  out_op->soid = recovery_info.soid;
  out_op->recovery_info = recovery_info;
  out_op->after_progress = new_progress;
  out_op->before_progress = progress;
  return 0;
}

int ReplicatedBackend::send_push_op_legacy(int prio, int peer, PushOp &pop)
{
  tid_t tid = osd->get_tid();
  osd_reqid_t rid(osd->get_cluster_msgr_name(), 0, tid);
  MOSDSubOp *subop = new MOSDSubOp(rid, get_info().pgid, pop.soid,
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

  osd->send_message_osd_cluster(peer, subop, get_osdmap()->get_epoch());
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
  int peer = reply->get_source().num();

  op->mark_started();
  
  PushReplyOp rop;
  rop.soid = soid;
  PushOp pop;
  bool more = handle_push_reply(peer, rop, &pop);
  if (more)
    send_push_op_legacy(op->get_req()->get_priority(), peer, pop);
}

bool ReplicatedBackend::handle_push_reply(int peer, PushReplyOp &op, PushOp *reply)
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
  handle_pull(m->get_source().num(), pop, &reply);
  send_push_op_legacy(
    m->get_priority(),
    m->get_source().num(),
    reply);

  log_subop_stats(osd, op, 0, l_osd_sop_pull_lat);
}

void ReplicatedBackend::handle_pull(int peer, PullOp &op, PushOp *reply)
{
  const hobject_t &soid = op.soid;
  struct stat st;
  int r = osd->store->stat(coll, soid, &st);
  if (r != 0) {
    osd->clog.error() << get_info().pgid << " "
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
      if (is_replica()) {
	// we are fully up to date.  tell the primary!
	osd->send_message_osd_cluster(get_primary(),
				      new MOSDPGTrim(get_osdmap()->get_epoch(), info.pgid,
						     last_complete_ondisk),
				      get_osdmap()->get_epoch());
      } else if (is_primary()) {
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
  if (pg_log.get_missing().is_missing(oid, v)) {
      if (is_primary())
	missing_loc.erase(oid);
  }
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
      m->get_source().num(), pop, &resp,
      &to_continue, t);
    if (more) {
      send_pull_legacy(
	m->get_priority(),
	m->get_source().num(),
	resp.recovery_info,
	resp.recovery_progress);
    } else {
      C_ReplicatedBackend_OnPullComplete *c =
	new C_ReplicatedBackend_OnPullComplete(
	  this,
	  op->get_req()->get_priority());
      c->to_continue.swap(to_continue);
      t->register_on_complete(
	new C_QueueInWQ(
	  &osd->push_wq,
	  get_parent()->bless_gencontext(c)));
    }
    run_recovery_op(h, op->get_req()->get_priority());
  } else {
    PushReplyOp resp;
    MOSDSubOpReply *reply = new MOSDSubOpReply(
      m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
    reply->set_priority(m->get_priority());
    assert(entity_name_t::TYPE_OSD == m->get_connection()->peer_type);
    handle_push(m->get_source().num(), pop, &resp, t);
    t->register_on_complete(new C_OSD_SendMessageOnConn(
			      osd, reply, m->get_connection()));
  }
  get_parent()->queue_transaction(t);
  return;
}

void ReplicatedPG::failed_push(int from, const hobject_t &soid)
{
  assert(recovering.count(soid));
  recovering.erase(soid);
  map<hobject_t,set<int> >::iterator p = missing_loc.find(soid);
  if (p != missing_loc.end()) {
    dout(0) << "_failed_push " << soid << " from osd." << from
	    << ", reps on " << p->second << dendl;

    p->second.erase(from);          // forget about this (bad) peer replica
    if (p->second.empty())
      missing_loc.erase(p);
  } else {
    dout(0) << "_failed_push " << soid << " from osd." << from
	    << " but not in missing_loc ???" << dendl;
  }
  finish_recovery_op(soid);  // close out this attempt,
}

void ReplicatedBackend::_failed_push(int from, const hobject_t &soid)
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
  int r = osd->store->queue_transaction(osr.get(), t);
  assert(r == 0);
}


eversion_t ReplicatedPG::pick_newest_available(const hobject_t& oid)
{
  eversion_t v;

  assert(pg_log.get_missing().is_missing(oid));
  v = pg_log.get_missing().missing.find(oid)->second.have;
  dout(10) << "pick_newest_available " << oid << " " << v << " on osd." << osd->whoami << " (local)" << dendl;

  for (unsigned i=1; i<acting.size(); ++i) {
    int peer = acting[i];
    if (!peer_missing[peer].is_missing(oid)) {
      assert(peer == backfill_target);
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
    waiting_for_missing_object.find(oid);
  if (wmo != waiting_for_missing_object.end()) {
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
  map<hobject_t, pg_missing_t::item>::const_iterator m = missing.missing.begin();
  map<hobject_t, pg_missing_t::item>::const_iterator mend = missing.missing.end();
  while (m != mend) {
    const hobject_t &oid(m->first);
    if (missing_loc.find(oid) != missing_loc.end()) {
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

	// delete local copy?  NOT YET!  FIXME
	if (m->second.have != eversion_t()) {
	  assert(0 == "not implemented.. tho i'm not sure how useful it really would be.");
	}
	pg_log.missing_rm(m++);
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
    dout(10) << " applying repop tid " << repop->rep_tid << dendl;
    if (!repop->applied && !repop->applying)
      apply_repop(repop);
    repop->aborted = true;

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
    assert(waiting_for_ondisk.empty());
  }

  waiting_for_ondisk.clear();
  waiting_for_ack.clear();
}

void ReplicatedPG::on_flushed()
{
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    derr << "on_flushed: object " << i.first << " obc still alive" << dendl;
  }
  assert(object_contexts.empty());
  pgbackend->on_flushed();
}

void ReplicatedPG::on_removal(ObjectStore::Transaction *t)
{
  dout(10) << "on_removal" << dendl;

  // adjust info to backfill
  info.last_backfill = hobject_t();
  dirty_info = true;
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
  apply_and_flush_repops(false);
  context_registry_on_change();

  osd->remote_reserver.cancel_reservation(info.pgid);
  osd->local_reserver.cancel_reservation(info.pgid);

  clear_primary_state();
  osd->remove_want_pg_temp(info.pgid);
  cancel_recovery();
}

void ReplicatedPG::on_activate()
{
  for (unsigned i = 1; i<acting.size(); i++) {
    if (peer_info[acting[i]].last_backfill != hobject_t::get_max()) {
      assert(backfill_target == -1);
      backfill_target = acting[i];
      last_backfill_started = peer_info[acting[i]].last_backfill;
      dout(10) << " chose backfill target osd." << backfill_target
	       << " from " << last_backfill_started << dendl;
    }
  }

  hit_set_setup();
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

  cancel_copy_ops(is_primary());

  // requeue object waiters
  if (is_primary()) {
    requeue_object_waiters(waiting_for_missing_object);
  } else {
    waiting_for_missing_object.clear();
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

  if (is_primary())
    requeue_ops(waiting_for_all_missing);
  else
    waiting_for_all_missing.clear();

  // this will requeue ops we were working on but didn't finish, and
  // any dups
  apply_and_flush_repops(is_primary());

  pgbackend->on_change(t);

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
  hit_set_setup();
}

// clear state.  called on recovery completion AND cancellation.
void ReplicatedPG::_clear_recovery_state()
{
  missing_loc.clear();
  missing_loc_sources.clear();
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
  assert(recovering.count(soid));
  recovering.erase(soid);
  finish_recovery_op(soid);
  pg_log.set_last_requested(0); // get recover_primary to start over
}

void ReplicatedPG::check_recovery_sources(const OSDMapRef osdmap)
{
  /*
   * check that any peers we are planning to (or currently) pulling
   * objects from are dealt with.
   */
  set<int> now_down;
  for (set<int>::iterator p = missing_loc_sources.begin();
       p != missing_loc_sources.end();
       ) {
    if (osdmap->is_up(*p)) {
      ++p;
      continue;
    }
    dout(10) << "check_recovery_sources source osd." << *p << " now down" << dendl;
    now_down.insert(*p);
    missing_loc_sources.erase(p++);
  }
  pgbackend->check_recovery_sources(osdmap);

  if (now_down.empty()) {
    dout(10) << "check_recovery_sources no source osds (" << missing_loc_sources << ") went down" << dendl;
  } else {
    dout(10) << "check_recovery_sources sources osds " << now_down << " now down, remaining sources are "
	     << missing_loc_sources << dendl;
    
    // filter missing_loc
    map<hobject_t, set<int> >::iterator p = missing_loc.begin();
    while (p != missing_loc.end()) {
      set<int>::iterator q = p->second.begin();
      while (q != p->second.end())
	if (now_down.count(*q)) {
	  p->second.erase(q++);
	} else {
	  assert(missing_loc_sources.count(*q));
	  ++q;
	}
      if (p->second.empty())
	missing_loc.erase(p++);
      else
	++p;
    }
  }

  for (set<int>::iterator i = peer_log_requested.begin();
       i != peer_log_requested.end();
       ) {
    if (!osdmap->is_up(*i)) {
      dout(10) << "peer_log_requested removing " << *i << dendl;
      peer_log_requested.erase(i++);
    } else {
      ++i;
    }
  }

  for (set<int>::iterator i = peer_missing_requested.begin();
       i != peer_missing_requested.end();
       ) {
    if (!osdmap->is_up(*i)) {
      dout(10) << "peer_missing_requested removing " << *i << dendl;
      peer_missing_requested.erase(i++);
    } else {
      ++i;
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
      backfill_target >= 0 && started < max &&
      missing.num_missing() == 0 &&
      !waiting_on_backfill) {
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

  return 0;
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

    bool unfound = (missing_loc.find(soid) == missing_loc.end());

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

	    set<int>& loc = missing_loc[soid];
	    for (map<int,pg_missing_t>::iterator p = peer_missing.begin(); p != peer_missing.end(); ++p)
	      if (p->second.is_missing(soid, need) &&
		  p->second.missing[soid].have == alternate_need) {
		missing_loc_sources.insert(p->first);
		loc.insert(p->first);
	      }
	    dout(10) << " will pull " << alternate_need << " or " << need << " from one of " << loc << dendl;
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
  dout(10) << __func__ << ": on " << soid << dendl;

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc) {
    pg_log.missing_add(soid, v, eversion_t());
    bool uhoh = true;
    for (unsigned i=1; i<acting.size(); i++) {
      int peer = acting[i];
      if (!peer_missing[peer].is_missing(soid, v)) {
	missing_loc[soid].insert(peer);
	missing_loc_sources.insert(peer);
	dout(10) << info.pgid << " unexpectedly missing " << soid << " v" << v
		 << ", there should be a copy on osd." << peer << dendl;
	uhoh = false;
      }
    }
    if (uhoh)
      osd->clog.error() << info.pgid << " missing primary copy of " << soid << ", unfound\n";
    else
      osd->clog.error() << info.pgid << " missing primary copy of " << soid
			<< ", will try copies on " << missing_loc[soid] << "\n";
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
  for (unsigned i=1; i<get_parent()->get_acting().size(); i++) {
    int peer = get_parent()->get_acting()[i];
    map<int, pg_missing_t>::const_iterator j =
      get_parent()->get_peer_missing().find(peer);
    assert(j != get_parent()->get_peer_missing().end());
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
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    map<int, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    assert(pm != peer_missing.end());
    map<int, pg_info_t>::const_iterator pi = peer_info.find(peer);
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
	dout(10) << __func__ << ": already recovering" << soid << dendl;
	continue;
      }

      if (pg_log.get_missing().is_missing(soid)) {
	if (missing_loc.find(soid) == missing_loc.end())
	  dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	else
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

/**
 * recover_backfill
 *
 * Invariants:
 *
 * backfilled: fully pushed to replica or present in replica's missing set (both
 * our copy and theirs).
 *
 * All objects on backfill_target in [MIN,peer_backfill_info.begin) are either
 * not present or backfilled (all removed objects have been removed).
 * There may be PG objects in this interval yet to be backfilled.
 *
 * All objects in PG in [MIN,backfill_info.begin) have been backfilled to
 * backfill_target.  There may be objects on backfill_target yet to be deleted.
 *
 * All objects < MIN(peer_backfill_info.begin, backfill_info.begin) in PG are
 * backfilled.  No deleted objects in this interval remain on backfill_target.
 *
 * All objects <= peer_info[backfill_target].last_backfill have been backfilled
 * to backfill_target
 *
 * There *MAY* be objects between last_backfill_started and
 * MIN(peer_backfill_info.begin, backfill_info.begin) in the event that client
 * io created objects since the last scan.  For this reason, we call
 * update_range() again before continuing backfill.
 */
int ReplicatedPG::recover_backfill(
  int max,
  ThreadPool::TPHandle &handle, bool *work_started)
{
  dout(10) << "recover_backfill (" << max << ")" << dendl;
  assert(backfill_target >= 0);

  pg_info_t& pinfo = peer_info[backfill_target];
  BackfillInterval& pbi = peer_backfill_info;

  // Initialize from prior backfill state
  if (pbi.begin < pinfo.last_backfill) {
    pbi.reset(pinfo.last_backfill);
    backfill_info.reset(pinfo.last_backfill);
  }

  dout(10) << " peer osd." << backfill_target
	   << " last_backfill_started " << last_backfill_started
	   << " info " << pinfo
	   << " interval " << pbi.begin << "-" << pbi.end
	   << " " << pbi.objects.size() << " objects" << dendl;

  // update our local interval to cope with recent changes
  backfill_info.begin = last_backfill_started;
  update_range(&backfill_info, handle);

  int ops = 0;
  map<hobject_t,
      boost::tuple<eversion_t, eversion_t, ObjectContextRef> > to_push;
  map<hobject_t, eversion_t> to_remove;
  set<hobject_t> add_to_stat;

  pbi.trim_to(last_backfill_started);
  backfill_info.trim_to(last_backfill_started);

  hobject_t backfill_pos = MIN(backfill_info.begin, pbi.begin);
  while (ops < max) {
    if (backfill_info.begin <= pbi.begin &&
	!backfill_info.extends_to_end() && backfill_info.empty()) {
      hobject_t next = backfill_info.end;
      backfill_info.clear();
      backfill_info.begin = next;
      backfill_info.end = hobject_t::get_max();
      update_range(&backfill_info, handle);
      backfill_info.trim();
    }
    backfill_pos = MIN(backfill_info.begin, pbi.begin);

    dout(20) << "   my backfill " << backfill_info.begin << "-" << backfill_info.end
	     << " " << backfill_info.objects << dendl;
    dout(20) << " peer backfill " << pbi.begin << "-" << pbi.end << " " << pbi.objects << dendl;

    if (pbi.begin <= backfill_info.begin &&
	!pbi.extends_to_end() && pbi.empty()) {
      dout(10) << " scanning peer osd." << backfill_target << " from " << pbi.end << dendl;
      epoch_t e = get_osdmap()->get_epoch();
      MOSDPGScan *m = new MOSDPGScan(MOSDPGScan::OP_SCAN_GET_DIGEST, e, e, info.pgid,
				     pbi.end, hobject_t());
      osd->send_message_osd_cluster(backfill_target, m, get_osdmap()->get_epoch());
      waiting_on_backfill = true;
      start_recovery_op(pbi.end);
      ops++;
      break;
    }

    if (backfill_info.empty() && pbi.empty()) {
      dout(10) << " reached end for both local and peer" << dendl;
      break;
    }

    if (pbi.begin < backfill_info.begin) {
      dout(20) << " removing peer " << pbi.begin << dendl;
      to_remove[pbi.begin] = pbi.objects.begin()->second;
      // Object was degraded, but won't be recovered
      if (waiting_for_degraded_object.count(pbi.begin)) {
	requeue_ops(
	  waiting_for_degraded_object[pbi.begin]);
	waiting_for_degraded_object.erase(pbi.begin);
      }
      last_backfill_started = pbi.begin;
      pbi.pop_front();
      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
    } else if (pbi.begin == backfill_info.begin) {
      eversion_t& obj_v = backfill_info.objects.begin()->second;
      if (pbi.objects.begin()->second != obj_v) {
	ObjectContextRef obc = get_object_context(backfill_info.begin, false);
	assert(obc);
	if (obc->get_backfill_read()) {
	  dout(20) << " replacing peer " << pbi.begin << " with local "
		   << obj_v << dendl;
	  to_push[pbi.begin] = boost::make_tuple(
	    obj_v, pbi.objects.begin()->second, obc);
	  ops++;
	} else {
	  *work_started = true;
	  dout(20) << "backfill blocking on " << backfill_info.begin
		   << "; could not get rw_manager lock" << dendl;
	  break;
	}
      } else {
	dout(20) << " keeping peer " << pbi.begin << " "
		 << pbi.objects.begin()->second << dendl;
	// Object was degraded, but won't be recovered
	if (waiting_for_degraded_object.count(pbi.begin)) {
	  requeue_ops(waiting_for_degraded_object[pbi.begin]);
	  waiting_for_degraded_object.erase(pbi.begin);
	}
      }
      last_backfill_started = pbi.begin;
      add_to_stat.insert(pbi.begin);
      backfill_info.pop_front();
      pbi.pop_front();
    } else {
      ObjectContextRef obc = get_object_context(backfill_info.begin, false);
      assert(obc);
      if (obc->get_backfill_read()) {
	dout(20) << " pushing local " << backfill_info.begin << " "
		 << backfill_info.objects.begin()->second
		 << " to peer osd." << backfill_target << dendl;
	to_push[backfill_info.begin] =
	  boost::make_tuple(
	    backfill_info.objects.begin()->second,
	    eversion_t(),
	    obc);
	add_to_stat.insert(backfill_info.begin);
	last_backfill_started = backfill_info.begin;
	backfill_info.pop_front();
	ops++;
      } else {
	*work_started = true;
	dout(20) << "backfill blocking on " << backfill_info.begin
		 << "; could not get rw_manager lock" << dendl;
	break;
      }
    }
  }
  backfill_pos = MIN(backfill_info.begin, pbi.begin);

  for (set<hobject_t>::iterator i = add_to_stat.begin();
       i != add_to_stat.end();
       ++i) {
    ObjectContextRef obc = get_object_context(*i, false);
    pg_stat_t stat;
    add_object_context_to_pg_stat(obc, &stat);
    pending_backfill_updates[*i] = stat;
  }
  for (map<hobject_t, eversion_t>::iterator i = to_remove.begin();
       i != to_remove.end();
       ++i) {
    handle.reset_tp_timeout();

    // ordered before any subsequent updates
    send_remove_op(i->first, i->second, backfill_target);

    pending_backfill_updates[i->first]; // add empty stat!
  }

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  map<int, vector<PushOp> > pushes;
  for (map<hobject_t,
	   boost::tuple<eversion_t, eversion_t, ObjectContextRef> >::iterator i =
	     to_push.begin();
       i != to_push.end();
       ++i) {
    handle.reset_tp_timeout();
    prep_backfill_object_push(
      i->first, i->second.get<0>(), i->second.get<1>(), i->second.get<2>(),
      backfill_target, h);
  }
  pgbackend->run_recovery_op(h, cct->_conf->osd_recovery_op_priority);

  dout(5) << "backfill_pos is " << backfill_pos << " and pinfo.last_backfill is "
	  << pinfo.last_backfill << dendl;
  for (set<hobject_t>::iterator i = backfills_in_flight.begin();
       i != backfills_in_flight.end();
       ++i) {
    dout(20) << *i << " is still in flight" << dendl;
  }

  hobject_t next_backfill_to_complete = backfills_in_flight.size() ?
    *(backfills_in_flight.begin()) : backfill_pos;
  hobject_t new_last_backfill = pinfo.last_backfill;
  for (map<hobject_t, pg_stat_t>::iterator i = pending_backfill_updates.begin();
       i != pending_backfill_updates.end() &&
	 i->first < next_backfill_to_complete;
       pending_backfill_updates.erase(i++)) {
    pinfo.stats.add(i->second);
    assert(i->first > new_last_backfill);
    new_last_backfill = i->first;
  }

  /* If last_backfill is snapdir, we know that head necessarily cannot exist,
   * therefore it's safe to bump the snap up to NOSNAP.  This is necessary
   * since we need avoid having SNAPDIR backfilled and HEAD not backfilled
   * since a transaction on HEAD might change SNAPDIR
   */
  if (new_last_backfill.is_snapdir())
    new_last_backfill = new_last_backfill.get_head();
  if (last_backfill_started.is_snapdir())
    last_backfill_started = last_backfill_started.get_head();

  assert(!pending_backfill_updates.empty() ||
	 new_last_backfill == last_backfill_started);
  if (pending_backfill_updates.empty() &&
      backfill_pos.is_max()) {
    assert(backfills_in_flight.empty());
    new_last_backfill = backfill_pos;
    last_backfill_started = backfill_pos;
  }
  if (new_last_backfill > pinfo.last_backfill) {
    pinfo.last_backfill = new_last_backfill;
    epoch_t e = get_osdmap()->get_epoch();
    MOSDPGBackfill *m = NULL;
    if (pinfo.last_backfill.is_max()) {
      m = new MOSDPGBackfill(MOSDPGBackfill::OP_BACKFILL_FINISH, e, e, info.pgid);
      // Use default priority here, must match sub_op priority
      /* pinfo.stats might be wrong if we did log-based recovery on the
       * backfilled portion in addition to continuing backfill.
       */
      pinfo.stats = info.stats;
      start_recovery_op(hobject_t::get_max());
    } else {
      m = new MOSDPGBackfill(MOSDPGBackfill::OP_BACKFILL_PROGRESS, e, e, info.pgid);
      // Use default priority here, must match sub_op priority
    }
    m->last_backfill = pinfo.last_backfill;
    m->stats = pinfo.stats;
    osd->send_message_osd_cluster(backfill_target, m, get_osdmap()->get_epoch());
  }

  dout(10) << " peer num_objects now " << pinfo.stats.stats.sum.num_objects
	   << " / " << info.stats.stats.sum.num_objects << dendl;
  if (ops)
    *work_started = true;
  return ops;
}

void ReplicatedPG::prep_backfill_object_push(
  hobject_t oid, eversion_t v, eversion_t have,
  ObjectContextRef obc,
  int peer,
  PGBackend::RecoveryHandle *h)
{
  dout(10) << "push_backfill_object " << oid << " v " << v << " to osd." << peer << dendl;

  backfills_in_flight.insert(oid);
  map<int, pg_missing_t>::iterator bpm = peer_missing.find(backfill_target);
  assert(bpm != peer_missing.end());
  bpm->second.add(oid, eversion_t(), eversion_t());

  assert(!recovering.count(oid));

  start_recovery_op(oid);
  recovering.insert(make_pair(oid, obc));

  // We need to take the read_lock here in order to flush in-progress writes
  obc->ondisk_read_lock();
  pgbackend->recover_object(
    oid,
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
      int r = osd->store->stat(coll, p->soid, &st);
      if (r != -ENOENT) {
	dout(10) << "Object " << p->soid << " exists, but should have been "
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
  ss << "hit_set_" << info.pgid << "_current_" << stamp;
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

hobject_t ReplicatedPG::get_hit_set_archive_object(utime_t start, utime_t end)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid << "_archive_" << start << "_" << end;
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

void ReplicatedPG::hit_set_clear()
{
  dout(20) << __func__ << dendl;
  hit_set.reset(NULL);
  hit_set_start_stamp = utime_t();
}

void ReplicatedPG::hit_set_setup()
{
  if (!pool.info.hit_set_count ||
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

void ReplicatedPG::hit_set_persist()
{
  dout(10) << __func__  << dendl;
  bufferlist bl;

  utime_t now = ceph_clock_now(cct);
  RepGather *repop;
  hobject_t oid;
  bool reset = false;

  if (!info.hit_set.current_info.begin)
    info.hit_set.current_info.begin = hit_set_start_stamp;
  if (hit_set->is_full() ||
      hit_set_start_stamp + pool.info.hit_set_period <= now) {
    // archive
    hit_set->seal();
    ::encode(*hit_set, bl);
    info.hit_set.current_info.end = now;
    oid = get_hit_set_archive_object(info.hit_set.current_info.begin,
				     info.hit_set.current_info.end);
    dout(20) << __func__ << " archive " << oid << dendl;
    reset = true;
  } else {
    // persist snapshot of current hitset
    ::encode(*hit_set, bl);
    oid = get_hit_set_current_object(now);
    dout(20) << __func__ << " checkpoint " << oid << dendl;
  }

  ObjectContextRef obc = get_object_context(oid, true);
  repop = simple_repop_create(obc);
  OpContext *ctx = repop->ctx;
  ctx->at_version = get_next_version();

  if (info.hit_set.current_last_stamp != utime_t()) {
    // FIXME: we cheat slightly here by bundling in a remove on a object
    // other the RepGather object.  we aren't carrying an ObjectContext for
    // the deleted object over this period.
    hobject_t old_obj =
      get_hit_set_current_object(info.hit_set.current_last_stamp);
    ctx->op_t.remove(coll, old_obj);
    ctx->log.push_back(
        pg_log_entry_t(pg_log_entry_t::DELETE,
		       old_obj,
		       ctx->at_version,
		       info.hit_set.current_last_update,
		       0,
		       osd_reqid_t(),
		       ctx->mtime));
    ++ctx->at_version.version;

    struct stat st;
    int r = osd->store->stat(coll, old_obj, &st);
    assert(r == 0);
    --ctx->delta_stats.num_objects;
    ctx->delta_stats.num_bytes -= st.st_size;
  }

  info.hit_set.current_last_update = info.last_update; // *after* above remove!
  info.hit_set.current_info.version = ctx->at_version;
  if (reset) {
    info.hit_set.history.push_back(info.hit_set.current_info);
    hit_set_create();
    info.hit_set.current_info = pg_hit_set_info_t();
    info.hit_set.current_last_stamp = utime_t();
  } else {
    info.hit_set.current_last_stamp = now;
  }

  // fabricate an object_info_t and SnapSet
  ctx->new_obs.oi.version = ctx->at_version;
  ctx->new_obs.oi.mtime = now;
  ctx->new_obs.oi.size = bl.length();
  ctx->new_obs.exists = true;
  ctx->new_snapset.head_exists = true;

  ctx->delta_stats.num_objects++;
  ctx->delta_stats.num_bytes += bl.length();

  bufferlist bss;
  ::encode(ctx->new_snapset, bss);
  bufferlist boi(sizeof(ctx->new_obs.oi));
  ::encode(ctx->new_obs.oi, boi);

  ctx->op_t.write(coll, oid, 0, bl.length(), bl);
  ctx->op_t.setattr(coll, oid, OI_ATTR, boi);
  ctx->op_t.setattr(coll, oid, SS_ATTR, bss);
  ctx->log.push_back(
        pg_log_entry_t(
	  pg_log_entry_t::MODIFY,
	  oid,
	  ctx->at_version,
	  ctx->obs->oi.version,
	  0,
	  osd_reqid_t(),
	  ctx->mtime)
        );

  hit_set_trim(repop, pool.info.hit_set_count);

  info.stats.stats.add(ctx->delta_stats, string());

  simple_repop_submit(repop);
}

void ReplicatedPG::hit_set_trim(RepGather *repop, unsigned max)
{
  for (unsigned num = info.hit_set.history.size(); num > max; --num) {
    list<pg_hit_set_info_t>::iterator p = info.hit_set.history.begin();
    assert(p != info.hit_set.history.end());
    hobject_t oid = get_hit_set_archive_object(p->begin, p->end);
    dout(20) << __func__ << " removing " << oid << dendl;
    repop->ctx->op_t.remove(coll, oid);
    ++repop->ctx->at_version.version;
    repop->ctx->log.push_back(
        pg_log_entry_t(pg_log_entry_t::DELETE,
		       oid,
		       repop->ctx->at_version,
		       p->version,
		       0,
		       osd_reqid_t(),
		       repop->ctx->mtime));
    info.hit_set.history.pop_front();

    struct stat st;
    int r = osd->store->stat(coll, oid, &st);
    assert(r == 0);
    --repop->ctx->delta_stats.num_objects;
    repop->ctx->delta_stats.num_bytes -= st.st_size;
  }
}


// ==========================================================================================
// SCRUB


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

  bufferlist last_data;

  for (map<hobject_t,ScrubMap::object>::reverse_iterator p = scrubmap.objects.rbegin(); 
       p != scrubmap.objects.rend(); 
       ++p) {
    const hobject_t& soid = p->first;
    object_stat_sum_t stat;
    if (soid.snap != CEPH_SNAPDIR)
      stat.num_objects++;

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
      if (head != hobject_t()) {
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
	dout(20) << "  snapset " << snapset << dendl;
      }

      // subtract off any clone overlap
      for (map<snapid_t,interval_set<uint64_t> >::iterator q = snapset.clone_overlap.begin();
	   q != snapset.clone_overlap.end();
	   ++q) {
	for (interval_set<uint64_t>::const_iterator r = q->second.begin();
	     r != q->second.end();
	     ++r) {
	  stat.num_bytes -= r.get_len();
	}	  
      }
    }
    if (soid.snap == CEPH_SNAPDIR) {
      string cat;
      scrub_cstat.add(stat, cat);
      continue;
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

    if (oi.size != p->second.size) {
      osd->clog.error() << mode << " " << info.pgid << " " << soid
			<< " on disk size (" << p->second.size
			<< ") does not match object info size (" << oi.size << ")";
      ++scrubber.shallow_errors;
    }

    dout(20) << mode << "  " << soid << " " << oi << dendl;

    stat.num_bytes += p->second.size;

    //bufferlist data;
    //osd->store->read(c, poid, 0, 0, data);
    //assert(data.length() == p->size);
    //

    if (soid.snap == CEPH_NOSNAP) {
      if (!snapset.head_exists) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " snapset.head_exists=false, but object exists";
        ++scrubber.shallow_errors;
	continue;
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
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " expected clone " << *curclone;
        ++scrubber.shallow_errors;
	assert(soid.snap == *curclone);
      }

      assert(p->second.size == snapset.clone_size[*curclone]);

      // verify overlap?
      // ...

      // what's next?
      if (curclone != snapset.clones.rend())
	++curclone;

      if (curclone == snapset.clones.rend())
	head = hobject_t();

    } else {
      // it's unversioned.
    }

    string cat; // fixme
    scrub_cstat.add(stat, cat);
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
  }

  dout(10) << mode << " got "
	   << scrub_cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
	   << scrub_cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
	   << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes."
	   << dendl;

  if (scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      scrub_cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      scrub_cstat.sum.num_bytes != info.stats.stats.sum.num_bytes) {
    osd->clog.error() << info.pgid << " " << mode
		      << " stat mismatch, got "
		      << scrub_cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
		      << scrub_cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
		      << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes.\n";
    ++scrubber.shallow_errors;

    if (repair) {
      ++scrubber.fixed;
      info.stats.stats = scrub_cstat;
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
  context< SnapTrimmer >().requeue = false;
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::NotTrimming::exit()
{
  context< SnapTrimmer >().requeue = true;
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

  // Get next
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
  assert(repop);
  repop->queue_snap_trimmer = true;
  repops.insert(repop->get());
  pg->simple_repop_submit(repop);
  return discard_event();
}
/* WaitingOnReplicasObjects */
ReplicatedPG::WaitingOnReplicas::WaitingOnReplicas(my_context ctx)
  : my_base(ctx),
    NamedState(context< SnapTrimmer >().pg->cct, "Trimming/WaitingOnReplicas")
{
  context< SnapTrimmer >().log_enter(state_name);
  context< SnapTrimmer >().requeue = false;
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
    if (!(*i)->applied || !(*i)->waitfor_ack.empty()) {
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
  int tr = pg->osd->store->queue_transaction(pg->osr.get(), t);
  assert(tr == 0);

  context<SnapTrimmer>().need_share_pg_info = true;

  // Back to the start
  post_event(SnapTrim());
  return transit< NotTrimming >();
}

void intrusive_ptr_add_ref(ReplicatedPG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(ReplicatedPG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
uint64_t get_with_id(ReplicatedPG *pg) { return pg->get_with_id(); }
void put_with_id(ReplicatedPG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif
