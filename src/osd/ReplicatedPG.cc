// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "Watch.h"

#include "mds/inode_backtrace.h" // Ugh

#include "common/config.h"
#include "include/compat.h"

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"
#include "include/assert.h"  // json_spirit clobbers it

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, osd->whoami, get_osdmap())
static ostream& _prefix(std::ostream *_dout, PG *pg, int whoami, OSDMapRef osdmap) {
  return *_dout << pg->gen_prefix();
}


#include <sstream>
#include <utility>

#include <errno.h>

static const int LOAD_LATENCY    = 1;
static const int LOAD_QUEUE_SIZE = 2;
static const int LOAD_HYBRID     = 3;

// Blank object locator
static const object_locator_t OLOC_BLANK;

PGLSFilter::PGLSFilter()
{
}

PGLSFilter::~PGLSFilter()
{
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
  return missing.missing.count(soid);
}

void ReplicatedPG::wait_for_missing_object(const hobject_t& soid, OpRequestRef op)
{
  assert(is_missing_object(soid));

  // we don't have it (yet).
  map<hobject_t, pg_missing_t::item>::const_iterator g = missing.missing.find(soid);
  assert(g != missing.missing.end());
  const eversion_t &v(g->second.need);

  map<hobject_t, PullInfo>::const_iterator p = pulling.find(soid);
  if (p != pulling.end()) {
    dout(7) << "missing " << soid << " v " << v << ", already pulling." << dendl;
  }
  else if (missing_loc.find(soid) == missing_loc.end()) {
    dout(7) << "missing " << soid << " v " << v << ", is unfound." << dendl;
  }
  else {
    dout(7) << "missing " << soid << " v " << v << ", pulling." << dendl;
    pull(soid, v, g_conf->osd_client_op_priority);
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
  if (missing.missing.count(soid))
    return true;
  for (unsigned i = 1; i < acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
	peer_missing[peer].missing.count(soid))
      return true;

    // Object is degraded if after last_backfill AND
    // we have are backfilling it
    if (peer == backfill_target &&
	peer_info[peer].last_backfill <= soid &&
	backfill_pos >= soid &&
	backfills_in_flight.count(soid))
      return true;
  }
  return false;
}

void ReplicatedPG::wait_for_degraded_object(const hobject_t& soid, OpRequestRef op)
{
  assert(is_degraded_object(soid));

  // we don't have it (yet).
  if (pushing.count(soid)) {
    dout(7) << "degraded "
	    << soid 
	    << ", already pushing"
	    << dendl;
  } else {
    dout(7) << "degraded " 
	    << soid 
	    << ", pushing"
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
    recover_object_replicas(soid, v, g_conf->osd_client_op_priority);
  }
  waiting_for_degraded_object[soid].push_back(op);
  op->mark_delayed("waiting for degraded object");
}

void ReplicatedPG::wait_for_backfill_pos(OpRequestRef op)
{
  waiting_for_backfill_pos.push_back(op);
}

void ReplicatedPG::release_waiting_for_backfill_pos()
{
  requeue_ops(waiting_for_backfill_pos);
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

  int ret = osd->store->getattr(coll_t(info.pgid), sobj, filter->get_xattr().c_str(), bl);
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

int ReplicatedPG::do_command(vector<string>& cmd, ostream& ss,
			     bufferlist& idata, bufferlist& odata)
{
  if (cmd.size() && cmd[0] == "query") {
    JSONFormatter jsf(true);
    jsf.open_object_section("pg");
    jsf.dump_string("state", pg_state_string(get_state()));
    jsf.dump_unsigned("epoch", get_osdmap()->get_epoch());
    jsf.open_array_section("up");
    for (vector<int>::iterator p = up.begin(); p != up.end(); ++p)
      jsf.dump_unsigned("osd", *p);
    jsf.close_section();
    jsf.open_array_section("acting");
    for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p)
      jsf.dump_unsigned("osd", *p);
    jsf.close_section();
    jsf.open_object_section("info");
    info.dump(&jsf);
    jsf.close_section();

    jsf.open_array_section("recovery_state");
    handle_query_state(&jsf);
    jsf.close_section();

    jsf.close_section();
    stringstream dss;
    jsf.flush(dss);
    odata.append(dss);
    return 0;
  }
  else if (cmd.size() > 1 &&
	   cmd[0] == "mark_unfound_lost") {
    if (cmd.size() > 2) {
      ss << "too many arguments";
      return -EINVAL;
    }
    if (cmd.size() == 1) {
      ss << "too few arguments; must specify mode as 'revert' (mark and delete not yet implemented)";
      return -EINVAL;
    }
    if (cmd[1] != "revert") {
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
	 << " objects but we haven't probed all sources, not marking lost";
      return -EINVAL;
    }

    ss << "pg has " << unfound << " objects unfound and apparently lost, marking";
    mark_all_unfound_lost(mode);
    return 0;
  }
  else if (cmd.size() >= 1 && cmd[0] == "list_missing") {
    JSONFormatter jf(true);
    hobject_t offset;
    if (cmd.size() > 1) {
      json_spirit::Value v;
      try {
	if (!json_spirit::read(cmd[1], v))
	  throw std::runtime_error("bad json");
	offset.decode(v);
      } catch (std::runtime_error& e) {
	ss << "error parsing offset: " << e.what();
	return -EINVAL;
      }
    }
    jf.open_object_section("missing");
    {
      jf.open_object_section("offset");
      offset.dump(&jf);
      jf.close_section();
    }
    jf.dump_int("num_missing", missing.num_missing());
    jf.dump_int("num_unfound", get_num_unfound());
    map<hobject_t,pg_missing_t::item>::iterator p = missing.missing.upper_bound(offset);
    {
      jf.open_array_section("objects");
      int32_t num = 0;
      set<int> empty;
      bufferlist bl;
      while (p != missing.missing.end() && num < g_conf->osd_command_max_records) {
	jf.open_object_section("object");
	{
	  jf.open_object_section("oid");
	  p->first.dump(&jf);
	  jf.close_section();
	}
	p->second.dump(&jf);  // have, need keys
	{
	  jf.open_array_section("locations");
	  map<hobject_t,set<int> >::iterator q = missing_loc.find(p->first);
	  if (q != missing_loc.end())
	    for (set<int>::iterator r = q->second.begin(); r != q->second.end(); ++r)
	      jf.dump_int("osd", *r);
	  jf.close_section();
	}
	jf.close_section();
	++p;
	num++;
      }
      jf.close_section();
    }
    jf.dump_int("more", p != missing.missing.end());
    jf.close_section();
    stringstream jss;
    jf.flush(jss);
    odata.append(jss);
    return 0;
  };

  ss << "unknown command " << cmd;
  return -EINVAL;
}

// ==========================================================

bool ReplicatedPG::pg_op_must_wait(MOSDOp *op)
{
  if (missing.missing.empty())
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
  MOSDOp *m = (MOSDOp *)op->request;
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  dout(10) << "do_pg_op " << *m << dendl;

  op->mark_started();

  bufferlist outdata;
  int result = 0;
  string cname, mname;
  PGLSFilter *filter = NULL;
  bufferlist filter_out;

  snapid_t snapid = m->get_snapid();

  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); p++) {
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
	unsigned list_size = MIN(g_conf->osd_max_pgls, p->op.pgls.count);

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
	int r = osd->store->collection_list_partial(coll, current,
						    list_size,
						    list_size,
						    snapid,
						    &sentries,
						    &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	assert(snapid == CEPH_NOSNAP || missing.missing.empty());
	map<hobject_t, pg_missing_t::item>::iterator missing_iter =
	  missing.missing.lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	while (1) {
	  if (ls_iter == sentries.end()) {
	    break;
	  }

	  hobject_t candidate;
	  if (missing_iter == missing.missing.end() ||
	      *ls_iter < missing_iter->first) {
	    candidate = *(ls_iter++);
	  } else {
	    candidate = (missing_iter++)->first;
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
	      osd->store->getattr(coll, candidate, SS_ATTR, bl);
	      SnapSet snapset(bl);
	      if (snapid <= snapset.seq)
		continue;
	    } else {
	      bufferlist attr_bl;
	      osd->store->getattr(coll, candidate, OI_ATTR, attr_bl);
	      object_info_t oi(attr_bl);
	      vector<snapid_t>::iterator i = find(oi.snaps.begin(),
						  oi.snaps.end(),
						  snapid);
	      if (i == oi.snaps.end())
		continue;
	    }
	  }

	  if (filter && !pgls_filter(filter, candidate, filter_out))
	    continue;

	  response.entries.push_back(make_pair(candidate.oid,
					       candidate.get_key()));
	}
	if (next.is_max() &&
	    missing_iter == missing.missing.end() &&
	    ls_iter == sentries.end()) {
	  result = 1;
	}
	response.handle = next;
	::encode(response, outdata);
	if (filter)
	  ::encode(filter_out, outdata);
	dout(10) << " pgls result=" << result << " outdata.length()="
		 << outdata.length() << dendl;
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
  reply->set_data(outdata);
  reply->set_result(result);
  osd->send_message_osd_client(reply, m->get_connection());
  delete filter;
}

void ReplicatedPG::calc_trim_to()
{
  if (!is_degraded() && !is_scrubbing() && is_clean()) {
    if (min_last_complete_ondisk != eversion_t() &&
	min_last_complete_ondisk != pg_trim_to &&
	log.approx_size() > g_conf->osd_min_pg_log_entries) {
      size_t num_to_trim = log.approx_size() - g_conf->osd_min_pg_log_entries;
      list<pg_log_entry_t>::const_iterator it = log.log.begin();
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
      assert(pg_trim_to <= log.head);
      assert(pg_trim_to <= min_last_complete_ondisk);
    }
  } else {
    // don't trim
    pg_trim_to = eversion_t();
  }
}

ReplicatedPG::ReplicatedPG(OSDService *o, OSDMapRef curmap,
			   const PGPool &_pool, pg_t p, const hobject_t& oid,
			   const hobject_t& ioid) :
  PG(o, curmap, _pool, p, oid, ioid), temp_created(false),
  temp_coll(coll_t::make_temp_coll(p)), snap_trimmer_machine(this)
{ 
  snap_trimmer_machine.initiate();
}

void ReplicatedPG::get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc)
{
  src_oloc = oloc;
  if (oloc.key.empty())
    src_oloc.key = oid.name;
}

/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void ReplicatedPG::do_op(OpRequestRef op)
{
  MOSDOp *m = (MOSDOp*)op->request;
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  if (op->includes_pg_op()) {
    if (pg_op_must_wait(m)) {
      wait_for_all_missing(op);
      return;
    }
    return do_pg_op(op);
  }

  dout(10) << "do_op " << *m << (op->may_write() ? " may_write" : "") << dendl;

  hobject_t head(m->get_oid(), m->get_object_locator().key,
		 CEPH_NOSNAP, m->get_pg().ps(),
		 info.pgid.pool());

  if (op->may_write() && scrubber.write_blocked_by_scrub(head)) {
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
  if (op->may_write() && is_degraded_object(head)) {
    wait_for_degraded_object(head, op);
    return;
  }

  if (head == backfill_pos) {
    wait_for_backfill_pos(op);
    return;
  }

  // missing snapdir?
  hobject_t snapdir(m->get_oid(), m->get_object_locator().key,
		    CEPH_SNAPDIR, m->get_pg().ps(), info.pgid.pool());
  if (is_missing_object(snapdir)) {
    wait_for_missing_object(snapdir, op);
    return;
  }

  // degraded object?
  if (op->may_write() && is_degraded_object(snapdir)) {
    wait_for_degraded_object(snapdir, op);
    return;
  }
 
  entity_inst_t client = m->get_source_inst();

  ObjectContext *obc;
  bool can_create = op->may_write();
  snapid_t snapid;
  int r = find_object_context(
    hobject_t(m->get_oid(), 
	      m->get_object_locator().key,
	      m->get_snapid(),
	      m->get_pg().ps(),
	      m->get_object_locator().get_pool()),
    m->get_object_locator(),
    &obc, can_create, &snapid);
  if (r) {
    if (r == -EAGAIN) {
      // If we're not the primary of this OSD, and we have
      // CEPH_OSD_FLAG_LOCALIZE_READS set, we just return -EAGAIN. Otherwise,
      // we have to wait for the object.
      if (is_primary() || (!(m->get_flags() & CEPH_OSD_FLAG_LOCALIZE_READS))) {
	// missing the specific snap we need; requeue and wait.
	assert(!can_create); // only happens on a read
	hobject_t soid(m->get_oid(), m->get_object_locator().key,
		       snapid, m->get_pg().ps(),
		       info.pgid.pool());
	wait_for_missing_object(soid, op);
	return;
      }
    }
    osd->reply_op_error(op, r);
    return;
  }
  
  // make sure locator is consistent
  if (m->get_object_locator() != obc->obs.oi.oloc) {
    dout(10) << " provided locator " << m->get_object_locator() 
	     << " != object's " << obc->obs.oi.oloc
	     << " on " << obc->obs.oi.soid << dendl;
    osd->clog.warn() << "bad locator " << m->get_object_locator() 
		     << " on object " << obc->obs.oi.oloc
		     << " loc " << m->get_object_locator() 
		     << " op " << *m << "\n";
  }

  if ((op->may_read()) && (obc->obs.oi.lost)) {
    // This object is lost. Reading from it returns an error.
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
	     << " is lost" << dendl;
    osd->reply_op_error(op, -ENFILE);
    return;
  }
  dout(25) << __func__ << ": object " << obc->obs.oi.soid
	   << " has oi of " << obc->obs.oi << dendl;
  
  bool ok;
  dout(10) << "do_op mode is " << mode << dendl;
  assert(!mode.wake);   // we should never have woken waiters here.
  if ((op->may_read() && op->may_write()) ||
      (m->get_flags() & CEPH_OSD_FLAG_RWORDERED))
    ok = mode.try_rmw(client);
  else if (op->may_write())
    ok = mode.try_write(client);
  else if (op->may_read())
    ok = mode.try_read(client);
  else
    assert(0);
  if (!ok) {
    dout(10) << "do_op waiting on mode " << mode << dendl;
    mode.waiting.push_back(op);
    op->mark_delayed("waiting on pg mode");
    return;
  }

  if (!op->may_write() && !obc->obs.exists) {
    osd->reply_op_error(op, -ENOENT);
    put_object_context(obc);
    return;
  }

  dout(10) << "do_op mode now " << mode << dendl;

  // are writes blocked by another object?
  if (obc->blocked_by) {
    dout(10) << "do_op writes for " << obc->obs.oi.soid << " blocked by "
	     << obc->blocked_by->obs.oi.soid << dendl;
    wait_for_degraded_object(obc->blocked_by->obs.oi.soid, op);
    put_object_context(obc);
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
  map<hobject_t,ObjectContext*> src_obc;
  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); p++) {
    OSDOp& osd_op = *p;
    if (!ceph_osd_op_type_multi(osd_op.op.op))
      continue;
    if (osd_op.soid.oid.name.length()) {
      object_locator_t src_oloc;
      get_src_oloc(m->get_oid(), m->get_object_locator(), src_oloc);
      hobject_t src_oid(osd_op.soid, src_oloc.key, m->get_pg().ps(),
			info.pgid.pool());
      if (!src_obc.count(src_oid)) {
	ObjectContext *sobc;
	snapid_t ssnapid;

	int r = find_object_context(src_oid, src_oloc, &sobc, false, &ssnapid);
	if (r == -EAGAIN) {
	  // missing the specific snap we need; requeue and wait.
	  hobject_t wait_oid(osd_op.soid.oid, src_oloc.key, ssnapid, m->get_pg().ps(),
			     info.pgid.pool());
	  wait_for_missing_object(wait_oid, op);
	} else if (r) {
	  osd->reply_op_error(op, r);
	} else if (sobc->obs.oi.oloc.key != obc->obs.oi.oloc.key &&
		   sobc->obs.oi.oloc.key != obc->obs.oi.soid.oid.name &&
		   sobc->obs.oi.soid.oid.name != obc->obs.oi.oloc.key) {
	  dout(1) << " src_oid " << osd_op.soid << " oloc " << sobc->obs.oi.oloc << " != "
		  << m->get_oid() << " oloc " << obc->obs.oi.oloc << dendl;
	  osd->reply_op_error(op, -EINVAL);
	} else if (is_degraded_object(sobc->obs.oi.soid) ||
		   (before_backfill && sobc->obs.oi.soid > backfill_target_info->last_backfill)) {
	  wait_for_degraded_object(sobc->obs.oi.soid, op);
	  dout(10) << " writes for " << obc->obs.oi.soid << " now blocked by "
		   << sobc->obs.oi.soid << dendl;
	  obc->get();
	  obc->blocked_by = sobc;
	  sobc->get();
	  sobc->blocking.insert(obc);
	} else {
	  dout(10) << " src_oid " << src_oid << " obc " << src_obc << dendl;
	  src_obc[src_oid] = sobc;
	  continue;
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
    put_object_contexts(src_obc);
    put_object_context(obc);
    return;
  }

  op->mark_started();

  const hobject_t& soid = obc->obs.oi.soid;
  OpContext *ctx = new OpContext(op, m->get_reqid(), m->ops,
				 &obc->obs, obc->ssc, 
				 this);
  ctx->obc = obc;
  ctx->src_obc = src_obc;

  if (op->may_write()) {
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
	       << " on " << soid << dendl;
      delete ctx;
      put_object_context(obc);
      put_object_contexts(src_obc);
      osd->reply_op_error(op, -EOLDSNAPC);
      return;
    }

    eversion_t oldv = log.get_request_version(ctx->reqid);
    if (oldv != eversion_t()) {
      dout(3) << "do_op dup " << ctx->reqid << " was " << oldv << dendl;
      delete ctx;
      put_object_context(obc);
      put_object_contexts(src_obc);
      if (already_complete(oldv)) {
	osd->reply_op_error(op, 0, oldv);
      } else {
	if (m->wants_ack()) {
	  if (already_ack(oldv)) {
	    MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
	    reply->add_flags(CEPH_OSD_FLAG_ACK);
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

    // version
    ctx->at_version = log.head;

    ctx->at_version.epoch = get_osdmap()->get_epoch();
    ctx->at_version.version++;
    assert(ctx->at_version > info.last_update);
    assert(ctx->at_version > log.head);

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

  // note my stats
  utime_t now = ceph_clock_now(g_ceph_context);

  // note some basic context for op replication that prepare_transaction may clobber
  eversion_t old_last_update = log.head;
  bool old_exists = obc->obs.exists;
  uint64_t old_size = obc->obs.oi.size;
  eversion_t old_version = obc->obs.oi.version;

  if (op->may_read()) {
    dout(10) << " taking ondisk_read_lock" << dendl;
    obc->ondisk_read_lock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " taking ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_lock();
  }

  int result = prepare_transaction(ctx);

  if (op->may_read()) {
    dout(10) << " dropping ondisk_read_lock" << dendl;
    obc->ondisk_read_unlock();
  }
  for (map<hobject_t,ObjectContext*>::iterator p = src_obc.begin(); p != src_obc.end(); ++p) {
    dout(10) << " dropping ondisk_read_lock for src " << p->first << dendl;
    p->second->ondisk_read_unlock();
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
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
  if (ctx->op_t.empty() && !ctx->modify) {
    // read.
    ctx->reply->claim_op_out_data(ctx->ops);
    ctx->reply->get_header().data_off = ctx->data_off;
  } else {
    // write.  normalize the result code.
    if (result > 0)
      result = 0;
  }
  ctx->reply->set_result(result);

  if (result >= 0)
    ctx->reply->set_version(ctx->reply_version);
  else if (result == -ENOENT)
    ctx->reply->set_version(info.last_update);

  // read or error?
  if (ctx->op_t.empty() || result < 0) {
    if (result >= 0) {
      log_op_stats(ctx);
      update_stats();
    }
    
    MOSDOpReply *reply = ctx->reply;
    ctx->reply = NULL;
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    osd->send_message_osd_client(reply, m->get_connection());
    delete ctx;
    put_object_context(obc);
    put_object_contexts(src_obc);
    return;
  }

  assert(op->may_write());

  // trim log?
  calc_trim_to();

  append_log(ctx->log, pg_trim_to, ctx->local_t);
  
  // continuing on to write path, make sure object context is registered
  assert(obc->registered);

  // issue replica writes
  tid_t rep_tid = osd->get_tid();
  RepGather *repop = new_repop(ctx, obc, rep_tid);  // new repop claims our obc, src_obc refs
  // note: repop now owns ctx AND ctx->op

  repop->src_obc.swap(src_obc); // and src_obc.

  issue_repop(repop, now, old_last_update, old_exists, old_size, old_version);

  eval_repop(repop);
  repop->put();
}


void ReplicatedPG::log_op_stats(OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  MOSDOp *m = (MOSDOp*)op->request;

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= ctx->op->request->get_recv_stamp();

  utime_t rlatency;
  if (ctx->readable_stamp != utime_t()) {
    rlatency = ctx->readable_stamp;
    rlatency -= ctx->op->request->get_recv_stamp();
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

void ReplicatedPG::log_subop_stats(OpRequestRef op, int tag_inb, int tag_lat)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t latency = now;
  latency -= op->request->get_recv_stamp();

  uint64_t inb = op->request->get_data().length();

  osd->logger->inc(l_osd_sop);

  osd->logger->inc(l_osd_sop_inb, inb);
  osd->logger->tinc(l_osd_sop_lat, latency);

  if (tag_inb)
    osd->logger->inc(tag_inb, inb);
  osd->logger->tinc(tag_lat, latency);

  dout(15) << "log_subop_stats " << *op->request << " inb " << inb << " latency " << latency << dendl;
}



void ReplicatedPG::do_sub_op(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(require_same_or_newer_map(m->map_epoch));
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(15) << "do_sub_op " << *op->request << dendl;

  OSDOp *first = NULL;
  if (m->ops.size() >= 1) {
    first = &m->ops[0];
    switch (first->op.op) {
    case CEPH_OSD_OP_PULL:
      sub_op_pull(op);
      return;
    }
  }

  if (!is_active()) {
    waiting_for_active.push_back(op);
    op->mark_delayed("waiting for active");
    return;
  }

  if (first) {
    switch (first->op.op) {
    case CEPH_OSD_OP_PUSH:
      sub_op_push(op);
      return;
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
  MOSDSubOpReply *r = (MOSDSubOpReply *)op->request;
  assert(r->get_header().type == MSG_OSD_SUBOPREPLY);
  if (r->ops.size() >= 1) {
    OSDOp& first = r->ops[0];
    switch (first.op.op) {
    case CEPH_OSD_OP_PUSH:
      // continue peer recovery
      sub_op_push_reply(op);
      return;

    case CEPH_OSD_OP_SCRUB_RESERVE:
      sub_op_scrub_reserve_reply(op);
      return;
    }
  }

  sub_op_modify_reply(op);
}

void ReplicatedPG::do_scan(OpRequestRef op)
{
  MOSDPGScan *m = (MOSDPGScan*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_SCAN);
  dout(10) << "do_scan " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGScan::OP_SCAN_GET_DIGEST:
    {
      BackfillInterval bi;
      osr->flush();
      scan_range(m->begin, g_conf->osd_backfill_scan_min, g_conf->osd_backfill_scan_max, &bi);
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

      backfill_pos = backfill_info.begin > peer_backfill_info.begin ?
	peer_backfill_info.begin : backfill_info.begin;
      release_waiting_for_backfill_pos();
      dout(10) << " backfill_pos now " << backfill_pos << dendl;

      assert(waiting_on_backfill);
      waiting_on_backfill = false;
      finish_recovery_op(bi.begin);
    }
    break;
  }
}

void ReplicatedPG::do_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = (MOSDPGBackfill*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);
  dout(10) << "do_backfill " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGBackfill::OP_BACKFILL_FINISH:
    {
      assert(is_replica());
      assert(g_conf->osd_kill_backfill_at != 1);

      MOSDPGBackfill *reply = new MOSDPGBackfill(MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
						 get_osdmap()->get_epoch(), m->query_epoch,
						 info.pgid);
      reply->set_priority(g_conf->osd_recovery_op_priority);
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
      assert(g_conf->osd_kill_backfill_at != 2);

      info.last_backfill = m->last_backfill;
      info.stats.stats = m->stats;

      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      write_info(*t);
      int tr = osd->store->queue_transaction(osr.get(), t);
      assert(tr == 0);
    }
    break;

  case MOSDPGBackfill::OP_BACKFILL_FINISH_ACK:
    {
      assert(is_primary());
      assert(g_conf->osd_kill_backfill_at != 3);
      finish_recovery_op(hobject_t::get_max());
    }
    break;
  }
}

/* Returns head of snap_trimq as snap_to_trim and the relevant objects as 
 * obs_to_trim */
bool ReplicatedPG::get_obs_to_trim(snapid_t &snap_to_trim,
				   coll_t &col_to_trim,
				   vector<hobject_t> &obs_to_trim)
{
  assert_locked();
  obs_to_trim.clear();

  interval_set<snapid_t> s;
  s.intersection_of(snap_trimq, info.purged_snaps);
  if (!s.empty()) {
    dout(0) << "WARNING - snap_trimmer: snap_trimq contained snaps already in "
	    << "purged_snaps" << dendl;
    snap_trimq.subtract(s);
  }

  dout(10) << "get_obs_to_trim , purged_snaps " << info.purged_snaps << dendl;

  if (snap_trimq.size() == 0)
    return false;

  snap_to_trim = snap_trimq.range_start();
  col_to_trim = coll_t(info.pgid, snap_to_trim);

  if (!snap_collections.contains(snap_to_trim)) {
    return true;
  }

  // flush pg ops to fs so we can rely on collection_list()
  osr->flush();

  osd->store->collection_list(col_to_trim, obs_to_trim);

  return true;
}

ReplicatedPG::RepGather *ReplicatedPG::trim_object(const hobject_t &coid,
						   const snapid_t &sn)
{
  // load clone info
  bufferlist bl;
  ObjectContext *obc = 0;
  int r = find_object_context(
    hobject_t(coid.oid, coid.get_key(), sn, coid.hash, info.pgid.pool()),
    OLOC_BLANK, &obc, false, NULL);
  if (r == -ENOENT || coid.snap != obc->obs.oi.soid.snap) {
    if (obc) put_object_context(obc);
    return 0;
  }
  assert(r == 0);
  assert(obc->registered);
  object_info_t &coi = obc->obs.oi;
  vector<snapid_t>& snaps = coi.snaps;

  // get snap set context
  if (!obc->ssc)
    obc->ssc = get_snapset_context(coid.oid, coid.get_key(), coid.hash, false);
  SnapSetContext *ssc = obc->ssc;
  assert(ssc);
  SnapSet& snapset = ssc->snapset;

  dout(10) << coid << " snaps " << snaps << " old snapset " << snapset << dendl;
  assert(snapset.seq);

  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops, &obc->obs, ssc, this);
  ctx->mtime = ceph_clock_now(g_ceph_context);

  ctx->at_version.epoch = get_osdmap()->get_epoch();
  ctx->at_version.version = log.head.version + 1;


  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;
    
  // trim clone's snaps
  vector<snapid_t> newsnaps;
  for (unsigned i=0; i<snaps.size(); i++)
    if (!pool.info.is_removed_snap(snaps[i]))
      newsnaps.push_back(snaps[i]);

  if (newsnaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << " ... deleting" << dendl;
    t->remove(coll, coid);
    t->collection_remove(coll_t(info.pgid, snaps[0]), coid);
    if (snaps.size() > 1)
      t->collection_remove(coll_t(info.pgid, snaps[snaps.size()-1]), coid);

    // ...from snapset
    snapid_t last = coid.snap;
    vector<snapid_t>::iterator p;
    for (p = snapset.clones.begin(); p != snapset.clones.end(); p++)
      if (*p == last)
	break;
    assert(p != snapset.clones.end());
    object_stat_sum_t delta;
    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      interval_set<uint64_t> keep;
      keep.union_of(snapset.clone_overlap[*n], snapset.clone_overlap[*p]);
      add_interval_usage(keep, delta);  // not deallocated
      snapset.clone_overlap[*n].intersection_of(snapset.clone_overlap[*p]);
    } else {
      add_interval_usage(snapset.clone_overlap[last], delta);  // not deallocated
    }
    delta.num_objects--;
    delta.num_object_clones--;
    delta.num_bytes -= snapset.clone_size[last];
    info.stats.stats.add(delta, obc->obs.oi.category);

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);
	
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::DELETE, coid, ctx->at_version, ctx->obs->oi.version,
				  osd_reqid_t(), ctx->mtime));
    ctx->at_version.version++;
  } else {
    // save adjusted snaps for this object
    dout(10) << coid << " snaps " << snaps << " -> " << newsnaps << dendl;
    coi.snaps.swap(newsnaps);
    vector<snapid_t>& oldsnaps = newsnaps;
    coi.prior_version = coi.version;
    coi.version = ctx->at_version;
    bl.clear();
    ::encode(coi, bl);
    t->setattr(coll, coid, OI_ATTR, bl);

    set<snapid_t> old_snapdirs, new_snapdirs;

    old_snapdirs.insert(oldsnaps[0]);
    old_snapdirs.insert(*(oldsnaps.rbegin()));

    new_snapdirs.insert(snaps[0]);
    new_snapdirs.insert(*(snaps.rbegin()));

    set<snapid_t> to_remove, to_create;
    for (set<snapid_t>::iterator i = old_snapdirs.begin();
	 i != old_snapdirs.end();
	 ++i) {
      if (new_snapdirs.count(*i))
	continue;
      t->collection_remove(coll_t(info.pgid, *i), coid);
      to_remove.insert(*i);
    }
    for (set<snapid_t>::iterator i = new_snapdirs.begin();
	 i != new_snapdirs.end();
	 ++i) {
      if (old_snapdirs.count(*i))
	continue;
      make_snap_collection(ctx->local_t, *i);
      t->collection_add(coll_t(info.pgid, *i), coll, coid);
      to_create.insert(*i);
    }

    dout(10) << "removing coid " << coid << " from snap collections "
	     << to_remove << " and adding to snap collections "
	     << to_create << " for final snaps " << coi.snaps << dendl;

    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, coid, coi.version, coi.prior_version,
				  osd_reqid_t(), ctx->mtime));
    ::encode(coi.snaps, ctx->log.back().snaps);
    ctx->at_version.version++;
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << dendl;

  hobject_t snapoid(coid.oid, coid.get_key(),
		    snapset.head_exists ? CEPH_NOSNAP:CEPH_SNAPDIR, coid.hash,
		    info.pgid.pool());
  ctx->snapset_obc = get_object_context(snapoid, coi.oloc, false);
  assert(ctx->snapset_obc->registered);
  if (snapset.clones.empty() && !snapset.head_exists) {
    dout(10) << coid << " removing " << snapoid << dendl;
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::DELETE, snapoid, ctx->at_version, 
				  ctx->snapset_obc->obs.oi.version, osd_reqid_t(), ctx->mtime));
    ctx->snapset_obc->obs.exists = false;

    t->remove(coll, snapoid);
  } else {
    dout(10) << coid << " updating snapset on " << snapoid << dendl;
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, snapoid, ctx->at_version, 
				  ctx->snapset_obc->obs.oi.version, osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc->obs.oi.prior_version = ctx->snapset_obc->obs.oi.version;
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
    assert(mode.try_write(nobody));
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

void ReplicatedPG::dump_watchers(ObjectContext *obc)
{
  assert(osd->watch_lock.is_locked());
  
  dout(10) << "dump_watchers " << obc->obs.oi.soid << " " << obc->obs.oi << dendl;
  for (map<entity_name_t, OSD::Session *>::iterator iter = obc->watchers.begin(); 
       iter != obc->watchers.end();
       ++iter)
    dout(10) << " * obc->watcher: " << iter->first << " session=" << iter->second << dendl;
  
  for (map<entity_name_t, watch_info_t>::iterator oi_iter = obc->obs.oi.watchers.begin();
       oi_iter != obc->obs.oi.watchers.end();
       oi_iter++) {
    watch_info_t& w = oi_iter->second;
    dout(10) << " * oi->watcher: " << oi_iter->first << " cookie=" << w.cookie << dendl;
  }
}

void ReplicatedPG::remove_watcher(ObjectContext *obc, entity_name_t entity)
{
  assert_locked();
  assert(osd->watch_lock.is_locked());
  dout(10) << "remove_watcher " << *obc << " " << entity << dendl;
  map<entity_name_t, OSD::Session *>::iterator iter = obc->watchers.find(entity);
  assert(iter != obc->watchers.end());
  OSD::Session *session = iter->second;
  dout(10) << "remove_watcher removing session " << session << dendl;

  obc->watchers.erase(iter);
  assert(session->watches.count(obc));
  session->watches.erase(obc);

  put_object_context(obc);
  session->con->put();
  session->put();
}

void ReplicatedPG::remove_notify(ObjectContext *obc, Watch::Notification *notif)
{
  assert_locked();
  assert(osd->watch_lock.is_locked());
  map<Watch::Notification *, bool>::iterator niter = obc->notifs.find(notif);

  // Cancel notification
  if (notif->timeout)
    osd->watch_timer.cancel_event(notif->timeout);
  osd->watch->remove_notification(notif);

  assert(niter != obc->notifs.end());

  niter->first->session->con->put();
  niter->first->session->put();
  obc->notifs.erase(niter);

  put_object_context(obc);
  delete notif;
}

void ReplicatedPG::remove_watchers_and_notifies()
{
  assert_locked();

  dout(10) << "remove_watchers" << dendl;

  osd->watch_lock.Lock();
  for (map<hobject_t, ObjectContext*>::iterator oiter = object_contexts.begin();
       oiter != object_contexts.end();
       ) {
    map<hobject_t, ObjectContext *>::iterator iter = oiter++;
    ObjectContext *obc = iter->second;
    obc->ref++;
    for (map<entity_name_t, OSD::Session *>::iterator witer = obc->watchers.begin();
	 witer != obc->watchers.end();
	 remove_watcher(obc, (witer++)->first)) ;
    for (map<entity_name_t,Watch::C_WatchTimeout*>::iterator iter = obc->unconnected_watchers.begin();
	 iter != obc->unconnected_watchers.end();
	 ) {
      map<entity_name_t,Watch::C_WatchTimeout*>::iterator i = iter++;
      unregister_unconnected_watcher(obc, i->first);
    }
    for (map<Watch::Notification *, bool>::iterator niter = obc->notifs.begin();
	 niter != obc->notifs.end();
	 remove_notify(obc, (niter++)->first)) ;
    put_object_context(obc);
  }
  osd->watch_lock.Unlock();
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
	  break;
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

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); p++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;
 
    dout(10) << "do_osd_op  " << osd_op << dendl;

    bufferlist::iterator bp = osd_op.indata.begin();

    // user-visible modifcation?
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
      break;
    default:
      if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;
    }

    ObjectContext *src_obc = 0;
    if (ceph_osd_op_type_multi(op.op)) {
      object_locator_t src_oloc;
      get_src_oloc(soid.oid, ((MOSDOp *)ctx->op->request)->get_object_locator(), src_oloc);
      hobject_t src_oid(osd_op.soid, src_oloc.key, soid.hash,
			info.pgid.pool());
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
	op.extent.offset + op.extent.length >= oi.size) {
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      
      // --- READS ---

    case CEPH_OSD_OP_READ:
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
	  if (g_conf->osd_verify_sparse_read_holes &&
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
	if (g_conf->osd_verify_sparse_read_holes) {
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
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);
	dout(10) << "method called response length=" << outdata.length() << dendl;
	op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
	dout(30) << "out dump: ";
	osd_op.outdata.hexdump(*_dout);
	*_dout << dendl;
      }
      break;

    case CEPH_OSD_OP_STAT:
      {
	if (obs.exists) {
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

    case CEPH_OSD_OP_GETXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	int r = osd->store->getattr(coll, soid, name.c_str(), osd_op.outdata);
	if (r >= 0) {
	  op.xattr.value_len = r;
	  result = 0;
	} else
	  result = r;
	ctx->delta_stats.num_rd++;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
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
        osd_op.outdata.claim_append(bl);
      }
      break;
      
    case CEPH_OSD_OP_CMPXATTR:
    case CEPH_OSD_OP_SRC_CMPXATTR:
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;
	
	bufferlist xattr;
	if (op.op == CEPH_OSD_OP_CMPXATTR)
	  result = osd->store->getattr(coll, soid, name.c_str(), xattr);
	else
	  result = osd->store->getattr(coll, src_obc->obs.oi.soid, name.c_str(), xattr);
	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;
	
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
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      {
	uint64_t ver = op.watch.ver;
	if (!ver)
	  result = -EINVAL;
        else if (ver < oi.user_version.version)
	  result = -ERANGE;
	else if (ver > oi.user_version.version)
	  result = -EOVERFLOW;
	break;
      }

    case CEPH_OSD_OP_ASSERT_SRC_VERSION:
      {
	uint64_t ver = op.watch.ver;
	if (!ver)
	  result = -EINVAL;
        else if (ver < src_obc->obs.oi.user_version.version)
	  result = -ERANGE;
	else if (ver > src_obc->obs.oi.user_version.version)
	  result = -EOVERFLOW;
	break;
      }

   case CEPH_OSD_OP_NOTIFY:
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
	  timeout = g_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.cookie = op.watch.cookie;
        n.bl = bl;
	ctx->notifies.push_back(n);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      {
	osd->watch_lock.Lock();
	entity_name_t source = ctx->op->request->get_source();
	map<entity_name_t, watch_info_t>::iterator oi_iter = oi.watchers.find(source);
	Watch::Notification *notif = osd->watch->get_notif(op.watch.cookie);
	if (oi_iter != oi.watchers.end() && notif) {
	  ctx->notify_acks.push_back(op.watch.cookie);
	} else {
	  if (!notif)
	    dout(10) << " no pending notify for cookie " << op.watch.cookie << dendl;
	  else
	    dout(10) << " not registered as a watcher" << dendl;
	  result = -EINVAL;
	}
	osd->watch_lock.Unlock();
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      { // write
        __u32 seq = oi.truncate_seq;
        if (seq && (seq > op.extent.truncate_seq) &&
            (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
        }
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  if (obs.exists) {
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
	bufferlist nbl;
	bp.copy(op.extent.length, nbl);
	t.write(coll, soid, op.extent.offset, op.extent.length, nbl);
	write_update_size_and_usage(ctx->delta_stats, oi, ssc->snapset, ctx->modified_ranges,
				    op.extent.offset, op.extent.length, true);
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      { // write full object
	bufferlist nbl;
	bp.copy(op.extent.length, nbl);
	if (obs.exists) {
	  t.truncate(coll, soid, 0);
	} else {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.write(coll, soid, op.extent.offset, op.extent.length, nbl);
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
      result = _rollback_to(ctx, op);
      break;

    case CEPH_OSD_OP_ZERO:
      { // zero
	assert(op.extent.length);
	if (obs.exists) {
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
      {
        int flags = le32_to_cpu(op.flags);
	if (obs.exists && (flags & CEPH_OSD_OP_FLAG_EXCL)) {
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
	      if (obs.exists) {
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
      {
	// truncate
	if (!obs.exists) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
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
      if (ctx->obc->obs.oi.watchers.size()) {
	// Cannot delete an object with watchers
	result = -EBUSY;
      } else {
	result = _delete_head(ctx);
      }
      break;

    case CEPH_OSD_OP_CLONERANGE:
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
      {
        uint64_t cookie = op.watch.cookie;
	bool do_watch = op.watch.flag & 1;
        entity_name_t entity = ctx->reqid.name;
	ObjectContext *obc = ctx->obc;

	dout(10) << "watch: ctx->obc=" << (void *)obc << " cookie=" << cookie
		 << " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(10) << "watch: oi.user_version=" << oi.user_version.version << dendl;

	watch_info_t w(cookie, 30);  // FIXME: where does the timeout come from?
	if (do_watch) {
	  if (oi.watchers.count(entity) && oi.watchers[entity] == w) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[entity] = w;
	    t.nop();  // make sure update the object_info on disk!
	  }
	  ctx->watch_connect = true;
	  ctx->watch_info = w;
	  assert(obc->registered);
        } else {
	  map<entity_name_t, watch_info_t>::iterator oi_iter = oi.watchers.find(entity);
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by " << entity << dendl;
            oi.watchers.erase(entity);
	    t.nop();  // update oi on disk

	    ctx->watch_disconnect = true;

	    // FIXME: trigger notifies?

	  } else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
        }
      }
      break;


      // -- object attrs --
      
    case CEPH_OSD_OP_SETXATTR:
      {
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

	if (g_conf->osd_tmapput_sets_uses_tmap) {
	  assert(g_conf->osd_auto_upgrade_tmap);
	  oi.uses_tmap = true;
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
      result = do_tmapup(ctx, bp, osd_op);
      break;

      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
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

	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  dout(20) << "CEPH_OSD_OP_OMAPGETKEYS: "
		   << " Reading " << oi.soid << " omap from tmap" << dendl;
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    map<string, bufferlist>::iterator iter =
	      vals.upper_bound(start_after);
	    for (uint64_t i = 0;
		 i < max_return && iter != vals.end();
		 ++i, iter++) {
	      out_set.insert(iter->first);
	    }
	    ::encode(out_set, osd_op.outdata);
	    break;
	  }
	  dout(10) << "failed, reading from omap" << dendl;
	  // No valid tmap, use omap
	}

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
      }
      break;
    case CEPH_OSD_OP_OMAPGETVALS:
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

	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  dout(20) << "CEPH_OSD_OP_OMAPGETVALS: "
		   << " Reading " << oi.soid << " omap from tmap" << dendl;
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    map<string, bufferlist>::iterator iter = vals.upper_bound(start_after);
	    if (filter_prefix > start_after) iter = vals.lower_bound(filter_prefix);
	    for (uint64_t i = 0;
		 i < max_return && iter != vals.end() &&
		   iter->first.substr(0, filter_prefix.size()) == filter_prefix;
		 ++i, iter++) {
	      out_set.insert(*iter);
	    }
	    ::encode(out_set, osd_op.outdata);
	    break;
	  }
	  // No valid tmap, use omap
	  dout(10) << "failed, reading from omap" << dendl;
	}

	{
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    coll, soid
	    );
	  assert(iter);
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
      }
      break;
    case CEPH_OSD_OP_OMAPGETHEADER:
      {
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  dout(20) << "CEPH_OSD_OP_OMAPGETHEADER: "
		   << " Reading " << oi.soid << " omap from tmap" << dendl;
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    osd_op.outdata.claim(header);
	    break;
	  }
	  // No valid tmap, fall through to omap
	  dout(10) << "failed, reading from omap" << dendl;
	}
	osd->store->omap_get_header(coll, soid, &osd_op.outdata);
      }
      break;
    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
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
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  dout(20) << "CEPH_OSD_OP_OMAPGET: "
		   << " Reading " << oi.soid << " omap from tmap" << dendl;
	  map<string, bufferlist> vals;
	  bufferlist header;
	  int r = _get_tmap(ctx, &vals, &header);
	  if (r == 0) {
	    for (set<string>::iterator iter = keys_to_get.begin();
		 iter != keys_to_get.end();
		 iter++) {
	      if (vals.count(*iter)) {
		out.insert(*(vals.find(*iter)));
	      }
	    }
	    ::encode(out, osd_op.outdata);
	    break;
	  }
	  // No valid tmap, use omap
	  dout(10) << "failed, reading from omap" << dendl;
	}
	osd->store->omap_get_values(coll, soid, keys_to_get, &out);
	::encode(out, osd_op.outdata);
      }
      break;
    case CEPH_OSD_OP_OMAP_CMP:
      {
	if (!obs.exists) {
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
      {
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
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
      }
      break;
    case CEPH_OSD_OP_OMAPSETHEADER:
      {
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
	if (!obs.exists) {
	  ctx->delta_stats.num_objects++;
	  obs.exists = true;
	}
	t.touch(coll, soid);
	t.omap_setheader(coll, soid, osd_op.indata);
      }
      break;
    case CEPH_OSD_OP_OMAPCLEAR:
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
	}
	t.touch(coll, soid);
	t.omap_clear(coll, soid);
      }
      break;
    case CEPH_OSD_OP_OMAPRMKEYS:
      {
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (oi.uses_tmap && g_conf->osd_auto_upgrade_tmap) {
	  _copy_up_tmap(ctx);
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

int ReplicatedPG::_copy_up_tmap(OpContext *ctx)
{
  dout(20) << "copying up tmap for " << ctx->new_obs.oi.soid << dendl;
  ctx->new_obs.oi.uses_tmap = false;
  map<string, bufferlist> vals;
  bufferlist header;
  int r = _get_tmap(ctx, &vals, &header);
  if (r < 0)
    return 0;
  ctx->op_t.omap_setkeys(coll, ctx->new_obs.oi.soid,
			 vals);
  ctx->op_t.omap_setheader(coll, ctx->new_obs.oi.soid,
			   header);
  return 0;
}

inline int ReplicatedPG::_delete_head(OpContext *ctx)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  ObjectStore::Transaction& t = ctx->op_t;

  if (!obs.exists)
    return -ENOENT;
  
  t.remove(coll, soid);

  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
  }

  ctx->delta_stats.num_objects--;
  ctx->delta_stats.num_bytes -= oi.size;

  oi.size = 0;
  snapset.head_exists = false;
  obs.exists = false;

  ctx->delta_stats.num_wr++;
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

  ObjectContext *rollback_to;
  int ret = find_object_context(
    hobject_t(soid.oid, oi.oloc.key, snapid, soid.hash, info.pgid.pool()), 
    oi.oloc, &rollback_to, false, &cloneid);
  if (ret) {
    if (-ENOENT == ret) {
      // there's no snapshot here, or there's no object.
      // if there's no snapshot, we delete the object; otherwise, do nothing.
      dout(20) << "_rollback_to deleting head on " << soid.oid
	       << " because got ENOENT on find_object_context" << dendl;
      if (ctx->obc->obs.oi.watchers.size()) {
	// Cannot delete an object with watchers
	ret = -EBUSY;
      } else {
	_delete_head(ctx);
	ret = 0;
      }
    } else if (-EAGAIN == ret) {
      /* a different problem, like degraded pool
       * with not-yet-restored object. We shouldn't have been able
       * to get here; recovery should have completed first! */
      hobject_t rollback_target(soid.oid, soid.get_key(), cloneid, soid.hash,
				info.pgid.pool());
      assert(is_missing_object(rollback_target));
      dout(20) << "_rollback_to attempted to roll back to a missing object " 
	       << rollback_target << " (requested snapid: ) " << snapid << dendl;
      wait_for_missing_object(rollback_target, ctx->op);
    } else {
      // ummm....huh? It *can't* return anything else at time of writing.
      assert(0);
    }
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
      
      map<string, bufferptr> attrs;
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
    put_object_context(rollback_to);
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
    object_info_t static_snap_oi(coid, ctx->obs->oi.oloc);
    object_info_t *snap_oi;
    if (is_primary()) {
      ctx->clone_obc = new ObjectContext(static_snap_oi, true, NULL);
      ctx->clone_obc->get();
      register_object_context(ctx->clone_obc);
      snap_oi = &ctx->clone_obc->obs.oi;
    } else {
      snap_oi = &static_snap_oi;
    }
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);
    snap_oi->snaps = snaps;
    _make_clone(t, soid, coid, snap_oi);
    
    // add to snap bound collections
    coll_t fc = make_snap_collection(ctx->local_t, snaps[0]);
    t.collection_add(fc, coll, coid);
    if (snaps.size() > 1) {
      coll_t lc = make_snap_collection(ctx->local_t, snaps[snaps.size()-1]);
      t.collection_add(lc, coll, coid);
    }
    
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
				  ctx->obs->oi.version, ctx->reqid, ctx->new_obs.oi.mtime));
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
  if (ctx->watch_connect || ctx->watch_disconnect ||
      !ctx->notifies.empty() || !ctx->notify_acks.empty()) {
    OSD::Session *session = (OSD::Session *)ctx->op->request->get_connection()->get_priv();
    assert(session);
    ObjectContext *obc = ctx->obc;
    object_info_t& oi = ctx->new_obs.oi;
    hobject_t& soid = oi.soid;
    entity_name_t entity = ctx->reqid.name;

    dout(10) << "do_osd_op_effects applying watch/notify effects on session " << session << dendl;

    osd->watch_lock.Lock();
    dump_watchers(obc);
    
    map<entity_name_t, OSD::Session *>::iterator iter = obc->watchers.find(entity);
    if (ctx->watch_connect) {
      watch_info_t w = ctx->watch_info;

      if (iter == obc->watchers.end()) {
	dout(10) << " connected to " << w << " by " << entity << " session " << session << dendl;
	obc->watchers[entity] = session;
	session->con->get();
	session->get();
	session->watches[obc] = get_osdmap()->object_locator_to_pg(soid.oid, obc->obs.oi.oloc);
	obc->ref++;
      } else if (iter->second == session) {
	// already there
	dout(10) << " already connected to " << w << " by " << entity
		 << " session " << session << dendl;
      } else {
	// weird: same entity, different session.
	dout(10) << " reconnected (with different session!) watch " << w << " by " << entity
		 << " session " << session << " (was " << iter->second << ")" << dendl;
	session->con->get();
	session->get();

	iter->second->watches.erase(obc);
	iter->second->con->put();
	iter->second->put();

	iter->second = session;
	session->watches[obc] = get_osdmap()->object_locator_to_pg(soid.oid, obc->obs.oi.oloc);
      }
      map<entity_name_t,Watch::C_WatchTimeout*>::iterator un_iter =
	obc->unconnected_watchers.find(entity);
      if (un_iter != obc->unconnected_watchers.end()) {
	unregister_unconnected_watcher(obc, un_iter->first);
      }

      map<Watch::Notification *, bool>::iterator niter;
      for (niter = obc->notifs.begin(); niter != obc->notifs.end(); ++niter) {
	Watch::Notification *notif = niter->first;
	map<entity_name_t, Watch::WatcherState>::iterator iter = notif->watchers.find(entity);
	if (iter != notif->watchers.end()) {
	  /* there is a pending notification for this watcher, we should resend it anyway
	     even if we already sent it as it might not have received it */
	  MWatchNotify *notify_msg = new MWatchNotify(w.cookie, oi.user_version.version, notif->id, WATCH_NOTIFY, notif->bl);
	  osd->send_message_osd_client(notify_msg, session->con);
	}
      }
    }
    
    if (ctx->watch_disconnect) {
      if (iter != obc->watchers.end()) {
	remove_watcher(obc, entity);
      } else {
	assert(obc->unconnected_watchers.count(entity));
	unregister_unconnected_watcher(obc, entity);
      }

      // ack any pending notifies
      map<Watch::Notification *, bool>::iterator p = obc->notifs.begin();
      while (p != obc->notifs.end()) {
	Watch::Notification *notif = p->first;
	entity_name_t by = entity;
	p++;
	assert(notif->obc == obc);
	dout(10) << " acking pending notif " << notif->id << " by " << by << dendl;
	// TODOSAM: osd->osd-> not good
	osd->osd->ack_notification(entity, notif, obc, this);
      }
    }

    for (list<notify_info_t>::iterator p = ctx->notifies.begin();
	 p != ctx->notifies.end();
	 ++p) {

      dout(10) << " " << *p << dendl;

      Watch::Notification *notif = new Watch::Notification(ctx->reqid.name, session, p->cookie, p->bl);
      session->get();  // notif got a reference
      session->con->get();
      notif->pgid = get_osdmap()->object_locator_to_pg(soid.oid, obc->obs.oi.oloc);

      osd->watch->add_notification(notif);
      dout(20) << " notify id " << notif->id << dendl;

      // connected
      for (map<entity_name_t, watch_info_t>::iterator i = obc->obs.oi.watchers.begin();
	   i != obc->obs.oi.watchers.end();
	   ++i) {
	map<entity_name_t, OSD::Session*>::iterator q = obc->watchers.find(i->first);
	if (q != obc->watchers.end()) {
	  entity_name_t name = q->first;
	  OSD::Session *s = q->second;
	  watch_info_t& w = obc->obs.oi.watchers[q->first];

	  notif->add_watcher(name, Watch::WATCHER_NOTIFIED); // adding before send_message to avoid race

	  MWatchNotify *notify_msg = new MWatchNotify(w.cookie, oi.user_version.version, notif->id, WATCH_NOTIFY, notif->bl);
	  osd->send_message_osd_client(notify_msg, s->con);
	} else {
	  // unconnected
	  entity_name_t name = i->first;
	  notif->add_watcher(name, Watch::WATCHER_PENDING);
	}
      }

      notif->reply = new MWatchNotify(p->cookie, oi.user_version.version, notif->id, WATCH_NOTIFY_COMPLETE, notif->bl);
      if (notif->watchers.empty()) {
	// TODOSAM: osd->osd-> not good
	osd->osd->complete_notify(notif, obc);
      } else {
	obc->notifs[notif] = true;
	obc->ref++;
	notif->obc = obc;
	// TODOSAM: osd->osd not good
	notif->timeout = new Watch::C_NotifyTimeout(osd->osd, notif);
	osd->watch_timer.add_event_after(p->timeout, notif->timeout);
      }
    }

    for (list<uint64_t>::iterator p = ctx->notify_acks.begin(); p != ctx->notify_acks.end(); ++p) {
      uint64_t cookie = *p;
      
      dout(10) << " notify_ack " << cookie << dendl;
      map<entity_name_t, watch_info_t>::iterator oi_iter = oi.watchers.find(entity);
      assert(oi_iter != oi.watchers.end());

      Watch::Notification *notif = osd->watch->get_notif(cookie);
      assert(notif);

      // TODOSAM: osd->osd-> not good
      osd->osd->ack_notification(entity, notif, obc, this);
    }

    osd->watch_lock.Unlock();
    session->put();
  }
}

bool ReplicatedPG::have_temp_coll()
{
  return temp_created || osd->store->collection_exists(temp_coll);
}

coll_t ReplicatedPG::get_temp_coll(ObjectStore::Transaction *t)
{
  if (temp_created)
    return temp_coll;
  if (!osd->store->collection_exists(temp_coll))
      t->create_collection(temp_coll);
  temp_created = true;
  return temp_coll;
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
    ctx->reply_version = ctx->obs->oi.user_version;
    return result;
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
			info.pgid.pool());

      ctx->snapset_obc = get_object_context(snapoid, ctx->new_obs.oi.oloc, false);
      if (ctx->snapset_obc && ctx->snapset_obc->obs.exists) {
	ctx->op_t.remove(coll, snapoid);
	dout(10) << " removing old " << snapoid << dendl;

	ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::DELETE, snapoid, ctx->at_version, old_version,
				      osd_reqid_t(), ctx->mtime));
 	ctx->at_version.version++;

	ctx->snapset_obc->obs.exists = false;
	assert(ctx->snapset_obc->registered);
      }
    }
  } else if (ctx->new_snapset.clones.size()) {
    // save snapset on _snap
    hobject_t snapoid(soid.oid, soid.get_key(), CEPH_SNAPDIR, soid.hash,
		      info.pgid.pool());
    dout(10) << " final snapset " << ctx->new_snapset
	     << " in " << snapoid << dendl;
    ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, snapoid, ctx->at_version, old_version,
				  osd_reqid_t(), ctx->mtime));

    ctx->snapset_obc = get_object_context(snapoid, ctx->new_obs.oi.oloc, true);
    ctx->snapset_obc->obs.exists = true;
    ctx->snapset_obc->obs.oi.version = ctx->at_version;
    ctx->snapset_obc->obs.oi.last_reqid = ctx->reqid;
    ctx->snapset_obc->obs.oi.mtime = ctx->mtime;
    assert(ctx->snapset_obc->registered);

    bufferlist bv(sizeof(ctx->new_obs.oi));
    ::encode(ctx->snapset_obc->obs.oi, bv);
    ctx->op_t.touch(coll, snapoid);
    ctx->op_t.setattr(coll, snapoid, OI_ATTR, bv);
    ctx->op_t.setattr(coll, snapoid, SS_ATTR, bss);
    ctx->at_version.version++;
  }

  // finish and log the op.
  if (ctx->user_modify) {
    /* update the user_version for any modify ops, except for the watch op */
    ctx->new_obs.oi.user_version = ctx->at_version;
  }
  ctx->reply_version = ctx->new_obs.oi.user_version;
  ctx->bytes_written = ctx->op_t.get_encoded_bytes();
  ctx->new_obs.oi.version = ctx->at_version;
 
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
				ctx->reqid, ctx->mtime));

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;
  ctx->obc->ssc->snapset = ctx->new_snapset;
  info.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);

  if (backfill_target >= 0) {
    pg_info_t& pinfo = peer_info[backfill_target];
    if (soid < pinfo.last_backfill)
      pinfo.stats.stats.add(ctx->delta_stats, ctx->obc->obs.oi.category);
    else if (soid < backfill_pos)
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
// rep op gather

class C_OSD_OpApplied : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpApplied(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->op_applied(repop);
    pg->put();
  }
};

class C_OSD_OpCommit : public Context {
public:
  ReplicatedPG *pg;
  ReplicatedPG::RepGather *repop;

  C_OSD_OpCommit(ReplicatedPG *p, ReplicatedPG::RepGather *rg) :
    pg(p), repop(rg) {
    repop->get();
    pg->get();    // we're copying the pointer
  }
  void finish(int r) {
    pg->op_commit(repop);
    pg->put();
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

  Context *oncommit = new C_OSD_OpCommit(this, repop);
  Context *onapplied = new C_OSD_OpApplied(this, repop);
  Context *onapplied_sync = new C_OSD_OndiskWriteUnlock(repop->obc,
							repop->ctx->clone_obc);
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
    put_object_context(repop->ctx->clone_obc);
    repop->ctx->clone_obc = 0;
  }
  if (repop->ctx->snapset_obc) {
    put_object_context(repop->ctx->snapset_obc);
    repop->ctx->snapset_obc = 0;
  }

  dout(10) << "op_applied mode was " << mode << dendl;
  mode.write_applied();
  dout(10) << "op_applied mode now " << mode << " (finish_write)" << dendl;

  put_object_context(repop->obc);
  put_object_contexts(repop->src_obc);
  repop->obc = 0;

  if (!repop->aborted) {
    assert(repop->waitfor_ack.count(whoami) ||
	   repop->waitfor_disk.count(whoami) == 0);  // commit before ondisk
    repop->waitfor_ack.erase(whoami);

    assert(info.last_update >= repop->v);
    assert(last_update_applied < repop->v);
    last_update_applied = repop->v;

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
    
    last_update_ondisk = repop->v;

    last_complete_ondisk = repop->pg_local_last_complete;
    eval_repop(repop);
  }

  repop->put();
  unlock();
}



void ReplicatedPG::eval_repop(RepGather *repop)
{
  MOSDOp *m = NULL;
  if (repop->ctx->op)
    m = (MOSDOp *)repop->ctx->op->request;

  if (m)
    dout(10) << "eval_repop " << *repop
	     << " wants=" << (m->wants_ack() ? "a":"") << (m->wants_ondisk() ? "d":"")
	     << (repop->done ? " DONE" : "")
	     << dendl;
  else
    dout(10) << "eval_repop " << *repop << " (no op)"
	     << (repop->done ? " DONE" : "")
	     << dendl;

  if (repop->done)
    return;

  // apply?
  if (!repop->applied && !repop->applying &&
      ((mode.is_delayed_mode() &&
	repop->waitfor_ack.size() == 1) ||  // all other replicas have acked
       mode.is_rmw_mode()))
    apply_repop(repop);
  
  if (m) {

    // an 'ondisk' reply implies 'ack'. so, prefer to send just one
    // ondisk instead of ack followed by ondisk.

    // ondisk?
    if (repop->waitfor_disk.empty()) {

      log_op_stats(repop->ctx);
      update_stats();

      // send dup commits, in order
      if (waiting_for_ondisk.count(repop->v)) {
	assert(waiting_for_ondisk.begin()->first == repop->v);
	for (list<OpRequestRef>::iterator i = waiting_for_ondisk[repop->v].begin();
	     i != waiting_for_ondisk[repop->v].end();
	     ++i) {
	  osd->reply_op_error(*i, 0, repop->v);
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
	else
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending commit on " << *repop << " " << reply << dendl;
	assert(entity_name_t::TYPE_OSD != m->get_connection()->peer_type);
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
	  MOSDOp *m = (MOSDOp*)(*i)->request;
	  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
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
	else
	  reply = new MOSDOpReply(m, 0, get_osdmap()->get_epoch(), 0);
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
	repop->ctx->readable_stamp = ceph_clock_now(g_ceph_context);
    }
  }

  // done.
  if (repop->waitfor_ack.empty() && repop->waitfor_disk.empty() &&
      repop->applied) {
    repop->done = true;

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

void ReplicatedPG::issue_repop(RepGather *repop, utime_t now,
			       eversion_t old_last_update, bool old_exists, uint64_t old_size, eversion_t old_version)
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
	(((MOSDOp *)ctx->op->request)->get_flags() & CEPH_OSD_FLAG_PARALLELEXEC)) {
      // replicate original op for parallel execution on replica
      assert(0 == "broken implementation, do not use");
      wr->oloc = repop->ctx->obs->oi.oloc;
      wr->ops = repop->ctx->ops;
      wr->mtime = repop->ctx->mtime;
      wr->old_exists = old_exists;
      wr->old_size = old_size;
      wr->old_version = old_version;
      wr->snapset = repop->obc->ssc->snapset;
      wr->snapc = repop->ctx->snapc;
      wr->set_data(repop->ctx->op->request->get_data());   // _copy_ bufferlist
    } else {
      // ship resulting transaction, log entries, and pg_stats
      if (peer == backfill_target && soid >= backfill_pos) {
	dout(10) << "issue_repop shipping empty opt to osd." << peer << ", object beyond backfill_pos "
		 << backfill_pos << ", last_backfill is " << pinfo.last_backfill << dendl;
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
    }
    
    wr->pg_trim_to = pg_trim_to;
    osd->send_message_osd_cluster(peer, wr, get_osdmap()->get_epoch());

    // keep peer_info up to date
    if (pinfo.last_complete == pinfo.last_update)
      pinfo.last_update = ctx->at_version;
    pinfo.last_update = ctx->at_version;
  }
}

ReplicatedPG::RepGather *ReplicatedPG::new_repop(OpContext *ctx, ObjectContext *obc,
						 tid_t rep_tid)
{
  if (ctx->op)
    dout(10) << "new_repop rep_tid " << rep_tid << " on " << *ctx->op->request << dendl;
  else
    dout(10) << "new_repop rep_tid " << rep_tid << " (no op)" << dendl;

  RepGather *repop = new RepGather(ctx, obc, rep_tid, info.last_complete);

  dout(10) << "new_repop mode was " << mode << dendl;
  mode.write_start();
  dout(10) << "new_repop mode now " << mode << " (start_write)" << dendl;

  repop->start = ceph_clock_now(g_ceph_context);

  repop_queue.push_back(&repop->queue_item);
  repop_map[repop->rep_tid] = repop;
  repop->get();

  osd->logger->set(l_osd_op_wip, repop_map.size());

  return repop;
}
 
void ReplicatedPG::remove_repop(RepGather *repop)
{
  repop_map.erase(repop->rep_tid);
  repop->put();

  osd->logger->set(l_osd_op_wip, repop_map.size());
}

void ReplicatedPG::repop_ack(RepGather *repop, int result, int ack_type,
			     int fromosd, eversion_t peer_lcod)
{
  MOSDOp *m = NULL;

  if (repop->ctx->op)
    m = (MOSDOp *)repop->ctx->op->request;

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







// -------------------------------------------------------

void ReplicatedPG::populate_obc_watchers(ObjectContext *obc)
{
  assert(is_active());
  assert(!is_missing_object(obc->obs.oi.soid) ||
	 (log.objects.count(obc->obs.oi.soid) && // or this is a revert... see recover_primary()
	  log.objects[obc->obs.oi.soid]->op == pg_log_entry_t::LOST_REVERT &&
	  log.objects[obc->obs.oi.soid]->reverting_to == obc->obs.oi.version));

  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  if (!obc->obs.oi.watchers.empty()) {
    Mutex::Locker l(osd->watch_lock);
    assert(obc->unconnected_watchers.size() == 0);
    assert(obc->watchers.size() == 0);
    // populate unconnected_watchers
    utime_t now = ceph_clock_now(g_ceph_context);
    for (map<entity_name_t, watch_info_t>::iterator p = obc->obs.oi.watchers.begin();
	 p != obc->obs.oi.watchers.end();
	 p++) {
      utime_t expire = now;
      expire += p->second.timeout_seconds;
      dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
      register_unconnected_watcher(obc, p->first, expire);
    }
  }
}

void ReplicatedPG::unregister_unconnected_watcher(void *_obc,
						  entity_name_t entity)
{
  ObjectContext *obc = static_cast<ObjectContext *>(_obc);

  /* If we failed to cancel the event, the event will fire and the obc
   * ref and the pg ref will be taken care of */
  if (osd->watch_timer.cancel_event(obc->unconnected_watchers[entity])) {
    put_object_context(obc);
    put();
  }
  obc->unconnected_watchers.erase(entity);
}

void ReplicatedPG::register_unconnected_watcher(void *_obc,
						entity_name_t entity,
						utime_t expire)
{
  ObjectContext *obc = static_cast<ObjectContext *>(_obc);
  pg_t pgid = info.pgid;
  pgid.set_ps(obc->obs.oi.soid.hash);
  get();
  obc->ref++;
  Watch::C_WatchTimeout *cb = new Watch::C_WatchTimeout(osd->osd,
							static_cast<void *>(obc),
							this,
							entity, expire);
  osd->watch_timer.add_event_at(expire, cb);
  obc->unconnected_watchers[entity] = cb;
}

void ReplicatedPG::handle_watch_timeout(void *_obc,
					entity_name_t entity,
					utime_t expire)
{
  dout(10) << "handle_watch_timeout obc " << _obc << dendl;
  struct HandleWatchTimeout : public Context {
    epoch_t cur_epoch;
    boost::intrusive_ptr<ReplicatedPG> pg;
    void *obc;
    entity_name_t entity;
    utime_t expire;
    HandleWatchTimeout(
      epoch_t cur_epoch,
      ReplicatedPG *pg,
      void *obc,
      entity_name_t entity,
      utime_t expire) : cur_epoch(cur_epoch),
			pg(pg), obc(obc), entity(entity), expire(expire) {
      assert(pg->is_locked());
      static_cast<ReplicatedPG::ObjectContext*>(obc)->get();
    }
    void finish(int) {
      assert(pg->is_locked());
      if (cur_epoch < pg->last_peering_reset)
	return;
      // handle_watch_timeout gets its own ref
      static_cast<ReplicatedPG::ObjectContext*>(obc)->get();
      pg->handle_watch_timeout(obc, entity, expire);
    }
    ~HandleWatchTimeout() {
      assert(pg->is_locked());
      pg->put_object_context(static_cast<ReplicatedPG::ObjectContext*>(obc));
    }
  };

  ObjectContext *obc = static_cast<ObjectContext *>(_obc);

  if (obc->unconnected_watchers.count(entity) == 0 ||
      (obc->unconnected_watchers[entity] &&
       obc->unconnected_watchers[entity]->expire != expire)) {
     /* If obc->unconnected_watchers[entity] == NULL we know at least that
      * the watcher for obc,entity should expire.  We might not have been
      * the intended Context*, but that's ok since the intended one will
      * take this branch and assume it raced. */
    dout(10) << "handle_watch_timeout must have raced, no/wrong unconnected_watcher "
	     << entity << dendl;
    put_object_context(obc);
    return;
  }

  if (is_degraded_object(obc->obs.oi.soid)) {
    callbacks_for_degraded_object[obc->obs.oi.soid].push_back(
      new HandleWatchTimeout(get_osdmap()->get_epoch(),
			     this, _obc, entity, expire)
      );
    dout(10) << "handle_watch_timeout waiting for degraded on obj "
	     << obc->obs.oi.soid
	     << dendl;
    obc->unconnected_watchers[entity] = 0; // Callback in progress, but not this one!
    put_object_context(obc); // callback got its own ref
    return;
  }

  if (scrubber.write_blocked_by_scrub(obc->obs.oi.soid)) {
    dout(10) << "handle_watch_timeout waiting for scrub on obj "
	     << obc->obs.oi.soid
	     << dendl;
    scrubber.add_callback(new HandleWatchTimeout(get_osdmap()->get_epoch(),
						 this, _obc, entity, expire));
    obc->unconnected_watchers[entity] = 0; // Callback in progress, but not this one!
    put_object_context(obc); // callback got its own ref
    return;
  }

  obc->unconnected_watchers.erase(entity);
  obc->obs.oi.watchers.erase(entity);

  vector<OSDOp> ops;
  tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContext *ctx = new OpContext(OpRequestRef(), reqid, ops,
				 &obc->obs, obc->ssc, this);
  ctx->mtime = ceph_clock_now(g_ceph_context);

  ctx->at_version.epoch = get_osdmap()->get_epoch();
  ctx->at_version.version = log.head.version + 1;

  entity_inst_t nobody;

  /* Currently, mode.try_write always returns true.  If this changes, we will
   * need to delay the repop accordingly */
  assert(mode.try_write(nobody));
  RepGather *repop = new_repop(ctx, obc, rep_tid);

  ObjectStore::Transaction *t = &ctx->op_t;

  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, obc->obs.oi.soid,
				ctx->at_version,
				obc->obs.oi.version,
				osd_reqid_t(), ctx->mtime));

  eversion_t old_last_update = log.head;
  bool old_exists = repop->obc->obs.exists;
  uint64_t old_size = repop->obc->obs.oi.size;
  eversion_t old_version = repop->obc->obs.oi.version;

  obc->obs.oi.prior_version = old_version;
  obc->obs.oi.version = ctx->at_version;
  bufferlist bl;
  ::encode(obc->obs.oi, bl);
  t->setattr(coll, obc->obs.oi.soid, OI_ATTR, bl);

  append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);

  // obc ref swallowed by repop!
  issue_repop(repop, repop->ctx->mtime, old_last_update, old_exists,
	      old_size, old_version);
  eval_repop(repop);
}

ReplicatedPG::ObjectContext *ReplicatedPG::_lookup_object_context(const hobject_t& oid)
{
  map<hobject_t, ObjectContext*>::iterator p = object_contexts.find(oid);
  if (p != object_contexts.end())
    return p->second;
  return NULL;
}

ReplicatedPG::ObjectContext *ReplicatedPG::create_object_context(const object_info_t& oi,
								 SnapSetContext *ssc)
{
  ObjectContext *obc = new ObjectContext(oi, false, ssc);
  dout(10) << "create_object_context " << obc << " " << oi.soid << " " << obc->ref << dendl;
  register_object_context(obc);
  populate_obc_watchers(obc);
  obc->ref++;
  return obc;
}

ReplicatedPG::ObjectContext *ReplicatedPG::get_object_context(const hobject_t& soid,
							      const object_locator_t& oloc,
							      bool can_create)
{
  map<hobject_t, ObjectContext*>::iterator p = object_contexts.find(soid);
  ObjectContext *obc;
  if (p != object_contexts.end()) {
    obc = p->second;
    dout(10) << "get_object_context " << obc << " " << soid << " " << obc->ref
	     << " -> " << (obc->ref+1) << dendl;
  } else {
    // check disk
    bufferlist bv;
    int r = osd->store->getattr(coll, soid, OI_ATTR, bv);
    if (r < 0) {
      if (!can_create)
	return NULL;   // -ENOENT!

      // new object.
      object_info_t oi(soid, oloc);
      SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, true);
      return create_object_context(oi, ssc);
    }

    object_info_t oi(bv);

    // if the on-disk oloc is bad/undefined, set up the pool value
    if (oi.oloc.get_pool() < 0) {
      oi.oloc.pool = info.pgid.pool();
    }

    SnapSetContext *ssc = NULL;
    if (can_create)
      ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, true);
    obc = new ObjectContext(oi, true, ssc);
    obc->obs.oi.decode(bv);
    obc->obs.exists = true;

    register_object_context(obc);

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, true);

    populate_obc_watchers(obc);
    dout(10) << "get_object_context " << obc << " " << soid << " 0 -> 1 read " << obc->obs.oi << dendl;
  }
  obc->ref++;
  return obc;
}

void ReplicatedPG::context_registry_on_change()
{
  remove_watchers_and_notifies();
}


int ReplicatedPG::find_object_context(const hobject_t& oid,
				      const object_locator_t& oloc,
				      ObjectContext **pobc,
				      bool can_create,
				      snapid_t *psnapid)
{
  // want the head?
  hobject_t head(oid.oid, oid.get_key(), CEPH_NOSNAP, oid.hash,
		 info.pgid.pool());
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContext *obc = get_object_context(head, oloc, can_create);
    if (!obc)
      return -ENOENT;
    dout(10) << "find_object_context " << oid << " @" << oid.snap << dendl;
    *pobc = obc;

    if (can_create && !obc->ssc)
      obc->ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, true);

    return 0;
  }

  // we want a snap
  SnapSetContext *ssc = get_snapset_context(oid.oid, oid.get_key(), oid.hash, can_create);
  if (!ssc)
    return -ENOENT;

  dout(10) << "find_object_context " << oid << " @" << oid.snap
	   << " snapset " << ssc->snapset << dendl;
 
  // head?
  if (oid.snap > ssc->snapset.seq) {
    if (ssc->snapset.head_exists) {
      ObjectContext *obc = get_object_context(head, oloc, false);
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
		 info.pgid.pool());

  put_snapset_context(ssc); // we're done with ssc
  ssc = 0;

  if (missing.is_missing(soid)) {
    dout(20) << "find_object_context  " << soid << " missing, try again later" << dendl;
    if (psnapid)
      *psnapid = soid.snap;
    return -EAGAIN;
  }

  ObjectContext *obc = get_object_context(soid, oloc, false);

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
    put_object_context(obc);
    return -ENOENT;
  }
}

void ReplicatedPG::put_object_context(ObjectContext *obc)
{
  dout(10) << "put_object_context " << obc << " " << obc->obs.oi.soid << " "
	   << obc->ref << " -> " << (obc->ref-1) << dendl;

  if (mode.wake) {
    requeue_ops(mode.waiting);
    for (list<Cond*>::iterator p = mode.waiting_cond.begin(); p != mode.waiting_cond.end(); p++)
      (*p)->Signal();
    mode.wake = false;
  }

  --obc->ref;
  if (obc->ref == 0) {
    if (obc->ssc)
      put_snapset_context(obc->ssc);

    if (obc->registered)
      object_contexts.erase(obc->obs.oi.soid);
    delete obc;

    if (object_contexts.empty())
      kick();
  }
}

void ReplicatedPG::put_object_contexts(map<hobject_t,ObjectContext*>& obcv)
{
  if (obcv.empty())
    return;
  dout(10) << "put_object_contexts " << obcv << dendl;
  for (map<hobject_t,ObjectContext*>::iterator p = obcv.begin(); p != obcv.end(); ++p)
    put_object_context(p->second);
  obcv.clear();
}

void ReplicatedPG::add_object_context_to_pg_stat(ObjectContext *obc, pg_stat_t *pgstat)
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
				     false);

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

ReplicatedPG::SnapSetContext *ReplicatedPG::create_snapset_context(const object_t& oid)
{
  SnapSetContext *ssc = new SnapSetContext(oid);
  dout(10) << "create_snapset_context " << ssc << " " << ssc->oid << dendl;
  register_snapset_context(ssc);
  ssc->ref++;
  return ssc;
}

ReplicatedPG::SnapSetContext *ReplicatedPG::get_snapset_context(const object_t& oid,
								const string& key,
								ps_t seed,
								bool can_create)
{
  SnapSetContext *ssc;
  map<object_t, SnapSetContext*>::iterator p = snapset_contexts.find(oid);
  if (p != snapset_contexts.end()) {
    ssc = p->second;
  } else {
    bufferlist bv;
    hobject_t head(oid, key, CEPH_NOSNAP, seed,
		   info.pgid.pool());
    int r = osd->store->getattr(coll, head, SS_ATTR, bv);
    if (r < 0) {
      // try _snapset
      hobject_t snapdir(oid, key, CEPH_SNAPDIR, seed,
			info.pgid.pool());
      r = osd->store->getattr(coll, snapdir, SS_ATTR, bv);
      if (r < 0 && !can_create)
	return NULL;
    }
    ssc = new SnapSetContext(oid);
    register_snapset_context(ssc);
    if (r >= 0) {
      bufferlist::iterator bvp = bv.begin();
      ssc->snapset.decode(bvp);
    }
  }
  assert(ssc);
  dout(10) << "get_snapset_context " << ssc->oid << " "
	   << ssc->ref << " -> " << (ssc->ref+1) << dendl;
  ssc->ref++;
  return ssc;
}

void ReplicatedPG::put_snapset_context(SnapSetContext *ssc)
{
  dout(10) << "put_snapset_context " << ssc->oid << " "
	   << ssc->ref << " -> " << (ssc->ref-1) << dendl;

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
  MOSDSubOp *m = (MOSDSubOp*)op->request;
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
  assert(!missing.is_missing(soid));

  int ackerosd = acting[0];
  
  op->mark_started();

  RepModify *rm = new RepModify;
  rm->pg = this;
  get();
  rm->op = op;
  rm->ctx = 0;
  rm->ackerosd = ackerosd;
  rm->last_complete = info.last_complete;
  rm->epoch_started = get_osdmap()->get_epoch();

  if (!m->noop) {
    if (m->logbl.length()) {
      // shipped transaction and log entries
      vector<pg_log_entry_t> log;
      
      bufferlist::iterator p = m->get_data().begin();

      ::decode(rm->opt, p);
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
      
      info.stats = m->pg_stats;
      if (!rm->opt.empty()) {
	// If the opt is non-empty, we infer we are before
	// last_backfill (according to the primary, not our
	// not-quite-accurate value), and should update the
	// collections now.  Otherwise, we do it later on push.
	update_snap_collections(log, rm->localt);
      }
      append_log(log, m->pg_trim_to, rm->localt);

      rm->tls.push_back(&rm->localt);
      rm->tls.push_back(&rm->opt);

    } else {
      // do op
      assert(0);

      // TODO: this is severely broken because we don't know whether this object is really lost or
      // not. We just always assume that it's not right now.
      // Also, we're taking the address of a variable on the stack. 
      object_info_t oi(soid, m->oloc);
      oi.lost = false; // I guess?
      oi.version = m->old_version;
      oi.size = m->old_size;
      ObjectState obs(oi, m->old_exists);
      SnapSetContext ssc(m->poid.oid);
      
      rm->ctx = new OpContext(op, m->reqid, m->ops, &obs, &ssc, this);
      
      rm->ctx->mtime = m->mtime;
      rm->ctx->at_version = m->version;
      rm->ctx->snapc = m->snapc;

      ssc.snapset = m->snapset;
      rm->ctx->obc->ssc = &ssc;
      
      prepare_transaction(rm->ctx);
      append_log(rm->ctx->log, m->pg_trim_to, rm->ctx->local_t);
    
      rm->tls.push_back(&rm->ctx->op_t);
      rm->tls.push_back(&rm->ctx->local_t);
    }

    rm->bytes_written = rm->opt.get_encoded_bytes();

  } else {
    // just trim the log
    if (m->pg_trim_to != eversion_t()) {
      trim(rm->localt, m->pg_trim_to);
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

  if (rm->epoch_started >= last_peering_reset) {
    dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op->request << dendl;
    MOSDSubOp *m = (MOSDSubOp*)rm->op->request;
    assert(m->get_header().type == MSG_OSD_SUBOP);
    
    if (!rm->committed) {
      // send ack to acker only if we haven't sent a commit already
      MOSDSubOpReply *ack = new MOSDSubOpReply(m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
      ack->set_priority(CEPH_MSG_PRIO_HIGH); // this better match commit priority!
      osd->send_message_osd_cluster(rm->ackerosd, ack, get_osdmap()->get_epoch());
    }
    
    assert(info.last_update >= m->version);
    assert(last_update_applied < m->version);
    last_update_applied = m->version;
    if (scrubber.active_rep_scrub) {
      if (last_update_applied == scrubber.active_rep_scrub->scrub_to) {
	osd->rep_scrub_wq.queue(scrubber.active_rep_scrub);
	scrubber.active_rep_scrub = 0;
      }
    }
  } else {
    dout(10) << "sub_op_modify_applied on " << rm << " op " << *rm->op->request
	     << " from epoch " << rm->epoch_started << " < last_peering_reset "
	     << last_peering_reset << dendl;
  }

  bool done = rm->applied && rm->committed;
  unlock();
  if (done) {
    delete rm->ctx;
    delete rm;
    put();
  }
}

void ReplicatedPG::sub_op_modify_commit(RepModify *rm)
{
  lock();
  rm->op->mark_commit_sent();
  rm->committed = true;

  if (rm->epoch_started >= last_peering_reset) {
    // send commit.
    dout(10) << "sub_op_modify_commit on op " << *rm->op->request
	     << ", sending commit to osd." << rm->ackerosd
	     << dendl;
    
    if (get_osdmap()->is_up(rm->ackerosd)) {
      last_complete_ondisk = rm->last_complete;
      MOSDSubOpReply *commit = new MOSDSubOpReply((MOSDSubOp*)rm->op->request, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ONDISK);
      commit->set_last_complete_ondisk(rm->last_complete);
      commit->set_priority(CEPH_MSG_PRIO_HIGH); // this better match ack priority!
      osd->send_message_osd_cluster(rm->ackerosd, commit, get_osdmap()->get_epoch());
    }
  } else {
    dout(10) << "sub_op_modify_commit " << rm << " op " << *rm->op->request
	     << " from epoch " << rm->epoch_started << " < last_peering_reset "
	     << last_peering_reset << dendl;
  }
  
  log_subop_stats(rm->op, l_osd_sop_w_inb, l_osd_sop_w_lat);
  bool done = rm->applied && rm->committed;
  unlock();
  if (done) {
    delete rm->ctx;
    delete rm;
    put();
  }
}

void ReplicatedPG::sub_op_modify_reply(OpRequestRef op)
{
  MOSDSubOpReply *r = (MOSDSubOpReply*)op->request;
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

void ReplicatedPG::calc_head_subsets(ObjectContext *obc, SnapSet& snapset, const hobject_t& head,
				     pg_missing_t& missing,
				     const hobject_t &last_backfill,
				     interval_set<uint64_t>& data_subset,
				     map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  dout(10) << "calc_head_subsets " << head
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = obc->obs.oi.size;
  if (size)
    data_subset.insert(0, size);

  if (!g_conf->osd_recover_clone_overlap) {
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

  // what's left for us to push?
  data_subset.subtract(cloning);

  dout(10) << "calc_head_subsets " << head
	   << "  data_subset " << data_subset
	   << "  clone_subsets " << clone_subsets << dendl;
}

void ReplicatedPG::calc_clone_subsets(SnapSet& snapset, const hobject_t& soid,
				      pg_missing_t& missing,
				      const hobject_t &last_backfill,
				      interval_set<uint64_t>& data_subset,
				      map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  dout(10) << "calc_clone_subsets " << soid
	   << " clone_overlap " << snapset.clone_overlap << dendl;

  uint64_t size = snapset.clone_size[soid.snap];
  if (size)
    data_subset.insert(0, size);

  if (!g_conf->osd_recover_clone_overlap) {
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

int ReplicatedPG::pull(
  const hobject_t& soid, eversion_t v,
  int priority)
{
  int fromosd = -1;
  map<hobject_t,set<int> >::iterator q = missing_loc.find(soid);
  if (q != missing_loc.end()) {
    // randomize the list of possible sources
    // should we take weights into account?
    vector<int> shuffle(q->second.begin(), q->second.end());
    random_shuffle(shuffle.begin(), shuffle.end());
    for (vector<int>::iterator p = shuffle.begin();
	 p != shuffle.end();
	 p++) {
      if (get_osdmap()->is_up(*p)) {
	fromosd = *p;
	break;
      }
    }
  }
  if (fromosd < 0) {
    dout(7) << "pull " << soid
	    << " v " << v 
	    << " but it is unfound" << dendl;
    return PULL_NONE;
  }

  assert(peer_missing.count(fromosd));
  if (peer_missing[fromosd].is_missing(soid, v)) {
    assert(peer_missing[fromosd].missing[soid].have != v);
    dout(10) << "pulling soid " << soid << " from osd " << fromosd
	     << " at version " << peer_missing[fromosd].missing[soid].have
	     << " rather than at version " << v << dendl;
    v = peer_missing[fromosd].missing[soid].have;
    assert(log.objects.count(soid) &&
	   log.objects[soid]->op == pg_log_entry_t::LOST_REVERT &&
	   log.objects[soid]->reverting_to == v);
  }
  
  dout(7) << "pull " << soid
          << " v " << v 
	  << " on osds " << missing_loc[soid]
	  << " from osd." << fromosd
	  << dendl;

  ObjectRecoveryInfo recovery_info;

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head and/or snapdir?
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling head " << head << dendl;
	return PULL_NONE;
      } else {
	int r = pull(head, missing.missing[head].need, priority);
	if (r != PULL_NONE)
	  return PULL_OTHER;
	return PULL_NONE;
      }
    }
    head.snap = CEPH_SNAPDIR;
    if (missing.is_missing(head)) {
      if (pulling.count(head)) {
	dout(10) << " missing but already pulling snapdir " << head << dendl;
	return PULL_NONE;
      } else {
	int r = pull(head, missing.missing[head].need, priority);
	if (r != PULL_NONE)
	  return PULL_OTHER;
	return PULL_NONE;
      }
    }

    // check snapset
    SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
    dout(10) << " snapset " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, missing, info.last_backfill,
		       recovery_info.copy_subset,
		       recovery_info.clone_subset);
    put_snapset_context(ssc);
    // FIXME: this may overestimate if we are pulling multiple clones in parallel...
    dout(10) << " pulling " << recovery_info << dendl;
  } else {
    // pulling head or unversioned object.
    // always pull the whole thing.
    recovery_info.copy_subset.insert(0, (uint64_t)-1);
    recovery_info.size = ((uint64_t)-1);
  }


  recovery_info.soid = soid;
  recovery_info.version = v;
  ObjectRecoveryProgress progress;
  progress.data_complete = false;
  progress.omap_complete = false;
  progress.data_recovered_to = 0;
  progress.first = true;
  assert(!pulling.count(soid));
  pull_from_peer[fromosd].insert(soid);
  PullInfo &pi = pulling[soid];
  pi.recovery_info = recovery_info;
  pi.recovery_progress = progress;
  pi.priority = priority;
  send_pull(priority, fromosd, recovery_info, progress);

  start_recovery_op(soid);
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
void ReplicatedPG::push_to_replica(
  ObjectContext *obc, const hobject_t& soid, int peer,
  int prio)
{
  const object_info_t& oi = obc->obs.oi;
  uint64_t size = obc->obs.oi.size;

  dout(10) << "push_to_replica " << soid << " v" << oi.version << " size " << size << " to osd." << peer << dendl;

  map<hobject_t, interval_set<uint64_t> > clone_subsets;
  interval_set<uint64_t> data_subset;
  
  // are we doing a clone on the replica?
  if (soid.snap && soid.snap < CEPH_NOSNAP) {	
    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    // try to base push off of clones that succeed/preceed poid
    // we need the head (and current SnapSet) locally to do that.
    if (missing.is_missing(head)) {
      dout(15) << "push_to_replica missing head " << head << ", pushing raw clone" << dendl;
      return push_start(prio, obc, soid, peer);
    }
    hobject_t snapdir = head;
    snapdir.snap = CEPH_SNAPDIR;
    if (missing.is_missing(snapdir)) {
      dout(15) << "push_to_replica missing snapdir " << snapdir << ", pushing raw clone" << dendl;
      return push_start(prio, obc, soid, peer);
    }
    
    SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_clone_subsets(ssc->snapset, soid, peer_missing[peer],
		       peer_info[peer].last_backfill,
		       data_subset, clone_subsets);
    put_snapset_context(ssc);
  } else if (soid.snap == CEPH_NOSNAP) {
    // pushing head or unversioned object.
    // base this on partially on replica's clones?
    SnapSetContext *ssc = get_snapset_context(soid.oid, soid.get_key(), soid.hash, false);
    dout(15) << "push_to_replica snapset is " << ssc->snapset << dendl;
    calc_head_subsets(obc, ssc->snapset, soid, peer_missing[peer],
		      peer_info[peer].last_backfill,
		      data_subset, clone_subsets);
    put_snapset_context(ssc);
  }

  push_start(prio, obc, soid, peer, oi.version, data_subset, clone_subsets);
}

void ReplicatedPG::push_start(int prio,
			      ObjectContext *obc,
			      const hobject_t& soid, int peer)
{
  interval_set<uint64_t> data_subset;
  if (obc->obs.oi.size)
    data_subset.insert(0, obc->obs.oi.size);
  map<hobject_t, interval_set<uint64_t> > clone_subsets;

  push_start(prio, obc, soid, peer,
	     obc->obs.oi.version, data_subset, clone_subsets);
}

void ReplicatedPG::push_start(
  int prio,
  ObjectContext *obc,
  const hobject_t& soid, int peer,
  eversion_t version,
  interval_set<uint64_t> &data_subset,
  map<hobject_t, interval_set<uint64_t> >& clone_subsets)
{
  peer_missing[peer].revise_have(soid, eversion_t());
  // take note.
  PushInfo &pi = pushing[soid][peer];
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
  pi.priority = prio;

  ObjectRecoveryProgress new_progress;
  send_push(pi.priority,
	    peer, pi.recovery_info,
	    pi.recovery_progress, &new_progress);
  pi.recovery_progress = new_progress;
}

int ReplicatedPG::send_pull(int prio, int peer,
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

  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, recovery_info.soid,
				   false, CEPH_OSD_FLAG_ACK,
				   get_osdmap()->get_epoch(), tid,
				   recovery_info.version);
  subop->set_priority(prio);
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PULL;
  subop->ops[0].op.extent.length = g_conf->osd_recovery_max_chunk;
  subop->recovery_info = recovery_info;
  subop->recovery_progress = progress;

  osd->send_message_osd_cluster(peer, subop, get_osdmap()->get_epoch());

  osd->logger->inc(l_osd_pull);
  return 0;
}

void ReplicatedPG::submit_push_data(
  const ObjectRecoveryInfo &recovery_info,
  bool first,
  const interval_set<uint64_t> &intervals_included,
  bufferlist data_included,
  bufferlist omap_header,
  map<string, bufferptr> &attrs,
  map<string, bufferlist> &omap_entries,
  ObjectStore::Transaction *t)
{
  if (first) {
    missing.revise_have(recovery_info.soid, eversion_t());
    remove_object_with_snap_hardlinks(*t, recovery_info.soid);
    t->remove(get_temp_coll(t), recovery_info.soid);
    t->touch(get_temp_coll(t), recovery_info.soid);
    t->omap_setheader(get_temp_coll(t), recovery_info.soid, omap_header);
  }
  uint64_t off = 0;
  for (interval_set<uint64_t>::const_iterator p = intervals_included.begin();
       p != intervals_included.end();
       ++p) {
    bufferlist bit;
    bit.substr_of(data_included, off, p.get_len());
    t->write(get_temp_coll(t), recovery_info.soid,
	     p.get_start(), p.get_len(), bit);
    off += p.get_len();
  }

  t->omap_setkeys(get_temp_coll(t), recovery_info.soid,
		  omap_entries);
  t->setattrs(get_temp_coll(t), recovery_info.soid,
	      attrs);
}

void ReplicatedPG::submit_push_complete(ObjectRecoveryInfo &recovery_info,
					ObjectStore::Transaction *t)
{
  remove_object_with_snap_hardlinks(*t, recovery_info.soid);
  t->collection_move(coll, get_temp_coll(t), recovery_info.soid);
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

  if (recovery_info.soid.snap < CEPH_NOSNAP) {
    if (recovery_info.oi.snaps.size()) {
      coll_t lc = make_snap_collection(*t,
				       recovery_info.oi.snaps[0]);
      t->collection_add(lc, coll, recovery_info.soid);
      if (recovery_info.oi.snaps.size() > 1) {
	coll_t hc = make_snap_collection(
	  *t,
	  recovery_info.oi.snaps[recovery_info.oi.snaps.size()-1]);
	t->collection_add(hc, coll, recovery_info.soid);
      }
    }
  }

  if (missing.is_missing(recovery_info.soid) &&
      missing.missing[recovery_info.soid].need > recovery_info.version) {
    assert(is_primary());
    pg_log_entry_t *latest = log.objects[recovery_info.soid];
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
  recover_got(recovery_info.soid, recovery_info.version);

  // update pg
  write_info(*t);
}

ObjectRecoveryInfo ReplicatedPG::recalc_subsets(const ObjectRecoveryInfo& recovery_info)
{
  if (!recovery_info.soid.snap || recovery_info.soid.snap >= CEPH_NOSNAP)
    return recovery_info;

  SnapSetContext *ssc = get_snapset_context(recovery_info.soid.oid,
					    recovery_info.soid.get_key(),
					    recovery_info.soid.hash,
					    false);
  ObjectRecoveryInfo new_info = recovery_info;
  new_info.copy_subset.clear();
  new_info.clone_subset.clear();
  assert(ssc);
  calc_clone_subsets(ssc->snapset, new_info.soid, missing, info.last_backfill,
		     new_info.copy_subset, new_info.clone_subset);
  put_snapset_context(ssc);
  return new_info;
}

void ReplicatedPG::handle_pull_response(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp *)op->request;
  bufferlist data;
  m->claim_data(data);
  interval_set<uint64_t> data_included = m->data_included;
  dout(10) << "handle_pull_response "
	   << m->recovery_info
	   << m->recovery_progress
	   << " data.size() is " << data.length()
	   << " data_included: " << data_included
	   << dendl;
  if (m->version == eversion_t()) {
    // replica doesn't have it!
    _failed_push(op);
    return;
  }

  hobject_t &hoid = m->recovery_info.soid;
  assert((data_included.empty() && data.length() == 0) ||
	 (!data_included.empty() && data.length() > 0));

  if (!pulling.count(hoid)) {
    return;
  }

  PullInfo &pi = pulling[hoid];
  if (pi.recovery_info.size == (uint64_t(-1))) {
    pi.recovery_info.size = m->recovery_info.size;
    pi.recovery_info.copy_subset.intersection_of(
      m->recovery_info.copy_subset);
  }

  pi.recovery_info = recalc_subsets(pi.recovery_info);

  interval_set<uint64_t> usable_intervals;
  bufferlist usable_data;
  trim_pushed_data(pi.recovery_info.copy_subset,
		   data_included,
		   data,
		   &usable_intervals,
		   &usable_data);
  data_included = usable_intervals;
  data.claim(usable_data);

  info.stats.stats.sum.num_bytes_recovered += data.length();

  bool first = pi.recovery_progress.first;
  pi.recovery_progress = m->recovery_progress;

  dout(10) << "new recovery_info " << pi.recovery_info
	   << ", new progress " << pi.recovery_progress
	   << dendl;

  if (first) {
    bufferlist oibl;
    if (m->attrset.count(OI_ATTR)) {
      oibl.push_back(m->attrset[OI_ATTR]);
      ::decode(pi.recovery_info.oi, oibl);
    } else {
      assert(0);
    }
    bufferlist ssbl;
    if (m->attrset.count(SS_ATTR)) {
      ssbl.push_back(m->attrset[SS_ATTR]);
      ::decode(pi.recovery_info.ss, ssbl);
    } else {
      assert(pi.recovery_info.soid.snap != CEPH_NOSNAP &&
	     pi.recovery_info.soid.snap != CEPH_SNAPDIR);
    }
  }

  bool complete = pi.is_complete();

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  Context *onreadable = 0;
  Context *onreadable_sync = 0;
  Context *oncomplete = 0;
  submit_push_data(pi.recovery_info, first,
		   data_included, data,
		   m->omap_header,
		   m->attrset,
		   m->omap_entries,
		   t);

  info.stats.stats.sum.num_keys_recovered += m->omap_entries.size();

  if (complete) {
    submit_push_complete(pi.recovery_info, t);
    info.stats.stats.sum.num_objects_recovered++;

    SnapSetContext *ssc;
    if (hoid.snap == CEPH_NOSNAP || hoid.snap == CEPH_SNAPDIR) {
      ssc = create_snapset_context(hoid.oid);
      ssc->snapset = pi.recovery_info.ss;
    } else {
      ssc = get_snapset_context(hoid.oid, hoid.get_key(), hoid.hash, false);
      assert(ssc);
    }
    ObjectContext *obc = create_object_context(pi.recovery_info.oi, ssc);
    obc->obs.exists = true;

    obc->ondisk_write_lock();

    // keep track of active pushes for scrub
    ++active_pushes;

    onreadable = new C_OSD_AppliedRecoveredObject(this, t, obc);
    onreadable_sync = new C_OSD_OndiskWriteUnlock(obc);
    oncomplete = new C_OSD_CompletedPull(this, hoid, get_osdmap()->get_epoch());
  } else {
    onreadable = new ObjectStore::C_DeleteTransaction(t);
  }

  int r = osd->store->
    queue_transaction(
      osr.get(), t,
      onreadable,
      new C_OSD_CommittedPushedObject(this, op,
				      info.history.same_interval_since,
				      info.last_complete),
      onreadable_sync,
      oncomplete,
      TrackedOpRef()
      );
  assert(r == 0);

  if (complete) {
    pulling.erase(hoid);
    pull_from_peer[m->get_source().num()].erase(hoid);
    update_stats();
    if (waiting_for_missing_object.count(hoid)) {
      dout(20) << " kicking waiters on " << hoid << dendl;
      requeue_ops(waiting_for_missing_object[hoid]);
      waiting_for_missing_object.erase(hoid);
      if (missing.missing.size() == 0) {
	requeue_ops(waiting_for_all_missing);
	waiting_for_all_missing.clear();
      }
    }
  } else {
    send_pull(pi.priority,
	      m->get_source().num(),
	      pi.recovery_info,
	      pi.recovery_progress);
  }
}

void ReplicatedPG::handle_push(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp *)op->request;
  dout(10) << "handle_push "
	   << m->recovery_info
	   << m->recovery_progress
	   << dendl;
  bufferlist data;
  m->claim_data(data);
  bool first = m->current_progress.first;
  bool complete = m->recovery_progress.data_complete &&
    m->recovery_progress.omap_complete;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;

  // keep track of active pushes for scrub
  ++active_pushes;

  MOSDSubOpReply *reply = new MOSDSubOpReply(
    m, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  assert(entity_name_t::TYPE_OSD == m->get_connection()->peer_type);

  Context *oncomplete = new C_OSD_CompletedPushedObjectReplica(
    osd, reply, m->get_connection());
  Context *onreadable = new C_OSD_AppliedRecoveredObjectReplica(this, t);
  Context *onreadable_sync = 0;
  submit_push_data(m->recovery_info,
		   first,
		   m->data_included,
		   data,
		   m->omap_header,
		   m->attrset,
		   m->omap_entries,
		   t);
  if (complete)
    submit_push_complete(m->recovery_info,
			 t);

  int r = osd->store->
    queue_transaction(
      osr.get(), t,
      onreadable,
      new C_OSD_CommittedPushedObject(
	this, op,
	info.history.same_interval_since,
	info.last_complete),
      onreadable_sync,
      oncomplete,
      OpRequestRef()
      );
  assert(r == 0);

  osd->logger->inc(l_osd_push_in);
  osd->logger->inc(l_osd_push_inb, m->ops[0].indata.length());

}

int ReplicatedPG::send_push(int prio, int peer,
			    const ObjectRecoveryInfo &recovery_info,
			    ObjectRecoveryProgress progress,
			    ObjectRecoveryProgress *out_progress)
{
  ObjectRecoveryProgress new_progress = progress;

  tid_t tid = osd->get_tid();
  osd_reqid_t rid(osd->get_cluster_msgr_name(), 0, tid);
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, recovery_info.soid,
				   false, 0, get_osdmap()->get_epoch(),
				   tid, recovery_info.version);
  subop->set_priority(prio);

  dout(7) << "send_push_op " << recovery_info.soid
	  << " v " << recovery_info.version
	  << " size " << recovery_info.size
          << " to osd." << peer
	  << " recovery_info: " << recovery_info
          << dendl;

  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;

  if (progress.first) {
    osd->store->omap_get_header(coll, recovery_info.soid, &subop->omap_header);
    osd->store->getattrs(coll, recovery_info.soid, subop->attrset);

    // Debug
    bufferlist bv;
    bv.push_back(subop->attrset[OI_ATTR]);
    object_info_t oi(bv);

    if (oi.version != recovery_info.version) {
      osd->clog.error() << info.pgid << " push "
			<< recovery_info.soid << " v "
			<< recovery_info.version << " to osd." << peer
			<< " failed because local copy is "
			<< oi.version << "\n";
      subop->put();
      return -1;
    }

    new_progress.first = false;
  }

  uint64_t available = g_conf->osd_recovery_max_chunk;
  if (!progress.omap_complete) {
    ObjectMap::ObjectMapIterator iter =
      osd->store->get_omap_iterator(coll,
				    recovery_info.soid);
    for (iter->lower_bound(progress.omap_recovered_to);
	 iter->valid();
	 iter->next()) {
      if (!subop->omap_entries.empty() &&
	  available <= (iter->key().size() + iter->value().length()))
	break;
      subop->omap_entries.insert(make_pair(iter->key(), iter->value()));

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
    subop->data_included.span_of(recovery_info.copy_subset,
				 progress.data_recovered_to,
				 available);
  } else {
    subop->data_included.clear();
  }

  for (interval_set<uint64_t>::iterator p = subop->data_included.begin();
       p != subop->data_included.end();
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
    subop->ops[0].indata.claim_append(bit);
  }

  if (!subop->data_included.empty())
    new_progress.data_recovered_to = subop->data_included.range_end();

  if (new_progress.is_complete(recovery_info)) {
    new_progress.data_complete = true;
    info.stats.stats.sum.num_objects_recovered++;
  }

  info.stats.stats.sum.num_keys_recovered += subop->omap_entries.size();
  info.stats.stats.sum.num_bytes_recovered += subop->ops[0].indata.length();

  osd->logger->inc(l_osd_push);
  osd->logger->inc(l_osd_push_outb, subop->ops[0].indata.length());
  
  // send
  subop->recovery_info = recovery_info;
  subop->recovery_progress = new_progress;
  subop->current_progress = progress;
  osd->send_message_osd_cluster(peer, subop, get_osdmap()->get_epoch());
  if (out_progress)
    *out_progress = new_progress;
  return 0;
}

void ReplicatedPG::send_push_op_blank(const hobject_t& soid, int peer)
{
  // send a blank push back to the primary
  tid_t tid = osd->get_tid();
  osd_reqid_t rid(osd->get_cluster_msgr_name(), 0, tid);
  MOSDSubOp *subop = new MOSDSubOp(rid, info.pgid, soid, false, 0,
				   get_osdmap()->get_epoch(), tid, eversion_t());
  subop->ops = vector<OSDOp>(1);
  subop->ops[0].op.op = CEPH_OSD_OP_PUSH;
  subop->first = false;
  subop->complete = false;
  osd->send_message_osd_cluster(peer, subop, get_osdmap()->get_epoch());
}

void ReplicatedPG::sub_op_push_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = (MOSDSubOpReply*)op->request;
  assert(reply->get_header().type == MSG_OSD_SUBOPREPLY);
  dout(10) << "sub_op_push_reply from " << reply->get_source() << " " << *reply << dendl;

  op->mark_started();
  
  int peer = reply->get_source().num();
  const hobject_t& soid = reply->get_poid();
  
  if (pushing.count(soid) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << ", or anybody else"
	     << dendl;
  } else if (pushing[soid].count(peer) == 0) {
    dout(10) << "huh, i wasn't pushing " << soid << " to osd." << peer
	     << dendl;
  } else {
    PushInfo *pi = &pushing[soid][peer];

    if (!pi->recovery_progress.data_complete) {
      dout(10) << " pushing more from, "
	       << pi->recovery_progress.data_recovered_to
	       << " of " << pi->recovery_info.copy_subset << dendl;
      ObjectRecoveryProgress new_progress;
      send_push(
	pi->priority,
	peer, pi->recovery_info,
	pi->recovery_progress, &new_progress);
      pi->recovery_progress = new_progress;
    } else {
      // done!
      if (peer == backfill_target && backfills_in_flight.count(soid))
	backfills_in_flight.erase(soid);
      else
	peer_missing[peer].got(soid, pi->recovery_info.version);
      
      pushing[soid].erase(peer);
      pi = NULL;
      
      update_stats();
      
      if (pushing[soid].empty()) {
	pushing.erase(soid);
	dout(10) << "pushed " << soid << " to all replicas" << dendl;
	finish_recovery_op(soid);
	if (waiting_for_degraded_object.count(soid)) {
	  requeue_ops(waiting_for_degraded_object[soid]);
	  waiting_for_degraded_object.erase(soid);
	}
	finish_degraded_object(soid);
      } else {
	dout(10) << "pushed " << soid << ", still waiting for push ack from " 
		 << pushing[soid].size() << " others" << dendl;
      }
    }
  }
}

void ReplicatedPG::finish_degraded_object(const hobject_t& oid)
{
  dout(10) << "finish_degraded_object " << oid << dendl;
  map<hobject_t, ObjectContext *>::iterator i = object_contexts.find(oid);
  if (i != object_contexts.end()) {
    i->second->get();
    for (set<ObjectContext*>::iterator j = i->second->blocking.begin();
	 j != i->second->blocking.end();
	 i->second->blocking.erase(j++)) {
      dout(10) << " no longer blocking writes for " << (*j)->obs.oi.soid << dendl;
      (*j)->blocked_by = NULL;
      put_object_context(*j);
      put_object_context(i->second);
    }
    put_object_context(i->second);
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
void ReplicatedPG::sub_op_pull(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);

  op->mark_started();

  const hobject_t soid = m->poid;

  dout(7) << "op_pull " << soid << " v " << m->version
          << " from " << m->get_source()
          << dendl;

  assert(!is_primary());  // we should be a replica or stray.

  struct stat st;
  int r = osd->store->stat(coll, soid, &st);
  if (r != 0) {
    osd->clog.error() << info.pgid << " " << m->get_source() << " tried to pull " << soid
		      << " but got " << cpp_strerror(-r) << "\n";
    send_push_op_blank(soid, m->get_source().num());
  } else {
    ObjectRecoveryInfo recovery_info = m->recovery_info;
    ObjectRecoveryProgress progress = m->recovery_progress;
    if (progress.first && recovery_info.size == ((uint64_t)-1)) {
      // Adjust size and copy_subset
      recovery_info.size = st.st_size;
      recovery_info.copy_subset.clear();
      if (st.st_size)
	recovery_info.copy_subset.insert(0, st.st_size);
      assert(recovery_info.clone_subset.empty());
    }

    r = send_push(m->get_priority(),
		  m->get_source().num(),
		  recovery_info, progress);
    if (r < 0)
      send_push_op_blank(soid, m->get_source().num());
  }

  log_subop_stats(op, 0, l_osd_sop_pull_lat);
}


void ReplicatedPG::_committed_pushed_object(OpRequestRef op, epoch_t same_since, eversion_t last_complete)
{
  lock();
  if (same_since == info.history.same_interval_since) {
    dout(10) << "_committed_pushed_object last_complete " << last_complete << " now ondisk" << dendl;
    last_complete_ondisk = last_complete;

    if (last_complete_ondisk == info.last_update) {
      if (is_replica()) {
	// we are fully up to date.  tell the primary!
	osd->send_message_osd_cluster(get_primary(),
				      new MOSDPGTrim(get_osdmap()->get_epoch(), info.pgid,
						     last_complete_ondisk),
				      get_osdmap()->get_epoch());

	// adjust local snaps!
	adjust_local_snaps();
      } else if (is_primary()) {
	// we are the primary.  tell replicas to trim?
	if (calc_min_last_complete_ondisk())
	  trim_peers();
      }
    }

  } else {
    dout(10) << "_committed_pushed_object pg has changed, not touching last_complete_ondisk" << dendl;
  }

  if (op) {
    log_subop_stats(op, l_osd_sop_push_inb, l_osd_sop_push_lat);
    op->mark_event("committed");
  }

  unlock();
}

void ReplicatedPG::_applied_recovered_object(ObjectStore::Transaction *t, ObjectContext *obc)
{
  lock();
  dout(10) << "_applied_recovered_object " << *obc << dendl;
  put_object_context(obc);

  assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (active_pushes == 0 && scrubber.is_chunky_scrub_active()) {
    osd->scrub_wq.queue(this);
  }

  unlock();
  delete t;
}

void ReplicatedPG::_applied_recovered_object_replica(ObjectStore::Transaction *t)
{
  lock();
  dout(10) << "_applied_recovered_object_replica" << dendl;

  assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (active_pushes == 0 &&
      scrubber.active_rep_scrub && scrubber.active_rep_scrub->chunky) {
    osd->rep_scrub_wq.queue(scrubber.active_rep_scrub);
    scrubber.active_rep_scrub = 0;
  }

  unlock();
  delete t;
}

void ReplicatedPG::recover_got(hobject_t oid, eversion_t v)
{
  if (missing.is_missing(oid, v)) {
    dout(10) << "got missing " << oid << " v " << v << dendl;
    missing.got(oid, v);
    if (is_primary())
      missing_loc.erase(oid);
      
    // raise last_complete?
    if (missing.missing.empty()) {
      log.complete_to = log.log.end();
      info.last_complete = info.last_update;
    }
    while (log.complete_to != log.log.end()) {
      if (missing.missing[missing.rmissing.begin()->second].need <=
	  log.complete_to->version)
	break;
      if (info.last_complete < log.complete_to->version)
	info.last_complete = log.complete_to->version;
      log.complete_to++;
    }
    if (log.complete_to != log.log.end()) {
      dout(10) << "last_complete now " << info.last_complete
	       << " log.complete_to " << log.complete_to->version
	       << dendl;
    } else {
      dout(10) << "last_complete now " << info.last_complete
	       << " log.complete_to at end" << dendl;
      //below is not true in the repair case.
      //assert(missing.num_missing() == 0);  // otherwise, complete_to was wrong.
      assert(info.last_complete == info.last_update);
    }
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
void ReplicatedPG::trim_pushed_data(
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
void ReplicatedPG::sub_op_push(OpRequestRef op)
{
  op->mark_started();

  if (is_primary()) {
    handle_pull_response(op);
  } else {
    handle_push(op);
  }
  return;
}

void ReplicatedPG::_failed_push(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);
  const hobject_t& soid = m->poid;
  int from = m->get_source().num();
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
  pull_from_peer[from].erase(soid);
  pulling.erase(soid);
}

void ReplicatedPG::sub_op_remove(OpRequestRef op)
{
  MOSDSubOp *m = (MOSDSubOp*)op->request;
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_remove " << m->poid << dendl;

  op->mark_started();

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  remove_object_with_snap_hardlinks(*t, m->poid);
  int r = osd->store->queue_transaction(osr.get(), t);
  assert(r == 0);
}


eversion_t ReplicatedPG::pick_newest_available(const hobject_t& oid)
{
  eversion_t v;

  assert(missing.is_missing(oid));
  v = missing.missing[oid].have;
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
ReplicatedPG::ObjectContext *ReplicatedPG::mark_object_lost(ObjectStore::Transaction *t,
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
  pg_log_entry_t e(what, oid, info.last_update, version, osd_reqid_t(), mtime);
  log.add(e);
  
  object_locator_t oloc;
  oloc.pool = info.pgid.pool();
  oloc.key = oid.get_key();
  ObjectContext *obc = get_object_context(oid, oloc, true);

  obc->ondisk_write_lock();

  obc->obs.oi.lost = true;
  obc->obs.oi.version = info.last_update;
  obc->obs.oi.prior_version = version;

  bufferlist b2;
  obc->obs.oi.encode(b2);
  t->setattr(coll, oid, OI_ATTR, b2);

  return obc;
}

struct C_PG_MarkUnfoundLost : public Context {
  ReplicatedPG *pg;
  list<ReplicatedPG::ObjectContext*> obcs;
  C_PG_MarkUnfoundLost(ReplicatedPG *p) : pg(p) {
    pg->get();
  }
  void finish(int r) {
    pg->_finish_mark_all_unfound_lost(obcs);
    pg->put();
  }
};

/* Mark all unfound objects as lost.
 */
void ReplicatedPG::mark_all_unfound_lost(int what)
{
  dout(3) << __func__ << " " << pg_log_entry_t::get_op_name(what) << dendl;

  dout(30) << __func__ << ": log before:\n";
  log.print(*_dout);
  *_dout << dendl;

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  C_PG_MarkUnfoundLost *c = new C_PG_MarkUnfoundLost(this);

  utime_t mtime = ceph_clock_now(g_ceph_context);
  info.last_update.epoch = get_osdmap()->get_epoch();
  map<hobject_t, pg_missing_t::item>::iterator m = missing.missing.begin();
  map<hobject_t, pg_missing_t::item>::iterator mend = missing.missing.end();
  while (m != mend) {
    const hobject_t &oid(m->first);
    if (missing_loc.find(oid) != missing_loc.end()) {
      // We only care about unfound objects
      ++m;
      continue;
    }

    ObjectContext *obc = NULL;
    eversion_t prev;

    switch (what) {
    case pg_log_entry_t::LOST_MARK:
      obc = mark_object_lost(t, oid, m->second.need, mtime, pg_log_entry_t::LOST_MARK);
      missing.got(m++);
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
	  m->second.need, osd_reqid_t(), mtime);
	e.reverting_to = prev;
	log.add(e);
	dout(10) << e << dendl;

	// we are now missing the new version; recovery code will sort it out.
	m++;
	missing.revise_need(oid, info.last_update);
	break;
      }
      /** fall-thru **/

    case pg_log_entry_t::LOST_DELETE:
      {
	// log it
      	++info.last_update.version;
	pg_log_entry_t e(pg_log_entry_t::LOST_DELETE, oid, info.last_update, m->second.need,
		     osd_reqid_t(), mtime);
	log.add(e);
	dout(10) << e << dendl;

	// delete local copy?  NOT YET!  FIXME
	if (m->second.have != eversion_t()) {
	  assert(0 == "not implemented.. tho i'm not sure how useful it really would be.");
	}
	missing.rm(m++);
      }
      break;

    default:
      assert(0);
    }

    if (obc)
      c->obcs.push_back(obc);
  }

  dout(30) << __func__ << ": log after:\n";
  log.print(*_dout);
  *_dout << dendl;

  if (missing.num_missing() == 0) {
    // advance last_complete since nothing else is missing!
    info.last_complete = info.last_update;
    write_info(*t);
  }

  osd->store->queue_transaction(osr.get(), t, c, NULL, new C_OSD_OndiskWriteUnlockList(&c->obcs));
	      
  // Send out the PG log to all replicas
  // So that they know what is lost
  share_pg_log();

  // queue ourselves so that we push the (now-lost) object_infos to replicas.
  osd->queue_for_recovery(this);
}

void ReplicatedPG::_finish_mark_all_unfound_lost(list<ObjectContext*>& obcs)
{
  lock();
  dout(10) << "_finish_mark_all_unfound_lost " << dendl;

  requeue_ops(waiting_for_all_missing);
  waiting_for_all_missing.clear();

  while (!obcs.empty()) {
    ObjectContext *obc = obcs.front();
    put_object_context(obc);
    obcs.pop_front();
  }
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
	dout(10) << " requeuing " << *repop->ctx->op->request << dendl;
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

void ReplicatedPG::on_removal()
{
  dout(10) << "on_removal" << dendl;
  apply_and_flush_repops(false);
  remove_watchers_and_notifies();
}

void ReplicatedPG::on_shutdown()
{
  dout(10) << "on_shutdown" << dendl;
  apply_and_flush_repops(false);
  remove_watchers_and_notifies();
}

void ReplicatedPG::on_activate()
{
  for (unsigned i = 1; i<acting.size(); i++) {
    if (peer_info[acting[i]].last_backfill != hobject_t::get_max()) {
      assert(backfill_target == -1);
      backfill_target = acting[i];
      backfill_pos = peer_info[acting[i]].last_backfill;
      dout(10) << " chose backfill target osd." << backfill_target
	       << " from " << backfill_pos << dendl;
    }
  }
}

void ReplicatedPG::on_change()
{
  dout(10) << "on_change" << dendl;

  // requeue everything in the reverse order they should be
  // reexamined.
  requeue_ops(waiting_for_map);

  clear_scrub_reserved();
  scrub_clear_state();

  context_registry_on_change();

  // requeue object waiters
  requeue_ops(waiting_for_backfill_pos);
  requeue_object_waiters(waiting_for_missing_object);
  for (map<hobject_t,list<OpRequestRef> >::iterator p = waiting_for_degraded_object.begin();
       p != waiting_for_degraded_object.end();
       waiting_for_degraded_object.erase(p++)) {
    requeue_ops(p->second);
    finish_degraded_object(p->first);
  }

  requeue_ops(waiting_for_all_missing);
  waiting_for_all_missing.clear();

  // this will requeue ops we were working on but didn't finish, and
  // any dups
  apply_and_flush_repops(is_primary());

  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
  pull_from_peer.clear();

  // clear snap_trimmer state
  snap_trimmer_machine.process_event(Reset());
}

void ReplicatedPG::on_role_change()
{
  dout(10) << "on_role_change" << dendl;
}


// clear state.  called on recovery completion AND cancellation.
void ReplicatedPG::_clear_recovery_state()
{
  missing_loc.clear();
  missing_loc_sources.clear();
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
  backfill_pos = hobject_t();
  backfills_in_flight.clear();
  pending_backfill_updates.clear();
  pulling.clear();
  pushing.clear();
  pull_from_peer.clear();
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
      p++;
      continue;
    }
    dout(10) << "check_recovery_sources source osd." << *p << " now down" << dendl;
    now_down.insert(*p);

    // reset pulls?
    map<int, set<hobject_t> >::iterator j = pull_from_peer.find(*p);
    if (j != pull_from_peer.end()) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << *p
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t>::iterator i = j->second.begin();
	   i != j->second.end();
	   ++i) {
	assert(pulling.count(*i) == 1);
	pulling.erase(*i);
	finish_recovery_op(*i);
      }
      log.last_requested = 0;
      pull_from_peer.erase(j++);
    }

    // remove from missing_loc_sources
    missing_loc_sources.erase(p++);
  }
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
	  q++;
	}
      if (p->second.empty())
	missing_loc.erase(p++);
      else
	p++;
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
  

int ReplicatedPG::start_recovery_ops(int max, RecoveryCtx *prctx)
{
  int started = 0;
  assert(is_primary());

  if (!state_test(PG_STATE_RECOVERING) &&
      !state_test(PG_STATE_BACKFILL)) {
    dout(10) << "recovery raced and were queued twice, ignoring!" << dendl;
    return 0;
  }

  int num_missing = missing.num_missing();
  int num_unfound = get_num_unfound();

  if (num_missing == 0) {
    info.last_complete = info.last_update;
  }

  if (num_missing == num_unfound) {
    // All of the missing objects we have are unfound.
    // Recover the replicas.
    started = recover_replicas(max);
  }
  if (!started) {
    // We still have missing objects that we should grab from replicas.
    started += recover_primary(max);
  }
  if (!started && num_unfound != get_num_unfound()) {
    // second chance to recovery replicas
    started = recover_replicas(max);
  }

  bool deferred_backfill = false;
  if (state_test(PG_STATE_BACKFILL) &&
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
      started += recover_backfill(max - started);
    }
  }

  dout(10) << " started " << started << dendl;
  osd->logger->inc(l_osd_rop, started);

  if (started || recovery_ops_active > 0 || deferred_backfill)
    return started;

  assert(recovery_ops_active == 0);

  int unfound = get_num_unfound();
  if (unfound) {
    dout(10) << " still have " << unfound << " unfound" << dendl;
    return started;
  }

  if (missing.num_missing() > 0) {
    // this shouldn't happen!
    osd->clog.error() << info.pgid << " recovery ending with " << missing.num_missing()
		      << ": " << missing.missing << "\n";
    return started;
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
int ReplicatedPG::recover_primary(int max)
{
  assert(is_primary());

  dout(10) << "recover_primary pulling " << pulling.size() << " in pg" << dendl;
  dout(10) << "recover_primary " << missing << dendl;
  dout(25) << "recover_primary " << missing.missing << dendl;

  // look at log!
  pg_log_entry_t *latest = 0;
  int started = 0;
  int skipped = 0;

  map<version_t, hobject_t>::iterator p = missing.rmissing.lower_bound(log.last_requested);
  while (p != missing.rmissing.end()) {
    hobject_t soid;
    version_t v = p->first;

    if (log.objects.count(p->second)) {
      latest = log.objects[p->second];
      assert(latest->is_update());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    pg_missing_t::item& item = missing.missing[p->second];
    p++;

    hobject_t head = soid;
    head.snap = CEPH_NOSNAP;

    eversion_t need = item.need;

    bool unfound = (missing_loc.find(soid) == missing_loc.end());

    dout(10) << "recover_primary "
             << soid << " " << item.need
	     << (unfound ? " (unfound)":"")
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (pulling.count(soid) ? " (pulling)":"")
	     << (pulling.count(head) ? " (pulling head)":"")
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
	    // I have it locally.  Revert.
	    object_locator_t oloc;
	    oloc.pool = info.pgid.pool();
	    oloc.key = soid.get_key();
	    ObjectContext *obc = get_object_context(soid, oloc, true);
	    
	    if (obc->obs.oi.version == latest->version) {
	      // I'm already reverting
	      dout(10) << " already reverting " << soid << dendl;
	    } else {
	      dout(10) << " reverting " << soid << " to " << latest->prior_version << dendl;
	      obc->ondisk_write_lock();
	      obc->obs.oi.version = latest->version;

	      ObjectStore::Transaction *t = new ObjectStore::Transaction;
	      bufferlist b2;
	      obc->obs.oi.encode(b2);
	      t->setattr(coll, soid, OI_ATTR, b2);

	      recover_got(soid, latest->version);

	      ++active_pushes;

	      osd->store->queue_transaction(osr.get(), t,
					    new C_OSD_AppliedRecoveredObject(this, t, obc),
					    new C_OSD_CommittedPushedObject(this, OpRequestRef(),
									    info.history.same_interval_since,
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
   
    if (!pulling.count(soid)) {
      if (pulling.count(head)) {
	++skipped;
      } else if (unfound) {
	++skipped;
      } else {
	int r = pull(soid, need, g_conf->osd_recovery_op_priority);
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
	  return started;
      }
    }
    
    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      log.last_requested = v;
  }

  return started;
}

int ReplicatedPG::recover_object_replicas(
  const hobject_t& soid, eversion_t v, int prio)
{
  dout(10) << "recover_object_replicas " << soid << dendl;

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContext *obc = get_object_context(soid, OLOC_BLANK, false);
  if (!obc) {
    missing.add(soid, v, eversion_t());
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

  dout(10) << " ondisk_read_lock for " << soid << dendl;
  obc->ondisk_read_lock();
  
  // who needs it?  
  bool started = false;
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    if (peer_missing.count(peer) &&
	peer_missing[peer].is_missing(soid)) {
      if (!started) {
	start_recovery_op(soid);
	started = true;
      }
      push_to_replica(obc, soid, peer, prio);
    }
  }
  
  dout(10) << " ondisk_read_unlock on " << soid << dendl;
  obc->ondisk_read_unlock();
  put_object_context(obc);

  return 1;
}

int ReplicatedPG::recover_replicas(int max)
{
  dout(10) << __func__ << "(" << max << ")" << dendl;
  int started = 0;

  // this is FAR from an optimal recovery order.  pretty lame, really.
  for (unsigned i=1; i<acting.size(); i++) {
    int peer = acting[i];
    map<int, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    assert(pm != peer_missing.end());
    size_t m_sz = pm->second.num_missing();

    dout(10) << " peer osd." << peer << " missing " << m_sz << " objects." << dendl;
    dout(20) << " peer osd." << peer << " missing " << pm->second.missing << dendl;

    // oldest first!
    const pg_missing_t &m(pm->second);
    for (map<version_t, hobject_t>::const_iterator p = m.rmissing.begin();
	   p != m.rmissing.end() && started < max;
	   ++p) {
      const hobject_t soid(p->second);

      if (pushing.count(soid)) {
	dout(10) << __func__ << ": already pushing " << soid << dendl;
	continue;
      }

      if (missing.is_missing(soid)) {
	if (missing_loc.find(soid) == missing_loc.end())
	  dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	else
	  dout(10) << __func__ << ": " << soid << " still missing on primary" << dendl;
	continue;
      }

      dout(10) << __func__ << ": recover_object_replicas(" << soid << ")" << dendl;
      map<hobject_t,pg_missing_t::item>::const_iterator r = m.missing.find(soid);
      started += recover_object_replicas(soid, r->second.need,
					 g_conf->osd_recovery_op_priority);
    }
  }

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
 * peer_info[backfill_target].last_backfill = MIN(peer_backfill_info.begin,
 * backfill_info.begin, backfills_in_flight)
 */
int ReplicatedPG::recover_backfill(int max)
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
	   << " pos " << backfill_pos
	   << " info " << pinfo
	   << " interval " << pbi.begin << "-" << pbi.end
	   << " " << pbi.objects.size() << " objects" << dendl;

  int local_min = osd->store->get_ideal_list_min();
  int local_max = osd->store->get_ideal_list_max();

  // re-scan our local interval to cope with recent changes
  // FIXME: we could track the eversion_t when we last scanned, and invalidate
  // that way.  or explicitly modify/invalidate when we actually change specific
  // objects.
  dout(10) << " rescanning local backfill_info from " << backfill_pos << dendl;
  backfill_info.clear();
  osr->flush();
  scan_range(backfill_pos, local_min, local_max, &backfill_info);

  int ops = 0;
  map<hobject_t, pair<eversion_t, eversion_t> > to_push;
  map<hobject_t, eversion_t> to_remove;
  set<hobject_t> add_to_stat;

  pbi.trim();
  backfill_info.trim();

  while (ops < max) {
    if (backfill_info.begin <= pbi.begin &&
	!backfill_info.extends_to_end() && backfill_info.empty()) {
      osr->flush();
      scan_range(backfill_info.end, local_min, local_max, &backfill_info);
      backfill_info.trim();
    }
    backfill_pos = backfill_info.begin > pbi.begin ? pbi.begin : backfill_info.begin;

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
      pbi.pop_front();
      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
    } else if (pbi.begin == backfill_info.begin) {
      if (pbi.objects.begin()->second !=
	  backfill_info.objects.begin()->second) {
	dout(20) << " replacing peer " << pbi.begin << " with local "
		 << backfill_info.objects.begin()->second << dendl;
	to_push[pbi.begin] = make_pair(backfill_info.objects.begin()->second,
				       pbi.objects.begin()->second);
	ops++;
      } else {
	dout(20) << " keeping peer " << pbi.begin << " "
		 << pbi.objects.begin()->second << dendl;
	// Object was degraded, but won't be recovered
	if (waiting_for_degraded_object.count(pbi.begin)) {
	  requeue_ops(
	    waiting_for_degraded_object[pbi.begin]);
	  waiting_for_degraded_object.erase(pbi.begin);
	}
      }
      add_to_stat.insert(pbi.begin);
      backfill_info.pop_front();
      pbi.pop_front();
    } else {
      dout(20) << " pushing local " << backfill_info.begin << " "
	       << backfill_info.objects.begin()->second
	       << " to peer osd." << backfill_target << dendl;
      to_push[backfill_info.begin] =
	make_pair(backfill_info.objects.begin()->second,
		  eversion_t());
      add_to_stat.insert(backfill_info.begin);
      backfill_info.pop_front();
      ops++;
    }
  }
  backfill_pos = backfill_info.begin > pbi.begin ? pbi.begin : backfill_info.begin;

  for (set<hobject_t>::iterator i = add_to_stat.begin();
       i != add_to_stat.end();
       ++i) {
    ObjectContext *obc = get_object_context(*i, OLOC_BLANK, false);
    pg_stat_t stat;
    add_object_context_to_pg_stat(obc, &stat);
    pending_backfill_updates[*i] = stat;
    put_object_context(obc);
  }
  for (map<hobject_t, eversion_t>::iterator i = to_remove.begin();
       i != to_remove.end();
       ++i) {
    send_remove_op(i->first, i->second, backfill_target);
  }
  for (map<hobject_t, pair<eversion_t, eversion_t> >::iterator i = to_push.begin();
       i != to_push.end();
       ++i) {
    push_backfill_object(i->first, i->second.first, i->second.second, backfill_target);
  }

  release_waiting_for_backfill_pos();
  dout(5) << "backfill_pos is " << backfill_pos << " and pinfo.last_backfill is "
	  << pinfo.last_backfill << dendl;
  for (set<hobject_t>::iterator i = backfills_in_flight.begin();
       i != backfills_in_flight.end();
       ++i) {
    dout(20) << *i << " is still in flight" << dendl;
  }

  hobject_t bound = backfills_in_flight.size() ?
    *(backfills_in_flight.begin()) : backfill_pos;
  if (bound > pinfo.last_backfill) {
    pinfo.last_backfill = bound;
    for (map<hobject_t, pg_stat_t>::iterator i = pending_backfill_updates.begin();
	 i != pending_backfill_updates.end() && i->first < bound;
	 pending_backfill_updates.erase(i++)) {
      pinfo.stats.add(i->second);
    }
    epoch_t e = get_osdmap()->get_epoch();
    MOSDPGBackfill *m = NULL;
    if (bound.is_max()) {
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
    m->last_backfill = bound;
    m->stats = pinfo.stats.stats;
    osd->send_message_osd_cluster(backfill_target, m, get_osdmap()->get_epoch());
  }

  dout(10) << " peer num_objects now " << pinfo.stats.stats.sum.num_objects
	   << " / " << info.stats.stats.sum.num_objects << dendl;
  return ops;
}

void ReplicatedPG::push_backfill_object(hobject_t oid, eversion_t v, eversion_t have, int peer)
{
  dout(10) << "push_backfill_object " << oid << " v " << v << " to osd." << peer << dendl;

  backfills_in_flight.insert(oid);

  if (!pushing.count(oid))
    start_recovery_op(oid);
  ObjectContext *obc = get_object_context(oid, OLOC_BLANK, false);
  obc->ondisk_read_lock();
  push_to_replica(obc, oid, peer, g_conf->osd_recovery_op_priority);
  obc->ondisk_read_unlock();
  put_object_context(obc);
}

void ReplicatedPG::scan_range(hobject_t begin, int min, int max, BackfillInterval *bi)
{
  assert(is_locked());
  dout(10) << "scan_range from " << begin << dendl;
  bi->begin = begin;
  bi->objects.clear();  // for good measure

  vector<hobject_t> ls;
  ls.reserve(max);
  int r = osd->store->collection_list_partial(coll, begin, min, max,
					      0, &ls, &bi->end);
  assert(r >= 0);
  dout(10) << " got " << ls.size() << " items, next " << bi->end << dendl;
  dout(20) << ls << dendl;

  for (vector<hobject_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    ObjectContext *obc = NULL;
    if (is_primary())
      obc = _lookup_object_context(*p);
    if (obc) {
      bi->objects[*p] = obc->obs.oi.version;
      dout(20) << "  " << *p << " " << obc->obs.oi.version << dendl;
    } else {
      bufferlist bl;
      int r = osd->store->getattr(coll, *p, OI_ATTR, bl);
      assert(r >= 0);
      object_info_t oi(bl);
      bi->objects[*p] = oi.version;
      dout(20) << "  " << *p << " " << oi.version << dendl;
    }
  }
}


/** clean_up_local
 * remove any objects that we're storing but shouldn't.
 * as determined by log.
 */
void ReplicatedPG::clean_up_local(ObjectStore::Transaction& t)
{
  dout(10) << "clean_up_local" << dendl;

  assert(info.last_update >= log.tail);  // otherwise we need some help!

  // just scan the log.
  set<hobject_t> did;
  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend();
       p++) {
    if (did.count(p->soid))
      continue;
    did.insert(p->soid);

    if (p->is_delete()) {
      dout(10) << " deleting " << p->soid
	       << " when " << p->version << dendl;
      remove_object_with_snap_hardlinks(t, p->soid);
    } else {
      // keep old(+missing) objects, just for kicks.
    }
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
       p++) {
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
        ++scrubber.errors;
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
        ++scrubber.errors;
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
      ++scrubber.errors;
      continue;
    }
    bufferlist bv;
    bv.push_back(p->second.attrs[OI_ATTR]);
    object_info_t oi(bv);

    if (oi.size != p->second.size) {
      osd->clog.error() << mode << " " << info.pgid << " " << soid
			<< " on disk size (" << p->second.size
			<< ") does not match object info size (" << oi.size << ")";
      ++scrubber.errors;
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
        ++scrubber.errors;
	continue;
      }
    } else if (soid.snap) {
      // it's a clone
      stat.num_object_clones++;
      
      if (head == hobject_t()) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " found clone without head";
	++scrubber.errors;
	continue;
      }

      if (soid.snap != *curclone) {
	osd->clog.error() << mode << " " << info.pgid << " " << soid
			  << " expected clone " << *curclone;
        ++scrubber.errors;
	assert(soid.snap == *curclone);
      }

      assert(p->second.size == snapset.clone_size[*curclone]);

      // verify overlap?
      // ...

      // what's next?
      if (curclone != snapset.clones.rend())
	curclone++;

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
    ++scrubber.errors;

    if (repair) {
      ++scrubber.fixed;
      info.stats.stats = scrub_cstat;
      update_stats();
      share_pg_info();
    }
  }
}

static set<snapid_t> get_expected_snap_colls(
  const map<string, bufferptr> &attrs,
  object_info_t *oi = 0)
{
  object_info_t _oi;
  if (!oi)
    oi = &_oi;

  set<snapid_t> to_check;
  map<string, bufferptr>::const_iterator oiiter = attrs.find(OI_ATTR);
  if (oiiter == attrs.end())
    return to_check;

  bufferlist oiattr;
  oiattr.push_back(oiiter->second);
  *oi = object_info_t(oiattr);
  if (oi->snaps.size() > 0)
    to_check.insert(*(oi->snaps.begin()));
  if (oi->snaps.size() > 1)
    to_check.insert(*(oi->snaps.rbegin()));
  return to_check;
}

bool ReplicatedPG::_report_snap_collection_errors(
  const hobject_t &hoid,
  int osd,
  const map<string, bufferptr> &attrs,
  const set<snapid_t> &snapcolls,
  uint32_t nlinks,
  ostream &out)
{
  if (nlinks == 0)
    return false; // replica didn't encode snap_collection information
  bool errors = false;
  set<snapid_t> to_check = get_expected_snap_colls(attrs);
  if (to_check != snapcolls) {
    out << info.pgid << " osd." << osd << " inconsistent snapcolls on "
	<< hoid << " found " << snapcolls << " expected " << to_check
	<< std::endl;
    errors = true;
  }
  if (nlinks != snapcolls.size() + 1) {
    out << info.pgid << " osd." << osd << " unaccounted for links on object "
	<< hoid << " snapcolls " << snapcolls << " nlinks " << nlinks
	<< std::endl;
    errors = true;
  }
  return errors;
}

void ReplicatedPG::check_snap_collections(
  ino_t hino,
  const hobject_t &hoid,
  const map<string, bufferptr> &attrs,
  set<snapid_t> *snapcolls)
{
  object_info_t oi;
  set<snapid_t> to_check = get_expected_snap_colls(attrs, &oi);

  for (set<snapid_t>::iterator i = to_check.begin(); i != to_check.end(); ++i) {
    struct stat st;
    int r = osd->store->stat(coll_t(info.pgid, *i), hoid, &st);
    if (r == -ENOENT) {
    } else if (r == 0) {
      if (hino == st.st_ino) {
	snapcolls->insert(*i);
      }
    } else {
      assert(0);
    }
  }
}

/*---SnapTrimmer Logging---*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

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
ReplicatedPG::NotTrimming::NotTrimming(my_context ctx) : my_base(ctx)
{
  state_name = "NotTrimming";
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

  // Replica trimming
  if (pg->is_replica() && pg->is_active() &&
      pg->last_complete_ondisk.epoch >= pg->info.history.last_epoch_started) {
    return transit<RepColTrim>();
  } else if (!pg->is_primary() || !pg->is_active() || !pg->is_clean()) {
    dout(10) << "NotTrimming not primary, active, clean" << dendl;
    return discard_event();
  } else if (pg->scrubber.active) {
    dout(10) << "NotTrimming finalizing scrub" << dendl;
    pg->queue_snap_trim();
    return discard_event();
  }

  // Primary trimming
  vector<hobject_t> &obs_to_trim = context<SnapTrimmer>().obs_to_trim;
  snapid_t &snap_to_trim = context<SnapTrimmer>().snap_to_trim;
  coll_t &col_to_trim = context<SnapTrimmer>().col_to_trim;
  if (!pg->get_obs_to_trim(snap_to_trim,
			   col_to_trim,
			   obs_to_trim)) {
    // Nothing to trim
    dout(10) << "NotTrimming: nothing to trim" << dendl;
    return discard_event();
  }

  if (obs_to_trim.empty()) {
    // Nothing to actually trim, just update info and try again
    pg->info.purged_snaps.insert(snap_to_trim);
    pg->snap_trimq.erase(snap_to_trim);
    dout(10) << "NotTrimming: obs_to_trim empty!" << dendl;
    dout(10) << "purged_snaps now " << pg->info.purged_snaps << ", snap_trimq now " 
	     << pg->snap_trimq << dendl;
    if (pg->snap_collections.contains(snap_to_trim)) {
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      pg->snap_collections.erase(snap_to_trim);
      t->remove_collection(col_to_trim);
      pg->write_info(*t);
      int r = pg->osd->store->queue_transaction(
	NULL, t, new ObjectStore::C_DeleteTransaction(t));
      assert(r == 0);
    }
    post_event(SnapTrim());
    return discard_event();
  } else {
    // Actually have things to trim!
    dout(10) << "NotTrimming: something to trim!" << dendl;
    return transit< TrimmingObjects >();
  }
}

/* RepColTrim */
ReplicatedPG::RepColTrim::RepColTrim(my_context ctx) : my_base(ctx)
{
  state_name = "RepColTrim";
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::RepColTrim::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result ReplicatedPG::RepColTrim::react(const SnapTrim&)
{
  dout(10) << "RepColTrim react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;

  if (to_trim.empty()) {
    to_trim.intersection_of(pg->info.purged_snaps, pg->snap_collections);
    if (to_trim.empty()) {
      return transit<NotTrimming>();
    }
  }

  snapid_t snap_to_trim(to_trim.range_start());
  coll_t col_to_trim(pg->info.pgid, snap_to_trim);
  to_trim.erase(snap_to_trim);
  
  // flush all operations to fs so we can rely on collection_list
  // below.
  pg->osr->flush();

  vector<hobject_t> obs_to_trim;
  pg->osd->store->collection_list(col_to_trim, obs_to_trim);
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<hobject_t>::iterator i = obs_to_trim.begin();
       i != obs_to_trim.end();
       ++i) {
    t->collection_remove(col_to_trim, *i);
  }
  t->remove_collection(col_to_trim);
  pg->snap_collections.erase(snap_to_trim);
  pg->write_info(*t);
  int r = pg->osd->store->queue_transaction(NULL, t, new ObjectStore::C_DeleteTransaction(t));
  assert(r == 0);
  return discard_event();
}

/* TrimmingObjects */
ReplicatedPG::TrimmingObjects::TrimmingObjects(my_context ctx) : my_base(ctx)
{
  state_name = "Trimming/TrimmingObjects";
  position = context<SnapTrimmer>().obs_to_trim.begin();
  assert(position != context<SnapTrimmer>().obs_to_trim.end()); // Would not have transitioned if coll empty
  context< SnapTrimmer >().log_enter(state_name);
}

void ReplicatedPG::TrimmingObjects::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result ReplicatedPG::TrimmingObjects::react(const SnapTrim&)
{
  dout(10) << "TrimmingObjects react" << dendl;
  ReplicatedPG *pg = context< SnapTrimmer >().pg;
  vector<hobject_t> &obs_to_trim = context<SnapTrimmer>().obs_to_trim;
  snapid_t &snap_to_trim = context<SnapTrimmer>().snap_to_trim;
  set<RepGather *> &repops = context<SnapTrimmer>().repops;

  // Done, 
  if (position == obs_to_trim.end()) {
    post_event(SnapTrim());
    return transit< WaitingOnReplicas >();
  }

  dout(10) << "TrimmingObjects react trimming " << *position << dendl;
  RepGather *repop = pg->trim_object(*position, snap_to_trim);

  if (repop) {
    repop->queue_snap_trimmer = true;
    eversion_t old_last_update = pg->log.head;
    bool old_exists = repop->obc->obs.exists;
    uint64_t old_size = repop->obc->obs.oi.size;
    eversion_t old_version = repop->obc->obs.oi.version;
    
    pg->append_log(repop->ctx->log, eversion_t(), repop->ctx->local_t);
    pg->issue_repop(repop, repop->ctx->mtime, old_last_update, old_exists, old_size, old_version);
    pg->eval_repop(repop);
    
    repops.insert(repop);
    ++position;
  } else {
    // object has already been trimmed, this is an extra
    coll_t col_to_trim(pg->info.pgid, snap_to_trim);
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->collection_remove(col_to_trim, *position);
    int r = pg->osd->store->queue_transaction(NULL, t, new ObjectStore::C_DeleteTransaction(t));
    assert(r == 0);
  }
  return discard_event();
}
/* WaitingOnReplicasObjects */
ReplicatedPG::WaitingOnReplicas::WaitingOnReplicas(my_context ctx) : my_base(ctx)
{
  state_name = "Trimming/WaitingOnReplicas";
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

  // All applied, now to update and share info
  snapid_t &sn = context<SnapTrimmer>().snap_to_trim;
  coll_t &c = context<SnapTrimmer>().col_to_trim;

  pg->info.purged_snaps.insert(sn);
  pg->snap_trimq.erase(sn);
  dout(10) << "purged_snaps now " << pg->info.purged_snaps << ", snap_trimq now " 
	   << pg->snap_trimq << dendl;
  
  // remove snap collection
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  dout(10) << "removing snap " << sn << " collection " << c << dendl;
  pg->snap_collections.erase(sn);
  pg->write_info(*t);
  t->remove_collection(c);
  int tr = pg->osd->store->queue_transaction(pg->osr.get(), t);
  assert(tr == 0);

  context<SnapTrimmer>().need_share_pg_info = true;

  // Back to the start
  post_event(SnapTrim());
  return transit< NotTrimming >();
}


