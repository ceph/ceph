// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#include "include/types.h"

#include "OSD.h"
#include "OSDMap.h"

#ifdef USE_OBFS
# include "OBFSStore.h"
#else
# include "FakeStore.h"
#endif

#include "ebofs/Ebofs.h"

#include "Ager.h"


#include "msg/Messenger.h"
#include "msg/Message.h"

#include "mds/MDS.h"
#include "msg/HostMonitor.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGSummary.h"
#include "messages/MOSDPGLog.h"

#include "common/Logger.h"
#include "common/LogType.h"
#include "common/Timer.h"
#include "common/ThreadPool.h"

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << whoami << " "

char *osd_base_path = "./osddata";
char *ebofs_base_path = "./ebofsdev";

#define ROLE_TYPE(x)   ((x)>0 ? 1:(x))




// <hack> force remount hack for performance testing FakeStore
class C_Remount : public Context {
  OSD *osd;
public:
  C_Remount(OSD *o) : osd(o) {}
  void finish(int) {
	osd->force_remount();
  }
};

void OSD::force_remount()
{
  dout(0) << "forcing remount" << endl;
  osd_lock.Lock();
  {
	store->umount();
	store->mount();
  }
  osd_lock.Unlock();
  dout(0) << "finished remount" << endl;
}
// </hack>


// cons/des

LogType osd_logtype;

OSD::OSD(int id, Messenger *m) 
{
  whoami = id;

  messenger = m;

  osdmap = 0;

  last_tid = 0;

  max_recovery_ops = 5;

  pending_ops = 0;
  waiting_for_no_ops = false;

  if (g_conf.osd_remount_at) 
	g_timer.add_event_after(g_conf.osd_remount_at, new C_Remount(this));


  // init object store
  // try in this order:
  // ebofsdev/all
  // ebofsdev/$num
  // ebofsdev/$hostname

  char hostname[100];
  hostname[0] = 0;
  gethostname(hostname,100);
  sprintf(dev_path, "%s/all", ebofs_base_path);
  
  struct stat sta;
  if (::lstat(dev_path, &sta) != 0)
	sprintf(dev_path, "%s/%d", ebofs_base_path, whoami);
  
  if (::lstat(dev_path, &sta) != 0)
	sprintf(dev_path, "%s/%s", ebofs_base_path, hostname);	
  
  if (g_conf.ebofs) {
    store = new Ebofs(dev_path);
  }
#ifdef USE_OBFS
  else if (g_conf.uofs) {
	store = new OBFSStore(whoami, NULL, dev_path);
  }
#endif
  else {
	store = new FakeStore(osd_base_path, whoami); 
  }

  // monitor
  char s[80];
  sprintf(s, "osd%d", whoami);
  string st = s;
  monitor = new HostMonitor(m, st);
  monitor->set_notify_port(MDS_PORT_OSDMON);
  
  // <hack> for testing monitoring
  int i = whoami;
  if (++i == g_conf.num_osd) i = 0;
  monitor->get_hosts().insert(MSG_ADDR_OSD(i));
  if (++i == g_conf.num_osd) i = 0;
  monitor->get_hosts().insert(MSG_ADDR_OSD(i));
  if (++i == g_conf.num_osd) i = 0;  
  monitor->get_hosts().insert(MSG_ADDR_OSD(i));
  
  monitor->get_notify().insert(MSG_ADDR_MDS(0));
  // </hack>

  // log
  char name[80];
  sprintf(name, "osd%02d", whoami);
  logger = new Logger(name, (LogType*)&osd_logtype);
  osd_logtype.add_set("opq");
  osd_logtype.add_inc("op");
  osd_logtype.add_inc("c_rd");
  osd_logtype.add_inc("c_rdb");
  osd_logtype.add_inc("c_wr");
  osd_logtype.add_inc("c_wrb");

  osd_logtype.add_inc("r_pull");
  osd_logtype.add_inc("r_pullb");
  osd_logtype.add_inc("r_push");
  osd_logtype.add_inc("r_pushb");
  osd_logtype.add_inc("r_wr");
  osd_logtype.add_inc("r_wrb");

  osd_logtype.add_inc("rlsum");
  osd_logtype.add_inc("rlnum");

  // request thread pool
  {
	char name[80];
	sprintf(name,"osd%d.threadpool", whoami);
	threadpool = new ThreadPool<OSD*, object_t>(name, g_conf.osd_maxthreads, 
												static_dequeueop,
												this);
  }
}

OSD::~OSD()
{
  if (threadpool) { delete threadpool; threadpool = 0; }
  if (osdmap) { delete osdmap; osdmap = 0; }
  if (monitor) { delete monitor; monitor = 0; }
  if (messenger) { delete messenger; messenger = 0; }
  if (logger) { delete logger; logger = 0; }
  if (store) { delete store; store = 0; }
}

int OSD::init()
{
  osd_lock.Lock();
  {
	// mkfs?
	if (g_conf.osd_mkfs) {
	  dout(2) << "mkfs" << endl;
	  store->mkfs();
	}
	
	// mount.
	int r = store->mount();
	assert(r>=0);

	// age?
	if (g_conf.osd_age_time > 0) {
	  Ager ager(store);
	  ager.age(g_conf.osd_age_time, g_conf.osd_age, g_conf.osd_age / 2.0, 5, g_conf.osd_age);
	}

	// monitor.
	monitor->init();
  }
  osd_lock.Unlock();

  // i'm ready!
  messenger->set_dispatcher(this);

  return 0;
}

int OSD::shutdown()
{
  dout(1) << "shutdown, timer has " << g_timer.num_event << endl;

  // finish ops
  wait_for_no_ops();

  // stop threads
  delete threadpool;
  threadpool = 0;

  // shut everything else down
  monitor->shutdown();
  messenger->shutdown();

  osd_lock.Unlock();
  int r = store->umount();
  osd_lock.Lock();
  return r;
}



// object locks

void OSD::lock_object(object_t oid) 
{
  osd_lock.Lock();
  _lock_object(oid);
  osd_lock.Unlock();
}

void OSD::_lock_object(object_t oid)
{
  if (object_lock.count(oid)) {
	Cond c;
	dout(15) << "lock_object " << hex << oid << dec << " waiting as " << &c << endl;

	list<Cond*>& ls = object_lock_waiters[oid];   // this is commit, right?
	ls.push_back(&c);
	
	while (object_lock.count(oid) ||
		   ls.front() != &c)
	  c.Wait(osd_lock);

	assert(ls.front() == &c);
	ls.pop_front();
	if (ls.empty())
	  object_lock_waiters.erase(oid);
  }

  dout(15) << "lock_object " << hex << oid << dec << endl;
  object_lock.insert(oid);
}

void OSD::unlock_object(object_t oid) 
{
  osd_lock.Lock();

  // unlock
  assert(object_lock.count(oid));
  object_lock.erase(oid);

  if (object_lock_waiters.count(oid)) {
	// someone is in line
	Cond *c = object_lock_waiters[oid].front();
	assert(c);
	dout(15) << "unlock_object " << hex << oid << dec << " waking up next guy " << c << endl;
	c->Signal();
  } else {
	// nobody waiting
	dout(15) << "unlock_object " << hex << oid << dec << endl;
  }

  osd_lock.Unlock();
}



// --------------------------------------
// dispatch

void OSD::dispatch(Message *m) 
{
  // check clock regularly
  utime_t now = g_clock.now();
  //dout(-20) << now << endl;

  osd_lock.Lock();

  switch (m->get_type()) {

	// -- don't need OSDMap --

	// host monitor
  case MSG_PING_ACK:
  case MSG_FAILURE_ACK:
	monitor->proc_message(m);
	break;

	// map and replication
  case MSG_OSD_MAP:
	handle_osd_map((MOSDMap*)m);
	break;

	// osd
  case MSG_SHUTDOWN:
	shutdown();
	delete m;
	break;
	
  case MSG_PING:
	// take note.
	monitor->host_is_alive(m->get_source());
	handle_ping((MPing*)m);
	break;
	

	// -- need OSDMap --

  default:
	{
	  // no map?  starting up?
	  if (!osdmap) {
		dout(7) << "no OSDMap, asking MDS" << endl;
		if (waiting_for_osdmap.empty()) 
		  messenger->send_message(new MGenericMessage(MSG_OSD_GETMAP), 
								  MSG_ADDR_MDS(0), MDS_PORT_MAIN);
		waiting_for_osdmap.push_back(m);
		break;
	  }

	  // need OSDMap
	  switch (m->get_type()) {
		
	  case MSG_OSD_PG_NOTIFY:
		handle_pg_notify((MOSDPGNotify*)m);
		break;
	  case MSG_OSD_PG_QUERY:
		handle_pg_query((MOSDPGQuery*)m);
		break;
	  case MSG_OSD_PG_SUMMARY:
		handle_pg_summary((MOSDPGSummary*)m);
		break;

	  case MSG_OSD_OP:
		monitor->host_is_alive(m->get_source());
		handle_op((MOSDOp*)m);
		break;
		
		// for replication etc.
	  case MSG_OSD_OPREPLY:
		monitor->host_is_alive(m->get_source());
		handle_op_reply((MOSDOpReply*)m);
		break;
		
		
	  default:
		dout(1) << " got unknown message " << m->get_type() << endl;
		assert(0);
	  }
	}
  }

  // finishers?
  if (!finished.empty()) {
	list<Message*> waiting;
	waiting.splice(waiting.begin(), finished);

	osd_lock.Unlock();
	
	for (list<Message*>::iterator it = waiting.begin();
		 it != waiting.end();
		 it++) {
	  dispatch(*it);
	}
	return;
  }
  
  osd_lock.Unlock();
}


void OSD::handle_op_reply(MOSDOpReply *m)
{
  // did i get a new osdmap?
  if (m->get_map_epoch() > osdmap->get_epoch()) {
	dout(3) << "replica op reply includes a new osd map" << endl;
	update_map(m->get_osdmap());
  }

  // handle op
  switch (m->get_op()) {
  case OSD_OP_REP_PULL:
	op_rep_pull_reply(m);
	break;

  case OSD_OP_REP_WRITE:
  case OSD_OP_REP_TRUNCATE:
  case OSD_OP_REP_DELETE:
	handle_rep_op_ack(m->get_tid(), m->get_result(), m->get_commit(), MSG_ADDR_NUM(m->get_source()));
	delete m;
	break;

  default:
	assert(0);
  }
}

void OSD::handle_rep_op_ack(__uint64_t tid, int result, bool commit, int fromosd)
{
  if (!replica_ops.count(tid)) {
	dout(7) << "not waiting for tid " << tid << " replica op reply, map must have changed, dropping." << endl;
	return;
  }
  
  OSDReplicaOp *repop = replica_ops[tid];
  MOSDOp *op = repop->op;
  pg_t pgid = op->get_pg();

  dout(7) << "handle_rep_op_ack " << tid << " op " << op << " result " << result << " commit " << commit << " from osd" << fromosd << endl;

  if (result >= 0) {
	// success
	get_repop(repop);

	if (commit) {
	  // commit
	  assert(repop->waitfor_commit.count(tid));	  
	  repop->waitfor_commit.erase(tid);
	  repop->waitfor_ack.erase(tid);
	  replica_ops.erase(tid);
	  
	  replica_pg_osd_tids[pgid][fromosd].erase(tid);
	  if (replica_pg_osd_tids[pgid][fromosd].empty()) replica_pg_osd_tids[pgid].erase(fromosd);
	  if (replica_pg_osd_tids[pgid].empty()) replica_pg_osd_tids.erase(pgid);
	} else {
	  // ack
	  repop->waitfor_ack.erase(tid);
	}

	put_repop(repop);

  } else {
	assert(0);  // for now

	// failure
	get_repop(repop);

	// forget about this failed attempt..
	repop->osds.erase(fromosd);
	repop->waitfor_ack.erase(tid);
	repop->waitfor_commit.erase(tid);

	replica_ops.erase(tid);

	replica_pg_osd_tids[pgid][fromosd].erase(tid);
	if (replica_pg_osd_tids[pgid][fromosd].empty()) replica_pg_osd_tids[pgid].erase(fromosd);
	if (replica_pg_osd_tids[pgid].empty()) replica_pg_osd_tids.erase(pgid);


	bool did = false;
	PG *pg = get_pg(pgid);

	// am i no longer the primary?
	if (pg->get_primary() != whoami) {
	  // oh, it wasn't a replica.. primary must have changed
	  dout(4) << "i'm no longer the primary for " << *pg << endl;

	  // retry the whole thing
	  finished.push_back(repop->op);

	  // clean up
	  for (map<__uint64_t,int>::iterator it = repop->waitfor_ack.begin();
		   it != repop->waitfor_ack.end();
		   it++) {
		replica_ops.erase(it->first);
		replica_pg_osd_tids[pgid][it->second].erase(it->first);
		if (replica_pg_osd_tids[pgid][it->second].empty()) replica_pg_osd_tids[pgid].erase(it->second);
	  }
	  for (map<__uint64_t,int>::iterator it = repop->waitfor_commit.begin();
		   it != repop->waitfor_commit.end();
		   it++) {
		replica_ops.erase(it->first);
		replica_pg_osd_tids[pgid][it->second].erase(it->first);
		if (replica_pg_osd_tids[pgid][it->second].empty()) replica_pg_osd_tids[pgid].erase(it->second);
	  }
	  if (replica_pg_osd_tids[pgid].empty()) replica_pg_osd_tids.erase(pgid);

	  assert(0); // this is all busted
	  /*
	  if (repop->local_commit) {
		repop->lock.Unlock();
		delete repop;
	  } else {
		assert(0);
		repop->op = 0;      // we're forwarding it
		repop->cancel = true;     // will get deleted by local commit callback
		repop->lock.Unlock();
		}*/
	  did = true;
	}

	/* no!  don't do this, not without checking complete/clean-ness
	else {
	  // i am still primary.
	  // re-issue replica op to a moved replica?
	  for (unsigned i=1; i<pg->acting.size(); i++) {
		if (repop->osds.count(pg->acting[i])) continue;
		issue_replica_op(pg, repop, pg->acting[i]);
		did = true;
	  }	
	}
	*/

	if (!did) {
	  // an osd musta just gone down or somethin.  are we "done" now?
	  assert(0); // fix me all fuddup
	}


	put_repop(repop);

  }


}



void OSD::handle_ping(MPing *m)
{
  // play dead?
  if (whoami == 1) {
	dout(7) << "playing dead" << endl;
  } else {
	dout(7) << "got ping, replying" << endl;
	messenger->send_message(new MPingAck(m),
							m->get_source(), m->get_source_port(), 0);
  }
  
  delete m;
}



// =====================================================
// MAP

void OSD::wait_for_new_map(Message *m)
{
  // ask MDS
  messenger->send_message(new MGenericMessage(MSG_OSD_GETMAP), 
						  MSG_ADDR_MDS(0), MDS_PORT_MAIN);

  waiting_for_osdmap.push_back(m);
}


/** update_map
 * assimilate a new OSDMap.  scan pgs.
 */
void OSD::update_map(bufferlist& state)
{
  // decode new map
  osdmap = new OSDMap();
  osdmap->decode(state);
  osdmaps[osdmap->get_epoch()] = osdmap;
  dout(7) << "got osd map version " << osdmap->get_epoch() << endl;
	
  // pg list
  list<pg_t> pg_list;
  
  if (osdmap->is_mkfs()) {
	dout(1) << "mkfs" << endl;
	assert(osdmap->get_epoch() == 1);

	ps_t maxps = 1LL << osdmap->get_pg_bits();
	
	// create PGs
	for (int nrep = 1; 
		 nrep <= MIN(g_conf.num_osd, g_conf.osd_max_rep);    // for low osd counts..  hackish bleh
		 nrep++) {
	  for (pg_t ps = 0; ps < maxps; ps++) {
		pg_t pgid = osdmap->ps_nrep_to_pg(ps, nrep);
		int role = osdmap->get_pg_acting_role(pgid, whoami);
		if (role < 0) continue;
		
		PG *pg = create_pg(pgid);
		osdmap->pg_to_acting_osds(pgid, pg->acting);
		pg->set_role(role);
		pg->info.last_epoch_started = pg->info.same_primary_since = osdmap->get_epoch();
		pg->last_epoch_started_any = osdmap->get_epoch();
		pg->mark_complete();
		pg->mark_active();
		
		dout(7) << "created " << *pg << endl;
		pg_list.push_back(pgid);
	  }

	  // local PG too
	  pg_t pgid = osdmap->osd_nrep_to_pg(whoami, nrep);
	  int role = osdmap->get_pg_acting_role(pgid, whoami);
	  if (role < 0) continue;

	  PG *pg = create_pg(pgid);
	  osdmap->pg_to_acting_osds(pgid, pg->acting);
	  pg->set_role(role);
	  pg->info.last_epoch_started = pg->info.same_primary_since = osdmap->get_epoch();
	  pg->last_epoch_started_any = osdmap->get_epoch();
	  pg->mark_complete();
	  pg->mark_active();
	  
	  dout(7) << "created " << *pg << endl;
	  pg_list.push_back(pgid);
	}
  } else {
	// get pg list
	get_pg_list(pg_list);
  }
  
  // use our new map(s)
  advance_map(pg_list);
  activate_map(pg_list);
  
  // process waiters
  take_waiters(waiting_for_osdmap);
}


/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(list<pg_t>& ls)
{
  dout(7) << "advance_map version " << osdmap->get_epoch() << endl;
  
  // scan pg's
  for (list<pg_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	pg_t pgid = *it;
	PG *pg = get_pg(pgid);
	assert(pg);
	
	// did i finish this epoch?
	if (pg->is_active()) {
	  assert(pg->info.last_epoch_started == osdmap->get_epoch());
	  pg->info.last_epoch_finished = osdmap->get_epoch();
	}

	// get new acting set
	vector<int> acting;
	int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	
	int primary = -1;
	if (nrep > 0) primary = acting[0];
	
	int role = -1;        // -1, 0, 1
	for (int i=0; i<nrep; i++) 
	  if (acting[i] == whoami) role = i > 0 ? 1:0;

	// no change?
	if (acting == pg->acting) 
	  continue;

	// primary changed?
	if (pg->acting[0] != acting[0]) {
	  pg->info.same_primary_since = osdmap->get_epoch();
	}

	if (role != pg->get_role()) {
	  // my role changed.
	  dout(10) << " " << *pg << " role change " << pg->get_role() << " -> " << role << endl; 
	  
	  // old primary?
	  if (pg->get_role() == 0) {
		// take waiters
		take_waiters(pg->waiting_for_active);
		for (hash_map<object_t, list<Message*> >::iterator it = pg->waiting_for_missing_object.begin();
			 it != pg->waiting_for_missing_object.end();
			 it++)
		  take_waiters(it->second);
		pg->waiting_for_missing_object.clear();

		// drop peers
		pg->clear_content_recovery_state();
		pg->state_clear(PG::STATE_CLEAN);
	  }
	  
	  // new primary?
	  if (role == 0) {
		// i am new primary
		pg->state_clear(PG::STATE_ACTIVE);
	  } else {
		// i am now replica|stray.  we need to send a notify.
		pg->state_clear(PG::STATE_ACTIVE);
		pg->state_set(PG::STATE_STRAY);

		if (nrep == 0) 
		  dout(1) << "crashed pg " << *pg << endl;
	  }
	  
	} else {
	  // no role change.
	  // did primary change?
	  if (primary != pg->get_primary()) {
		dout(10) << " " << *pg << " acting primary change " 
				 << pg->get_primary() << " -> " << primary 
				 << ", !peered" << endl;
		
		// we need to announce
		pg->state_set(PG::STATE_STRAY);
	  } else {
		// primary is the same.
		if (role == 0) {
		  // i am (still) primary. but my replica set changed.
		  dout(10) << " " << *pg << " replica set changed, !clean !peered" << endl;
		  pg->state_clear(PG::STATE_ACTIVE);
		  pg->state_clear(PG::STATE_CLEAN);
		}
	  }
	}

	// update PG
	pg->acting = acting;
	pg->calc_role(whoami);
	//pg->store();

	// scan down osds
	for (set<int>::const_iterator down = osdmap->get_down_osds().begin();
		 down != osdmap->get_down_osds().end();
		 down++) {
	  if (!pg->is_acting(*down)) continue;
	  
	  dout(10) << " " << *pg << " peer osd" << *down << " is down" << endl;
	  
	  // NAK any ops to the down osd
	  if (replica_pg_osd_tids[pgid].count(*down)) {
		set<__uint64_t> s = replica_pg_osd_tids[pgid][*down];
		dout(10) << " " << *pg << " naking replica ops to down osd" << *down << " " << s << endl;
		for (set<__uint64_t>::iterator tid = s.begin();
			 tid != s.end();
			 tid++)
		  handle_rep_op_ack(*tid, -1, false, *down);
	  }
	}
  }
}

void OSD::activate_map(list<pg_t>& ls)
{
  dout(7) << "activate_map version " << osdmap->get_epoch() << endl;

  map< int, list<PG::PGInfo> >  notify_list;  // primary -> list
  map< int, map<pg_t,version_t> > query_map;    // peer -> PG -> get_summary_since

  // scan pg's
  for (list<pg_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	pg_t pgid = *it;
	PG *pg = get_pg(pgid);
	assert(pg);

	if (pg->get_role() == 0) {
	  // i am primary
	  pg->build_prior();
	  pg->peer(query_map);
	}
	else if (pg->is_stray()) {
	  // i am residual|replica
	  notify_list[pg->get_primary()].push_back(pg->info);
	}
  }  

  if (!osdmap->is_mkfs()) {   // hack: skip the queries/summaries if it's a mkfs
	// notify? (residual|replica)
	do_notifies(notify_list);
	
	// do queries.
	do_queries(query_map);
  }
}




void OSD::handle_osd_map(MOSDMap *m)
{
  // wait for ops to finish
  wait_for_no_ops();

  if (!osdmap ||
	  m->get_epoch() > osdmap->get_epoch()) {
	if (osdmap) {
	  dout(3) << "handle_osd_map got osd map epoch " << m->get_epoch() << " > " << osdmap->get_epoch() << endl;
	} else {
	  dout(3) << "handle_osd_map got osd map epoch " << m->get_epoch() << endl;
	}

	update_map(m->get_osdmap());

  } else {
	dout(3) << "handle_osd_map ignoring osd map epoch " << m->get_epoch() << " <= " << osdmap->get_epoch() << endl;
  }
  
  if (osdmap->is_mkfs()) {
	// ack
	messenger->send_message(new MGenericMessage(MSG_OSD_MKFS_ACK),
							m->get_source());
  }

  delete m;
}


OSDMap* OSD::get_osd_map(version_t v)
{
  assert(osdmaps[v]);
  return osdmaps[v];
}





bool OSD::require_current_map(Message *m, epoch_t ep) 
{
  int from = MSG_ADDR_NUM(m->get_source());

  // older map?
  if (ep < osdmap->get_epoch()) {
	dout(7) << "  from old map epoch " << ep << " < " << osdmap->get_epoch() << endl;
	delete m;   // discard and ignore.
	return false;
  }

  // newer map?
  if (ep > osdmap->get_epoch()) {
	dout(7) << "  from newer map epoch " << ep << " > " << osdmap->get_epoch() << endl;
	wait_for_new_map(m);
	return false;
  }

  // down?
  if (osdmap->is_down(from)) {
	dout(7) << "  from down OSD osd" << from << ", dropping" << endl;
	// FIXME
	return false;
  }

  assert(ep == osdmap->get_epoch());
  return true;
}


/*
 * require that we have same (or newer) map, and that
 * the source is the pg primary.
 */
bool OSD::require_same_or_newer_map(Message *m, epoch_t epoch)
{
  int from = MSG_ADDR_NUM(m->get_source());

  // newer map?
  if (epoch > osdmap->get_epoch()) {
	dout(7) << "  from newer map epoch " << epoch << " > " << osdmap->get_epoch() << endl;
	wait_for_new_map(m);
	return false;
  }

  // down?
  if (osdmap->is_down(from)) {
	dout(7) << "  from down OSD osd" << from 
			<< ", pinging?" << endl;
	assert(epoch < osdmap->get_epoch());
	// FIXME
	return false;
  }

  return true;
}




// ======================================================
// REPLICATION

// PG

void OSD::get_pg_list(list<pg_t>& ls)
{
  // just list collections; assume they're all pg's (for now)
  store->list_collections(ls);
}

bool OSD::pg_exists(pg_t pgid) 
{
  return store->collection_exists(pgid);
}

PG *OSD::create_pg(pg_t pgid)
{
  assert(pg_map.count(pgid) == 0);
  assert(!pg_exists(pgid));

  PG *pg = new PG(this, pgid);
  pg_map[pgid] = pg;

  store->create_collection(pgid);

  //pg->info.created = osdmap->get_epoch();
  //pg->store(store);

  return pg;
}

PG *OSD::get_pg(pg_t pgid)
{
  // already open?
  if (pg_map.count(pgid)) 
	return pg_map[pgid];

  // exists?
  if (!pg_exists(pgid))
	return 0;

  // open, stat collection
  PG *pg = new PG(this, pgid);
  //pg->fetch(store);
  pg_map[pgid] = pg;

  return pg;
}
 



/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void OSD::do_notifies(map< int, list<PG::PGInfo> >& notify_list) 
{
  for (map< int, list<PG::PGInfo> >::iterator it = notify_list.begin();
	   it != notify_list.end();
	   it++) {
	dout(7) << "do_notify osd" << it->first << " on " << it->second.size() << " PGs" << endl;
	MOSDPGNotify *m = new MOSDPGNotify(osdmap->get_epoch(), it->second);
	messenger->send_message(m, MSG_ADDR_OSD(it->first));
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void OSD::do_queries(map< int, map<pg_t,version_t> >& query_map)
{
  for (map< int, map<pg_t, version_t> >::iterator pit = query_map.begin();
	   pit != query_map.end();
	   pit++) {
	int who = pit->first;
	dout(7) << "do_queries querying osd" << who
			<< " on " << pit->second.size() << " PGs" << endl;

	MOSDPGQuery *m = new MOSDPGQuery(osdmap->get_epoch(),
									 pit->second);
	messenger->send_message(m,
							MSG_ADDR_OSD(who));
  }
}


/** PGNotify
 * from non-primary to primary
 * includes PGInfo.
 */

void OSD::handle_pg_notify(MOSDPGNotify *m)
{
  dout(7) << "handle_pg_notify from " << m->get_source() << endl;
  int from = MSG_ADDR_NUM(m->get_source());

  if (!require_same_or_newer_map(m, m->get_epoch())) return;
  
  // look for unknown PGs i'm primary for
  map< int, map<pg_t,version_t> > query_map;

  for (list<PG::PGInfo>::iterator it = m->get_pg_list().begin();
	   it != m->get_pg_list().end();
	   it++) {
	pg_t pgid = it->pgid;
	PG *pg = get_pg(pgid);

	if (!pg) {
	  pg = create_pg(pgid);

	  int nrep = osdmap->pg_to_acting_osds(pgid, pg->acting);
	  assert(nrep > 0);
	  assert(pg->acting[0] == whoami);
	  pg->info.same_primary_since = it->same_primary_since;
	  pg->set_role(0);

	  pg->last_epoch_started_any = it->last_epoch_started;
	  pg->build_prior();

	  dout(10) << " " << *pg << " is new" << endl;
	
	  // kick any waiters
	  if (waiting_for_pg.count(pgid)) {
		take_waiters(waiting_for_pg[pgid]);
		waiting_for_pg.erase(pgid);
	  }
	}

	// save info.
	pg->peer_info[from] = *it;

	// adjust prior?
	if (it->last_epoch_started > pg->last_epoch_started_any) 
	  pg->adjust_prior();

	// peer
	pg->peer(query_map);
  }
  
  do_queries(query_map);
  
  delete m;
}

void OSD::handle_pg_log(MOSDPGLog *m) 
{


}


/** PGQuery
 * from primary to replica | other
 */
void OSD::handle_pg_query(MOSDPGQuery *m) 
{
  dout(7) << "handle_pg_query from " << m->get_source() << endl;
  int from = MSG_ADDR_NUM(m->get_source());
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;
  
  map< int, list<PG::PGInfo> > notify_list;
  
  for (map<pg_t,version_t>::iterator it = m->pg_list.begin();
	   it != m->pg_list.end();
	   it++) {
	pg_t pgid = it->first;
	PG *pg = get_pg(pgid);

	if (!pg) {
	  // get active rush mapping
	  vector<int> acting;
	  int nrep = osdmap->pg_to_acting_osds(pgid, acting);
	  assert(nrep > 0);
	  int role = -1;
	  for (unsigned i=0; i<acting.size(); i++)
		if (acting[i] == whoami) role = i>0 ? 1:0;
	  assert(role != 0);

	  if (role < 0) {
		dout(10) << " pg " << hex << pgid << dec << " dne, and i am not an active replica" << endl;
		PG::PGInfo empty(pgid);
		notify_list[from].push_back(empty);
		continue;
	  }

	  pg = create_pg(pgid);
	  pg->acting = acting;
	  pg->set_role(role);

	  dout(10) << *pg << " dne (before), but i am role " << role << endl;
	}

	if (it->second == 0) {
	  // info
	  dout(10) << *pg << " sending info" << endl;
	  notify_list[from].push_back(pg->info);
	} else if (it->second == 1) {
	  // summary
	  dout(10) << *pg << " sending content summary" << endl;
	  PG::PGSummary summary;
	  pg->generate_summary(summary);
	  MOSDPGSummary *m = new MOSDPGSummary(osdmap->get_epoch(), pg->get_pgid(), summary);
	  messenger->send_message(m, MSG_ADDR_OSD(from));
	} else {
	  // log + info
	  dout(10) << *pg << " sending info+log since " << it->second << endl;
	  MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->get_pgid());
	  m->info = pg->info;
	  m->log.copy_after(pg->log, it->second);
	  messenger->send_message(m, MSG_ADDR_OSD(from));
	}	
  }
  
  do_notifies(notify_list);   

  delete m;
}



void OSD::handle_pg_summary(MOSDPGSummary *m)
{
  /*
  dout(7) << "handle_pg_summary from " << m->get_source() << endl;
  int from = MSG_ADDR_NUM(m->get_source());

  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  map< int, map<pg_t,version_t> > query_map;    // peer -> PG -> get_summary_since

  pg_t pgid = m->get_pgid();
  PG::PGSummary *sum = m->get_summary();
  PG *pg = get_pg(pgid);
  assert(pg);

  if (pg->is_primary()) {
	// PRIMARY
	dout(10) << *pg << " got summary from osd" << from
			 << endl;
	PG::PGPeer *pgp = pg->get_peer(from);
	assert(pgp);
	assert(pgp->content_summary == 0);  // ?
	pgp->content_summary = sum;
	pgp->state_set(PG::PGPeer::STATE_SUMMARY);
	
	if (pgp->info.last_update > pg->info.last_update) {
	  // start new summary?
	  if (pg->content_summary == 0)
		pg->content_summary = new PG::PGContentSummary();
	  
	  // assimilate summary info!
	  list<PG::ObjectInfo>::iterator myp = pg->content_summary->ls.begin();
	  list<PG::ObjectInfo>::iterator newp = sum->ls.begin();
	  
	  while (newp != sum->ls.end()) {
		if (myp == pg->content_summary->ls.end()) {
		  // add new item
		  pg->content_summary->ls.insert(myp, 1, *newp);
		  pg->info.last_update = newp->version;
		  if (myp->osd == from) {
			// remote
			pg->content_summary->remote++;
		  } else {
			// missing.
			myp->osd = -1;  
			pg->content_summary->missing++;
		  }
		  myp++;
		  assert(myp == pg->content_summary->ls.end());
		} else {
		  assert(myp->oid == newp->oid && myp->version == newp->version);
		  if (myp->osd == -1 && newp->osd == from) {
			myp->osd = from;  // found!
			pg->content_summary->remote++;
			pg->content_summary->missing--;
		  }
		  myp++;
		}
		newp++;
	  }
	  assert(myp == pg->content_summary->ls.end());
	}
		
	repeer(pg, query_map);
	
  } else {
	// REPLICA
	dout(10) << *pg << " got summary from primary osd" << from 
			 << endl;
	assert(from == pg->acting[0]);

	// copy summary.  FIXME.
	if (pg->content_summary == 0)
	  pg->content_summary = new PG::PGContentSummary();
	*pg->content_summary = *sum;

	// i'm now active!
	pg->state_set(PG::STATE_ACTIVE);
	
	// take any waiters
	take_waiters(pg->waiting_for_active);
	
	// initiate any recovery?
	pg->plan_recovery();
  }
  */
  delete m;
}







// RECOVERY





// pull

void OSD::pg_pull(PG *pg, int maxops)
{
  int ops = pg->num_active_ops();

  dout(7) << "pg_pull pg " << *pg 
		  << " " << pg->missing.num_missing() << " to do, " 
		  << ops << "/" << maxops << " active" <<  endl;
  
  /*
  while (ops < maxops &&
		 !pg->recovery_queue.empty()) {
	map<version_t, PG::ObjectInfo>::iterator first = pg->recovery_queue.upper_bound(pg->requested_through);
	
	pull_replica(pg, first->second);
	pg->requested_through = first->first;

	ops++;
  } 
  */ 
}

void OSD::pull_replica(PG *pg, object_t oid, version_t v)
{
/*  // get peer
  dout(7) << "pull_replica " << hex << oi.oid << dec 
		  << " v " << oi.version 
		  << " from osd" << oi.osd << endl;

  // send op
  tid_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
						  oi.oid, pg->get_pgid(),
						  osdmap->get_epoch(),
						  OSD_OP_REP_PULL);
  op->set_version(oi.version);
  op->set_pg_role(-1);  // whatever, not 0
  messenger->send_message(op, MSG_ADDR_OSD(oi.osd));

  // take note
  pull_ops[tid] = oi;
  pg->objects_pulling[oi.oid] = oi;
*/
}


void OSD::op_rep_pull(MOSDOp *op)
{
  long got = 0;
  
  dout(7) << "rep_pull on " << hex << op->get_oid() << dec << " v " << op->get_version() << endl;
  
  // get object size
  struct stat st;
  int r = store->stat(op->get_oid(), &st);
  assert(r == 0);
  
  // check version
  version_t v = 0;
  store->getattr(op->get_oid(), "version", &v, sizeof(v));
  assert(v >= op->get_version());
  
  // read
  bufferlist bl;
  got = store->read(op->get_oid(), 
					st.st_size, 0,
					bl);
  assert(got == st.st_size);
  
  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true); 
  reply->set_result(0);
  reply->set_data(bl);
  reply->set_length(got);
  reply->set_offset(0);
  reply->set_version(v);
  
  messenger->send_message(reply, op->get_asker());
  
  delete op;

  logger->inc("r_pull");
  logger->inc("r_pullb", got);
}

void OSD::op_rep_pull_reply(MOSDOpReply *op)
{
  /*
  object_t o = op->get_oid();
  version_t v = op->get_version();

  dout(7) << "rep_pull_reply " << hex << o << dec << " v " << v << " size " << op->get_length() << endl;

  PG::ObjectInfo oi = pull_ops[op->get_tid()];
  assert(v <= op->get_version());
  
  PG *pg = get_pg(op->get_pg());
  assert(pg);
  assert(pg->objects_pulling.count(oi.oid));

  // write it and add it to the PG
  store->write(o, op->get_length(), 0, op->get_data(), true);
  store->collection_add(pg->get_pgid(), o);
  store->setattr(o, "version", &v, sizeof(v));

  // close out pull op.
  pull_ops.erase(op->get_tid());
  pg->objects_pulling.erase(o);

  // bottom of queue?
  map<version_t,PG::ObjectInfo>::iterator bottom = pg->recovery_queue.begin();
  if (bottom->first == oi.version) 
	pg->info.last_complete = bottom->first;
  pg->recovery_queue.erase(oi.version);

  // now complete?
  if (pg->recovery_queue.empty()) {
	assert(pg->info.last_complete == pg->info.last_update);

	// tell primary?
	dout(7) << " " << *pg << " recovery complete, telling primary" << endl;
	list<PG::PGInfo> ls;
	ls.push_back(pg->info);
	messenger->send_message(new MOSDPGNotify(osdmap->get_epoch(),
											 ls),
							MSG_ADDR_OSD(pg->get_primary()));
  } else {
	// more?
	pg->do_recovery();
  }

  // finish waiters
  if (pg->waiting_for_missing_object.count(o)) 
	take_waiters(pg->waiting_for_missing_object[o]);

  delete op;
  */
}




// op_rep_modify

// commit (to disk) callback
class C_OSD_RepModifyCommit : public Context {
public:
  OSD *osd;
  MOSDOp *op;
  C_OSD_RepModifyCommit(OSD *o, MOSDOp *oo) : osd(o), op(oo) { }
  void finish(int r) {
	osd->op_rep_modify_commit(op);
  }
};

void OSD::op_rep_modify_commit(MOSDOp *op)
{
  // hack: hack_blah is true until 'ack' has been sent.
  if (op->hack_blah) {
	dout(0) << "got rep_modify_commit before rep_modify applied, waiting" << endl;
	g_timer.add_event_after(1, new C_OSD_RepModifyCommit(this, op));
  } else {
	dout(10) << "rep_modify_commit on op " << *op << endl;
	MOSDOpReply *commit = new MOSDOpReply(op, 0, osdmap, true);
	messenger->send_message(commit, op->get_asker());
	delete op;
  }
}

// process a modification operation

void OSD::op_rep_modify(MOSDOp *op)
{ 
  object_t oid = op->get_oid();

  // check current version
  version_t ov = 0;
  if (store->exists(oid)) 
	store->getattr(oid, "version", &ov, sizeof(ov));

  if (op->get_old_version() != ov) {
	assert(ov < op->get_old_version());

	// FIXME: block until i get the updated version.
	dout(0) << "rep_modify old version is " << ov << "  msg sez " << op->get_old_version() << endl;
  }
  assert(op->get_old_version() == ov);
  
  // PG
  PG *pg = get_pg(op->get_pg());
  assert(pg);
  
  dout(12) << "rep_modify in " << *pg << " o " << hex << oid << dec << " v " << op->get_version() << " (i have " << ov << ")" << endl;
  
  Context *oncommit = 0;
  
  op->hack_blah = true;  // hack: make sure any 'commit' goes out _after_ our ack

  oncommit = new C_OSD_RepModifyCommit(this, op);
  op_apply(op, op->get_version(), oncommit); 
 
  if (op->get_op() == OSD_OP_REP_WRITE) {
  	logger->inc("r_wr");
	logger->inc("r_wrb", op->get_length());
  }

  // update pg version too ... FIXME
  pg->info.last_update = op->get_version();
  if (pg->info.last_complete == ov)
	pg->info.last_complete = op->get_version();

  if (oncommit) {
	// ack
	MOSDOpReply *ack = new MOSDOpReply(op, 0, osdmap, false);
	messenger->send_message(ack, op->get_asker());
  } else {
	// commit, commit
	MOSDOpReply *ack = new MOSDOpReply(op, 0, osdmap, true);
	messenger->send_message(ack, op->get_asker());
	delete op;
  }
  
  op->hack_blah = false;  // hack: make sure any 'commit' goes out _after_ our ack
}


// =========================================================
// OPS

void OSD::handle_op(MOSDOp *op)
{
  pg_t pgid = op->get_pg();
  PG *pg = get_pg(pgid);

  // what kind of op?
  if (!OSD_OP_IS_REP(op->get_op())) {
	// REGULAR OP (non-replication)

	// is our map version up to date?
	if (op->get_map_epoch() > osdmap->get_epoch()) {
	  // op's is newer
	  dout(7) << "op map " << op->get_map_epoch() << " > " << osdmap->get_epoch() << endl;
	  wait_for_new_map(op);
	  return;
	}

	// am i the primary?
	int acting_primary = osdmap->get_pg_acting_primary( pgid );
	
	if (acting_primary != whoami) {
	  if (acting_primary >= 0) {
		dout(7) << " acting primary is " << acting_primary 
				<< ", forwarding" << endl;
		messenger->send_message(op, MSG_ADDR_OSD(acting_primary), 0);
		logger->inc("fwd");
	  } else {
		dout(1) << "crashed pg " << *pg << endl;
		messenger->send_message(new MOSDOpReply(op, -EIO, osdmap, true),
								op->get_asker());
		delete op;
	  }
	  return;
	}

	// proxy?
	if (!pg) {
	  dout(7) << "hit non-existent pg " << hex << pgid << dec 
			  << ", waiting" << endl;
	  waiting_for_pg[pgid].push_back(op);
	  return;
	}
	else {
	  dout(7) << "handle_op " << op << " in " << *pg << endl;
	  
	  // must be peered.
	  if (!pg->is_active()) {
		dout(7) << "op_write " << *pg << " not active (yet)" << endl;
		pg->waiting_for_active.push_back(op);
		return;
	  }

	  const object_t oid = op->get_oid();

	  if (!pg->is_complete()) {
		// consult PG object map
		if (pg->missing.missing.count(oid)) {
		  // need to pull
		  version_t v = pg->missing.missing[oid];
		  dout(7) << "need to pull object " << hex << oid << dec 
				  << " v " << v << endl;
		  if (!pg->objects_pulling.count(oid)) 
			pull_replica(pg, oid, v);
		  pg->waiting_for_missing_object[oid].push_back(op);
		  return;
		}
	  }	  

	  // okay!
	}	
	
  } else {
	// REPLICATION OP
	if (pg) {
	  dout(7) << "handle_rep_op " << op 
			  << " in " << *pg << endl;
	} else {
	  assert(0);
	  dout(7) << "handle_rep_op " << op 
			  << " in pgid " << hex << pgid << dec << endl;
	}
	
    // check osd map
	if (op->get_map_epoch() != osdmap->get_epoch()) {
	  // make sure source is still primary
	  int curprimary = osdmap->get_pg_acting_primary(op->get_pg());
	  int myrole = osdmap->get_pg_acting_role(op->get_pg(), whoami);
	  
	  if (curprimary != MSG_ADDR_NUM(op->get_source()) ||
		  myrole <= 0) {
		dout(5) << "op map " << op->get_map_epoch() << " != " << osdmap->get_epoch() << ", primary changed on pg " << hex << op->get_pg() << dec << endl;
		MOSDOpReply *fail = new MOSDOpReply(op, -1, osdmap, false);
		messenger->send_message(fail, op->get_asker());
		return;
	  } else {
		dout(5) << "op map " << op->get_map_epoch() << " != " << osdmap->get_epoch() << ", primary same on pg " << hex << op->get_pg() << dec << endl;
	  }
	}  
  }
  
  if (g_conf.osd_maxthreads < 1) {
	do_op(op); // do it now
  } else {
	enqueue_op(op->get_oid(), op); 	// queue for worker threads
  }
}


/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(object_t oid, MOSDOp *op)
{
  while (pending_ops > g_conf.osd_max_opq) {
	dout(10) << "enqueue_op waiting for pending_ops " << pending_ops << " to drop to " << g_conf.osd_max_opq << endl;
	op_queue_cond.Wait(osd_lock);
  }

  op_queue[oid].push_back(op);
  pending_ops++;
  logger->set("opq", pending_ops);
  
  threadpool->put_op(oid);
}

/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void OSD::dequeue_op(object_t oid)
{
  MOSDOp *op;

  osd_lock.Lock();
  {
	// lock oid
	_lock_object(oid);  

	// get pending op
	list<MOSDOp*> &ls  = op_queue[oid];
	assert(!ls.empty());
	op = ls.front();
	ls.pop_front();

	dout(10) << "dequeue_op " << hex << oid << dec << " op " << op << ", " 
			 << ls.size() << " / " << (pending_ops-1) << " more pending" << endl;
	
	if (ls.empty())
	  op_queue.erase(oid);
  }
  osd_lock.Unlock();
  
  // do it
  do_op(op);

  // unlock oid
  unlock_object(oid);
  
  // finish
  osd_lock.Lock();
  {
	dout(10) << "dequeue_op finish op " << op << endl;
	assert(pending_ops > 0);

	if (pending_ops > g_conf.osd_max_opq) 
	  op_queue_cond.Signal();
	
	pending_ops--;
	logger->set("opq", pending_ops);
	if (pending_ops == 0 && waiting_for_no_ops)
	  no_pending_ops.Signal();
  }
  osd_lock.Unlock();
}



/** do_op - do an op
 * object lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void OSD::do_op(MOSDOp *op) 
{
  dout(10) << "do_op " << op << " " << op->get_op() << endl;

  logger->inc("op");

  // replication ops?
  if (OSD_OP_IS_REP(op->get_op())) {
	// replication/recovery
	switch (op->get_op()) {
	  // push/pull/remove
	case OSD_OP_REP_PULL:
	  op_rep_pull(op);
	  break;

	  // replica ops
	case OSD_OP_REP_WRITE:
	case OSD_OP_REP_TRUNCATE:
	case OSD_OP_REP_DELETE:
	  op_rep_modify(op);
	  break;
	default:
	  assert(0);	  
	}
  } else {
	// regular op
	switch (op->get_op()) {
	case OSD_OP_READ:
	  op_read(op);
	  break;
	case OSD_OP_STAT:
	  op_stat(op);
	  break;
	case OSD_OP_WRITE:
	case OSD_OP_ZERO:
	case OSD_OP_DELETE:
	case OSD_OP_TRUNCATE:
	  op_modify(op);
	  break;
	default:
	  assert(0);
	}
  }

}

void OSD::wait_for_no_ops()
{
  if (pending_ops > 0) {
	dout(7) << "wait_for_no_ops - waiting for " << pending_ops << endl;
	waiting_for_no_ops = true;
	while (pending_ops > 0)
	  no_pending_ops.Wait(osd_lock);
	waiting_for_no_ops = false;
	assert(pending_ops == 0);
  } 
  dout(7) << "wait_for_no_ops - none" << endl;
}


// ==============================
// Object locking

//
// If the target object of the operation op is locked for writing by another client, the function puts op to the waiting queue waiting_for_wr_unlock
// returns true if object was locked, otherwise returns false
// 
bool OSD::block_if_wrlocked(MOSDOp* op)
{
  object_t oid = op->get_oid();

  msg_addr_t source;
  int len = store->getattr(oid, "wrlock", &source, sizeof(msg_addr_t));
  //cout << "getattr returns " << len << " on " << hex << oid << dec << endl;

  if (len == sizeof(source) &&
	  source != op->get_asker()) {
	//the object is locked for writing by someone else -- add the op to the waiting queue	  
	waiting_for_wr_unlock[oid].push_back(op);
	return true;
  }

  return false; //the object wasn't locked, so the operation can be handled right away
}



// ===============================
// OPS

// READ OPS

void OSD::op_read(MOSDOp *op)
{
  object_t oid = op->get_oid();
 
  //if the target object is locked for writing by another client, put 'op' to the waiting queue
  if (block_if_wrlocked(op)) {
	return; //read will be handled later, after the object becomes unlocked
  }
 
  // read into a buffer
  bufferlist bl;
  long got = store->read(oid, 
						 op->get_length(), op->get_offset(),
						 bl);
  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true); 
  if (got >= 0) {
	reply->set_result(0);
	reply->set_data(bl);
	reply->set_length(got);
	  
	logger->inc("c_rd");
	logger->inc("c_rdb", got);
	
  } else {
	reply->set_result(got);   // error
	reply->set_length(0);
  }
  
  dout(12) << "read got " << got << " / " << op->get_length() << " bytes from obj " << hex << oid << dec << endl;
  
  logger->inc("rd");
  if (got >= 0) logger->inc("rdb", got);
  
  // send it
  messenger->send_message(reply, op->get_asker());
  
  delete op;
}

void OSD::op_stat(MOSDOp *op)
{
  object_t oid = op->get_oid();

  struct stat st;
  memset(&st, sizeof(st), 0);
  int r = store->stat(oid, &st);
  
  dout(3) << "stat on " << hex << oid << dec << " r = " << r << " size = " << st.st_size << endl;
  
  MOSDOpReply *reply = new MOSDOpReply(op, r, osdmap, true);
  reply->set_object_size(st.st_size);
  messenger->send_message(reply, op->get_asker());
  
  logger->inc("stat");

  delete op;
}



// WRITE OPS

int OSD::apply_write(MOSDOp *op, version_t v, Context *oncommit)
{
  // take buffers from the message
  bufferlist bl;
  //bl = op->get_data();
  bl.claim( op->get_data() );  // save some memory?
  
  // write 
  int r = 0;
  if (oncommit) {
	/*if (g_conf.fake_osd_sync) {
	  // fake a delayed commit
	  r = store->write(op->get_oid(),
					   op->get_length(),
					   op->get_offset(),
					   bl,
					   false);
	  g_timer.add_event_after(1.0,
							  oncommit);
							  } else {
	*/
	// for real
	if (1) {
	//if (!op->get_source().is_client()) {   // don't write client?
	  r = store->write(op->get_oid(),
					   op->get_length(),
					   op->get_offset(),
					   bl,
					   oncommit);
	} else {
	  // don't actually write, but say we did, for network throughput testing...
	  g_timer.add_event_after(2.0, oncommit);
	}
  } else {
	// normal business
	assert(0);  // no more!
	r = store->write(op->get_oid(),
					 op->get_length(),
					 op->get_offset(),
					 bl,
					 false);
  }

  if (r < 0) {
	dout(0) << "apply_write failed with r = " << r << "... disk full?" << endl;
  }
  assert(r >= 0);   // disk full?

  // set version
  store->setattr(op->get_oid(), "version", &v, sizeof(v));

  return 0;
}



void OSD::issue_replica_op(PG *pg, OSDReplicaOp *repop, int osd)
{
  MOSDOp *op = repop->op;
  object_t oid = op->get_oid();

  dout(7) << " issue_replica_op in " << *pg << " o " << hex << oid << dec << " to osd" << osd << endl;
  
  // forward the write/update/whatever
  __uint64_t tid = ++last_tid;
  MOSDOp *wr = new MOSDOp(tid,
						  messenger->get_myaddr(),
						  oid,
						  pg->get_pgid(),
						  osdmap->get_epoch(),
						  100+op->get_op());
  wr->get_data() = op->get_data();   // copy bufferlist
  wr->set_length(op->get_length());
  wr->set_offset(op->get_offset());
  wr->set_version(repop->new_version);
  wr->set_old_version(repop->old_version);
  wr->set_pg_role(1); // replica
  messenger->send_message(wr, MSG_ADDR_OSD(osd));
  
  repop->osds.insert(osd);

  repop->waitfor_ack[tid] = osd;
  repop->waitfor_commit[tid] = osd;

  replica_ops[tid] = repop;
  replica_pg_osd_tids[pg->get_pgid()][osd].insert(tid);
}


void OSD::get_repop(OSDReplicaOp *repop)
{
  repop->lock.Lock();
  dout(10) << "get_repop " << *repop << endl;
}

void OSD::put_repop(OSDReplicaOp *repop)
{
  dout(10) << "put_repop " << *repop << endl;

  // commit?
  if (repop->can_send_commit() &&
	  repop->op->wants_commit()) {
	MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osdmap, true);
	dout(10) << "put_repop sending commit on " << *repop << " " << reply << endl;
	messenger->send_message(reply, repop->op->get_asker());
	repop->sent_commit = true;
  }

  // ack?
  else if (repop->can_send_ack() &&
		   repop->op->wants_ack()) {
	MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osdmap, false);
	dout(10) << "put_repop sending ack on " << *repop << " " << reply << endl;
	messenger->send_message(reply, repop->op->get_asker());
	repop->sent_ack = true;

	utime_t now = g_clock.now();
	now -= repop->start;
	logger->finc("rlsum", now);
	logger->inc("rlnum", 1);
  }

  // done.
  if (repop->can_delete()) {
	dout(10) << "put_repop deleting " << *repop << endl;
	repop->lock.Unlock();  
	delete repop->op;
	delete repop;
  } else {
	repop->lock.Unlock();
  }
}

class C_OSD_WriteCommit : public Context {
public:
  OSD *osd;
  OSD::OSDReplicaOp *repop;
  C_OSD_WriteCommit(OSD *o, OSD::OSDReplicaOp *op) : osd(o), repop(op) {}
  void finish(int r) {
	osd->op_modify_commit(repop);
  }
};

void OSD::op_modify_commit(OSDReplicaOp *repop)
{
  dout(10) << "op_modify_commit on op " << *repop->op << endl;
  get_repop(repop);
  assert(repop->waitfor_commit.count(0));
  repop->waitfor_commit.erase(0);
  put_repop(repop);
}

void OSD::op_modify(MOSDOp *op)
{
  object_t oid = op->get_oid();

  char *opname = 0;
  if (op->get_op() == OSD_OP_WRITE) opname = "op_write";
  if (op->get_op() == OSD_OP_ZERO) opname = "op_zero";
  if (op->get_op() == OSD_OP_DELETE) opname = "op_delete";
  if (op->get_op() == OSD_OP_TRUNCATE) opname = "op_truncate";
  if (op->get_op() == OSD_OP_WRLOCK) opname = "op_wrlock";
  if (op->get_op() == OSD_OP_WRUNLOCK) opname = "op_wrunlock";

  // version?  clean?
  version_t ov = 0;  // 0 == dne (yet)
  store->getattr(oid, "version", &ov, sizeof(ov));
  
  //version_t nv = messenger->get_lamport();//op->get_lamport_recv_stamp();
  version_t nv = ov + 1;   //FIXME later
  
  if (nv <= ov) 
	cerr << opname << " " << hex << oid << dec << " ov " << ov << " nv " << nv 
		 << " ... wtf?  msg sent " << op->get_lamport_send_stamp() 
		 << " recv " << op->get_lamport_recv_stamp() << endl;
  assert(nv > ov);
  
  dout(12) << " " << opname << " " << hex << oid << dec << " v " << nv << "  off " << op->get_offset() << " len " << op->get_length() << endl;  
  
  // issue replica writes
  OSDReplicaOp *repop = new OSDReplicaOp(op, nv, ov);
  repop->start = g_clock.now();
  repop->waitfor_ack[0] = whoami;    // will need local ack, commit
  repop->waitfor_commit[0] = whoami;
  
  pg_t pgid = op->get_pg();
  PG *pg;
  osd_lock.Lock();
  repop->lock.Lock();
  {
	pg = get_pg(pgid);
	for (unsigned i=1; i<pg->acting.size(); i++) {
	  issue_replica_op(pg, repop, pg->acting[i]);
	}
  }
  repop->lock.Unlock();
  osd_lock.Unlock();
  
  // pre-ack
  //MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, false);
  //messenger->send_message(reply, op->get_asker());
  
  // do it
  Context *oncommit = new C_OSD_WriteCommit(this, repop);
  op_apply(op, nv, oncommit);

  // local ack
  get_repop(repop);
  assert(repop->waitfor_ack.count(0));
  repop->waitfor_ack.erase(0);
  put_repop(repop);

  if (op->get_op() == OSD_OP_WRITE) {
	logger->inc("c_wr");
	logger->inc("c_wrb", op->get_length());
  }
}



void OSD::op_apply(MOSDOp *op, version_t version, Context* oncommit)
{
  object_t oid = op->get_oid();
  pg_t pgid = op->get_pg();
  
  // if the target object is locked for writing by another client, put 'op' to the waiting queue
  // for _any_ op type -- eg only the locker can unlock!
  if (block_if_wrlocked(op)) 
	return; // op will be handled later, after the object becomes unlocked

  // prepare the transaction
  ObjectStore::Transaction t;
  
  // the op
  switch (op->get_op()) {
  case OSD_OP_WRLOCK:
  case OSD_OP_REP_WRLOCK:
	{ // lock object
	  //r = store->setattr(oid, "wrlock", &op->get_asker(), sizeof(msg_addr_t), oncommit);
	  t.setattr(oid, "wrlock", &op->get_asker(), sizeof(msg_addr_t));
	}
	break;  

  case OSD_OP_WRUNLOCK:
  case OSD_OP_REP_WRUNLOCK:
	{ // unlock objects
	  //r = store->rmattr(oid, "wrlock", oncommit);
	  t.rmattr(oid, "wrlock");
	  
	  // unblock all operations that were waiting for this object to become unlocked
	  if (waiting_for_wr_unlock.count(oid)) {
		take_waiters(waiting_for_wr_unlock[oid]);
		waiting_for_wr_unlock.erase(oid);
	  }
	}
	break;

  case OSD_OP_WRITE:
  case OSD_OP_REP_WRITE:
	{ // write
	  assert(op->get_data().length() == op->get_length());
	  bufferlist bl;
	  //bl = op->get_data();
	  bl.claim( op->get_data() );
	  
	  // go
	  //r = store->write(op->get_oid(),
	  //						   op->get_length(), op->get_offset(),
	  //						   bl, oncommit);
	  t.write( oid, op->get_offset(), op->get_length(), bl );
	}
	break;

  case OSD_OP_TRUNCATE:
  case OSD_OP_REP_TRUNCATE:
	{ // truncate
	  //r = store->truncate(oid, op->get_offset());
	  t.truncate(oid, op->get_length() );
	}
	break;
	
  case OSD_OP_DELETE:
  case OSD_OP_REP_DELETE:
	{ // delete
	  //r = store->remove(oid);
	  t.remove(oid);
	}
	break;
	
  default:
	assert(0);
  }


  // log in pg log
  // FIXME


  // object collection, version
  if (op->get_op() == OSD_OP_TRUNCATE ||
	  op->get_op() == OSD_OP_REP_TRUNCATE) {
	// remove object from c
	t.collection_remove(pgid, oid);
  } else {
	// add object to c
	t.collection_add(pgid, oid);

	// object version
	t.setattr(oid, "version", &version, sizeof(version));
  }

  // inc pg version
  t.collection_setattr(pgid, "version", &version, sizeof(version));
  
  // ok, go!
  unsigned r = store->apply_transaction(t, oncommit);
  if (r == 0 &&   // no errors
	  r == 2) {   // or error on collection_add
	cerr << "error applying transaction: r = " << r << endl;
	assert(r == 0);
  }
}
