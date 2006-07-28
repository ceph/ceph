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

#include "msg/HostMonitor.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDBoot.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"

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
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " "

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

OSD::OSD(int id, Messenger *m, char *dev) 
{
  whoami = id;

  messenger = m;

  osdmap = 0;
  boot_epoch = 0;

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

  if (dev) {
	strcpy(dev_path,dev);
  } else {
	char hostname[100];
	hostname[0] = 0;
	gethostname(hostname,100);
	sprintf(dev_path, "%s/all", ebofs_base_path);
	
	struct stat sta;
	if (::lstat(dev_path, &sta) != 0)
	  sprintf(dev_path, "%s/%d", ebofs_base_path, whoami);
	
	if (::lstat(dev_path, &sta) != 0)
	  sprintf(dev_path, "%s/%s", ebofs_base_path, hostname);	
  }

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

	  // make up a superblock
	  //superblock.fsid = ???;
	  superblock.whoami = whoami;
	}
	
	// mount.
	dout(2) << "mounting " << dev_path << endl;
	int r = store->mount();
	assert(r>=0);

	if (g_conf.osd_mkfs) {
	  // age?
	  if (g_conf.osd_age_time > 0) {
		dout(2) << "age" << endl;
		Ager ager(store);
		ager.age(g_conf.osd_age_time, g_conf.osd_age, g_conf.osd_age / 2.0, 5, g_conf.osd_age);
	  }
	}
	else {
	  dout(2) << "boot" << endl;
	  
	  // read superblock
	  read_superblock();

	  // load up pgs (as they previously existed)
	  load_pgs();

	  dout(2) << "superblock: i am osd" << superblock.whoami << endl;
	  assert(whoami == superblock.whoami);
	}

	// monitor
	char s[80];
	sprintf(s, "osd%d", whoami);
	string st = s;
	monitor = new HostMonitor(messenger, st);
  	monitor->init();

	// <hack> for testing monitoring
	int i = whoami;
	if (++i == g_conf.num_osd) i = 0;
	monitor->get_hosts().insert(MSG_ADDR_OSD(i));
	if (++i == g_conf.num_osd) i = 0;
	monitor->get_hosts().insert(MSG_ADDR_OSD(i));
	if (++i == g_conf.num_osd) i = 0;  
	monitor->get_hosts().insert(MSG_ADDR_OSD(i));
	
	monitor->get_notify().insert(MSG_ADDR_MON(0));
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
	
	// i'm ready!
	messenger->set_dispatcher(this);
	
	// announce to monitor i exist and have booted.
	messenger->send_message(new MOSDBoot(superblock), MSG_ADDR_MON(0));

  }
  osd_lock.Unlock();

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

  // close pgs
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
	   p != pg_map.end();
	   p++) {
	delete p->second;
  }
  pg_map.clear();

  // shut everything else down
  monitor->shutdown();
  messenger->shutdown();

  osd_lock.Unlock();
  int r = store->umount();
  osd_lock.Lock();
  return r;
}



void OSD::write_superblock(ObjectStore::Transaction& t)
{
  dout(10) << "write_superblock " << superblock << endl;

  bufferlist bl;
  bl.append((char*)&superblock, sizeof(superblock));
  t.write(SUPERBLOCK_OBJECT, 0, sizeof(superblock), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(SUPERBLOCK_OBJECT, 0, sizeof(superblock), bl);
  if (bl.length() != sizeof(superblock)) {
	dout(10) << "read_superblock failed, r = " << r << ", i got " << bl.length() << " bytes, not " << sizeof(superblock) << endl;
	return -1;
  }

  bl.copy(0, sizeof(superblock), (char*)&superblock);
  
  dout(10) << "read_superblock " << superblock << endl;

  // load up "current" osdmap
  assert(!osdmap);
  osdmap = new OSDMap;
  bl.clear();
  get_map_bl(superblock.current_epoch, bl);
  osdmap->decode(bl);

  assert(whoami == superblock.whoami);  // fixme!
  return 0;
}


// object locks

PG *OSD::lock_pg(pg_t pgid) 
{
  osd_lock.Lock();
  PG *pg = _lock_pg(pgid);
  osd_lock.Unlock();
  return pg;
}

PG *OSD::_lock_pg(pg_t pgid)
{
  assert(pg_map.count(pgid));

  if (pg_lock.count(pgid)) {
	Cond c;
	dout(15) << "lock_pg " << hex << pgid << dec << " waiting as " << &c << endl;

	list<Cond*>& ls = pg_lock_waiters[pgid];   // this is commit, right?
	ls.push_back(&c);
	
	while (pg_lock.count(pgid) ||
		   ls.front() != &c)
	  c.Wait(osd_lock);

	assert(ls.front() == &c);
	ls.pop_front();
	if (ls.empty())
	  pg_lock_waiters.erase(pgid);
  }

  dout(15) << "lock_pg " << hex << pgid << dec << endl;
  pg_lock.insert(pgid);

  return pg_map[pgid];  
}

void OSD::unlock_pg(pg_t pgid) 
{
  osd_lock.Lock();
  _unlock_pg(pgid);
  osd_lock.Unlock();
}

void OSD::_unlock_pg(pg_t pgid) 
{
  // unlock
  assert(pg_lock.count(pgid));
  pg_lock.erase(pgid);

  if (pg_lock_waiters.count(pgid)) {
	// someone is in line
	Cond *c = pg_lock_waiters[pgid].front();
	assert(c);
	dout(15) << "unlock_pg " << hex << pgid << dec << " waking up next guy " << c << endl;
	c->Signal();
  } else {
	// nobody waiting
	dout(15) << "unlock_pg " << hex << pgid << dec << endl;
  }
}

void OSD::_remove_pg(pg_t pgid) 
{
  dout(10) << "_remove_pg " << hex << pgid << dec << endl;

  // remove from store
  list<object_t> olist;
  store->collection_list(pgid, olist);
  
  ObjectStore::Transaction t;
  {
	for (list<object_t>::iterator p = olist.begin();
		 p != olist.end();
		 p++)
	  t.remove(*p);
	t.remove_collection(pgid);
	t.remove(pgid);  // log too
  }
  store->apply_transaction(t);
  
  // hose from memory
  delete pg_map[pgid];
  pg_map.erase(pgid);
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
		dout(7) << "no OSDMap, not booted" << endl;
		/*if (waiting_for_osdmap.empty()) 
		  messenger->send_message(new MOSDGetMap(sb.current_map),
								  MSG_ADDR_MON(0));
		*/
		waiting_for_osdmap.push_back(m);
		break;
	  }
	  
	  // down?
	  if (osdmap->is_down(whoami)) {
		dout(7) << "i am marked down, dropping " << *m << endl;
		delete m;
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
	  case MSG_OSD_PG_LOG:
		handle_pg_log((MOSDPGLog*)m);
		break;
	  case MSG_OSD_PG_REMOVE:
		handle_pg_remove((MOSDPGRemove*)m);
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
  if (m->get_map_epoch() < boot_epoch) {
	dout(3) << "replica op reply from before boot" << endl;
	delete m;
	return;
  }
  
  if (m->get_source().is_osd() &&
	  osdmap->is_down(m->get_source().num())) {
	dout(3) << "ignoring reply from down " << m->get_source() << endl;
	delete m;
	return;
  }
  

  // handle op
  switch (m->get_op()) {
  case OSD_OP_REP_PULL:
	op_rep_pull_reply(m);
	break;

  case OSD_OP_REP_WRITE:
  case OSD_OP_REP_TRUNCATE:
  case OSD_OP_REP_DELETE:
	{
	  const pg_t pgid = m->get_pg();
	  if (pg_map.count(pgid)) {
		PG *pg = _lock_pg(pgid);
		assert(pg);
		handle_rep_op_ack(pg, m->get_tid(), m->get_result(), m->get_commit(), MSG_ADDR_NUM(m->get_source()),
						  m->get_pg_complete_thru());
		_unlock_pg(pgid);
	  } else {
		// pg dne!  whatev.
	  }
	  delete m;
	}
	break;

  default:
	assert(0);
  }
}

/*
 * NOTE: called holding pg lock      /////osd_lock, opqueue active.
 */
void OSD::handle_rep_op_ack(PG *pg, __uint64_t tid, int result, bool commit, int fromosd, version_t pg_complete_thru)
{
  if (!pg->replica_ops.count(tid)) {
	dout(7) << "not waiting for repop reply tid " << tid << " in " << *pg 
			<< ", map must have changed, dropping." << endl;
	return;
  }
  
  OSDReplicaOp *repop = pg->replica_ops[tid];
  MOSDOp *op = repop->op;

  dout(7) << "handle_rep_op_ack " << tid << " op " << op
		  << " result " << result << " commit " << commit << " from osd" << fromosd
		  << " in " << *pg
		  << endl;

  /* 
   * for now, we take a lazy approach to handling replica set changes
   * that overlap with writes.  replicas with newer maps will reply with
   * result == -1, but we treat them as a success, and ack the write to
   * the client.  this means somewhat weakened safety semantics for the client
   * write, but is much simpler on the osd end.  and no weaker than the rest of the 
   * data in the PG.. or this same write, if it had completed just before the failure.
   *
   * meanwhile, the regular recovery process will handle the object version
   * mismatch.. the new primary (and others) will pull the latest from the old
   * primary.  because of the PGLog stuff, it'll be pretty efficient, aside from
   * the fact that the entire object is copied.
   *
   * one optimization: if the rep_write is received by the new primary, they can
   * (at their discretion) apply it and remove the object from their missing list...
   * or: if a replica sees tha the old primary is not down, it might assume that its
   * state will be recovered (ie the new version) and apply the write.
   */
  if (1) {  //if (result >= 0) {
	// success
	get_repop(repop);
	{
	  if (commit) {
		// commit
		assert(repop->waitfor_commit.count(tid));	  
		repop->waitfor_commit.erase(tid);
		repop->waitfor_ack.erase(tid);

		repop->pg_complete_thru[fromosd] = pg_complete_thru;
		
		pg->replica_ops.erase(tid);
		pg->replica_tids_by_osd[fromosd].erase(tid);
		if (pg->replica_tids_by_osd[fromosd].empty()) pg->replica_tids_by_osd.erase(fromosd);
	  } else {
		// ack
		repop->waitfor_ack.erase(tid);
	  }
	}
	put_repop(repop);
  }

}



void OSD::handle_ping(MPing *m)
{
  dout(7) << "got ping, replying" << endl;
  messenger->send_message(new MPingAck(m),
						  m->get_source(), m->get_source_port(), 0);
  delete m;
}



// =====================================================
// MAP

void OSD::wait_for_new_map(Message *m)
{
  // ask 
  if (waiting_for_osdmap.empty())
	messenger->send_message(new MOSDGetMap(osdmap->get_epoch()),
							MSG_ADDR_MON(0));
  
  waiting_for_osdmap.push_back(m);
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */
void OSD::handle_osd_map(MOSDMap *m)
{
  ObjectStore::Transaction t;
  
  if (osdmap) {
	dout(3) << "handle_osd_map epochs [" 
			<< m->get_first() << "," << m->get_last() 
			<< "], i have " << osdmap->get_epoch()
			<< endl;
  } else {
	dout(3) << "handle_osd_map epochs [" 
			<< m->get_first() << "," << m->get_last() 
			<< "], i have none"
			<< endl;
	osdmap = new OSDMap;
	boot_epoch = m->get_last(); // hrm...?
  }

  // store them?
  for (map<epoch_t,bufferlist>::iterator p = m->maps.begin();
	   p != m->maps.end();
	   p++) {
	object_t oid = get_osdmap_object_name(p->first);
	if (store->exists(oid)) {
	  dout(10) << "handle_osd_map already had full map epoch " << p->first << endl;
	  bufferlist bl;
	  get_map_bl(p->first, bl);
	  dout(10) << " .. it is " << bl.length() << " bytes" << endl;
	  continue;
	}

	dout(10) << "handle_osd_map got full map epoch " << p->first << endl;
	t.write(oid, 0, p->second.length(), p->second);

	if (p->first > superblock.newest_map)
	  superblock.newest_map = p->first;
	if (p->first < superblock.oldest_map ||
		superblock.oldest_map == 0)
	  superblock.oldest_map = p->first;
  }
  for (map<epoch_t,bufferlist>::iterator p = m->incremental_maps.begin();
	   p != m->incremental_maps.end();
	   p++) {
	object_t oid = get_inc_osdmap_object_name(p->first);
	if (store->exists(oid)) {
	  dout(10) << "handle_osd_map already had incremental map epoch " << p->first << endl;
	  bufferlist bl;
	  get_inc_map_bl(p->first, bl);
	  dout(10) << " .. it is " << bl.length() << " bytes" << endl;
	  continue;
	}

	dout(10) << "handle_osd_map got incremental map epoch " << p->first << endl;
	t.write(oid, 0, p->second.length(), p->second);

	if (p->first > superblock.newest_map)
	  superblock.newest_map = p->first;
	if (p->first < superblock.oldest_map ||
		superblock.oldest_map == 0)
	  superblock.oldest_map = p->first;
  }

  // advance if we can
  bool advanced = false;
  epoch_t cur = superblock.current_epoch;
  while (cur < superblock.newest_map) {
	bufferlist bl;
	if (m->incremental_maps.count(cur+1) ||
		store->exists(get_inc_osdmap_object_name(cur+1))) {
	  dout(10) << "handle_osd_map decoding inc map epoch " << cur+1 << endl;
	  
	  bufferlist bl;
	  if (m->incremental_maps.count(cur+1))
		bl = m->incremental_maps[cur+1];
	  else
		get_inc_map_bl(cur+1, bl);

	  OSDMap::Incremental inc;
	  int off = 0;
	  inc.decode(bl, off);

	  osdmap->apply_incremental(inc);

	  // archive the full map
	  bl.clear();
	  osdmap->encode(bl);
	  t.write( get_osdmap_object_name(cur+1), 0, bl.length(), bl);

	  // notify messenger
	  for (map<int,entity_inst_t>::iterator i = inc.new_down.begin();
		   i != inc.new_down.end();
		   i++) {
		if (i->first == whoami) continue;
		messenger->mark_down(MSG_ADDR_OSD(i->first), i->second);
		peer_map_epoch.erase(MSG_ADDR_OSD(i->first));
	  }
	  for (map<int,entity_inst_t>::iterator i = inc.new_up.begin();
		   i != inc.new_up.end();
		   i++) {
		if (i->first == whoami) continue;
		messenger->mark_up(MSG_ADDR_OSD(i->first), i->second);
		peer_map_epoch.erase(MSG_ADDR_OSD(i->first));
	  }
	}
	else if (m->maps.count(cur+1) ||
			 store->exists(get_osdmap_object_name(cur+1))) {
	  dout(10) << "handle_osd_map decoding full map epoch " << cur+1 << endl;
	  bufferlist bl;
	  if (m->maps.count(cur+1))
		bl = m->maps[cur+1];
	  else
		get_map_bl(cur+1, bl);
	  osdmap->decode(bl);

	  // FIXME BUG: need to notify messenger of ups/downs!!
	}
	else {
	  dout(10) << "handle_osd_map missing epoch " << cur+1 << endl;
	  messenger->send_message(new MOSDGetMap(cur), MSG_ADDR_MON(0));
	  break;
	}

	cur++;
	superblock.current_epoch = cur;
	advance_map(t);
	advanced = true;
  }

  // all the way?
  if (advanced && cur == superblock.newest_map) {
	// yay!
	activate_map();
	
	// process waiters
	take_waiters(waiting_for_osdmap);
  }

  // write updated pg state to store
  for (hash_map<pg_t,PG*>::iterator i = pg_map.begin();
	   i != pg_map.end();
	   i++) {
	pg_t pgid = i->first;
	PG *pg = i->second;
	t.collection_setattr( pgid, "info", &pg->info, sizeof(pg->info));
  }

  // superblock and commit
  write_superblock(t);
  store->apply_transaction(t);

  //if (osdmap->get_epoch() == 1) store->sync();     // in case of early death, blah

  delete m;
}


/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t)
{
  dout(7) << "advance_map epoch " << osdmap->get_epoch() 
		  << "  " << pg_map.size() << " pgs"
		  << endl;
  
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
		
		PG *pg = create_pg(pgid, t);
		osdmap->pg_to_acting_osds(pgid, pg->acting);
		pg->set_role(role);
		pg->last_epoch_started_any = 
		  pg->info.last_epoch_started = 
		  pg->info.same_primary_since = 
		  pg->info.same_role_since = osdmap->get_epoch();
		pg->state_set(PG::STATE_ACTIVE);  
		
		dout(7) << "created " << *pg << endl;
	  }

	  // local PG too
	  pg_t pgid = osdmap->osd_nrep_to_pg(whoami, nrep);
	  int role = osdmap->get_pg_acting_role(pgid, whoami);
	  if (role < 0) continue;

	  PG *pg = create_pg(pgid, t);
	  osdmap->pg_to_acting_osds(pgid, pg->acting);
	  pg->set_role(role);
	  pg->last_epoch_started_any = 
		pg->info.last_epoch_started = 
		pg->info.same_primary_since = 
		pg->info.same_role_since = osdmap->get_epoch();
	  pg->state_set(PG::STATE_ACTIVE);  
	  
	  dout(7) << "created " << *pg << endl;
	}

  } else {
	// scan existing pg's
	for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
		 it != pg_map.end();
		 it++) {
	  pg_t pgid = it->first;
	  PG *pg = it->second;
	  
	  // did i finish this epoch?
	  if (pg->is_active()) {
		pg->info.last_epoch_finished = osdmap->get_epoch()-1;
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
	  int oldrole = pg->get_role();
	  int oldprimary = pg->get_primary();
	  vector<int> oldacting = pg->acting;
	  
	  // update PG
	  pg->acting = acting;
	  pg->calc_role(whoami);
	  
	  // did primary change?
	  if (oldprimary != primary) {
		pg->info.same_primary_since = osdmap->get_epoch();

		// forget about where missing items are, or anything we're pulling
		pg->missing.loc.clear();
		pg->objects_pulling.clear();
		pg->requested_thru = 0;
	  }
	  
	  if (role != oldrole) {
		pg->info.same_role_since = osdmap->get_epoch();

		// old primary?
		if (oldrole == 0) {
		  pg->state_clear(PG::STATE_CLEAN);

		  // take waiters
		  take_waiters(pg->waiting_for_active);
		  for (hash_map<object_t, list<Message*> >::iterator it = pg->waiting_for_missing_object.begin();
			   it != pg->waiting_for_missing_object.end();
			   it++)
			take_waiters(it->second);
		  pg->waiting_for_missing_object.clear();
		  
		  // drop peers
		  pg->clear_primary_state();
		}
		
		// new primary?
		if (role == 0) {
		  // i am new primary
		  pg->state_clear(PG::STATE_ACTIVE);
		  pg->state_clear(PG::STATE_STRAY);
		} else {
		  // i am now replica|stray.  we need to send a notify.
		  pg->state_clear(PG::STATE_ACTIVE);
		  pg->state_set(PG::STATE_STRAY);
		  
		  if (nrep == 0) 
			dout(1) << *pg << " is crashed" << endl;
		}
		
		// my role changed.
		dout(10) << *pg << " " << oldacting << " -> " << acting 
				 << ", role " << oldrole << " -> " << role << endl; 
		
	  } else {
		// no role change.
		// did primary change?
		if (primary != oldprimary) {	
		  // we need to announce
		  pg->state_set(PG::STATE_STRAY);
		  pg->state_clear(PG::STATE_ACTIVE);
		  
		  dout(10) << *pg << " " << oldacting << " -> " << acting 
				   << ", acting primary " 
				   << oldprimary << " -> " << primary 
				   << endl;
		} else {
		  // primary is the same.
		  if (role == 0) {
			// i am (still) primary. but my replica set changed.
			pg->state_clear(PG::STATE_ACTIVE);
			pg->state_clear(PG::STATE_CLEAN);

			dout(10) << *pg << " " << oldacting << " -> " << acting
					 << ", replicas changed" << endl;

			// clear peer_info for (re-)new replicas
			for (unsigned i=1; i<acting.size(); i++) {
			  bool had = false;
			  for (unsigned j=1; j<oldacting.size(); j++)
				if (acting[i] == oldacting[j]) { 
				  had = true; 
				  break;
				}
			  if (!had) {
				dout(10) << *pg << " hosing any peer state for new replica osd" << acting[i] << endl;
				pg->peer_info.erase(acting[i]);
				pg->peer_info_requested.erase(acting[i]);
				pg->peer_missing.erase(acting[i]);
				pg->peer_log_requested.erase(acting[i]);
				pg->peer_summary_requested.erase(acting[i]);
			  }
			}
		  }
		}
	  }
	  
	  // scan (FIXME newly!) down osds  
	  for (set<int>::const_iterator down = osdmap->get_down_osds().begin();
		   down != osdmap->get_down_osds().end();
		   down++) {
		if (*down == whoami) continue;

		// old peer?
		bool have = false;
		for (unsigned i=0; i<oldacting.size(); i++)
		  if (oldacting[i] == *down) have = true;
		if (!have) continue;
		
		dout(10) << *pg << " old peer osd" << *down << " is down" << endl;
		
		// NAK any ops to the down osd
		if (pg->replica_tids_by_osd.count(*down)) {
		  set<__uint64_t> s = pg->replica_tids_by_osd[*down];
		  dout(10) << " " << *pg << " naking replica ops to down osd" << *down << " " << s << endl;
		  for (set<__uint64_t>::iterator tid = s.begin();
			   tid != s.end();
			   tid++)
			handle_rep_op_ack(pg, *tid, -1, true, *down);
		}
	  }
	}
  }
}

void OSD::activate_map()
{
  dout(7) << "activate_map version " << osdmap->get_epoch() << endl;

  map< int, list<PG::PGInfo> >  notify_list;  // primary -> list
  map< int, map<pg_t,version_t> > query_map;    // peer -> PG -> get_summary_since

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
	   it != pg_map.end();
	   it++) {
	//pg_t pgid = it->first;
	PG *pg = it->second;

	if (pg->is_active()) {
	  // update started counter
	  pg->info.last_epoch_started = osdmap->get_epoch();
	} 
	else if (pg->get_role() == 0 && !pg->is_active()) {
	  // i am (inactive) primary
	  pg->build_prior();
	  pg->peer(query_map);
	}
	else if (pg->is_stray()) {
	  // i am residual|replica
	  notify_list[pg->get_primary()].push_back(pg->info);
	}

  }  

  if (osdmap->is_mkfs())    // hack: skip the queries/summaries if it's a mkfs
	return;

  // notify? (residual|replica)
  do_notifies(notify_list);
  
  // do queries.
  do_queries(query_map);
}


void OSD::send_incremental_map(epoch_t since, msg_addr_t dest, bool full)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
		   << " to " << dest << endl;
  
  MOSDMap *m = new MOSDMap;
  
  for (epoch_t e = osdmap->get_epoch();
	   e > since;
	   e--) {
	bufferlist bl;
	if (get_inc_map_bl(e,bl)) {
	  m->incremental_maps[e].claim(bl);
	} else if (get_map_bl(e,bl)) {
	  m->maps[e].claim(bl);
	  if (!full) break;
	}
	else {
	  assert(0);  // we should have all maps.
	}
  }

  messenger->send_message(m, dest);
}

bool OSD::get_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(get_osdmap_object_name(e), 0, 0, bl) >= 0;
}

bool OSD::get_map(epoch_t e, OSDMap &m)
{
  bufferlist bl;
  if (!get_map_bl(e, bl)) 
	return false;
  m.decode(bl);
  return true;
}

bool OSD::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(get_inc_osdmap_object_name(e), 0, 0, bl) >= 0;
}

bool OSD::get_inc_map(epoch_t e, OSDMap::Incremental &inc)
{
  bufferlist bl;
  if (!get_inc_map_bl(e, bl)) 
	return false;
  int off = 0;
  inc.decode(bl, off);
  return true;
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
  // newer map?
  if (epoch > osdmap->get_epoch()) {
	dout(7) << "  from newer map epoch " << epoch << " > " << osdmap->get_epoch() << endl;
	wait_for_new_map(m);
	return false;
  }

  if (epoch < boot_epoch) {
	dout(7) << "  from pre-boot epoch " << epoch << " < " << boot_epoch << endl;
	delete m;
	return false;
  }

  // down osd?
  if (m->get_source().is_osd() &&
	  osdmap->is_down(m->get_source().num())) {
	if (epoch > osdmap->get_epoch()) {
	  dout(7) << "msg from down " << m->get_source()
			  << ", waiting for new map" << endl;
	  wait_for_new_map(m);
	} else {
	  dout(7) << "msg from down " << m->get_source()
			  << ", dropping" << endl;
	  delete m;
	}
	return false;
  }

  return true;
}




// ======================================================
// REPLICATION

// PG

bool OSD::pg_exists(pg_t pgid) 
{
  return store->collection_exists(pgid);
}

PG *OSD::create_pg(pg_t pgid, ObjectStore::Transaction& t)
{
  assert(pg_map.count(pgid) == 0);
  assert(!pg_exists(pgid));

  PG *pg = new PG(this, pgid);
  pg_map[pgid] = pg;

  t.create_collection(pgid);

  return pg;
}


/*
void OSD::get_pg_list(list<pg_t>& ls)
{
  // just list collections; assume they're all pg's (for now)
  store->list_collections(ls);
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
  pg_map[pgid] = pg;

  // read pg info
  store->collection_getattr(pgid, "info", &pg->info, sizeof(pg->info));
  
  // read pg log
  pg->read_log(store);

  return pg;
}
*/

PG *OSD::get_pg(pg_t pgid)
{
  if (pg_map.count(pgid))
	return pg_map[pgid];
  return 0;
}

void OSD::load_pgs()
{
  dout(10) << "load_pgs" << endl;
  assert(pg_map.empty());

  list<pg_t> ls;
  store->list_collections(ls);

  for (list<pg_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	pg_t pgid = *it;

	PG *pg = new PG(this, pgid);
	pg_map[pgid] = pg;

	// read pg info
	store->collection_getattr(pgid, "info", &pg->info, sizeof(pg->info));
	
	// read pg log
	pg->read_log(store);

	// generate state for current mapping
	//int nrep = 
	osdmap->pg_to_acting_osds(pgid, pg->acting);
	int role = -1;
	for (unsigned i=0; i<pg->acting.size(); i++)
	  if (pg->acting[i] == whoami) role = i>0 ? 1:0;
	pg->set_role(role);

	dout(10) << "load_pgs loaded " << *pg << endl;
  }
}
 
/**
 * check epochs starting from start to verify the primary hasn't changed
 * up until now
 */
epoch_t OSD::calc_pg_primary_since(int primary, pg_t pgid, epoch_t start)
{
  for (epoch_t e = osdmap->get_epoch()-1;
	   e >= start;
	   e--) {
	// verify during intermediate epoch
	vector<int> acting;
	
	OSDMap oldmap;
	get_map(e, oldmap);
	oldmap.pg_to_acting_osds(pgid, acting);

	if (acting[0] != primary) 
	  return e+1;  // nope, primary only goes back through e!
  }

  return start;  // same all the way back thru start!
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
	if (it->first == whoami) {
	  dout(7) << "do_notify osd" << it->first << " is self, skipping" << endl;
	  continue;
	}
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
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_notify(MOSDPGNotify *m)
{
  dout(7) << "handle_pg_notify from " << m->get_source() << endl;
  int from = MSG_ADDR_NUM(m->get_source());

  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  ObjectStore::Transaction t;
  
  // look for unknown PGs i'm primary for
  map< int, map<pg_t,version_t> > query_map;

  for (list<PG::PGInfo>::iterator it = m->get_pg_list().begin();
	   it != m->get_pg_list().end();
	   it++) {
	pg_t pgid = it->pgid;
	PG *pg;

	if (pg_map.count(pgid) == 0) {
	  // check mapping.
	  vector<int> acting;
	  //int nrep = 
	  osdmap->pg_to_acting_osds(pgid, acting);
	  //assert(nrep > 0);
	  
	  // am i still the primary?
	  assert(it->same_primary_since <= osdmap->get_epoch());
	  if (acting[0] != whoami) {
		// not primary now, so who cares!
		dout(10) << "handle_pg_notify pg " << hex << pgid << dec << " dne, and i'm not the primary" << endl;
		continue;
	  } else {
		// ok, well, i'm primary now... was it continuous since caller's epoch?
		epoch_t since = calc_pg_primary_since(whoami, pgid, m->get_epoch());
		if (since > m->get_epoch()) {
		  dout(10) << "handle_pg_notify pg " << hex << pgid << dec << " dne, and i wasn't primary during intermediate epoch " << since
				   << " (caller " << m->get_epoch() << " < " << since << " < now " << osdmap->get_epoch() << ")" << endl;
		  continue;
		}
	  }
	  
	  // ok, create PG!
	  pg = create_pg(pgid, t);
	  pg->acting = acting;
	  pg->info.same_primary_since = it->same_primary_since;
	  pg->set_role(0);
	  pg->info.same_role_since = osdmap->get_epoch();

	  pg->last_epoch_started_any = it->last_epoch_started;
	  pg->build_prior();

	  t.collection_setattr(pgid, "info", (char*)&pg->info, sizeof(pg->info));
	  
	  dout(10) << *pg << " is new" << endl;
	
	  // kick any waiters
	  if (waiting_for_pg.count(pgid)) {
		take_waiters(waiting_for_pg[pgid]);
		waiting_for_pg.erase(pgid);
	  }

	  pg = _lock_pg(pgid);
	} else {
	  // already had it.  am i (still) the primary?
	  pg = _lock_pg(pgid);
	  if (pg->is_primary()) {
		if (pg->info.same_primary_since > m->get_epoch()) {
		  dout(10) << *pg << " requestor epoch " << m->get_epoch() 
				   << " < my primary start epoch " << pg->info.same_primary_since 
				   << endl;
		  _unlock_pg(pgid);
		  continue;
		}
	  } else {
		dout(10) << *pg << " not primary" << endl;
		assert(m->get_epoch() < osdmap->get_epoch());
		_unlock_pg(pgid);
		continue;
	  }
	}

	// ok!
	
	// stray?
	bool acting = pg->is_acting(from);
	if (!acting && (*it).last_epoch_started > 0) {
	  dout(10) << *pg << " osd" << from << " has stray content: " << *it << endl;
	  pg->stray_set.insert(from);
	  pg->state_clear(PG::STATE_CLEAN);
	}

	// save info.
	bool had = pg->peer_info.count(from);
	pg->peer_info[from] = *it;

	if (had) {
	  if ((*it).is_clean() && acting) {
		pg->clean_set.insert(from);
		dout(10) << *pg << " osd" << from << " now clean (" << pg->clean_set  
				 << "): " << *it << endl;
		if (pg->is_all_clean()) {
		  pg->state_set(PG::STATE_CLEAN);
		  pg->clean_replicas();
		}
	  } else {
		// hmm, maybe keep an eye out for cases where we see this, but peer should happen.
		dout(10) << *pg << " already had notify info from osd" << from << ": " << *it << endl;
	  }
	} else {
	  // adjust prior?
	  if (it->last_epoch_started > pg->last_epoch_started_any) 
		pg->adjust_prior();
	  
	  // peer
	  pg->peer(query_map);
	}

	_unlock_pg(pgid);
  }
  
  unsigned tr = store->apply_transaction(t);
  assert(tr == 0);

  do_queries(query_map);
  
  delete m;
}



/** PGLog
 * from non-primary to primary
 *  includes log and info
 * from primary to non-primary
 *  includes log for use in recovery
 * NOTE: called with opqueue active.
 */

void OSD::handle_pg_log(MOSDPGLog *m) 
{
  int from = MSG_ADDR_NUM(m->get_source());
  const pg_t pgid = m->get_pgid();

  if (!require_same_or_newer_map(m, m->get_epoch())) return;
  if (pg_map.count(pgid) == 0) {
	dout(10) << "handle_pg_log don't have pg " << hex << pgid << dec << ", dropping" << endl;
	assert(m->get_epoch() < osdmap->get_epoch());
	delete m;
	return;
  }

  PG *pg = _lock_pg(pgid);
  assert(pg);

  dout(7) << "handle_pg_log " << *pg 
		  << " got " << m->log
		  << " from " << m->get_source() << endl;

  ObjectStore::Transaction t;

  if (pg->acting[0] == whoami) {
	// i am PRIMARY
	assert(pg->peer_log_requested.count(from) ||
		   pg->peer_summary_requested.count(from));
	
	// note peer's missing.
	pg->peer_missing[from] = m->missing;

	// merge into our own log
	pg->merge_log(m->log);
	
	// and did i find anything?
	for (map<object_t, version_t>::iterator p = pg->missing.missing.begin();
		 p != pg->missing.missing.end();
		 p++) {
	  const object_t oid = p->first;
	  const version_t v = p->second;
	  if (v <= m->info.last_complete ||            // peer is complete through stamp?
		  (m->log.updated.count(oid) && 
		   m->log.updated[oid] == v &&  
		   m->missing.missing.count(oid) == 0)) {  // or logged it and aren't missing it?
		pg->missing.loc[oid] = from;               // ...then they have it!
	  }
	}
	dout(10) << *pg << " missing now " << pg->missing << endl;
	
	pg->clean_up_local();
	
	// peer
	map< int, map<pg_t,version_t> > query_map;
	pg->peer(query_map);
	do_queries(query_map);

  } else {
	// i am REPLICA
	dout(10) << *pg << " got " << m->log << " " << m->missing << endl;

	// merge log
	pg->merge_log(m->log);

	// locate missing items
	if (pg->missing.num_lost() > 0) {
	  // see if we can find anything new!
	  for (map<object_t,version_t>::iterator p = pg->missing.missing.begin();
		   p != pg->missing.missing.end();
		   p++) {
		const object_t oid = p->first;
		if (m->missing.loc.count(oid)) {
		  assert(m->missing.missing[oid] >= pg->missing.missing[oid]);
		  pg->missing.loc[oid] = m->missing.loc[oid];
		} else {
		  pg->missing.loc[oid] = from;
		}
	  }
	}

	dout(10) << *pg << " missing now " << pg->missing << endl;
	assert(pg->missing.num_lost() == 0);

	// clean up any stray objects
	pg->clean_up_local();

	// ok active!
	pg->info.last_epoch_started = osdmap->get_epoch();
	pg->info.same_primary_since = m->info.same_primary_since;
	pg->state_set(PG::STATE_ACTIVE);

	// take any waiters
	take_waiters(pg->waiting_for_active);

	// initiate any recovery?
	pg->start_recovery();
  }

  pg->write_log(t);
  unsigned tr = store->apply_transaction(t);
  assert(tr == 0);

  _unlock_pg(pgid);

  delete m;
}


/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
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
	PG *pg;

	if (pg_map.count(pgid) == 0) {
	  // get active rush mapping
	  vector<int> acting;
	  //int nrep = 
	  osdmap->pg_to_acting_osds(pgid, acting);
	  //assert(nrep > 0);
	  int role = -1;
	  for (unsigned i=0; i<acting.size(); i++)
		if (acting[i] == whoami) role = i>0 ? 1:0;

	  if (role == 0) {
		dout(10) << " pg " << hex << pgid << dec << " dne, and i am primary.  just waiting for notify." << endl;
		continue;
	  }
	  if (role < 0) {
		dout(10) << " pg " << hex << pgid << dec << " dne, and i am not an active replica" << endl;
		PG::PGInfo empty(pgid);
		notify_list[from].push_back(empty);
		continue;
	  }
	  
	  ObjectStore::Transaction t;
	  PG *pg = create_pg(pgid, t);
	  pg->acting = acting;
	  pg->set_role(role);
	  
	  pg->info.same_primary_since = calc_pg_primary_since(acting[0], pgid, m->get_epoch());
	  pg->info.same_role_since = osdmap->get_epoch();

	  t.collection_setattr(pgid, "info", (char*)&pg->info, sizeof(pg->info));
	  store->apply_transaction(t);

	  dout(10) << *pg << " dne (before), but i am role " << role << endl;
	}
	pg = _lock_pg(pgid);
	
	// verify this is from same primary
	if (pg->is_primary()) { 
	  dout(10) << *pg << " i am primary, skipping" << endl;
	  _unlock_pg(pgid);
	  continue;
 	} else {
	  if (from == pg->acting[0]) {
		if (m->get_epoch() < pg->info.same_primary_since) {
		  dout(10) << *pg << " not same primary since " << m->get_epoch() << ", skipping" << endl;
		  _unlock_pg(pgid);
		  continue;
		}
	  } else {
		dout(10) << *pg << " query not from primary, skipping" << endl;
		assert(m->get_epoch() < osdmap->get_epoch());
		_unlock_pg(pgid);
		continue;
	  }
	}

	if (it->second == PG_QUERY_INFO) {
	  // info
	  dout(10) << *pg << " sending info" << endl;
	  notify_list[from].push_back(pg->info);
	} else {
	  MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->get_pgid());
	  m->info = pg->info;
	  
	  if (it->second == PG_QUERY_SUMMARY) {
		dout(10) << *pg << " sending info+summary/backlog" << endl;
		m->log = pg->log;
		if (!m->log.backlog) pg->generate_backlog(m->log);
	  } 
	  else if (it->second == 0) {
		dout(10) << *pg << " sending info+entire log" << endl;
		m->log = pg->log;
	  } 
	  else {
		version_t since = MAX(it->second, pg->log.bottom);
		dout(10) << *pg << " sending info+log since " << since
				 << " (asked since " << it->second << ")" << endl;
		m->log.copy_after(pg->log, since);
	  }
	  m->missing = pg->missing;

	  messenger->send_message(m, MSG_ADDR_OSD(from));
	}	

	_unlock_pg(pgid);
  }
  
  do_notifies(notify_list);   

  delete m;
}


void OSD::handle_pg_remove(MOSDPGRemove *m)
{
  dout(7) << "handle_pg_query from " << m->get_source() << endl;
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  for (set<pg_t>::iterator it = m->pg_list.begin();
	   it != m->pg_list.end();
	   it++) {
	pg_t pgid = *it;
	PG *pg;

	if (pg_map.count(pgid) == 0) {
	  dout(10) << " don't have pg " << hex << pgid << dec << endl;
	  continue;
	}

	pg = _lock_pg(pgid);

	dout(10) << *pg << " removing." << endl;
	assert(pg->get_role() == -1);
	
	_remove_pg(pgid);

	// unlock.  there shouldn't be any waiters, since we're a stray, and pg is presumably clean.
	assert(pg_lock_waiters.count(pgid) == 0);
	_unlock_pg(pgid);
  }

  delete m;
}







// RECOVERY





// pull


void OSD::pull(PG *pg, object_t oid, version_t v)
{
  assert(pg->missing.loc.count(oid));
  int osd = pg->missing.loc[oid];
  
  dout(7) << "pull " << hex << oid << dec 
		  << " v " << v 
		  << " from osd" << osd
		  << " in " << *pg
		  << endl;

  // send op
  tid_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
						  oid, pg->get_pgid(),
						  osdmap->get_epoch(),
						  OSD_OP_REP_PULL);
  op->set_version(v);
  op->set_pg_role(-1);  // whatever, not 0
  messenger->send_message(op, MSG_ADDR_OSD(osd));
  
  // take note
  assert(pg->objects_pulling.count(oid) == 0);
  pg->objects_pulling[oid] = v;
}


/** op_rep_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void OSD::op_rep_pull(MOSDOp *op, PG *pg)
{
  long got = 0;
  
  const object_t oid = op->get_oid();

  dout(7) << "rep_pull on " << hex << oid << dec << " v >= " << op->get_version() << endl;

  // read data+attrs
  bufferlist bl;
  version_t v;
  int vlen = sizeof(v);
  map<string,bufferptr> attrset;

  ObjectStore::Transaction t;
  t.read(oid, 0, 0, &bl);
  t.getattr(oid, "version", &v, &vlen);
  t.getattrs(oid, attrset);
  unsigned tr = store->apply_transaction(t);

  if (tr != 0) {
	// reply with -EEXIST
	dout(7) << "rep_pull don't have " << hex << oid << dec << endl;  
	MOSDOpReply *reply = new MOSDOpReply(op, -EEXIST, osdmap->get_epoch(), true); 
	messenger->send_message(reply, op->get_asker());
	delete op;
	return;
  }
  
  dout(7) << "rep_pull has " 
		  << hex << op->get_oid() << dec 
		  << " v " << v << " >= " << op->get_version()
		  << " size " << bl.length()
		  << " in " << *pg
		  << endl;
  assert(v >= op->get_version());
  
  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap->get_epoch(), true); 
  reply->set_result(0);
  reply->set_length(bl.length());
  reply->set_data(bl);   // note: claims bl, set length above here!
  reply->set_offset(0);
  reply->set_version(v);
  reply->set_attrset(attrset);
  
  messenger->send_message(reply, op->get_asker());
  
  delete op;

  logger->inc("r_pull");
  logger->inc("r_pullb", got);
}


/*
 * NOTE: called holding osd_lock.  opqueue active.
 */
void OSD::op_rep_pull_reply(MOSDOpReply *op)
{
  object_t oid = op->get_oid();
  version_t v = op->get_version();
  pg_t pgid = op->get_pg();

  if (pg_map.count(pgid) == 0) {
	dout(7) << "rep_pull_reply on pg " << hex << pgid << dec << ", dne" << endl;
	return;
  }

  PG *pg = _lock_pg(pgid);

  if (!pg->objects_pulling.count(oid)) {
	dout(7) << "rep_pull_reply on object " << hex << oid << dec << ", not pulling" << endl;	
	_unlock_pg(pgid);
	return;
  }
  
  dout(7) << "rep_pull_reply " 
		  << hex << oid << dec 
		  << " v " << v 
		  << " size " << op->get_length() << " " << op->get_data().length()
		  << " in " << *pg
		  << endl;

  assert(op->get_data().length() == op->get_length());

  // write object and add it to the PG
  ObjectStore::Transaction t;
  t.remove(oid);  // in case old version exists
  t.write(oid, 0, op->get_length(), op->get_data());
  t.setattrs(oid, op->get_attrset());
  t.collection_add(pgid, oid);
  unsigned r = store->apply_transaction(t);
  assert(r == 0);

  // close out pull op.
  pg->objects_pulling.erase(oid);
  pg->missing.got(oid, v);

  // kick waiters
  if (pg->waiting_for_missing_object.count(oid)) 
	take_waiters(pg->waiting_for_missing_object[oid]);

  // raise last_complete?
  map<version_t, object_t>::iterator p;
  for (p = pg->log.rupdated.lower_bound(pg->info.last_complete);
	   p != pg->log.rupdated.end() && pg->missing.missing.count(p->second) == 0;
	   p++) 
	if (p->first > pg->info.last_complete) 
	  pg->info.last_complete = p->first;
  dout(10) << *pg << " last_complete now " << pg->info.last_complete << endl;
  
  if (pg->missing.num_missing() == 0) {
	assert(pg->info.last_complete == pg->info.last_update);
	
	if (pg->is_primary()) {
	  // i am primary
	  pg->clean_set.insert(whoami);
	  if (pg->is_all_clean()) {
		pg->state_set(PG::STATE_CLEAN);
		pg->clean_replicas();
	  }
	} else {
	  // tell primary
	  dout(7) << *pg << " recovery complete, telling primary" << endl;
	  list<PG::PGInfo> ls;
	  ls.push_back(pg->info);
	  messenger->send_message(new MOSDPGNotify(osdmap->get_epoch(),
											   ls),
							  MSG_ADDR_OSD(pg->get_primary()));
	}
  } else {
	// continue
	pg->do_recovery();
  }
 
  _unlock_pg(pgid);

  delete op;
}




// op_rep_modify

// commit (to disk) callback
class C_OSD_RepModifyCommit : public Context {
public:
  OSD *osd;
  MOSDOp *op;

  version_t pg_last_complete;

  Mutex lock;
  Cond cond;
  bool acked;
  bool waiting;

  C_OSD_RepModifyCommit(OSD *o, MOSDOp *oo, version_t lc) : osd(o), op(oo), pg_last_complete(lc),
															acked(false), waiting(false) { }
  void finish(int r) {
	lock.Lock();
	while (!acked) {
	  waiting = true;
	  cond.Wait(lock);
	}
	assert(acked);
	lock.Unlock();
	osd->op_rep_modify_commit(op, pg_last_complete);
  }
  void ack() {
	lock.Lock();
	acked = true;
	if (waiting) cond.Signal();
	lock.Unlock();
  }
};

void OSD::op_rep_modify_commit(MOSDOp *op, version_t last_complete)
{
  // send commit.
  dout(10) << "rep_modify_commit on op " << *op << endl;
  MOSDOpReply *commit = new MOSDOpReply(op, 0, osdmap->get_epoch(), true);
  commit->set_pg_complete_thru(last_complete);
  messenger->send_message(commit, op->get_asker());
  delete op;
}

// process a modification operation

class C_OSD_WriteCommit : public Context {
public:
  OSD *osd;
  OSDReplicaOp *repop;
  version_t pg_last_complete;
  C_OSD_WriteCommit(OSD *o, OSDReplicaOp *op, version_t lc) : osd(o), repop(op), pg_last_complete(lc) {}
  void finish(int r) {
	osd->op_modify_commit(repop, pg_last_complete);
  }
};


/** op_rep_modify
 * process a replicated modify.
 * NOTE: called from opqueue.
 */
void OSD::op_rep_modify(MOSDOp *op, PG *pg)
{ 
  object_t oid = op->get_oid();
  version_t nv = op->get_version();

  ObjectStore::Transaction t;

  // update PG log
  if (pg->info.last_update < nv)
	prepare_log_transaction(t, op, nv, pg, op->get_pg_trim_to());
  // else, we are playing catch-up, don't update pg metadata!  (FIXME?)

  // do op?
  C_OSD_RepModifyCommit *oncommit = 0;
  
  // check current version
  version_t myv = 0;
  store->getattr(oid, "version", &myv, sizeof(myv));  // this is a noop if oid dne
  dout(10) << "op_rep_modify existing " << hex << oid << dec << " v " << myv << endl;

  // is this an old update?
  if (nv <= myv) {
	// we have a newer version.  pretend we do a regular commit!
	dout(10) << "op_rep_modify on " << hex << oid << dec 
			 << " v " << nv << " <= myv, noop"
			 << " in " << *pg 
			 << endl;
	oncommit = new C_OSD_RepModifyCommit(this, op,
										 pg->info.last_complete);
  }

  // missing?
  else if (pg->missing.missing.count(oid)) {
	// old or missing.  wait!
	dout(10) << "op_rep_modify on " << hex << oid << dec 
			 << " v " << nv << " > myv, wait"
			 << " in " << *pg 
			 << endl;
	if (pg->missing.missing[oid] > op->get_version())
	  pg->missing.add(oid, op->get_version());  // now we're missing the _newer_ version.
	waitfor_missing_object(op, pg);
  } 
  else {
	// we're good.
	dout(10) << "op_rep_modify on " << hex << oid << dec 
			 << " v " << nv << " (from " << myv << ")"
			 << " in " << *pg 
			 << endl;
	assert(op->get_old_version() == myv);
	
	prepare_op_transaction(t, op, nv, pg);
	oncommit = new C_OSD_RepModifyCommit(this, op,
										 pg->info.last_complete);
  }

  // go
  unsigned tr = store->apply_transaction(t, oncommit);
  if (tr != 0 &&   // no errors
	  tr != 2) {   // or error on collection_add
	cerr << "error applying transaction: r = " << tr << endl;
	assert(tr == 0);
  }

  // ack?
  if (oncommit) {
	// ack
	MOSDOpReply *ack = new MOSDOpReply(op, 0, osdmap->get_epoch(), false);
	messenger->send_message(ack, op->get_asker());
	oncommit->ack(); 
  }
}


// =========================================================
// OPS

void OSD::handle_op(MOSDOp *op)
{
  const pg_t pgid = op->get_pg();
  int acting_primary = osdmap->get_pg_acting_primary( pgid );
  PG *pg = get_pg(pgid);

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) return;

  // does client have old map?
  if (op->get_source().is_client()) {
	if (op->get_map_epoch() < osdmap->get_epoch()) {
	  dout(10) << "handle_op client has old map " << op->get_map_epoch() << " < " << osdmap->get_epoch() << endl;
	  send_incremental_map(op->get_map_epoch(), op->get_source(), false);
	}
  }

  // does peer have old map?
  if (op->get_source().is_osd()) {
	// remember
	if (peer_map_epoch[op->get_source()] < op->get_map_epoch())
	  peer_map_epoch[op->get_source()] = op->get_map_epoch();
	  
	// older?
	if (peer_map_epoch[op->get_source()] < osdmap->get_epoch()) {
	  dout(10) << "handle_op osd has old map " << op->get_map_epoch() << " < " << osdmap->get_epoch() << endl;
	  send_incremental_map(op->get_map_epoch(), op->get_source(), true);
	  peer_map_epoch[op->get_source()] = osdmap->get_epoch();  // so we don't send it again.
	}
  }

  // crashed?
  if (acting_primary < 0) {
	dout(1) << "crashed pg " << hex << pgid << dec << endl;
	messenger->send_message(new MOSDOpReply(op, -EIO, osdmap->get_epoch(), true),
							op->get_asker());
	delete op;
	return;
  }
  
  // what kind of op?
  if (!OSD_OP_IS_REP(op->get_op())) {
	// REGULAR OP (non-replication)

	// am i the primary?
	if (acting_primary != whoami) {
	  dout(7) << "acting primary is osd" << acting_primary
			  << ", replying with -EAGAIN" << endl;
	  assert(op->get_map_epoch() < osdmap->get_epoch());
	  
	  // tell client.
	  MOSDOpReply *fail = new MOSDOpReply(op, -EAGAIN, osdmap->get_epoch(), true);  // FIXME error code?
	  messenger->send_message(fail, op->get_asker());

	  /*
	  dout(7) << "acting primary is osd" << acting_primary 
			  << ", forwarding" << endl;
	  messenger->send_message(op, MSG_ADDR_OSD(acting_primary), 0);
	  logger->inc("fwd");
	  */
	  return;
	}

	// have pg?
	if (!pg) {
	  dout(7) << "hit non-existent pg " 
			  << hex << pgid << dec 
			  << ", waiting" << endl;
	  waiting_for_pg[pgid].push_back(op);
	  return;
	}
	
	// must be active.
	if (!pg->is_active()) {
	  dout(7) << *pg << " not active (yet)" << endl;
	  pg->waiting_for_active.push_back(op);
	  return;
	}

	dout(7) << "handle_op " << op << " in " << *pg << endl;
	
  } else {
	// REPLICATION OP

	// have pg?
	if (!pg) {
	  dout(7) << "handle_rep_op " << op 
			  << " in pgid " << hex << pgid << dec << endl;
	  waiting_for_pg[pgid].push_back(op);
	  return;
	}

    // check osd map.  same primary?
	if (op->get_map_epoch() != osdmap->get_epoch()) {
	  // make sure source is still primary
	  const int myrole = pg->get_role();  //osdmap->get_pg_acting_role(op->get_pg(), whoami);
	  
	  if (acting_primary != MSG_ADDR_NUM(op->get_source()) ||
		  myrole <= 0 ||
		  op->get_map_epoch() < pg->info.same_primary_since) {
		dout(5) << "op map " << op->get_map_epoch() << " != " << osdmap->get_epoch()
				<< ", primary changed on pg " << hex << op->get_pg() << dec
				<< endl;
		MOSDOpReply *fail = new MOSDOpReply(op, -EAGAIN, osdmap->get_epoch(), true);  // FIXME error code?
		messenger->send_message(fail, op->get_asker());
		return;
	  }
	  
	  dout(5) << "op map " << op->get_map_epoch() << " != " << osdmap->get_epoch()
			  << ", primary same on pg " << hex << op->get_pg() << dec
			  << endl;
	}
	
	dout(7) << "handle_rep_op " << op << " in " << *pg << endl;
  }
  
  if (g_conf.osd_maxthreads < 1) {
	do_op(op, pg); // do it now
  } else {
	enqueue_op(pgid, op); 	// queue for worker threads
  }
}


/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(pg_t pgid, MOSDOp *op)
{
  while (pending_ops > g_conf.osd_max_opq) {
	dout(10) << "enqueue_op waiting for pending_ops " << pending_ops << " to drop to " << g_conf.osd_max_opq << endl;
	op_queue_cond.Wait(osd_lock);
  }

  op_queue[pgid].push_back(op);
  pending_ops++;
  logger->set("opq", pending_ops);
  
  threadpool->put_op(pgid);
}

/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void OSD::dequeue_op(pg_t pgid)
{
  MOSDOp *op;
  PG *pg;

  osd_lock.Lock();
  {
	// lock pg
	pg = _lock_pg(pgid);  

	// get pending op
	list<MOSDOp*> &ls  = op_queue[pgid];
	assert(!ls.empty());
	op = ls.front();
	ls.pop_front();

	dout(10) << "dequeue_op pg " << hex << pgid << dec << " op " << op << ", " 
			 << ls.size() << " / " << (pending_ops-1) << " more pending" << endl;
	
	if (ls.empty())
	  op_queue.erase(pgid);
  }
  osd_lock.Unlock();
  
  // do it
  do_op(op, pg);

  // unlock pg
  unlock_pg(pgid);
  
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
void OSD::do_op(MOSDOp *op, PG *pg) 
{
  dout(10) << "do_op " << *op
	//<< " on " << hex << op->get_oid() << dec
		   << " in " << *pg << endl;

  logger->inc("op");

  // replication ops?
  if (OSD_OP_IS_REP(op->get_op())) {
	// replication/recovery
	switch (op->get_op()) {
	case OSD_OP_REP_PULL:
	  op_rep_pull(op, pg);
	  break;

	  // replicated ops
	case OSD_OP_REP_WRITE:
	case OSD_OP_REP_TRUNCATE:
	case OSD_OP_REP_DELETE:
	  op_rep_modify(op, pg);
	  break;

	default:
	  assert(0);	  
	}
  } else {
	// regular op
	switch (op->get_op()) {
	case OSD_OP_READ:
	  op_read(op, pg);
	  break;
	case OSD_OP_STAT:
	  op_stat(op, pg);
	  break;
	case OSD_OP_WRITE:
	case OSD_OP_ZERO:
	case OSD_OP_DELETE:
	case OSD_OP_TRUNCATE:
	  op_modify(op, pg);
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

bool OSD::waitfor_missing_object(MOSDOp *op, PG *pg)
{
  const object_t oid = op->get_oid();

  // are we missing the object?
  if (pg->missing.missing.count(oid)) {
	// we don't have it (yet).
	version_t v = pg->missing.missing[oid];
	if (pg->objects_pulling.count(oid)) {
	  dout(7) << "missing "
			  << hex << oid << dec 
			  << " v " << v
			  << " in " << *pg
			  << ", already pulling"
			  << endl;
	} else {
	  dout(7) << "missing " 
			  << hex << oid << dec 
			  << " v " << v
			  << " in " << *pg
			  << ", pulling"
			  << endl;
	  pull(pg, oid, v);
	}
	pg->waiting_for_missing_object[oid].push_back(op);
	return true;
  }

  return false;
}


// READ OPS

/** op_read
 * client read op
 * NOTE: called from opqueue.
 */
void OSD::op_read(MOSDOp *op, PG *pg)
{
  const object_t oid = op->get_oid();
  
  if (waitfor_missing_object(op, pg)) return;
  
  // if the target object is locked for writing by another client, put 'op' to the waiting queue
  // for _any_ op type -- eg only the locker can unlock!
  if (block_if_wrlocked(op)) return; // op will be handled later, after the object unlocks
 
  dout(10) << "op_read " << hex << oid << dec 
		   << " " << op->get_offset() << "~" << op->get_length() 
		   << " in " << *pg 
		   << endl;

  // read into a buffer
  bufferlist bl;
  long got = store->read(oid, 
						 op->get_offset(), op->get_length(),
						 bl);
  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap->get_epoch(), true); 
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
  
  dout(12) << " read got " << got << " / " << op->get_length() << " bytes from obj " << hex << oid << dec << endl;
  
  logger->inc("rd");
  if (got >= 0) logger->inc("rdb", got);
  
  // send it
  messenger->send_message(reply, op->get_asker());
  
  delete op;
}


/** op_stat
 * client stat
 * NOTE: called from opqueue
 */
void OSD::op_stat(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();

  if (waitfor_missing_object(op, pg)) return;

  // if the target object is locked for writing by another client, put 'op' to the waiting queue
  if (block_if_wrlocked(op)) return; //read will be handled later, after the object unlocks

  struct stat st;
  memset(&st, sizeof(st), 0);
  int r = store->stat(oid, &st);
  
  dout(3) << "stat on " << hex << oid << dec << " r = " << r << " size = " << st.st_size << endl;
  
  MOSDOpReply *reply = new MOSDOpReply(op, r, osdmap->get_epoch(), true);
  reply->set_object_size(st.st_size);
  messenger->send_message(reply, op->get_asker());
  
  logger->inc("stat");

  delete op;
}



// WRITE OPS



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
  wr->set_pg_trim_to(pg->peers_complete_thru);
  messenger->send_message(wr, MSG_ADDR_OSD(osd));
  
  repop->osds.insert(osd);

  repop->waitfor_ack[tid] = osd;
  repop->waitfor_commit[tid] = osd;
  
  //replica_ops[tid] = repop;
  //replica_pg_osd_tids[pg->get_pgid()][osd].insert(tid);
  pg->replica_ops[tid] = repop;
  pg->replica_tids_by_osd[osd].insert(tid);
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
	MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osdmap->get_epoch(), true);
	dout(10) << "put_repop sending commit on " << *repop << " " << reply << endl;
	messenger->send_message(reply, repop->op->get_asker());
	repop->sent_commit = true;
  }

  // ack?
  else if (repop->can_send_ack() &&
		   repop->op->wants_ack()) {
	MOSDOpReply *reply = new MOSDOpReply(repop->op, 0, osdmap->get_epoch(), false);
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
	// adjust peers_complete_thru
	if (!repop->pg_complete_thru.empty()) {
	  map<int,version_t>::iterator p = repop->pg_complete_thru.begin();
	  version_t min = p->second;
	  p++;
	  while (p != repop->pg_complete_thru.end()) {
		if (p->second < min) min = p->second;
		p++;
	  }
	  
	  pg_t pgid = repop->op->get_pg();
	  osd_lock.Lock();
	  PG *pg = get_pg(pgid);
	  if (pg) {
		if (min > pg->peers_complete_thru) {
		  //dout(10) << *pg << "put_repop peers_complete_thru " << pg->peers_complete_thru << " -> " << min << endl;
		  pg->peers_complete_thru = min;
		}
		//_unlock_pg(pgid);
	  }
	  osd_lock.Unlock();
	}

	dout(10) << "put_repop deleting " << *repop << endl;
	repop->lock.Unlock();  
	delete repop->op;
	delete repop;
  } else {
	repop->lock.Unlock();
  }
}


void OSD::op_modify_commit(OSDReplicaOp *repop, version_t pg_complete_thru)
{
  dout(10) << "op_modify_commit on op " << *repop->op << endl;
  get_repop(repop);
  {
	assert(repop->waitfor_commit.count(0));
	repop->waitfor_commit.erase(0);
	repop->pg_complete_thru[whoami] = pg_complete_thru;
  }
  put_repop(repop);
}

/** op_modify
 * process client modify op
 * NOTE: called from opqueue.
 */
void OSD::op_modify(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();

  if (waitfor_missing_object(op, pg)) return;

  // if the target object is locked for writing by another client, put 'op' to the waiting queue
  // for _any_ op type -- eg only the locker can unlock!
  if (block_if_wrlocked(op)) return; // op will be handled later, after the object unlocks

  char *opname = 0;
  switch (op->get_op()) {
  case OSD_OP_WRITE: opname = "write"; break;
  case OSD_OP_ZERO: opname = "zero"; break;
  case OSD_OP_DELETE: opname = "delete"; break;
  case OSD_OP_TRUNCATE: opname = "truncate"; break;
  case OSD_OP_WRLOCK: opname = "wrlock"; break;
  case OSD_OP_WRUNLOCK: opname = "wrunlock"; break;
  default: assert(0);
  }

  // bump version.
  version_t ov = 0;  // 0 == dne (yet)
  store->getattr(oid, "version", &ov, sizeof(ov));
  version_t nv = messenger->get_lamport();

  dout(10) << "op_modify " << opname 
		   << " " << hex << oid << dec 
		   << " v " << nv 
		   << " ov " << ov 
		   << "  off " << op->get_offset() << " len " << op->get_length() 
		   << endl;  
 
  if (ov && nv <= ov) 
	cerr << opname << " " << hex << oid << dec << " ov " << ov << " nv " << nv 
		 << " ... wtf?  msg sent " << op->get_lamport_send_stamp() 
		 << " recv " << op->get_lamport_recv_stamp() << endl;
  assert(nv > ov);
  

  // issue replica writes
  OSDReplicaOp *repop = new OSDReplicaOp(op, nv, ov);
  repop->start = g_clock.now();
  repop->waitfor_ack[0] = whoami;    // will need local ack, commit
  repop->waitfor_commit[0] = whoami;
  
  repop->lock.Lock();
  {
	for (unsigned i=1; i<pg->acting.size(); i++) {
	  issue_replica_op(pg, repop, pg->acting[i]);
	}
  }
  repop->lock.Unlock();
  
  // pre-ack
  //MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap->get_epoch(), false);
  //messenger->send_message(reply, op->get_asker());
  
  // prepare
  ObjectStore::Transaction t;
  prepare_log_transaction(t, op, nv, pg, pg->peers_complete_thru);
  prepare_op_transaction(t, op, nv, pg);

  // go
  Context *oncommit = new C_OSD_WriteCommit(this, repop, pg->info.last_complete);
  unsigned r = store->apply_transaction(t, oncommit);
  if (r != 0 &&   // no errors
	  r != 2) {   // or error on collection_add
	cerr << "error applying transaction: r = " << r << endl;
	assert(r == 0);
  }

  // local ack
  get_repop(repop);
  {
	assert(repop->waitfor_ack.count(0));
	repop->waitfor_ack.erase(0);
  }
  put_repop(repop);

  if (op->get_op() == OSD_OP_WRITE) {
	logger->inc("c_wr");
	logger->inc("c_wrb", op->get_length());
  }
}



void OSD::prepare_log_transaction(ObjectStore::Transaction& t, 
								  MOSDOp *op, version_t& version, PG *pg,
								  version_t trim_to)
{
  const object_t oid = op->get_oid();
  const pg_t pgid = op->get_pg();
  
  PG::PGOndiskLog::Entry logentry(op->get_oid(), version);

  // raise last_complete?
  if (pg->info.last_complete == pg->log.top)
	pg->info.last_complete = version;

  // update pg log
  assert(version > pg->log.top);
  assert(pg->info.last_update == pg->log.top);
  if (op->get_op() == OSD_OP_DELETE ||
  	  op->get_op() == OSD_OP_REP_DELETE) {
	dout(10) << "prepare_log_transaction " << op->get_op()
			 << " - " << hex << oid << dec 
			 << " v " << version
			 << " in " << *pg << endl;
	pg->log.add_delete(oid, version);
	logentry.deleted = true;
  } else {
	dout(10) << "prepare_log_transaction " << op->get_op()
			 << " + " << hex << oid << dec 
			 << " v " << version
			 << " in " << *pg << endl;
	pg->log.add_update(oid, version);
  }
  assert(pg->log.top == version);
  pg->info.last_update = version;

  // write to pg log
  pg->append_log(t, logentry, trim_to);
  
  // write pg info
  t.collection_setattr(pgid, "info", &pg->info, sizeof(pg->info));
}


/** prepare_op_transaction
 * apply an op to the store wrapped in a transaction.
 */
void OSD::prepare_op_transaction(ObjectStore::Transaction& t, 
								 MOSDOp *op, version_t& version, PG *pg)
{
  const object_t oid = op->get_oid();
  const pg_t pgid = op->get_pg();

  dout(10) << "prepare_op_transaction " << op->get_op()
		   << " " << hex << oid << dec 
		   << " v " << version
		   << " in " << *pg << endl;
  
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
	  bl.claim( op->get_data() );  // give buffers to store; we keep *op in memory for a long time!
	  
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
  
  // object collection, version
  if (op->get_op() == OSD_OP_DELETE ||
	  op->get_op() == OSD_OP_REP_DELETE) {
	// remove object from c
	t.collection_remove(pgid, oid);
  } else {
	// add object to c
	t.collection_add(pgid, oid);
	
	// object version
	t.setattr(oid, "version", &version, sizeof(version));
  }
}
