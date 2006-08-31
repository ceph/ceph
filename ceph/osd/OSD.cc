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

//#include "msg/HostMonitor.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"

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
  num_pulling = 0;


  pending_ops = 0;
  waiting_for_no_ops = false;

  if (g_conf.osd_remount_at) 
	g_timer.add_event_after(g_conf.osd_remount_at, new C_Remount(this));

										   

  // init object store
  // try in this order:
  // ebofsdev/$num
  // ebofsdev/$hostname
  // ebofsdev/all

  if (dev) {
	strcpy(dev_path,dev);
  } else {
	char hostname[100];
	hostname[0] = 0;
	gethostname(hostname,100);
	
	sprintf(dev_path, "%s/%d", ebofs_base_path, whoami);
	
	struct stat sta;
	if (::lstat(dev_path, &sta) != 0)
	  sprintf(dev_path, "%s/%s", ebofs_base_path, hostname);	
	
	if (::lstat(dev_path, &sta) != 0)
	  sprintf(dev_path, "%s/all", ebofs_base_path);
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
  //if (monitor) { delete monitor; monitor = 0; }
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
		ager.age(g_conf.osd_age_time, g_conf.osd_age, g_conf.osd_age / 2.0, 50000, g_conf.osd_age);
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
	/*
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
	*/
	
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
	osd_logtype.add_inc("r_wr");
	osd_logtype.add_inc("r_wrb");
	
	osd_logtype.add_inc("rlnum");

	osd_logtype.add_set("numpg");
	osd_logtype.add_set("pingset");

	osd_logtype.add_inc("map");
	osd_logtype.add_inc("mapi");
	osd_logtype.add_inc("mapidup");
	osd_logtype.add_inc("mapf");
	osd_logtype.add_inc("mapfdup");
	
	// request thread pool
	{
	  char name[80];
	  sprintf(name,"osd%d.threadpool", whoami);
	  threadpool = new ThreadPool<OSD*, pg_t>(name, g_conf.osd_maxthreads, 
											  static_dequeueop,
											  this);
	}
	
	// i'm ready!
	messenger->set_dispatcher(this);
	
	// announce to monitor i exist and have booted.
	messenger->send_message(new MOSDBoot(superblock), MSG_ADDR_MON(0));
	
	// start the heart
	next_heartbeat = new C_Heartbeat(this);
	g_timer.add_event_after(g_conf.osd_heartbeat_interval, next_heartbeat);
  }
  osd_lock.Unlock();

  return 0;
}

int OSD::shutdown()
{
  dout(1) << "shutdown, timer has " << g_timer.num_event << endl;

  if (next_heartbeat) g_timer.cancel_event(next_heartbeat);

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
  //monitor->shutdown();
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


void OSD::activate_pg(pg_t pgid, epoch_t epoch)
{
  osd_lock.Lock();
  {
	if (pg_map.count(pgid)) {
	  PG *pg = _lock_pg(pgid);
	  if (pg->is_crashed() &&
		  pg->is_replay() &&
		  pg->get_role() == 0 &&
		  pg->info.same_primary_since <= epoch) {
		ObjectStore::Transaction t;
		pg->activate(t);
		store->apply_transaction(t);
	  }
	  _unlock_pg(pgid);
	}
  }
  osd_lock.Unlock();

  // kick myself w/ a ping .. HACK
  messenger->send_message(new MPing, MSG_ADDR_OSD(whoami));
}


// -------------------------------------

void OSD::heartbeat()
{
  osd_lock.Lock();

  utime_t now = g_clock.now();
  utime_t since = now;
  since.sec_ref() -= g_conf.osd_heartbeat_interval;

  dout(-15) << "heartbeat " << now << endl;

  // send pings
  set<int> pingset;
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
	   i != pg_map.end();
	   i++) {
	PG *pg = i->second;

	// we want to ping the primary.
	if (pg->get_role() <= 0) continue;   
	if (pg->acting.size() < 1) continue; 

	if (pg->last_heartbeat < since) {
	  pg->last_heartbeat = now;
	  pingset.insert(pg->acting[0]);
	}
  }
  for (set<int>::iterator i = pingset.begin();
	   i != pingset.end();
	   i++) {
	_share_map_outgoing( MSG_ADDR_OSD(*i) );
	messenger->send_message(new MOSDPing(osdmap->get_epoch()), 
							MSG_ADDR_OSD(*i));
  }

  if (logger) logger->set("pingset", pingset.size());

  // hack: fake reorg?
  if (osdmap && g_conf.fake_osdmap_updates) {
	if ((rand() % (2*g_conf.num_osd)) == whoami) {
	  if (osdmap->is_out(whoami)) {
		messenger->send_message(new MOSDIn(osdmap->get_epoch()),
								MSG_ADDR_MON(0));
	  } 
	  else if ((rand() % g_conf.fake_osdmap_updates) == 0) {
		//messenger->send_message(new MOSDOut(osdmap->get_epoch()),
		messenger->send_message(new MOSDIn(osdmap->get_epoch()),
								MSG_ADDR_MON(0));
	  }
	}
  }

  // schedule next!
  next_heartbeat = new C_Heartbeat(this);
  g_timer.add_event_after(g_conf.osd_heartbeat_interval, next_heartbeat);

  osd_lock.Unlock();  
}



// --------------------------------------
// dispatch

bool OSD::_share_map_incoming(msg_addr_t who, epoch_t epoch)
{
  bool shared = false;

  // does client have old map?
  if (who.is_client()) {
	if (epoch < osdmap->get_epoch()) {
	  dout(10) << who << " has old map " << epoch << " < " << osdmap->get_epoch() << endl;
	  send_incremental_map(epoch, who, true);
	  shared = true;
	}
  }

  // does peer have old map?
  if (who.is_osd()) {
	// remember
	if (peer_map_epoch[who] < epoch)
	  peer_map_epoch[who] = epoch;
	
	// older?
	if (peer_map_epoch[who] < osdmap->get_epoch()) {
	  dout(10) << who << " has old map " << epoch << " < " << osdmap->get_epoch() << endl;
	  send_incremental_map(epoch, who, true);
	  peer_map_epoch[who] = osdmap->get_epoch();  // so we don't send it again.
	  shared = true;
	}
  }

  return shared;
}


void OSD::_share_map_outgoing(msg_addr_t dest) 
{
  assert(dest.is_osd());

  if (dest.is_osd()) {
	// send map?
	if (peer_map_epoch.count(dest)) {
	  epoch_t pe = peer_map_epoch[dest];
	  if (pe < osdmap->get_epoch()) {
		send_incremental_map(pe, dest, true);
		peer_map_epoch[dest] = osdmap->get_epoch();
	  }
	} else {
	  // no idea about peer's epoch.
	  // ??? send recent ???
	  // do nothing.
	}
  }
}



void OSD::dispatch(Message *m) 
{
  // check clock regularly
  //utime_t now = g_clock.now();
  //dout(-20) << now << endl;

  /*// -- don't need lock --
  switch (m->get_type()) {
	return;
  }
  */


  // lock!
  osd_lock.Lock();

  switch (m->get_type()) {

	// -- don't need lock -- 
  case MSG_PING:
	dout(10) << "ping from " << m->get_source() << endl;
	delete m;
	break;

	// -- don't need OSDMap --

	/*
	// host monitor
  case MSG_PING_ACK:
  case MSG_FAILURE_ACK:
	monitor->proc_message(m);
	break;
	*/

	// map and replication
  case MSG_OSD_MAP:
	handle_osd_map((MOSDMap*)m);
	break;

	// osd
  case MSG_SHUTDOWN:
	shutdown();
	delete m;
	break;
	
	

	// -- need OSDMap --

  default:
	{
	  // no map?  starting up?
	  if (!osdmap) {
		dout(7) << "no OSDMap, not booted" << endl;
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

	  case MSG_OSD_PING:
		// take note.
		handle_osd_ping((MOSDPing*)m);
		break;
		
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
		handle_op((MOSDOp*)m);
		break;
		
		// for replication etc.
	  case MSG_OSD_OPREPLY:
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


Message* OSD::ms_handle_failure(msg_addr_t dest, entity_inst_t& inst)
{
  dout(0) << "ms_handle_failure " << dest << " inst " << inst << endl;
  messenger->send_message(new MOSDFailure(dest, inst, osdmap->get_epoch()),
						  MSG_ADDR_MON(0));
  return 0;
}

bool OSD::ms_lookup(msg_addr_t dest, entity_inst_t& inst)
{
  if (dest.is_osd()) {
	assert(osdmap);
	return osdmap->get_inst(dest.num(), inst);
  } 

  assert(0);
  return false;
}



void OSD::handle_op_reply(MOSDOpReply *m)
{
  if (m->get_map_epoch() < boot_epoch) {
	dout(3) << "replica op reply from before boot" << endl;
	delete m;
	return;
  }
  

  // handle op
  switch (m->get_op()) {
  case OSD_OP_REP_PULL:
	op_rep_pull_reply(m);
	break;

  case OSD_OP_REP_WRNOOP:
  case OSD_OP_REP_WRITE:
  case OSD_OP_REP_TRUNCATE:
  case OSD_OP_REP_DELETE:
  case OSD_OP_REP_WRLOCK:
  case OSD_OP_REP_WRUNLOCK:
  case OSD_OP_REP_RDLOCK:
  case OSD_OP_REP_RDUNLOCK:
  case OSD_OP_REP_UPLOCK:
  case OSD_OP_REP_DNLOCK:
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
void OSD::handle_rep_op_ack(PG *pg, __uint64_t tid, int result, bool commit, 
							int fromosd, eversion_t pg_complete_thru)
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



void OSD::handle_osd_ping(MOSDPing *m)
{
  dout(20) << "osdping from " << m->get_source() << endl;
  _share_map_incoming(m->get_source(), ((MOSDPing*)m)->map_epoch);
  
  //if (!m->ack)
  //messenger->send_message(new MOSDPing(osdmap->get_epoch(), true),
  //m->get_source());
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
  wait_for_no_ops();
  
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

  logger->inc("mapmsg");

  // store them?
  for (map<epoch_t,bufferlist>::iterator p = m->maps.begin();
	   p != m->maps.end();
	   p++) {
	object_t oid = get_osdmap_object_name(p->first);
	if (store->exists(oid)) {
	  dout(10) << "handle_osd_map already had full map epoch " << p->first << endl;
	  logger->inc("mapfdup");
	  bufferlist bl;
	  get_map_bl(p->first, bl);
	  dout(10) << " .. it is " << bl.length() << " bytes" << endl;
	  continue;
	}

	dout(10) << "handle_osd_map got full map epoch " << p->first << endl;
	//t.write(oid, 0, p->second.length(), p->second);
	store->write(oid, 0, p->second.length(), p->second, 0);

	if (p->first > superblock.newest_map)
	  superblock.newest_map = p->first;
	if (p->first < superblock.oldest_map ||
		superblock.oldest_map == 0)
	  superblock.oldest_map = p->first;

	logger->inc("mapf");
  }
  for (map<epoch_t,bufferlist>::iterator p = m->incremental_maps.begin();
	   p != m->incremental_maps.end();
	   p++) {
	object_t oid = get_inc_osdmap_object_name(p->first);
	if (store->exists(oid)) {
	  dout(10) << "handle_osd_map already had incremental map epoch " << p->first << endl;
	  logger->inc("mapidup");
	  bufferlist bl;
	  get_inc_map_bl(p->first, bl);
	  dout(10) << " .. it is " << bl.length() << " bytes" << endl;
	  continue;
	}

	dout(10) << "handle_osd_map got incremental map epoch " << p->first << endl;
	//t.write(oid, 0, p->second.length(), p->second);
	store->write(oid, 0, p->second.length(), p->second, 0);

	if (p->first > superblock.newest_map)
	  superblock.newest_map = p->first;
	if (p->first < superblock.oldest_map ||
		superblock.oldest_map == 0)
	  superblock.oldest_map = p->first;

	logger->inc("mapi");
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
	activate_map(t);
	
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

	//cerr << "osdmap " << osdmap->get_ctime() << " logger start " << logger->get_start() << endl;
	logger->set_start( osdmap->get_ctime() );

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
		pg->activate(t);
		
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
	  pg->activate(t);
		
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
		pg->cancel_recovery();
	  }
	  
	  if (role != oldrole) {
		pg->info.same_role_since = osdmap->get_epoch();

		// old primary?
		if (oldrole == 0) {
		  pg->state_clear(PG::STATE_CLEAN);

		  // take replay queue waiters
		  list<Message*> ls;
		  for (map<eversion_t,MOSDOp*>::iterator it = pg->replay_queue.begin();
			   it != pg->replay_queue.end();
			   it++)
			ls.push_back(it->second);
		  pg->replay_queue.clear();
		  take_waiters(ls);

		  // take active waiters
		  take_waiters(pg->waiting_for_active);
		  
		  // take object waiters
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
		  pg->last_epoch_started_any = pg->info.last_epoch_started;
		} else {
		  // i am now replica|stray.  we need to send a notify.
		  pg->state_clear(PG::STATE_ACTIVE);
		  pg->state_set(PG::STATE_STRAY);

		  if (nrep == 0) {
			pg->state_set(PG::STATE_CRASHED);
			dout(1) << *pg << " is crashed" << endl;
		  }
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
			pg->state_clear(PG::STATE_REPLAY);

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

void OSD::activate_map(ObjectStore::Transaction& t)
{
  dout(7) << "activate_map version " << osdmap->get_epoch() << endl;

  map< int, list<PG::Info> >  notify_list;  // primary -> list
  map< int, map<pg_t,PG::Query> > query_map;    // peer -> PG -> get_summary_since

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
	  pg->peer(t, query_map);
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

  logger->set("numpg", pg_map.size());
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

bool OSD::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  return store->read(get_inc_osdmap_object_name(e), 0, 0, bl) >= 0;
}

void OSD::get_map(epoch_t epoch, OSDMap &m)
{
  // find a complete map
  list<OSDMap::Incremental> incs;
  epoch_t e;
  for (e = epoch; e > 0; e--) {
	bufferlist bl;
	if (get_map_bl(e, bl)) {
	  //dout(10) << "get_map " << epoch << " full " << e << endl;
	  m.decode(bl);
	  break;
	} else {
	  OSDMap::Incremental inc;
	  bool got = get_inc_map(e, inc);
	  assert(got);
	  incs.push_front(inc);
	}
  }
  assert(e > 0);

  // apply incrementals
  for (e++; e <= epoch; e++) {
	//dout(10) << "get_map " << epoch << " inc " << e << endl;
	m.apply_incremental( incs.front() );
	incs.pop_front();
  }
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
  // older map?
  if (ep < osdmap->get_epoch()) {
	dout(7) << "require_current_map epoch " << ep << " < " << osdmap->get_epoch() << endl;
	delete m;   // discard and ignore.
	return false;
  }

  // newer map?
  if (ep > osdmap->get_epoch()) {
	dout(7) << "require_current_map epoch " << ep << " > " << osdmap->get_epoch() << endl;
	wait_for_new_map(m);
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
  dout(10) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ")" << endl;

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

	dout(10) << "load_pgs loaded " << *pg << " " << pg->log << endl;
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

void OSD::do_notifies(map< int, list<PG::Info> >& notify_list) 
{
  for (map< int, list<PG::Info> >::iterator it = notify_list.begin();
	   it != notify_list.end();
	   it++) {
	if (it->first == whoami) {
	  dout(7) << "do_notify osd" << it->first << " is self, skipping" << endl;
	  continue;
	}
	dout(7) << "do_notify osd" << it->first << " on " << it->second.size() << " PGs" << endl;
	MOSDPGNotify *m = new MOSDPGNotify(osdmap->get_epoch(), it->second);
	_share_map_outgoing(MSG_ADDR_OSD(it->first));
	messenger->send_message(m, MSG_ADDR_OSD(it->first));
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void OSD::do_queries(map< int, map<pg_t,PG::Query> >& query_map)
{
  for (map< int, map<pg_t,PG::Query> >::iterator pit = query_map.begin();
	   pit != query_map.end();
	   pit++) {
	int who = pit->first;
	dout(7) << "do_queries querying osd" << who
			<< " on " << pit->second.size() << " PGs" << endl;

	MOSDPGQuery *m = new MOSDPGQuery(osdmap->get_epoch(),
									 pit->second);
	_share_map_outgoing(MSG_ADDR_OSD(who));
	messenger->send_message(m, MSG_ADDR_OSD(who));
  }
}




/** PGNotify
 * from non-primary to primary
 * includes PG::Info.
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_notify(MOSDPGNotify *m)
{
  dout(7) << "handle_pg_notify from " << m->get_source() << endl;
  int from = MSG_ADDR_NUM(m->get_source());

  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  ObjectStore::Transaction t;
  
  // look for unknown PGs i'm primary for
  map< int, map<pg_t,PG::Query> > query_map;

  for (list<PG::Info>::iterator it = m->get_pg_list().begin();
	   it != m->get_pg_list().end();
	   it++) {
	pg_t pgid = it->pgid;
	PG *pg;

	if (pg_map.count(pgid) == 0) {
	  // check mapping.
	  vector<int> acting;
	  osdmap->pg_to_acting_osds(pgid, acting);
	  
	  // am i still the primary?
	  assert(it->same_primary_since <= osdmap->get_epoch());
	  if (acting.empty() || acting[0] != whoami) {
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
		  dout(-10) << *pg << " now clean on all replicas" << endl;
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
	  pg->peer(t, query_map);
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
	pg->merge_log(m->log, m->missing, from);
	
	// peer
	map< int, map<pg_t,PG::Query> > query_map;
	pg->peer(t, query_map);
	do_queries(query_map);

  } else {
	// i am REPLICA
	dout(10) << *pg << " got " << m->log << " " << m->missing << endl;

	// merge log
	pg->merge_log(m->log, m->missing, from);
	assert(pg->missing.num_lost() == 0);

	// ok active!
	pg->info.same_primary_since = m->info.same_primary_since;
 	pg->activate(t);
  }

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

  map< int, list<PG::Info> > notify_list;
  
  for (map<pg_t,PG::Query>::iterator it = m->pg_list.begin();
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
		PG::Info empty(pgid);
		notify_list[from].push_back(empty);
		continue;
	  }
	  
	  ObjectStore::Transaction t;
	  PG *pg = create_pg(pgid, t);
	  pg->acting = acting;
	  pg->set_role(role);
	  
	  pg->info.same_primary_since = it->second.same_primary_since;  //calc_pg_primary_since(acting[0], pgid, m->get_epoch());
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

	if (it->second.type == PG::Query::INFO) {
	  // info
	  dout(10) << *pg << " sending info" << endl;
	  notify_list[from].push_back(pg->info);
	} else {
	  MOSDPGLog *m = new MOSDPGLog(osdmap->get_epoch(), pg->get_pgid());
	  m->info = pg->info;
	  
	  if (it->second.type == PG::Query::BACKLOG) {
		dout(10) << *pg << " sending info+summary/backlog" << endl;
		if (pg->log.backlog) {
		  m->log = pg->log;
		} else {
		  pg->generate_backlog();
		  m->log = pg->log;
		  pg->drop_backlog();
		}
	  } 
	  else if (it->second.version == 0) {
		dout(10) << *pg << " sending info+entire log" << endl;
		m->log = pg->log;
	  } 
	  else {
		eversion_t since = MAX(it->second.version, pg->log.bottom);
		dout(10) << *pg << " sending info+log since " << since
				 << " (asked since " << it->second.version << ")" << endl;
		m->log.copy_after(pg->log, since);
	  }
	  m->missing = pg->missing;

	  _share_map_outgoing(MSG_ADDR_OSD(from));
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

	// unlock.  there shouldn't be any waiters, since we're a stray, and pg is presumably clean0.
	assert(pg_lock_waiters.count(pgid) == 0);
	_unlock_pg(pgid);
  }

  delete m;
}







// RECOVERY





// pull


void OSD::pull(PG *pg, object_t oid, eversion_t v)
{
  assert(pg->missing.loc.count(oid));
  int osd = pg->missing.loc[oid];
  
  dout(7) << *pg << " pull " << hex << oid << dec 
		  << " v " << v 
		  << " from osd" << osd
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
  num_pulling++;
  pg->objects_pulling[oid] = v;
}


/** op_rep_pull
 * process request to pull an entire object.
 * NOTE: called from opqueue.
 */
void OSD::op_rep_pull(MOSDOp *op, PG *pg)
{
  const object_t oid = op->get_oid();

  dout(7) << "rep_pull on " << hex << oid << dec << " v >= " << op->get_version() << endl;

  // read data+attrs
  bufferlist bl;
  eversion_t v;
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

  logger->inc("r_pull");
  logger->inc("r_pullb", bl.length());
  
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
}


/*
 * NOTE: called holding osd_lock.  opqueue active.
 */
void OSD::op_rep_pull_reply(MOSDOpReply *op)
{
  object_t oid = op->get_oid();
  eversion_t v = op->get_version();
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

  // close out pull op.
  num_pulling--;
  pg->objects_pulling.erase(oid);
  pg->missing.got(oid, v);

  // kick waiters
  if (pg->waiting_for_missing_object.count(oid)) 
	take_waiters(pg->waiting_for_missing_object[oid]);

  // raise last_complete?
  assert(pg->log.complete_to != pg->log.log.end());
  while (pg->log.complete_to != pg->log.log.end()) {
	if (pg->missing.missing.count(pg->log.complete_to->oid)) break;
	if (pg->info.last_complete < pg->log.complete_to->version)
	  pg->info.last_complete = pg->log.complete_to->version;
	pg->log.complete_to++;
  }
  dout(10) << *pg << " last_complete now " << pg->info.last_complete << endl;
  
  // continue
  pg->do_recovery();

  // apply to disk!
  t.collection_setattr(pgid, "info", &pg->info, sizeof(pg->info));
  unsigned r = store->apply_transaction(t);
  assert(r == 0);

  _unlock_pg(pgid);

  delete op;
}




// op_rep_modify

// commit (to disk) callback
class C_OSD_RepModifyCommit : public Context {
public:
  OSD *osd;
  MOSDOp *op;

  eversion_t pg_last_complete;

  Mutex lock;
  Cond cond;
  bool acked;
  bool waiting;

  C_OSD_RepModifyCommit(OSD *o, MOSDOp *oo, eversion_t lc) : osd(o), op(oo), pg_last_complete(lc),
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

void OSD::op_rep_modify_commit(MOSDOp *op, eversion_t last_complete)
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
  eversion_t pg_last_complete;
  C_OSD_WriteCommit(OSD *o, OSDReplicaOp *op, eversion_t lc) : osd(o), repop(op), pg_last_complete(lc) {}
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
  eversion_t nv = op->get_version();

  ObjectStore::Transaction t;

  if (op->get_op() != OSD_OP_WRNOOP) {
	// update PG log
	if (pg->info.last_update < nv)
	  prepare_log_transaction(t, op, nv, pg, op->get_pg_trim_to());
	// else, we are playing catch-up, don't update pg metadata!  (FIXME?)
  }
  
  // do op?
  C_OSD_RepModifyCommit *oncommit = 0;
  
  // check current version
  eversion_t myv = 0;
  store->getattr(oid, "version", &myv, sizeof(myv));  // this is a noop if oid dne
  dout(10) << "op_rep_modify existing " << hex << oid << dec << " v " << myv << endl;

  // is this an old update?  or WRNOOP?
  if (nv <= myv || op->get_op() == OSD_OP_WRNOOP) {
	// we have a newer version.  pretend we do a regular commit!
	dout(10) << "op_rep_modify on " << hex << oid << dec 
			 << " v " << nv << " <= myv | wrnoop, noop"
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

	logger->inc("r_wr");
	logger->inc("r_wrb", op->get_length());
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

	pg->last_heartbeat = g_clock.now();
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

  _share_map_incoming(op->get_source(), op->get_map_epoch());

  // what kind of op?
  if (!OSD_OP_IS_REP(op->get_op())) {
	// REGULAR OP (non-replication)

	// have pg?
	if (!pg) {
	  dout(7) << "hit non-existent pg " 
			  << hex << pgid << dec 
			  << ", waiting" << endl;
	  waiting_for_pg[pgid].push_back(op);
	  return;
	}
		
	// am i the (same) primary?
	if (acting_primary != whoami ||
		op->get_map_epoch() < pg->info.same_primary_since) {
	  dout(7) << "acting primary is osd" << acting_primary
			  << " since " << pg->info.same_primary_since 
			  << ", dropping" << endl;
	  assert(op->get_map_epoch() < osdmap->get_epoch());
	  return;
	}

	// must be active.
	if (!pg->is_active()) {
	  // replay?
	  if (op->get_version().version > 0) {
		if (op->get_version() > pg->info.last_update) {
		  dout(7) << *pg << " queueing replay at " << op->get_version() << " for " << *op << endl;
		  pg->replay_queue[op->get_version()] = op;
		  return;
		} else {
		  dout(7) << *pg << " replay at " << op->get_version() << " <= " << pg->info.last_update 
				  << ", will queue for WRNOOP" << endl;
		}
	  }
	  
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
	case OSD_OP_REP_WRNOOP:
	case OSD_OP_REP_WRITE:
	case OSD_OP_REP_TRUNCATE:
	case OSD_OP_REP_DELETE:
	case OSD_OP_REP_WRLOCK:
	case OSD_OP_REP_WRUNLOCK:
	case OSD_OP_REP_RDLOCK:
	case OSD_OP_REP_RDUNLOCK:
	case OSD_OP_REP_UPLOCK:
	case OSD_OP_REP_DNLOCK:
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
	case OSD_OP_WRNOOP:
	case OSD_OP_WRITE:
	case OSD_OP_ZERO:
	case OSD_OP_DELETE:
	case OSD_OP_TRUNCATE:
	case OSD_OP_WRLOCK:
	case OSD_OP_WRUNLOCK:
	case OSD_OP_RDLOCK:
	case OSD_OP_RDUNLOCK:
	case OSD_OP_UPLOCK:
	case OSD_OP_DNLOCK:
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
	eversion_t v = pg->missing.missing[oid];
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
  wr->set_orig_asker(op->get_asker());
  wr->set_orig_tid(op->get_tid());
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
	  pg_t pgid = repop->op->get_pg();
	  osd_lock.Lock();
	  PG *pg = get_pg(pgid);
	  if (pg) {
		eversion_t min = pg->info.last_complete;  // hrm....
		for (unsigned i=1; i<pg->acting.size(); i++) {
		  if (repop->pg_complete_thru[i] < min)      // note: if we haven't heard, it'll be zero, which is what we want.
			min = repop->pg_complete_thru[i];
		}

		if (min > pg->peers_complete_thru) {
		  dout(10) << *pg << "put_repop peers_complete_thru " << pg->peers_complete_thru << " -> " << min << endl;
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


void OSD::op_modify_commit(OSDReplicaOp *repop, eversion_t pg_complete_thru)
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

  const char *opname = MOSDOp::get_opname(op->get_op());

  // missing?
  if (waitfor_missing_object(op, pg)) return;
  
  // dup op?
  reqid_t reqid(op->get_asker(), op->get_tid());
  if (pg->log.logged_req(reqid)) {
	dout(-3) << "op_modify " << opname << " dup op " << reqid
			 << ", doing WRNOOP" << endl;
	op->set_op(OSD_OP_WRNOOP);
	opname = MOSDOp::get_opname(op->get_op());
  }

  // locked by someone else?
  // for _any_ op type -- eg only the locker can unlock!
  if (op->get_op() != OSD_OP_WRNOOP &&  // except WRNOOP; we just want to flush
	  block_if_wrlocked(op)) 
	return; // op will be handled later, after the object unlocks


  // old version
  eversion_t ov = 0;  // 0 == dne (yet)
  store->getattr(oid, "version", &ov, sizeof(ov));
  
  // new version
  eversion_t nv;
  if (op->get_op() == OSD_OP_WRNOOP)
	nv = ov;
  else {
	nv = pg->info.last_update;
	nv.epoch = osdmap->get_epoch();
	nv.version++;
	assert(nv > pg->info.last_update);
	assert(nv > ov);

	if (op->get_version().version) {
	  // replay
	  if (nv.version < op->get_version().version)
		nv.version = op->get_version().version; 
	} 

	// set version in op, for benefit of client and our eventual reply
	op->set_version(nv);
  }
  
  dout(10) << "op_modify " << opname 
		   << " " << hex << oid << dec 
		   << " v " << nv 
		   << " ov " << ov 
		   << "  off " << op->get_offset() << " len " << op->get_length() 
		   << endl;  
 
  // share latest osd map?
  osd_lock.Lock();
  {
	for (unsigned i=1; i<pg->acting.size(); i++) 
	  _share_map_outgoing( MSG_ADDR_OSD(i) ); 
  }
  osd_lock.Unlock();
  
  // issue replica writes
  OSDReplicaOp *repop = new OSDReplicaOp(op, nv, ov);
  repop->start = g_clock.now();
  repop->waitfor_ack[0] = whoami;    // will need local ack, commit
  repop->waitfor_commit[0] = whoami;
  
  repop->lock.Lock();
  {
	for (unsigned i=1; i<pg->acting.size(); i++)
	  issue_replica_op(pg, repop, pg->acting[i]);
  }
  repop->lock.Unlock();

  // prepare
  ObjectStore::Transaction t;
  if (op->get_op() != OSD_OP_WRNOOP) {
	prepare_log_transaction(t, op, nv, pg, pg->peers_complete_thru);
	prepare_op_transaction(t, op, nv, pg);
  }
  
  // apply
  Context *oncommit = new C_OSD_WriteCommit(this, repop, pg->info.last_complete);
  unsigned r = store->apply_transaction(t, oncommit);
  if (r != 0 &&   // no errors
	  r != 2) {   // or error on collection_add
	cerr << "error applying transaction: r = " << r << endl;
	assert(r == 0);
  }

  // pre-ack
  //MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap->get_epoch(), false);
  //messenger->send_message(reply, op->get_asker());
 
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
								  MOSDOp *op, eversion_t& version, PG *pg,
								  eversion_t trim_to)
{
  const object_t oid = op->get_oid();
  const pg_t pgid = op->get_pg();
  
  int opcode = PG::Log::Entry::UPDATE;
  if (op->get_op() == OSD_OP_DELETE ||
	  op->get_op() == OSD_OP_REP_DELETE) opcode = PG::Log::Entry::DELETE;
  PG::Log::Entry logentry(opcode, op->get_oid(), version,
						  op->get_orig_asker(), op->get_orig_tid());

  dout(10) << "prepare_log_transaction " << op->get_op()
		   << (logentry.is_delete() ? " - ":" + ")
		   << hex << oid << dec 
		   << " v " << version
		   << " in " << *pg << endl;

  // raise last_complete?
  if (pg->info.last_complete == pg->log.top)
	pg->info.last_complete = version;
  
  // update pg log
  assert(version > pg->log.top);
  assert(pg->info.last_update == pg->log.top);
  pg->log.add(logentry);
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
								 MOSDOp *op, eversion_t& version, PG *pg)
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
