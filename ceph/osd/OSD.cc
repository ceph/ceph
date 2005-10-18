
#include "include/types.h"

#include "OSD.h"
#include "OSDMap.h"

#ifdef USE_OBFS
# include "OBFSStore.h"
#else
# include "FakeStore.h"
#endif


#include "mds/MDS.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "msg/HostMonitor.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGPeer.h"
#include "messages/MOSDPGPeerAck.h"
#include "messages/MOSDPGUpdate.h"

//#include "messages/MOSDPGQuery.h"
//#include "messages/MOSDPGQueryReply.h"

#include "common/Logger.h"
#include "common/LogType.h"

#include "common/ThreadPool.h"

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << whoami << " "

char *osd_base_path = "./osddata";


#define ROLE_TYPE(x)   ((x)>0 ? 1:(x))


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


  // use fake store
#ifdef USE_OBFS
  store = new OBFSStore(whoami, NULL, "/dev/sdb3");
#else
  store = new FakeStore(osd_base_path, whoami);
#endif

  // monitor
  char s[80];
  sprintf(s, "osd%d", whoami);
  string st = s;
  monitor = new HostMonitor(m, st);
  monitor->set_notify_port(MDS_PORT_OSDMON);
  
  // hack
  int i = whoami;
  if (++i == g_conf.num_osd) i = 0;
  monitor->get_hosts().insert(MSG_ADDR_OSD(i));
  if (++i == g_conf.num_osd) i = 0;
  monitor->get_hosts().insert(MSG_ADDR_OSD(i));
  if (++i == g_conf.num_osd) i = 0;  
  monitor->get_hosts().insert(MSG_ADDR_OSD(i));
  
  monitor->get_notify().insert(MSG_ADDR_MDS(0));

  // log
  char name[80];
  sprintf(name, "osd%02d", whoami);
  logger = new Logger(name, (LogType*)&osd_logtype);
  osd_logtype.add_inc("op");
  osd_logtype.add_inc("rd");
  osd_logtype.add_inc("rdb");
  osd_logtype.add_inc("wr");
  osd_logtype.add_inc("wrb");

  // Thread pool
  {
	char name[80];
	sprintf(name,"osd%d.threadpool", whoami);
	threadpool = new ThreadPool<OSD, MOSDOp>(name, g_conf.osd_maxthreads, (void (*)(OSD*, MOSDOp*))doop, this);
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

  int r = store->init();

  monitor->init();

  osd_lock.Unlock();

  // i'm ready!
  messenger->set_dispatcher(this);

  return r;
}

int OSD::shutdown()
{
  dout(1) << "shutdown" << endl;

  // stop threads
  delete threadpool;
  threadpool = 0;

  // shut everything else down
  monitor->shutdown();
  messenger->shutdown();

  int r = store->finalize();
  return r;
}



// object locks

void OSD::lock_object(object_t oid) 
{
  osd_lock.Lock();
  if (object_lock.count(oid)) {
	Cond c;
	dout(15) << "lock_object " << hex << oid << dec << " waiting as " << &c << endl;
	object_lock_waiters[oid].push_back(&c);
	c.Wait(osd_lock);
	assert(object_lock.count(oid));
  } else {
	dout(15) << "lock_object " << hex << oid << dec << endl;
	object_lock.insert(oid);
  }
  osd_lock.Unlock();
}

void OSD::unlock_object(object_t oid) 
{
  osd_lock.Lock();
  assert(object_lock.count(oid));
  if (object_lock_waiters.count(oid)) {
	// someone is in line
	list<Cond*>& ls = object_lock_waiters[oid];
	Cond *c = ls.front();
	dout(15) << "unlock_object " << hex << oid << dec << " waking up next guy " << c << endl;
	ls.pop_front();
	if (ls.empty()) 
	  object_lock_waiters.erase(oid);
	c->Signal();
  } else {
	// nobody waiting
	dout(15) << "unlock_object " << hex << oid << dec << endl;
	object_lock.erase(oid);
  }
  osd_lock.Unlock();
}



// --------------------------------------
// dispatch

void OSD::dispatch(Message *m) 
{
  // check clock regularly
  g_clock.now();


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
		osd_lock.Lock();
		dout(7) << "no OSDMap, asking MDS" << endl;
		if (waiting_for_osdmap.empty()) 
		  messenger->send_message(new MGenericMessage(MSG_OSD_GETMAP), 
								  MSG_ADDR_MDS(0), MDS_PORT_MAIN);
		waiting_for_osdmap.push_back(m);
		osd_lock.Unlock();
		return;
	  }

	  // need OSDMap
	  switch (m->get_type()) {
		
	  case MSG_OSD_PG_NOTIFY:
		handle_pg_notify((MOSDPGNotify*)m);
		break;
	  case MSG_OSD_PG_PEER:
		handle_pg_peer((MOSDPGPeer*)m);
		break;
	  case MSG_OSD_PG_PEERACK:
		handle_pg_peer_ack((MOSDPGPeerAck*)m);
		break;
	  case MSG_OSD_PG_UPDATE:
		handle_pg_update((MOSDPGUpdate*)m);
		break;

		/*
	  case MSG_OSD_PG_QUERYREPLY:
		handle_pg_query_reply((MOSDPGQueryReply*)m);
		break;
		*/

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
	for (list<Message*>::iterator it = waiting.begin();
		 it != waiting.end();
		 it++) {
	  dispatch(*it);
	}
  }

}


void OSD::handle_op_reply(MOSDOpReply *m)
{
  switch (m->get_op()) {
  case OSD_OP_REP_PULL:
	op_rep_pull_reply(m);
	break;
  case OSD_OP_REP_PUSH:
	op_rep_push_reply(m);
	break;
  case OSD_OP_REP_REMOVE:
	op_rep_remove_reply(m);
	break;

  case OSD_OP_REP_WRITE:
  case OSD_OP_REP_TRUNCATE:
  case OSD_OP_REP_DELETE:
	ack_replica_op(m->get_tid(), m->get_result(), MSG_ADDR_NUM(m->get_source()));
	delete m;
	break;

  default:
	assert(0);
  }
}

void OSD::ack_replica_op(__uint64_t tid, int result, int fromosd)
{
  replica_write_lock.Lock();
  
  if (replica_writes.count(tid)) {
	MOSDOp *op = replica_writes[tid];
	dout(7) << "rep_write_reply ack tid " << tid << " orig op " << op << endl;
	
	replica_writes.erase(tid);
	replica_write_tids[op].erase(tid);
		
	pg_t pgid = op->get_pg();

	replica_pg_osd_tids[pgid][fromosd].erase(tid);
	if (replica_pg_osd_tids[pgid][fromosd].empty()) replica_pg_osd_tids[pgid].erase(fromosd);
	if (replica_pg_osd_tids[pgid].empty()) replica_pg_osd_tids.erase(pgid);
	
	if (replica_write_tids[op].empty()) {
	  // reply?
	  if (replica_write_local.count(op)) {
		replica_write_local.erase(op);
		
		if (result >= 0) {
		  dout(7) << "last one, replying to write op" << endl;
		  
		  // written locally too, reply
		  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true);
		  messenger->send_message(reply, op->get_asker());
		  delete op;
		} else {
		  dout(7) << "last one, but replica write failed, resubmit" << endl;
		  finished.push_back(op);  // handle_op will fw this to new primary, probably!
		}
		replica_write_result.erase(op);
	  } else {
		// not yet written locally.
		dout(9) << "not yet written locally, still waiting for that" << endl;
		replica_write_result[op] = -1;
	  }
	  replica_write_tids.erase(op);
	}
	
  } else {
	dout(7) << "not waiting for tid " << tid << " rep_write reply, map must have changed, dropping." << endl;
  }
  
  replica_write_lock.Unlock();
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

  osd_lock.Lock();
  waiting_for_osdmap.push_back(m);
  osd_lock.Unlock();
}


/** update_map
 * assimilate a new OSDMap.  scan pgs.
 */
/*
void OSD::update_map(bufferlist& state)
{
  // decode new map
  if (!osdmap) osdmap = new OSDMap();
  osdmap->decode(state);
  dout(7) << "update_map version " << osdmap->get_version() << endl;

  osdmaps[osdmap->get_version()] = osdmap;

  // FIXME mutliple maps?

  // scan PGs
  list<pg_t> ls;
  get_pg_list(ls);

  advance_map(ls);
  activate_map(ls);
}
*/

void OSD::handle_osd_map(MOSDMap *m)
{
  // wait for ops to finish
  wait_for_no_ops();

  osd_lock.Lock();     // actually, don't need this if we finish all ops?

  if (m->is_mkfs()) {
	dout(1) << "MKFS" << endl;
	store->mkfs();
  }

  if (!osdmap ||
	  m->get_version() > osdmap->get_version()) {
	if (osdmap) {
	  dout(3) << "handle_osd_map got osd map version " << m->get_version() << " > " << osdmap->get_version() << endl;
	} else {
	  dout(3) << "handle_osd_map got osd map version " << m->get_version() << endl;
	}

	// decode new map
	osdmap = new OSDMap();
	osdmap->decode(m->get_osdmap());
	osdmaps[osdmap->get_version()] = osdmap;
	dout(7) << "got osd map version " << osdmap->get_version() << endl;
	
	// pg list
	list<pg_t> pg_list;

	if (m->is_mkfs()) {
	  // create PGs
	  for (int nrep = 2; nrep <= g_conf.osd_max_rep; nrep++) {
		ps_t maxps = 1LL << osdmap->get_pg_bits();
		for (pg_t ps = 0; ps < maxps; ps++) {
		  pg_t pgid = osdmap->ps_nrep_to_pg(ps, nrep);
		  vector<int> acting;
		  osdmap->pg_to_acting_osds(pgid, acting);
		  
		  
		  if (acting[0] == whoami) {
			PG *pg = create_pg(pgid);
			pg->acting = acting;
			pg->set_role(0);
			pg->set_primary_since(osdmap->get_version());
			pg->state_set(PG_STATE_COMPLETE);
			
			dout(7) << "created " << *pg << endl;
			
			pg_list.push_back(pgid);
		  }
		}
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

  } else {
	dout(3) << "handle_osd_map ignoring osd map version " << m->get_version() << " <= " << osdmap->get_version() << endl;
  }
  
  osd_lock.Unlock();

  if (m->is_mkfs()) {
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


// ======================================================
// REPLICATION




// ------------------------------------
// placement supersets
/*
void OSD::get_ps_list(list<pg_t>& ls)
{
  list<coll_t>& cl;
  store->list_collections(cl);
  
  for (list<coll_t>::iterator it = cl.begin();
	   it != cl.end();
	   it++) {
	// is it a PS (and not a PG)?
	if (*it & PG_PS_MASK == *it)
	  ls.push_back(*it);
  }
}

bool OSD::ps_exists(ps_t psid)
{
  struct stat st;
  if (store->collection_stat(psid, &st) == 0) 
	return true;
  else
	return false;
}

PS* OSD::create_ps(ps_t psid)
{
  assert(ps_map.count(psid) == 0);
  assert(!ps_exists(psid));

  PS *ps = new PS(psid);
  ps->store(store);
  ps_map[psid] = ps;
  return ps;
}

PS* OSD::open_ps(ps_t psid)
{
  // already open?
  if (ps_map.count(psid)) 
	return ps_map[psid];

  // exists?
  if (!ps_exists(psid))
	return 0;

  // open, stat collection
  PS *ps = new PS(whoami, psid);
  ps->fetch(store);
  ps_map[psid] = ps;

  return ps;
}

void OSD::close_ps(ps_t psid)
{
  assert(0);
}

void OSD::remove_ps(ps_t psid) 
{
  assert(0);
}
*/


// PG

void OSD::get_pg_list(list<pg_t>& ls)
{
  // just list collections; assume they're all pg's (for now)
  store->list_collections(ls);
}

bool OSD::pg_exists(pg_t pgid) 
{
  struct stat st;
  if (store->collection_stat(pgid, &st) == 0) 
	return true;
  else
	return false;
}

PG *OSD::create_pg(pg_t pgid)
{
  assert(pg_map.count(pgid) == 0);
  assert(!pg_exists(pgid));

  PG *pg = new PG(whoami, pgid);
  //pg->info.created = osdmap->get_version();
  pg->last_complete = osdmap->get_version();
  
  pg->store(store);
  pg_map[pgid] = pg;
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
  PG *pg = new PG(whoami, pgid);
  pg->fetch(store);
  pg_map[pgid] = pg;

  return pg;
}
 




/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(list<pg_t>& ls)
{
  dout(7) << "advance_map version " << osdmap->get_version() << endl;
  
  // scan pg's
  for (list<pg_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	pg_t pgid = *it;
	PG *pg = get_pg(pgid);
	assert(pg);
	
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
	
	if (role != pg->get_role()) {
	  // role change.
	  dout(10) << " " << *pg << " role change " << pg->get_role() << " -> " << role << endl; 
	  
	  // old primary?
	  if (pg->get_role() == 0) {
		// drop peers
		take_waiters(pg->waiting_for_peered);
		pg->drop_peers();
		pg->state_clear(PG_STATE_CLEAN);
		pg->discard_recovery_plan();
	  }

	  // new primary?
	  if (role == 0) {
		pg->set_primary_since(osdmap->get_version());
		pg->state_clear(PG_STATE_PEERED);
	  } else {
		// we need to announce
		pg->state_set(PG_STATE_STRAY);

		if (nrep == 0) 
		  dout(1) << "crashed pg " << *pg << endl;
	  }
	  
	} else {
	  // no role change.
	  // did primary change?
	  if (primary != pg->get_primary()) {
		dout(10) << " " << *pg << " acting primary change " << pg->get_primary() << " -> " << primary << ", !peered" << endl;
		
		// we need to announce
		pg->state_set(PG_STATE_STRAY);
	  } else {
		// primary is the same.
		if (role == 0) {
		  // i am (still) primary. but replica set changed.
		  dout(10) << " " << *pg << " replica set changed, !clean !peered" << endl;
		  pg->state_clear(PG_STATE_PEERED);
		  pg->state_clear(PG_STATE_CLEAN);
		}
	  }
	}

	// update PG
	pg->acting = acting;
	pg->calc_role(whoami);
	pg->store(store);


	// scan down osds
	for (set<int>::const_iterator down = osdmap->get_down_osds().begin();
		 down != osdmap->get_down_osds().end();
		 down++) {
	  PGPeer *pgp = pg->get_peer(*down);
	  if (!pgp) continue;

	  dout(10) << " " << *pg << " peer osd" << *down << " is down, removing" << endl;
	  pg->remove_peer(*down);
	  
	  // NAK any ops to the down osd
	  if (replica_pg_osd_tids[pgid].count(*down)) {
		set<__uint64_t> s = replica_pg_osd_tids[pgid][*down];
		dout(10) << " " << *pg << " naking replica ops to down osd" << *down << " " << s << endl;
		for (set<__uint64_t>::iterator tid = s.begin();
			 tid != s.end();
			 tid++)
		  ack_replica_op(*tid, -1, *down);
	  }
	}

  }

}

void OSD::activate_map(list<pg_t>& ls)
{
  dout(7) << "activate_map version " << osdmap->get_version() << endl;

  map< int, map<pg_t, version_t> > notify_list;  // primary -> pgid -> last_complete
  map< int, map<PG*,int> >   start_map;    // peer -> PG -> peer_role

  // scan pg's
  for (list<pg_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	pg_t pgid = *it;
	PG *pg = get_pg(pgid);
	assert(pg);

	if (pg->get_role() == 0) {
	  // i am primary
	  start_peers(pg, start_map);
	} 
	else if (pg->is_stray()) {
	  // i am residual|replica
	  notify_list[pg->get_primary()][pgid] = pg->get_last_complete();
	}
  }  

  // notify? (residual|replica)
  for (map< int, map<pg_t, version_t> >::iterator pit = notify_list.begin();
	   pit != notify_list.end();
	   pit++)
	peer_notify(pit->first, pit->second);

  // start peer? (primary)
  for (map< int, map<PG*, int> >::iterator pit = start_map.begin();
	   pit != start_map.end();
	   pit++)
	peer_start(pit->first, pit->second);

}



/** peer_notify
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */
void OSD::peer_notify(int primary, map<pg_t, version_t>& pg_list)
{
  dout(7) << "peer_notify osd" << primary << " on " << pg_list.size() << " PGs" << endl;
  MOSDPGNotify *m = new MOSDPGNotify(osdmap->get_version(), pg_list);
  messenger->send_message(m, MSG_ADDR_OSD(primary));
}


void OSD::start_peers(PG *pg, map< int, map<PG*,int> >& start_map) 
{
  dout(10) << " " << *pg << " was last_complete " << pg->get_last_complete() << endl;

  // determine initial peer set
  map<int,int> peerset;  // peer -> role
  
  // prior map(s), if OSDs are still up
  for (version_t epoch = pg->get_last_complete();
	   epoch < osdmap->get_version();
	   epoch++) {
	OSDMap *omap = get_osd_map(epoch);
	assert(omap);
	
	vector<int> acting;
	omap->pg_to_acting_osds(pg->get_pgid(), acting);
	
	for (unsigned i=0; i<acting.size(); i++) 
	  if (osdmap->is_up(acting[i]))
		peerset[acting[i]] = -1;
  }
  
  // current map
  for (unsigned i=1; i<pg->acting.size(); i++)
	peerset[pg->acting[i]] = i>0 ? 1:0;
  

  // check peers
  bool havepeers = true;
  for (map<int,int>::iterator it = peerset.begin();
	   it != peerset.end();
	   it++) {
	int who = it->first;
	int role = it->second;
	if (who == whoami) continue;      // nevermind me

	PGPeer *pgp = pg->get_peer(who);
	if (pgp && pgp->is_active() &&
		pgp->get_role() == role) {
	  dout(10) << " " << *pg << " actively peered with osd" << who << " role " << role << endl;	  
	} else {
	  if (pgp) {
		pg->remove_peer(who);
		dout(10) << " " << *pg << " need to re-peer with osd" << who << " role " << role << endl;
	  } else {
		dout(10) << " " << *pg << " need to peer with osd" << who << " role " << role << endl;
	  }
	  start_map[who][pg] = role;
	  havepeers = false;
	}
  }

  if (havepeers && 
	  !pg->is_peered()) {
	dout(10) << " " << *pg << " already has necessary peers, analyzing" << endl;
	pg->mark_peered();
	pg->plan_recovery(store, osdmap->get_version());
	do_recovery(pg);
  }
}


/** peer_start
 * initiate a peer session with a replica on given list of PGs
 */
void OSD::peer_start(int replica, map<PG*,int>& pg_map)
{
  dout(7) << "peer_start with osd" << replica << " on " << pg_map.size() << " PGs" << endl;
  
  list<pg_t> pgids;

  for (map<PG*,int>::iterator it = pg_map.begin();
	   it != pg_map.end();
	   it++) {
	PG *pg = it->first;
	int role = it->second;
	
	assert(pg->get_peer(replica) == 0);
	//PGPeer *p = 
	pg->new_peer(replica, role);
	
	// set last_request stamp?
	// ...

	pgids.push_back(pg->get_pgid());	// add to list
  }

  MOSDPGPeer *m = new MOSDPGPeer(osdmap->get_version(), pgids);
  messenger->send_message(m,
						  MSG_ADDR_OSD(replica));
}



bool OSD::require_current_map(Message *m, version_t v) 
{
  int from = MSG_ADDR_NUM(m->get_source());

  // older map?
  if (v < osdmap->get_version()) {
	dout(7) << "  from old map version " << v << " < " << osdmap->get_version() << endl;
	delete m;   // discard and ignore.
	return false;
  }

  // newer map?
  if (v > osdmap->get_version()) {
	dout(7) << "  from newer map version " << v << " > " << osdmap->get_version() << endl;
	wait_for_new_map(m);
	return false;
  }

  // down?
  if (osdmap->is_down(from)) {
	dout(7) << "  from down OSD osd" << from << ", pinging" << endl;
	// FIXME
	return false;
  }

  assert(v == osdmap->get_version());

  return true;
}


/*
 * require that we have same (or newer) map, and that
 * the source is the pg primary.
 */
bool OSD::require_current_pg_primary(Message *m, version_t v, PG *pg) 
{
  int from = MSG_ADDR_NUM(m->get_source());

  // newer map?
  if (v > osdmap->get_version()) {
	dout(7) << "  from newer map version " << v << " > " << osdmap->get_version() << endl;
	wait_for_new_map(m);
	return false;
  }

  // older map?
  if (v < osdmap->get_version()) {
	// same primary?   
	// FIXME.. line of succession must match!
	if (from != pg->get_primary()) {
	  dout(7) << "  not from pg primary osd" << pg->get_primary() << ", dropping" << endl;
	  delete m;   // discard and ignore.
	  return false;
	}
  }

  // down?
  if (osdmap->is_down(from)) {
	dout(7) << "  from down OSD osd" << from << ", pinging" << endl;
	// FIXME
	return false;
  }

  return true;
}



void OSD::handle_pg_notify(MOSDPGNotify *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_pg_notify from osd" << from << endl;

  if (!require_current_map(m, m->get_version())) return;
  
  // look for unknown PGs i'm primary for
  map< int, map<PG*,int> > start_map;

  for (map<pg_t, version_t>::iterator it = m->get_pg_list().begin();
	   it != m->get_pg_list().end();
	   it++) {
	pg_t pgid = it->first;
	PG *pg = get_pg(pgid);

	if (!pg) {
	  pg = create_pg(pgid);

	  int nrep = osdmap->pg_to_acting_osds(pgid, pg->acting);
	  assert(nrep > 0);
	  assert(pg->acting[0] == whoami);
	  pg->set_role(0);
	  pg->set_primary_since( osdmap->get_version() );  // FIXME: this may miss a few epochs!
	  pg->set_last_complete( 0 );

	  dout(10) << " " << *pg << " is new, nrep=" << nrep << endl;	  

	  // start peers
	  start_peers(pg, start_map);

	  // kick any waiters
	  if (waiting_for_pg.count(pgid)) {
		take_waiters(waiting_for_pg[pgid]);
		waiting_for_pg.erase(pgid);
	  }
	}

	// peered with this guy specifically?
	PGPeer *pgp = pg->get_peer(from);
	if (!pgp && 
		start_map[from].count(pg) == 0) {
	  dout(7) << " " << *pg << " primary needs to peer with residual notifier osd" << from << endl;
	  start_map[from][pg] = -1; 
	}
  }
  
  // start peers?
  if (start_map.empty()) {
	dout(7) << " no new peers" << endl;
  } else {
	for (map< int, map<PG*,int> >::iterator pit = start_map.begin();
		 pit != start_map.end();
		 pit++)
	  peer_start(pit->first, pit->second);
  }
  
  delete m;
}

void OSD::handle_pg_peer(MOSDPGPeer *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_pg_peer from osd" << from << endl;

  if (!require_current_map(m, m->get_version())) return;

  // go
  MOSDPGPeerAck *ack = new MOSDPGPeerAck(osdmap->get_version());

  for (list<pg_t>::iterator it = m->get_pg_list().begin();
	   it != m->get_pg_list().end();
	   it++) {
	pg_t pgid = *it;
	
	// open PG
	PG *pg = get_pg(pgid);

	// dne?
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
		ack->pg_dne.push_back(pgid);
		continue;
	  }

	  pg = create_pg(pgid);
	  pg->acting = acting;
	  pg->set_role(role);

	  dout(10) << " " << *pg << " dne (before), but i am role " << role << endl;

	  // take any waiters
	  if (waiting_for_pg.count(pgid)) {
		take_waiters(waiting_for_pg[pgid]);
		waiting_for_pg.erase(pgid);
	  }
	}

	// PEER

	// report back state and pg content
	ack->pg_state[pgid].state = pg->get_state();
	ack->pg_state[pgid].last_complete = pg->get_last_complete();
	pg->scan_local_objects(ack->pg_state[pgid].objects, store);	// list my objects
	
	// i am now peered
	pg->state_set(PG_STATE_PEERED);
	pg->state_clear(PG_STATE_STRAY);
	
	dout(10) << "sending peer ack " << *pg << " " << ack->pg_state[pgid].objects.size() << " objects" << endl;
  }

  // reply
  messenger->send_message(ack,
						  MSG_ADDR_OSD(from));

  delete m;
}


void OSD::handle_pg_peer_ack(MOSDPGPeerAck *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_pg_peer_ack from osd" << from << endl;

  if (!require_current_map(m, m->get_version())) return;

  // pg_dne first
  for (list<pg_t>::iterator it = m->pg_dne.begin();
	   it != m->pg_dne.end();
	   it++) {
	PG *pg = get_pg(*it);
	assert(pg);

	dout(10) << " " << *pg << " dne on osd" << from << endl;
	
	PGPeer *pgp = pg->get_peer(from);
	if (pgp) {
	  pg->remove_peer(from);
	} else {
	  dout(10) << "  weird, i didn't have it!" << endl;   // multiple lagged peer requests?
	  assert(0); // not until peer requests span epochs!
	}
  }

  // pg_state
  for (map<pg_t, PGReplicaInfo>::iterator it = m->pg_state.begin();
	   it != m->pg_state.end();
	   it++) {
	PG *pg = get_pg(it->first);
	assert(pg);

	dout(10) << " " << *pg << " osd" << from << " remote state " << it->second.state 
			 << " w/ " << it->second.objects.size() << " objects" << endl;

	PGPeer *pgp = pg->get_peer(from);
	assert(pgp);
	
	pgp->last_complete = it->second.last_complete;
	pgp->objects = it->second.objects;
	pgp->state_set(PG_PEER_STATE_ACTIVE);

	// fully peered?
	bool fully = true;
	for (map<int, PGPeer*>::iterator pit = pg->get_peers().begin();
		 pit != pg->get_peers().end();
		 pit++) {
	  dout(10) << " " << *pg << "  peer osd" << pit->first << " state " << pit->second->get_state() << endl;
	  if (!pit->second->is_active()) fully = false;
	}

	if (fully) {
	  if (!pg->is_peered()) {
		// now we're peered!
		pg->mark_peered();
		
		// waiters?
		take_waiters(pg->waiting_for_peered);

		dout(10) << " " << *pg << " fully peered, analyzing" << endl;
		pg->plan_recovery(store, osdmap->get_version());
		do_recovery(pg);
	  } else {
		// we're already peered.
		// what's the use of this new guy?
		
	  }
	}	  
  }

  // done
  delete m;
}


void OSD::handle_pg_update(MOSDPGUpdate *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_pg_update on " << hex << m->get_pgid() << dec << " from osd" << from << endl;
  
  PG *pg = get_pg(m->get_pgid());
  if (!require_current_pg_primary(m, m->get_version(), pg)) return;

  // update
  if (!pg) {
	dout(7) << "don't have pg " << hex << m->get_pgid() << dec << endl;
  } else {
	// update my info.   --what info?
	//pg->assim_info( m->get_pginfo() );
	
	// complete?
	if (m->is_complete()) 
	  pg->mark_complete();

	pg->store(store);
  }

  delete m;
}



// RECOVERY

void OSD::do_recovery(PG *pg)
{
  // recover
  if (!pg->is_complete()) {
	pg_pull(pg, max_recovery_ops);
  }
  
  // replicate
  if (pg->is_complete()) {
	if (!pg->objects_unrep.empty()) 
	  pg_push(pg, max_recovery_ops);
	if (!pg->objects_stray.empty()) 
	  pg_clean(pg, max_recovery_ops);
  }
}


// pull

void OSD::pg_pull(PG *pg, int maxops)
{
  int ops = pg->num_active_ops();

  dout(7) << "pg_pull pg " << hex << pg->get_pgid() << dec << " " << pg->objects_missing.size() << " to do, " << ops << "/" << maxops << " active" <<  endl;
  
  while (ops < maxops) {
	object_t oid;
	if (!pg->get_next_pull(oid)) {
	  dout(7) << "pg_pull done " << *pg << endl;
	  break;
	}
	pull_replica(pg, oid);
	ops++;
  }  
}

void OSD::pull_replica(PG *pg, object_t oid)
{
  version_t v = pg->objects_missing_v[oid];
  
  // choose a peer
  set<int>::iterator pit = pg->objects_missing[oid].begin();
  PGPeer *p = pg->get_peer(*pit);
  dout(7) << "pull_replica " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

  // add to fetching list
  p->pull(oid, v);
  pg->objects_pulling[oid] = p;

  // send op
  __uint64_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
						  oid, p->pg->get_pgid(),
						  osdmap->get_version(),
						  OSD_OP_REP_PULL);
  op->set_version(v);
  op->set_pg_role(-1);  // whatever, not 0
  messenger->send_message(op, MSG_ADDR_OSD(p->get_peer()));

  // register
  pull_ops[tid] = p;
}

void OSD::op_rep_pull(MOSDOp *op)
{
  dout(7) << "rep_pull on " << hex << op->get_oid() << dec << " v " << op->get_version() << endl;
  lock_object(op->get_oid());
  
  // get object size
  struct stat st;
  int r = store->stat(op->get_oid(), &st);
  assert(r == 0);

  // check version
  version_t v = 0;
  store->getattr(op->get_oid(), "version", &v, sizeof(v));
  assert(v == op->get_version());
  
  // read
  bufferptr bptr = new buffer(st.st_size);   // prealloc space for entire read
  long got = store->read(op->get_oid(), 
						 st.st_size, 0,
						 bptr.c_str());
  assert(got == st.st_size);
  
  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true); 
  bptr.set_length(got);   // properly size the buffer
  bufferlist bl;
  bl.push_back( bptr );
  reply->set_result(0);
  reply->set_data(bl);
  reply->set_length(got);
  reply->set_offset(0);
  
  messenger->send_message(reply, op->get_asker());

  unlock_object(op->get_oid());
  delete op;
}

void OSD::op_rep_pull_reply(MOSDOpReply *op)
{
  object_t o = op->get_oid();
  version_t v = op->get_version();

  dout(7) << "rep_pull_reply " << hex << o << dec << " v " << v << " size " << op->get_length() << endl;

  osd_lock.Lock();
  PGPeer *p = pull_ops[op->get_tid()];
  PG *pg = p->pg;
  assert(p);   // FIXME: how will this work?
  assert(p->is_pulling(o));
  assert(p->pulling_version(o) == v);
  osd_lock.Unlock();

  // write it and add it to the PG
  store->write(o, op->get_length(), 0, op->get_data().c_str());
  p->pg->add_object(store, o);

  store->setattr(o, "version", &v, sizeof(v));

  // close out pull op.
  osd_lock.Lock();
  pull_ops.erase(op->get_tid());

  pg->pulled(o, v, p);
  
  // finish waiters
  if (pg->waiting_for_missing_object.count(o)) 
	take_waiters(pg->waiting_for_missing_object[o]);

  // more?
  do_recovery(pg);

  osd_lock.Unlock();
  
  delete op;
}


// push

void OSD::pg_push(PG *pg, int maxops)
{
  int ops = pg->num_active_ops();

  dout(7) << "pg_push pg " << hex << pg->get_pgid() << dec << " " << pg->objects_unrep.size() << " objects, " << ops << "/" << maxops << " active ops" <<  endl;
  
  while (ops < maxops) {
	object_t oid;
	if (!pg->get_next_push(oid)) {
	  dout(7) << "pg_push done " << *pg << endl;
	  break;
	}

	push_replica(pg, oid);
	ops++;
  }  
  
}

void OSD::push_replica(PG *pg, object_t oid)
{
  version_t v = 0;
  store->getattr(oid, "version", &v, sizeof(v));
  assert(v > 0);

  set<int>& peers = pg->objects_unrep[oid];

  dout(7) << "push_replica " << hex << oid << dec << " v " << v << " to osds " << peers << endl;
  
  // load object content
  struct stat st;
  store->stat(oid, &st);
  bufferptr b = new buffer(st.st_size);
  store->read(oid, st.st_size, 0, b.c_str());

  for (set<int>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	PGPeer *p = pg->get_peer(*pit);
	assert(p);
	
	// add to list
	p->push(oid, v);
	pg->objects_pushing[oid].insert(p);

	// send op
	__uint64_t tid = ++last_tid;
	MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
							oid, pg->get_pgid(),
							osdmap->get_version(),
							OSD_OP_REP_PUSH);
	op->set_version(v);
	op->set_pg_role(-1);  // whatever, not 0
	
	// include object content
	op->get_data().append(b);
	op->set_length(st.st_size);
	op->set_offset(0);
	
	messenger->send_message(op, MSG_ADDR_OSD(*pit));

	// register
	push_ops[tid] = p;
  }

}

void OSD::op_rep_push(MOSDOp *op)
{
  dout(7) << "rep_push on " << hex << op->get_oid() << dec << " v " << op->get_version() <<  endl;
  lock_object(op->get_oid());

  // exists?
  if (store->exists(op->get_oid())) {
	store->truncate(op->get_oid(), 0);

	version_t ov = 0;
	store->getattr(op->get_oid(), "version", &ov, sizeof(ov));
	assert(ov <= op->get_version());
  }

  // write out buffers
  bufferlist bl;
  bl.claim( op->get_data() );

  off_t off = 0;
  for (list<bufferptr>::iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {
	int r = store->write(op->get_oid(),
						 (*it).length(), off,
						 (*it).c_str(), 
						 false);  // write async, no rush
	assert((unsigned)r == (*it).length());
	off += (*it).length();
  }

  // set version
  version_t v = op->get_version();
  store->setattr(op->get_oid(), "version", &v, sizeof(v));

  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true);
  messenger->send_message(reply, op->get_asker());
  
  unlock_object(op->get_oid());
  delete op;
}

void OSD::op_rep_push_reply(MOSDOpReply *op)
{
  object_t oid = op->get_oid();
  version_t v = op->get_version();

  dout(7) << "rep_push_reply " << hex << oid << dec << endl;

  osd_lock.Lock();
  PGPeer *p = push_ops[op->get_tid()];
  PG *pg = p->pg;
  assert(p);   // FIXME: how will this work?
  assert(p->is_pushing(oid));
  assert(p->pushing_version(oid) == v);

  // close out push op.
  push_ops.erase(op->get_tid());
  pg->pushed(oid, v, p);

  if (p->is_complete()) {
	dout(7) << " telling replica they are complete" << endl;
	messenger->send_message(new MOSDPGUpdate(osdmap->get_version(), pg->get_pgid(), true),
							MSG_ADDR_OSD(p->get_peer()));
  }

  // anybody waiting on this object?
  if (pg->waiting_for_clean_object.count(oid) &&
	  pg->objects_unrep.count(oid) == 0) {
	dout(7) << "kicking waiter on now replicated object " << hex << oid << dec << endl;
	take_waiters(pg->waiting_for_clean_object[oid]);
	pg->waiting_for_clean_object.erase(oid);
  }

  // more?
  do_recovery(pg);

  osd_lock.Unlock();
  
  delete op;
}


// clean

void OSD::pg_clean(PG *pg, int maxops)
{
  int ops = pg->num_active_ops();

  dout(7) << "pg_clean pg " << hex << pg->get_pgid() << dec << " " << pg->objects_stray.size() << " stray, " << ops << "/" << maxops << " active ops" <<  endl;
  
  while (ops < maxops) {
	object_t oid;
	if (!pg->get_next_remove(oid)) {
	  dout(7) << "pg_clean done " << *pg << endl;
	  break;
	}
	
	remove_replica(pg, oid);
	ops++;
  }  
}

void OSD::remove_replica(PG *pg, object_t oid)
{
  dout(7) << "remove_replica " << hex << oid << dec << endl;//" v " << v << " from osd" << p->get_peer() << endl;

  map<int,version_t>& stray = pg->objects_stray[oid];
  for (map<int, version_t>::iterator it = stray.begin();
	   it != stray.end();
	   it++) {
	PGPeer *p = pg->get_peer(it->first);
	assert(p);
	const version_t v = it->second;

	p->remove(oid, v);
	pg->objects_removing[oid][p] = v;
  
	// send op
	__uint64_t tid = ++last_tid;
	MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
							oid, p->pg->get_pgid(),
							osdmap->get_version(),
							OSD_OP_REP_REMOVE);
	op->set_version(v);
	op->set_pg_role(-1);  // whatever, not 0
	messenger->send_message(op, MSG_ADDR_OSD(p->get_peer()));
	
	// register
	remove_ops[tid] = p;
  }
}

void OSD::op_rep_remove(MOSDOp *op)
{
  dout(7) << "rep_remove on " << hex << op->get_oid() << dec << " v " << op->get_version() <<  endl;
  lock_object(op->get_oid());
  
  // sanity checks
  assert(store->exists(op->get_oid()));

  version_t v = 0;
  store->getattr(op->get_oid(), "version", &v, sizeof(v));
  assert(v == op->get_version());

  // remove
  store->collection_remove(op->get_pg(), op->get_oid());
  int r = store->remove(op->get_oid());
  assert(r == 0);

  // reply
  messenger->send_message(new MOSDOpReply(op, r, osdmap, true), 
						  op->get_asker());

  unlock_object(op->get_oid());
  delete op;
}

void OSD::op_rep_remove_reply(MOSDOpReply *op)
{
  object_t oid = op->get_oid();
  version_t v = op->get_version();
  dout(7) << "rep_remove_reply " << hex << oid << dec << endl;

  osd_lock.Lock();
  PGPeer *p = remove_ops[op->get_tid()];
  PG *pg = p->pg;
  assert(p);   // FIXME: how will this work?
  assert(p->is_removing(oid));
  assert(p->removing_version(oid) == v);
  
  // close out push op.
  remove_ops.erase(op->get_tid());
  pg->removed(oid, v, p);
  
  if (p->is_complete()) {
	dout(7) << " telling replica they are complete" << endl;
	messenger->send_message(new MOSDPGUpdate(osdmap->get_version(), pg->get_pgid(), true),
							MSG_ADDR_OSD(p->get_peer()));
  }

  // more?
  do_recovery(pg);

  osd_lock.Unlock();
  
  delete op;
}


void OSD::op_rep_modify(MOSDOp *op)
{ 
  // when we introduce unordered messaging.. FIXME
  object_t oid = op->get_oid();
  version_t ov = 0;
  if (store->exists(oid)) 
	store->getattr(oid, "version", &ov, sizeof(ov));

  assert(op->get_old_version() == ov);

  // PG
  PG *pg = get_pg(op->get_pg());
  assert(pg);

  
  dout(12) << "rep_modify " << hex << oid << dec << " v " << op->get_version() << " (i have " << ov << ")" << endl;

  // pre-ack
  //MOSDOpReply *ack1 = new MOSDOpReply(op, 0, osdmap);
  //messenger->send_message(ack1, op->get_asker());

  /*
  // update pg stamp(s)
  pg->last_modify_stamp = op->get_version();
  if (pg->is_complete()) 
	pg->last_complete_stamp = pg->last_modify_stamp;
  */

  int r = 0;
  if (op->get_op() == OSD_OP_REP_WRITE) {
	// write
	r = apply_write(op, false, op->get_version());
	if (ov == 0) pg->add_object(store, oid);
  } else if (op->get_op() == OSD_OP_REP_DELETE) {
	// delete
	store->collection_remove(pg->get_pgid(), op->get_oid());
	r = store->remove(oid);
  } else if (op->get_op() == OSD_OP_REP_TRUNCATE) {
	// truncate
	r = store->truncate(oid, op->get_offset());
  } else assert(0);
  
  // ack
  MOSDOpReply *ack2 = new MOSDOpReply(op, 0, osdmap, true);
  messenger->send_message(ack2, op->get_asker());

  delete op;
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
	if (op->get_map_version() > osdmap->get_version()) {
	  // op's is newer
	  dout(7) << "op map " << op->get_map_version() << " > " << osdmap->get_version() << endl;
	  wait_for_new_map(op);
	  return;
	}

	// am i the primary?
	int acting_primary = osdmap->get_pg_acting_primary( pgid );
	
	if (acting_primary != whoami) {
	  if (acting_primary >= 0) {
		dout(7) << " acting primary is " << acting_primary << ", forwarding" << endl;
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
	  dout(7) << "hit non-existent pg " << hex << op->get_pg() << dec << ", waiting" << endl;
	  waiting_for_pg[pgid].push_back(op);
	  return;
	}
	else {
	  if (!pg->is_complete()) {
		// consult PG object map
		if (pg->objects_missing.count(op->get_oid())) {
		  // need to pull
		  dout(7) << "need to pull object " << hex << op->get_oid() << dec << endl;
		  if (!pg->objects_pulling.count(op->get_oid())) 
			pull_replica(pg, op->get_oid());
		  pg->waiting_for_missing_object[op->get_oid()].push_back(op);
		  return;
		}
	  }
	  
	}	
	
  } else {
	// REPLICATION OP
	
    // check osd map
	if (op->get_map_version() != osdmap->get_version()) {
	  // make sure source is still primary
	  int curprimary = osdmap->get_pg_acting_primary(op->get_pg());
	  int myrole = osdmap->get_pg_acting_role(op->get_pg(), whoami);
	  
	  if (curprimary != MSG_ADDR_NUM(op->get_source()) ||
		  myrole <= 0) {
		dout(5) << "op map " << op->get_map_version() << " != " << osdmap->get_version() << ", primary changed on pg " << hex << op->get_pg() << dec << endl;
		MOSDOpReply *fail = new MOSDOpReply(op, -1, osdmap, false);
		messenger->send_message(fail, op->get_asker());
		return;
	  } else {
		dout(5) << "op map " << op->get_map_version() << " != " << osdmap->get_version() << ", primary same on pg " << hex << op->get_pg() << dec << endl;
	  }
	}  
  }

  // queue op
  if (g_conf.osd_maxthreads < 1) {
	pending_ops++;
	do_op(op);
  } else
	queue_op(op);
}

void OSD::queue_op(MOSDOp *op) {
  // inc pending count
  osd_lock.Lock();
  pending_ops++;
  osd_lock.Unlock();

  threadpool->put_op(op);
}


void doop(OSD *u, MOSDOp *p) {
  u->do_op(p);
}

void OSD::do_op(MOSDOp *op) 
{
  dout(12) << "do_op " << op << endl;

  logger->inc("op");

  // replication ops?
  if (OSD_OP_IS_REP(op->get_op())) {
	// replication/recovery
	switch (op->get_op()) {
	  // push/pull/remove
	case OSD_OP_REP_PULL:
	  op_rep_pull(op);
	  break;
	case OSD_OP_REP_PUSH:
	  op_rep_push(op);
	  break;
	case OSD_OP_REP_REMOVE:
	  op_rep_remove(op);
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
	pg_t pgid = op->get_pg();
	PG *pg = get_pg(pgid);

	// PG must be peered for all client ops.
	if (!pg) {
	  dout(7) << "op_write pg " << hex << pgid << dec << " dne (yet)" << endl;
	  waiting_for_pg[pgid].push_back(op);
	}	
	else if (!pg->is_peered()) {
	  dout(7) << "op_write " << *pg << " not peered (yet)" << endl;
	  pg->waiting_for_peered.push_back(op);
	}
	else {
	  // do op
	  switch (op->get_op()) {
	  case OSD_OP_READ:
		op_read(op, pg);
		break;
	  case OSD_OP_STAT:
		op_stat(op, pg);
		break;
	  case OSD_OP_WRITE:
	  case OSD_OP_DELETE:
	  case OSD_OP_TRUNCATE:
		op_modify(op, pg);
		break;
	  default:
		assert(0);
	  }
	}
  }

  //dout(12) << "finish op " << op << endl;

  // finish
  osd_lock.Lock();
  assert(pending_ops > 0);
  pending_ops--;
  if (pending_ops == 0 && waiting_for_no_ops)
	no_pending_ops.Signal();
  osd_lock.Unlock();
}

void OSD::wait_for_no_ops()
{
  osd_lock.Lock();
  if (pending_ops > 0) {
	dout(7) << "wait_for_no_ops - waiting for " << pending_ops << endl;
	waiting_for_no_ops = true;
	no_pending_ops.Wait(osd_lock);
	waiting_for_no_ops = false;
	assert(pending_ops == 0);
  } 
  dout(7) << "wait_for_no_ops - none" << endl;
  osd_lock.Unlock();
}







// ===============================
// OPS

// READ OPS

bool OSD::object_complete(PG *pg, object_t oid, Message *op)
{
  if (!pg->is_complete()) {
	if (pg->objects_missing.count(oid)) {
	  dout(7) << "object " << hex << oid << dec << /*" v " << v << */" in " << *pg 
			  << " exists but not local (yet)" << endl;
	  pg->waiting_for_missing_object[oid].push_back(op);
	  return false;
	}
  }
  return true;
}

void OSD::op_read(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();
  lock_object(oid);
  
  // version?  clean?
  if (!object_complete(pg, oid, op)) {
	unlock_object(oid);
	return;
  }

  // read into a buffer
  bufferptr bptr = new buffer(op->get_length());   // prealloc space for entire read
  long got = store->read(oid, 
						 op->get_length(), op->get_offset(),
						 bptr.c_str());
  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true); 
  if (got >= 0) {
	bptr.set_length(got);   // properly size the buffer

	// give it to the reply in a bufferlist
	bufferlist bl;
	bl.push_back( bptr );
	
	reply->set_result(0);
	reply->set_data(bl);
	reply->set_length(got);
  } else {
	bptr.set_length(0);
	reply->set_result(got);   // error
	reply->set_length(0);
  }
  
  dout(12) << "read got " << got << " / " << op->get_length() << " bytes from obj " << hex << oid << dec << endl;

  logger->inc("rd");
  if (got >= 0) logger->inc("rdb", got);
  
  // send it
  messenger->send_message(reply, op->get_asker());

  delete op;

  unlock_object(oid);
}

void OSD::op_stat(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();
  lock_object(oid);
  
  // version?  clean?
  if (!object_complete(pg, oid, op)) {
	unlock_object(oid);
	return;
  }

  struct stat st;
  memset(&st, sizeof(st), 0);
  int r = store->stat(oid, &st);
  
  dout(3) << "stat on " << hex << oid << dec << " r = " << r << " size = " << st.st_size << endl;
	  
  MOSDOpReply *reply = new MOSDOpReply(op, r, osdmap, true);
  reply->set_object_size(st.st_size);
  messenger->send_message(reply, op->get_asker());
	  
  logger->inc("stat");
  delete op;

  unlock_object(oid);
}



// WRITE OPS

int OSD::apply_write(MOSDOp *op, bool write_sync, version_t v)
{
  // take buffers from the message
  bufferlist bl;
  bl.claim( op->get_data() );
  
  // write out buffers
  off_t off = op->get_offset();
  for (list<bufferptr>::iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {

	int r = store->write(op->get_oid(),
						 (*it).length(), off,
						 (*it).c_str(),
						 write_sync);  // write synchronously
	off += (*it).length();
	if (r < 0) {
	  dout(1) << "write error on " << hex << op->get_oid() << dec << " len " << (*it).length() << "  off " << off << "  r = " << r << endl;
	  assert(r >= 0);
	}
  }

  // set version
  store->setattr(op->get_oid(), "version", &v, sizeof(v));

  return 0;
}


bool OSD::object_clean(PG *pg, object_t oid, version_t& v, Message *op)
{
  v = 0;
  
  if (pg->is_complete() && pg->is_clean()) {
	// PG is complete+clean, easy shmeasy!
	if (store->exists(oid)) {
	  store->getattr(oid, "version", &v, sizeof(v));
	  assert(v>0);
	} 
  } else {
	// PG is recovering, blech.

	// does oid exist, and what version.
	if (pg->is_complete()) {
	  // pg !clean, complete
	  if (store->exists(oid)) {
		store->getattr(oid, "version", &v, sizeof(v));
		assert(v > 0);
	  }
	} else {
	  // pg !clean, !complete
	  if (pg->objects_missing.count(oid)) 
		v = pg->objects_missing_v[oid];
	}

	if (v > 0) {
	  dout(10) << " pg not clean, checking if " << hex << oid << dec << " v " << v << " is specifically clean yet! " << *pg << endl;
	  // object (logically) exists
	  if (!pg->existant_object_is_clean(oid, v)) {
		dout(7) << "object " << hex << oid << dec << " v " << v << " in " << *pg 
				<< " exists but is not clean" << endl;
		pg->waiting_for_clean_object[oid].push_back(op);
		if (pg->objects_pushing.count(oid) == 0)
		  push_replica(pg, oid);
		return false;
	  }
	} else {
	  // object (logically) dne
	  if (store->exists(oid) ||
		  !pg->nonexistant_object_is_clean(oid)) {
		dout(7) << "object " << hex << oid << dec << " v " << v << " in " << *pg 
				<< " dne but is not clean" << endl;
		pg->waiting_for_clean_object[oid].push_back(op);
		if (pg->objects_removing.count(oid) == 0)
		  remove_replica(pg, oid);
		return false;
	  }
	}
  }

  return true;
}

void OSD::op_modify(MOSDOp *op, PG *pg)
{
  object_t oid = op->get_oid();

  char *opname = 0;
  if (op->get_op() == OSD_OP_WRITE) opname = "op_write";
  if (op->get_op() == OSD_OP_DELETE) opname = "op_delete";
  if (op->get_op() == OSD_OP_TRUNCATE) opname = "op_truncate";

  lock_object(oid);

  // version?  clean?
  version_t ov = 0;  // 0 == dne (yet)
  if (!object_clean(pg, oid, ov, op)) {
	unlock_object(oid);
	return;
  }

  version_t v = messenger->peek_lamport();

  dout(12) << opname << " " << hex << oid << dec << " v " << v << endl;  

  // issue replica writes
  replica_write_lock.Lock();
  assert(replica_write_tids.count(op) == 0);
  assert(replica_write_local.count(op) == 0);

  for (unsigned i=1; i<pg->acting.size(); i++) {
	int osd = pg->acting[i];
	dout(7) << "  replica write in " << *pg << " o " << hex << oid << dec << " to osd" << osd << endl;
	
	// forward the write
	__uint64_t tid = ++last_tid;
	MOSDOp *wr = new MOSDOp(tid,
							messenger->get_myaddr(),
							oid,
							op->get_pg(),
							osdmap->get_version(),
							100+op->get_op());
	wr->get_data() = op->get_data();   // copy bufferlist
	wr->set_length(op->get_length());
	wr->set_offset(op->get_offset());
	wr->set_version(v);
	wr->set_old_version(ov);
	wr->set_pg_role(1); // replica
	messenger->send_message(wr, MSG_ADDR_OSD(osd));
	
	replica_write_tids[op].insert(tid);
	replica_writes[tid] = op;
	replica_pg_osd_tids[pg->get_pgid()][osd].insert(tid);
  }
  replica_write_lock.Unlock();

  // pre-ack
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, false);
  messenger->send_message(reply, op->get_asker());
  
  /*
  // update pg stamp(s)
  pg->last_modify_stamp = v;
  if (pg->is_complete())
	pg->last_complete_stamp = v;
  */

  // do it
  int r;
  if (op->get_op() == OSD_OP_WRITE) {
	// write
	r = apply_write(op, true, v);
	
	// put new object in proper collection
	if (ov == 0) pg->add_object(store, oid);
  } 
  else if (op->get_op() == OSD_OP_TRUNCATE) {
	// truncate
	r = store->truncate(oid, op->get_offset());
  }
  else if (op->get_op() == OSD_OP_DELETE) {
	// delete
	store->collection_remove(pg->get_pgid(), op->get_oid());
	r = store->remove(oid);
  }
  else assert(0);

  // reply?
  replica_write_lock.Lock();
  if (replica_write_tids.count(op) == 0) {
	// all replica writes completed.
	if (replica_write_result[op] == 0) {
	  dout(10) << opname << " wrote locally: rep writes already finished, replying" << endl;
	  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap, true);
	  messenger->send_message(reply, op->get_asker());
	  delete op;
	} else {
	  dout(10) << opname << " wrote locally, but rep writes failed, fw to new primary" << endl;
	  //finished.push_back(op);
	  messenger->send_message(op, MSG_ADDR_OSD(pg->get_primary()));
	}
	replica_write_result.erase(op);
  } else {
	// note that it's written locally
	dout(10) << opname << " wrote locally: rep writes not yet finished, waiting" << endl;
	replica_write_local.insert(op);
  }
  replica_write_lock.Unlock();

  unlock_object(oid);
}



