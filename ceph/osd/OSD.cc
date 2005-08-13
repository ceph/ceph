
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
#include "messages/MOSDRGNotify.h"
#include "messages/MOSDRGPeer.h"
#include "messages/MOSDRGPeerAck.h"

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
	dout(7) << "lock_object " << hex << oid << dec << " waiting as " << &c << endl;
	object_lock_waiters[oid].push_back(&c);
	c.Wait(osd_lock);
	assert(object_lock.count(oid));
  } else {
	dout(7) << "lock_object " << hex << oid << dec << endl;
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
	dout(7) << "unlock_object " << hex << oid << dec << " waking up next guy " << c << endl;
	ls.pop_front();
	if (ls.empty()) 
	  object_lock_waiters.erase(oid);
	c->Signal();
  } else {
	// nobody waiting
	dout(7) << "unlock_object " << hex << oid << dec << endl;
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
	// host monitor
  case MSG_PING_ACK:
  case MSG_FAILURE_ACK:
	monitor->proc_message(m);
	break;
  
	// map and replication
  case MSG_OSD_MAP:
	handle_osd_map((MOSDMap*)m);
	break;
	
  case MSG_OSD_RG_NOTIFY:
	handle_rg_notify((MOSDRGNotify*)m);
	break;
  case MSG_OSD_RG_PEER:
	handle_rg_peer((MOSDRGPeer*)m);
	break;
  case MSG_OSD_RG_PEERACK:
	handle_rg_peer_ack((MOSDRGPeerAck*)m);
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
	{ // oldcrap
	  /*
	  replica_write_lock.Lock();
	  MOSDOp *op = replica_writes[m->get_tid()];
	  dout(7) << "got replica write ack tid " << m->get_tid() << " orig op " << op << endl;
	  
	  replica_write_tids[op].erase(m->get_tid());
	  if (replica_write_tids[op].empty())
		replica_write_cond[op]->Signal();
	  
	  replica_write_lock.Unlock();
	  */
	}
	break;

  default:
	assert(0);
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

  osd_lock.Lock();
  waiting_for_osdmap.push_back(m);
  osd_lock.Unlock();
}


/** update_map
 * assimilate a new OSDMap.  scan rgs.
 */
void OSD::update_map(bufferlist& state)
{
  // decode new map
  if (!osdmap) osdmap = new OSDMap();
  osdmap->decode(state);
  dout(7) << "update_map version " << osdmap->get_version() << endl;

  // scan known replica groups!
  scan_rg();
}


void OSD::handle_osd_map(MOSDMap *m)
{
  // wait for ops to finish
  wait_for_no_ops();

  osd_lock.Lock();     // actually, don't need this if we finish all ops?

  if (!osdmap ||
	  m->get_version() > osdmap->get_version()) {
	if (osdmap) {
	  dout(3) << "handle_osd_map got osd map version " << m->get_version() << " > " << osdmap->get_version() << endl;
	} else {
	  dout(3) << "handle_osd_map got osd map version " << m->get_version() << endl;
	}

	update_map(m->get_osdmap());
	delete m;

	// process waiters
	take_waiters(waiting_for_osdmap);

  } else {
	dout(3) << "handle_osd_map ignoring osd map version " << m->get_version() << " <= " << osdmap->get_version() << endl;
  }
  
  osd_lock.Unlock();
}



// ======================================================
// REPLICATION


// ------------------------------------
// replica groups

void OSD::get_rg_list(list<repgroup_t>& ls)
{
  // just list collections; assume they're all rg's (for now)
  store->list_collections(ls);
}


bool OSD::rg_exists(repgroup_t rgid) 
{
  struct stat st;
  if (store->collection_stat(rgid, &st) == 0) 
	return true;
  else
	return false;
}


RG *OSD::open_rg(repgroup_t rgid)
{
  // already open?
  if (rg_map.count(rgid)) 
	return rg_map[rgid];

  // exists?
  if (!rg_exists(rgid))
	return 0;

  // open, stat collection
  RG *rg = new RG(whoami, rgid);
  rg->fetch(store);
  rg_map[rgid] = rg;

  return rg;
}
 
RG *OSD::new_rg(repgroup_t rgid)
{
  assert(rg_map.count(rgid) == 0);
  assert(!rg_exists(rgid));

  RG *rg = new RG(whoami, rgid);
  rg->store(store);
  rg_map[rgid] = rg;
  return rg;
}




/** 
 * scan replica groups, initiate any replication
 * activities.
 */
void OSD::scan_rg()
{
  //dout(7) << "scan_rg map version " << osdmap->get_version() << endl;

  // scan replica groups
  list<repgroup_t> ls;
  get_rg_list(ls);
  
  map< int, list<repgroup_t> > notify_list;
  map< int, map<RG*,int> >   start_map;    // peer -> RG -> peer_role

  for (list<repgroup_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	repgroup_t rgid = *it;
	RG *rg = open_rg(rgid);
	assert(rg);

	// get active rush mapping
	vector<int> acting;
	int nrep = osdmap->repgroup_to_acting_osds(rgid, acting);
	assert(nrep > 0);
	int primary = acting[0];
	int role = -1;        // -1, 0, 1
	for (int i=0; i<nrep; i++) 
	  if (acting[i] == whoami) role = i>0 ? 1:0;
	
	if (role != rg->get_role()) {
	  // role change.
	  dout(10) << " rg " << hex << rgid << dec << " acting role change " << rg->get_role() << " -> " << role << endl; 
	  
	  // am i old-primary?
	  if (rg->get_role() == 0) {
		// note potential replica set, and drop old peering sessions.
		for (map<int, RGPeer*>::iterator it = rg->get_peers().begin();
			 it != rg->get_peers().end();
			 it++) {
		  dout(10) << " rg " << hex << rgid << dec << " old-primary, dropping old peer " << it->first << endl;
		  rg->get_old_replica_set().insert(it->first);
		  delete it->second;
		}
		rg->get_peers().clear();
	  }

	  // we need to re-peer
	  rg->state_clear(RG_STATE_PEERED);
	  rg->set_role(role);
	  rg->set_primary(primary);
	  rg->store(store);

	  if (role == 0) {
		// i am new primary
		
	  } else {
		// i am replica
		notify_list[primary].push_back(rgid);
	  }
	  
	} else {
	  // no role change.

	  if (role > 0) {  
		// i am replica.
		
		// did primary change?
		if (primary != rg->get_primary()) {
		  dout(10) << " rg " << hex << rgid << dec << " acting primary change " << rg->get_primary() << " -> " << primary << endl;
		  
		  // re-peer
		  rg->state_clear(RG_STATE_PEERED);
		  rg->set_primary(primary);
		  rg->store(store);
		  notify_list[primary].push_back(rgid);
		}
	  }
	}
	
	if (role == 0) {
	  // i am primary.
	  
	  // old peers
	  // ***

	  // check replicas
	  for (int r=1; r<nrep; r++) {
		if (rg->get_peer(r) == 0) {
		  dout(10) << " rg " << hex << rgid << dec << " primary needs to peer with replica " << r << " osd" << acting[r] << endl;
		  start_map[acting[r]][rg] = r;
		} 
	  }
	}
  }
  

  // notify?
  for (map< int, list<repgroup_t> >::iterator pit = notify_list.begin();
	   pit != notify_list.end();
	   pit++)
	peer_notify(pit->first, pit->second);

  // start peer?
  for (map< int, map<RG*, int> >::iterator pit = start_map.begin();
	   pit != start_map.end();
	   pit++)
	peer_start(pit->first, pit->second);


}



/** peer_notify
 * Send an MOSDRGNotify to a primary, with a list of RGs that I have
 * content for, and they are primary for.
 */
void OSD::peer_notify(int primary, list<repgroup_t>& rg_list)
{
  dout(7) << "peer_notify osd" << primary << " on " << rg_list.size() << " RGs" << endl;
  MOSDRGNotify *m = new MOSDRGNotify(osdmap->get_version(), rg_list);
  messenger->send_message(m, MSG_ADDR_OSD(primary));
}


/** peer_start
 * initiate a peer session with a replica on given list of RGs
 */
void OSD::peer_start(int replica, map<RG*,int>& rg_map)
{
  dout(7) << "peer_start with osd" << replica << " on " << rg_map.size() << " RGs" << endl;
  
  list<repgroup_t> rgids;

  for (map<RG*,int>::iterator it = rg_map.begin();
	   it != rg_map.end();
	   it++) {
	RG *rg = it->first;
	int role = it->second;

	RGPeer *rgp = rg->get_peer(replica);
	if (!rgp) {
	  rgp = rg->new_peer(replica, role);
	} else {
	  assert(rgp->get_role() == role);
	}

	// set last_request stamp
	//rgp->last

	// add to list
	rgids.push_back(rg->get_rgid());
  }

  MOSDRGPeer *m = new MOSDRGPeer(osdmap->get_version(), rgids);
  messenger->send_message(m,
						  MSG_ADDR_OSD(replica));
  
}





void OSD::handle_rg_notify(MOSDRGNotify *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_rg_notify from osd" << from << endl;

  // older map?
  if (m->get_version() < osdmap->get_version()) {
	dout(7) << "  from old map version " << m->get_version() << " < " << osdmap->get_version() << endl;
	delete m;   // discard and ignore.*
	return;
  }

  // newer map?
  if (m->get_version() > osdmap->get_version()) {
	dout(7) << "  for newer map version " << m->get_version() << " > " << osdmap->get_version() << endl;
	wait_for_new_map(m);
	return;
  }
  
  assert(m->get_version() == osdmap->get_version());
  
  // look for unknown RGs i'm primary for
  map< int, map<RG*,int> > start_map;

  for (list<repgroup_t>::iterator it = m->get_rg_list().begin();
	   it != m->get_rg_list().end();
	   it++) {
	repgroup_t rgid = *it;
	
	vector<int> acting;
	int nrep = osdmap->repgroup_to_acting_osds(rgid, acting);
	assert(nrep > 0);
	assert(acting[0] == whoami);
	
	// open RG?
	RG *rg = open_rg(rgid);

	// previously unknown RG?
	if (!rg) {
	  rg = new_rg(rgid);
	  dout(10) << " rg " << hex << rgid << dec << " is new, nrep=" << nrep << endl;
	  for (int r=1; r<nrep; r++) {
		dout(10) << " rg " << hex << rgid << dec << " primary needs to peer with replica " << r << " osd" << acting[r] << endl;
		start_map[acting[r]][rg] = r;
	  }
	}

	// peered with this guy specifically?
	RGPeer *rgp = rg->get_peer(from);
	if (!rgp && start_map[from].count(rg) == 0) {
	  dout(7) << " rg " << hex << rgid << dec << " primary needs to peer with residual notifier osd" << from << endl;
	  start_map[from][rg] = -1; 
	}
  }

  // start peers?
  if (start_map.empty()) {
	dout(7) << " no new peers" << endl;
  } else {
	for (map< int, map<RG*,int> >::iterator pit = start_map.begin();
		 pit != start_map.end();
		 pit++)
	  peer_start(pit->first, pit->second);
  }
  
  delete m;
}

void OSD::handle_rg_peer(MOSDRGPeer *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_rg_peer from osd" << from << endl;

  // older map?
  if (m->get_version() < osdmap->get_version()) {
	dout(7) << "  from old map version " << m->get_version() << " < " << osdmap->get_version() << endl;
	delete m;   // discard and ignore.*
	return;
  }

  // newer map?
  if (m->get_version() > osdmap->get_version()) {
	dout(7) << "  for newer map version " << m->get_version() << " > " << osdmap->get_version() << endl;
	wait_for_new_map(m);
	return;
  }
  
  assert(m->get_version() == osdmap->get_version());

  // go
  MOSDRGPeerAck *ack = new MOSDRGPeerAck(osdmap->get_version());

  for (list<repgroup_t>::iterator it = m->get_rg_list().begin();
	   it != m->get_rg_list().end();
	   it++) {
	repgroup_t rgid = *it;
	
	// open RG
	RG *rg = open_rg(rgid);

	// dne?
	if (!rg) {
	  // get active rush mapping
	  vector<int> acting;
	  int nrep = osdmap->repgroup_to_acting_osds(rgid, acting);
	  assert(nrep > 0);
	  int role = -1;
	  for (int i=0; i<nrep; i++) 
		if (acting[i] == whoami) role = i>0 ? 1:0;
	  assert(role != 0);

	  if (role < 0) {
		dout(10) << " rg " << hex << rgid << dec << " dne, and i am not an active replica" << endl;
		ack->rg_dne.push_back(rgid);
		continue;
	  }
	  
	  dout(10) << " rg " << hex << rgid << dec << " dne (yet), but i am new active replica " << role << endl;
	  rg = new_rg(rgid);
	}

	// report back state and rg content
	ack->rg_state[rgid].state = rg->get_state();
	ack->rg_state[rgid].deleted = rg->get_deleted_objects();

	// list objects
	rg->scan_local_objects(store);
	ack->rg_state[rgid].objects = rg->local_objects;
	
	dout(10) << " rg " << hex << rgid << dec << " has state " << rg->get_state() << ", " << ack->rg_state[rgid].objects.size() << " objects" << endl;
  }

  // reply
  messenger->send_message(ack,
						  MSG_ADDR_OSD(from));

  delete m;
}

void OSD::handle_rg_peer_ack(MOSDRGPeerAck *m)
{
  int from = MSG_ADDR_NUM(m->get_source());
  dout(7) << "handle_rg_peer_ack from osd" << from << endl;

  // older map?
  if (m->get_version() < osdmap->get_version()) {
	dout(7) << "  from old map version " << m->get_version() << " < " << osdmap->get_version() << endl;
	delete m;   // discard and ignore.*
	return;
  }

  // newer map?
  if (m->get_version() > osdmap->get_version()) {
	dout(7) << "  for newer map version " << m->get_version() << " > " << osdmap->get_version() << endl;
	wait_for_new_map(m);
	return;
  }
  
  assert(m->get_version() == osdmap->get_version());

  // 
  //list<repgroup_t>                rg_dne;   // rg dne
  //map<repgroup_t, RGReplicaInfo > rg_state; // state, lists, etc.

  // rg_dne first
  for (list<repgroup_t>::iterator it = m->rg_dne.begin();
	   it != m->rg_dne.end();
	   it++) {
	dout(10) << " rg " << hex << *it << dec << " dne on osd" << from << endl;
	
	RG *rg = open_rg(*it);
	assert(rg);
	RGPeer *rgp = rg->get_peer(from);
	if (rgp) {
	  rg->remove_peer(from);
	} else {
	  dout(10) << "  weird, i didn't have it!" << endl;   // multiple lagged peer requests?
	}
  }

  // rg_state
  for (map<repgroup_t, RGReplicaInfo>::iterator it = m->rg_state.begin();
	   it != m->rg_state.end();
	   it++) {
	dout(10) << " rg " << hex << it->first << dec << " got state " << it->second.state 
			 << " " << it->second.objects.size() << " objects, " 
			 << it->second.deleted.size() << " deleted" << endl;

	RG *rg = open_rg(it->first);
	assert(rg);
	RGPeer *rgp = rg->get_peer(from);
	assert(rgp);

	rgp->peer_state = it->second;
	rgp->state_set(RG_PEER_STATE_ACTIVE);

	// fully peered?
	bool fully = true;
	for (map<int, RGPeer*>::iterator pit = rg->get_peers().begin();
		 pit != rg->get_peers().end();
		 pit++) {
	  if (!pit->second->is_active()) fully = false;
	}

	if (fully) {
	  dout(10) << " rg " << hex << it->first << dec << " fully peered, analyzing" << endl;
	  rg->mark_peered();
	  rg->analyze_peers(store);

	  do_recovery(rg);
	}	  
  }

  // done
  delete m;
}



// RECOVERY

void OSD::do_recovery(RG *rg)
{
  // pull?
  if (!rg->is_complete()) {
	rg_pull(rg, max_recovery_ops);
  } else {
	if (!rg->is_clean()) {
	  rg_push(rg, max_recovery_ops);
	  rg_clean(rg, max_recovery_ops);
	}
  }
}


// pull

void OSD::rg_pull(RG *rg, int maxops)
{
  int ops = rg->num_active_ops();

  dout(7) << "rg_pull rg " << hex << rg->get_rgid() << dec << " " << ops << "/" << maxops << " active ops" <<  endl;
  
  while (ops < maxops) {
	object_t oid;
	version_t v;
	int peer;
	if (!rg->pull_plan.get_next(oid, v, peer)) break;
	RGPeer *rgp = rg->get_proxy_peer(oid);
	if (rgp == 0) {
	  dout(7) << " apparently already pulled " << hex << oid << dec << endl;
	  continue;
	}
	if (rgp->is_pulling(oid)) {
	  dout(7) << " already pulling " << hex << oid << dec << endl;
	  continue;
	}
	pull_replica(oid, v, rgp);
	ops++;
  }  
}

void OSD::pull_replica(object_t oid, version_t v, RGPeer *p)
{
  dout(7) << "pull_replica " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

  // add to fetching list
  p->pull(oid, v);

  // send op
  __uint64_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
						  oid, p->rg->get_rgid(),
						  osdmap->get_version(),
						  OSD_OP_REP_PULL);
  op->set_version(v);
  op->set_rg_role(-1);  // whatever, not 0
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
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap); 
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
  dout(7) << "op_rep_pull_reply " << hex << op->get_oid() << dec << " size " << op->get_length() << endl;

  osd_lock.Lock();
  RGPeer *p = pull_ops[op->get_tid()];
  RG *rg = p->rg;
  assert(p);   // FIXME: how will this work?
  assert(p->is_pulling(op->get_oid()));
  assert(p->pulling_version(op->get_oid()) == op->get_version());
  osd_lock.Unlock();

  // write it and add it to the RG
  store->write(op->get_oid(), op->get_length(), 0, op->get_data().c_str());
  p->rg->add_object(store, op->get_oid());

  // close out pull op.
  osd_lock.Lock();
  pull_ops.erase(op->get_tid());
  rg->pulled(op->get_oid(), op->get_version(), p);

  // finish waiters
  if (waiting_for_object.count(op->get_oid())) 
	take_waiters(waiting_for_object[op->get_oid()]);

  // more?
  do_recovery(rg);

  osd_lock.Unlock();
  
  delete op;
}


// push

void OSD::rg_push(RG *rg, int maxops)
{
  int ops = rg->num_active_ops();

  dout(7) << "rg_push rg " << hex << rg->get_rgid() << dec << " " << ops << "/" << maxops << " active ops" <<  endl;
  
  while (ops < maxops) {
	object_t oid;
	version_t v;
	int peer;
	if (!rg->push_plan.get_next(oid, v, peer)) break;

	RGPeer *p = rg->get_peer(peer);
	assert(p);
	push_replica(oid, v, p);
	ops++;
  }  
  
}

void OSD::push_replica(object_t oid, version_t v, RGPeer *p)
{
  dout(7) << "push_replica " << hex << oid << dec << " v " << v << " to osd" << p->get_peer() << endl;

  // add to list
  p->push(oid, v);

  // send op
  __uint64_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
						  oid, p->rg->get_rgid(),
						  osdmap->get_version(),
						  OSD_OP_REP_PUSH);
  op->set_version(v);
  op->set_rg_role(-1);  // whatever, not 0

  // include object content
  struct stat st;
  store->stat(oid, &st);
  bufferptr b = new buffer(st.st_size);
  store->read(oid, st.st_size, 0, b.c_str());
  op->get_data().append(b);
  op->set_length(st.st_size);
  op->set_offset(0);

  messenger->send_message(op, MSG_ADDR_OSD(p->get_peer()));

  // register
  push_ops[tid] = p;
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
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap);
  messenger->send_message(reply, op->get_asker());
  
  unlock_object(op->get_oid());
  delete op;
}

void OSD::op_rep_push_reply(MOSDOpReply *op)
{
  dout(7) << "op_rep_push_reply " << hex << op->get_oid() << dec << endl;

  osd_lock.Lock();
  RGPeer *p = push_ops[op->get_tid()];
  RG *rg = p->rg;
  assert(p);   // FIXME: how will this work?
  assert(p->is_pushing(op->get_oid()));
  assert(p->pushing_version(op->get_oid()) == op->get_version());

  // close out push op.
  push_ops.erase(op->get_tid());
  rg->pushed(op->get_oid(), op->get_version(), p);

  // more?
  do_recovery(rg);

  osd_lock.Unlock();
  
  delete op;
}


// clean

void OSD::rg_clean(RG *rg, int maxops)
{
  int ops = rg->num_active_ops();

  dout(7) << "rg_clean rg " << hex << rg->get_rgid() << dec << " " << ops << "/" << maxops << " active ops" <<  endl;
  
  while (ops < maxops) {
	object_t oid;
	version_t v;
	int peer;
	if (!rg->clean_plan.get_next(oid, v, peer)) break;

	RGPeer *p = rg->get_peer(peer);
	assert(p);
	remove_replica(oid, v, p);
	ops++;
  }  
}

void OSD::remove_replica(object_t oid, version_t v, RGPeer *p)
{
  dout(7) << "remove_replica " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

  p->remove(oid, v);
  
  // send op
  __uint64_t tid = ++last_tid;
  MOSDOp *op = new MOSDOp(tid, messenger->get_myaddr(),
						  oid, p->rg->get_rgid(),
						  osdmap->get_version(),
						  OSD_OP_REP_REMOVE);
  op->set_version(v);
  op->set_rg_role(-1);  // whatever, not 0
  messenger->send_message(op, MSG_ADDR_OSD(p->get_peer()));
  
  // register
  remove_ops[tid] = p;
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
  int r = store->remove(op->get_oid());
  assert(r == 0);

  // reply
  messenger->send_message(new MOSDOpReply(op, r, osdmap), 
						  op->get_asker());

  unlock_object(op->get_oid());
  delete op;
}

void OSD::op_rep_remove_reply(MOSDOpReply *op)
{
  dout(7) << "op_rep_remove_reply " << hex << op->get_oid() << dec << endl;

  osd_lock.Lock();
  RGPeer *p = remove_ops[op->get_tid()];
  RG *rg = p->rg;
  assert(p);   // FIXME: how will this work?
  assert(p->is_removing(op->get_oid()));
  assert(p->removing_version(op->get_oid()) == op->get_version());

  // close out push op.
  remove_ops.erase(op->get_tid());
  rg->removed(op->get_oid(), op->get_version(), p);

  // more?
  do_recovery(rg);

  osd_lock.Unlock();
  
  delete op;
}




// =========================================================
// OPS


void OSD::handle_op(MOSDOp *op)
{
  // mkfs is special
  if (op->get_op() == OSD_OP_MKFS) {
	op_mkfs(op);
	return;
  }

  // no map?  starting up?
  if (!osdmap) {
    osd_lock.Lock();
	dout(7) << "no OSDMap, asking MDS" << endl;
	if (waiting_for_osdmap.empty()) 
	  messenger->send_message(new MGenericMessage(MSG_OSD_GETMAP), 
							  MSG_ADDR_MDS(0), MDS_PORT_MAIN);
	waiting_for_osdmap.push_back(op);
	osd_lock.Unlock();
	return;
  }
  
  // is our map version up to date?
  if (op->get_map_version() > osdmap->get_version()) {
	// op's is newer
	dout(7) << "op map " << op->get_map_version() << " > " << osdmap->get_version() << endl;
	wait_for_new_map(op);
	return;
  }

  // does user have old map?
  if (op->get_map_version() < osdmap->get_version()) {
	// op's is old
	dout(7) << "op map " << op->get_map_version() << " < " << osdmap->get_version() << endl;
	
	if (op->get_rg_role() != 0) {
	  dout(7) << " dropping rep op with old map" << endl;
	  delete op;
	  return;
	}
  }


  // did this op go to the right OSD?
  if (op->get_rg_role() == 0) {
    repgroup_t rgid = op->get_rg();
	int acting_primary = osdmap->get_rg_acting_primary( rgid );
	
	if (acting_primary != whoami) {
	  dout(7) << " acting primary is " << acting_primary << ", forwarding" << endl;
	  messenger->send_message(op, MSG_ADDR_OSD(acting_primary), 0);
	  logger->inc("fwd");
	  return;
	}

	// proxy?
	RG *rg = open_rg(rgid);
	if (!rg) {
	  // fail now?
	  dout(7) << "hit non-existent rg " << hex << op->get_rg() << dec << ", creating willy nilly for now" << endl;
	  rg = new_rg(rgid);  // for now.. FIXME
	}
	else {
	  if (!rg->is_complete()) {
		// consult RG object map
		RGPeer *rgp = rg->get_proxy_peer(op->get_oid());
		version_t v = rg->get_proxy_version(op->get_oid());

		if (op->get_op() == OSD_OP_WRITE && v == 0) {
		  // totally new object.
		}
		else if (rgp) {
		  // need to pull
		  dout(7) << "need to pull object " << hex << op->get_oid() << dec << endl;
		  RGPeer *rgp = rg->get_proxy_peer(op->get_oid());
		  if (!rgp->is_pulling(op->get_oid())) {
			pull_replica(op->get_oid(), v, rgp);
		  }
		  waiting_for_object[op->get_oid()].push_back(op);
		  return;
		}
	  }
	  
	}	
  }

  // queue op
  queue_op(op);
}

void OSD::queue_op(MOSDOp *op) {
  // inc pending count
  osd_lock.Lock();
  pending_ops++;
  osd_lock.Unlock();

  threadpool->put_op(op);
}
  
void OSD::do_op(MOSDOp *op) 
{
  logger->inc("op");

  // do the op
  switch (op->get_op()) {

  case OSD_OP_MKFS:
    op_mkfs(op);
    break;

  case OSD_OP_READ:
    op_read(op);
    break;
  case OSD_OP_WRITE:
    op_write(op);
    break;
  case OSD_OP_DELETE:
    op_delete(op);
    break;
  case OSD_OP_TRUNCATE:
    op_truncate(op);
    break;
  case OSD_OP_STAT:
    op_stat(op);
    break;

	// replication/recovery
  case OSD_OP_REP_PULL:
	op_rep_pull(op);
	break;
  case OSD_OP_REP_PUSH:
	op_rep_push(op);
	break;
  case OSD_OP_REP_REMOVE:
	op_rep_remove(op);
	break;
	
  default:
    assert(0);
  }

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

void OSD::op_read(MOSDOp *r)
{
  // read into a buffer
  bufferptr bptr = new buffer(r->get_length());   // prealloc space for entire read
  long got = store->read(r->get_oid(), 
						 r->get_length(), r->get_offset(),
						 bptr.c_str());
  // set up reply
  MOSDOpReply *reply = new MOSDOpReply(r, 0, osdmap); 
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
  
  dout(12) << "read got " << got << " / " << r->get_length() << " bytes from obj " << hex << r->get_oid() << dec << endl;

  logger->inc("rd");
  if (got >= 0) logger->inc("rdb", got);
  
  // send it
  messenger->send_message(reply, r->get_asker());

  delete r;
}


// -- osd_write

void OSD::op_write(MOSDOp *op)
{
  /* this is old
  // replicated write?
  Cond *cond = 0;
  if (op->get_rg_role() == 0) {
	// primary
	if (op->get_rg_nrep() > 1) {
	  dout(7) << "op_write nrep=" << op->get_rg_nrep() << endl;
	  int reps[op->get_rg_nrep()];
	  osdmap->repgroup_to_acting_osds(op->get_rg(),
									  reps);

	  replica_write_lock.Lock();
	  for (int i=1; i<op->get_rg_nrep(); i++) {
		// forward the write
		dout(7) << "  replica write to " << reps[i] << endl;

		__uint64_t tid = ++last_tid;
		MOSDOp *wr = new MOSDOp(tid,
								messenger->get_myaddr(),
								op->get_oid(),
								op->get_rg(),
								osdmap->get_version(),
								op->get_op());
		wr->get_data() = op->get_data();   // copy bufferlist
		messenger->send_message(wr, MSG_ADDR_OSD(reps[i]));

		replica_write_tids[op].insert(tid);
		replica_writes[tid] = op;
	  }

	  replica_write_cond[op] = cond = new Cond;
	  replica_write_lock.Unlock();
	}
  }
  */
  bool write_sync = op->get_rg_role() == 0;  // primary writes synchronously, replicas don't.

  
  // new object?
  bool existed = store->exists(op->get_oid());

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

  // update object metadata
  osd_lock.Lock();
  version_t v = 1;
  if (op->get_rg_role() == -1) {
	v = op->get_version();
	store->setattr(op->get_oid(), "version", &v, sizeof(v));
  } else if (existed) {
	// get + inc version
	store->getattr(op->get_oid(), "version", &v, sizeof(v));
	v++;
  } else {
	// add to RG collection
	RG *r = open_rg(op->get_rg());
	r->add_object(store, op->get_oid());
  }
  store->setattr(op->get_oid(), "version", &v, sizeof(v));
  osd_lock.Unlock();

  logger->inc("wr");
  logger->inc("wrb", op->get_length());

  // assume success.  FIXME.

  // wait for replicas?
  /* old old old
  if (cond) {
	replica_write_lock.Lock();
	while (!replica_write_tids[op].empty()) {
	  // wait
	  dout(7) << "op_write " << op << " waiting for " << replica_write_tids[op].size() << " replicas to write" << endl;
	  cond->Wait(replica_write_lock);
	}

	dout(7) << "op_write " << op << " all replicas finished, replying" << endl;
	
	replica_write_tids.erase(op);
	replica_write_cond.erase(op);
	replica_write_lock.Unlock();
  }
  */

  // reply
  MOSDOpReply *reply = new MOSDOpReply(op, 0, osdmap);
  messenger->send_message(reply, op->get_asker());

  delete op;
}

void OSD::op_mkfs(MOSDOp *op)
{
  dout(3) << "MKFS" << endl;
  {
    int r = store->mkfs();	
    messenger->send_message(new MOSDOpReply(op, r, osdmap), op->get_asker());
  }
  delete op;
}

void OSD::op_delete(MOSDOp *op)
{
  int r = store->remove(op->get_oid());
  dout(12) << "delete on " << hex << op->get_oid() << dec << " r = " << r << endl;
  
  // "ack"
  messenger->send_message(new MOSDOpReply(op, r, osdmap), op->get_asker());
  
  logger->inc("rm");
  delete op;
}

void OSD::op_truncate(MOSDOp *op)
{
  int r = store->truncate(op->get_oid(), op->get_offset());
  dout(3) << "truncate on " << hex << op->get_oid() << dec << " at " << op->get_offset() << " r = " << r << endl;
  
  // "ack"
  messenger->send_message(new MOSDOpReply(op, r, osdmap), op->get_asker());
  
  logger->inc("trunc");

  delete op;
}

void OSD::op_stat(MOSDOp *op)
{
  struct stat st;
  memset(&st, sizeof(st), 0);
  int r = store->stat(op->get_oid(), &st);
  
  dout(3) << "stat on " << hex << op->get_oid() << dec << " r = " << r << " size = " << st.st_size << endl;
	  
  MOSDOpReply *reply = new MOSDOpReply(op, r, osdmap);
  reply->set_object_size(st.st_size);
  messenger->send_message(reply, op->get_asker());
	  
  logger->inc("stat");
  delete op;
}

void doop(OSD *u, MOSDOp *p) {
  u->do_op(p);
}
