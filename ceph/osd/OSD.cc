
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
#include "messages/MOSDGetMapAck.h"

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
  messenger->set_dispatcher(this);

  osdmap = 0;

  last_tid = 0;

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



// ------------------------------------
// replica groups

void OSD::get_rg_list(list<repgroup_t>& ls)
{
  // just list collections; assume they're all rg's (for now)
  store->list_collections(ls);
}


bool OSD::rg_exists(repgroup_t rg) 
{
  struct stat st;
  if (store->collection_stat(rg, &st) == 0) 
	return true;
  else
	return false;
}


RG *OSD::open_rg(repgroup_t rg)
{
  // already open?
  if (rg_map.count(rg)) 
	return rg_map[rg];

  // stat collection
  RG *r = new RG(rg);
  if (rg_exists(rg)) {
	// exists
	r->fetch(store);
  } else {
	// dne
	r->store(store);
  }
  rg_map[rg] = r;

  return r;
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
  
	
	// osd
  case MSG_SHUTDOWN:
	shutdown();
	delete m;
	break;

  case MSG_OSD_GETMAPACK:
	handle_getmap_ack((MOSDGetMapAck*)m);
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
}


void OSD::handle_op_reply(MOSDOpReply *m)
{
  replica_write_lock.Lock();
  MOSDOp *op = replica_writes[m->get_tid()];
  dout(7) << "got replica write ack tid " << m->get_tid() << " orig op " << op << endl;

  replica_write_tids[op].erase(m->get_tid());
  if (replica_write_tids[op].empty())
	replica_write_cond[op]->Signal();

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



void OSD::update_map(bufferlist& state)
{
  // decode new map
  if (!osdmap) osdmap = new OSDMap();
  osdmap->decode(state);
  dout(7) << "update_map version " << osdmap->get_version() << endl;

  // scan replica groups
  list<repgroup_t> ls;
  get_rg_list(ls);
  
  map< int, list<RG*> > primary_ping_queue;

  for (list<repgroup_t>::iterator it = ls.begin();
	   it != ls.end();
	   it++) {
	repgroup_t rgid = *it;
	RG *rg = open_rg(rgid);
	assert(rg);

	// get active rush mapping
	int acting[NUM_RUSH_REPLICAS];
	int nrep = osdmap->repgroup_to_acting_osds(rgid, acting, NUM_RUSH_REPLICAS);
	int primary = acting[0];
	int role = -1;
	for (int i=0; i<nrep; i++) 
	  if (acting[i] == whoami) role = i;
	

	if (role != rg->get_role()) {
	  // role change.
	  dout(7) << " rg " << rgid << " acting role change " << rg->get_role() << " -> " << role << endl; 
	  
	  // am i old-primary?
	  if (rg->get_role() == 0) {
		// note potential replica set, and drop old peering sessions.
		for (map<int, RGPeer*>::iterator it = rg->get_peers().begin();
			 it != rg->get_peers().end();
			 it++) {
		  dout(7) << " rg " << rgid << " old-primary, dropping old peer " << it->first << endl;
		  rg->get_old_replica_set().insert(it->first);
		  delete it->second;
		}
		rg->get_peers().clear();
	  }

	  // we need to re-peer
	  rg->state_clear(RG_STATE_PEERED);
	  rg->set_role(role);
	  rg->store(store);
	  primary_ping_queue[primary].push_back(rg);
	  
	} else {
	  // no role change.

	  if (role > 0) {  
		// i am replica.
		
		// did primary change?
		if (primary != rg->get_primary()) {
		  dout(7) << " rg " << rgid << " acting primary change " << rg->get_primary() << " -> " << primary << endl;
		  
		  // re-peer
		  rg->state_clear(RG_STATE_PEERED);
		  rg->set_primary(primary);
		  rg->store(store);
		  primary_ping_queue[primary].push_back(rg);
		}
	  }
	  else if (role == 0) {
		// i am primary.

		// check replicas
		for (int r=1; r<nrep; r++) {
		  if (rg->get_peer(r) == 0) {
			dout(7) << " rg " << rgid << " primary not peered with replica " << r << " osd" << acting[r] << endl;
			
			// ***
		  } 
		}

	  }
	}
  }
  

  // initiate any new peering sessions!
  for (map< int, list<RG*> >::iterator pit = primary_ping_queue.begin();
	   pit != primary_ping_queue.end();
	   pit++) {
	// create peer message
	int primary = pit->first;
	

	for (list<RG*>::iterator rit = pit->second.begin();
		 rit != pit->second.end();
		 rit++) {
	  // add this RG to peer message
	}

	// send
	
  }

}


void OSD::handle_getmap_ack(MOSDGetMapAck *m)
{
  // SAB
  osd_lock.Lock();

  update_map(m->get_osdmap());
  delete m;

  // process waiters
  list<MOSDOp*> waiting;
  waiting.splice(waiting.begin(), waiting_for_osdmap);

  list<MOSDOp*> w = waiting;

  osd_lock.Unlock();

  for (list<MOSDOp*>::iterator it = w.begin();
	   it != w.end();
	   it++) {
	handle_op(*it);
  }
}

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
	dout(7) << "no OSDMap, starting up" << endl;
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
	
	// query MDS
	dout(7) << "querying MDS" << endl;
	messenger->send_message(new MGenericMessage(MSG_OSD_GETMAP), 
							MSG_ADDR_MDS(0), MDS_PORT_MAIN);

	assert(0);

	osd_lock.Lock();
	waiting_for_osdmap.push_back(op);
	osd_lock.Unlock();
	return;
  }

  // does user have old map?
  if (op->get_map_version() < osdmap->get_version()) {
	// op's is old
	dout(7) << "op map " << op->get_map_version() << " < " << osdmap->get_version() << endl;
  }


  // did this op go to the right OSD?
  if (op->get_rg_role() == 0) {
    repgroup_t rg = op->get_rg();
	int acting_primary = osdmap->get_rg_acting_primary( rg );
	
	if (acting_primary != whoami) {
	  dout(7) << " acting primary is " << acting_primary << ", forwarding" << endl;
	  messenger->send_message(op, MSG_ADDR_OSD(acting_primary), 0);
	  logger->inc("fwd");
	  return;
	}
  }

  // queue op
  queue_op(op);
}

void OSD::queue_op(MOSDOp *op) {
  threadpool->put_op(op);
}
  
void OSD::do_op(MOSDOp *op) 
{
  logger->inc("op");

  // do the op
  switch (op->get_op()) {

  case OSD_OP_READ:
    op_read(op);
    break;

  case OSD_OP_WRITE:
    op_write(op);
    break;

  case OSD_OP_MKFS:
    op_mkfs(op);
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
	
  default:
    assert(0);
  }
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
  
  dout(10) << "read got " << got << " / " << r->get_length() << " bytes from " << r->get_oid() << endl;

  logger->inc("rd");
  if (got >= 0) logger->inc("rdb", got);
  
  // send it
  messenger->send_message(reply, r->get_asker());

  delete r;
}


// -- osd_write

void OSD::op_write(MOSDOp *op)
{

  // replicated write?
  Cond *cond = 0;
  if (op->get_rg_role() == 0) {
	// primary
	if (op->get_rg_nrep() > 1) {
	  dout(7) << "op_write nrep=" << op->get_rg_nrep() << endl;
	  int reps[op->get_rg_nrep()];
	  osdmap->repgroup_to_osds(op->get_rg(),
								   reps,
								   op->get_rg_nrep());

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
	  dout(1) << "write error on " << op->get_oid() << " len " << (*it).length() << "  off " << off << "  r = " << r << endl;
	  assert(r >= 0);
	}
  }

  // update object metadata
  if (!existed) {
	// add to RG collection
	RG *r = open_rg(op->get_rg());
	r->add_object(store, op->get_oid());
  }

  logger->inc("wr");
  logger->inc("wrb", op->get_length());

  // assume success.  FIXME.

  // wait for replicas?
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
  dout(3) << "delete on " << op->get_oid() << " r = " << r << endl;
  
  // "ack"
  messenger->send_message(new MOSDOpReply(op, r, osdmap), op->get_asker());
  
  logger->inc("rm");
  delete op;
}

void OSD::op_truncate(MOSDOp *op)
{
  int r = store->truncate(op->get_oid(), op->get_offset());
  dout(3) << "truncate on " << op->get_oid() << " at " << op->get_offset() << " r = " << r << endl;
  
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
  
  dout(3) << "stat on " << op->get_oid() << " r = " << r << " size = " << st.st_size << endl;
	  
  MOSDOpReply *reply = new MOSDOpReply(op, r, osdmap);
  reply->set_object_size(st.st_size);
  messenger->send_message(reply, op->get_asker());
	  
  logger->inc("stat");
  delete op;
}

void doop(OSD *u, MOSDOp *p) {
  u->do_op(p);
}
