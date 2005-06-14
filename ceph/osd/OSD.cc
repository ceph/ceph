
#include "include/types.h"

#include "OSD.h"
#include "FakeStore.h"
#include "OSDCluster.h"

#include "mds/MDS.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "msg/HostMonitor.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDGetClusterAck.h"

#include "common/Logger.h"
#include "common/LogType.h"

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>


#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << " "

char *osd_base_path = "./osddata";



// cons/des

LogType osd_logtype;

OSD::OSD(int id, Messenger *m) 
{
  whoami = id;

  messenger = m;
  messenger->set_dispatcher(this);

  osdcluster = 0;

  // use fake store
  store = new FakeStore(osd_base_path, whoami);

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

}

OSD::~OSD()
{
  if (osdcluster) { delete osdcluster; osdcluster = 0; }
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
  monitor->shutdown();
  messenger->shutdown();
  int r = store->finalize();
  return r;
}



// dispatch

void OSD::dispatch(Message *m) 
{
  osd_lock.Lock();

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

  case MSG_OSD_GETCLUSTERACK:
	handle_getcluster_ack((MOSDGetClusterAck*)m);
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

  default:
	dout(1) << " got unknown message " << m->get_type() << endl;
  }

  osd_lock.Unlock();
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


void OSD::handle_getcluster_ack(MOSDGetClusterAck *m)
{
  if (!osdcluster) osdcluster = new OSDCluster();
  osdcluster->decode(m->get_osdcluster());
  dout(7) << "got OSDCluster version " << osdcluster->get_version() << endl;
  delete m;

  // process waiters
  list<MOSDOp*> waiting;
  waiting.splice(waiting.begin(), waiting_for_osdcluster);

  for (list<MOSDOp*>::iterator it = waiting.begin();
	   it != waiting.end();
	   it++) {
	handle_op(*it);
  }

}

void OSD::handle_op(MOSDOp *op)
{
  // starting up?
  if (!osdcluster) {
	dout(7) << "no OSDCluster, starting up" << endl;
	if (waiting_for_osdcluster.empty()) 
	  messenger->send_message(new MGenericMessage(MSG_OSD_GETCLUSTER), 
							  MSG_ADDR_MDS(0), MDS_PORT_MAIN);
	waiting_for_osdcluster.push_back(op);
	return;
  }

  // check cluster version
  if (op->get_ocv() > osdcluster->get_version()) {
	// op's is newer
	dout(7) << "op cluster " << op->get_ocv() << " > " << osdcluster->get_version() << endl;
	
	// query MDS
	dout(7) << "querying MDS" << endl;
	messenger->send_message(new MGenericMessage(MSG_OSD_GETCLUSTER), 
							MSG_ADDR_MDS(0), MDS_PORT_MAIN);
	assert(0);
	waiting_for_osdcluster.push_back(op);
	return;
  }

  if (op->get_ocv() < osdcluster->get_version()) {
	// op's is old
	dout(7) << "op cluster " << op->get_ocv() << " > " << osdcluster->get_version() << endl;

	// verify that we are primary, or acting primary
	int acting_primary = osdcluster->get_rg_acting_primary( op->get_rg() );
	if (acting_primary != whoami) {
	  dout(7) << " acting primary is " << acting_primary << ", forwarding" << endl;
	  messenger->send_message(op, MSG_ADDR_OSD(acting_primary), 0);
	  logger->inc("fwd");
	  return;
	}
  }

  
  // do the op
  switch (op->get_op()) {

  case OSD_OP_READ:
	op_read(op);
	break;

  case OSD_OP_WRITE:
	op_write(op);
	break;

  case OSD_OP_MKFS:
	dout(3) << "MKFS" << endl;
	{
	  int r = store->mkfs();	
	  messenger->send_message(new MOSDOpReply(op, r, osdcluster), 
							  op->get_asker());
	}
	delete op;
	break;

  case OSD_OP_DELETE:
	{
	  int r = store->remove(op->get_oid());
	  dout(3) << "delete on " << op->get_oid() << " r = " << r << endl;
	  
	  // "ack"
	  messenger->send_message(new MOSDOpReply(op, r, osdcluster), 
							  op->get_asker());

	  logger->inc("rm");
	}
	delete op;
	break;

  case OSD_OP_TRUNCATE:
	{
	  int r = store->truncate(op->get_oid(), op->get_offset());
	  dout(3) << "truncate on " << op->get_oid() << " at " << op->get_offset() << " r = " << r << endl;
	  
	  // "ack"
	  messenger->send_message(new MOSDOpReply(op, r, osdcluster), 
							  op->get_asker());
	  logger->inc("trunc");
	}
	delete op;
	break;

  case OSD_OP_STAT:
	{
	  struct stat st;
	  memset(&st, sizeof(st), 0);
	  int r = store->stat(op->get_oid(), &st);
  
	  dout(3) << "stat on " << op->get_oid() << " r = " << r << " size = " << st.st_size << endl;
	  
	  MOSDOpReply *reply = new MOSDOpReply(op, r, osdcluster);
	  reply->set_object_size(st.st_size);
	  messenger->send_message(reply, op->get_asker());
	  
	  logger->inc("stat");
	}
	delete op;
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
  MOSDOpReply *reply = new MOSDOpReply(r, 0, osdcluster); 
  if (got >= 0) {
	bptr.set_length(got);   // properly size the buffer

	// give it to the reply in a bufferlist
	reply->get_data().push_back( bptr );
	
	reply->set_result(0);
	reply->set_data(bl);
	reply->set_length(got);
  } else {
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

void OSD::op_write(MOSDOp *m)
{
  // take buffers from the message
  bufferlist bl;
  bl.claim( m->get_data() );
  
  // write out buffers
  off_t off = m->get_offset();
  for (list<bufferptr>::iterator it = bl.buffers().begin();
	   it != bl.buffers().end();
	   it++) {

	int r = store->write(m->get_oid(),
						 (*it).length(), off,
						 (*it).c_str(),
						 g_conf.osd_fsync);
	off += (*it).length();
	if (r < 0) {
	  dout(1) << "write error on " << m->get_oid() << " r = " << r << endl;
	  assert(r >= 0);
	}
  }

  // trucnate after?
  /*
  if (m->get_flags() & OSD_OP_FLAG_TRUNCATE) {
	size_t at = m->get_offset() + m->get_length();
	int r = store->truncate(m->get_oid(), at);
	dout(7) << "truncating object after tail of write at " << at << ", r = " << r << endl;
  }
  */

  logger->inc("wr");
  logger->inc("wrb", m->get_length());

  
  // assume success.  FIXME.

  // reply
  MOSDOpReply *reply = new MOSDOpReply(m, 0, osdcluster);
  messenger->send_message(reply, m->get_asker());

  delete m;
}

