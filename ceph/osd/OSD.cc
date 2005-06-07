
#include "include/types.h"

#include "OSD.h"
#include "FakeStore.h"

#include "mds/MDS.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "msg/HostMonitor.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MOSDRead.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

#include <iostream>
#include <cassert>
#include <errno.h>
#include <sys/stat.h>


#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "osd" << whoami << " "

char *osd_base_path = "./osddata";



// cons/des

OSD::OSD(int id, Messenger *m) 
{
  whoami = id;

  messenger = m;
  messenger->set_dispatcher(this);

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
}

OSD::~OSD()
{
  if (messenger) { delete messenger; messenger = 0; }
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
	break;

  case MSG_PING:
	// take note.
	monitor->host_is_alive(m->get_source());

	handle_ping((MPing*)m);
	break;
	
  case MSG_OSD_READ:
	handle_read((MOSDRead*)m);
	break;

  case MSG_OSD_WRITE:
	handle_write((MOSDWrite*)m);
	break;

  case MSG_OSD_OP:
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




void OSD::handle_op(MOSDOp *op)
{
  switch (op->get_op()) {
  case OSD_OP_MKFS:
	dout(3) << "MKFS" << endl;
	{
	  int r = store->mkfs();	
	  messenger->send_message(new MOSDOpReply(op, r), 
							  op->get_source(), op->get_source_port());
	}
	break;

  case OSD_OP_DELETE:
	{
	  int r = store->remove(op->get_oid());
	  dout(3) << "delete on " << op->get_oid() << " r = " << r << endl;
	  
	  // "ack"
	  messenger->send_message(new MOSDOpReply(op, r), 
							  op->get_source(), op->get_source_port());
	}
	break;

  case OSD_OP_STAT:
	{
	  struct stat st;
	  memset(&st, sizeof(st), 0);
	  int r = store->stat(op->get_oid(), &st);
  
	  dout(3) << "stat on " << op->get_oid() << " r = " << r << " size = " << st.st_size << endl;
	  
	  MOSDOpReply *reply = new MOSDOpReply(op, r);
	  reply->set_size(st.st_size);
	  messenger->send_message(reply,
							  op->get_source(), op->get_source_port());
	}
	break;
	
  default:
	assert(0);
  }

  delete op;
}




void OSD::handle_read(MOSDRead *r)
{
  // read into a buffer
  bufferptr bptr = new buffer(r->get_len());   // prealloc space for entire read
  long got = store->read(r->get_oid(), 
						 r->get_len(), r->get_offset(),
						 bptr.c_str());
  MOSDReadReply *reply = new MOSDReadReply(r, 0); 
  if (got >= 0) {
	bptr.set_length(got);     // properly size buffer
	
	// give it to the reply in a bufferlist
	bufferlist bl;
	bl.push_back( bptr );
	reply->set_data(bl);
  } else {
	reply->set_result(got);   // error
  }
  
  dout(10) << "read got " << got << " / " << r->get_len() << " bytes from " << r->get_oid() << endl;
  
  // send it
  messenger->send_message(reply, r->get_source(), r->get_source_port());

  delete r;
}


// -- osd_write

void OSD::handle_write(MOSDWrite *m)
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
						 (*it).c_str());
	off += (*it).length();
	if (r < 0) {
	  dout(1) << "write error on " << m->get_oid() << " r = " << r << endl;
	  assert(r >= 0);
	}
  }
  
  // assume success.  FIXME.

  // reply
  MOSDWriteReply *reply = new MOSDWriteReply(m, 0);
  messenger->send_message(reply, m->get_source(), m->get_source_port());

  delete m;
}

