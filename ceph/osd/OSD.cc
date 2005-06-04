
#include "include/types.h"

#include "OSD.h"

#include "FakeStore.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MPing.h"
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
}

OSD::~OSD()
{
  if (messenger) { delete messenger; messenger = 0; }
}

int OSD::init()
{
  osd_lock.Lock();
  int r = store->init();
  osd_lock.Unlock();
  return r;
}

int OSD::shutdown()
{
  messenger->shutdown();
  return store->finalize();
}



// dispatch

void OSD::dispatch(Message *m) 
{
  osd_lock.Lock();

  switch (m->get_type()) {
  
  case MSG_SHUTDOWN:
	shutdown();
	break;

  case MSG_PING:
	handle_ping((MPing*)m);
	break;
	
  case MSG_OSD_READ:
	read((MOSDRead*)m);
	break;

  case MSG_OSD_WRITE:
	write((MOSDWrite*)m);
	break;

  case MSG_OSD_OP:
	handle_op((MOSDOp*)m);
	break;

  default:
	dout(1) << " got unknown message " << m->get_type() << endl;
  }

  delete m;

  osd_lock.Unlock();
}


void OSD::handle_ping(MPing *m)
{
  // play dead?
  if (whoami == 3) {
	dout(7) << "got ping, replying" << endl;
	messenger->send_message(new MPing(0),
							m->get_source(), m->get_source_port(), 0);
  } else {
	dout(7) << "playing dead" << endl;
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
}




void OSD::read(MOSDRead *r)
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
}


// -- osd_write

void OSD::write(MOSDWrite *m)
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
	assert(r >= 0);
  }
  
  // assume success.  FIXME.

  // reply
  MOSDWriteReply *reply = new MOSDWriteReply(m, 0);
  messenger->send_message(reply, m->get_source(), m->get_source_port());
}

