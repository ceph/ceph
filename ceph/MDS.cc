
#include "include/types.h"
#include "include/MDS.h"
#include "include/MDCache.h"
#include "include/Messenger.h"
#include "include/MDStore.h"
#include "include/MDLog.h"

#include "messages/MPing.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"

#include <list>

#include <iostream>
using namespace std;

// extern 
//MDS *g_mds;


// cons/des
MDS::MDS(int id, int num, Messenger *m) {
  whoami = id;
  num_nodes = num;
  
  mdcache = new DentryCache();
  mdstore = new MDStore(this);
  messenger = m;
  mdlog = new MDLog(this);
}
MDS::~MDS() {
  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdstore) { delete mdstore; mdstore = NULL; }
  if (messenger) { delete messenger; messenger = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
}


int MDS::init()
{
  // init messenger
  messenger->init(this);
}

int MDS::shutdown()
{
  // shut down cache
  mdcache->clear();
  
  // shut down messenger
  messenger->shutdown();

  return 0;
}

void MDS::proc_message(Message *m) 
{

  switch (m->get_type()) {
	// MISC
  case MSG_PING:
	cout << "mds" << whoami << " received ping from " << m->get_source() << " with ttl " << ((MPing*)m)->ttl << endl;
	if (((MPing*)m)->ttl > 0) {
	  //cout << "mds" << whoami << " responding to " << m->get_source() << endl;
	  messenger->send_message(new MPing(((MPing*)m)->ttl-1), 
							  m->get_source(), m->get_source_port(),
							  MDS_PORT_MAIN);
	}
	break;


	// CLIENTS
  case MSG_CLIENT_REQUEST:
	handle_client_request((MClientRequest*)m);
	break;

	// OSD I/O
  case MSG_OSD_READREPLY:
	cout << "mds" << whoami << " read reply!" << endl;
	osd_read_finish(m);
	break;

  case MSG_OSD_WRITEREPLY:
	cout << "mds" << whoami << " write reply!" << endl;
	osd_write_finish(m);
	break;
	
  default:
	cout << "mds" << whoami << " unknown message " << m->get_type() << endl;
  }

}


void MDS::dispatch(Message *m)
{
  switch (m->get_dest_port()) {
	
  case MDS_PORT_STORE:
	mdstore->proc_message(m);
	break;
	
	/*
  case MSG_SUBSYS_MDLOG:
	mymds->logger->proc_message(m);
	break;
	
  case MSG_SUBSYS_BALANCER:
	mymds->balancer->proc_message(m);
	break;
	*/

  case MDS_PORT_MAIN:
  case MDS_PORT_SERVER:
	proc_message(m);
	break;

  default:
	cout << "MDS unkown message port" << m->get_dest_port() << endl;
  }
}





// Client fun

int MDS::handle_client_request(MClientRequest *req)
{
  cout << "mds" << whoami << " got client request from " << req->get_source() << ", op " << req->op << endl;

  
  
}


















// OSD fun

int MDS::osd_read(int osd, object_t oid, size_t len, size_t offset, char **bufptr, size_t *read, Context *c)
{
  osd_last_tid++;
  MOSDRead *m = new MOSDRead(osd_last_tid,
							 oid,
							 len, offset);
  
  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->bufptr = bufptr;
  p->buf = 0;
  p->bytesread = read;
  p->context = c;
  osd_reads[osd_last_tid] = p;

  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_read(int osd, object_t oid, size_t len, size_t offset, char *buf, size_t *bytesread, Context *c)
{
  osd_last_tid++;
  MOSDRead *m = new MOSDRead(osd_last_tid,
							 oid,
							 len, offset);

  PendingOSDRead_t *p = new PendingOSDRead_t;
  p->buf = buf;
  p->bytesread = bytesread;
  p->context = c;
  osd_reads[osd_last_tid] = p;

  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_read_finish(Message *rawm) 
{
  MOSDReadReply *m = (MOSDReadReply*)rawm;
  
  // get pio
  PendingOSDRead_t *p = osd_reads[ m->tid ];
  osd_reads.erase( m->tid );
  Context *c = p->context;

  if (m->len >= 0) {
	// success!  
	*p->bytesread = m->len;

	if (p->buf) { // user buffer
	  memcpy(p->buf, m->buf, m->len);  // copy
	  delete m->buf;                   // free message buf
	} else {      // new buffer
	  *p->bufptr = m->buf;     // steal message's buffer
	}
	m->buf = 0;
  }

  delete p;

  long result = m->len;
  delete m;

  if (c) {
	c->finish(result);
	delete c;
  }
}



// -- osd_write

int MDS::osd_write(int osd, object_t oid, size_t len, size_t offset, char *buf, int flags, Context *c)
{
  osd_last_tid++;

  char *nbuf = new char[len];
  memcpy(nbuf, buf, len);

  MOSDWrite *m = new MOSDWrite(osd_last_tid,
							   oid,
							   len, offset,
							   nbuf, flags);
  osd_writes[ osd_last_tid ] = c;
  cout << "mds: sending MOSDWrite " << m->get_type() << endl;
  messenger->send_message(m,
						  MSG_ADDR_OSD(osd),
						  0, MDS_PORT_MAIN);
}


int MDS::osd_write_finish(Message *rawm)
{
  MOSDWriteReply *m = (MOSDWriteReply *)rawm;

  Context *c = osd_writes[ m->tid ];
  osd_writes.erase(m->tid);

  long result = m->len;
  delete m;

  cout << "mds" << whoami << " finishing osd_write" << endl;

  if (c) {
	c->finish(result);
	delete c;
  }
}



// ---------------------------
// open_root

class OpenRootContext : public Context {
protected:
  Context *c;
  MDS *mds;
public:
  OpenRootContext(MDS *m, Context *c) {
	mds = m;
	this->c = c;
  }
  void finish(int result) {
	mds->open_root_2(result, c);
  }
};

bool MDS::open_root(Context *c)
{
  // open root inode
  if (whoami == 0) { 
	// i am root
	CInode *root = new CInode();
	root->inode.ino = 1;

	// make it up (FIXME)
	root->inode.mode = 0755;
	root->inode.size = 0;

	mdcache->set_root( root );

	if (c) {
	  c->finish(0);
	  delete c;
	}
  } else {
	// request inode from root mds
	

  }
}

bool MDS::open_root_2(int result, Context *c)
{
  c->finish(0);
  delete c;
}
