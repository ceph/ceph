
#include "include/types.h"
#include "include/Messenger.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDStore.h"
#include "MDLog.h"
#include "MDCluster.h"

#include "messages/MPing.h"

#include "messages/MOSDRead.h"
#include "messages/MOSDWrite.h"
#include "messages/MOSDReadReply.h"
#include "messages/MOSDWriteReply.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MDiscover.h"

#include <list>

#include <iostream>
using namespace std;


#define TRAVERSE_FORWARD  1
#define TRAVERSE_DISCOVER 2
#define TRAVERSE_FAIL     3

// extern 
//MDS *g_mds;


// cons/des
MDS::MDS(MDCluster *mdc, Messenger *m) {
  whoami = mdc->add_mds(this);
  mdcluster = mdc;

  messenger = m;

  mdcache = new DentryCache();
  mdstore = new MDStore(this);
  mdlog = new MDLog(this);

  opening_root = false;
  osd_last_tid = 0;
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



class C_MDS_RetryMessage : public Context {
  Message *m;
  MDS *mds;
public:
  C_MDS_RetryMessage(MDS *mds, Message *m) {
	this->m = m;
	this->mds = mds;
  }
  virtual void finish(int r) {
	mds->proc_message(m);
  }
};


void MDS::proc_message(Message *m) 
{

  switch (m->get_type()) {
	// MISC
  case MSG_PING:
	cout << "mds" << whoami << " received ping from " << MSG_ADDR_NICE(m->get_source()) << " with ttl " << ((MPing*)m)->ttl << endl;
	if (((MPing*)m)->ttl > 0) {
	  //cout << "mds" << whoami << " responding to " << m->get_source() << endl;
	  messenger->send_message(new MPing(((MPing*)m)->ttl-1), 
							  m->get_source(), m->get_source_port(),
							  MDS_PORT_MAIN);
	}
	break;


	// MDS
  case MSG_MDS_DISCOVER:
	handle_discover((MDiscover*)m);
	break;


	// CLIENTS
  case MSG_CLIENT_REQUEST:
	handle_client_request((MClientRequest*)m);
	break;


	// OSD I/O
  case MSG_OSD_READREPLY:
	cout << "mds" << whoami << " osd_read reply" << endl;
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




int MDS::handle_discover(MDiscover *dis) 
{
  if (dis->asker == whoami) {
	// this is a result
	
	if (dis->want->size() == 0) {
	  cout << "got root" << endl;

	  CInode *root = new CInode();
	  root->inode = dis->trace[0].inode;
	  root->dir_dist = dis->trace[0].dir_dist;
	  root->dir_rep = dis->trace[0].dir_rep;

	  mdcache->set_root( root );
	  return 0;
	}
	
	// traverse to start point
	vector<CInode*> trav;
	vector<string>  trav_dn;

	int r = path_traverse(dis->basepath, trav, trav_dn, NULL, TRAVERSE_FAIL);
	if (r != 0) throw "wtf";
	
	CInode *cur = trav[trav.size()-1];
	CInode *start = cur;

	cur->put(); // unpin

	// add duplicated inodes
	for (int i=0; i<dis->trace.size(); i++) {
	  CInode *in = new CInode();
	  in->inode = dis->trace[i].inode;
	  in->dir_dist = dis->trace[i].dir_dist;
	  in->dir_rep = dis->trace[i].dir_rep;
	  
	  mdcache->add_inode( in );
	  mdcache->link_inode( cur, (*dis->want)[i], in );

	  cur = in;
	}

	// finish off waiting items
	list<Context*> finished;
	finished.splice(finished.begin(),
					start->waiting_for_discover[ (*dis->want)[0] ]);
	list<Context*>::iterator it;
	for (it = finished.begin(); it != finished.end(); it++) {
	  Context *c = *it;
	  c->finish(0);
	  delete c;				
	}	

  } else {
	// this is a request

	while (!dis->done()) {
	  // go for the next bit
	  string path = dis->current_need();

	  cout << "mds" << whoami << " dis " << path << " for " << dis->asker << endl;
	
	  // traverse to start point
	  vector<CInode*> trav;
	  vector<string>  trav_dn;

	  int r = path_traverse(dis->basepath, trav, trav_dn, dis, TRAVERSE_FORWARD);
	  if (r > 0) return 0;
	  
	  // add it
	  CInode *got = trav[trav.size()-1];
	  MDiscoverRec_t bit;
	  bit.inode = got->inode;
	  bit.dir_dist = got->dir_dist;
	  bit.dir_rep = got->dir_rep;
	  dis->add_bit(bit);
	}

	// send result!
	cout << "mds" << whoami << " finished discovery, sending back to " << dis->asker << endl;
	messenger->send_message(dis,
							MSG_ADDR_CLIENT(dis->asker), MDS_PORT_SERVER,
							MDS_PORT_SERVER);
  }

}






int MDS::handle_client_request(MClientRequest *req)
{
  cout << "mds" << whoami << " req " << MSG_ADDR_NICE(req->get_source()) << '.' << req->tid << " op " << req->op << " on " << req->path <<  endl;

  
  vector<CInode*> trace;
  vector<string>  trace_dn;

  int r = path_traverse(req->path, trace, trace_dn, req, TRAVERSE_FORWARD);
  if (r > 0) return 0;  // delayed

  CInode *cur = trace[trace.size()-1];
  
  // need contents too?
  vector<c_inode_info*> dir_contents;
 
  if (req->op == MDS_OP_READDIR) {
	if (cur->is_dir()) {
	  if (cur->dir) {
		// build dir contents
		CDir_map_t::iterator it;
		for (it = cur->dir->begin(); it != cur->dir->end(); it++) {
		  CInode *in = it->second->inode;
		  c_inode_info *i = new c_inode_info;
		  i->inode = in->inode;
		  i->dist = in->get_dist_spec(this);
		  i->ref_dn = it->first;
		  dir_contents.push_back(i);
		}
	  } else {
		// fetch
		cout << "mds" << whoami << " no dir contents for readdir on " << cur->inode.ino << ", fetching" << endl;
		mdstore->fetch_dir(cur, new C_MDS_RetryMessage(this, req));
		return 0;
	  }
	} else {
	  cout << "readdir on non-dir" << endl;
	}

  }

  // send reply
  cout << "mds" << whoami << " reply to client" << req->client << '.' << req->tid << " result " << r << endl;
  MClientReply *reply = new MClientReply(req);
  reply->result = r;
  reply->set_trace_dist(trace, 
						trace_dn,
						this);
  
  if (dir_contents.size())
	reply->dir_contents = dir_contents;
  
  messenger->send_message(reply,
						  MSG_ADDR_CLIENT(req->client), 0,
						  MDS_PORT_SERVER);
}


void split_path(string& path, 
				vector<string>& bits)
{
  int off = 0;
  while (off < path.length()) {
	// skip trailing/duplicate slash(es)
	int nextslash = path.find('/', off);
	if (nextslash == off) {
	  off++;
	  continue;
	}
	if (nextslash < 0) 
	  nextslash = path.length();  // no more slashes

	bits.push_back( path.substr(off,nextslash-off) );
	off = nextslash+1;
  }
}



int MDS::path_traverse(string& path, 
					   vector<CInode*>& trace, 
					   vector<string>& trace_dn, 
					   Message *req,
					   int onfail)
{
  // break path into bits.
  trace.clear();
  CInode *cur = mdcache->get_root();
  trace.push_back(cur);

  string have_clean;

  vector<string> path_bits;
  split_path(path, path_bits);

  for (int depth = 0; depth < path_bits.size(); depth++) {
	string dname = path_bits[depth];
	cout << " path seg " << dname << endl;

	// lookup dentry
	if (cur->is_dir()) {
	  if (!cur->dir)
		cur->dir = new CDir(cur);

	  CDentry *dn = cur->dir->lookup(dname);
	  if (dn && dn->inode) {
		// have it, keep going.
		cur = dn->inode;
		have_clean += "/";
		have_clean += dname;
	  } else {
		// don't have it.
		int dauth = cur->dir->dentry_authority( dname, mdcluster );

		if (dauth == whoami) {
		  // mine.
		  if (cur->dir->is_complete()) {
			// file not found
			return -ENOENT;
		  } else {
			// directory isn't complete; reload
			cout << "mds" << whoami << " incomplete dir contents for " << cur->inode.ino << ", fetching" << endl;
			mdstore->fetch_dir(cur, new C_MDS_RetryMessage(this, req));
			return 1;		   
		  }
		} else {
		  // not mine.

		  if (onfail == TRAVERSE_DISCOVER) {
			// discover
			cout << "mds" << whoami << " discover on " << have_clean << " for " << dname << "..., to mds" << dauth << endl;

			// assemble+send request
			vector<string> *want = new vector<string>;
			for (int i=depth; i<path_bits.size(); i++)
			  want->push_back(path_bits[i]);

			cur->get();  // pin discoveree

			MDiscover *dis = new MDiscover(whoami, have_clean, want);
			messenger->send_message(req,
									MSG_ADDR_MDS(dauth), MDS_PORT_SERVER,
									MDS_PORT_SERVER);
			
			// delay processing of current request
			cur->wait_on_discover(dname, new C_MDS_RetryMessage(this, req));

			return 1;
		  } 
		  if (onfail == TRAVERSE_FORWARD) {
			// forward
			cout << "mds" << whoami << " not authoritative for " << dname << ", fwd to mds" << dauth << endl;
			messenger->send_message(req,
									MSG_ADDR_MDS(dauth), MDS_PORT_SERVER,
									MDS_PORT_SERVER);
		  }	
		  if (onfail == TRAVERSE_FAIL) {
			return -1;
		  }
		}
	  }
	} else {
	  cout << cur->inode.ino << " not a dir " << cur->inode.isdir << endl;
	  return -ENOTDIR;
	}
	
	trace_dn.push_back(dname);
	trace.push_back(cur);
  }

  return 0;
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

bool MDS::open_root(Context *c)
{
  // open root inode
  if (whoami == 0) { 
	// i am root
	CInode *root = new CInode();
	root->inode.ino = 1;
	root->inode.isdir = true;

	// make it up (FIXME)
	root->inode.mode = 0755;
	root->inode.size = 0;

	root->dir_dist = 0;  // 0's

	mdcache->set_root( root );

	if (c) {
	  c->finish(0);
	  delete c;
	}
  } else {
	// request inode from root mds
	if (c) 
	  waiting_for_root.push_back(c);
	
	if (!opening_root) {
	  opening_root = true;

	  MDiscover *req = new MDiscover(whoami,
								   string(""),
								   new vector<string>);
	  messenger->send_message(req,
							  0, MDS_PORT_SERVER,
							  MDS_PORT_MAIN);
	}
  }
}
