
#include "include/types.h"
#include "include/Messenger.h"
#include "include/Message.h"

#include "mds/MDS.h"
#include "mds/MDCluster.h"

#include "Client.h"
#include "ClNode.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include "messages/MInodeSyncStart.h"
#include "messages/MInodeSyncAck.h"
#include "messages/MInodeSyncRelease.h"

#include <stdlib.h>
#include <unistd.h>

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << whoami << " "


Client::Client(MDCluster *mdc, int id, Messenger *m, long req)
{
  mdcluster = mdc;
  whoami = id;
  messenger = m;

  max_requests = req;

  did_close_all = false;

  cwd = 0;
  root = 0;
  tid = 0;

  cache_lru.lru_set_max(g_conf.client_cache_size);
  cache_lru.lru_set_midpoint(g_conf.client_cache_mid);
}

Client::~Client()
{
  if (messenger) { delete messenger; messenger = 0; }
}

int Client::init()
{
  messenger->set_dispatcher(this);
  return 0;
}

int Client::shutdown()
{
  messenger->shutdown();

  cache_lru.lru_set_max(0);
  trim_cache();
  if (root) {  // not in lru
	delete root;
	root = 0;
  }
  return 0;
}



// dispatch

void Client::dispatch(Message *m) 
{
  switch (m->get_type()) {

	// basic stuff
  case MSG_CLIENT_REPLY:
	assim_reply((MClientReply*)m);

	if (tid < max_requests)
	  issue_request();	
	else {
	  done();
	}
	break;


	// sync
  case MSG_MDS_INODESYNCSTART:
	handle_sync_start((MInodeSyncStart*)m);
	break;

  case MSG_MDS_INODESYNCRELEASE:
	handle_sync_release((MInodeSyncRelease*)m);
	break;

  default:
	dout(1) << "got unknown message " << m->get_type() << endl;
  }

  delete m;
}

void Client::handle_sync_start(MInodeSyncStart *m)
{
  ClNode *node = get_node(m->get_ino());
  assert(node);

  dout(1) << "sync_start on " << node->ino << endl;
  
  assert(is_open(node));
  
  // move from open_files to open_files_sync
  assert(open_files.count(m->get_ino()) > 0);
  
  int mds = MSG_ADDR_NUM(m->get_source());
  multimap<inodeno_t,int>::iterator it;
  for (it = open_files.find(node->ino);
	   it->second != mds;
	   it++) ;
  assert(it->second == mds);
  open_files.erase(it);
  open_files_sync.insert(pair<inodeno_t,int>(node->ino, mds));

  // reply
  messenger->send_message(new MInodeSyncAck(node->ino, true, true),   // wantback
						  m->get_source(), m->get_source_port(),
						  0);
}

void Client::handle_sync_release(MInodeSyncRelease *m)
{
  ClNode *node = get_node(m->get_ino());
  assert(node);
  assert(is_sync(node));

  dout(1) << "sync_release on " << node->ino << endl;

  assert(is_sync(node));

    // move from open_files_sync back to open_files
  assert(open_files_sync.count(node->ino) > 0);
  
  int mds = MSG_ADDR_NUM(m->get_source());
  multimap<inodeno_t,int>::iterator it;
  for (it = open_files_sync.find(node->ino);
	   it->second != mds;
	   it++) ;
  assert(it->second == mds);
  open_files_sync.erase(it);
  open_files.insert(pair<inodeno_t,int>(node->ino, mds));
}
	

void Client::done() {
  if (open_files.size() + open_files_sync.size()) {
	if (!did_close_all) {
	  dout(1) << "closing all open files" << endl;
	  for (multimap<inodeno_t,int>::iterator it = open_files.begin();
		   it != open_files_sync.end();
		   it++) {
		if (it == open_files.end()) {
		  it = open_files_sync.begin();
		  if (it == open_files_sync.end()) break;
		}
		dout(10) << "  closing " << it->first << " to " << it->second << endl;
		MClientRequest *req = new MClientRequest(tid++, MDS_OP_CLOSE, whoami);
		req->set_ino(it->first);
		dout(9) << "sending " << *req << " to mds" << it->second << endl;
		messenger->send_message(req,
								MSG_ADDR_MDS(it->second), MDS_PORT_SERVER,
								0);
	  }
	  did_close_all = true;
	} else 
	  dout(10) << "waiting for files to close, there are " << open_files.size() << " more" << endl;
  }

  else {
	dout(1) << "done, all files closed, sending msg to mds0" << endl;
	messenger->send_message(new Message(MSG_CLIENT_DONE),
							MSG_ADDR_MDS(0), MDS_PORT_MAIN, 0);
  }
}

void Client::assim_reply(MClientReply *r)
{
  if (r->get_result() != 0) {
	dout(12) << "error " << r->get_result() << endl;
	return;
  }
  dout(10) << "success" << endl;


  // closed a file?
  if (r->get_op() == MDS_OP_CLOSE) {
	dout(12) << "closed inode " << r->get_ino() << " " << r->get_path() << endl;

	// note that the file might be sync (if our close and the sync crossed paths!)
	multimap<inodeno_t, int>::iterator it = open_files.find(r->get_ino());
	if (it != open_files.end()) {
	  open_files.erase(it);
	} else {
	  it = open_files_sync.find(r->get_ino());
	  assert(it != open_files_sync.end());
	  open_files_sync.erase(it);
	}
	ClNode *node = get_node(r->get_ino());
	assert(node);
	node->put();
	if (node->dangling) {
	  dout(12) << "removing dangling node " << node->ino << endl;
	  remove_node(node);
	}
	return;
  }

  if (r->get_op() == MDS_OP_UNLINK) {
	ClNode *node = cwd;
	if (node->ino == r->get_ino()) {
	  cwd = cwd->parent;
	  if (node) {
		detach_node(node);
		if (is_open(node)) {
		  dout(12) << "dangling unlinked open node " << node->ino << endl;
		  node->dangling = true;
		} else {
		  dout(12) << "removing unlinked node " << node->ino << endl;
		  remove_node(node);	  
		}
	  }
	} else {
	  dout(12) << "oops, i unlinked somethign that moved.  whatever." << endl;
	}
	return;
  }

  // normal crap
  ClNode *cur = root;

  vector<c_inode_info*> trace = r->get_trace();

  // update trace items in cache
  for (int i=0; i<trace.size(); i++) {
	if (i == 0) {
	  if (!root) {
		cur = root = new ClNode();
	  }
	} else {
	  ClNode *next = cur->lookup( last_req_dn[i] );
	  
	  if (next) {
		if (next->ino == trace[i]->inode.ino) {
		  cache_lru.lru_touch(next);
		  dout(12) << " had dentry " << last_req_dn[i] << " with correct ino " << next->ino << endl;
		} else {
		  dout(12) << " had dentry " << last_req_dn[i] << " with WRONG ino " << next->ino << endl;
		  detach_node(next);
		  if (is_open(next)) {
			next->dangling = true;
		  } else {
			dout(12) << "removing changed dentry node " << next->ino << endl;;
			remove_node(next);
		  }
		  next = NULL;
		}
	  }

	  if (!next) {
		next = get_node(trace[i]->inode.ino);
		if (next) {
		  dout(12) << " had ino " << next->ino << " at wrong position, moving" << endl;
		  next->detach();
		  cur->link( last_req_dn[i], next );
		}
	  }
	  
	  if (!next) {
		next = new ClNode();
		next->ino = trace[i]->inode.ino;
		cur->link( last_req_dn[i], next );
		add_node(next);
		if (i < last_req_dn.size()-1) cur->isdir = true;  // clearly!
		cache_lru.lru_insert_top( next );
		dout(12) << " new dentry+node with ino " << next->ino << endl;
	  }

	  cur = next;
	}

	for (set<int>::iterator it = trace[i]->dist.begin(); it != trace[i]->dist.end(); it++)
	  cur->dist.push_back(*it);
	cur->isdir = trace[i]->inode.isdir;
  }


  // add dir contents
  if (r->get_op() == MDS_OP_READDIR) {
	cur->havedircontents = true;

	vector<c_inode_info*>::iterator it;
	for (it = r->get_dir_contents().begin(); 
		 it != r->get_dir_contents().end(); 
		 it++) {
	  ClNode *n = cur->lookup((*it)->ref_dn);
	  if (n) {
		if (n->ino == (*it)->inode.ino)
		  continue;  // skip if we already have it
		
		// wrong ino!
		detach_node(n);
		if (is_open(n))
		  n->dangling = true;
		else
		  remove_node(n);
	  }

	  n = new ClNode();
	  n->ino = (*it)->inode.ino;
	  n->isdir = (*it)->inode.isdir;

	  for (set<int>::iterator i = (*it)->dist.begin(); i != (*it)->dist.end(); i++)
		cur->dist.push_back(*i);

	  cur->link( (*it)->ref_dn, n );
	  add_node(n);
	  cache_lru.lru_insert_mid( n );

	  dout(12) << "client got dir item " << (*it)->ref_dn << endl;
	}
  }

  // opened a file?
  if (r->get_op() == MDS_OP_OPENRD ||
	  r->get_op() == MDS_OP_OPENWR ||
	  r->get_op() == MDS_OP_OPENWRC) {
	if (trace[trace.size()-1]->is_sync) {
	  dout(10) << "opened inode SYNC " << r->get_ino() << " " << r->get_path() << endl;
	  open_files_sync.insert(pair<inodeno_t,int>(r->get_ino(), r->get_source()));
	} else {
	  dout(10) << "opened inode " << r->get_ino() << " " << r->get_path() << endl;
	  open_files.insert(pair<inodeno_t,int>(r->get_ino(), r->get_source()));
	}
	ClNode *node = get_node(r->get_ino());
	assert(node);
	node->get();
  }


  cwd = cur;

  trim_cache();
}


void Client::trim_cache()
{
  int expired = 0;
  while (cache_lru.lru_get_size() > cache_lru.lru_get_max()) {
	ClNode *i = (ClNode*)cache_lru.lru_expire();
	if (!i) 
	  break;  // out of things to expire!
	
	dout(12) << "expiring inode " << i->ino << endl;
	assert(i->refs == 0);
	detach_node(i);
	remove_node(i);
	expired++;
  }
  if (g_conf.debug > 11)
	cache_lru.lru_status();
  if (expired) 
	dout(12) << "EXPIRED " << expired << " items" << endl;
}


void Client::issue_request()
{
  int op = 0;

  if (!cwd) {
	dout(12) << "no cwd, starting at root" << endl;
	cwd = root;
  }
  string p = "";
  if (cwd) {

	string curp;
	vector<string> blahcrap;
	cwd->full_path(curp,blahcrap);
	dout(12) << "cwd = " << curp << endl;
	
	// back out to a dir
	while (!cwd->isdir) 
	  cwd = cwd->parent;

	curp = "";
	cwd->full_path(curp,blahcrap);
	dout(12) << "back to dir: cwd = " << curp << endl;
	
	if (rand() % 20 > 1+cwd->depth()) {
	  // descend
	  if (cwd->havedircontents && cwd->children.size() && (rand()%10 > 1)) {
		dout(12) << "descending" << endl;
		// descend
		int n = rand() % cwd->children.size();
		hash_map<string, ClNode*>::iterator it = cwd->children.begin();
		while (n-- > 0) it++;
		cwd = (*it).second;
	  } else {
		// readdir
		dout(12) << "readdir" << endl;
		op = MDS_OP_READDIR;
	  }
	} else {
	  // ascend
	  dout(12) << "ascending" << endl;
	  if (cwd->parent)
		cwd = cwd->parent;
	}

	curp = "";
	cwd->full_path(curp,blahcrap);
	dout(12) << "end: cwd = " << curp << endl;

	last_req_dn.clear();
	cwd->full_path(p,last_req_dn);
  } 
  
  string arg;
  if (!op) {
	int r = rand() % 100;
	op = MDS_OP_STAT;
	if (cwd) {  // stat root if we don't have it.
	  if (r < 10)
		op = MDS_OP_TOUCH;
	  else if (r < 11) 
		op = MDS_OP_CHMOD;
	  else if (r < 20 && !is_open(cwd) && !cwd->isdir) 
		op = MDS_OP_OPENRD;
	  else if (r < 30 && !is_open(cwd) && !cwd->isdir)
		op = MDS_OP_OPENWR;
	  else if (!g_conf.client_deterministic &&
			   r < 34 && cwd->isdir) {
		op = MDS_OP_OPENWRC;
		string dn = "blah_client_created.";
		char pid[10];
		sprintf(pid,"%d",getpid());
		
		if (!g_conf.client_deterministic)
		  dn += pid;

		if (cwd->lookup(dn))
		  op = MDS_OP_STAT; // nevermind
		else {
		  // do wrc!
		  p += "/";
		  p += dn;
		  last_req_dn.push_back(dn);
		}
	  }
	  else if (!g_conf.client_deterministic &&
			   r < 35 && !cwd->isdir)
		op = MDS_OP_UNLINK;
	  else if (!g_conf.client_deterministic &&
			   r < 100 && !cwd->isdir) {
		op = MDS_OP_RENAME;
		char s[100];
		sprintf(s,"rename.%d", rand() % 50);
		string ss = s;
		filepath p1 = p;
		filepath p2 = p1.subpath(p1.length() - 1);
		p2.add_dentry(ss);
		arg = p2.get_path();
	  }
	  else if (false && !g_conf.client_deterministic &&
			   r < 37 && cwd->isdir) {
		op = MDS_OP_MKDIR;
		string dn = "client_dir.";
		char num[10];
		sprintf(num,"%d",rand()%100);
		dn += num;

		dout(10) << "trying mkdir on " << p << " / " << dn << endl;
		if (cwd->lookup(dn))
		  op = MDS_OP_STAT; // nevermind, exists
		else {
		  // do it
		  p += "/";
		  p += dn;
		  last_req_dn.push_back(dn);
		}
	  }
	  else if (r < 41 + open_files.size() && open_files.size() > 0)
		return close_a_file();  // close file
	}
  }

  send_request(p, op, arg);  // root, if !cwd
}


bool Client::is_open(ClNode *c) 
{
  if (open_files.count(c->ino) ||
	  open_files_sync.count(c->ino))
	return true;
  return false;
}
bool Client::is_sync(ClNode *c) 
{
  if (open_files_sync.count(c->ino))
	return true;
  return false;
}

void Client::close_a_file()
{
  multimap<inodeno_t,int>::iterator it = open_files.begin();
  for (int r = rand() % (open_files.size()); r > 0; r--) 
	it++;

  MClientRequest *req = new MClientRequest(tid++, MDS_OP_CLOSE, whoami);
  req->set_ino(it->first);
  assert(get_node(it->first));

  int mds = it->second;

  dout(9) << "sending close " << *req << " on " << it->first << " to mds" << mds << endl;
  messenger->send_message(req,
						  MSG_ADDR_MDS(mds), MDS_PORT_SERVER,
						  0);
}

void Client::send_request(string& path, int op, string& arg) 
{

  MClientRequest *req = new MClientRequest(tid++, op, whoami);
  req->set_ino(1);
  req->set_path(path);
  req->set_arg(arg);

  // direct it
  int mds = 0;

  if (root) {
	int off = 0;
	ClNode *cur = root;
	while (off < path.length()) {
	  int nextslash = path.find('/', off);
	  if (nextslash == off) {
		off++;
		continue;
	  }
	  if (nextslash < 0) 
		nextslash = path.length();  // no more slashes
	  
	  string dname = path.substr(off,nextslash-off);
	  //cout << "//path segment is " << dname << endl;
	  
	  ClNode *n = cur->lookup(dname);
	  if (n) {
		cur = n;
		off = nextslash+1;
	  } else {
		dout(12) << " don't have it. " << endl;
		break;
	  }

	  if (!cur->dist.empty()) {
		int r = rand() % cur->dist.size();
		mds = cur->dist[r];  
	  }
	}
  } else {
	// we need the root inode
	mds = 0;
  }

  dout(9) << "sending " << *req << " to mds" << mds << endl;
  messenger->send_message(req,
						  MSG_ADDR_MDS(mds), MDS_PORT_SERVER,
						  0);
}
