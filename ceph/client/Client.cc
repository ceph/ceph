
#include "include/types.h"
#include "include/Messenger.h"
#include "include/Message.h"

#include "mds/MDS.h"
#include "mds/MDCluster.h"

#include "Client.h"
#include "ClNode.h"

#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"

#include <stdlib.h>

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
  case MSG_CLIENT_REPLY:
	dout(9) << "got reply" << endl;
	assim_reply((MClientReply*)m);

	if (tid < max_requests)
	  issue_request();	
	else {
	  done();
	}
	break;

  default:
	dout(1) << "got unknown message " << m->get_type() << endl;
  }

  delete m;
}
	

void Client::done() {
  if (open_files.size()) {
	if (!did_close_all) {
	  dout(1) << "closing all open files" << endl;
	  for (multimap<inodeno_t,int>::iterator it = open_files.begin();
		   it != open_files.end();
		   it++) {
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

  // closed a file?
  if (r->get_op() == MDS_OP_CLOSE) {
	dout(12) << "closed inode " << r->get_ino() << " " << r->get_path() << endl;
	open_files.erase(open_files.find(r->get_ino()));
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
	  if (cur->lookup(last_req_dn[i]) == NULL) {
		ClNode *n = new ClNode();
		cur->link( last_req_dn[i], n );
		cur->isdir = true;  // clearly!
		cache_lru.lru_insert_top( n );
		cur = n;
	  } else {
		cur = cur->lookup( last_req_dn[i] );
		cache_lru.lru_touch(cur);
	  }
	}
	cur->ino = trace[i]->inode.ino;
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
	  if (cur->lookup((*it)->ref_dn)) 
		continue;  // skip if we already have it

	  ClNode *n = new ClNode();
	  n->ino = (*it)->inode.ino;
	  n->isdir = (*it)->inode.isdir;

	  for (set<int>::iterator i = (*it)->dist.begin(); i != (*it)->dist.end(); i++)
		cur->dist.push_back(*i);

	  cur->link( (*it)->ref_dn, n );
	  cache_lru.lru_insert_mid( n );

	  dout(12) << "client got dir item " << (*it)->ref_dn << endl;
	}
  }

  // opened a file?
  if (r->get_op() == MDS_OP_OPENRD ||
	  r->get_op() == MDS_OP_OPENWR) {
	dout(12) << "opened inode " << r->get_ino() << " " << r->get_path() << endl;
	open_files.insert(pair<inodeno_t,int>(r->get_ino(), r->get_source()));
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
	
	i->detach();
	delete i;
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

  if (!cwd) cwd = root;
  string p = "";
  if (cwd) {
	
	// back out to a dir
	while (!cwd->isdir) 
	  cwd = cwd->parent;
	
	if (rand() % 20 > 1+cwd->depth()) {
	  // descend
	  if (cwd->havedircontents) {
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

	last_req_dn.clear();
	cwd->full_path(p,last_req_dn);
  } 

  if (!op) {
	int r = rand() % 100;
	if (r < 10)
	  op = MDS_OP_TOUCH;
	else if (r < 20 && !is_open(cwd)) 
	  op = MDS_OP_OPENRD;
	else if (r < 30 && !is_open(cwd))
	  op = MDS_OP_OPENWR;
	else if (r < 40 + open_files.size() && open_files.size() > 0) {
	  // close file
	  return close_a_file();
	} 
	else   
	  op = MDS_OP_STAT;
  }

  send_request(p, op);  // root, if !cwd
}


bool Client::is_open(ClNode *c) 
{
  if (open_files.count(c->ino))
	return true;
  return false;
}

void Client::close_a_file()
{
  multimap<inodeno_t,int>::iterator it = open_files.begin();
  for (int r = rand() % open_files.size(); r > 0; r--) it++;

  MClientRequest *req = new MClientRequest(tid++, MDS_OP_CLOSE, whoami);
  req->set_ino(it->first);

  int mds = it->second;

  dout(9) << "sending close " << *req << " to mds" << mds << endl;
  messenger->send_message(req,
						  MSG_ADDR_MDS(mds), MDS_PORT_SERVER,
						  0);
}

void Client::send_request(string& path, int op) 
{

  MClientRequest *req = new MClientRequest(tid++, op, whoami);
  req->set_ino(1);
  req->set_path(path);

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
