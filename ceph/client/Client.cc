
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

#define debug 1

Client::Client(MDCluster *mdc, int id, Messenger *m, long req)
{
  mdcluster = mdc;
  whoami = id;
  messenger = m;

  max_requests = req;

  cwd = 0;
  root = 0;
  tid = 0;

  cache_lru.lru_set_max(CLIENT_CACHE);
  cache_lru.lru_set_midpoint(CLIENT_CACHE_MID);
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
	if (debug > 1)
	  cout << "client" << whoami << " got reply" << endl;
	assim_reply((MClientReply*)m);

	if (tid < max_requests)
	  issue_request();	
	break;

  default:
	cout << "client" << whoami << " got unknown message " << m->get_type() << endl;
  }

  delete m;
}
	

void Client::assim_reply(MClientReply *r)
{
  ClNode *cur = root;

  // update trace items in cache
  for (int i=0; i<r->trace.size(); i++) {
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
	cur->ino = r->trace[i]->inode.ino;
	for (set<int>::iterator it = r->trace[i]->dist.begin(); it != r->trace[i]->dist.end(); it++)
	  cur->dist.push_back(*it);
	cur->isdir = r->trace[i]->inode.isdir;

	// free c_inode_info
	delete r->trace[i];
  }


  // add dir contents
  if (r->op == MDS_OP_READDIR) {
	cur->havedircontents = true;

	vector<c_inode_info*>::iterator it;
	for (it = r->dir_contents->begin(); it != r->dir_contents->end(); it++) {
	  if (cur->lookup((*it)->ref_dn)) {
		delete *it;
		continue;  // skip if we already have it
	  }

	  ClNode *n = new ClNode();
	  n->ino = (*it)->inode.ino;
	  n->isdir = (*it)->inode.isdir;

	  for (set<int>::iterator i = (*it)->dist.begin(); i != (*it)->dist.end(); i++)
		cur->dist.push_back(*i);

	  cur->link( (*it)->ref_dn, n );
	  cache_lru.lru_insert_mid( n );

	  if (debug > 3)
		cout << "client got dir item " << (*it)->ref_dn << endl;

	  // free the c_inode_info
	  delete *it;
	}

	// free dir_contents vector
	delete r->dir_contents;
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
  if (debug > 1)
	cache_lru.lru_status();
  if (expired && debug > 2) 
	cout << "EXPIRED " << expired << " items" << endl;
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
	
	if (rand() % 10 > 1+(cwd->depth()/2)) {
	  // descend
	  if (cwd->havedircontents) {
		if (debug > 3)
		  cout << "descending" << endl;
		// descend
		int n = rand() % cwd->children.size();
		hash_map<string, ClNode*>::iterator it = cwd->children.begin();
		while (n-- > 0) it++;
		cwd = (*it).second;
	  } else {
		// readdir
		if (debug > 3) cout << "readdir" << endl;
		op = MDS_OP_READDIR;
	  }
	} else {
	  // ascend
	  if (debug > 3) cout << "ascending" << endl;
	  if (cwd->parent)
		cwd = cwd->parent;
	}

	last_req_dn.clear();
	cwd->full_path(p,last_req_dn);
  } 

  if (!op) {
	if (rand() % 10 > 8)
	  op = MDS_OP_TOUCH;
	else
	  op = MDS_OP_STAT;
  }

  send_request(p, op);  // root, if !cwd
}

void Client::send_request(string& p, int op) 
{

  MClientRequest *req = new MClientRequest(tid++, op, whoami);
  req->ino = 1;
  req->path = p;

  // direct it
  int mds = 0;

  if (root) {
	int off = 0;
	ClNode *cur = root;
	while (off < req->path.length()) {
	  int nextslash = req->path.find('/', off);
	  if (nextslash == off) {
		off++;
		continue;
	  }
	  if (nextslash < 0) 
		nextslash = req->path.length();  // no more slashes
	  
	  string dname = req->path.substr(off,nextslash-off);
	  //cout << "//path segment is " << dname << endl;
	  
	  ClNode *n = cur->lookup(dname);
	  if (n) {
		cur = n;
		off = nextslash+1;
	  } else {
		if (debug > 3) cout << " don't have it. " << endl;
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

  if (debug > 0)
	cout << "client" << whoami << " req " << req->tid << " op " << req->op << " to mds" << mds << " for " << req->path << endl;
  messenger->send_message(req,
						  MSG_ADDR_MDS(mds), MDS_PORT_SERVER,
						  0);
}
