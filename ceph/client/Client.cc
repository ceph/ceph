
#include "Client.h"
#include "ClNode.h"

#include "../include/types.h"
#include "../include/Messenger.h"
#include "../include/Message.h"

#include "../include/MDS.h"

#include "../messages/MClientRequest.h"
#include "../messages/MClientReply.h"

#include <stdlib.h>

Client::Client(int id, Messenger *m)
{
  whoami = id;
  messenger = m;
  cwd = 0;
  root = 0;
  tid = 0;
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
  return 0;
}



// dispatch

void Client::dispatch(Message *m) 
{
  switch (m->get_type()) {
  case MSG_CLIENT_REPLY:
	cout << "client" << whoami << " got reply" << endl;
	assim_reply((MClientReply*)m);
	delete m;

	issue_request();	
	break;

  default:
	cout << "client" << whoami << " got unknown message " << m->get_type() << endl;
  }
}
	

void Client::assim_reply(MClientReply *r)
{
  ClNode *cur = root;

  // add items to cache
  for (int i=0; i<r->trace_dist.size(); i++) {
	if (i == 0) {
	  if (!root) {
		cur = root = new ClNode();
	  }
	} else {
	  if (cur->lookup(r->trace_dn[i-1]) == NULL) {
		ClNode *n = new ClNode();
		cur->link( r->trace_dn[i-1], n );
		cache_lru.lru_insert_top( n );
		cur = n;
	  } else {
		cur = cur->lookup( r->trace_dn[i-1] );
	  }
	}
	cur->ino = r->trace_ino[i];
	cur->dist = r->trace_dist[i];
  }

  cwd = cur;
}



void Client::issue_request()
{
  if (!cwd) cwd = root;
  string p = "";
  if (cwd) {
	
	if (rand() % 10 > 5) {
	  // descend
	  p = "/CVS/Root";
	} else {
	  // ascend
	  if (cwd->parent)
		cwd = cwd->parent;
	  cwd->full_path(p);
	}
  } 

  send_request(p);  // root, if !cwd
}

void Client::send_request(string& p) 
{

  MClientRequest *req = new MClientRequest(tid++, MDS_OP_STAT);
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
	  cout << "//path segment is " << dname << endl;
	  
	  ClNode *n = cur->lookup(dname);
	  if (n) {
		cur = n;
		off = nextslash+1;
	  } else {
		cout << " don't have it. " << endl;
		int b = cur->dist.size();
		//cout << " b is " << b << endl;
		for (int i=0; i<b; i++) {
		  if (cur->dist[i]) {
			mds = i;
			break;
		  }
		}
		break;
	  }
	}
  } else {
	// we need the root inode
	mds = 0;
  }

  cout << "client" << whoami << " sending req to mds " << mds << " for " << req->path << endl;
  messenger->send_message(req,
						  MSG_ADDR_MDS(mds), MDS_PORT_SERVER,
						  0);
}
