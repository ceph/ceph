
#include "Client.h"
#include "ClNode.h"

#include "../include/types.h"
#include "../include/Messenger.h"
#include "../include/Message.h"

#include "../include/MDS.h"

#include "../messages/MClientRequest.h"
#include "../messages/MClientReply.h"

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

	//issue_request();	
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
}



void Client::issue_request()
{
  MClientRequest *req = new MClientRequest(tid++, MDS_OP_STAT);
  req->ino = 1;
  req->path = "/CVS/Root";
  messenger->send_message(req,
						  MSG_ADDR_MDS(0), MDS_PORT_SERVER,
						  0);
}
