
#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <list>

#include "Message.h"


class Messenger {
 protected:
  list<Message*> incoming;
  list<Message*> outgoing;

 public:
  Messenger();
  ~Messenger();

  // ...
  bool send_message(Message *m);

  void dispatch_message(Message *m) {
	switch (m->get_subsys()) {

	case MSG_SUBSYS_MDSTORE:
	  g_mds->mdstore->proc_message(m);
	  break;
	  
	case MSG_SUBSYS_MDLOG:
	  g_mds->logger->proc_message(m);
	  break;

	case MSG_SUBSYS_BALANCER:
	  g_mds->balancer->proc_message(m);
	  break;

	case MSG_SUBSYS_SERVER:
	  g_mds->proc_message(m);
	  break;
	}
  }

  void dispatch_context(Context *c) {
	switch (c->subsys) {

	case MSG_SUBSYS_MDSTORE:
	  g_mds->mdstore->proc_context(m);
	  break;
	  
	case MSG_SUBSYS_MDLOG:
	  g_mds->logger->proc_context(m);
	  break;

	case MSG_SUBSYS_BALANCER:
	  g_mds->balancer->proc_context(m);
	  break;

	case MSG_SUBSYS_SERVER:
	  g_mds->proc_context(m);
	  break;
	}
  }
  

  bool queue_incoming(Message *m) {
	incoming.push_back(m);
	return true;
  }

};

#endif
