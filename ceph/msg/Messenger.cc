
#include "include/Messenger.h"
#include "include/mds.h"

#include "include/MDStore.h"


void Messenger::dispatch_message(Message *m)
{
  switch (m->get_subsys()) {
	
  case MSG_SUBSYS_MDSTORE:
	mymds->mdstore->proc_message(m);
	break;
	
	/*
  case MSG_SUBSYS_MDLOG:
	mymds->logger->proc_message(m);
	break;
	
  case MSG_SUBSYS_BALANCER:
	mymds->balancer->proc_message(m);
	break;
	*/

  case MSG_SUBSYS_SERVER:
	mymds->proc_message(m);
	break;
  }
}
