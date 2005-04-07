


#include "Message.h"
#include "FakeMessenger.h"
#include "mds/MDS.h"
#include "include/LogType.h"
#include "include/Logger.h"

#include "include/config.h"

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <ext/hash_map>
#include <cassert>
#include <iostream>

using namespace std;



#include "include/config.h"


// global queue.

map<int, FakeMessenger*> directory;
hash_map<int, Logger*>        loggers;
LogType fakemsg_logtype;

// lame main looper

int fakemessenger_do_loop()
{
  dout(1) << "do_loop begin." << endl;
  while (1) {
	bool didone = false;
	
	dout(11) << "do_loop top" << endl;

	map<int, FakeMessenger*>::iterator it = directory.begin();
	while (it != directory.end()) {
	  Message *m = it->second->get_message();
	  if (m) {
		dout(20) << m << endl;
		dout(3) << "---- do_loop dispatching '" << m->get_type_name() << 
		  "' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
		  " to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " << m 
			 << endl;
		
		if (g_conf.fakemessenger_serialize) {
		  int t = m->get_type();
		  if (true
			  || t == MSG_CLIENT_REQUEST
			  || t == MSG_CLIENT_REPLY
			  || t == MSG_MDS_DISCOVER
			  ) {
			// serialize
			crope buffer;
			m->encode(buffer);
			delete m;
			
			// decode
			m = decode_message(buffer);
			assert(m);
		  }
		}
		
		didone = true;
		it->second->dispatch(m);
	  }
	  it++;
	}

	if (!didone)
	  break;
  }
  dout(1) << "do_loop end (no more messages)." << endl;
}


// class

int FakeMessenger::loop() 
{
  // this only better be called once or we'll overflow the stack or something dumb.
  fakemessenger_do_loop();
}

FakeMessenger::FakeMessenger(long me)
{
  whoami = me;
  directory[ whoami ] = this;

  string name;
  name = "m.";
  name += MSG_ADDR_TYPE(whoami);
  int w = MSG_ADDR_NUM(whoami);
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  logger = new Logger(name, (LogType*)&fakemsg_logtype);
  loggers[ whoami ] = logger;
}

FakeMessenger::~FakeMessenger()
{
  shutdown();

  delete logger;
}


int FakeMessenger::init(Dispatcher *d)
{
  set_dispatcher(d);
}

int FakeMessenger::shutdown()
{
  directory.erase(whoami);

  remove_dispatcher();
}


int FakeMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  m->set_source(whoami, fromport);
  m->set_dest(dest, port);

  // deliver
  try {
#ifdef LOG_MESSAGES
	// stats
	loggers[whoami]->inc("+send",1);
	loggers[dest]->inc("-recv",1);

	char s[20];
	sprintf(s,"+%s", m->get_type_name());
	loggers[whoami]->inc(s);
	sprintf(s,"-%s", m->get_type_name());
	loggers[dest]->inc(s);
#endif

	// queue
	FakeMessenger *dm = directory[dest];
	dm->queue_incoming(m);

	dout(10) << "sending " << m << " to " << dest << endl;
	
  }
  catch (...) {
	cout << "no destination " << dest << endl;
	assert(0);
  }
}

int FakeMessenger::wait_message(time_t seconds)
{
  return incoming.size();
}
