
#include "FakeMessenger.h"
#include "mds/MDS.h"

#include <map>
#include <ext/hash_map>

#include <iostream>
using namespace std;

// global queue.

hash_map<int, FakeMessenger*> directory;


// lame main looper

int fakemessenger_do_loop()
{
  cout << "do_loop begin." << endl;
  while (1) {
	bool didone = false;
	
	hash_map<int, FakeMessenger*>::iterator it = directory.begin();
	while (it != directory.end()) {
	  Message *m = it->second->get_message();
	  if (m) {
		cout << "---- do_loop dispatching t " << m->get_type() << 
		  " from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() << " ----" <<
		  " to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << endl;
		
		didone = true;
		it->second->dispatch(m);
	  }
	  it++;
	}

	if (!didone)
	  break;
  }
  cout << "do_loop end (no more messages)." << endl;
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

bool FakeMessenger::send_message(Message *m, long dest, int port, int fromport)
{
  m->set_source(whoami, fromport);
  m->set_dest(dest, port);

  // deliver
  try {
	FakeMessenger *dm = directory[dest];
	dm->queue_incoming(m);
  }
  catch (...) {
	cout << "no destination " << dest << endl;
  }
}

int FakeMessenger::wait_message(time_t seconds)
{
  return incoming.size();
}
