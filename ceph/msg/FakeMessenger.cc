
#include "include/FakeMessenger.h"
#include "include/mds.h"

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
		cout << "do_loop doing message for " << it->first << endl;
		didone = true;
		it->second->dispatch_message(m);
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

FakeMessenger::FakeMessenger() 
{
}


int FakeMessenger::init(MDS *mds)
{
  mymds = mds;
  whoami = mds->get_nodeid();
  directory[ whoami ] = this;
}

int FakeMessenger::shutdown()
{
  directory.erase(whoami);
}

bool FakeMessenger::send_message(Message *m, int dest)
{
  m->set_from(whoami);
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
