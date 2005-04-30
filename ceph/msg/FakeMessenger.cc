


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


#include "Semaphore.h"
#include "Mutex.h"
#include <pthread.h>

#include "include/config.h"


// global queue.

map<int, FakeMessenger*> directory;
hash_map<int, Logger*>        loggers;
LogType fakemsg_logtype;

Mutex lock;
Semaphore sem;
Semaphore shutdownsem;
bool      awake = false;
bool      shutdown = false;
pthread_t thread_id;

void *fakemessenger_thread(void *ptr) 
{
  dout(1) << "thread start" << endl;

  while (1) {
	awake = false;
	sem.Wait();
	dout(1) << "thread woke up" << endl;

	if (shutdown) break;
	fakemessenger_do_loop();
  }

  dout(1) << "thread finish (i woke up but no messages, bye)" << endl;
  shutdownsem.Post();
}


void fakemessenger_startthread() {
  pthread_create(&thread_id, NULL, fakemessenger_thread, 0);
}

void fakemessenger_stopthread() {
  shutdown = true;
  sem.Post();
  shutdownsem.Wait();
}




// lame main looper

int fakemessenger_do_loop()
{
  dout(1) << "do_loop begin." << endl;

  while (1) {
	bool didone = false;
	
	dout(11) << "do_loop top" << endl;

	lock.Lock();

	map<int, FakeMessenger*>::iterator it = directory.begin();
	while (it != directory.end()) {
	  Message *m = it->second->get_message();
	  if (m) {
		dout(15) << "got " << m << endl;
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
		
		lock.Unlock();

		didone = true;
		it->second->dispatch(m);

		lock.Lock();
	  }
	  it++;
	}

	lock.Unlock();


	if (!didone)
	  break;
  }


  dout(1) << "do_loop end (no more messages)." << endl;
  return 0;
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

  cout << "fakemessenger " << whoami << " messenger is " << this << endl;

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

  lock.Lock();

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

  // wake up loop?
  if (!awake) {
	dout(1) << "waking up fakemessenger thread" << endl; 
	awake = true;
	sem.Post();
  }

  lock.Unlock();
}

int FakeMessenger::wait_message(time_t seconds)
{
  return incoming.size();
}
