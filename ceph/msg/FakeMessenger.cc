
#include "Message.h"
#include "FakeMessenger.h"
#include "mds/MDS.h"

#include "common/Timer.h"

#include "common/LogType.h"
#include "common/Logger.h"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <cassert>
#include <iostream>

using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


#include "common/Cond.h"
#include "common/Mutex.h"
#include <pthread.h>


// global queue.

map<int, FakeMessenger*>      directory;
hash_map<int, Logger*>        loggers;
LogType fakemsg_logtype;

set<FakeMessenger*>           shutdown_set;

Mutex lock;
Cond  cond;

bool pending_timer = false;

bool      awake = false;
bool      shutdown = false;
pthread_t thread_id;


class C_FakeKicker : public Context {
  void finish(int r) {
	dout(18) << "timer kick" << endl;
	pending_timer = true;
	lock.Lock();
	cond.Signal();  // why not
	lock.Unlock();
  }
};

void *fakemessenger_thread(void *ptr) 
{
  //dout(1) << "thread start, setting timer kicker" << endl;
  //g_timer.set_messenger_kicker(new C_FakeKicker());
  msgr_callback_kicker = new C_FakeKicker();

  lock.Lock();
  while (1) {
	dout(20) << "thread waiting" << endl;
	if (shutdown) break;
	awake = false;
	cond.Wait(lock);
	awake = true;
	dout(20) << "thread woke up" << endl;
	if (shutdown) break;

	fakemessenger_do_loop_2();

	if (directory.empty()) break;
  }
  lock.Unlock();

  cout << "unsetting messenger" << endl;
  //g_timer.unset_messenger_kicker();
  g_timer.unset_messenger();
  msgr_callback_kicker = 0;

  dout(1) << "thread finish (i woke up but no messages, bye)" << endl;
  return 0;
}


void fakemessenger_startthread() {
  pthread_create(&thread_id, NULL, fakemessenger_thread, 0);
}

void fakemessenger_stopthread() {
  cout << "fakemessenger_stopthread setting stop flag" << endl;
  lock.Lock();  
  shutdown = true;
  lock.Unlock();
  cond.Signal();
  
  fakemessenger_wait();
}

void fakemessenger_wait()
{
  cout << "fakemessenger_wait waiting" << endl;
  void *ptr;
  pthread_join(thread_id, &ptr);
}




// lame main looper

int fakemessenger_do_loop()
{
  lock.Lock();
  fakemessenger_do_loop_2();
  lock.Unlock();

  g_timer.shutdown();
  return 0;
}


int fakemessenger_do_loop_2()
{
  //lock.Lock();
  dout(18) << "do_loop begin." << endl;

  while (1) {
	bool didone = false;
	
	dout(18) << "do_loop top" << endl;

	/*// timer?
	if (pending_timer) {
	  pending_timer = false;
	  dout(5) << "pending timer" << endl;
	  g_timer.execute_pending();
	}
	*/

	// callbacks
	lock.Unlock();
	messenger_do_callbacks();
	lock.Lock();

	// messages
	map<int, FakeMessenger*>::iterator it = directory.begin();
	while (it != directory.end()) {

	  dout(18) << "messenger " << it->second << " at " << MSG_ADDR_NICE(it->first) << " has " << it->second->num_incoming() << " queued" << endl;

	  FakeMessenger *mgr = it->second;
	  Message *m = mgr->get_message();
	  it++;
	  
	  if (m) {
		//dout(18) << "got " << m << endl;
		dout(5) << "---- '" << m->get_type_name() << 
		  "' from " << MSG_ADDR_NICE(m->get_source()) << ':' << m->get_source_port() <<
		  " to " << MSG_ADDR_NICE(m->get_dest()) << ':' << m->get_dest_port() << " ---- " << m 
			 << endl;
		
		if (g_conf.fakemessenger_serialize) {
		  // encode
		  if (m->empty_payload()) 
			m->encode_payload();
		  msg_envelope_t env = m->get_envelope();
		  bufferlist bl;
		  bl.claim( m->get_payload() );
		  bl.c_str();   // condense into 1 buffer

		  delete m;
		  
		  // decode
		  m = decode_message(env, bl);
		  assert(m);
		} 
		
		didone = true;

		lock.Unlock();
		mgr->dispatch(m);
		lock.Lock();
	  }
	}
	
	// deal with shutdowns.. dleayed to avoid concurrent directory modification
	if (!shutdown_set.empty()) {
	  for (set<FakeMessenger*>::iterator it = shutdown_set.begin();
		   it != shutdown_set.end();
		   it++) {
		dout(7) << "fakemessenger: removing " << MSG_ADDR_NICE((*it)->get_myaddr()) << " from directory" << endl;
		assert(directory.count((*it)->get_myaddr()));
		directory.erase((*it)->get_myaddr());
		if (directory.empty()) {
		  dout(1) << "fakemessenger: last shutdown" << endl;
		  ::shutdown = true;
		}
	  }
	  shutdown_set.clear();
	}
	
	if (!didone)
	  break;
  }


  dout(18) << "do_loop end (no more messages)." << endl;
  //lock.Unlock();
  return 0;
}


FakeMessenger::FakeMessenger(long me)  : Messenger(me)
{
  whoami = me;
  lock.Lock();
  directory[ whoami ] = this;
  lock.Unlock();

  cout << "fakemessenger " << whoami << " messenger is " << this << endl;

  g_timer.set_messenger(this);

  /*
  string name;
  name = "m.";
  name += MSG_ADDR_TYPE(whoami);
  int w = MSG_ADDR_NUM(whoami);
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  loggers[ whoami ] = new Logger(name, (LogType*)&fakemsg_logtype);
  */
}

FakeMessenger::~FakeMessenger()
{

}


int FakeMessenger::shutdown()
{
  //cout << "shutdown on messenger " << this << " has " << num_incoming() << " queued" << endl;
  lock.Lock();
  assert(directory.count(whoami) == 1);
  shutdown_set.insert(this);
  
  /*
  directory.erase(whoami);
  if (directory.empty()) {
	dout(1) << "fakemessenger: last shutdown" << endl;
	::shutdown = true;
	cond.Signal();  // why not
  } 
  */

  /*
  if (loggers[whoami]) {
	delete loggers[whoami];
	loggers.erase(whoami);
  }
  */

  lock.Unlock();
  return 0;
}

/*
void FakeMessenger::trigger_timer(Timer *t) 
{
  // note timer to call
  pending_timer = t;

  // wake up thread?
  cond.Signal();  // why not
}
*/

int FakeMessenger::send_message(Message *m, msg_addr_t dest, int port, int fromport)
{
  m->set_source(whoami, fromport);
  m->set_dest(dest, port);
  m->set_lamport_stamp( get_lamport() );

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
	if (!dm) {
	  dout(1) << "** destination " << MSG_ADDR_NICE(dest) << " (" << dest << ") dne" << endl;
	  assert(dm);
	}
	dm->queue_incoming(m);

	dout(5) << "--> sending " << m << " to " << MSG_ADDR_NICE(dest) << endl;//" m " << dm << " has " << dm->num_incoming() << " queued" << endl;
	
  }
  catch (...) {
	cout << "no destination " << dest << endl;
	assert(0);
  }


  // wake up loop?
  if (!awake) {
	dout(10) << "waking up fakemessenger thread" << endl; 
	cond.Signal();
	lock.Unlock();
  } else
	lock.Unlock();
  
  return 0;
}


