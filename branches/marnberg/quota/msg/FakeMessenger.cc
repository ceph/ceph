// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "Message.h"
#include "FakeMessenger.h"
#include "mds/MDS.h"

#include "common/Timer.h"

#include "common/LogType.h"
#include "common/Logger.h"

#include "config.h"

#undef dout
#define dout(x) if ((x) <= g_conf.debug_ms) cout << g_clock.now() << " "



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

int nranks = 0;  // this identify each entity_inst_t

map<entity_addr_t, FakeMessenger*>      directory;
hash_map<int, Logger*>        loggers;
LogType fakemsg_logtype;

set<entity_addr_t>           shutdown_set;

Mutex lock;
Cond  cond;

bool      awake = false;
bool      fm_shutdown = false;
pthread_t thread_id;




void *fakemessenger_thread(void *ptr) 
{
  lock.Lock();
  while (1) {
    dout(20) << "thread waiting" << endl;
    if (fm_shutdown) break;
    awake = false;
    cond.Wait(lock);
    awake = true;
    dout(20) << "thread woke up" << endl;
    if (fm_shutdown) break;

    fakemessenger_do_loop_2();

    if (directory.empty()) break;
  }
  lock.Unlock();

  dout(1) << "thread finish (i woke up but no messages, bye)" << endl;
  return 0;
}


void fakemessenger_startthread() {
  pthread_create(&thread_id, NULL, fakemessenger_thread, 0);
}

void fakemessenger_stopthread() {
  cout << "fakemessenger_stopthread setting stop flag" << endl;
  lock.Lock();  
  fm_shutdown = true;
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

    // messages
    map<entity_addr_t, FakeMessenger*>::iterator it = directory.begin();
    while (it != directory.end()) {
      FakeMessenger *mgr = it->second;

      dout(18) << "messenger " << mgr << " at " << mgr->get_myname() << " has " << mgr->num_incoming() << " queued" << endl;

      if (!mgr->is_ready()) {
        dout(18) << "messenger " << mgr << " at " << mgr->get_myname() << " has no dispatcher, skipping" << endl;
        it++;
        continue;
      }

      Message *m = mgr->get_message();
      it++;
      
      if (m) {
        //dout(18) << "got " << m << endl;
        dout(1) << "---- " << m->get_dest() 
		<< " <- " << m->get_source()
                << " ---- " << *m 
                << endl;
        
        if (g_conf.fakemessenger_serialize) {
          // encode
          if (m->empty_payload()) 
            m->encode_payload();
          msg_envelope_t env = m->get_envelope();
          bufferlist bl;
          bl.claim( m->get_payload() );
          //bl.c_str();   // condense into 1 buffer

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
      for (set<entity_addr_t>::iterator it = shutdown_set.begin();
           it != shutdown_set.end();
           it++) {
        dout(7) << "fakemessenger: removing " << *it << " from directory" << endl;
        assert(directory.count(*it));
        directory.erase(*it);
        if (directory.empty()) {
          dout(1) << "fakemessenger: last shutdown" << endl;
          ::fm_shutdown = true;
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


FakeMessenger::FakeMessenger(entity_name_t me)  : Messenger(me)
{
  lock.Lock();
  {
    // assign rank
    _myinst.name = me;
    _myinst.addr.port = nranks++;
    //if (!me.is_mon())
    //_myinst.addr.nonce = getpid();

    // add to directory
    directory[ _myinst.addr ] = this;
  }
  lock.Unlock();


  cout << "fakemessenger " << get_myname() << " messenger is " << this << " at " << _myinst << endl;

  qlen = 0;

  /*
  string name;
  name = "m.";
  name += MSG_ADDR_TYPE(myaddr);
  int w = MSG_ADDR_NUM(myaddr);
  if (w >= 1000) name += ('0' + ((w/1000)%10));
  if (w >= 100) name += ('0' + ((w/100)%10));
  if (w >= 10) name += ('0' + ((w/10)%10));
  name += ('0' + ((w/1)%10));

  loggers[ myaddr ] = new Logger(name, (LogType*)&fakemsg_logtype);
  */
}

FakeMessenger::~FakeMessenger()
{
  // hose any undelivered messages
  for (list<Message*>::iterator p = incoming.begin();
       p != incoming.end();
       ++p)
    delete *p;
}


int FakeMessenger::shutdown()
{
  //cout << "shutdown on messenger " << this << " has " << num_incoming() << " queued" << endl;
  lock.Lock();
  assert(directory.count(_myinst.addr) == 1);
  shutdown_set.insert(_myinst.addr);
  
  /*
  if (loggers[myaddr]) {
    delete loggers[myaddr];
    loggers.erase(myaddr);
  }
  */

  lock.Unlock();
  return 0;
}


void FakeMessenger::reset_myname(entity_name_t m)
{
  dout(1) << "reset_myname from " << get_myname() << " to " << m << endl;
  _set_myname(m);

  directory.erase(_myinst.addr);
  _myinst.name = m;
  directory[_myinst.addr] = this;
  
}


int FakeMessenger::send_message(Message *m, entity_inst_t inst, int port, int fromport)
{
  entity_name_t dest = inst.name;
  
  m->set_source(get_myname(), fromport);
  m->set_source_addr(get_myaddr());

  m->set_dest(inst.name, port);

  lock.Lock();

#ifdef LOG_MESSAGES
  // stats
  loggers[get_myaddr()]->inc("+send",1);
  loggers[dest]->inc("-recv",1);
  
  char s[20];
  sprintf(s,"+%s", m->get_type_name());
  loggers[get_myaddr()]->inc(s);
  sprintf(s,"-%s", m->get_type_name());
  loggers[dest]->inc(s);
#endif

  // queue
  if (directory.count(inst.addr)) {
    dout(1) << "--> " << get_myname() << " -> " << inst.name << " " << *m << endl;
    directory[inst.addr]->queue_incoming(m);
  } else {
    dout(0) << "--> " << get_myname() << " -> " << inst.name << " " << *m
	    << " *** destination DNE ***" << endl;
    for (map<entity_addr_t, FakeMessenger*>::iterator p = directory.begin();
	 p != directory.end();
	 ++p) {
      dout(0) << "** have " << p->first << " to " << p->second << endl;
    }
    //assert(dm);
    delete m;
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


