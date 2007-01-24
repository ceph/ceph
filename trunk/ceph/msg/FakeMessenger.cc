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

map<int, FakeMessenger*>      directory;
hash_map<int, Logger*>        loggers;
LogType fakemsg_logtype;

set<int>           shutdown_set;

Mutex lock;
Cond  cond;

bool pending_timer = false;

bool      awake = false;
bool      fm_shutdown = false;
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

void FakeMessenger::callback_kick() 
{
  pending_timer = true;
  lock.Lock();
  cond.Signal();  // why not
  lock.Unlock();
}

void *fakemessenger_thread(void *ptr) 
{
  //dout(1) << "thread start, setting timer kicker" << endl;
  //g_timer.set_messenger_kicker(new C_FakeKicker());
  //msgr_callback_kicker = new C_FakeKicker();

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

  //cout << "unsetting messenger" << endl;
  //g_timer.unset_messenger_kicker();
  //g_timer.unset_messenger();
  //msgr_callback_kicker = 0;

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

    /*// timer?
    if (pending_timer) {
      pending_timer = false;
      dout(5) << "pending timer" << endl;
      g_timer.execute_pending();
    }
    */

    // callbacks
    lock.Unlock();
    Messenger::do_callbacks();
    lock.Lock();

    // messages
    map<int, FakeMessenger*>::iterator it = directory.begin();
    while (it != directory.end()) {
      FakeMessenger *mgr = it->second;

      dout(18) << "messenger " << mgr << " at " << mgr->get_myaddr() << " has " << mgr->num_incoming() << " queued" << endl;


      if (!mgr->is_ready()) {
        dout(18) << "messenger " << mgr << " at " << mgr->get_myaddr() << " has no dispatcher, skipping" << endl;
        it++;
        continue;
      }

      Message *m = mgr->get_message();
      it++;
      
      if (m) {
        //dout(18) << "got " << m << endl;
        dout(1) << "---- '" << m->get_type_name() 
                << "' from " << m->get_source() // << ':' << m->get_source_port() 
                << " to " << m->get_dest() //<< ':' << m->get_dest_port() 
                << " ---- " << m 
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
      for (set<int>::iterator it = shutdown_set.begin();
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


FakeMessenger::FakeMessenger(msg_addr_t me)  : Messenger(me)
{
  entity_inst_t fakeinst;
  lock.Lock();
  {
    // assign rank
    fakeinst.addr.sin_port = 
      fakeinst.rank = nranks++;
    set_myinst(fakeinst);

    // add to directory
    directory[ fakeinst.rank ] = this;
  }
  lock.Unlock();


  cout << "fakemessenger " << get_myaddr() << " messenger is " << this << " at " << fakeinst << endl;

  //g_timer.set_messenger(this);

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

}


int FakeMessenger::shutdown()
{
  //cout << "shutdown on messenger " << this << " has " << num_incoming() << " queued" << endl;
  lock.Lock();
  assert(directory.count(get_myinst().rank) == 1);
  shutdown_set.insert(get_myinst().rank);
  
  /*
  directory.erase(myaddr);
  if (directory.empty()) {
    dout(1) << "fakemessenger: last shutdown" << endl;
    ::fm_shutdown = true;
    cond.Signal();  // why not
  } 
  */

  /*
  if (loggers[myaddr]) {
    delete loggers[myaddr];
    loggers.erase(myaddr);
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

void FakeMessenger::reset_myaddr(msg_addr_t m)
{
  dout(1) << "reset_myaddr from " << get_myaddr() << " to " << m << endl;
  _set_myaddr(m);
}


int FakeMessenger::send_message(Message *m, msg_addr_t dest, entity_inst_t inst, int port, int fromport)
{
  m->set_source(get_myaddr(), fromport);
  m->set_dest(dest, port);
  //m->set_lamport_send_stamp( get_lamport() );

  m->set_source_inst(get_myinst());

  lock.Lock();

  // deliver
  try {
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
    FakeMessenger *dm = directory[inst.rank];
    if (!dm) {
      dout(1) << "** destination " << dest << " (" << inst << ") dne" << endl;
      assert(dm);
    }
    dm->queue_incoming(m);

    dout(1) << "--> " << get_myaddr() << " sending " << m << " '" << m->get_type_name() << "'"
            << " to " << dest 
            << endl;//" m " << dm << " has " << dm->num_incoming() << " queued" << endl;
    
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


