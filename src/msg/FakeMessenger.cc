// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#define dout(x) if ((x) <= g_conf.debug_ms) *_dout << dbeginl << g_clock.now() << " "



#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <iostream>

using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


#include "common/Cond.h"
#include "common/Mutex.h"
#include <pthread.h>


// global queue.

int num_entity;
vector<FakeMessenger*> directory;

hash_map<int, Logger*>        loggers;
LogType fakemsg_logtype;

set<entity_addr_t>           shutdown_set;

Mutex lock("FakeMessenger.cc lock");
Cond  cond;

bool      awake = false;
bool      fm_shutdown = false;
pthread_t thread_id;

extern std::map<entity_name_t,float> g_fake_kill_after;  // in config.cc
utime_t start_time;
map<utime_t,entity_name_t> fail_queue;
list<Message*> sent_to_failed_queue;

void *fakemessenger_thread(void *ptr) 
{
  start_time = g_clock.now();
  
  lock.Lock();
  while (1) {
    if (fm_shutdown) break;
    fakemessenger_do_loop_2();
    
    if (num_entity == 0 && directory.size() > 0) break;
    
    dout(20) << "thread waiting" << dendl;
    if (fm_shutdown) break;
    awake = false;
    cond.Wait(lock);
    awake = true;
    dout(20) << "thread woke up" << dendl;
  }
  lock.Unlock();

  dout(1) << "thread finish (i woke up but no messages, bye)" << dendl;
  return 0;
}


void fakemessenger_startthread() {
  pthread_create(&thread_id, NULL, fakemessenger_thread, 0);
}

void fakemessenger_stopthread() {
  dout(0) << "fakemessenger_stopthread setting stop flag" << dendl;
  lock.Lock();  
  fm_shutdown = true;
  lock.Unlock();
  cond.Signal();
  
  fakemessenger_wait();
}

void fakemessenger_wait()
{
  dout(0) << "fakemessenger_wait waiting" << dendl;
  void *ptr;
  pthread_join(thread_id, &ptr);
}


// fake failure



// lame main looper

int fakemessenger_do_loop()
{
  lock.Lock();
  fakemessenger_do_loop_2();
  lock.Unlock();

  return 0;
}


int fakemessenger_do_loop_2()
{
  //lock.Lock();
  dout(18) << "do_loop begin." << dendl;
  
  while (1) {
    bool didone = false;
    
    dout(18) << "do_loop top" << dendl;

    // fail_queue
    while (!fail_queue.empty() && 
	   fail_queue.begin()->first < g_clock.now()) {
      entity_name_t nm = fail_queue.begin()->second;
      fail_queue.erase(fail_queue.begin());

      dout(0) << "MUST FAKE KILL " << nm << dendl;
      
      for (unsigned i=0; i<directory.size(); i++) {
	if (directory[i] && directory[i]->get_myname() == nm) {
	  dout(0) << "FAKING FAILURE of " << nm << " at " << directory[i]->get_myaddr() << dendl;
	  directory[i]->failed = true;
	  directory[i] = 0;
	  num_entity--;
	  break;
	}
      }
    }

    list<Message*> ls;
    ls.swap(sent_to_failed_queue);
    for (list<Message*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) {
      Message *m = *p;
      FakeMessenger *mgr = 0;
      Dispatcher *dis = 0;

      unsigned drank = m->get_source_addr().erank;
      if (drank < directory.size() && directory[drank]) {
	mgr = directory[drank];
	if (mgr) 
	  dis = mgr->get_dispatcher();
      }
      if (dis) {
	dout(1) << "fail on " << *m 
		<< " to " << m->get_dest() << " from " << m->get_source()
		<< ", passing back to sender." << dendl;
	dis->ms_handle_failure(m, m->get_dest_inst());
      } else {
	dout(1) << "fail on " << *m
		<< " to " << m->get_dest() << " from " << m->get_source()
		<< ", sender gone, dropping." << dendl;
	delete m;
      }
    }

    // messages
    for (unsigned i=0; i<directory.size(); i++) {
      FakeMessenger *mgr = directory[i];
      if (!mgr) continue;

      dout(18) << "messenger " << mgr << " at " << mgr->get_myname() << " has " << mgr->num_incoming() << " queued" << dendl;

      if (!mgr->is_ready()) {
        dout(18) << "messenger " << mgr << " at " << mgr->get_myname() << " has no dispatcher, skipping" << dendl;
        continue;
      }

      Message *m = mgr->get_message();
      
      if (m) {
        //dout(18) << "got " << m << dendl;
        dout(1) << "==== " << m->get_dest() 
		<< " <- " << m->get_source()
                << " ==== " << *m 
		<< " ---- " << m 
                << dendl;
        
        if (g_conf.fakemessenger_serialize) {
          // encode
          if (m->empty_payload()) 
            m->encode_payload();
          ceph_msg_header head = m->get_header();
          ceph_msg_footer foot = m->get_footer();
          bufferlist front;
          front.claim( m->get_payload() );
	  bufferlist data;
	  data.claim( m->get_data() );
          //bl.c_str();   // condense into 1 buffer

          delete m;
          
          // decode
          m = decode_message(head, foot, front, data);
          assert(m);
        } 

	m->set_recv_stamp(g_clock.now());
        
        didone = true;

        lock.Unlock();
        mgr->dispatch(m);
        lock.Lock();
      }
    }
    
    // deal with shutdowns.. delayed to avoid concurrent directory modification
    if (!shutdown_set.empty()) {
      for (set<entity_addr_t>::iterator it = shutdown_set.begin();
           it != shutdown_set.end();
           it++) {
        dout(7) << "fakemessenger: removing " << *it << " from directory" << dendl;
	int r = it->erank;
        assert(directory[r]);
        directory[r] = 0;
	num_entity--;
        if (num_entity == 0) {
          dout(1) << "fakemessenger: last shutdown" << dendl;
          ::fm_shutdown = true;
        }
      }
      shutdown_set.clear();
    }
    
    if (!didone)
      break;
  }


  dout(18) << "do_loop end (no more messages)." << dendl;
  //lock.Unlock();
  return 0;
}


FakeMessenger::FakeMessenger(entity_name_t me)  : Messenger(me)
{
  failed = false;

  lock.Lock();
  {
    // assign rank
    unsigned r = directory.size();
    _myinst.name = me;
    _myinst.addr.set_port(0);
    _myinst.addr.erank = r;
    _myinst.addr.nonce = getpid();

    // add to directory
    directory.push_back(this);
    assert(directory.size() == r+1);

    num_entity++;
    
    // put myself in the fail queue?
    if (g_fake_kill_after.count(me)) {
      utime_t w = start_time;
      w += g_fake_kill_after[me];
      dout(0) << "will fake failure of " << me << " at " << w << dendl;
      fail_queue[w] = me;
    }
  }
  lock.Unlock();


  dout(0) << "fakemessenger " << get_myname() << " messenger is " << this 
	  << " at " << get_myaddr() << dendl;

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
  dout(2) << "shutdown on messenger " << this << " has " << num_incoming() << " queued" << dendl;
  lock.Lock();
  assert(directory[_myinst.addr.erank] == this);
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
  dout(1) << "reset_myname from " << get_myname() << " to " << m << dendl;
  _myinst.name = m;

  // put myself in the fail queue?
  if (g_fake_kill_after.count(m)) {
    utime_t w = start_time;
    w += g_fake_kill_after[m];
    dout(0) << "will fake failure of " << m << " at " << w << dendl;
    fail_queue[w] = m;
  }

}


int FakeMessenger::send_message(Message *m, entity_inst_t inst)
{
  m->set_source_inst(_myinst);
  m->set_orig_source_inst(_myinst);
  m->set_dest_inst(inst);
  return submit_message(m, inst);
}

int FakeMessenger::forward_message(Message *m, entity_inst_t inst)
{
  m->set_source_inst(_myinst);
  m->set_dest_inst(inst);
  return submit_message(m, inst);
}

int FakeMessenger::submit_message(Message *m, entity_inst_t inst)
{
  entity_name_t dest = inst.name;

  lock.Lock();

#ifdef LOG_MESSAGES
  // stats
  loggers[get_myaddr()]->inc("+send",1);
  loggers[dest]->inc("-recv",1);
  
  char s[20];
  snprintf(s, sizeof(s), "+%s", m->get_type_name());
  loggers[get_myaddr()]->inc(s);
  snprintf(s, sizeof(s), ,"-%s", m->get_type_name());
  loggers[dest]->inc(s);
#endif

  // queue
  unsigned drank = inst.addr.erank;
  if (drank < directory.size() && directory[drank] &&
      shutdown_set.count(inst.addr) == 0) {
    dout(1) << "--> " << get_myname() << " -> " << inst.name << " --- " << *m << " -- " << m
	    << dendl;
    directory[drank]->queue_incoming(m);
  } else {
    dout(0) << "--> " << get_myname() << " -> " << inst.name << " " << *m << " -- " << m
	    << " *** destination " << inst.addr << " DNE ***" 
	    << dendl;

    // do the failure callback
    sent_to_failed_queue.push_back(m);
  }

  // wake up loop?
  if (!awake) {
    dout(10) << "waking up fakemessenger thread" << dendl; 
    cond.Signal();
    lock.Unlock();
  } else
    lock.Unlock();
  
  return 0;
}


