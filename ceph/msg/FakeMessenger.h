// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#ifndef __FAKEMESSENGER_H
#define __FAKEMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#include <list>
#include <map>

class Timer;

class FakeMessenger : public Messenger {
 protected:
  int whoami;

  class Logger *logger;

  list<Message*>       incoming;        // incoming queue

 public:
  FakeMessenger(long me);
  ~FakeMessenger();

  virtual int shutdown();

  // msg interface
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
  
  // events
  //virtual void trigger_timer(Timer *t);


  // -- incoming queue --
  // (that nothing uses)
  Message *get_message() {
	if (!incoming.empty()) {
	  Message *m = incoming.front();
	  incoming.pop_front();
	  return m;
	}
	return NULL;
  }
  bool queue_incoming(Message *m) {
	incoming.push_back(m);
	return true;
  }
  int num_incoming() {
	return incoming.size();
  }

};

int fakemessenger_do_loop();
int fakemessenger_do_loop_2();
void fakemessenger_startthread();
void fakemessenger_stopthread();
void fakemessenger_wait();

#endif
