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



#include <ext/rope>
#include "include/types.h"

#include "Message.h"
#include "Messenger.h"
#include "messages/MGenericMessage.h"

#include <cassert>
#include <iostream>
using namespace std;


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "messenger: "
#define DEBUGLVL  10    // debug level of output



// -------- 
// callbacks

Mutex                msgr_callback_lock;
list<Context*>       msgr_callback_queue;
//Context*             msgr_callback_kicker = 0;

void Messenger::queue_callback(Context *c) {
  msgr_callback_lock.Lock();
  msgr_callback_queue.push_back(c);
  msgr_callback_lock.Unlock();

  callback_kick();
}
void Messenger::queue_callbacks(list<Context*>& ls) {
  msgr_callback_lock.Lock();
  msgr_callback_queue.splice(msgr_callback_queue.end(), ls);
  msgr_callback_lock.Unlock();

  callback_kick();
}

void Messenger::do_callbacks() {
  // take list
  msgr_callback_lock.Lock();
  list<Context*> ls;
  ls.splice(ls.begin(), msgr_callback_queue);
  msgr_callback_lock.Unlock();

  // do them
  for (list<Context*>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    dout(10) << "--- doing callback " << *it << endl;
    (*it)->finish(0);
    delete *it;
  }
}

// ---------
// incoming messages

void Messenger::dispatch(Message *m) 
{
  assert(dispatcher);
  dispatcher->dispatch(m);
}



