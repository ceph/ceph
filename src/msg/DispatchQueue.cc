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

#include "msg/Message.h"
#include "DispatchQueue.h"
#include "SimpleMessenger.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_ms
#include "common/debug.h"


/*******************
 * DispatchQueue
 */

#undef dout_prefix
#define dout_prefix *_dout << "-- " << msgr->get_myaddr() << " "

double DispatchQueue::get_max_age(utime_t now) {
  Mutex::Locker l(lock);
  if (marrival.empty())
    return 0;
  else
    return (now - marrival.begin()->first);
}

uint64_t DispatchQueue::pre_dispatch(Message *m)
{
  ldout(cct,1) << "<== " << m->get_source_inst()
	       << " " << m->get_seq()
	       << " ==== " << *m
	       << " ==== " << m->get_payload().length()
	       << "+" << m->get_middle().length()
	       << "+" << m->get_data().length()
	       << " (" << m->get_footer().front_crc << " "
	       << m->get_footer().middle_crc
	       << " " << m->get_footer().data_crc << ")"
	       << " " << m << " con " << m->get_connection()
	       << dendl;
  uint64_t msize = m->get_dispatch_throttle_size();
  m->set_dispatch_throttle_size(0); // clear it out, in case we requeue this message.
  return msize;
}

void DispatchQueue::post_dispatch(Message *m, uint64_t msize)
{
  msgr->dispatch_throttle_release(msize);
  ldout(cct,20) << "done calling dispatch on " << m << dendl;
}

void DispatchQueue::enqueue(Message *m, int priority, uint64_t id)
{
  if (msgr->ms_can_fast_dispatch(m)) {
    uint64_t msize = pre_dispatch(m);
    msgr->ms_fast_dispatch(m);
    post_dispatch(m, msize);
  } else {
    Mutex::Locker l(lock);
    ldout(cct,20) << "queue " << m << " prio " << priority << dendl;
    add_arrival(m);
    if (priority >= CEPH_MSG_PRIO_LOW) {
      mqueue.enqueue_strict(
	id, priority, QueueItem(m));
    } else {
      mqueue.enqueue(
	id, priority, m->get_cost(), QueueItem(m));
    }
    cond.Signal();
  }
}

void DispatchQueue::local_delivery(Message *m, int priority)
{
  Mutex::Locker l(lock);
  m->set_connection(msgr->local_connection.get());
  add_arrival(m);
  if (priority >= CEPH_MSG_PRIO_LOW) {
    mqueue.enqueue_strict(
      0, priority, QueueItem(m));
  } else {
    mqueue.enqueue(
      0, priority, m->get_cost(), QueueItem(m));
  }
  cond.Signal();
}

/*
 * This function delivers incoming messages to the Messenger.
 * Pipes with messages are kept in queues; when beginning a message
 * delivery the highest-priority queue is selected, the pipe from the
 * front of the queue is removed, and its message read. If the pipe
 * has remaining messages at that priority level, it is re-placed on to the
 * end of the queue. If the queue is empty; it's removed.
 * The message is then delivered and the process starts again.
 */
void DispatchQueue::entry()
{
  lock.Lock();
  while (true) {
    while (!mqueue.empty()) {
      QueueItem qitem = mqueue.dequeue();
      if (!qitem.is_code())
	remove_arrival(qitem.get_message());
      lock.Unlock();

      if (qitem.is_code()) {
	switch (qitem.get_code()) {
	case D_BAD_REMOTE_RESET:
	  msgr->ms_deliver_handle_remote_reset(qitem.get_connection());
	  break;
	case D_CONNECT:
	  msgr->ms_deliver_handle_connect(qitem.get_connection());
	  break;
	case D_ACCEPT:
	  msgr->ms_deliver_handle_accept(qitem.get_connection());
	  break;
	case D_BAD_RESET:
	  msgr->ms_deliver_handle_reset(qitem.get_connection());
	  break;
	default:
	  assert(0);
	}
      } else {
	Message *m = qitem.get_message();
	if (stop) {
	  ldout(cct,10) << " stop flag set, discarding " << m << " " << *m << dendl;
	  m->put();
	} else {
	  uint64_t msize = pre_dispatch(m);
	  msgr->ms_deliver_dispatch(m);
	  post_dispatch(m, msize);
	}
      }

      lock.Lock();
    }
    if (stop)
      break;

    // wait for something to be put on queue
    cond.Wait(lock);
  }
  lock.Unlock();
}

void DispatchQueue::discard_queue(uint64_t id) {
  Mutex::Locker l(lock);
  list<QueueItem> removed;
  mqueue.remove_by_class(id, &removed);
  for (list<QueueItem>::iterator i = removed.begin();
       i != removed.end();
       ++i) {
    assert(!(i->is_code())); // We don't discard id 0, ever!
    Message *m = i->get_message();
    remove_arrival(m);
    msgr->dispatch_throttle_release(m->get_dispatch_throttle_size());
    m->put();
  }
}

void DispatchQueue::start()
{
  assert(!stop);
  assert(!dispatch_thread.is_started());
  dispatch_thread.create();
}

void DispatchQueue::wait()
{
  dispatch_thread.join();
}

void DispatchQueue::shutdown()
{
  // stop my dispatch thread
  lock.Lock();
  stop = true;
  cond.Signal();
  lock.Unlock();
}
