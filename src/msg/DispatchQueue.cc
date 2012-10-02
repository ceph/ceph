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


/**************************************
 * IncomingQueue
 */

#undef dout_prefix
#define dout_prefix *_dout << "incomingqueue(" << this << " " << parent << ")."

void IncomingQueue::queue(Message *m, int priority, bool hold_dq_lock)
{
  Mutex::Locker l(lock);
  ldout(cct,20) << "queue " << m << " prio " << priority << dendl;
  if (in_q.count(priority) == 0) {
    // queue inq AND message under inq AND dispatch_queue locks.
    if (!hold_dq_lock) {
      lock.Unlock();
      dq->lock.Lock();
      lock.Lock();
    } else {
      assert(dq->lock.is_locked());
    }

    if (halt) {
      if (!hold_dq_lock) {
	dq->lock.Unlock();
      } else {
	assert(dq->lock.is_locked());
      }
      goto halt;
    }

    list<Message *>& queue = in_q[priority];
    if (queue.empty()) {
      ldout(cct,20) << "queue " << m << " under newly queued queue" << dendl;
      if (!queue_items.count(priority))
	queue_items[priority] = new xlist<IncomingQueue *>::item(this);
      if (dq->queued_pipes.empty())
	dq->cond.Signal();

      map<int, xlist<IncomingQueue*>*>::iterator p = dq->queued_pipes.find(priority);
      xlist<IncomingQueue*> *qlist;
      if (p != dq->queued_pipes.end())
	qlist = p->second;
      else {
	qlist = new xlist<IncomingQueue*>;
	dq->queued_pipes[priority] = qlist;
      }
      qlist->push_back(queue_items[priority]);
      get();  // dq now has a ref
    }

    queue.push_back(m);

    if (!hold_dq_lock) {
      dq->lock.Unlock();
    } else {
      assert(dq->lock.is_locked());
    }
  } else {
    ldout(cct,20) << "queue " << m << " under existing queue" << dendl;
    // just queue message under our lock.
    list<Message *>& queue = in_q[priority];
    queue.push_back(m);
  }
  
  // increment queue length counters
  in_qlen++;
  dq->qlen.inc();
  return;

 halt:
  ldout(cct, 20) << "queue " << m << " halt, discarding" << dendl;
  // don't want to put local-delivery signals
  if (m>(void *)DispatchQueue::D_NUM_CODES) {
    msgr->dispatch_throttle_release(m->get_dispatch_throttle_size());
    m->put();
  }
}

void IncomingQueue::discard_queue()
{
  halt = true;

  // dequeue ourselves
  dq->lock.Lock();
  lock.Lock();

  for (map<int, xlist<IncomingQueue *>::item* >::iterator i = queue_items.begin();
       i != queue_items.end();
       ++i) {
    xlist<IncomingQueue *>* list_on;
    if ((list_on = i->second->get_list())) { //if in round-robin
      i->second->remove_myself(); //take off
      put();  // dq loses its ref
      if (list_on->empty()) { //if round-robin queue is empty
	delete list_on;
	dq->queued_pipes.erase(i->first); //remove from map
      }
    }
  }
  dq->lock.Unlock();

  while (!queue_items.empty()) {
    delete queue_items.begin()->second;
    queue_items.erase(queue_items.begin());
  }

  // adjust qlen
  dq->qlen.sub(in_qlen);

  for (map<int,list<Message*> >::iterator p = in_q.begin(); p != in_q.end(); p++)
    for (list<Message*>::iterator r = p->second.begin(); r != p->second.end(); r++) {
      if (*r < (void *) DispatchQueue::D_NUM_CODES) {
        continue; // skip non-Message dispatch codes
      }
      dq->msgr->dispatch_throttle_release((*r)->get_dispatch_throttle_size());
      ldout(cct,20) << "  discard " << *r << dendl;
      (*r)->put();
    }
  in_q.clear();
  in_qlen = 0;

  lock.Unlock();
}

void IncomingQueue::restart_queue()
{
  halt = false;
}



/*******************
 * DispatchQueue
 */

#undef dout_prefix
#define dout_prefix *_dout << "-- " << msgr->get_myaddr() << " "


void DispatchQueue::local_delivery(Message *m, int priority)
{
  m->set_connection(msgr->local_connection->get());
  local_queue.queue(m, priority);
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
  while (!stop) {
    while (!queued_pipes.empty() && !stop) {
      //get highest-priority pipe
      map<int, xlist<IncomingQueue *>* >::reverse_iterator high_iter =
	queued_pipes.rbegin();
      int priority = high_iter->first;
      xlist<IncomingQueue *> *qlist = high_iter->second;

      IncomingQueue *inq = qlist->front();
      //move pipe to back of line -- or just take off if no more messages
      inq->lock.Lock();
      list<Message *>& m_queue = inq->in_q[priority];
      Message *m = m_queue.front();
      m_queue.pop_front();

      bool dequeued = false;
      if (m_queue.empty()) {
	qlist->pop_front();  // pipe is done
	dequeued = true;     // must drop dq's ref below
	if (qlist->empty()) {
	  delete qlist;
	  queued_pipes.erase(priority);
	}
	inq->in_q.erase(priority);
	ldout(cct,20) << "dispatch_entry inq " << inq << " parent " << inq->parent << " dequeued " << m
		      << ", dequeued queue" << dendl;
      } else {
	ldout(cct,20) << "dispatch_entry inq " << inq << " parent " << inq->parent << " dequeued " << m
		      << ", moved to end of list" << dendl;
	qlist->push_back(inq->queue_items[priority]);  // move to end of list
      }

      Connection *con = NULL;
      if ((unsigned long)m < DispatchQueue::D_NUM_CODES) {
	assert(inq == &local_queue);
	con = con_q.front();
	con_q.pop_front();
      }

      lock.Unlock();

      inq->in_qlen--;
      qlen.dec();

      inq->lock.Unlock();
      if (dequeued)
	inq->put();

      if ((unsigned long)m == DispatchQueue::D_BAD_REMOTE_RESET) {
	msgr->ms_deliver_handle_remote_reset(con);
	con->put();
      } else if ((unsigned long)m == DispatchQueue::D_CONNECT) {
	msgr->ms_deliver_handle_connect(con);
	con->put();
      } else if ((unsigned long)m == DispatchQueue::D_ACCEPT) {
	msgr->ms_deliver_handle_accept(con);
	con->put();
      } else if ((unsigned long)m == DispatchQueue::D_BAD_RESET) {
	msgr->ms_deliver_handle_reset(con);
	con->put();
      } else {
	uint64_t msize = m->get_dispatch_throttle_size();
	m->set_dispatch_throttle_size(0);  // clear it out, in case we requeue this message.

	ldout(cct,1) << "<== " << m->get_source_inst()
		     << " " << m->get_seq()
		     << " ==== " << *m
		     << " ==== " << m->get_payload().length() << "+" << m->get_middle().length()
		     << "+" << m->get_data().length()
		     << " (" << m->get_footer().front_crc << " " << m->get_footer().middle_crc
		     << " " << m->get_footer().data_crc << ")"
		     << " " << m << " con " << m->get_connection()
		     << dendl;
	msgr->ms_deliver_dispatch(m);

	msgr->dispatch_throttle_release(msize);

	ldout(cct,20) << "done calling dispatch on " << m << dendl;
      }

      lock.Lock();
    }
    if (!stop)
      cond.Wait(lock); //wait for something to be put on queue
  }
  lock.Unlock();
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
  if (dispatch_thread.am_self()) {
    ldout(cct,10) << "shutdown i am dispatch, setting stop flag" << dendl;
    stop = true;
  } else {
    ldout(cct,10) << "shutdown i am not dispatch, setting stop flag" << dendl;
    lock.Lock();
    stop = true;
    cond.Signal();
    lock.Unlock();
  }
}
