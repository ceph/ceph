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
#include "Messenger.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_ms
#include "common/debug.h"

using ceph::cref_t;
using ceph::ref_t;

/*******************
 * DispatchQueue
 */

#undef dout_prefix
#define dout_prefix *_dout << "-- " << msgr->get_myaddrs() << " "

double DispatchQueue::get_max_age(utime_t now) const {
  std::lock_guard l{lock};
  if (marrival.empty())
    return 0;
  else
    return (now - marrival.begin()->first);
}

uint64_t DispatchQueue::pre_dispatch(const ref_t<Message>& m)
{
  ldout(cct,1) << "<== " << m->get_source_inst()
	       << " " << m->get_seq()
	       << " ==== " << *m
	       << " ==== " << m->get_payload().length()
	       << "+" << m->get_middle().length()
	       << "+" << m->get_data().length()
	       << " (" << ceph_con_mode_name(m->get_connection()->get_con_mode())
	       << " " << m->get_footer().front_crc << " "
	       << m->get_footer().middle_crc
	       << " " << m->get_footer().data_crc << ")"
	       << " " << m << " con " << m->get_connection()
	       << dendl;
  uint64_t msize = m->get_dispatch_throttle_size();
  m->set_dispatch_throttle_size(0); // clear it out, in case we requeue this message.
  return msize;
}

void DispatchQueue::post_dispatch(const ref_t<Message>& m, uint64_t msize)
{
  dispatch_throttle_release(msize);
  ldout(cct,20) << "done calling dispatch on " << m << dendl;
}

bool DispatchQueue::can_fast_dispatch(const cref_t<Message> &m) const
{
  return msgr->ms_can_fast_dispatch(m);
}

void DispatchQueue::fast_dispatch(const ref_t<Message>& m)
{
  uint64_t msize = pre_dispatch(m);
  msgr->ms_fast_dispatch(m);
  post_dispatch(m, msize);
}

void DispatchQueue::fast_preprocess(const ref_t<Message>& m)
{
  msgr->ms_fast_preprocess(m);
}

void DispatchQueue::enqueue(const ref_t<Message>& m, int priority, uint64_t id)
{
  std::lock_guard l{lock};
  if (stop) {
    return;
  }
  ldout(cct,20) << "queue " << m << " prio " << priority << dendl;
  add_arrival(m);
  if (priority >= CEPH_MSG_PRIO_LOW) {
    mqueue.enqueue_strict(id, priority, QueueItem(m));
  } else {
    mqueue.enqueue(id, priority, m->get_cost(), QueueItem(m));
  }
  cond.notify_all();
}

void DispatchQueue::local_delivery(const ref_t<Message>& m, int priority)
{
  auto local_delivery_stamp = ceph_clock_now();
  m->set_recv_stamp(local_delivery_stamp);
  m->set_throttle_stamp(local_delivery_stamp);
  m->set_recv_complete_stamp(local_delivery_stamp);
  std::lock_guard l{local_delivery_lock};
  if (local_messages.empty())
    local_delivery_cond.notify_all();
  local_messages.emplace(m, priority);
  return;
}

void DispatchQueue::run_local_delivery()
{
  std::unique_lock l{local_delivery_lock};
  while (true) {
    if (stop_local_delivery)
      break;
    if (local_messages.empty()) {
      local_delivery_cond.wait(l);
      continue;
    }
    auto p = std::move(local_messages.front());
    local_messages.pop();
    l.unlock();
    const ref_t<Message>& m = p.first;
    int priority = p.second;
    fast_preprocess(m);
    if (can_fast_dispatch(m)) {
      fast_dispatch(m);
    } else {
      enqueue(m, priority, 0);
    }
    l.lock();
  }
}

void DispatchQueue::dispatch_throttle_release(uint64_t msize)
{
  if (msize) {
    ldout(cct,10) << __func__ << " " << msize << " to dispatch throttler "
	    << dispatch_throttler.get_current() << "/"
	    << dispatch_throttler.get_max() << dendl;
    dispatch_throttler.put(msize);
  }
}

/*
 * This function delivers incoming messages to the Messenger.
 * Connections with messages are kept in queues; when beginning a message
 * delivery the highest-priority queue is selected, the connection from the
 * front of the queue is removed, and its message read. If the connection
 * has remaining messages at that priority level, it is re-placed on to the
 * end of the queue. If the queue is empty; it's removed.
 * The message is then delivered and the process starts again.
 */
void DispatchQueue::entry()
{
  std::unique_lock l{lock};
  while (true) {
    while (!mqueue.empty()) {
      QueueItem qitem = mqueue.dequeue();
      if (!qitem.is_code())
	remove_arrival(qitem.get_message());
      l.unlock();

      if (qitem.is_code()) {
	if (cct->_conf->ms_inject_internal_delays &&
	    cct->_conf->ms_inject_delay_probability &&
	    (rand() % 10000)/10000.0 < cct->_conf->ms_inject_delay_probability) {
	  utime_t t;
	  t.set_from_double(cct->_conf->ms_inject_internal_delays);
	  ldout(cct, 1) << "DispatchQueue::entry  inject delay of " << t
			<< dendl;
	  t.sleep();
	}
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
	case D_CONN_REFUSED:
	  msgr->ms_deliver_handle_refused(qitem.get_connection());
	  break;
	default:
	  ceph_abort();
	}
      } else {
	const ref_t<Message>& m = qitem.get_message();
	if (stop) {
	  ldout(cct,10) << " stop flag set, discarding " << m << " " << *m << dendl;
	} else {
	  uint64_t msize = pre_dispatch(m);
	  msgr->ms_deliver_dispatch(m);
	  post_dispatch(m, msize);
	}
      }

      l.lock();
    }
    if (stop)
      break;

    // wait for something to be put on queue
    cond.wait(l);
  }
}

void DispatchQueue::discard_queue(uint64_t id) {
  std::lock_guard l{lock};
  std::list<QueueItem> removed;
  mqueue.remove_by_class(id, &removed);
  for (auto i = removed.begin(); i != removed.end(); ++i) {
    ceph_assert(!(i->is_code())); // We don't discard id 0, ever!
    const ref_t<Message>& m = i->get_message();
    remove_arrival(m);
    dispatch_throttle_release(m->get_dispatch_throttle_size());
  }
}

void DispatchQueue::start()
{
  ceph_assert(!stop);
  ceph_assert(!dispatch_thread.is_started());
  dispatch_thread.create("ms_dispatch");
  local_delivery_thread.create("ms_local");
}

void DispatchQueue::wait()
{
  local_delivery_thread.join();
  dispatch_thread.join();
}

void DispatchQueue::discard_local()
{
  decltype(local_messages)().swap(local_messages);
}

void DispatchQueue::shutdown()
{
  // stop my local delivery thread
  {
    std::scoped_lock l{local_delivery_lock};
    stop_local_delivery = true;
    local_delivery_cond.notify_all();
  }
  // stop my dispatch thread
  {
    std::scoped_lock l{lock};
    stop = true;
    cond.notify_all();
  }
}
