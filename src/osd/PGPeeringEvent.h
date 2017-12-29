// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/event.hpp>

class PGPeeringEvent {
  epoch_t epoch_sent;
  epoch_t epoch_requested;
  boost::intrusive_ptr< const boost::statechart::event_base > evt;
  string desc;
public:
  MEMPOOL_CLASS_HELPERS();
  template <class T>
  PGPeeringEvent(
    epoch_t epoch_sent,
    epoch_t epoch_requested,
    const T &evt_)
    : epoch_sent(epoch_sent),
      epoch_requested(epoch_requested),
      evt(evt_.intrusive_from_this()) {
    stringstream out;
    out << "epoch_sent: " << epoch_sent
	<< " epoch_requested: " << epoch_requested << " ";
    evt_.print(&out);
    desc = out.str();
  }
  epoch_t get_epoch_sent() {
    return epoch_sent;
  }
  epoch_t get_epoch_requested() {
    return epoch_requested;
  }
  const boost::statechart::event_base &get_event() {
    return *evt;
  }
  const string& get_desc() {
    return desc;
  }
};
typedef ceph::shared_ptr<PGPeeringEvent> PGPeeringEventRef;
