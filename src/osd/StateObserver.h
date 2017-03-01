// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSD_STATE_OBSERVER_H
#define CEPH_OSD_STATE_OBSERVER_H

#include <set>
#include "include/types.h"


struct OSDStateObserver {
  virtual ~OSDStateObserver() {}
  virtual void on_osd_state(int state, epoch_t epoch) {}
};


class OSDStateNotifier {
private:
  std::set<OSDStateObserver*> observers;
protected:
  void notify_state_observers(int state, epoch_t epoch) {
    for (auto o : observers)
      o->on_osd_state(state, epoch);
  }
public:
  void add_state_observer(OSDStateObserver *o) {
    observers.insert(o);
  }
  void remove_state_observer(OSDStateObserver *o) {
    observers.erase(o);
  }
};

#endif // CEPH_OSD_STATE_OBSERVER_H
