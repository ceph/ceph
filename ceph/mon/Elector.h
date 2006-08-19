// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __MON_ELECTOR_H
#define __MON_ELECTOR_H

#include <map>
using namespace std;

#include "include/types.h"
#include "msg/Message.h"


class Monitor;


class Elector {
 public:

  //// sub-classes

  // Epoch
  class Epoch {
  public:
    int p_id;
    int s_num;
	
    Epoch(int p_id=0, int s_num=0) {
      this->p_id = p_id;
      this->s_num = s_num;
    }
  };	


  // State
  class State {
  public:
    Epoch epoch;
    int freshness;

	State() {};
    State (Epoch& e, int f) :
	  epoch(e), freshness(f) {}
  };


  class View {
  public:
    State state;
    bool expired;
	View() : expired(false) {}
    View(State& s, bool e) : state(s), expired(e) {}
  };


  ///////////////
 private:
  Monitor *mon;
  int whoami;
  Mutex lock;

  // used during refresh phase
  int ack_msg_count;
  int refresh_num;
  
  // used during read phase
  int read_num;
  int status_msg_count;
  
  // the leader process id
  int leader_id;
  // f-accessible
  int f;
  
  // the processes that compose the group
  //   vector<int> processes;
  // parameters for the process
  int main_delta;
  int trip_delta;
  
  // state variables
  map<int, State> registry;
  map<int, View>  views;
  map<int, View>  old_views;

  // get the minimum epoch in the view map
  Epoch get_min_epoch();
  
  // handlers for election messages
  void handle_ack(class MMonElectionAck *m);
  void handle_collect(class MMonElectionCollect *m);
  void handle_refresh(class MMonElectionRefresh *m);
  void handle_status(class MMonElectionStatus *m);

 public:  
  Elector(Monitor *m, int w) : mon(m), whoami(w) {
	// initialize all those values!
	// ...
  }

  // timer methods
  void read_timer();
  void trip_timer();
  void refresh_timer();
  
  void dispatch(Message *m);

};


inline bool operator>(const Elector::Epoch& l, const Elector::Epoch& r) {
  if (l.s_num == r.s_num)
	return (l.p_id > r.p_id);
  else
	return (l.s_num > r.s_num);
}

inline bool operator<(const Elector::Epoch& l, const Elector::Epoch& r) {
  if (l.s_num == r.s_num)
	return (l.p_id < r.p_id);
  else
	return (l.s_num < r.s_num);
}

inline bool operator==(const Elector::Epoch& l, const Elector::Epoch& r) {
  return ((l.s_num == r.s_num) && (l.p_id > r.p_id));
}

  
inline bool operator>(const Elector::State& l, const Elector::State& r) 
{
  if (l.epoch == r.epoch)
	return (l.freshness > r.freshness);
  else
	return l.epoch > r.epoch;
}
 
inline bool operator<(const Elector::State& l, const Elector::State& r) 
{
  if (l.epoch == r.epoch)
	return (l.freshness < r.freshness);
  else
	return l.epoch < r.epoch;
}
 
inline bool operator==(const Elector::State& l, const Elector::State& r) 
{
  return ( (l.epoch == r.epoch) && (l.freshness == r.freshness) );
}


#endif
