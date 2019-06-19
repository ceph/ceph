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


#ifndef CEPH_ELECTIONLOGIC_H
#define CEPH_ELECTIONLOGIC_H

#include <map>
#include "include/types.h"

class ElectionOwner {
public:
  virtual void persist_epoch(epoch_t e) = 0;
  virtual epoch_t read_persisted_epoch() = 0;
  virtual void validate_store() = 0;
  virtual void notify_bump_epoch() = 0;
  virtual void trigger_new_election() = 0;
  virtual int get_my_rank() = 0;
  virtual void propose_to_peers(epoch_t e) = 0;
  virtual void reset_election() = 0;
  virtual bool ever_participated() = 0;
  virtual unsigned paxos_size() = 0;
  virtual void _start() = 0;
  virtual void _defer_to(int who) = 0;
  virtual void message_victory(const set<int>& quorum) = 0;
  virtual bool is_current_member(int rank) = 0;
  virtual ~ElectionOwner() {}
};

class ElectionLogic {
public:
  ElectionOwner *elector;
  CephContext *cct;
  epoch_t epoch = 0;
  bool participating;
  bool electing_me;
  set<int> acked_me;
  int leader_acked;

  ElectionLogic(ElectionOwner *e, CephContext *c) : elector(e), cct(c),
						    participating(true),
						    electing_me(false), leader_acked(-1) {}
  void declare_standalone_victory();
  void start();
  void defer(int who);
  void end_election_period();
  void receive_propose(epoch_t mepoch, int from);
  void receive_ack(int from, epoch_t from_epoch);
  bool receive_victory_claim(int from, epoch_t from_epoch);

  
private:
  void init();
  void bump_epoch(epoch_t e);
  void declare_victory();
};

#endif
