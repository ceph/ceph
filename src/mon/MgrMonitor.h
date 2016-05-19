// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "MgrMap.h"
#include "PaxosService.h"

class MgrMonitor : public PaxosService
{
  MgrMap map;
  MgrMap pending_map;

public:
  MgrMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name)
  {}

  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void create_pending();
  void encode_pending(MonitorDBStore::TransactionRef t);
  bool preprocess_query(MonOpRequestRef op);
  bool prepare_update(MonOpRequestRef op);
  void encode_full(MonitorDBStore::TransactionRef t) { }

  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  void check_sub(Subscription *sub);
  void check_subs();
  void send_digests();

  void tick();

  friend class C_Updated;
};

