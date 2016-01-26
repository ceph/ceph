// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_MON_CONFIG_KEY_SERVICE_H
#define CEPH_MON_CONFIG_KEY_SERVICE_H

#include "mon/Monitor.h"
#include "mon/QuorumService.h"

#include "messages/MMonHealth.h"

#include "common/config.h"
#include "common/Formatter.h"

class Paxos;

class ConfigKeyService : public QuorumService
{
  Paxos *paxos;

  int store_get(string key, bufferlist &bl);
  void store_put(string key, bufferlist &bl, Context *cb = NULL);
  void store_delete(string key, Context *cb = NULL);
  void store_list(stringstream &ss);
  bool store_exists(string key);

  static const string STORE_PREFIX;

protected:
  virtual void service_shutdown() { }

public:
  ConfigKeyService(Monitor *m, Paxos *p) :
    QuorumService(m),
    paxos(p)
  { }
  virtual ~ConfigKeyService() { }


  /**
   * @defgroup ConfigKeyService_Inherited_h Inherited abstract methods
   * @{
   */
  virtual void init() { }
  virtual void get_health(Formatter *f,
			  list<pair<health_status_t,string> >& summary,
                          list<pair<health_status_t,string> > *detail) { }
  virtual bool service_dispatch(MonOpRequestRef op);

  virtual void start_epoch() { }
  virtual void finish_epoch() { }
  virtual void cleanup() { }
  virtual void service_tick() { }

  virtual int get_type() {
    return QuorumService::SERVICE_CONFIG_KEY;
  }

  virtual string get_name() const {
    return "config_key";
  }
  virtual void get_store_prefixes(set<string>& s);
  /**
   * @} // ConfigKeyService_Inherited_h
   */
};

#endif // CEPH_MON_CONFIG_KEY_SERVICE_H
