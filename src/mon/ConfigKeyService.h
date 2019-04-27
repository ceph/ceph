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

#include "mon/QuorumService.h"
#include "mon/MonitorDBStore.h"

class Paxos;
class Monitor;

class ConfigKeyService : public QuorumService
{
  Paxos *paxos;

  int store_get(const string &key, bufferlist &bl);
  void store_put(const string &key, bufferlist &bl, Context *cb = NULL);
  void store_delete(MonitorDBStore::TransactionRef t, const string &key);
  void store_delete(const string &key, Context *cb = NULL);
  void store_delete_prefix(
      MonitorDBStore::TransactionRef t,
      const string &prefix);
  void store_list(stringstream &ss);
  void store_dump(stringstream &ss, const string& prefix);
  bool store_exists(const string &key);
  bool store_has_prefix(const string &prefix);

  static const string STORE_PREFIX;

protected:
  void service_shutdown() override { }

public:
  ConfigKeyService(Monitor *m, Paxos *p) :
    QuorumService(m),
    paxos(p)
  { }
  ~ConfigKeyService() override { }


  /**
   * @defgroup ConfigKeyService_Inherited_h Inherited abstract methods
   * @{
   */
  void init() override { }
  bool service_dispatch(MonOpRequestRef op) override;

  void start_epoch() override { }
  void finish_epoch() override { }
  void cleanup() override { }
  void service_tick() override { }

  int validate_osd_destroy(const int32_t id, const uuid_d& uuid);
  void do_osd_destroy(int32_t id, uuid_d& uuid);
  int validate_osd_new(
      const uuid_d& uuid,
      const string& dmcrypt_key,
      stringstream& ss);
  void do_osd_new(const uuid_d& uuid, const string& dmcrypt_key);

  int get_type() override {
    return QuorumService::SERVICE_CONFIG_KEY;
  }

  string get_name() const override {
    return "config_key";
  }
  void get_store_prefixes(set<string>& s) const;
  /**
   * @} // ConfigKeyService_Inherited_h
   */
};

#endif // CEPH_MON_CONFIG_KEY_SERVICE_H
