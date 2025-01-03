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

#ifndef CEPH_AUTHMONITOR_H
#define CEPH_AUTHMONITOR_H

#include <map>
#include <set>

#include "global/global_init.h"
#include "include/ceph_features.h"
#include "include/types.h"
#include "mon/PaxosService.h"
#include "mon/MonitorDBStore.h"

class MAuth;
class KeyRing;
class Monitor;

#define MIN_GLOBAL_ID 0x1000

class AuthMonitor : public PaxosService {

public:
  typedef enum {
    CAPS_UPDATE_NOT_REQD, CAPS_UPDATE_REQD, CAPS_PARSING_ERR
  } caps_update;

  enum IncType {
    GLOBAL_ID,
    AUTH_DATA,
  };
  struct Incremental {
    IncType inc_type;
    uint64_t max_global_id;
    uint32_t auth_type;
    ceph::buffer::list auth_data;

    Incremental() : inc_type(GLOBAL_ID), max_global_id(0), auth_type(0) {}

    void encode(ceph::buffer::list& bl, uint64_t features=-1) const {
      using ceph::encode;
      ENCODE_START(2, 2, bl);
      __u32 _type = (__u32)inc_type;
      encode(_type, bl);
      if (_type == GLOBAL_ID) {
	encode(max_global_id, bl);
      } else {
	encode(auth_type, bl);
	encode(auth_data, bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
      __u32 _type;
      decode(_type, bl);
      inc_type = (IncType)_type;
      ceph_assert(inc_type >= GLOBAL_ID && inc_type <= AUTH_DATA);
      if (_type == GLOBAL_ID) {
	decode(max_global_id, bl);
      } else {
	decode(auth_type, bl);
	decode(auth_data, bl);
      }
      DECODE_FINISH(bl);
    }
    void dump(ceph::Formatter *f) const {
      f->dump_int("type", inc_type);
      f->dump_int("max_global_id", max_global_id);
      f->dump_int("auth_type", auth_type);
      f->dump_int("auth_data_len", auth_data.length());
    }
    static void generate_test_instances(std::list<Incremental*>& ls) {
      ls.push_back(new Incremental);
      ls.push_back(new Incremental);
      ls.back()->inc_type = GLOBAL_ID;
      ls.back()->max_global_id = 1234;
      ls.push_back(new Incremental);
      ls.back()->inc_type = AUTH_DATA;
      ls.back()->auth_type = 12;
      ls.back()->auth_data.append("foo");
    }
  };

  struct auth_entity_t {
    EntityName name;
    EntityAuth auth;
  };


private:
  std::vector<Incremental> pending_auth;
  uint64_t max_global_id;
  uint64_t last_allocated_id;

  // these are protected by mon->auth_lock
  int mon_num = 0, mon_rank = 0;

  bool _upgrade_format_to_dumpling();
  bool _upgrade_format_to_luminous();
  bool _upgrade_format_to_mimic();
  void upgrade_format() override;

  void export_keyring(KeyRing& keyring);
  int import_keyring(KeyRing& keyring);

  void push_cephx_inc(KeyServerData::Incremental& auth_inc) {
    Incremental inc;
    inc.inc_type = AUTH_DATA;
    encode(auth_inc, inc.auth_data);
    inc.auth_type = CEPH_AUTH_CEPHX;
    pending_auth.push_back(inc);
  }

  template<typename CAP_ENTITY_CLASS>
  bool _was_parsing_fine(const std::string& entity, const std::string& caps,
    std::ostream* out);
  /* validate mon/osd/mgr/mds caps; fail on unrecognized service/type */
  bool valid_caps(const std::string& entity, const std::string& caps,
		  std::ostream *out);
  bool valid_caps(const std::string& type, const ceph::buffer::list& bl, std::ostream *out) {
    auto p = bl.begin();
    std::string v;
    try {
      using ceph::decode;
      decode(v, p);
    } catch (ceph::buffer::error& e) {
      *out << "corrupt capability encoding";
      return false;
    }
    return valid_caps(type, v, out);
  }
  bool valid_caps(const std::map<std::string, std::string>& caps,
		  std::ostream *out);

  void on_active() override;
  bool should_propose(double& delay) override;
  void get_initial_keyring(KeyRing *keyring);
  void create_initial_keys(KeyRing *keyring);
  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;  // prepare a new pending
  bool prepare_global_id(MonOpRequestRef op);
  bool _should_increase_max_global_id(); ///< called under mon->auth_lock
  void increase_max_global_id();
  uint64_t assign_global_id(bool should_increase_max);
public:
  uint64_t _assign_global_id(); ///< called under mon->auth_lock
  void _set_mon_num_rank(int num, int rank); ///< called under mon->auth_lock

private:
  bool prepare_used_pending_keys(MonOpRequestRef op);

  // propose pending update to peers
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  void encode_full(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  bool preprocess_query(MonOpRequestRef op) override;  // true if processed.
  bool prepare_update(MonOpRequestRef op) override;

  bool prep_auth(MonOpRequestRef op, bool paxos_writable);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void _encode_keyring(KeyRing& kr, const EntityName& entity,
    bufferlist& rdata, Formatter* fmtr,
    std::map<std::string, bufferlist>* wanted_caps=nullptr);
  void _encode_auth(const EntityName& entity, const EntityAuth& eauth,
    bufferlist& rdata, Formatter* fmtr, bool pending_key=false,
    std::map<std::string, bufferlist>* caps=nullptr);
  void _encode_key(const EntityName& entity, const EntityAuth& eauth,
    bufferlist& rdata, Formatter* fmtr, bool pending_key=false,
    std::map<std::string, bufferlist>* caps=nullptr);

  int _check_and_encode_caps(const std::map<std::string, std::string>& caps,
    std::map<std::string, bufferlist>& encoded_caps, std::stringstream& ss);

  int _update_or_create_entity(const EntityName& entity,
    const std::map<std::string, std::string>& caps, MonOpRequestRef op,
    std::stringstream& ss, std::stringstream& ds, bufferlist* rdata=nullptr,
    Formatter* fmtr=nullptr, bool create_entity=false);
  int _create_entity(const EntityName& entity,
    const std::map<std::string, std::string>& caps, MonOpRequestRef op,
    std::stringstream& ss, std::stringstream& ds, bufferlist* rdata,
    Formatter* fmtr);
  int _update_caps(const EntityName& entity,
    const std::map<std::string, std::string>& caps, MonOpRequestRef op,
    std::stringstream& ss, std::stringstream& ds, bufferlist* rdata,
    Formatter* fmtr);

  caps_update _gen_wanted_caps(EntityAuth& e_auth,
    std::map<std::string, std::string>& newcaps, std::ostream& out);
  template<typename CAP_ENTITY_CLASS>
  caps_update _merge_caps(const std::string& cap_entity,
    const std::string& new_cap_str, const std::string& cur_cap_str,
    std::map<std::string, std::string>& newcaps, std::ostream& out);

  bool check_rotate();
  void process_used_pending_keys(const std::map<EntityName,CryptoKey>& keys);

  bool entity_is_pending(EntityName& entity);
  int exists_and_matches_entity(
      const auth_entity_t& entity,
      bool has_secret,
      std::stringstream& ss);
  int exists_and_matches_entity(
      const EntityName& name,
      const EntityAuth& auth,
      const std::map<std::string,ceph::buffer::list>& caps,
      bool has_secret,
      std::stringstream& ss);
  int remove_entity(const EntityName &entity);
  int add_entity(
      const EntityName& name,
      const EntityAuth& auth);

 public:
  AuthMonitor(Monitor &mn, Paxos &p, const std::string& service_name)
    : PaxosService(mn, p, service_name),
      max_global_id(0),
      last_allocated_id(0)
  {}

  void pre_auth(MAuth *m);

  void tick() override;  // check state, take actions

  int validate_osd_destroy(
      int32_t id,
      const uuid_d& uuid,
      EntityName& cephx_entity,
      EntityName& lockbox_entity,
      std::stringstream& ss);
  void do_osd_destroy(
       const EntityName& cephx_entity,
       const EntityName& lockbox_entity);

  int do_osd_new(
      const auth_entity_t& cephx_entity,
      const auth_entity_t& lockbox_entity,
      bool has_lockbox);
  int validate_osd_new(
      int32_t id,
      const uuid_d& uuid,
      const std::string& cephx_secret,
      const std::string& lockbox_secret,
      auth_entity_t& cephx_entity,
      auth_entity_t& lockbox_entity,
      std::stringstream& ss);

  void dump_info(ceph::Formatter *f);

  bool is_valid_cephx_key(const std::string& k) {
    if (k.empty())
      return false;

    EntityAuth ea;
    try {
      ea.key.decode_base64(k);
      return true;
    } catch (ceph::buffer::error& e) { /* fallthrough */ }
    return false;
  }
};


WRITE_CLASS_ENCODER_FEATURES(AuthMonitor::Incremental)

#endif
