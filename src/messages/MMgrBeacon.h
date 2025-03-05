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

#ifndef CEPH_MMGRBEACON_H
#define CEPH_MMGRBEACON_H

#include "messages/PaxosServiceMessage.h"
#include "mon/MonCommand.h"
#include "mon/MgrMap.h"

#include "include/types.h"


class MMgrBeacon final : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 11;
  static constexpr int COMPAT_VERSION = 8;

protected:
  uint64_t gid;
  entity_addrvec_t server_addrs;
  bool available;
  std::string name;
  uuid_d fsid;

  // From active daemon to populate MgrMap::services
  std::map<std::string, std::string> services;

  // Only populated during activation
  std::vector<MonCommand> command_descs;

  // Information about the modules found locally on this daemon
  std::vector<MgrMap::ModuleInfo> modules;

  std::map<std::string,std::string> metadata; ///< misc metadata about this osd

  std::multimap<std::string, entity_addrvec_t> clients;

  uint64_t mgr_features = 0;   ///< reporting mgr's features

public:
  MMgrBeacon()
    : PaxosServiceMessage{MSG_MGR_BEACON, 0, HEAD_VERSION, COMPAT_VERSION},
      gid(0), available(false)
  {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

  MMgrBeacon(const uuid_d& fsid_, uint64_t gid_, const std::string &name_,
             entity_addrvec_t server_addrs_, bool available_,
	     std::vector<MgrMap::ModuleInfo>&& modules_,
	     std::map<std::string,std::string>&& metadata_,
             std::multimap<std::string, entity_addrvec_t>&& clients_,
	     uint64_t feat)
    : PaxosServiceMessage{MSG_MGR_BEACON, 0, HEAD_VERSION, COMPAT_VERSION},
      gid(gid_), server_addrs(server_addrs_), available(available_), name(name_),
      fsid(fsid_), modules(std::move(modules_)), metadata(std::move(metadata_)),
      clients(std::move(clients_)),
      mgr_features(feat)
  {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

  uint64_t get_gid() const { return gid; }
  entity_addrvec_t get_server_addrs() const { return server_addrs; }
  bool get_available() const { return available; }
  const std::string& get_name() const { return name; }
  const uuid_d& get_fsid() const { return fsid; }
  const std::map<std::string,std::string>& get_metadata() const {
    return metadata;
  }
  const std::map<std::string,std::string>& get_services() const {
    return services;
  }
  uint64_t get_mgr_features() const { return mgr_features; }

  void set_services(const std::map<std::string, std::string> &svcs)
  {
    services = svcs;
  }

  void set_command_descs(const std::vector<MonCommand> &cmds)
  {
    command_descs = cmds;
  }

  const std::vector<MonCommand> &get_command_descs()
  {
    return command_descs;
  }

  const std::vector<MgrMap::ModuleInfo> &get_available_modules() const
  {
    return modules;
  }

  const auto& get_clients() const
  {
    return clients;
  }

private:
  ~MMgrBeacon() final {}

public:

  std::string_view get_type_name() const override { return "mgrbeacon"; }

  void print(std::ostream& out) const override {
    out << get_type_name() << " mgr." << name << "(" << fsid << ","
	<< gid << ", " << server_addrs << ", " << available
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    using ceph::encode;
    paxos_encode();

    assert(HAVE_FEATURE(features, SERVER_NAUTILUS));
    encode(server_addrs, payload, features);
    encode(gid, payload);
    encode(available, payload);
    encode(name, payload);
    encode(fsid, payload);

    // Fill out old-style list of module names (deprecated by
    // later field of full ModuleInfos)
    std::set<std::string> module_names;
    for (const auto &info : modules) {
      module_names.insert(info.name);
    }
    encode(module_names, payload);

    encode(command_descs, payload);
    encode(metadata, payload);
    encode(services, payload);

    encode(modules, payload);
    encode(mgr_features, payload);

    std::vector<std::string> clients_names;
    std::vector<entity_addrvec_t> clients_addrs;
    for (const auto& i : clients) {
      clients_names.push_back(i.first);
      clients_addrs.push_back(i.second);
    }
    // The address vector needs to be encoded first to produce a
    // backwards compatible messsage for older monitors.
    encode(clients_addrs, payload, features);
    encode(clients_names, payload, features);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    assert(header.version >= 8);
    decode(server_addrs, p);  // entity_addr_t for version < 8
    decode(gid, p);
    decode(available, p);
    decode(name, p);
    decode(fsid, p);
    std::set<std::string> module_name_list;
    decode(module_name_list, p);
    decode(command_descs, p);
    decode(metadata, p);
    decode(services, p);
    decode(modules, p);
    if (header.version >= 9) {
      decode(mgr_features, p);
    }
    if (header.version >= 10) {
      std::vector<entity_addrvec_t> clients_addrs;
      decode(clients_addrs, p);
      clients.clear();
      if (header.version >= 11) {
	std::vector<std::string> clients_names;
	decode(clients_names, p);
	if (clients_names.size() != clients_addrs.size()) {
	  throw ceph::buffer::malformed_input(
	    "clients_names.size() != clients_addrs.size()");
	}
	auto cn = clients_names.begin();
	auto ca = clients_addrs.begin();
	for(; cn != clients_names.end(); ++cn, ++ca) {
	  clients.emplace(*cn, *ca);
	}
      } else {
	for (const auto& i : clients_addrs) {
	  clients.emplace("", i);
	}
      }
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};


#endif
