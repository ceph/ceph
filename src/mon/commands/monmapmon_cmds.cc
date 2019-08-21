// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC <contact@suse.com> 
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "mon/Monitor.h"
#include "mon/MonmapMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"
#include "mon/commands/monmapmon_cmds.h"

#include "messages/MMonCommand.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"

#define dout_subsys ceph_subsys_mon

bool MonMonStatCommand::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const MonMap &monmap)
{

  ceph_assert(handles_command(prefix));

  mon->monmap->print_summary(ss);
  ss << ", election epoch " << mon->get_epoch() << ", leader "
    << mon->get_leader() << " " << mon->get_leader_name()
    << ", quorum " << mon->get_quorum() << " " << mon->get_quorum_names();
  rdata.append(ss);
  ss.str("");
  reply_with_data(op, 0, ss, rdata, service->get_last_committed());
  return true;
}


bool MonMonGetMap::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const MonMap &monmap)
{

  ceph_assert(handles_command(prefix));

  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());

  epoch_t epoch;
  int64_t epochnum;
  cmd_getval(cct, cmdmap, "epoch", epochnum, (int64_t)0);
  epoch = epochnum;

  MonMap *p = mon->monmap;
  if (epoch) {
    bufferlist bl;
    int r = service->get_version(epoch, bl);
    if (r == -ENOENT) {
      ss << "there is no map for epoch " << epoch;
      reply(op, -ENOENT, ss, service->get_last_committed());
    }
    ceph_assert(r == 0);
    ceph_assert(bl.length() > 0);
    p = new MonMap;
    p->decode(bl);
  }

  ceph_assert(p);

  if (prefix == "mon getmap") {
    p->encode(rdata, m->get_connection()->get_features());
    ss << "got monmap epoch " << p->get_epoch();
  } else if (prefix == "mon dump") {
    stringstream ds;
    if (f) {
      f->open_object_section("monmap");
      p->dump(f.get());
      f->open_array_section("quorum");
      for (set<int>::iterator q = mon->get_quorum().begin();
	  q != mon->get_quorum().end(); ++q) {
	f->dump_int("mon", *q);
      }
      f->close_section();
      f->close_section();
      f->flush(ds);
    } else {
      p->print(ds);
    }
    rdata.append(ds);
    ss << "dumped monmap epoch " << p->get_epoch();
  }
  if (p != mon->monmap) {
    delete p;
    p = nullptr;
  }

  reply_with_data(op, 0, ss, rdata, service->get_last_committed());
  return true;
}

bool MonMonFeatureLs::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const MonMap &monmap)
{

  ceph_assert(handles_command(prefix));

  bool list_with_value = false;
  string with_value;
  if (cmd_getval(g_ceph_context, cmdmap, "with_value", with_value) &&
      with_value == "--with-value") {
    list_with_value = true;
  }

  // list features
  mon_feature_t supported = ceph::features::mon::get_supported();
  mon_feature_t persistent = ceph::features::mon::get_persistent();
  mon_feature_t required = monmap.get_required_features();

  stringstream ds;
  auto print_feature = [&](mon_feature_t& m_features, const char* m_str) {
    if (f) {
      if (list_with_value)
	m_features.dump_with_value(f.get(), m_str);
      else
	m_features.dump(f.get(), m_str);
    } else {
      if (list_with_value)
	m_features.print_with_value(ds);
      else
	m_features.print(ds);
    }
  };

  mon_feature_t mon_persistent = monmap.persistent_features;
  mon_feature_t mon_optional = monmap.optional_features;

  if (f) {
    f->open_object_section("features");

    f->open_object_section("all");
    print_feature(supported, "supported");
    print_feature(persistent, "persistent");
    f->close_section(); // all

    f->open_object_section("monmap");
    print_feature(mon_persistent, "persistent");
    print_feature(mon_optional, "optional");
    print_feature(required, "required");
    f->close_section(); // monmap 

    f->close_section(); // features
    f->flush(ds);

  } else {
    ds << "all features" << std::endl
      << "\tsupported: ";
    print_feature(supported, nullptr);
    ds << std::endl
      << "\tpersistent: ";
    print_feature(persistent, nullptr);
    ds << std::endl
      << std::endl;

    ds << "on current monmap (epoch "
      << monmap.get_epoch() << ")" << std::endl
      << "\tpersistent: ";
    print_feature(mon_persistent, nullptr);
    ds << std::endl
      // omit optional features in plain-text
      // makes it easier to read, and they're, currently, empty.
      << "\trequired: ";
    print_feature(required, nullptr);
    ds << std::endl;
  }
  rdata.append(ds);

  reply_with_data(op, 0, ss, rdata, service->get_last_committed());
  return true;
}

bool MonMonAdd::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  string name;
  cmd_getval(g_ceph_context, cmdmap, "name", name);
  string addrstr;
  cmd_getval(g_ceph_context, cmdmap, "addr", addrstr);
  entity_addr_t addr;

  if (!addr.parse(addrstr.c_str())) {
    ss << "addr " << addrstr << "does not parse";
    reply(op, -EINVAL, ss, service->get_last_committed());
    return false;
  }

  entity_addrvec_t addrs;
  if (stable.persistent_features.contains_all(
	ceph::features::mon::FEATURE_NAUTILUS)) {
    if (addr.get_port() == CEPH_MON_PORT_IANA) {
      addr.set_type(entity_addr_t::TYPE_MSGR2);
    }
    if (addr.get_port() == CEPH_MON_PORT_LEGACY) {
      // if they specified the *old* default they probably don't care
      addr.set_port(0);
    }
    if (addr.get_port()) {
      addrs.v.push_back(addr);
    } else {
      addr.set_type(entity_addr_t::TYPE_MSGR2);
      addr.set_port(CEPH_MON_PORT_IANA);
      addrs.v.push_back(addr);
      addr.set_type(entity_addr_t::TYPE_LEGACY);
      addr.set_port(CEPH_MON_PORT_LEGACY);
      addrs.v.push_back(addr);
    }
  } else {
    if (addr.get_port() == 0) {
      addr.set_port(CEPH_MON_PORT_LEGACY);
    }
    addr.set_type(entity_addr_t::TYPE_LEGACY);
    addrs.v.push_back(addr);
  }
  dout(20) << __func__ << " addr " << addr << " -> addrs " << addrs << dendl;

  /**
   * If we have a monitor with the same name and different addr, then EEXIST
   * If we have a monitor with the same addr and different name, then EEXIST
   * If we have a monitor with the same addr and same name, then wait for
   * the proposal to finish and return success.
   * If we don't have the monitor, add it.
   */

  if (!ss.str().empty())
    ss << "; ";

  do {
    if (stable.contains(name)) {
      if (stable.get_addrs(name) == addrs) {
	// stable map contains monitor with the same name at the same address.
	// serialize before current pending map.
	ss << "mon." << name << " at " << addrs << " already exists";
	reply(op, 0, ss, service->get_last_committed());
	return false;
      } else {
	ss << "mon." << name
	   << " already exists at address " << stable.get_addrs(name);
      }
    } else if (stable.contains(addrs)) {
      // we established on the previous branch that name is different
      ss << "mon." << stable.get_name(addrs)
	 << " already exists at address " << addr;
    } else {
      // go ahead and add
      break;
    }
    reply(op, -EEXIST, ss, service->get_last_committed());
    return false;
  } while (false);

  /* Given there's no delay between proposals on the MonmapMonitor (see
   * MonmapMonitor::should_propose()), there is no point in checking for
   * a mismatch between name and addr on pending_map.
   *
   * Once we established the monitor does not exist in the committed state,
   * we can simply go ahead and add the monitor.
   */

  pending_map.add(name, addrs);
  pending_map.last_changed = ceph_clock_now();
  ss << "adding mon." << name << " at " << addrs;
  dout(0) << __func__ << " proposing new mon." << name << dendl;

  // TODO: let's get back to this one. The whole reply without waiting is
  // still boggling me.
  reply(op, 0, ss, get_last_committed());
  return true;
}

bool MonMonRemove::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  string name;
  cmd_getval(g_ceph_context, cmdmap, "name", name);
  if (!stable.contains(name)) {
    ss << "mon." << name << " does not exist or has already been removed";
    reply(op, 0, ss, get_last_committed());
    return false;
  }

  if (stable.size() == 1) {
    ss << "error: refusing removal of last monitor " << name;
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }

  /* At the time of writing, there is no risk of races when multiple clients
   * attempt to use the same name. The reason is simple but may not be
   * obvious.
   *
   * In a nutshell, we do not collate proposals on the MonmapMonitor. As
   * soon as we return 'true' below, PaxosService::dispatch() will check if
   * the service should propose, and - if so - the service will be marked as
   * 'proposing' and a proposal will be triggered. The PaxosService class
   * guarantees that once a service is marked 'proposing' no further writes
   * will be handled.
   *
   * The decision on whether the service should propose or not is, in this
   * case, made by MonmapMonitor::should_propose(), which always considers
   * the proposal delay being 0.0 seconds. This is key for PaxosService to
   * trigger the proposal immediately.
   * 0.0 seconds of delay.
   *
   * From the above, there's no point in performing further checks on the
   * pending_map, as we don't ever have multiple proposals in-flight in
   * this service. As we've established the committed state contains the
   * monitor, we can simply go ahead and remove it.
   *
   * Please note that the code hinges on all of the above to be true. It
   * has been true since time immemorial and we don't see a good reason
   * to make it sturdier at this time - mainly because we don't think it's
   * going to change any time soon, lest for any bug that may be unwillingly
   * introduced.
   */

  entity_addrvec_t addrs = pending_map.get_addrs(name);
  pending_map.remove(name);
  pending_map.last_changed = ceph_clock_now();
  ss << "removing mon." << name << " at " << addrs
     << ", there will be " << pending_map.size() << " monitors" ;
  reply(op, 0, ss, service->get_last_committed());
  return true;
}

bool MonMonFeatureSet::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  /* PLEASE NOTE:
   *
   * We currently only support setting/unsetting persistent features.
   * This is by design, given at the moment we still don't have optional
   * features, and, as such, there is no point introducing an interface
   * to manipulate them. This allows us to provide a cleaner, more
   * intuitive interface to the user, modifying solely persistent
   * features.
   *
   * In the future we should consider adding another interface to handle
   * optional features/flags; e.g., 'mon feature flag set/unset', or
   * 'mon flag set/unset'.
   */
  string feature_name;
  if (!cmd_getval(g_ceph_context, cmdmap, "feature_name", feature_name)) {
    ss << "missing required feature name";
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }

  mon_feature_t feature;
  feature = ceph::features::mon::get_feature_by_name(feature_name);
  if (feature == ceph::features::mon::FEATURE_NONE) {
    ss << "unknown feature '" << feature_name << "'";
    reply(op, -ENOENT, ss, get_last_committed());
    return false;
  }

  bool sure = false;
  cmd_getval(g_ceph_context, cmdmap, "yes_i_really_mean_it", sure);
  if (!sure) {
    ss << "please specify '--yes-i-really-mean-it' if you "
       << "really, **really** want to set feature '"
       << feature << "' in the monmap.";
    reply(op, -EPERM, ss, get_last_committed());
    return false;
  }

  if (!mon->get_quorum_mon_features().contains_all(feature)) {
    ss << "current quorum does not support feature '" << feature
       << "'; supported features: "
       << mon->get_quorum_mon_features();
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }

  ss << "setting feature '" << feature << "'";

  if (stable.persistent_features.contains_all(feature)) {
    dout(10) << __func__ << " feature '" << feature
	     << "' already set on monmap; no-op." << dendl;
    reply(op, 0, ss, get_last_committed());
    return false;
  }

  pending_map.persistent_features.set_feature(feature);
  pending_map.last_changed = ceph_clock_now();

  dout(1) << __func__ << " " << ss.str() << "; new features will be: "
	  << "persistent = " << pending_map.persistent_features
    // output optional nevertheless, for auditing purposes.
	  << ", optional = " << pending_map.optional_features << dendl;

  reply(op, 0, ss, get_last_committed());
  return true;
}

bool MonMonSetRank::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  string name;
  int64_t rank;
  if (!cmd_getval(g_ceph_context, cmdmap, "name", name) ||
      !cmd_getval(g_ceph_context, cmdmap, "rank", rank)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  int oldrank = pending_map.get_rank(name);
  if (oldrank < 0) {
    ss << "mon." << name << " does not exist in monmap";
    reply(op, -ENOENT, ss, get_last_committed());
    return false;
  }
  dout(10) << __func__ << " setting rank for " << name << " to " << rank
	   << dendl;

  pending_map.set_rank(name, rank);
  pending_map.last_changed = ceph_clock_now();
  reply(op, 0, ss, get_last_committed());
  return true;
}

bool MonMonSetAddrs::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  string name;
  string addrs;
  if (!cmd_getval(g_ceph_context, cmdmap, "name", name) ||
      !cmd_getval(g_ceph_context, cmdmap, "addrs", addrs)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  if (!pending_map.contains(name)) {
    ss << "mon." << name << " does not exist";
    reply(op, -ENOENT, ss, get_last_committed());
    return false;
  }
  entity_addrvec_t av;
  if (!av.parse(addrs.c_str(), nullptr)) {
    ss << "failed to parse addrs '" << addrs << "'";
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  for (auto& a : av.v) {
    a.set_nonce(0);
    if (!a.get_port()) {
      ss << "monitor must bind to a non-zero port, not " << a;
      reply(op, -EINVAL, ss, get_last_committed());
      return false;
    }
  }
  pending_map.set_addrvec(name, av);
  pending_map.last_changed = ceph_clock_now();
  reply(op, 0, ss, get_last_committed());
  return true;
}

bool MonMonSetWeight::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  string name;
  int64_t weight;
  if (!cmd_getval(g_ceph_context, cmdmap, "name", name) ||
      !cmd_getval(g_ceph_context, cmdmap, "weight", weight)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  if (!pending_map.contains(name)) {
    ss << "mon." << name << " does not exist";
    reply(op, -ENOENT, ss, get_last_committed());
    return false;
  }
  pending_map.set_weight(name, weight);
  pending_map.last_changed = ceph_clock_now();
  reply(op, 0, ss, get_last_committed());
  return true;
}

bool MonMonEnableMsgr2::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    MonMap &pending_map,
    MonMap &stable)
{
  ceph_assert(handles_command(prefix));

  if (!stable.get_required_features().contains_all(
	ceph::features::mon::FEATURE_NAUTILUS)) {
    ss << "all monitors must be running nautilus to enable v2";
    reply(op, -EACCES, ss, get_last_committed());
    return false;
  }

  bool propose = false;
  for (auto& i : pending_map.mon_info) {
    if (i.second.public_addrs.v.size() == 1 &&
	i.second.public_addrs.front().is_legacy() &&
	i.second.public_addrs.front().get_port() == CEPH_MON_PORT_LEGACY) {
      entity_addrvec_t av;
      entity_addr_t a = i.second.public_addrs.front();
      a.set_type(entity_addr_t::TYPE_MSGR2);
      a.set_port(CEPH_MON_PORT_IANA);
      av.v.push_back(a);
      av.v.push_back(i.second.public_addrs.front());
      dout(10) << " setting mon." << i.first
	       << " addrs " << i.second.public_addrs
	       << " -> " << av << dendl;
      pending_map.set_addrvec(i.first, av);
      pending_map.last_changed = ceph_clock_now();
      propose = true;
    }
  }

  reply(op, 0, ss, get_last_committed());
  return propose;
}

