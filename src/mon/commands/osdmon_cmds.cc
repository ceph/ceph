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
#include "mon/OSDMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"
#include "mon/commands/osdmon_cmds.h"

#include "osd/OSDMap.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"

#define dout_subsys ceph_subsys_mon

bool OSDMonSetCrushmap::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());

  int64_t osdid;
  bool osdid_present = cmd_getval(cct, cmdmap, "id", osdid);

  string osd_name;
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << osdid;
    osd_name = oss.str();
  }

  if (pending_inc.crush.length()) {
    dout(10) << __func__ << " waiting for pending crush update " << dendl;
    wait_retry(op);
    return true;
  }
  dout(10) << "prepare_command setting new crush map" << dendl;
  bufferlist data(m->get_data());
  CrushWrapper crush;
  try {
    auto bl = data.cbegin();
    crush.decode(bl);
  }
  catch (const std::exception &e) {
    ss << "Failed to parse crushmap: " << e.what();
    reply(op, -EINVAL, ss, get_last_committed()); 
    return false;
  }

  int64_t prior_version = 0;
  if (cmd_getval(cct, cmdmap, "prior_version", prior_version)) {
    if (prior_version == osdmap.get_crush_version() - 1) {
      // see if we are a resend of the last update.  this is imperfect
      // (multiple racing updaters may not both get reliable success)
      // but we expect crush updaters (via this interface) to be rare-ish.
      bufferlist current, proposed;
      osdmap.crush->encode(current, mon->get_quorum_con_features());
      crush.encode(proposed, mon->get_quorum_con_features());
      if (current.contents_equal(proposed)) {
	dout(10) << __func__
	  << " proposed matches current and version equals previous"
	  << dendl;
	ss << osdmap.get_crush_version();
	reply(op, 0, ss, get_last_committed());
	return false;
      }
    }
    if (prior_version != osdmap.get_crush_version()) {
      ss << "prior_version " << prior_version << " != crush version "
	<< osdmap.get_crush_version();
      reply(op, -EPERM, ss, get_last_committed());
      return false;
    }
  }

  if (crush.has_legacy_rule_ids()) {
    ss << "crush maps with ruleset != ruleid are no longer allowed";
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  OSDMonitor *osdmon = static_cast<OSDMonitor*>(service);
  if (!osdmon->validate_crush_against_features(&crush, ss)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }

  int err = osdmap.validate_crush_rules(&crush, &ss);
  if (err < 0) {
    reply(op, err, ss, get_last_committed());
    return false;
  }

  if (g_conf()->mon_osd_crush_smoke_test) {
    // sanity check: test some inputs to make sure this map isn't
    // totally broken
    dout(10) << " testing map" << dendl;
    stringstream ess;
    CrushTester tester(crush, ess);
    tester.set_min_x(0);
    tester.set_max_x(50);
    auto start = ceph::coarse_mono_clock::now();
    int r = tester.test_with_fork(g_conf()->mon_lease);
    auto duration = ceph::coarse_mono_clock::now() - start;
    if (r < 0) {
      dout(10) << " tester.test_with_fork returns " << r
	<< ": " << ess.str() << dendl;
      ss << "crush smoke test failed with " << r << ": " << ess.str();
      reply(op, r, ss, get_last_committed());
      return false;
    }
    dout(10) << __func__ << " crush somke test duration: "
      << duration << ", result: " << ess.str() << dendl;
  }

  pending_inc.crush = data;
  ss << osdmap.get_crush_version() + 1;
  update(op, ss, get_last_committed() + 1);
  return true;
}


bool OSDMonCrushSetStrawBuckets::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  CrushWrapper newcrush;
  service->_get_pending_crush(newcrush);
  for (int b = 0; b < newcrush.get_max_buckets(); ++b) {
    int bid = -1 - b;
    if (newcrush.bucket_exists(bid) &&
	newcrush.get_bucket_alg(bid) == CRUSH_BUCKET_STRAW) {
      dout(20) << " bucket " << bid << " is straw, can convert" << dendl;
      newcrush.bucket_set_alg(bid, CRUSH_BUCKET_STRAW2);
    }
  }
  if (!service->validate_crush_against_features(&newcrush, ss)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  update(op, ss, get_last_committed() + 1);
  return true;
}


bool OSDMonCrushSetDeviceClass::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  string device_class;
  if (!cmd_getval(cct, cmdmap, "class", device_class)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }

  OSDMonitor *osdmon = service;

  bool stop = false;
  vector<string> idvec;
  cmd_getval(cct, cmdmap, "ids", idvec);
  CrushWrapper newcrush;
  osdmon->_get_pending_crush(newcrush);
  set<int> updated;

  int err = 0;
  for (unsigned j = 0; j < idvec.size() && !stop; j++) {
    set<int> osds;
    // wildcard?
    if (j == 0 &&
	(idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
      osdmap.get_all_osds(osds);
      stop = true;
    } else {
      // try traditional single osd way
      long osd = parse_osd_id(idvec[j].c_str(), &ss);
      if (osd < 0) {
	// ss has reason for failure
	ss << ", unable to parse osd id:\"" << idvec[j] << "\". ";
	err = -EINVAL;
	continue;
      }
      osds.insert(osd);
    }

    for (auto &osd : osds) {
      if (!osdmap.exists(osd)) {
	ss << "osd." << osd << " does not exist. ";
	continue;
      }

      ostringstream oss;
      oss << "osd." << osd;
      string name = oss.str();

      if (newcrush.get_max_devices() < osd + 1) {
	newcrush.set_max_devices(osd + 1);
      }
      string action;
      if (newcrush.item_exists(osd)) {
	action = "updating";
      } else {
	action = "creating";
	newcrush.set_item_name(osd, name);
      }

      dout(5) << action << " crush item id " << osd << " name '" << name
	      << "' device_class '" << device_class << "'" << dendl;
      err = newcrush.update_device_class(osd, device_class, name, &ss);
      if (err < 0) {
	reply(op, err, ss, get_last_committed());
	return false;
      }
      if (err == 0 && !osdmon->_have_pending_crush()) {
	if (!stop) {
	  // for single osd only, wildcard makes too much noise
	  ss << "set-device-class item id " << osd << " name '" << name
	     << "' device_class '" << device_class << "': no change. ";
	}
      } else {
	updated.insert(osd);
      }
    }
  }

  if (!updated.empty()) {
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "set osd(s) " << updated << " to class '" << device_class << "'";
    update(op, ss, get_last_committed() + 1);
    return true;
  }

  return false;
}


bool OSDMonCrushRemoveDeviceClass::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  bool stop = false;
  vector<string> idvec;
  cmd_getval(cct, cmdmap, "ids", idvec);
  CrushWrapper newcrush;
  service->_get_pending_crush(newcrush);
  set<int> updated;

  for (unsigned j = 0; j < idvec.size() && !stop; j++) {
    set<int> osds;

    // wildcard?
    if (j == 0 &&
	(idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
      osdmap.get_all_osds(osds);
      stop = true;
    } else {
      // try traditional single osd way
      long osd = parse_osd_id(idvec[j].c_str(), &ss);
      if (osd < 0) {
	// ss has reason for failure
	ss << ", unable to parse osd id:\"" << idvec[j] << "\". ";
	reply(op, -EINVAL, ss, get_last_committed());
	return false;
      }
      osds.insert(osd);
    }

    for (auto &osd : osds) {
      if (!osdmap.exists(osd)) {
	ss << "osd." << osd << " does not exist. ";
	continue;
      }

      auto class_name = newcrush.get_item_class(osd);
      if (!class_name) {
	ss << "osd." << osd << " belongs to no class, ";
	continue;
      }
      // note that we do not verify if class_is_in_use here
      // in case the device is misclassified and user wants
      // to overridely reset...

      int err = newcrush.remove_device_class(cct, osd, &ss);
      if (err < 0) {
	// ss has reason for failure
	reply(op, err, ss, get_last_committed());
	return false;
      }
      updated.insert(osd);
    }
  }

  if (!updated.empty()) {
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "done removing class of osd(s): " << updated;
    update(op, ss, get_last_committed() + 1);
    return true;
  }

  return false;
}

bool OSDMonCrushClassCreate::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  string device_class;
  if (!cmd_getval(g_ceph_context, cmdmap, "class", device_class)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  if (osdmap.require_osd_release < ceph_release_t::luminous) {
    ss << "you must complete the upgrade and 'ceph osd require-osd-release "
       << "luminous' before using crush device classes";
    reply(op, -EPERM, ss, get_last_committed());
    return false;
  }
  if (!service->_have_pending_crush() &&
      service->_get_stable_crush().class_exists(device_class)) {
    ss << "class '" << device_class << "' already exists";
    reply(op, 0, ss, get_last_committed());
    return false;
  }
  CrushWrapper newcrush;
  service->_get_pending_crush(newcrush);
  if (newcrush.class_exists(device_class)) {
    ss << "class '" << device_class << "' already exists";
    update(op, ss, get_last_committed() + 1);
    return true;
  }
  int class_id = newcrush.get_or_create_class_id(device_class);
  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  ss << "created class " << device_class << " with id " << class_id
     << " to crush map";
  update(op, ss, get_last_committed() + 1);
  return true;
}


bool OSDMonCrushClassRemove::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  int err = 0;
  string device_class;
  if (!cmd_getval(g_ceph_context, cmdmap, "class", device_class)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  if (osdmap.require_osd_release < ceph_release_t::luminous) {
    ss << "you must complete the upgrade and 'ceph osd require-osd-release "
       << "luminous' before using crush device classes";
    reply(op, -EPERM, ss, get_last_committed());
    return false;
  }

  if (!osdmap.crush->class_exists(device_class)) {
    reply(op, 0, ss, get_last_committed());
    return false;
  }

  CrushWrapper newcrush;
  service->_get_pending_crush(newcrush);
  if (!newcrush.class_exists(device_class)) {
    wait_retry(op);
    return true;
  }
  int class_id = newcrush.get_class_id(device_class);
  stringstream ts;
  if (newcrush.class_is_in_use(class_id, &ts)) {
    ss << "class '" << device_class << "' " << ts.str();
    reply(op, -EBUSY, ss, get_last_committed());
    return false;
  }

  // check if class is used by any erasure-code-profiles
  mempool::osdmap::map<string,map<string,string>> old_ec_profiles =
    osdmap.get_erasure_code_profiles();
  auto ec_profiles = pending_inc.get_erasure_code_profiles();
#ifdef HAVE_STDLIB_MAP_SPLICING
  ec_profiles.merge(old_ec_profiles);
#else
  ec_profiles.insert(make_move_iterator(begin(old_ec_profiles)),
      make_move_iterator(end(old_ec_profiles)));
#endif
  list<string> referenced_by;
  for (auto &i: ec_profiles) {
    for (auto &j: i.second) {
      if ("crush-device-class" == j.first && device_class == j.second) {
	referenced_by.push_back(i.first);
      }
    }
  }
  if (!referenced_by.empty()) {
    ss << "class '" << device_class
       << "' is still referenced by erasure-code-profile(s): "
       << referenced_by;
    reply(op, -EBUSY, ss, get_last_committed());
    return false;
  }

  set<int> osds;
  newcrush.get_devices_by_class(device_class, &osds);
  for (auto& p: osds) {
    dout(15) << "ClassRemove::" << __func__ << " removing '" << device_class
	     << "' from " << p << dendl;
    err = newcrush.remove_device_class(g_ceph_context, p, &ss);
    if (err < 0) {
      // ss has reason for failure
      reply(op, err, ss, get_last_committed());
      return false;
    }
  }

  if (osds.empty()) {
    // empty class, remove directly
    err = newcrush.remove_class_name(device_class);
    dout(15) << "ClassRemove::" << __func__
	     << " removing '" << device_class << "'" << dendl;
    if (err < 0) {
      ss << "class '" << device_class << "' cannot be removed '"
	 << cpp_strerror(err) << "'";
      reply(op, err, ss, get_last_committed());
      return false;
    }
  }

  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  ss << "removed class " << device_class << " with id " << class_id
     << " from crush map";
  update(op, ss, get_last_committed() + 1);
  return true;
}


bool OSDMonCrushClassRename::do_prepare(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    OSDMap::Incremental &pending_inc,
    OSDMap &osdmap)
{
  ceph_assert(handles_command(prefix));

  string srcname, dstname;
  if (!cmd_getval(cct, cmdmap, "srcname", srcname)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }
  if (!cmd_getval(cct, cmdmap, "dstname", dstname)) {
    reply(op, -EINVAL, ss, get_last_committed());
    return false;
  }

  CrushWrapper newcrush;
  service->_get_pending_crush(newcrush);
  if (!newcrush.class_exists(srcname) && newcrush.class_exists(dstname)) {
    // suppose this is a replay and return success
    // so command is idempotent
    ss << "already renamed to '" << dstname << "'";
    reply(op, 0, ss, get_last_committed());
    return false;
  }

  int err = newcrush.rename_class(srcname, dstname);
  if (err < 0) {
    ss << "fail to rename '" << srcname << "' to '" << dstname << "' : "
       << cpp_strerror(err);
    reply(op, err, ss, get_last_committed());
    return false;
  }

  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  ss << "rename class '" << srcname << "' to '" << dstname << "'";
  update(op, ss, get_last_committed() + 1);
  return true;
}
