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
    const OSDMap &osdmap)
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
    const OSDMap &osdmap)
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
    const OSDMap &osdmap)
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
    const OSDMap &osdmap)
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
    const OSDMap &osdmap)
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
    const OSDMap &osdmap)
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
    const OSDMap &osdmap)
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

bool OSDMonStat::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const OSDMap &osdmap)
{
  stringstream ds;
  osdmap.print_summary(f.get(), ds, "", true);
  if (f)
    f->flush(rdata);
  else
    rdata.append(ds);

  reply_with_data(op, 0, ss, rdata, get_last_committed());
  return true;
}

bool OSDMonDumpAndFriends::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const OSDMap &osdmap)
{
  string val;

  epoch_t epoch = 0;
  int64_t epochnum;
  cmd_getval(cct, cmdmap, "epoch", epochnum, (int64_t)osdmap.get_epoch());
  epoch = epochnum;

  bufferlist osdmap_bl;
  int err = service->get_version_full(epoch, osdmap_bl);
  if (err == -ENOENT) {
    ss << "there is no map for epoch " << epoch;
    reply(op, -ENOENT, ss, get_last_committed());
    return true;
  }
  ceph_assert(err == 0);
  ceph_assert(osdmap_bl.length());

  const OSDMap *p;
  if (epoch == osdmap.get_epoch()) {
    p = &osdmap;
  } else {
    OSDMap *osdm = new OSDMap;
    osdm->decode(osdmap_bl);
    p = osdm;
  }

  auto sg = make_scope_guard([&] {
      if (p != &osdmap) {
	delete p;
      }
  });

  stringstream ds;
  if (prefix == "osd dump") {
    if (f) {
      f->open_object_section("osdmap");
      p->dump(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      p->print(ds);
    }
    rdata.append(ds);
    if (!f)
      ds << " ";
  } else if (prefix == "osd ls") {
    if (f) {
      f->open_array_section("osds");
      for (int i = 0; i < osdmap.get_max_osd(); i++) {
	if (osdmap.exists(i)) {
	  f->dump_int("osd", i);
	}
      }
      f->close_section();
      f->flush(ds);
    } else {
      bool first = true;
      for (int i = 0; i < osdmap.get_max_osd(); i++) {
	if (osdmap.exists(i)) {
	  if (!first)
	    ds << "\n";
	  first = false;
	  ds << i;
	}
      }
    }
    rdata.append(ds);
  } else if (prefix == "osd info") {
    int64_t osd_id;
    bool do_single_osd = true;
    if (!cmd_getval(cct, cmdmap, "id", osd_id)) {
      do_single_osd = false;
    }

    if (do_single_osd && !osdmap.exists(osd_id)) {
      ss << "osd." << osd_id << " does not exist";
      reply(op, -EINVAL, ss, get_last_committed());
      return true;
    }

    if (f) {
      if (do_single_osd) {
	osdmap.dump_osd(osd_id, f.get());
      } else {
	osdmap.dump_osds(f.get());
      }
      f->flush(ds);
    } else {
      if (do_single_osd) {
	osdmap.print_osd(osd_id, ds);
      } else {
	osdmap.print_osds(ds);
      }
    }
    rdata.append(ds);
  } else if (prefix == "osd tree" || prefix == "osd tree-from") {
    string bucket;
    if (prefix == "osd tree-from") {
      cmd_getval(cct, cmdmap, "bucket", bucket);
      if (!osdmap.crush->name_exists(bucket)) {
	ss << "bucket '" << bucket << "' does not exist";
	reply(op, -ENOENT, ss, get_last_committed());
	return true;
      }
      int id = osdmap.crush->get_item_id(bucket);
      if (id >= 0) {
	ss << "\"" << bucket << "\" is not a bucket";
	reply(op, -EINVAL, ss, get_last_committed());
	return true;
      }
    }

    vector<string> states;
    cmd_getval(cct, cmdmap, "states", states);
    unsigned filter = 0;
    for (auto& s : states) {
      if (s == "up") {
	filter |= OSDMap::DUMP_UP;
      } else if (s == "down") {
	filter |= OSDMap::DUMP_DOWN;
      } else if (s == "in") {
	filter |= OSDMap::DUMP_IN;
      } else if (s == "out") {
	filter |= OSDMap::DUMP_OUT;
      } else if (s == "destroyed") {
	filter |= OSDMap::DUMP_DESTROYED;
      } else {
	ss << "unrecognized state '" << s << "'";
	reply(op, -EINVAL, ss, get_last_committed());
	return true;
      }
    }
    if ((filter & (OSDMap::DUMP_IN|OSDMap::DUMP_OUT)) ==
	(OSDMap::DUMP_IN|OSDMap::DUMP_OUT)) {
      ss << "cannot specify both 'in' and 'out'";
      reply(op, -EINVAL, ss, get_last_committed());
      return true;
    }
    if (((filter & (OSDMap::DUMP_UP|OSDMap::DUMP_DOWN)) ==
	  (OSDMap::DUMP_UP|OSDMap::DUMP_DOWN)) ||
	((filter & (OSDMap::DUMP_UP|OSDMap::DUMP_DESTROYED)) ==
	 (OSDMap::DUMP_UP|OSDMap::DUMP_DESTROYED)) ||
	((filter & (OSDMap::DUMP_DOWN|OSDMap::DUMP_DESTROYED)) ==
	 (OSDMap::DUMP_DOWN|OSDMap::DUMP_DESTROYED))) {
      ss << "can specify only one of 'up', 'down' and 'destroyed'";
      reply(op, -EINVAL, ss, get_last_committed());
      return true;
    }
    if (f) {
      f->open_object_section("tree");
      p->print_tree(f.get(), NULL, filter, bucket);
      f->close_section();
      f->flush(ds);
    } else {
      p->print_tree(NULL, &ds, filter, bucket);
    }
    rdata.append(ds);
  } else if (prefix == "osd getmap") {
    rdata.append(osdmap_bl);
    ss << "got osdmap epoch " << p->get_epoch();
  } else if (prefix == "osd getcrushmap") {
    p->crush->encode(rdata, mon->get_quorum_con_features());
    ss << p->get_crush_version();
  } else if (prefix == "osd ls-tree") {
    string bucket_name;
    cmd_getval(cct, cmdmap, "name", bucket_name);
    set<int> osds;
    int r = p->get_osds_by_bucket_name(bucket_name, &osds);
    if (r == -ENOENT) {
      ss << "\"" << bucket_name << "\" does not exist";
      reply(op, -ENOENT, ss, get_last_committed());
      return true;
    } else if (r < 0) {
      ss << "can not parse bucket name:\"" << bucket_name << "\"";
      reply(op, r, ss, get_last_committed());
      return true;
    }

    if (f) {
      f->open_array_section("osds");
      for (auto &i : osds) {
	if (osdmap.exists(i)) {
	  f->dump_int("osd", i);
	}
      }
      f->close_section();
      f->flush(ds);
    } else {
      bool first = true;
      for (auto &i : osds) {
	if (osdmap.exists(i)) {
	  if (!first)
	    ds << "\n";
	  first = false;
	  ds << i;
	}
      }
    }

    rdata.append(ds);
  }
  reply_with_data(op, 0, ss, rdata, get_last_committed());
  return true;
}

bool OSDMonGetMaxOSD::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const OSDMap &osdmap)
{
  if (f) {
    f->open_object_section("getmaxosd");
    f->dump_unsigned("epoch", osdmap.get_epoch());
    f->dump_int("max_osd", osdmap.get_max_osd());
    f->close_section();
    f->flush(rdata);
  } else {
    stringstream ds;
    ds << "max_osd = " << osdmap.get_max_osd()
       << " in epoch " << osdmap.get_epoch();
    rdata.append(ds);
  }
  reply_with_data(op, 0, ss, rdata, get_last_committed());
  return true;
}

bool OSDMonGetOSDUtilization::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const OSDMap &osdmap)
{
  string out;
  osdmap.summarize_mapping_stats(NULL, NULL, &out, f.get());
  if (f)
    f->flush(rdata);
  else
    rdata.append(out);
  reply_with_data(op, 0, ss, rdata, get_last_committed());
  return true;
}

bool OSDMonFind::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const OSDMap &osdmap)
{
  int64_t osd;
  if (!cmd_getval(cct, cmdmap, "id", osd)) {
    auto p = cmdmap.find("id");
    if (p == std::cend(cmdmap)) {
      reply(op, -EINVAL, ss, get_last_committed());
      return true;
    }
    ss << "unable to parse osd id value '"
       << cmd_vartype_stringify(p->second) << "'";
    reply(op, -EINVAL, ss, get_last_committed());
    return true;
  }
  if (!osdmap.exists(osd)) {
    ss << "osd." << osd << " does not exist";
    reply(op, -ENOENT, ss, get_last_committed());
    return true;
  }
  string format;
  cmd_getval(cct, cmdmap, "format", format);
  f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
  f->open_object_section("osd_location");
  f->dump_int("osd", osd);
  f->dump_object("addrs", osdmap.get_addrs(osd));
  f->dump_stream("osd_fsid") << osdmap.get_uuid(osd);

  // try to identify host, pod/container name, etc.
  map<string,string> m;
  service->load_metadata(osd, m, nullptr);
  if (auto p = m.find("hostname"); p != m.end()) {
    f->dump_string("host", p->second);
  }
  for (auto& k : {
      "pod_name", "pod_namespace", // set by rook
      "container_name"             // set by ceph-ansible
      }) {
    if (auto p = m.find(k); p != m.end()) {
      f->dump_string(k, p->second);
    }
  }

  // crush is helpful too
  f->open_object_section("crush_location");
  map<string,string> loc = osdmap.crush->get_full_location(osd);
  for (map<string,string>::iterator p = loc.begin(); p != loc.end(); ++p)
    f->dump_string(p->first.c_str(), p->second);
  f->close_section();
  f->close_section();
  f->flush(rdata);

  reply_with_data(op, 0, ss, rdata, get_last_committed());
  return true;
}
