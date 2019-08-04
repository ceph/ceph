// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 * Copyright (C) 2019 SUSE LLC <contact@suse.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>

#include "mon/Monitor.h"
#include "mon/OSDMonitor.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "osd/OSDMap.h"

#define dout_subsys ceph_subsys_mon
/*
 * functions
 *
 *  _have_pending_crush		      ++
 *  _get_stable_crush		      ++
 *  _get_pending_crush		      ++
 *  crush_rename_bucket		      ++
 *  crush_rule_create_erasure	      ++
 *  validate_crush_against_features   ++
 *  prepare_pool_crush_rule	      ++
 *  get_crush_rule		      ++
 *  _prepare_command_osd_crush	      ++
 *  do_osd_crush_remove		      ++
 *  prepare_command_osd_crush_remove  ++
 *
 *  prepare_crush_command
 *  preprocess_crush_command
 */

bool OSDMonitor::_have_pending_crush()
{
  return pending_inc.crush.length() > 0;
}

CrushWrapper &OSDMonitor::_get_stable_crush()
{
  return *osdmap.crush;
}

void OSDMonitor::_get_pending_crush(CrushWrapper& newcrush)
{
  bufferlist bl;
  if (pending_inc.crush.length())
    bl = pending_inc.crush;
  else
    osdmap.crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

  auto p = bl.cbegin();
  newcrush.decode(p);
}

int OSDMonitor::crush_rename_bucket(const string& srcname,
    const string& dstname,
    ostream *ss)
{
  int ret;
  //
  // Avoid creating a pending crush if it does not already exists and
  // the rename would fail.
  //
  if (!_have_pending_crush()) {
    ret = _get_stable_crush().can_rename_bucket(srcname,
	dstname,
	ss);
    if (ret)
      return ret;
  }

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

  ret = newcrush.rename_bucket(srcname,
      dstname,
      ss);
  if (ret)
    return ret;

  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  *ss << "renamed bucket " << srcname << " into " << dstname;	
  return 0;
}

int OSDMonitor::crush_rule_create_erasure(const string &name,
    const string &profile,
    int *rule,
    ostream *ss)
{
  int ruleid = osdmap.crush->get_rule_id(name);
  if (ruleid != -ENOENT) {
    *rule = osdmap.crush->get_rule_mask_ruleset(ruleid);
    return -EEXIST;
  }

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

  ruleid = newcrush.get_rule_id(name);
  if (ruleid != -ENOENT) {
    *rule = newcrush.get_rule_mask_ruleset(ruleid);
    return -EALREADY;
  } else {
    ErasureCodeInterfaceRef erasure_code;
    int err = get_erasure_code(profile, &erasure_code, ss);
    if (err) {
      *ss << "failed to load plugin using profile " << profile << std::endl;
      return err;
    }

    err = erasure_code->create_rule(name, newcrush, ss);
    erasure_code.reset();
    if (err < 0)
      return err;
    *rule = err;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    return 0;
  }
}

bool OSDMonitor::validate_crush_against_features(const CrushWrapper *newcrush,
    stringstream& ss)
{
  OSDMap::Incremental new_pending = pending_inc;
  encode(*newcrush, new_pending.crush, mon->get_quorum_con_features());
  OSDMap newmap;
  newmap.deepish_copy_from(osdmap);
  newmap.apply_incremental(new_pending);

  // client compat
  if (newmap.require_min_compat_client != ceph_release_t::unknown) {
    auto mv = newmap.get_min_compat_client();
    if (mv > newmap.require_min_compat_client) {
      ss << "new crush map requires client version " << mv
	<< " but require_min_compat_client is "
	<< newmap.require_min_compat_client;
      return false;
    }
  }

  // osd compat
  uint64_t features =
    newmap.get_features(CEPH_ENTITY_TYPE_MON, NULL) |
    newmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL);
  stringstream features_ss;
  int r = check_cluster_features(features, features_ss);
  if (r) {
    ss << "Could not change CRUSH: " << features_ss.str();
    return false;
  }

  return true;
}

int OSDMonitor::prepare_pool_crush_rule(const unsigned pool_type,
    const string &erasure_code_profile,
    const string &rule_name,
    int *crush_rule,
    ostream *ss)
{

  if (*crush_rule < 0) {
    switch (pool_type) {
      case pg_pool_t::TYPE_REPLICATED:
	{
	  if (rule_name == "") {
	    // Use default rule
	    *crush_rule = osdmap.crush->get_osd_pool_default_crush_replicated_ruleset(cct);
	    if (*crush_rule < 0) {
	      // Errors may happen e.g. if no valid rule is available
	      *ss << "No suitable CRUSH rule exists, check "
		<< "'osd pool default crush *' config options";
	      return -ENOENT;
	    }
	  } else {
	    return get_crush_rule(rule_name, crush_rule, ss);
	  }
	}
	break;
      case pg_pool_t::TYPE_ERASURE:
	{
	  int err = crush_rule_create_erasure(rule_name,
	      erasure_code_profile,
	      crush_rule, ss);
	  switch (err) {
	    case -EALREADY:
	      dout(20) << "prepare_pool_crush_rule: rule "
		<< rule_name << " try again" << dendl;
	      // fall through
	    case 0:
	      // need to wait for the crush rule to be proposed before proceeding
	      err = -EAGAIN;
	      break;
	    case -EEXIST:
	      err = 0;
	      break;
	  }
	  return err;
	}
	break;
      default:
	*ss << "prepare_pool_crush_rule: " << pool_type
	  << " is not a known pool type";
	return -EINVAL;
	break;
    }
  } else {
    if (!osdmap.crush->ruleset_exists(*crush_rule)) {
      *ss << "CRUSH rule " << *crush_rule << " not found";
      return -ENOENT;
    }
  }

  return 0;
}

int OSDMonitor::get_crush_rule(const string &rule_name,
    int *crush_rule,
    ostream *ss)
{
  int ret;
  ret = osdmap.crush->get_rule_id(rule_name);
  if (ret != -ENOENT) {
    // found it, use it
    *crush_rule = ret;
  } else {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    ret = newcrush.get_rule_id(rule_name);
    if (ret != -ENOENT) {
      // found it, wait for it to be proposed
      dout(20) << __func__ << ": rule " << rule_name
	<< " try again" << dendl;
      return -EAGAIN;
    } else {
      // Cannot find it , return error
      *ss << "specified rule " << rule_name << " doesn't exist";
      return ret;
    }
  }
  return 0;
}

int OSDMonitor::_prepare_command_osd_crush_remove(
    CrushWrapper &newcrush,
    int32_t id,
    int32_t ancestor,
    bool has_ancestor,
    bool unlink_only)
{
  int err = 0;

  if (has_ancestor) {
    err = newcrush.remove_item_under(cct, id, ancestor,
	unlink_only);
  } else {
    err = newcrush.remove_item(cct, id, unlink_only);
  }
  return err;
}

void OSDMonitor::do_osd_crush_remove(CrushWrapper& newcrush)
{
  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
}

int OSDMonitor::prepare_command_osd_crush_remove(
    CrushWrapper &newcrush,
    int32_t id,
    int32_t ancestor,
    bool has_ancestor,
    bool unlink_only)
{
  int err = _prepare_command_osd_crush_remove(
      newcrush, id, ancestor,
      has_ancestor, unlink_only);

  if (err < 0)
    return err;

  ceph_assert(err == 0);
  do_osd_crush_remove(newcrush);

  return 0;
}

bool OSDMonitor::preprocess_crush_command(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    derr << __func__ << " no session" << dendl;
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));


  if (prefix == "osd crush get-tunable") {
    string tunable;
    cmd_getval(cct, cmdmap, "tunable", tunable);
    ostringstream rss;
    if (f)
      f->open_object_section("tunable");
    if (tunable == "straw_calc_version") {
      if (f)
	f->dump_int(tunable.c_str(), osdmap.crush->get_straw_calc_version());
      else
	rss << osdmap.crush->get_straw_calc_version() << "\n";
    } else {
      r = -EINVAL;
      goto reply;
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    } else {
      rdata.append(rss.str());
    }
    r = 0;

  } else if (prefix == "osd crush rule list" ||
	     prefix == "osd crush rule ls") {
    if (f) {
      f->open_array_section("rules");
      osdmap.crush->list_rules(f.get());
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream ss;
      osdmap.crush->list_rules(&ss);
      rdata.append(ss.str());
    }
  } else if (prefix == "osd crush rule ls-by-class") {
    string class_name;
    cmd_getval(cct, cmdmap, "class", class_name);
    if (class_name.empty()) {
      ss << "no class specified";
      r = -EINVAL;
      goto reply;
    }
    set<int> rules;
    r = osdmap.crush->get_rules_by_class(class_name, &rules);
    if (r < 0) {
      ss << "failed to get rules by class '" << class_name << "'";
      goto reply;
    }
    if (f) {
      f->open_array_section("rules");
      for (auto &rule: rules) {
	f->dump_string("name", osdmap.crush->get_rule_name(rule));
      }
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream rs;
      for (auto &rule: rules) {
	rs << osdmap.crush->get_rule_name(rule) << "\n";
      }
      rdata.append(rs.str());
    }
  } else if (prefix == "osd crush rule dump") {
    string name;
    cmd_getval(cct, cmdmap, "name", name);
    string format;
    cmd_getval(cct, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
						     "json-pretty"));
    if (name == "") {
      f->open_array_section("rules");
      osdmap.crush->dump_rules(f.get());
      f->close_section();
    } else {
      int ruleno = osdmap.crush->get_rule_id(name);
      if (ruleno < 0) {
	ss << "unknown crush rule '" << name << "'";
	r = ruleno;
	goto reply;
      }
      osdmap.crush->dump_rule(ruleno, f.get());
    }
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush dump") {
    string format;
    cmd_getval(cct, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
						     "json-pretty"));
    f->open_object_section("crush_map");
    osdmap.crush->dump(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush show-tunables") {
    string format;
    cmd_getval(cct, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
						     "json-pretty"));
    f->open_object_section("crush_map_tunables");
    osdmap.crush->dump_tunables(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush tree") {
    string shadow;
    cmd_getval(cct, cmdmap, "shadow", shadow);
    bool show_shadow = shadow == "--show-shadow";
    boost::scoped_ptr<Formatter> f(Formatter::create(format));
    if (f) {
      f->open_object_section("crush_tree");
      osdmap.crush->dump_tree(nullptr,
	  f.get(),
	  osdmap.get_pool_names(),
	  show_shadow);
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream ss;
      osdmap.crush->dump_tree(&ss,
	  nullptr,
	  osdmap.get_pool_names(),
	  show_shadow);
      rdata.append(ss.str());
    }
  } else if (prefix == "osd crush ls") {
    string name;
    if (!cmd_getval(cct, cmdmap, "node", name)) {
      ss << "no node specified";
      r = -EINVAL;
      goto reply;
    }
    if (!osdmap.crush->name_exists(name)) {
      ss << "node '" << name << "' does not exist";
      r = -ENOENT;
      goto reply;
    }
    int id = osdmap.crush->get_item_id(name);
    list<int> result;
    if (id >= 0) {
      result.push_back(id);
    } else {
      int num = osdmap.crush->get_bucket_size(id);
      for (int i = 0; i < num; ++i) {
	result.push_back(osdmap.crush->get_bucket_item(id, i));
      }
    }
    if (f) {
      f->open_array_section("items");
      for (auto i : result) {
	f->dump_string("item", osdmap.crush->get_item_name(i));
      }
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream ss;
      for (auto i : result) {
	ss << osdmap.crush->get_item_name(i) << "\n";
      }
      rdata.append(ss.str());
    }
    r = 0;
  } else if (prefix == "osd crush class ls") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
						     "json-pretty"));
    f->open_array_section("crush_classes");
    for (auto i : osdmap.crush->class_name)
      f->dump_string("class", i.second);
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd crush class ls-osd") {
    string name;
    cmd_getval(cct, cmdmap, "class", name);
    set<int> osds;
    osdmap.crush->get_devices_by_class(name, &osds);
    if (f) {
      f->open_array_section("osds");
      for (auto &osd: osds)
	f->dump_int("osd", osd);
      f->close_section();
      f->flush(rdata);
    } else {
      bool first = true;
      for (auto &osd : osds) {
	if (!first)
	  ds << "\n";
	first = false;
	ds << osd;
      }
      rdata.append(ds);
    }
  } else if (prefix == "osd crush get-device-class") {
    vector<string> idvec;
    cmd_getval(cct, cmdmap, "ids", idvec);
    map<int, string> class_by_osd;
    for (auto& id : idvec) {
      ostringstream ts;
      long osd = parse_osd_id(id.c_str(), &ts);
      if (osd < 0) {
	ss << "unable to parse osd id:'" << id << "'";
	r = -EINVAL;
	goto reply;
      }
      auto device_class = osdmap.crush->get_item_class(osd);
      if (device_class)
	class_by_osd[osd] = device_class;
      else
	class_by_osd[osd] = ""; // no class
    }
    if (f) {
      f->open_array_section("osd_device_classes");
      for (auto& i : class_by_osd) {
	f->open_object_section("osd_device_class");
	f->dump_int("osd", i.first);
	f->dump_string("device_class", i.second);
	f->close_section();
      }
      f->close_section();
      f->flush(rdata);
    } else {
      if (class_by_osd.size() == 1) {
	// for single input, make a clean output
	ds << class_by_osd.begin()->second;
      } else {
	// note that we do not group osds by class here
	for (auto it = class_by_osd.begin();
	    it != class_by_osd.end();
	    it++) {
	  ds << "osd." << it->first << ' ' << it->second;
	  if (next(it) != class_by_osd.end())
	    ds << '\n';
	}
      }
      rdata.append(ds);
    }
  } else if (prefix == "osd erasure-code-profile ls") {
    const auto &profiles = osdmap.get_erasure_code_profiles();
    if (f)
      f->open_array_section("erasure-code-profiles");
    for (auto i = profiles.begin(); i != profiles.end(); ++i) {
      if (f)
	f->dump_string("profile", i->first.c_str());
      else
	rdata.append(i->first + "\n");
    }
    if (f) {
      f->close_section();
      ostringstream rs;
      f->flush(rs);
      rs << "\n";
      rdata.append(rs.str());
    }
  } else if (prefix == "osd crush weight-set ls") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format));
    if (f) {
      f->open_array_section("weight_sets");
      if (osdmap.crush->have_choose_args(CrushWrapper::DEFAULT_CHOOSE_ARGS)) {
	f->dump_string("pool", "(compat)");
      }
      for (auto& i : osdmap.crush->choose_args) {
	if (i.first >= 0) {
	  f->dump_string("pool", osdmap.get_pool_name(i.first));
	}
      }
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream rs;
      if (osdmap.crush->have_choose_args(CrushWrapper::DEFAULT_CHOOSE_ARGS)) {
	rs << "(compat)\n";
      }
      for (auto& i : osdmap.crush->choose_args) {
	if (i.first >= 0) {
	  rs << osdmap.get_pool_name(i.first) << "\n";
	}
      }
      rdata.append(rs.str());
    }
  } else if (prefix == "osd crush weight-set dump") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
						     "json-pretty"));
    osdmap.crush->dump_choose_args(f.get());
    f->flush(rdata);
  } else {
    // try prepare update
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

bool OSDMonitor::prepare_crush_command(
    MonOpRequestRef op,
    const cmdmap_t& cmdmap)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  string format;
  cmd_getval(cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  int64_t osdid;
  string osd_name;
  bool osdid_present = cmd_getval(cct, cmdmap, "id", osdid);

  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << osdid;
    osd_name = oss.str();
  }

  if (prefix == "osd setcrushmap" ||
      (prefix == "osd crush set" && !osdid_present)) {
    if (pending_inc.crush.length()) {
      dout(10) << __func__ << " waiting for pending crush update " << dendl;
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
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
      err = -EINVAL;
      ss << "Failed to parse crushmap: " << e.what();
      goto reply;
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
	  err = 0;
	  ss << osdmap.get_crush_version();
	  goto reply;
	}
      }
      if (prior_version != osdmap.get_crush_version()) {
	err = -EPERM;
	ss << "prior_version " << prior_version << " != crush version "
	   << osdmap.get_crush_version();
	goto reply;
      }
    }

    if (crush.has_legacy_rule_ids()) {
      err = -EINVAL;
      ss << "crush maps with ruleset != ruleid are no longer allowed";
      goto reply;
    }
    if (!validate_crush_against_features(&crush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    err = osdmap.validate_crush_rules(&crush, &ss);
    if (err < 0) {
      goto reply;
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
	err = r;
	goto reply;
      }
      dout(10) << __func__ << " crush somke test duration: "
               << duration << ", result: " << ess.str() << dendl;
    }

    pending_inc.crush = data;
    ss << osdmap.get_crush_version() + 1;
    goto update;

  } else if (prefix == "osd crush set-all-straw-buckets-to-straw2") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    for (int b = 0; b < newcrush.get_max_buckets(); ++b) {
      int bid = -1 - b;
      if (newcrush.bucket_exists(bid) &&
	  newcrush.get_bucket_alg(bid) == CRUSH_BUCKET_STRAW) {
	dout(20) << " bucket " << bid << " is straw, can convert" << dendl;
	newcrush.bucket_set_alg(bid, CRUSH_BUCKET_STRAW2);
      }
    }
    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush set-device-class") {
    string device_class;
    if (!cmd_getval(cct, cmdmap, "class", device_class)) {
      err = -EINVAL; // no value!
      goto reply;
    }

    bool stop = false;
    vector<string> idvec;
    cmd_getval(cct, cmdmap, "ids", idvec);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
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
                << "' device_class '" << device_class << "'"
                << dendl;
        err = newcrush.update_device_class(osd, device_class, name, &ss);
        if (err < 0) {
          goto reply;
        }
        if (err == 0 && !_have_pending_crush()) {
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
      getline(ss, rs);
      wait_for_finished_proposal(op,
        new Monitor::C_Command(mon,op, 0, rs, get_last_committed() + 1));
      return true;
    }

 } else if (prefix == "osd crush rm-device-class") {
    bool stop = false;
    vector<string> idvec;
    cmd_getval(cct, cmdmap, "ids", idvec);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
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
          err = -EINVAL;
          goto reply;
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

        err = newcrush.remove_device_class(cct, osd, &ss);
        if (err < 0) {
          // ss has reason for failure
          goto reply;
        }
        updated.insert(osd);
      }
    }

    if (!updated.empty()) {
      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
      ss << "done removing class of osd(s): " << updated;
      getline(ss, rs);
      wait_for_finished_proposal(op,
        new Monitor::C_Command(mon,op, 0, rs, get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd crush class create") {
    string device_class;
    if (!cmd_getval(g_ceph_context, cmdmap, "class", device_class)) {
      err = -EINVAL; // no value!
      goto reply;
    }
    if (osdmap.require_osd_release < ceph_release_t::luminous) {
      ss << "you must complete the upgrade and 'ceph osd require-osd-release "
         << "luminous' before using crush device classes";
      err = -EPERM;
      goto reply;
    }
    if (!_have_pending_crush() &&
        _get_stable_crush().class_exists(device_class)) {
      ss << "class '" << device_class << "' already exists";
      goto reply;
    }
     CrushWrapper newcrush;
    _get_pending_crush(newcrush);
     if (newcrush.class_exists(device_class)) {
      ss << "class '" << device_class << "' already exists";
      goto update;
    }
    int class_id = newcrush.get_or_create_class_id(device_class);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "created class " << device_class << " with id " << class_id
       << " to crush map";
    goto update;
  } else if (prefix == "osd crush class rm") {
    string device_class;
    if (!cmd_getval(g_ceph_context, cmdmap, "class", device_class)) {
       err = -EINVAL; // no value!
       goto reply;
     }
    if (osdmap.require_osd_release < ceph_release_t::luminous) {
       ss << "you must complete the upgrade and 'ceph osd require-osd-release "
         << "luminous' before using crush device classes";
       err = -EPERM;
       goto reply;
     }

     if (!osdmap.crush->class_exists(device_class)) {
       err = 0;
       goto reply;
     }

     CrushWrapper newcrush;
     _get_pending_crush(newcrush);
     if (!newcrush.class_exists(device_class)) {
       err = 0; // make command idempotent
       goto wait;
     }
     int class_id = newcrush.get_class_id(device_class);
     stringstream ts;
     if (newcrush.class_is_in_use(class_id, &ts)) {
       err = -EBUSY;
       ss << "class '" << device_class << "' " << ts.str();
       goto reply;
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
       err = -EBUSY;
       ss << "class '" << device_class
          << "' is still referenced by erasure-code-profile(s): " << referenced_by;
       goto reply;
     }

     set<int> osds;
     newcrush.get_devices_by_class(device_class, &osds);
     for (auto& p: osds) {
       err = newcrush.remove_device_class(g_ceph_context, p, &ss);
       if (err < 0) {
         // ss has reason for failure
         goto reply;
       }
     }

     if (osds.empty()) {
       // empty class, remove directly
       err = newcrush.remove_class_name(device_class);
       if (err < 0) {
         ss << "class '" << device_class << "' cannot be removed '"
            << cpp_strerror(err) << "'";
         goto reply;
       }
     }

     pending_inc.crush.clear();
     newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
     ss << "removed class " << device_class << " with id " << class_id
        << " from crush map";
     goto update;
  } else if (prefix == "osd crush class rename") {
    string srcname, dstname;
    if (!cmd_getval(cct, cmdmap, "srcname", srcname)) {
      err = -EINVAL;
      goto reply;
    }
    if (!cmd_getval(cct, cmdmap, "dstname", dstname)) {
      err = -EINVAL;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (!newcrush.class_exists(srcname) && newcrush.class_exists(dstname)) {
      // suppose this is a replay and return success
      // so command is idempotent
      ss << "already renamed to '" << dstname << "'";
      err = 0;
      goto reply;
    }

    err = newcrush.rename_class(srcname, dstname);
    if (err < 0) {
      ss << "fail to rename '" << srcname << "' to '" << dstname << "' : "
         << cpp_strerror(err);
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "rename class '" << srcname << "' to '" << dstname << "'";
    goto update;
  } else if (prefix == "osd crush add-bucket") {
    // os crush add-bucket <name> <type>
    string name, typestr;
    vector<string> argvec;
    cmd_getval(cct, cmdmap, "name", name);
    cmd_getval(cct, cmdmap, "type", typestr);
    cmd_getval(cct, cmdmap, "args", argvec);
    map<string,string> loc;
    if (!argvec.empty()) {
      CrushWrapper::parse_loc_map(argvec, &loc);
      dout(0) << "will create and move bucket '" << name
              << "' to location " << loc << dendl;
    }

    if (!_have_pending_crush() &&
	_get_stable_crush().name_exists(name)) {
      ss << "bucket '" << name << "' already exists";
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (newcrush.name_exists(name)) {
      ss << "bucket '" << name << "' already exists";
      goto update;
    }
    int type = newcrush.get_type_id(typestr);
    if (type < 0) {
      ss << "type '" << typestr << "' does not exist";
      err = -EINVAL;
      goto reply;
    }
    if (type == 0) {
      ss << "type '" << typestr << "' is for devices, not buckets";
      err = -EINVAL;
      goto reply;
    }
    int bucketno;
    err = newcrush.add_bucket(0, 0,
			      CRUSH_HASH_DEFAULT, type, 0, NULL,
			      NULL, &bucketno);
    if (err < 0) {
      ss << "add_bucket error: '" << cpp_strerror(err) << "'";
      goto reply;
    }
    err = newcrush.set_item_name(bucketno, name);
    if (err < 0) {
      ss << "error setting bucket name to '" << name << "'";
      goto reply;
    }

    if (!loc.empty()) {
      if (!newcrush.check_item_loc(cct, bucketno, loc,
          (int *)NULL)) {
        err = newcrush.move_bucket(cct, bucketno, loc);
        if (err < 0) {
          ss << "error moving bucket '" << name << "' to location " << loc;
          goto reply;
        }
      } else {
        ss << "no need to move item id " << bucketno << " name '" << name
           << "' to location " << loc << " in crush map";
      }
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    if (loc.empty()) {
      ss << "added bucket " << name << " type " << typestr
         << " to crush map";
    } else {
      ss << "added bucket " << name << " type " << typestr
         << " to location " << loc;
    }
    goto update;
  } else if (prefix == "osd crush rename-bucket") {
    string srcname, dstname;
    cmd_getval(cct, cmdmap, "srcname", srcname);
    cmd_getval(cct, cmdmap, "dstname", dstname);

    err = crush_rename_bucket(srcname, dstname, &ss);
    if (err == -EALREADY) // equivalent to success for idempotency
      err = 0;
    if (err)
      goto reply;
    else
      goto update;
  } else if (prefix == "osd crush weight-set create" ||
	     prefix == "osd crush weight-set create-compat") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    int64_t pool;
    int positions;
    if (newcrush.has_non_straw2_buckets()) {
      ss << "crush map contains one or more bucket(s) that are not straw2";
      err = -EPERM;
      goto reply;
    }
    if (prefix == "osd crush weight-set create") {
      if (osdmap.require_min_compat_client != ceph_release_t::unknown &&
	  osdmap.require_min_compat_client < ceph_release_t::luminous) {
	ss << "require_min_compat_client "
	   << osdmap.require_min_compat_client
	   << " < luminous, which is required for per-pool weight-sets. "
           << "Try 'ceph osd set-require-min-compat-client luminous' "
           << "before using the new interface";
	err = -EPERM;
	goto reply;
      }
      string poolname, mode;
      cmd_getval(cct, cmdmap, "pool", poolname);
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply;
      }
      cmd_getval(cct, cmdmap, "mode", mode);
      if (mode != "flat" && mode != "positional") {
	ss << "unrecognized weight-set mode '" << mode << "'";
	err = -EINVAL;
	goto reply;
      }
      positions = mode == "flat" ? 1 : osdmap.get_pg_pool(pool)->get_size();
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
      positions = 1;
    }
    if (!newcrush.create_choose_args(pool, positions)) {
      if (pool == CrushWrapper::DEFAULT_CHOOSE_ARGS) {
        ss << "compat weight-set already created";
      } else {
        ss << "weight-set for pool '" << osdmap.get_pool_name(pool)
           << "' already created";
      }
      goto reply;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    goto update;

  } else if (prefix == "osd crush weight-set rm" ||
	     prefix == "osd crush weight-set rm-compat") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    int64_t pool;
    if (prefix == "osd crush weight-set rm") {
      string poolname;
      cmd_getval(cct, cmdmap, "pool", poolname);
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply;
      }
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
    }
    newcrush.rm_choose_args(pool);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    goto update;

  } else if (prefix == "osd crush weight-set reweight" ||
	     prefix == "osd crush weight-set reweight-compat") {
    string poolname, item;
    vector<double> weight;
    cmd_getval(cct, cmdmap, "pool", poolname);
    cmd_getval(cct, cmdmap, "item", item);
    cmd_getval(cct, cmdmap, "weight", weight);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    int64_t pool;
    if (prefix == "osd crush weight-set reweight") {
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply;
      }
      if (!newcrush.have_choose_args(pool)) {
	ss << "no weight-set for pool '" << poolname << "'";
	err = -ENOENT;
	goto reply;
      }
      auto arg_map = newcrush.choose_args_get(pool);
      int positions = newcrush.get_choose_args_positions(arg_map);
      if (weight.size() != (size_t)positions) {
         ss << "must specify exact " << positions << " weight values";
         err = -EINVAL;
         goto reply;
      }
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
      if (!newcrush.have_choose_args(pool)) {
	ss << "no backward-compatible weight-set";
	err = -ENOENT;
	goto reply;
      }
    }
    if (!newcrush.name_exists(item)) {
      ss << "item '" << item << "' does not exist";
      err = -ENOENT;
      goto reply;
    }
    err = newcrush.choose_args_adjust_item_weightf(
      cct,
      newcrush.choose_args_get(pool),
      newcrush.get_item_id(item),
      weight,
      &ss);
    if (err < 0) {
      goto reply;
    }
    err = 0;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    goto update;
  } else if (osdid_present &&
	     (prefix == "osd crush set" || prefix == "osd crush add")) {
    // <OsdName> is 'osd.<id>' or '<id>', passed as int64_t id
    // osd crush set <OsdName> <weight> <loc1> [<loc2> ...]
    // osd crush add <OsdName> <weight> <loc1> [<loc2> ...]

    if (!osdmap.exists(osdid)) {
      err = -ENOENT;
      ss << osd_name
	 << " does not exist. Create it before updating the crush map";
      goto reply;
    }

    double weight;
    if (!cmd_getval(cct, cmdmap, "weight", weight)) {
      ss << "unable to parse weight value '"
         << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply;
    }

    string args;
    vector<string> argvec;
    cmd_getval(cct, cmdmap, "args", argvec);
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);

    if (prefix == "osd crush set"
        && !_get_stable_crush().item_exists(osdid)) {
      err = -ENOENT;
      ss << "unable to set item id " << osdid << " name '" << osd_name
         << "' weight " << weight << " at location " << loc
         << ": does not exist";
      goto reply;
    }

    dout(5) << "adding/updating crush item id " << osdid << " name '"
      << osd_name << "' weight " << weight << " at location "
      << loc << dendl;
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string action;
    if (prefix == "osd crush set" ||
        newcrush.check_item_loc(cct, osdid, loc, (int *)NULL)) {
      action = "set";
      err = newcrush.update_item(cct, osdid, weight, osd_name, loc);
    } else {
      action = "add";
      err = newcrush.insert_item(cct, osdid, weight, osd_name, loc);
      if (err == 0)
        err = 1;
    }

    if (err < 0)
      goto reply;

    if (err == 0 && !_have_pending_crush()) {
      ss << action << " item id " << osdid << " name '" << osd_name
	 << "' weight " << weight << " at location " << loc << ": no change";
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << action << " item id " << osdid << " name '" << osd_name << "' weight "
       << weight << " at location " << loc << " to crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush create-or-move") {
    do {
      // osd crush create-or-move <OsdName> <initial_weight> <loc1> [<loc2> ...]
      if (!osdmap.exists(osdid)) {
	err = -ENOENT;
	ss << osd_name
	   << " does not exist.  create it before updating the crush map";
	goto reply;
      }

      double weight;
      if (!cmd_getval(cct, cmdmap, "weight", weight)) {
        ss << "unable to parse weight value '"
           << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
        err = -EINVAL;
        goto reply;
      }

      string args;
      vector<string> argvec;
      cmd_getval(cct, cmdmap, "args", argvec);
      map<string,string> loc;
      CrushWrapper::parse_loc_map(argvec, &loc);

      dout(0) << "create-or-move crush item name '" << osd_name
	      << "' initial_weight " << weight << " at location " << loc
	      << dendl;

      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      err = newcrush.create_or_move_item(cct, osdid, weight, osd_name, loc,
					 g_conf()->osd_crush_update_weight_set);
      if (err == 0) {
	ss << "create-or-move updated item name '" << osd_name
	   << "' weight " << weight
	   << " at location " << loc << " to crush map";
	break;
      }
      if (err > 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
	ss << "create-or-move updating item name '" << osd_name
	   << "' weight " << weight
	   << " at location " << loc << " to crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush move") {
    do {
      // osd crush move <name> <loc1> [<loc2> ...]
      string name;
      vector<string> argvec;
      cmd_getval(cct, cmdmap, "name", name);
      cmd_getval(cct, cmdmap, "args", argvec);
      map<string,string> loc;
      CrushWrapper::parse_loc_map(argvec, &loc);

      dout(0) << "moving crush item name '" << name << "' to location " << loc << dendl;
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      if (!newcrush.name_exists(name)) {
	err = -ENOENT;
	ss << "item " << name << " does not exist";
	break;
      }
      int id = newcrush.get_item_id(name);

      if (!newcrush.check_item_loc(cct, id, loc, (int *)NULL)) {
	if (id >= 0) {
	  err = newcrush.create_or_move_item(
	    cct, id, 0, name, loc,
	    g_conf()->osd_crush_update_weight_set);
	} else {
	  err = newcrush.move_bucket(cct, id, loc);
	}
	if (err >= 0) {
	  ss << "moved item id " << id << " name '" << name << "' to location "
	     << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
	  getline(ss, rs);
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						   get_last_committed() + 1));
	  return true;
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name
	   << "' to location " << loc << " in crush map";
	err = 0;
      }
    } while (false);
  } else if (prefix == "osd crush swap-bucket") {
    string source, dest;
    cmd_getval(cct, cmdmap, "source", source);
    cmd_getval(cct, cmdmap, "dest", dest);

    bool force = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", force);

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (!newcrush.name_exists(source)) {
      ss << "source item " << source << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    if (!newcrush.name_exists(dest)) {
      ss << "dest item " << dest << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    int sid = newcrush.get_item_id(source);
    int did = newcrush.get_item_id(dest);
    int sparent;
    if (newcrush.get_immediate_parent_id(sid, &sparent) == 0 && !force) {
      ss << "source item " << source << " is not an orphan bucket; "
	 << "pass --yes-i-really-mean-it to proceed anyway";
      err = -EPERM;
      goto reply;
    }
    if (newcrush.get_bucket_alg(sid) != newcrush.get_bucket_alg(did) &&
	!force) {
      ss << "source bucket alg "
	 << crush_alg_name(newcrush.get_bucket_alg(sid)) << " != "
	 << "dest bucket alg " << crush_alg_name(newcrush.get_bucket_alg(did))
	 << "; pass --yes-i-really-mean-it to proceed anyway";
      err = -EPERM;
      goto reply;
    }
    int r = newcrush.swap_bucket(cct, sid, did);
    if (r < 0) {
      ss << "failed to swap bucket contents: " << cpp_strerror(r);
      err = r;
      goto reply;
    }
    ss << "swapped bucket of " << source << " to " << dest;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    wait_for_finished_proposal(op,
			       new Monitor::C_Command(mon, op, err, ss.str(),
						      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush link") {
    // osd crush link <name> <loc1> [<loc2> ...]
    string name;
    cmd_getval(cct, cmdmap, "name", name);
    vector<string> argvec;
    cmd_getval(cct, cmdmap, "args", argvec);
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);

    // Need an explicit check for name_exists because get_item_id returns
    // 0 on unfound.
    int id = osdmap.crush->get_item_id(name);
    if (!osdmap.crush->name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply;
    } else {
      dout(5) << "resolved crush name '" << name << "' to id " << id << dendl;
    }
    if (osdmap.crush->check_item_loc(cct, id, loc, (int*) NULL)) {
      ss << "no need to move item id " << id << " name '" << name
	 << "' to location " << loc << " in crush map";
      err = 0;
      goto reply;
    }

    dout(5) << "linking crush item name '" << name << "' at location "
	    << loc << dendl;
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply;
    } else {
      int id = newcrush.get_item_id(name);
      if (!newcrush.check_item_loc(cct, id, loc, (int *)NULL)) {
	err = newcrush.link_bucket(cct, id, loc);
	if (err >= 0) {
	  ss << "linked item id " << id << " name '" << name
             << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
	} else {
	  ss << "cannot link item id " << id << " name '" << name
             << "' to location " << loc;
          goto reply;
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name
           << "' to location " << loc << " in crush map";
	err = 0;
      }
    }
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, err,
					      ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush rm" ||
	     prefix == "osd crush remove" ||
	     prefix == "osd crush unlink") {
    do {
      // osd crush rm <id> [ancestor]
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      string name;
      cmd_getval(cct, cmdmap, "name", name);

      if (!osdmap.crush->name_exists(name)) {
	err = 0;
	ss << "device '" << name << "' does not appear in the crush map";
	break;
      }
      if (!newcrush.name_exists(name)) {
	err = 0;
	ss << "device '" << name << "' does not appear in the crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
      int id = newcrush.get_item_id(name);
      int ancestor = 0;

      bool unlink_only = prefix == "osd crush unlink";
      string ancestor_str;
      if (cmd_getval(cct, cmdmap, "ancestor", ancestor_str)) {
	if (!newcrush.name_exists(ancestor_str)) {
	  err = -ENOENT;
	  ss << "ancestor item '" << ancestor_str
	     << "' does not appear in the crush map";
	  break;
	}
        ancestor = newcrush.get_item_id(ancestor_str);
      }

      err = prepare_command_osd_crush_remove(
          newcrush,
          id, ancestor,
          (ancestor < 0), unlink_only);

      if (err == -ENOENT) {
	ss << "item " << id << " does not appear in that position";
	err = 0;
	break;
      }
      if (err == 0) {
        if (!unlink_only)
          pending_inc.new_crush_node_flags[id] = 0;
	ss << "removed item id " << id << " name '" << name << "' from crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush reweight-all") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    newcrush.reweight(cct);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "reweighted crush hierarchy";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string name;
    cmd_getval(cct, cmdmap, "name", name);
    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "device '" << name << "' does not appear in the crush map";
      goto reply;
    }

    int id = newcrush.get_item_id(name);
    if (id < 0) {
      ss << "device '" << name << "' is not a leaf in the crush map";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(cct, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply;
    }

    err = newcrush.adjust_item_weightf(cct, id, w,
				       g_conf()->osd_crush_update_weight_set);
    if (err < 0)
      goto reply;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "reweighted item id " << id << " name '" << name << "' to " << w
       << " in crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight-subtree") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string name;
    cmd_getval(cct, cmdmap, "name", name);
    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "device '" << name << "' does not appear in the crush map";
      goto reply;
    }

    int id = newcrush.get_item_id(name);
    if (id >= 0) {
      ss << "device '" << name << "' is not a subtree in the crush map";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(cct, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply;
    }

    err = newcrush.adjust_subtree_weightf(cct, id, w,
					  g_conf()->osd_crush_update_weight_set);
    if (err < 0)
      goto reply;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "reweighted subtree id " << id << " name '" << name << "' to " << w
       << " in crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush tunables") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    err = 0;
    string profile;
    cmd_getval(cct, cmdmap, "profile", profile);
    if (profile == "legacy" || profile == "argonaut") {
      newcrush.set_tunables_legacy();
    } else if (profile == "bobtail") {
      newcrush.set_tunables_bobtail();
    } else if (profile == "firefly") {
      newcrush.set_tunables_firefly();
    } else if (profile == "hammer") {
      newcrush.set_tunables_hammer();
    } else if (profile == "jewel") {
      newcrush.set_tunables_jewel();
    } else if (profile == "optimal") {
      newcrush.set_tunables_optimal();
    } else if (profile == "default") {
      newcrush.set_tunables_default();
    } else {
      ss << "unrecognized profile '" << profile << "'";
      err = -EINVAL;
      goto reply;
    }

    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "adjusted tunables profile to " << profile;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush set-tunable") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    err = 0;
    string tunable;
    cmd_getval(cct, cmdmap, "tunable", tunable);

    int64_t value = -1;
    if (!cmd_getval(cct, cmdmap, "value", value)) {
      err = -EINVAL;
      ss << "failed to parse integer value "
	 << cmd_vartype_stringify(cmdmap.at("value"));
      goto reply;
    }

    if (tunable == "straw_calc_version") {
      if (value != 0 && value != 1) {
	ss << "value must be 0 or 1; got " << value;
	err = -EINVAL;
	goto reply;
      }
      newcrush.set_straw_calc_version(value);
    } else {
      ss << "unrecognized tunable '" << tunable << "'";
      err = -EINVAL;
      goto reply;
    }

    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "adjusted tunable " << tunable << " to " << value;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-simple") {
    string name, root, type, mode;
    cmd_getval(cct, cmdmap, "name", name);
    cmd_getval(cct, cmdmap, "root", root);
    cmd_getval(cct, cmdmap, "type", type);
    cmd_getval(cct, cmdmap, "mode", mode);
    if (mode == "")
      mode = "firstn";

    if (osdmap.crush->rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (newcrush.rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
    } else {
      int ruleno = newcrush.add_simple_rule(name, root, type, "", mode,
					       pg_pool_t::TYPE_REPLICATED, &ss);
      if (ruleno < 0) {
	err = ruleno;
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-replicated") {
    string name, root, type, device_class;
    cmd_getval(cct, cmdmap, "name", name);
    cmd_getval(cct, cmdmap, "root", root);
    cmd_getval(cct, cmdmap, "type", type);
    cmd_getval(cct, cmdmap, "class", device_class);

    if (osdmap.crush->rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (newcrush.rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
    } else {
      int ruleno = newcrush.add_simple_rule(
	name, root, type, device_class,
	"firstn", pg_pool_t::TYPE_REPLICATED, &ss);
      if (ruleno < 0) {
	err = ruleno;
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-erasure") {
    err = check_cluster_features(CEPH_FEATURE_CRUSH_V2, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply;
    string name, poolstr;
    cmd_getval(cct, cmdmap, "name", name);
    string profile;
    cmd_getval(cct, cmdmap, "profile", profile);
    if (profile == "")
      profile = "default";
    if (profile == "default") {
      if (!osdmap.has_erasure_code_profile(profile)) {
	if (pending_inc.has_erasure_code_profile(profile)) {
	  dout(20) << "erasure code profile " << profile
		   << " already pending" << dendl;
	  goto wait;
	}

	map<string,string> profile_map;
	err = osdmap.get_erasure_code_profile_default(cct,
						      profile_map,
						      &ss);
	if (err)
	  goto reply;
	err = normalize_profile(name, profile_map, true, &ss);
	if (err)
	  goto reply;
	dout(20) << "erasure code profile set " << profile << "="
		 << profile_map << dendl;
	pending_inc.set_erasure_code_profile(profile, profile_map);
	goto wait;
      }
    }

    int rule;
    err = crush_rule_create_erasure(name, profile, &rule, &ss);
    if (err < 0) {
      switch(err) {
      case -EEXIST: // return immediately
	ss << "rule " << name << " already exists";
	err = 0;
	goto reply;
	break;
      case -EALREADY: // wait for pending to be proposed
	ss << "rule " << name << " already exists";
	err = 0;
	break;
      default: // non recoverable error
 	goto reply;
	break;
      }
    } else {
      ss << "created rule " << name << " at " << rule;
    }

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                    get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule rm") {
    string name;
    cmd_getval(cct, cmdmap, "name", name);

    if (!osdmap.crush->rule_exists(name)) {
      ss << "rule " << name << " does not exist";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (!newcrush.rule_exists(name)) {
      ss << "rule " << name << " does not exist";
      err = 0;
    } else {
      int ruleno = newcrush.get_rule_id(name);
      ceph_assert(ruleno >= 0);

      // make sure it is not in use.
      // FIXME: this is ok in some situations, but let's not bother with that
      // complexity now.
      int ruleset = newcrush.get_rule_mask_ruleset(ruleno);
      if (osdmap.crush_rule_in_use(ruleset)) {
	ss << "crush ruleset " << name << " " << ruleset << " is in use";
	err = -EBUSY;
	goto reply;
      }

      err = newcrush.remove_rule(ruleno);
      if (err < 0) {
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule rename") {
    string srcname;
    string dstname;
    cmd_getval(cct, cmdmap, "srcname", srcname);
    cmd_getval(cct, cmdmap, "dstname", dstname);
    if (srcname.empty() || dstname.empty()) {
      ss << "must specify both source rule name and destination rule name";
      err = -EINVAL;
      goto reply;
    }
    if (srcname == dstname) {
      ss << "destination rule name is equal to source rule name";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (!newcrush.rule_exists(srcname) && newcrush.rule_exists(dstname)) {
      // srcname does not exist and dstname already exists
      // suppose this is a replay and return success
      // (so this command is idempotent)
      ss << "already renamed to '" << dstname << "'";
      err = 0;
      goto reply;
    }

    err = newcrush.rename_rule(srcname, dstname, &ss);
    if (err < 0) {
      // ss has reason for failure
      goto reply;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
				      get_last_committed() + 1));
    return true;

  } else {
    err = -EINVAL;
  }

 reply:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0) {
    rs = cpp_strerror(err);
  }
  mon->reply_command(op, err, rs, rdata, get_last_committed());
  return ret;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
			     get_last_committed() + 1));
  return true;

 wait:
  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  return true;
}
