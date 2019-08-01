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
 *  prepare_pool_crush_rule	      --
 *  get_crush_rule		      --
 *  _prepare_command_osd_crush	      --
 *  do_osd_crush_remove		      --
 *  prepare_command_osd_crush_remove  --
 *
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


