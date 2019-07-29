// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2019 SUSE LLC <contact@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/* osd pool
 *
 * functions
 *
 *  is_osd_writeable	      ++
 *  is_unmanaged_snap_op_permitted ++
 *  _check_become_tier
 *  _check_remove_tier
 *
 *  _pool_op_reply	      ++
 *
 *  preprocess_pool_op	      ++
 *  prepare_pool_op	      ++
 *  enforce_pool_op_caps      ++
 *
 *  preprocess_pool_op_create ++
 *  prepare_pool_op_create    ++
 *  prepare_new_pool	      ++
 *  prepare_new_pool	      ++
 *  prepare_pool_size	      ++
 *  prepare_pool_stripe_width ++
 *
 *  prepare_pool_op_delete    ++
 *  _prepare_remove_pool      ++
 *  _check_remove_pool	      ++
 *
 *  do_set_pool_opt (ext. call)
 *
 *  update_pools_status	      ++
 *  set_pool_flags	      ++
 *  clear_pool_flags	      ++
 *
 *  prepare_command_impl (to be rewritten as prepare_command)
 *  prepare_command_pool_set		  ++
 *  prepare_command_pool_application	  ++
 *  preprocess_command_pool_application	  ++
 *  _command_pool_applicatio		  ++
 *  _prepare_rename_pool		  ++
 *
 *  prepare_command (TBC)
 *
 *
 * maybe
 *  prepare_pool_crush_rule
 *  check_pg_num
 *
 */

#include <sstream>
#include <algorithm>
#include <boost/algorithm/string.hpp>

#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/cmdparse.h"
#include "common/Checksummer.h"

#include "include/ceph_assert.h"

#include "mon/Session.h"
#include "mon/Monitor.h"
#include "mon/OSDMonitor.h"
#include "mon/MDSMonitor.h"
#include "mon/MgrStatMonitor.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "mds/FSMap.h"

#include "osd/OSDCap.h"
#include "osd/HitSet.h"

#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"


#define dout_subsys ceph_subsys_mon

namespace { // anonymous namespace

const uint32_t MAX_POOL_APPLICATIONS = 4;
const uint32_t MAX_POOL_APPLICATION_KEYS = 64;
const uint32_t MAX_POOL_APPLICATION_LENGTH = 128;


bool is_osd_writable(const OSDCapGrant& grant, const std::string* pool_name) {
  // Note: this doesn't include support for the application tag match
  if ((grant.spec.allow & OSD_CAP_W) != 0) {
    auto& match = grant.match;
    if (match.is_match_all()) {
      return true;
    } else if (pool_name != nullptr &&
               !match.pool_namespace.pool_name.empty() &&
               match.pool_namespace.pool_name == *pool_name) {
      return true;
    }
  }
  return false;
}

bool is_unmanaged_snap_op_permitted(CephContext* cct,
                                    const KeyServer& key_server,
                                    const EntityName& entity_name,
                                    const MonCap& mon_caps,
				    const entity_addr_t& peer_socket_addr,
                                    const std::string* pool_name)
{
  typedef std::map<std::string, std::string> CommandArgs;

  if (mon_caps.is_capable(
	cct, CEPH_ENTITY_TYPE_MON,
	entity_name, "osd",
	"osd pool op unmanaged-snap",
	(pool_name == nullptr ?
	 CommandArgs{} /* pool DNE, require unrestricted cap */ :
	 CommandArgs{{"poolname", *pool_name}}),
	false, true, false,
	peer_socket_addr)) {
    return true;
  }

  AuthCapsInfo caps_info;
  if (!key_server.get_service_caps(entity_name, CEPH_ENTITY_TYPE_OSD,
                                   caps_info)) {
    dout(10) << "unable to locate OSD cap data for " << entity_name
             << " in auth db" << dendl;
    return false;
  }

  string caps_str;
  if (caps_info.caps.length() > 0) {
    auto p = caps_info.caps.cbegin();
    try {
      decode(caps_str, p);
    } catch (const buffer::error &err) {
      derr << "corrupt OSD cap data for " << entity_name << " in auth db"
           << dendl;
      return false;
    }
  }

  OSDCap osd_cap;
  if (!osd_cap.parse(caps_str, nullptr)) {
    dout(10) << "unable to parse OSD cap data for " << entity_name
             << " in auth db" << dendl;
    return false;
  }

  // if the entity has write permissions in one or all pools, permit
  // usage of unmanaged-snapshots
  if (osd_cap.allow_all()) {
    return true;
  }

  for (auto& grant : osd_cap.grants) {
    if (grant.profile.is_valid()) {
      for (auto& profile_grant : grant.profile_grants) {
        if (is_osd_writable(profile_grant, pool_name)) {
          return true;
        }
      }
    } else if (is_osd_writable(grant, pool_name)) {
      return true;
    }
  }

  return false;
}

} // anonymous namespace

bool OSDMonitor::prepare_pool_command(
    MonOpRequestRef op,
    const cmdmap_t& cmdmap)
{

  op->mark_osdmon_event(__func__);
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

  if (prefix == "osd pool mksnap") {
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string snapname;
    cmd_getval(cct, cmdmap, "snap", snapname);
    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    if (p->is_unmanaged_snaps_mode()) {
      ss << "pool " << poolstr << " is in unmanaged snaps mode";
      err = -EINVAL;
      goto reply;
    } else if (p->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " already exists";
      err = 0;
      goto reply;
    } else if (p->is_tier()) {
      ss << "pool " << poolstr << " is a cache tier";
      err = -EINVAL;
      goto reply;
    }
    pg_pool_t *pp = 0;
    if (pending_inc.new_pools.count(pool))
      pp = &pending_inc.new_pools[pool];
    if (!pp) {
      pp = &pending_inc.new_pools[pool];
      *pp = *p;
    }
    if (pp->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " already exists";
    } else {
      pp->add_snap(snapname.c_str(), ceph_clock_now());
      pp->set_snap_epoch(pending_inc.epoch);
      ss << "created pool " << poolstr << " snap " << snapname;
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool rmsnap") {
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string snapname;
    cmd_getval(cct, cmdmap, "snap", snapname);
    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    if (p->is_unmanaged_snaps_mode()) {
      ss << "pool " << poolstr << " is in unmanaged snaps mode";
      err = -EINVAL;
      goto reply;
    } else if (!p->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " does not exist";
      err = 0;
      goto reply;
    }
    pg_pool_t *pp = 0;
    if (pending_inc.new_pools.count(pool))
      pp = &pending_inc.new_pools[pool];
    if (!pp) {
      pp = &pending_inc.new_pools[pool];
      *pp = *p;
    }
    snapid_t sn = pp->snap_exists(snapname.c_str());
    if (sn) {
      pp->remove_snap(sn);
      pp->set_snap_epoch(pending_inc.epoch);
      ss << "removed pool " << poolstr << " snap " << snapname;
    } else {
      ss << "already removed pool " << poolstr << " snap " << snapname;
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool create") {
    int64_t pg_num, pg_num_min;
    int64_t pgp_num;
    cmd_getval(cct, cmdmap, "pg_num", pg_num, int64_t(0));
    cmd_getval(cct, cmdmap, "pgp_num", pgp_num, pg_num);
    cmd_getval(cct, cmdmap, "pg_num_min", pg_num_min, int64_t(0));

    string pool_type_str;
    cmd_getval(cct, cmdmap, "pool_type", pool_type_str);
    if (pool_type_str.empty())
      pool_type_str = g_conf().get_val<string>("osd_pool_default_type");

    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id >= 0) {
      const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
      if (pool_type_str != p->get_type_name()) {
	ss << "pool '" << poolstr << "' cannot change to type " << pool_type_str;
 	err = -EINVAL;
      } else {
	ss << "pool '" << poolstr << "' already exists";
	err = 0;
      }
      goto reply;
    }

    int pool_type;
    if (pool_type_str == "replicated") {
      pool_type = pg_pool_t::TYPE_REPLICATED;
    } else if (pool_type_str == "erasure") {
      pool_type = pg_pool_t::TYPE_ERASURE;
    } else {
      ss << "unknown pool type '" << pool_type_str << "'";
      err = -EINVAL;
      goto reply;
    }

    bool implicit_rule_creation = false;
    int64_t expected_num_objects = 0;
    string rule_name;
    cmd_getval(cct, cmdmap, "rule", rule_name);
    string erasure_code_profile;
    cmd_getval(cct, cmdmap, "erasure_code_profile", erasure_code_profile);

    if (pool_type == pg_pool_t::TYPE_ERASURE) {
      if (erasure_code_profile == "")
	erasure_code_profile = "default";
      //handle the erasure code profile
      if (erasure_code_profile == "default") {
	if (!osdmap.has_erasure_code_profile(erasure_code_profile)) {
	  if (pending_inc.has_erasure_code_profile(erasure_code_profile)) {
	    dout(20) << "erasure code profile " << erasure_code_profile << " already pending" << dendl;
	    goto wait;
	  }

	  map<string,string> profile_map;
	  err = osdmap.get_erasure_code_profile_default(cct,
						      profile_map,
						      &ss);
	  if (err)
	    goto reply;
	  dout(20) << "erasure code profile " << erasure_code_profile << " set" << dendl;
	  pending_inc.set_erasure_code_profile(erasure_code_profile, profile_map);
	  goto wait;
	}
      }
      if (rule_name == "") {
	implicit_rule_creation = true;
	if (erasure_code_profile == "default") {
	  rule_name = "erasure-code";
	} else {
	  dout(1) << "implicitly use rule named after the pool: "
		<< poolstr << dendl;
	  rule_name = poolstr;
	}
      }
      cmd_getval(g_ceph_context, cmdmap, "expected_num_objects",
                 expected_num_objects, int64_t(0));
    } else {
      //NOTE:for replicated pool,cmd_map will put rule_name to erasure_code_profile field
      //     and put expected_num_objects to rule field
      if (erasure_code_profile != "") { // cmd is from CLI
        if (rule_name != "") {
          string interr;
          expected_num_objects = strict_strtoll(rule_name.c_str(), 10, &interr);
          if (interr.length()) {
            ss << "error parsing integer value '" << rule_name << "': " << interr;
            err = -EINVAL;
            goto reply;
          }
        }
        rule_name = erasure_code_profile;
      } else { // cmd is well-formed
        cmd_getval(g_ceph_context, cmdmap, "expected_num_objects",
                   expected_num_objects, int64_t(0));
      }
    }

    if (!implicit_rule_creation && rule_name != "") {
      int rule;
      err = get_crush_rule(rule_name, &rule, &ss);
      if (err == -EAGAIN) {
	wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	return true;
      }
      if (err)
	goto reply;
    }

    if (expected_num_objects < 0) {
      ss << "'expected_num_objects' must be non-negative";
      err = -EINVAL;
      goto reply;
    }

    if (expected_num_objects > 0 &&
	cct->_conf->osd_objectstore == "filestore" &&
	cct->_conf->filestore_merge_threshold > 0) {
      ss << "'expected_num_objects' requires 'filestore_merge_threshold < 0'";
      err = -EINVAL;
      goto reply;
    }

    if (expected_num_objects == 0 &&
	cct->_conf->osd_objectstore == "filestore" &&
	cct->_conf->filestore_merge_threshold < 0) {
      int osds = osdmap.get_num_osds();
      if (osds && (pg_num >= 1024 || pg_num / osds >= 100)) {
        ss << "For better initial performance on pools expected to store a "
	   << "large number of objects, consider supplying the "
	   << "expected_num_objects parameter when creating the pool.\n";
      }
    }

    int64_t fast_read_param;
    cmd_getval(cct, cmdmap, "fast_read", fast_read_param, int64_t(-1));
    FastReadType fast_read = FAST_READ_DEFAULT;
    if (fast_read_param == 0)
      fast_read = FAST_READ_OFF;
    else if (fast_read_param > 0)
      fast_read = FAST_READ_ON;

    int64_t repl_size = 0;
    cmd_getval(cct, cmdmap, "size", repl_size);
    int64_t target_size_bytes = 0;
    double target_size_ratio = 0.0;
    cmd_getval(cct, cmdmap, "target_size_bytes", target_size_bytes);
    cmd_getval(cct, cmdmap, "target_size_ratio", target_size_ratio);

    err = prepare_new_pool(poolstr,
			   -1, // default crush rule
			   rule_name,
			   pg_num, pgp_num, pg_num_min,
                           repl_size, target_size_bytes, target_size_ratio,
			   erasure_code_profile, pool_type,
                           (uint64_t)expected_num_objects,
                           fast_read,
			   &ss);
    if (err < 0) {
      switch(err) {
      case -EEXIST:
	ss << "pool '" << poolstr << "' already exists";
	break;
      case -EAGAIN:
	wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	return true;
      case -ERANGE:
        goto reply;
      default:
	goto reply;
	break;
      }
    } else {
      ss << "pool '" << poolstr << "' created";
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pool delete" ||
             prefix == "osd pool rm") {
    // osd pool delete/rm <poolname> <poolname again> --yes-i-really-really-mean-it
    string poolstr, poolstr2, sure;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    cmd_getval(cct, cmdmap, "pool2", poolstr2);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "pool '" << poolstr << "' does not exist";
      err = 0;
      goto reply;
    }

    bool force_no_fake = false;
    cmd_getval(cct, cmdmap, "yes_i_really_really_mean_it", force_no_fake);
    bool force = false;
    cmd_getval(cct, cmdmap, "yes_i_really_really_mean_it_not_faking", force);
    if (poolstr2 != poolstr ||
	(!force && !force_no_fake)) {
      ss << "WARNING: this will *PERMANENTLY DESTROY* all data stored in pool " << poolstr
	 << ".  If you are *ABSOLUTELY CERTAIN* that is what you want, pass the pool name *twice*, "
	 << "followed by --yes-i-really-really-mean-it.";
      err = -EPERM;
      goto reply;
    }
    err = _prepare_remove_pool(pool, &ss, force_no_fake);
    if (err == -EAGAIN) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    if (err < 0)
      goto reply;
    goto update;
  } else if (prefix == "osd pool rename") {
    string srcpoolstr, destpoolstr;
    cmd_getval(cct, cmdmap, "srcpool", srcpoolstr);
    cmd_getval(cct, cmdmap, "destpool", destpoolstr);
    int64_t pool_src = osdmap.lookup_pg_pool_name(srcpoolstr.c_str());
    int64_t pool_dst = osdmap.lookup_pg_pool_name(destpoolstr.c_str());

    if (pool_src < 0) {
      if (pool_dst >= 0) {
        // src pool doesn't exist, dst pool does exist: to ensure idempotency
        // of operations, assume this rename succeeded, as it is not changing
        // the current state.  Make sure we output something understandable
        // for whoever is issuing the command, if they are paying attention,
        // in case it was not intentional; or to avoid a "wtf?" and a bug
        // report in case it was intentional, while expecting a failure.
        ss << "pool '" << srcpoolstr << "' does not exist; pool '"
          << destpoolstr << "' does -- assuming successful rename";
        err = 0;
      } else {
        ss << "unrecognized pool '" << srcpoolstr << "'";
        err = -ENOENT;
      }
      goto reply;
    } else if (pool_dst >= 0) {
      // source pool exists and so does the destination pool
      ss << "pool '" << destpoolstr << "' already exists";
      err = -EEXIST;
      goto reply;
    }

    int ret = _prepare_rename_pool(pool_src, destpoolstr);
    if (ret == 0) {
      ss << "pool '" << srcpoolstr << "' renamed to '" << destpoolstr << "'";
    } else {
      ss << "failed to rename pool '" << srcpoolstr << "' to '" << destpoolstr << "': "
        << cpp_strerror(ret);
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, ret, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pool set") {
    err = prepare_command_pool_set(cmdmap, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply;

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						   get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool set-quota") {
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }

    string field;
    cmd_getval(cct, cmdmap, "field", field);
    if (field != "max_objects" && field != "max_bytes") {
      ss << "unrecognized field '" << field << "'; should be 'max_bytes' or 'max_objects'";
      err = -EINVAL;
      goto reply;
    }

    // val could contain unit designations, so we treat as a string
    string val;
    cmd_getval(cct, cmdmap, "val", val);
    string tss;
    int64_t value;
    if (field == "max_objects") {
      value = strict_sistrtoll(val.c_str(), &tss);
    } else if (field == "max_bytes") {
      value = strict_iecstrtoll(val.c_str(), &tss);
    } else {
      ceph_abort_msg("unrecognized option");
    }
    if (!tss.empty()) {
      ss << "error parsing value '" << val << "': " << tss;
      err = -EINVAL;
      goto reply;
    }

    pg_pool_t *pi = pending_inc.get_new_pool(pool_id, osdmap.get_pg_pool(pool_id));
    if (field == "max_objects") {
      pi->quota_max_objects = value;
    } else if (field == "max_bytes") {
      pi->quota_max_bytes = value;
    } else {
      ceph_abort_msg("unrecognized option");
    }
    ss << "set-quota " << field << " = " << value << " for pool " << poolstr;
    rs = ss.str();
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool application enable" ||
             prefix == "osd pool application disable" ||
             prefix == "osd pool application set" ||
             prefix == "osd pool application rm") {
    err = prepare_command_pool_application(prefix, cmdmap, ss);
    if (err == -EAGAIN) {
      goto wait;
    } else if (err < 0) {
      goto reply;
    } else {
      goto update;
    }
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
  wait_for_finished_proposal(op,
      new Monitor::C_Command(mon, op, 0, rs, get_last_committed() + 1));
  return true;

 wait:
  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  return true;
}

/*
 * pool reply
 *
 */

void OSDMonitor::_pool_op_reply(MonOpRequestRef op,
                                int ret, epoch_t epoch, bufferlist *blp)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
				 ret, epoch, get_last_committed(), blp);
  mon->send_reply(op, reply);
}


/*
 *
 * -- pool op --
 *
 */

bool OSDMonitor::preprocess_pool_op(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());

  if (enforce_pool_op_caps(op)) {
    return true;
  }

  if (m->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " drop message on fsid " << m->fsid
            << " != " << mon->monmap->fsid << " for " << *m << dendl;
    _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
    return true;
  }

  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(op);

  const pg_pool_t *p = osdmap.get_pg_pool(m->pool);
  if (p == nullptr) {
    dout(10) << "attempt to operate on non-existent pool id " << m->pool
	     << dendl;
    if (m->op == POOL_OP_DELETE) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
    } else {
      _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
    }
    return true;
  }

  // check if the snap and snapname exist
  bool snap_exists = false;
  if (p->snap_exists(m->name.c_str()))
    snap_exists = true;

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (p->is_unmanaged_snaps_mode() || p->is_tier()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (snap_exists) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_SNAP:
    if (p->is_unmanaged_snaps_mode()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (!snap_exists) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (_is_removed_snap(m->pool, m->snapid)) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE:
    if (osdmap.lookup_pg_pool_name(m->name.c_str()) >= 0) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_AUID_CHANGE:
    return false;
  default:
    ceph_abort();
    break;
  }

  return false;
}

bool OSDMonitor::prepare_pool_op(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  dout(10) << "prepare_pool_op " << *m << dendl;
  if (m->op == POOL_OP_CREATE) {
    return prepare_pool_op_create(op);
  } else if (m->op == POOL_OP_DELETE) {
    return prepare_pool_op_delete(op);
  }

  int ret = 0;
  bool changed = false;

  if (!osdmap.have_pg_pool(m->pool)) {
    _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
    return false;
  }

  const pg_pool_t *pool = osdmap.get_pg_pool(m->pool);

  switch (m->op) {
    case POOL_OP_CREATE_SNAP:
      if (pool->is_tier()) {
        ret = -EINVAL;
        _pool_op_reply(op, ret, osdmap.get_epoch());
        return false;
      }  // else, fall through
    case POOL_OP_DELETE_SNAP:
      if (!pool->is_unmanaged_snaps_mode()) {
        bool snap_exists = pool->snap_exists(m->name.c_str());
        if ((m->op == POOL_OP_CREATE_SNAP && snap_exists)
          || (m->op == POOL_OP_DELETE_SNAP && !snap_exists)) {
          ret = 0;
        } else {
          break;
        }
      } else {
        ret = -EINVAL;
      }
      _pool_op_reply(op, ret, osdmap.get_epoch());
      return false;

    case POOL_OP_DELETE_UNMANAGED_SNAP:
      // we won't allow removal of an unmanaged snapshot from a pool
      // not in unmanaged snaps mode.
      if (!pool->is_unmanaged_snaps_mode()) {
        _pool_op_reply(op, -ENOTSUP, osdmap.get_epoch());
        return false;
      }
      /* fall-thru */
    case POOL_OP_CREATE_UNMANAGED_SNAP:
      // but we will allow creating an unmanaged snapshot on any pool
      // as long as it is not in 'pool' snaps mode.
      if (pool->is_pool_snaps_mode()) {
        _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
        return false;
      }
  }

  // projected pool info
  pg_pool_t pp;
  if (pending_inc.new_pools.count(m->pool))
    pp = pending_inc.new_pools[m->pool];
  else
    pp = *osdmap.get_pg_pool(m->pool);

  bufferlist reply_data;

  // pool snaps vs unmanaged snaps are mutually exclusive
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
  case POOL_OP_DELETE_SNAP:
    if (pp.is_unmanaged_snaps_mode()) {
      ret = -EINVAL;
      goto out;
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (pp.is_pool_snaps_mode()) {
      ret = -EINVAL;
      goto out;
    }
  }

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (!pp.snap_exists(m->name.c_str())) {
      pp.add_snap(m->name.c_str(), ceph_clock_now());
      dout(10) << "create snap in pool " << m->pool << " " << m->name
	       << " seq " << pp.get_snap_epoch() << dendl;
      changed = true;
    }
    break;

  case POOL_OP_DELETE_SNAP:
    {
      snapid_t s = pp.snap_exists(m->name.c_str());
      if (s) {
	pp.remove_snap(s);
	pending_inc.new_removed_snaps[m->pool].insert(s);
	changed = true;
      }
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
    {
      uint64_t snapid = pp.add_unmanaged_snap(
	osdmap.require_osd_release < ceph_release_t::octopus);
      encode(snapid, reply_data);
      changed = true;
    }
    break;

  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (!_is_removed_snap(m->pool, m->snapid) &&
	!_is_pending_removed_snap(m->pool, m->snapid)) {
      if (m->snapid > pp.get_snap_seq()) {
        _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
        return false;
      }
      pp.remove_unmanaged_snap(
	m->snapid,
	osdmap.require_osd_release < ceph_release_t::octopus);
      pending_inc.new_removed_snaps[m->pool].insert(m->snapid);
      // also record the new seq as purged: this avoids a discontinuity
      // after all of the snaps have been purged, since the seq assigned
      // during removal lives in the same namespace as the actual snaps.
      pending_pseudo_purged_snaps[m->pool].insert(pp.get_snap_seq());
      changed = true;
    }
    break;

  case POOL_OP_AUID_CHANGE:
    _pool_op_reply(op, -EOPNOTSUPP, osdmap.get_epoch());
    return false;

  default:
    ceph_abort();
    break;
  }

  if (changed) {
    pp.set_snap_epoch(pending_inc.epoch);
    pending_inc.new_pools[m->pool] = pp;
  }

 out:
  wait_for_finished_proposal(op,
      new OSDMonitor::C_PoolOp(this, op, ret, pending_inc.epoch, &reply_data));
  return true;
}

bool OSDMonitor::enforce_pool_op_caps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);

  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  MonSession *session = op->get_session();
  if (!session) {
    _pool_op_reply(op, -EPERM, osdmap.get_epoch());
    return true;
  }

  switch (m->op) {
  case POOL_OP_CREATE_UNMANAGED_SNAP:
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    {
      const std::string* pool_name = nullptr;
      const pg_pool_t *pg_pool = osdmap.get_pg_pool(m->pool);
      if (pg_pool != nullptr) {
        pool_name = &osdmap.get_pool_name(m->pool);
      }

      if (!is_unmanaged_snap_op_permitted(cct, mon->key_server,
                                          session->entity_name, session->caps,
					  session->get_peer_socket_addr(),
                                          pool_name)) {
        dout(0) << "got unmanaged-snap pool op from entity with insufficient "
                << "privileges. message: " << *m  << std::endl
                << "caps: " << session->caps << dendl;
        _pool_op_reply(op, -EPERM, osdmap.get_epoch());
        return true;
      }
    }
    break;
  default:
    if (!session->is_capable("osd", MON_CAP_W)) {
      dout(0) << "got pool op from entity with insufficient privileges. "
              << "message: " << *m  << std::endl
              << "caps: " << session->caps << dendl;
      _pool_op_reply(op, -EPERM, osdmap.get_epoch());
      return true;
    }
    break;
  }

  return false;
}


/*
 *
 * -- pool op create --
 *
 */

bool OSDMonitor::preprocess_pool_op_create(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  int64_t pool = osdmap.lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op_reply(op, 0, osdmap.get_epoch());
    return true;
  }

  return false;
}

bool OSDMonitor::prepare_pool_op_create(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  int err = prepare_new_pool(op);
  wait_for_finished_proposal(op, 
      new OSDMonitor::C_PoolOp(this, op, err, pending_inc.epoch));
  return true;
}

int OSDMonitor::prepare_new_pool(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  dout(10) << "prepare_new_pool from " << m->get_connection() << dendl;
  MonSession *session = op->get_session();
  if (!session)
    return -EPERM;
  string erasure_code_profile;
  stringstream ss;
  string rule_name;
  int ret = 0;
  ret = prepare_new_pool(m->name, m->crush_rule, rule_name,
			 0, 0, 0, 0, 0, 0.0,
			 erasure_code_profile,
			 pg_pool_t::TYPE_REPLICATED, 0, FAST_READ_OFF, &ss);

  if (ret < 0) {
    dout(10) << __func__ << " got " << ret << " " << ss.str() << dendl;
  }
  return ret;
}

/**
 * @param name The name of the new pool
 * @param crush_rule The crush rule to use. If <0, will use the system default
 * @param crush_rule_name The crush rule to use, if crush_rulset <0
 * @param pg_num The pg_num to use. If set to 0, will use the system default
 * @param pgp_num The pgp_num to use. If set to 0, will use the system default
 * @param repl_size Replication factor, or 0 for default
 * @param erasure_code_profile The profile name in OSDMap to be used for erasure code
 * @param pool_type TYPE_ERASURE, or TYPE_REP
 * @param expected_num_objects expected number of objects on the pool
 * @param fast_read fast read type. 
 * @param ss human readable error message, if any.
 *
 * @return 0 on success, negative errno on failure.
 */
int OSDMonitor::prepare_new_pool(string& name,
				 int crush_rule,
				 const string &crush_rule_name,
                                 unsigned pg_num, unsigned pgp_num,
				 unsigned pg_num_min,
                                 const uint64_t repl_size,
				 const uint64_t target_size_bytes,
				 const float target_size_ratio,
				 const string &erasure_code_profile,
                                 const unsigned pool_type,
                                 const uint64_t expected_num_objects,
                                 FastReadType fast_read,
				 ostream *ss)
{
  if (name.length() == 0)
    return -EINVAL;
  if (pg_num == 0)
    pg_num = g_conf().get_val<uint64_t>("osd_pool_default_pg_num");
  if (pgp_num == 0)
    pgp_num = g_conf().get_val<uint64_t>("osd_pool_default_pgp_num");
  if (!pgp_num)
    pgp_num = pg_num;
  if (pg_num > g_conf().get_val<uint64_t>("mon_max_pool_pg_num")) {
    *ss << "'pg_num' must be greater than 0 and less than or equal to "
        << g_conf().get_val<uint64_t>("mon_max_pool_pg_num")
        << " (you may adjust 'mon max pool pg num' for higher values)";
    return -ERANGE;
  }
  if (pgp_num > pg_num) {
    *ss << "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'"
        << ", which in this case is " << pg_num;
    return -ERANGE;
  }
  if (pool_type == pg_pool_t::TYPE_REPLICATED && fast_read == FAST_READ_ON) {
    *ss << "'fast_read' can only apply to erasure coding pool";
    return -EINVAL;
  }
  int r;
  r = prepare_pool_crush_rule(pool_type, erasure_code_profile,
				 crush_rule_name, &crush_rule, ss);
  if (r) {
    dout(10) << "prepare_pool_crush_rule returns " << r << dendl;
    return r;
  }
  if (g_conf()->mon_osd_crush_smoke_test) {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    ostringstream err;
    CrushTester tester(newcrush, err);
    tester.set_min_x(0);
    tester.set_max_x(50);
    tester.set_rule(crush_rule);
    auto start = ceph::coarse_mono_clock::now();
    r = tester.test_with_fork(g_conf()->mon_lease);
    auto duration = ceph::coarse_mono_clock::now() - start;
    if (r < 0) {
      dout(10) << "tester.test_with_fork returns " << r
	       << ": " << err.str() << dendl;
      *ss << "crush test failed with " << r << ": " << err.str();
      return r;
    }
    dout(10) << __func__ << " crush smoke test duration: "
             << duration << dendl;
  }
  unsigned size, min_size;
  r = prepare_pool_size(pool_type, erasure_code_profile, repl_size,
                        &size, &min_size, ss);
  if (r) {
    dout(10) << "prepare_pool_size returns " << r << dendl;
    return r;
  }
  r = check_pg_num(-1, pg_num, size, ss);
  if (r) {
    dout(10) << "check_pg_num returns " << r << dendl;
    return r;
  }

  if (!osdmap.crush->check_crush_rule(crush_rule, pool_type, size, *ss)) {
    return -EINVAL;
  }

  uint32_t stripe_width = 0;
  r = prepare_pool_stripe_width(pool_type, erasure_code_profile,
				&stripe_width, ss);
  if (r) {
    dout(10) << "prepare_pool_stripe_width returns " << r << dendl;
    return r;
  }
  
  bool fread = false;
  if (pool_type == pg_pool_t::TYPE_ERASURE) {
    switch (fast_read) {
      case FAST_READ_OFF:
        fread = false;
        break;
      case FAST_READ_ON:
        fread = true;
        break;
      case FAST_READ_DEFAULT:
        fread = g_conf()->osd_pool_default_ec_fast_read;
        break;
      default:
        *ss << "invalid fast_read setting: " << fast_read;
        return -EINVAL;
    }
  }

  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == name)
      return 0;
  }

  if (-1 == pending_inc.new_pool_max)
    pending_inc.new_pool_max = osdmap.pool_max;
  int64_t pool = ++pending_inc.new_pool_max;
  pg_pool_t empty;
  pg_pool_t *pi = pending_inc.get_new_pool(pool, &empty);
  pi->create_time = ceph_clock_now();
  pi->type = pool_type;
  pi->fast_read = fread; 
  pi->flags = g_conf()->osd_pool_default_flags;
  if (g_conf()->osd_pool_default_flag_hashpspool)
    pi->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  if (g_conf()->osd_pool_default_flag_nodelete)
    pi->set_flag(pg_pool_t::FLAG_NODELETE);
  if (g_conf()->osd_pool_default_flag_nopgchange)
    pi->set_flag(pg_pool_t::FLAG_NOPGCHANGE);
  if (g_conf()->osd_pool_default_flag_nosizechange)
    pi->set_flag(pg_pool_t::FLAG_NOSIZECHANGE);
  pi->set_flag(pg_pool_t::FLAG_CREATING);
  if (g_conf()->osd_pool_use_gmt_hitset)
    pi->use_gmt_hitset = true;
  else
    pi->use_gmt_hitset = false;

  pi->size = size;
  pi->min_size = min_size;
  pi->crush_rule = crush_rule;
  pi->expected_num_objects = expected_num_objects;
  pi->object_hash = CEPH_STR_HASH_RJENKINS;

  {
    auto m = pg_pool_t::get_pg_autoscale_mode_by_name(
      g_conf().get_val<string>("osd_pool_default_pg_autoscale_mode"));
    pi->pg_autoscale_mode = m >= 0 ? m : 0;
  }
  auto max = g_conf().get_val<int64_t>("mon_osd_max_initial_pgs");
  pi->set_pg_num(
    max > 0 ? std::min<uint64_t>(pg_num, std::max<int64_t>(1, max))
    : pg_num);
  pi->set_pg_num_pending(pi->get_pg_num());
  pi->set_pg_num_target(pg_num);
  pi->set_pgp_num(pi->get_pg_num());
  pi->set_pgp_num_target(pgp_num);
  if (osdmap.require_osd_release >= ceph_release_t::nautilus &&
      pg_num_min) {
    pi->opts.set(pool_opts_t::PG_NUM_MIN, static_cast<int64_t>(pg_num_min));
  }

  pi->last_change = pending_inc.epoch;
  pi->auid = 0;

  if (pool_type == pg_pool_t::TYPE_ERASURE) {
      pi->erasure_code_profile = erasure_code_profile;
  } else {
      pi->erasure_code_profile = "";
  }
  pi->stripe_width = stripe_width;

  if (osdmap.require_osd_release >= ceph_release_t::nautilus &&
      target_size_bytes) {
    // only store for nautilus+ because TARGET_SIZE_BYTES may be
    // larger than int32_t max.
    pi->opts.set(pool_opts_t::TARGET_SIZE_BYTES,
	static_cast<int64_t>(target_size_bytes));
  }
  if (target_size_ratio > 0.0 &&
      osdmap.require_osd_release >= ceph_release_t::nautilus) {
    // only store for nautilus+, just to be consistent and tidy.
    pi->opts.set(pool_opts_t::TARGET_SIZE_RATIO, target_size_ratio);
  }

  pi->cache_target_dirty_ratio_micro =
    g_conf()->osd_pool_default_cache_target_dirty_ratio * 1000000;
  pi->cache_target_dirty_high_ratio_micro =
    g_conf()->osd_pool_default_cache_target_dirty_high_ratio * 1000000;
  pi->cache_target_full_ratio_micro =
    g_conf()->osd_pool_default_cache_target_full_ratio * 1000000;
  pi->cache_min_flush_age = g_conf()->osd_pool_default_cache_min_flush_age;
  pi->cache_min_evict_age = g_conf()->osd_pool_default_cache_min_evict_age;

  pending_inc.new_pool_names[pool] = name;
  return 0;
}

int OSDMonitor::prepare_pool_size(const unsigned pool_type,
				  const string &erasure_code_profile,
                                  uint8_t repl_size,
				  unsigned *size, unsigned *min_size,
				  ostream *ss)
{
  int err = 0;
  switch (pool_type) {
  case pg_pool_t::TYPE_REPLICATED:
    if (repl_size == 0) {
      repl_size = g_conf().get_val<uint64_t>("osd_pool_default_size");
    }
    *size = repl_size;
    *min_size = g_conf().get_osd_pool_default_min_size(repl_size);
    break;
  case pg_pool_t::TYPE_ERASURE:
    {
      ErasureCodeInterfaceRef erasure_code;
      err = get_erasure_code(erasure_code_profile, &erasure_code, ss);
      if (err == 0) {
	*size = erasure_code->get_chunk_count();
	*min_size =
	  erasure_code->get_data_chunk_count() +
	  std::min<int>(1, erasure_code->get_coding_chunk_count() - 1);
	assert(*min_size <= *size);
	assert(*min_size >= erasure_code->get_data_chunk_count());
      }
    }
    break;
  default:
    *ss << "prepare_pool_size: " << pool_type << " is not a known pool type";
    err = -EINVAL;
    break;
  }
  return err;
}

int OSDMonitor::prepare_pool_stripe_width(const unsigned pool_type,
					  const string &erasure_code_profile,
					  uint32_t *stripe_width,
					  ostream *ss)
{
  int err = 0;
  switch (pool_type) {
  case pg_pool_t::TYPE_REPLICATED:
    // ignored
    break;
  case pg_pool_t::TYPE_ERASURE:
    {
      ErasureCodeProfile profile =
	osdmap.get_erasure_code_profile(erasure_code_profile);
      ErasureCodeInterfaceRef erasure_code;
      err = get_erasure_code(erasure_code_profile, &erasure_code, ss);
      if (err)
	break;
      uint32_t data_chunks = erasure_code->get_data_chunk_count();
      uint32_t stripe_unit =
	g_conf().get_val<Option::size_t>("osd_pool_erasure_code_stripe_unit");
      auto it = profile.find("stripe_unit");
      if (it != profile.end()) {
	string err_str;
	stripe_unit = strict_iecstrtoll(it->second.c_str(), &err_str);
	ceph_assert(err_str.empty());
      }
      *stripe_width = data_chunks *
	erasure_code->get_chunk_size(stripe_unit * data_chunks);
    }
    break;
  default:
    *ss << "prepare_pool_stripe_width: "
       << pool_type << " is not a known pool type";
    err = -EINVAL;
    break;
  }
  return err;
}


/*
 * -- pool op delete --
 *
 */

bool OSDMonitor::prepare_pool_op_delete(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  ostringstream ss;
  int ret = _prepare_remove_pool(m->pool, &ss, false);
  if (ret == -EAGAIN) {
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
    return true;
  }
  if (ret < 0)
    dout(10) << __func__ << " got " << ret << " " << ss.str() << dendl;
  wait_for_finished_proposal(op, new OSDMonitor::C_PoolOp(this, op, ret,
						      pending_inc.epoch));
  return true;
}

int OSDMonitor::_prepare_remove_pool(
  int64_t pool, ostream *ss, bool no_fake)
{
  dout(10) << __func__ << " " << pool << dendl;
  const pg_pool_t *p = osdmap.get_pg_pool(pool);
  int r = _check_remove_pool(pool, *p, ss);
  if (r < 0)
    return r;

  auto new_pool = pending_inc.new_pools.find(pool);
  if (new_pool != pending_inc.new_pools.end()) {
    // if there is a problem with the pending info, wait and retry
    // this op.
    const auto& p = new_pool->second;
    int r = _check_remove_pool(pool, p, ss);
    if (r < 0)
      return -EAGAIN;
  }

  if (pending_inc.old_pools.count(pool)) {
    dout(10) << __func__ << " " << pool << " already pending removal"
	     << dendl;
    return 0;
  }

  if (g_conf()->mon_fake_pool_delete && !no_fake) {
    string old_name = osdmap.get_pool_name(pool);
    string new_name = old_name + "." + stringify(pool) + ".DELETED";
    dout(1) << __func__ << " faking pool deletion: renaming " << pool << " "
	    << old_name << " -> " << new_name << dendl;
    pending_inc.new_pool_names[pool] = new_name;
    return 0;
  }

  // remove
  pending_inc.old_pools.insert(pool);

  // remove any pg_temp mappings for this pool
  for (auto p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {
    if (p->first.pool() == pool) {
      dout(10) << __func__ << " " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc.new_pg_temp[p->first].clear();
    }
  }
  // remove any primary_temp mappings for this pool
  for (auto p = osdmap.primary_temp->begin();
      p != osdmap.primary_temp->end();
      ++p) {
    if (p->first.pool() == pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete primary_temp" << p->first << dendl;
      pending_inc.new_primary_temp[p->first] = -1;
    }
  }
  // remove any pg_upmap mappings for this pool
  for (auto& p : osdmap.pg_upmap) {
    if (p.first.pool() == pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete pg_upmap "
               << p.first << dendl;
      pending_inc.old_pg_upmap.insert(p.first);
    }
  }
  // remove any pending pg_upmap mappings for this pool
  {
    auto it = pending_inc.new_pg_upmap.begin();
    while (it != pending_inc.new_pg_upmap.end()) {
      if (it->first.pool() == pool) {
        dout(10) << __func__ << " " << pool
                 << " removing pending pg_upmap "
                 << it->first << dendl;
        it = pending_inc.new_pg_upmap.erase(it);
      } else {
        it++;
      }
    }
  }
  // remove any pg_upmap_items mappings for this pool
  for (auto& p : osdmap.pg_upmap_items) {
    if (p.first.pool() == pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete pg_upmap_items " << p.first
               << dendl;
      pending_inc.old_pg_upmap_items.insert(p.first);
    }
  }
  // remove any pending pg_upmap mappings for this pool
  {
    auto it = pending_inc.new_pg_upmap_items.begin();
    while (it != pending_inc.new_pg_upmap_items.end()) {
      if (it->first.pool() == pool) {
        dout(10) << __func__ << " " << pool
                 << " removing pending pg_upmap_items "
                 << it->first << dendl;
        it = pending_inc.new_pg_upmap_items.erase(it);
      } else {
        it++;
      }
    }
  }

  // remove any choose_args for this pool
  CrushWrapper newcrush;
  _get_pending_crush(newcrush);
  if (newcrush.have_choose_args(pool)) {
    dout(10) << __func__ << " removing choose_args for pool " << pool << dendl;
    newcrush.rm_choose_args(pool);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  }
  return 0;
}

int OSDMonitor::_check_remove_pool(int64_t pool_id, const pg_pool_t& pool,
				   ostream *ss)
{
  const string& poolstr = osdmap.get_pool_name(pool_id);

  // If the Pool is in use by CephFS, refuse to delete it
  FSMap const &pending_fsmap = mon->mdsmon()->get_pending_fsmap();
  if (pending_fsmap.pool_in_use(pool_id)) {
    *ss << "pool '" << poolstr << "' is in use by CephFS";
    return -EBUSY;
  }

  if (pool.tier_of >= 0) {
    *ss << "pool '" << poolstr << "' is a tier of '"
	<< osdmap.get_pool_name(pool.tier_of) << "'";
    return -EBUSY;
  }
  if (!pool.tiers.empty()) {
    *ss << "pool '" << poolstr << "' has tiers";
    for(auto tier : pool.tiers) {
      *ss << " " << osdmap.get_pool_name(tier);
    }
    return -EBUSY;
  }

  if (!g_conf()->mon_allow_pool_delete) {
    *ss << "pool deletion is disabled; "
	<< "you must first set the mon_allow_pool_delete config option "
	<< "to true before you can destroy a pool";
    return -EPERM;
  }

  if (pool.has_flag(pg_pool_t::FLAG_NODELETE)) {
    *ss << "pool deletion is disabled; "
	<< "you must unset nodelete flag for the pool first";
    return -EPERM;
  }

  *ss << "pool '" << poolstr << "' removed";
  return 0;
}


/*
 * -- pools status --
 *
 */

bool OSDMonitor::update_pools_status()
{
  if (!mon->mgrstatmon()->is_readable())
    return false;

  bool ret = false;

  auto& pools = osdmap.get_pools();
  for (auto it = pools.begin(); it != pools.end(); ++it) {
    const pool_stat_t *pstat = mon->mgrstatmon()->get_pool_stat(it->first);
    if (!pstat)
      continue;
    const object_stat_sum_t& sum = pstat->stats.sum;
    const pg_pool_t &pool = it->second;
    const string& pool_name = osdmap.get_pool_name(it->first);

    bool pool_is_full =
      (pool.quota_max_bytes > 0 &&
	(uint64_t)sum.num_bytes >= pool.quota_max_bytes) ||
      (pool.quota_max_objects > 0 &&
	(uint64_t)sum.num_objects >= pool.quota_max_objects);

    if (pool.has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
      if (pool_is_full)
        continue;

      mon->clog->info() << "pool '" << pool_name
                       << "' no longer out of quota; removing NO_QUOTA flag";
      // below we cancel FLAG_FULL too, we'll set it again in
      // OSDMonitor::encode_pending if it still fails the osd-full checking.
      clear_pool_flags(it->first,
                       pg_pool_t::FLAG_FULL_QUOTA | pg_pool_t::FLAG_FULL);
      ret = true;
    } else {
      if (!pool_is_full)
	continue;

      if (pool.quota_max_bytes > 0 &&
          (uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
        mon->clog->warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_bytes: "
                         << byte_u_t(pool.quota_max_bytes) << ")";
      }
      if (pool.quota_max_objects > 0 &&
		 (uint64_t)sum.num_objects >= pool.quota_max_objects) {
        mon->clog->warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_objects: "
                         << pool.quota_max_objects << ")";
      }
      // set both FLAG_FULL_QUOTA and FLAG_FULL
      // note that below we try to cancel FLAG_BACKFILLFULL/NEARFULL too
      // since FLAG_FULL should always take precedence
      set_pool_flags(it->first,
                     pg_pool_t::FLAG_FULL_QUOTA | pg_pool_t::FLAG_FULL);
      clear_pool_flags(it->first,
                       pg_pool_t::FLAG_NEARFULL |
                       pg_pool_t::FLAG_BACKFILLFULL);
      ret = true;
    }
  }
  return ret;
}

void OSDMonitor::set_pool_flags(int64_t pool_id, uint64_t flags)
{
  pg_pool_t *pool = pending_inc.get_new_pool(pool_id,
    osdmap.get_pg_pool(pool_id));
  ceph_assert(pool);
  pool->set_flag(flags);
}

void OSDMonitor::clear_pool_flags(int64_t pool_id, uint64_t flags)
{
  pg_pool_t *pool = pending_inc.get_new_pool(pool_id,
    osdmap.get_pg_pool(pool_id));
  ceph_assert(pool);
  pool->unset_flag(flags);
}

/*
 * pool commands
 *
 */

int OSDMonitor::prepare_command_pool_set(const cmdmap_t& cmdmap,
                                         stringstream& ss)
{
  string poolstr;
  cmd_getval(cct, cmdmap, "pool", poolstr);
  int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
  if (pool < 0) {
    ss << "unrecognized pool '" << poolstr << "'";
    return -ENOENT;
  }
  string var;
  cmd_getval(cct, cmdmap, "var", var);

  pg_pool_t p = *osdmap.get_pg_pool(pool);
  if (pending_inc.new_pools.count(pool))
    p = pending_inc.new_pools[pool];

  // accept val as a json string in the normal case (current
  // generation monitor).  parse out int or float values from the
  // string as needed.  however, if it is not a string, try to pull
  // out an int, in case an older monitor with an older json schema is
  // forwarding a request.
  string val;
  string interr, floaterr;
  int64_t n = 0;
  double f = 0;
  int64_t uf = 0;  // micro-f
  cmd_getval(cct, cmdmap, "val", val);

  // parse string as both int and float; different fields use different types.
  n = strict_strtoll(val.c_str(), 10, &interr);
  f = strict_strtod(val.c_str(), &floaterr);
  uf = llrintl(f * (double)1000000.0);

  if (!p.is_tier() &&
      (var == "hit_set_type" || var == "hit_set_period" ||
       var == "hit_set_count" || var == "hit_set_fpp" ||
       var == "target_max_objects" || var == "target_max_bytes" ||
       var == "cache_target_full_ratio" || var == "cache_target_dirty_ratio" ||
       var == "cache_target_dirty_high_ratio" || var == "use_gmt_hitset" ||
       var == "cache_min_flush_age" || var == "cache_min_evict_age" ||
       var == "hit_set_grade_decay_rate" || var == "hit_set_search_last_n" ||
       var == "min_read_recency_for_promote" ||
       var == "min_write_recency_for_promote")) {
    return -EACCES;
  }

  if (var == "size") {
    if (p.has_flag(pg_pool_t::FLAG_NOSIZECHANGE)) {
      ss << "pool size change is disabled; "
	 << "you must unset nosizechange flag for the pool first";
      return -EPERM;
    }
    if (p.type == pg_pool_t::TYPE_ERASURE) {
      ss << "can not change the size of an erasure-coded pool";
      return -ENOTSUP;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= 0 || n > 10) {
      ss << "pool size must be between 1 and 10";
      return -EINVAL;
    }
    int r = check_pg_num(pool, p.get_pg_num(), n, &ss);
    if (r < 0) {
      return r;
    }
    p.size = n;
    if (n < p.min_size)
      p.min_size = n;
  } else if (var == "min_size") {
    if (p.has_flag(pg_pool_t::FLAG_NOSIZECHANGE)) {
      ss << "pool min size change is disabled; "
	 << "you must unset nosizechange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }

    if (p.type != pg_pool_t::TYPE_ERASURE) {
      if (n < 1 || n > p.size) {
	ss << "pool min_size must be between 1 and size, which is set to "
	   << (int)p.size;
	return -EINVAL;
      }
    } else {
       ErasureCodeInterfaceRef erasure_code;
       int k;
       stringstream tmp;
       int err = get_erasure_code(p.erasure_code_profile, &erasure_code, &tmp);
       if (err == 0) {
	 k = erasure_code->get_data_chunk_count();
       } else {
	 ss << __func__ << " get_erasure_code failed: " << tmp.str();
	 return err;
       }

       if (n < k || n > p.size) {
	 ss << "pool min_size must be between "
	    << k << " and size, which is set to " << (int)p.size;
	 return -EINVAL;
       }
    }
    p.min_size = n;
  } else if (var == "pg_num_actual") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n == (int)p.get_pg_num()) {
      return 0;
    }
    if (static_cast<uint64_t>(n) >
	g_conf().get_val<uint64_t>("mon_max_pool_pg_num")) {

      ss << "'pg_num' must be greater than 0 and less than or equal to "
         << g_conf().get_val<uint64_t>("mon_max_pool_pg_num")
         << " (you may adjust 'mon max pool pg num' for higher values)";
      return -ERANGE;
    }
    if (p.has_flag(pg_pool_t::FLAG_CREATING)) {
      ss << "cannot adjust pg_num while initial PGs are being created";
      return -EBUSY;
    }
    if (n > (int)p.get_pg_num()) {
      if (p.get_pg_num() != p.get_pg_num_pending()) {
	// force pre-nautilus clients to resend their ops, since they
	// don't understand pg_num_pending changes form a new interval
	p.last_force_op_resend_prenautilus = pending_inc.epoch;
      }
      p.set_pg_num(n);
    } else {
      if (osdmap.require_osd_release < ceph_release_t::nautilus) {
	ss << "nautilus OSDs are required to adjust pg_num_pending";
	return -EPERM;
      }
      if (n < (int)p.get_pgp_num()) {
	ss << "specified pg_num " << n << " < pgp_num " << p.get_pgp_num();
	return -EINVAL;
      }
      if (n < (int)p.get_pg_num() - 1) {
	ss << "specified pg_num " << n << " < pg_num (" << p.get_pg_num()
	   << ") - 1; only single pg decrease is currently supported";
	return -EINVAL;
      }
      p.set_pg_num_pending(n);
      // force pre-nautilus clients to resend their ops, since they
      // don't understand pg_num_pending changes form a new interval
      p.last_force_op_resend_prenautilus = pending_inc.epoch;
    }
    // force pre-luminous clients to resend their ops, since they
    // don't understand that split PGs now form a new interval.
    p.last_force_op_resend_preluminous = pending_inc.epoch;
  } else if (var == "pg_num") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pg_num change is disabled; "
	 << "you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n == (int)p.get_pg_num_target()) {
      return 0;
    }
    if (n <= 0 || static_cast<uint64_t>(n) >
                  g_conf().get_val<uint64_t>("mon_max_pool_pg_num")) {
      ss << "'pg_num' must be greater than 0 and less than or equal to "
         << g_conf().get_val<uint64_t>("mon_max_pool_pg_num")
         << " (you may adjust 'mon max pool pg num' for higher values)";
      return -ERANGE;
    }
    if (n > (int)p.get_pg_num_target()) {
      int r = check_pg_num(pool, n, p.get_size(), &ss);
      if (r) {
	return r;
      }
      bool force = false;
      cmd_getval(cct,cmdmap, "yes_i_really_mean_it", force);
      if (p.cache_mode != pg_pool_t::CACHEMODE_NONE && !force) {
	ss << "splits in cache pools must be followed by scrubs and leave sufficient free space to avoid overfilling.  use --yes-i-really-mean-it to force.";
	return -EPERM;
      }
    } else {
      if (osdmap.require_osd_release < ceph_release_t::nautilus) {
	ss << "nautilus OSDs are required to decrease pg_num";
	return -EPERM;
      }
    }
    if (osdmap.require_osd_release < ceph_release_t::nautilus) {
      // pre-nautilus osdmap format; increase pg_num directly
      assert(n > (int)p.get_pg_num());
      // force pre-nautilus clients to resend their ops, since they
      // don't understand pg_num_target changes form a new interval
      p.last_force_op_resend_prenautilus = pending_inc.epoch;
      // force pre-luminous clients to resend their ops, since they
      // don't understand that split PGs now form a new interval.
      p.last_force_op_resend_preluminous = pending_inc.epoch;
      p.set_pg_num(n);
    } else {
      // set targets; mgr will adjust pg_num_actual and pgp_num later.
      // make pgp_num track pg_num if it already matches.  if it is set
      // differently, leave it different and let the user control it
      // manually.
      if (p.get_pg_num_target() == p.get_pgp_num_target()) {
	p.set_pgp_num_target(n);
      }
      p.set_pg_num_target(n);
    }
  } else if (var == "pgp_num_actual") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pgp_num change is disabled; "
	 << "you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= 0) {
      ss << "specified pgp_num must > 0, but you set to " << n;
      return -EINVAL;
    }
    if (n > (int)p.get_pg_num()) {
      ss << "specified pgp_num " << n << " > pg_num " << p.get_pg_num();
      return -EINVAL;
    }
    if (n > (int)p.get_pg_num_pending()) {
      ss << "specified pgp_num " << n
	 << " > pg_num_pending " << p.get_pg_num_pending();
      return -EINVAL;
    }
    p.set_pgp_num(n);
  } else if (var == "pgp_num") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pgp_num change is disabled; "
	 << "you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= 0) {
      ss << "specified pgp_num must > 0, but you set to " << n;
      return -EINVAL;
    }
    if (n > (int)p.get_pg_num_target()) {
      ss << "specified pgp_num " << n << " > pg_num " << p.get_pg_num_target();
      return -EINVAL;
    }
    if (osdmap.require_osd_release < ceph_release_t::nautilus) {
      // pre-nautilus osdmap format; increase pgp_num directly
      p.set_pgp_num(n);
    } else {
      p.set_pgp_num_target(n);
    }
  } else if (var == "pg_autoscale_mode") {
    n = pg_pool_t::get_pg_autoscale_mode_by_name(val);
    if (n < 0) {
      ss << "specified invalid mode " << val;
      return -EINVAL;
    }
    if (osdmap.require_osd_release < ceph_release_t::nautilus) {
      ss << "must set require_osd_release to nautilus or later before setting pg_autoscale_mode";
      return -EINVAL;
    }
    p.pg_autoscale_mode = n;
  } else if (var == "crush_rule") {
    int id = osdmap.crush->get_rule_id(val);
    if (id == -ENOENT) {
      ss << "crush rule " << val << " does not exist";
      return -ENOENT;
    }
    if (id < 0) {
      ss << cpp_strerror(id);
      return -ENOENT;
    }
    if (!osdmap.crush->check_crush_rule(id, p.get_type(), p.get_size(), ss)) {
      return -EINVAL;
    }
    p.crush_rule = id;
  } else if (var == "nodelete" || var == "nopgchange" ||
	     var == "nosizechange" || var == "write_fadvise_dontneed" ||
	     var == "noscrub" || var == "nodeep-scrub") {
    uint64_t flag = pg_pool_t::get_flag_by_name(var);
    // make sure we only compare against 'n' if we didn't receive a string
    if (val == "true" || (interr.empty() && n == 1)) {
      p.set_flag(flag);
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.unset_flag(flag);
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "hashpspool") {
    uint64_t flag = pg_pool_t::get_flag_by_name(var);
    bool force = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", force);

    if (!force) {
      ss << "are you SURE?  this will remap all placement groups in this pool,"
	    " this triggers large data movement,"
	    " pass --yes-i-really-mean-it if you really do.";
      return -EPERM;
    }
    // make sure we only compare against 'n' if we didn't receive a string
    if (val == "true" || (interr.empty() && n == 1)) {
      p.set_flag(flag);
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.unset_flag(flag);
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "hit_set_type") {
    if (val == "none")
      p.hit_set_params = HitSet::Params();
    else {
      int err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
      if (err)
	return err;
      if (val == "bloom") {
	BloomHitSet::Params *bsp = new BloomHitSet::Params;
	bsp->set_fpp(g_conf().get_val<double>("osd_pool_default_hit_set_bloom_fpp"));
	p.hit_set_params = HitSet::Params(bsp);
      } else if (val == "explicit_hash")
	p.hit_set_params = HitSet::Params(new ExplicitHashHitSet::Params);
      else if (val == "explicit_object")
	p.hit_set_params = HitSet::Params(new ExplicitObjectHitSet::Params);
      else {
	ss << "unrecognized hit_set type '" << val << "'";
	return -EINVAL;
      }
    }
  } else if (var == "hit_set_period") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    } else if (n < 0) {
      ss << "hit_set_period should be non-negative";
      return -EINVAL;
    }
    p.hit_set_period = n;
  } else if (var == "hit_set_count") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    } else if (n < 0) {
      ss << "hit_set_count should be non-negative";
      return -EINVAL;
    }
    p.hit_set_count = n;
  } else if (var == "hit_set_fpp") {
    if (floaterr.length()) {
      ss << "error parsing floating point value '" << val << "': " << floaterr;
      return -EINVAL;
    } else if (f < 0 || f > 1.0) {
      ss << "hit_set_fpp should be in the range 0..1";
      return -EINVAL;
    }
    if (p.hit_set_params.get_type() != HitSet::TYPE_BLOOM) {
      ss << "hit set is not of type Bloom; "
	 << "invalid to set a false positive rate!";
      return -EINVAL;
    }
    BloomHitSet::Params *bloomp = static_cast<BloomHitSet::Params*>(p.hit_set_params.impl.get());
    bloomp->set_fpp(f);
  } else if (var == "use_gmt_hitset") {
    if (val == "true" || (interr.empty() && n == 1)) {
      p.use_gmt_hitset = true;
    } else {
      ss << "expecting value 'true' or '1'";
      return -EINVAL;
    }
  } else if (var == "allow_ec_overwrites") {
    if (!p.is_erasure()) {
      ss << "ec overwrites can only be enabled for an erasure coded pool";
      return -EINVAL;
    }
    stringstream err;
    if (!g_conf()->mon_debug_no_require_bluestore_for_ec_overwrites &&
	!is_pool_currently_all_bluestore(pool, p, &err)) {
      ss << "pool must only be stored on bluestore for scrubbing to work: "
	 << err.str();
      return -EINVAL;
    }
    if (val == "true" || (interr.empty() && n == 1)) {
	p.flags |= pg_pool_t::FLAG_EC_OVERWRITES;
    } else if (val == "false" || (interr.empty() && n == 0)) {
      ss << "ec overwrites cannot be disabled once enabled";
      return -EINVAL;
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "target_max_objects") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.target_max_objects = n;
  } else if (var == "target_max_bytes") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.target_max_bytes = n;
  } else if (var == "cache_target_dirty_ratio") {
    if (floaterr.length()) {
      ss << "error parsing float '" << val << "': " << floaterr;
      return -EINVAL;
    }
    if (f < 0 || f > 1.0) {
      ss << "value must be in the range 0..1";
      return -ERANGE;
    }
    p.cache_target_dirty_ratio_micro = uf;
  } else if (var == "cache_target_dirty_high_ratio") {
    if (floaterr.length()) {
      ss << "error parsing float '" << val << "': " << floaterr;
      return -EINVAL;
    }
    if (f < 0 || f > 1.0) {
      ss << "value must be in the range 0..1";
      return -ERANGE;
    }
    p.cache_target_dirty_high_ratio_micro = uf;
  } else if (var == "cache_target_full_ratio") {
    if (floaterr.length()) {
      ss << "error parsing float '" << val << "': " << floaterr;
      return -EINVAL;
    }
    if (f < 0 || f > 1.0) {
      ss << "value must be in the range 0..1";
      return -ERANGE;
    }
    p.cache_target_full_ratio_micro = uf;
  } else if (var == "cache_min_flush_age") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.cache_min_flush_age = n;
  } else if (var == "cache_min_evict_age") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.cache_min_evict_age = n;
  } else if (var == "min_read_recency_for_promote") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.min_read_recency_for_promote = n;
  } else if (var == "hit_set_grade_decay_rate") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n > 100 || n < 0) {
      ss << "value out of range,valid range is 0 - 100";
      return -EINVAL;
    }
    p.hit_set_grade_decay_rate = n;
  } else if (var == "hit_set_search_last_n") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n > p.hit_set_count || n < 0) {
      ss << "value out of range,valid range is 0 - hit_set_count";
      return -EINVAL;
    }
    p.hit_set_search_last_n = n;
  } else if (var == "min_write_recency_for_promote") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.min_write_recency_for_promote = n;
  } else if (var == "fast_read") {
    if (p.is_replicated()) {
        ss << "fast read is not supported in replication pool";
        return -EINVAL;
    }
    if (val == "true" || (interr.empty() && n == 1)) {
      p.fast_read = true;
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.fast_read = false;
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (pool_opts_t::is_opt_name(var)) {
    bool unset = val == "unset";
    if (var == "compression_mode") {
      if (!unset) {
        auto cmode = Compressor::get_comp_mode_type(val);
        if (!cmode) {
	  ss << "unrecognized compression mode '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "compression_algorithm") {
      if (!unset) {
        auto alg = Compressor::get_comp_alg_type(val);
        if (!alg) {
          ss << "unrecognized compression_algorithm '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "compression_required_ratio") {
      if (floaterr.length()) {
        ss << "error parsing float value '" << val << "': " << floaterr;
        return -EINVAL;
      }
      if (f < 0 || f > 1) {
        ss << "compression_required_ratio is out of range (0-1): '"
	   << val << "'";
	return -EINVAL;
      }
    } else if (var == "csum_type") {
      auto t = unset ? 0 : Checksummer::get_csum_string_type(val);
      if (t < 0 ) {
        ss << "unrecognized csum_type '" << val << "'";
	return -EINVAL;
      }
      //preserve csum_type numeric value
      n = t;
      interr.clear(); 
    } else if (var == "compression_max_blob_size" ||
               var == "compression_min_blob_size" ||
               var == "csum_max_block" ||
               var == "csum_min_block") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
    } else if (var == "fingerprint_algorithm") {
      if (!unset) {
        auto alg = pg_pool_t::get_fingerprint_from_str(val);
        if (!alg) {
          ss << "unrecognized fingerprint_algorithm '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "pg_num_min") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
      if (n > (int)p.get_pg_num_target()) {
	ss << "specified pg_num_min " << n
	   << " > pg_num " << p.get_pg_num_target();
	return -EINVAL;
      }
    } else if (var == "recovery_priority") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
      if (!g_conf()->debug_allow_any_pool_priority) {
        if (n > OSD_POOL_PRIORITY_MAX || n < OSD_POOL_PRIORITY_MIN) {
          ss << "pool recovery_priority must be between "
	     << OSD_POOL_PRIORITY_MIN << " and " << OSD_POOL_PRIORITY_MAX;
          return -EINVAL;
        }
      }
    } else if (var == "pg_autoscale_bias") {
      if (f < 0.0 || f > 1000.0) {
	ss << "pg_autoscale_bias must be between 0 and 1000";
	return -EINVAL;
      }
    }

    pool_opts_t::opt_desc_t desc = pool_opts_t::get_opt_desc(var);
    switch (desc.type) {
    case pool_opts_t::STR:
      if (unset) {
	p.opts.unset(desc.key);
      } else {
	p.opts.set(desc.key, static_cast<std::string>(val));
      }
      break;
    case pool_opts_t::INT:
      if (interr.length()) {
	ss << "error parsing integer value '" << val << "': " << interr;
	return -EINVAL;
      }
      if (n == 0) {
	p.opts.unset(desc.key);
      } else {
	p.opts.set(desc.key, static_cast<int64_t>(n));
      }
      break;
    case pool_opts_t::DOUBLE:
      if (floaterr.length()) {
	ss << "error parsing floating point value '" << val << "': " << floaterr;
	return -EINVAL;
      }
      if (f == 0) {
	p.opts.unset(desc.key);
      } else {
	p.opts.set(desc.key, static_cast<double>(f));
      }
      break;
    default:
      ceph_assert(!"unknown type");
    }
  } else {
    ss << "unrecognized variable '" << var << "'";
    return -EINVAL;
  }
  if (val != "unset") {
    ss << "set pool " << pool << " " << var << " to " << val;
  } else {
    ss << "unset pool " << pool << " " << var;
  }
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool] = p;
  return 0;
}

int OSDMonitor::prepare_command_pool_application(const string &prefix,
                                                 const cmdmap_t& cmdmap,
                                                 stringstream& ss)
{
  return _command_pool_application(prefix, cmdmap, ss, nullptr, true);
}

int OSDMonitor::preprocess_command_pool_application(const string &prefix,
                                                    const cmdmap_t& cmdmap,
                                                    stringstream& ss,
                                                    bool *modified)
{
  return _command_pool_application(prefix, cmdmap, ss, modified, false);
}


/**
 * Common logic for preprocess and prepare phases of pool application
 * tag commands.  In preprocess mode we're only detecting invalid
 * commands, and determining whether it was a modification or a no-op.
 * In prepare mode we're actually updating the pending state.
 */
int OSDMonitor::_command_pool_application(const string &prefix,
                                          const cmdmap_t& cmdmap,
                                          stringstream& ss,
                                          bool *modified,
                                          bool preparing)
{
  string pool_name;
  cmd_getval(cct, cmdmap, "pool", pool_name);
  int64_t pool = osdmap.lookup_pg_pool_name(pool_name.c_str());
  if (pool < 0) {
    ss << "unrecognized pool '" << pool_name << "'";
    return -ENOENT;
  }

  pg_pool_t p = *osdmap.get_pg_pool(pool);
  if (preparing) {
    if (pending_inc.new_pools.count(pool)) {
      p = pending_inc.new_pools[pool];
    }
  }

  string app;
  cmd_getval(cct, cmdmap, "app", app);
  bool app_exists = (p.application_metadata.count(app) > 0);

  string key;
  cmd_getval(cct, cmdmap, "key", key);
  if (key == "all") {
    ss << "key cannot be 'all'";
    return -EINVAL;
  }

  string value;
  cmd_getval(cct, cmdmap, "value", value);
  if (value == "all") {
    ss << "value cannot be 'all'";
    return -EINVAL;
  }

  if (boost::algorithm::ends_with(prefix, "enable")) {
    if (app.empty()) {
      ss << "application name must be provided";
      return -EINVAL;
    }

    if (p.is_tier()) {
      ss << "application must be enabled on base tier";
      return -EINVAL;
    }

    bool force = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", force);

    if (!app_exists && !p.application_metadata.empty() && !force) {
      ss << "Are you SURE? Pool '" << pool_name << "' already has an enabled "
         << "application; pass --yes-i-really-mean-it to proceed anyway";
      return -EPERM;
    }

    if (!app_exists && p.application_metadata.size() >= MAX_POOL_APPLICATIONS) {
      ss << "too many enabled applications on pool '" << pool_name << "'; "
         << "max " << MAX_POOL_APPLICATIONS;
      return -EINVAL;
    }

    if (app.length() > MAX_POOL_APPLICATION_LENGTH) {
      ss << "application name '" << app << "' too long; max length "
         << MAX_POOL_APPLICATION_LENGTH;
      return -EINVAL;
    }

    if (!app_exists) {
      p.application_metadata[app] = {};
    }
    ss << "enabled application '" << app << "' on pool '" << pool_name << "'";

  } else if (boost::algorithm::ends_with(prefix, "disable")) {
    bool force = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", force);

    if (!force) {
      ss << "Are you SURE? Disabling an application within a pool might "
	 << "result in loss of application functionality; pass "
         << "--yes-i-really-mean-it to proceed anyway";
      return -EPERM;
    }

    if (!app_exists) {
      ss << "application '" << app << "' is not enabled on pool '" << pool_name
         << "'";
      return 0; // idempotent
    }

    p.application_metadata.erase(app);
    ss << "disable application '" << app << "' on pool '" << pool_name << "'";

  } else if (boost::algorithm::ends_with(prefix, "set")) {
    if (p.is_tier()) {
      ss << "application metadata must be set on base tier";
      return -EINVAL;
    }

    if (!app_exists) {
      ss << "application '" << app << "' is not enabled on pool '" << pool_name
         << "'";
      return -ENOENT;
    }

    string key;
    cmd_getval(cct, cmdmap, "key", key);

    if (key.empty()) {
      ss << "key must be provided";
      return -EINVAL;
    }

    auto &app_keys = p.application_metadata[app];
    if (app_keys.count(key) == 0 &&
        app_keys.size() >= MAX_POOL_APPLICATION_KEYS) {
      ss << "too many keys set for application '" << app << "' on pool '"
         << pool_name << "'; max " << MAX_POOL_APPLICATION_KEYS;
      return -EINVAL;
    }

    if (key.length() > MAX_POOL_APPLICATION_LENGTH) {
      ss << "key '" << app << "' too long; max length "
         << MAX_POOL_APPLICATION_LENGTH;
      return -EINVAL;
    }

    string value;
    cmd_getval(cct, cmdmap, "value", value);
    if (value.length() > MAX_POOL_APPLICATION_LENGTH) {
      ss << "value '" << value << "' too long; max length "
         << MAX_POOL_APPLICATION_LENGTH;
      return -EINVAL;
    }

    p.application_metadata[app][key] = value;
    ss << "set application '" << app << "' key '" << key << "' to '"
       << value << "' on pool '" << pool_name << "'";
  } else if (boost::algorithm::ends_with(prefix, "rm")) {
    if (!app_exists) {
      ss << "application '" << app << "' is not enabled on pool '" << pool_name
         << "'";
      return -ENOENT;
    }

    string key;
    cmd_getval(cct, cmdmap, "key", key);
    auto it = p.application_metadata[app].find(key);
    if (it == p.application_metadata[app].end()) {
      ss << "application '" << app << "' on pool '" << pool_name
         << "' does not have key '" << key << "'";
      return 0; // idempotent
    }

    p.application_metadata[app].erase(it);
    ss << "removed application '" << app << "' key '" << key << "' on pool '"
       << pool_name << "'";
  } else {
    ceph_abort();
  }

  if (preparing) {
    p.last_change = pending_inc.epoch;
    pending_inc.new_pools[pool] = p;
  }

  // Because we fell through this far, we didn't hit no-op cases,
  // so pool was definitely modified
  if (modified != nullptr) {
    *modified = true;
  }

  return 0;
}

int OSDMonitor::_prepare_rename_pool(int64_t pool, string newname)
{
  dout(10) << "_prepare_rename_pool " << pool << dendl;
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_rename_pool " << pool << " pending removal" << dendl;
    return -ENOENT;
  }
  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == newname && p->first != pool) {
      return -EEXIST;
    }
  }

  pending_inc.new_pool_names[pool] = newname;
  return 0;
}


