// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Ltd
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "OSDMonitor.h"

#include "FSCommands.h"
#include "MDSMonitor.h"
#include "MgrStatMonitor.h"
#include "mds/cephfs_features.h"

using TOPNSPC::common::cmd_getval;

using std::list;
using std::make_pair;
using std::pair;
using std::set;
using std::string;
using std::string_view;
using std::vector;
using std::ostream;


static const auto& APP_NAME_CEPHFS = pg_pool_t::APPLICATION_NAME_CEPHFS;

class FlagSetHandler : public FileSystemCommandHandler
{
  public:
  FlagSetHandler()
    : FileSystemCommandHandler("fs flag set")
  {
  }

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    string flag_name;
    cmd_getval(cmdmap, "flag_name", flag_name);

    string flag_val;
    cmd_getval(cmdmap, "val", flag_val);

    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);

    if (flag_name == "enable_multiple") {
      bool flag_bool = false;
      int r = parse_bool(flag_val, &flag_bool, ss);
      if (r != 0) {
        ss << "Invalid boolean value '" << flag_val << "'";
        return r;
      }

      fsmap.set_enable_multiple(flag_bool);
      return 0;
    } else {
      ss << "Unknown flag '" << flag_name << "'";
      return -EINVAL;
    }
  }
};

class FailHandler : public FileSystemCommandHandler
{
  public:
  FailHandler()
    : FileSystemCommandHandler("fs fail")
  {
  }

  int handle(
      Monitor* mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream& ss) override
  {
    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }

    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
    }

  bool confirm = false;
  cmd_getval(cmdmap, "yes_i_really_mean_it", confirm);
  if (!confirm &&
      mon->mdsmon()->has_health_warnings({
	MDS_HEALTH_TRIM, MDS_HEALTH_CACHE_OVERSIZED})) {
    ss << errmsg_for_unhealthy_mds;
    return -EPERM;
  }

    auto f = [](auto&& fs) {
      fs.get_mds_map().set_flag(CEPH_MDSMAP_NOT_JOINABLE);
    };
    fsmap.modify_filesystem(fsp->get_fscid(), std::move(f));

    vector<mds_gid_t> to_fail;
    for (const auto& p : fsp->get_mds_map().get_mds_info()) {
      to_fail.push_back(p.first);
    }

    for (const auto& gid : to_fail) {
      mon->mdsmon()->fail_mds_gid(fsmap, gid);
    }
    if (!to_fail.empty()) {
      mon->osdmon()->propose_pending();
    }

    ss << fs_name;
    ss << " marked not joinable; MDS cannot join the cluster. All MDS ranks marked failed.";

    return 0;
  }
};

class FsNewHandler : public FileSystemCommandHandler
{
  public:
  explicit FsNewHandler(Paxos *paxos)
    : FileSystemCommandHandler("fs new"), m_paxos(paxos)
  {
  }

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    ceph_assert(m_paxos->is_plugged());

    string metadata_name;
    cmd_getval(cmdmap, "metadata", metadata_name);
    int64_t metadata = mon->osdmon()->osdmap.lookup_pg_pool_name(metadata_name);
    if (metadata < 0) {
      ss << "pool '" << metadata_name << "' does not exist";
      return -ENOENT;
    }

    string data_name;
    cmd_getval(cmdmap, "data", data_name);
    int64_t data = mon->osdmon()->osdmap.lookup_pg_pool_name(data_name);
    if (data < 0) {
      ss << "pool '" << data_name << "' does not exist";
      return -ENOENT;
    }
    if (data == 0) {
      ss << "pool '" << data_name << "' has id 0, which CephFS does not allow. Use another pool or recreate it to get a non-zero pool id.";
      return -EINVAL;
    }

    string fs_name;
    cmd_getval(cmdmap, "fs_name", fs_name);
    if (fs_name.empty()) {
        // Ensure fs name is not empty so that we can implement
        // commmands that refer to FS by name in future.
        ss << "Filesystem name may not be empty";
        return -EINVAL;
    }

    if (auto* fsp = fsmap.get_filesystem(fs_name); fsp) {
      if (*(fsp->get_mds_map().get_data_pools().begin()) == data
          && fsp->get_mds_map().get_metadata_pool() == metadata) {
        // Identical FS created already, this is a no-op
        ss << "filesystem '" << fs_name << "' already exists";
        return 0;
      } else {
        ss << "filesystem already exists with name '" << fs_name << "'";
        return -EINVAL;
      }
    }

    bool force = false;
    cmd_getval(cmdmap, "force", force);

    const pool_stat_t *stat = mon->mgrstatmon()->get_pool_stat(metadata);
    if (stat) {
      int64_t metadata_num_objects = stat->stats.sum.num_objects;
      if (!force && metadata_num_objects > 0) {
	ss << "pool '" << metadata_name
	   << "' already contains some objects. Use an empty pool instead.";
	return -EINVAL;
      }
    }

    if (fsmap.filesystem_count() > 0
        && !fsmap.get_enable_multiple()) {
      ss << "Creation of multiple filesystems is disabled.  To enable "
            "this experimental feature, use 'ceph fs flag set enable_multiple "
            "true'";
      return -EINVAL;
    }

    bool allow_overlay = false;
    cmd_getval(cmdmap, "allow_dangerous_metadata_overlay", allow_overlay);

    for (const auto& [fscid, fs] : std::as_const(fsmap)) {
      const vector<int64_t> &data_pools = fs.get_mds_map().get_data_pools();
      if ((std::find(data_pools.begin(), data_pools.end(), data) != data_pools.end()
	   || fs.get_mds_map().get_metadata_pool() == metadata)
	  && !allow_overlay) {
	ss << "Filesystem '" << fs_name
	   << "' is already using one of the specified RADOS pools. This should ONLY be done in emergencies and after careful reading of the documentation. Pass --allow-dangerous-metadata-overlay to permit this.";
	return -EINVAL;
      }
    }

    int64_t fscid = FS_CLUSTER_ID_NONE;
    if (cmd_getval(cmdmap, "fscid", fscid)) {
      if (!force) {
        ss << "Pass --force to create a file system with a specific ID";
        return -EINVAL;
      }
      if (fsmap.filesystem_exists(fscid)) {
        ss << "filesystem already exists with id '" << fscid << "'";
        return -EINVAL;
      }
    }

    vector<string> fsops_vec;
    cmd_getval(cmdmap, "set", fsops_vec);
    if(!fsops_vec.empty()) {
      if(fsops_vec[0] != "set") {
        ss << "invalid command";
        return -EINVAL;
      }
      if(fsops_vec.size() % 2 == 0 || fsops_vec.size() < 2) {
      /* since "set" is part of fs options vector, if size of vec is divisble
      by 2, it indicates that the fsops key-value pairs are incomplete e.g.
      ["set", "max_mds", "2"]   # valid
      ["set", "max_mds"]        # invalid 
      */  
        ss << "incomplete list of key-val pairs provided "
           << fsops_vec.size() - 1;
        return -EINVAL;
      }
    }

    pg_pool_t const *data_pool = mon->osdmon()->osdmap.get_pg_pool(data);
    ceph_assert(data_pool != NULL);  // Checked it existed above
    pg_pool_t const *metadata_pool = mon->osdmon()->osdmap.get_pg_pool(metadata);
    ceph_assert(metadata_pool != NULL);  // Checked it existed above

    int r = _check_pool(mon->osdmon()->osdmap, data, POOL_DATA_DEFAULT, force, &ss, allow_overlay);
    if (r < 0) {
      return r;
    }

    r = _check_pool(mon->osdmon()->osdmap, metadata, POOL_METADATA, force, &ss, allow_overlay);
    if (r < 0) {
      return r;
    }
    
    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }

    bool recover = false;
    cmd_getval(cmdmap, "recover", recover);

    auto fs = fsmap.create_filesystem(fs_name, metadata, data, mon->get_quorum_con_features(), recover);

    // set fs options
    string set_fsops_info;
    for (size_t i = 1 ; i < fsops_vec.size() ; i+=2) {
      std::ostringstream oss;
      int ret = set_val(mon, fsmap, op, cmdmap, oss, &fs, fsops_vec[i], fsops_vec[i+1]);
      if (ret < 0) {
        ss << oss.str();
        return ret;
      }
      if ((i + 2) <= fsops_vec.size()) {
        set_fsops_info.append("; ");
      }
      set_fsops_info.append(oss.str());
    }

    {
      auto& cfs = fsmap.commit_filesystem(fscid, std::move(fs));

      ss << "new fs with metadata pool " << metadata << " and data pool " << data;
      ss << set_fsops_info;

      mon->osdmon()->do_application_enable(data,
					   pg_pool_t::APPLICATION_NAME_CEPHFS,
					   "data", fs_name, true);
      mon->osdmon()->do_application_enable(metadata,
					   pg_pool_t::APPLICATION_NAME_CEPHFS,
					   "metadata", fs_name, true);
      mon->osdmon()->do_set_pool_opt(metadata,
				     pool_opts_t::RECOVERY_PRIORITY,
				     static_cast<int64_t>(5));
      mon->osdmon()->do_set_pool_opt(metadata,
				     pool_opts_t::PG_NUM_MIN,
				     static_cast<int64_t>(16));
      mon->osdmon()->do_set_pool_opt(metadata,
				     pool_opts_t::PG_AUTOSCALE_BIAS,
				     static_cast<double>(4.0));
      mon->osdmon()->propose_pending();

      if (recover) {
        return 0;
      }

      // assign a standby to all the ranks to avoid health warnings
      for (int i = 0 ; i < cfs.get_mds_map().get_max_mds() ; ++i) {
        auto info = fsmap.find_replacement_for({cfs.get_fscid(), i});

        if (info) {
          mon->clog->info() << info->human_name() << " assigned to filesystem "
                            << cfs.get_mds_map().get_fs_name() << " as rank " << i;
          fsmap.promote(info->global_id, cfs.get_fscid(), i);
        } else {
          break;
        }
      }
    }

    return 0;
  }

private:
  Paxos *m_paxos;
};

class SetHandler : public FileSystemCommandHandler
{
public:
  SetHandler()
    : FileSystemCommandHandler("fs set")
  {}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    string var;
    if (!cmd_getval(cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      return -EINVAL;
    }
    string val;
    if (!cmd_getval(cmdmap, "val", val)) {
      return -EINVAL;
    }

    return set_val(mon, fsmap, op, cmdmap, ss, fsp->get_fscid(), var, val);
  }
};

static void modify_filesystem(FSMap& fsmap, auto&& fsv, auto&& fn)
{
  if (std::holds_alternative<Filesystem*>(fsv)) {
    fn(*std::get<Filesystem*>(fsv));
  } else if (std::holds_alternative<fs_cluster_id_t>(fsv)) {
    fsmap.modify_filesystem(std::get<fs_cluster_id_t>(fsv), std::move(fn));
  } else ceph_assert(0);
}

int FileSystemCommandHandler::set_val(Monitor *mon, FSMap& fsmap, MonOpRequestRef op,
            const cmdmap_t& cmdmap, std::ostream &ss, fs_or_fscid fsv,
            std::string var, std::string val)
{
  const Filesystem* fsp;
  if (std::holds_alternative<Filesystem*>(fsv)) {
    fsp = std::get<Filesystem*>(fsv);
  } else if (std::holds_alternative<fs_cluster_id_t>(fsv)) {
    fsp = &fsmap.get_filesystem(std::get<fs_cluster_id_t>(fsv));
  } else ceph_assert(0);

  {
    std::string interr;
    // we got a string.  see if it contains an int.
    int64_t n = strict_strtoll(val.c_str(), 10, &interr);
    if (var == "max_mds") {
      // NOTE: see also "mds set_max_mds", which can modify the same field.
      if (interr.length()) {
        ss << interr;
	return -EINVAL;
      }

      if (n <= 0) {
        ss << "You must specify at least one MDS";
        return -EINVAL;
      }

      if (n > 1 && n > fsp->get_mds_map().get_max_mds()) {
	if (fsp->get_mds_map().was_snaps_ever_allowed() &&
	    !fsp->get_mds_map().allows_multimds_snaps()) {
	  ss << "multi-active MDS is not allowed while there are snapshots possibly created by pre-mimic MDS";
	  return -EINVAL;
	}
      }
      if (n > MAX_MDS) {
        ss << "may not have more than " << MAX_MDS << " MDS ranks";
        return -EINVAL;
      }

      modify_filesystem(fsmap, fsv,
          [n](auto&& fs)
      {
	fs.get_mds_map().clear_flag(CEPH_MDSMAP_NOT_JOINABLE);
        fs.get_mds_map().set_max_mds(n);
      });
    } else if (var == "inline_data") {
      bool enable_inline = false;
      int r = parse_bool(val, &enable_inline, ss);
      if (r != 0) {
        return r;
      }

      if (enable_inline) {
        bool confirm = false;
        cmd_getval(cmdmap, "yes_i_really_really_mean_it", confirm);
	if (!confirm) {
	  ss << "Inline data support is deprecated and will be removed in a future release. "
	     << "Add --yes-i-really-really-mean-it if you are certain you want this enabled.";
	  return -EPERM;
	}
	ss << "inline data enabled";

        modify_filesystem(fsmap, fsv,
            [](auto&& fs)
        {
          fs.get_mds_map().set_inline_data_enabled(true);
        });
      } else {
	ss << "inline data disabled";
        modify_filesystem(fsmap, fsv,
            [](auto&& fs)
        {
          fs.get_mds_map().set_inline_data_enabled(false);
        });
      }
    } else if (var == "balancer") {
      if (val.empty()) {
        ss << "unsetting the metadata load balancer";
      } else {
        ss << "setting the metadata load balancer to " << val;
      }
      modify_filesystem(fsmap, fsv,
	[val](auto&& fs)
        {
          fs.get_mds_map().set_balancer(val);
        });
      return true;
    } else if (var == "bal_rank_mask") {
      if (val.empty()) {
        ss << "bal_rank_mask may not be empty";
	return -EINVAL;
      }

      if (fsp->get_mds_map().check_special_bal_rank_mask(val, MDSMap::BAL_RANK_MASK_TYPE_ANY) == false) {
	string bin_string;
	int r = fsp->get_mds_map().hex2bin(val, bin_string, MAX_MDS, ss);
	if (r != 0) {
	  return r;
	}
      }
      ss << "setting the metadata balancer rank mask to " << val;

      modify_filesystem(fsmap, fsv,
	[val](auto&& fs)
        {
          fs.get_mds_map().set_bal_rank_mask(val);
        });
      return true;
    } else if (var == "max_file_size") {
      if (interr.length()) {
	ss << var << " requires an integer value";
	return -EINVAL;
      }
      if (n < CEPH_MIN_STRIPE_UNIT) {
	ss << var << " must at least " << CEPH_MIN_STRIPE_UNIT;
	return -ERANGE;
      }
      modify_filesystem(fsmap, fsv,
          [n](auto&& fs)
      {
        fs.get_mds_map().set_max_filesize(n);
      });
    } else if (var == "max_xattr_size") {
      if (interr.length()) {
	ss << var << " requires an integer value";
	return -EINVAL;
      }
      modify_filesystem(fsmap, fsv,
          [n](auto&& fs)
      {
        fs.get_mds_map().set_max_xattr_size(n);
      });
    } else if (var == "allow_new_snaps") {
      bool enable_snaps = false;
      int r = parse_bool(val, &enable_snaps, ss);
      if (r != 0) {
        return r;
      }

      if (!enable_snaps) {
        modify_filesystem(fsmap, fsv,
            [](auto&& fs)
        {
          fs.get_mds_map().clear_snaps_allowed();
        });
	ss << "disabled new snapshots";
      } else {
        modify_filesystem(fsmap, fsv,
            [](auto&& fs)
        {
          fs.get_mds_map().set_snaps_allowed();
        });
	ss << "enabled new snapshots";
      }
    } else if (var == "allow_multimds") {
        ss << "Multiple MDS is always enabled. Use the max_mds"
           << " parameter to control the number of active MDSs"
           << " allowed. This command is DEPRECATED and will be"
           << " REMOVED from future releases.";
    } else if (var == "allow_multimds_snaps") {
      bool enable = false;
      int r = parse_bool(val, &enable, ss);
      if (r != 0) {
        return r;
      }

      string confirm;
      if (!cmd_getval(cmdmap, "confirm", confirm) ||
	  confirm != "--yes-i-am-really-a-mds") {
	ss << "Warning! This command is for MDS only. Do not run it manually";
	return -EPERM;
      }

      if (enable) {
	ss << "enabled multimds with snapshot";
        modify_filesystem(fsmap, fsv,
            [](auto&& fs)
        {
	  fs.get_mds_map().set_multimds_snaps_allowed();
        });
      } else {
	ss << "disabled multimds with snapshot";
        modify_filesystem(fsmap, fsv,
            [](auto&& fs)
        {
	  fs.get_mds_map().clear_multimds_snaps_allowed();
        });
      }
    } else if (var == "allow_dirfrags") {
        ss << "Directory fragmentation is now permanently enabled."
           << " This command is DEPRECATED and will be REMOVED from future releases.";
    } else if (var == "down") {
      bool is_down = false;
      int r = parse_bool(val, &is_down, ss);
      if (r != 0) {
        return r;
      }

      ss << fsp->get_mds_map().get_fs_name();

      modify_filesystem(fsmap, fsv,
          [is_down](auto&& fs)
      {
	if (is_down) {
          if (fs.get_mds_map().get_max_mds() > 0) {
	    fs.get_mds_map().set_old_max_mds();
	    fs.get_mds_map().set_max_mds(0);
          } /* else already down! */
	} else {
	  mds_rank_t oldmax = fs.get_mds_map().get_old_max_mds();
	  fs.get_mds_map().set_max_mds(oldmax ? oldmax : 1);
	}
      });

      if (is_down) {
	ss << " marked down. ";
      } else {
	ss << " marked up, max_mds = " << fsp->get_mds_map().get_max_mds();
      }
    } else if (var == "cluster_down" || var == "joinable") {
      bool joinable = true;
      int r = parse_bool(val, &joinable, ss);
      if (r != 0) {
        return r;
      }
      if (var == "cluster_down") {
        joinable = !joinable;
      }

      ss << fsp->get_mds_map().get_fs_name();

      modify_filesystem(fsmap, fsv,
          [joinable](auto&& fs)
      {
	if (joinable) {
	  fs.get_mds_map().clear_flag(CEPH_MDSMAP_NOT_JOINABLE);
	} else {
	  fs.get_mds_map().set_flag(CEPH_MDSMAP_NOT_JOINABLE);
	}
      });

      if (joinable) {
	ss << " marked joinable; MDS may join as newly active.";
      } else {
	ss << " marked not joinable; MDS cannot join as newly active.";
      }

      if (var == "cluster_down") {
        ss << " WARNING: cluster_down flag is deprecated and will be"
           << " removed in a future version. Please use \"joinable\".";
      }
    } else if (var == "standby_count_wanted") {
      if (interr.length()) {
       ss << var << " requires an integer value";
       return -EINVAL;
      }
      if (n < 0) {
       ss << var << " must be non-negative";
       return -ERANGE;
      }
      modify_filesystem(fsmap, fsv,
          [n](auto&& fs)
      {
        fs.get_mds_map().set_standby_count_wanted(n);
      });
    } else if (var == "session_timeout") {
      if (interr.length()) {
       ss << var << " requires an integer value";
       return -EINVAL;
      }
      if (n < 30) {
       ss << var << " must be at least 30s";
       return -ERANGE;
      }
      modify_filesystem(fsmap, fsv,
          [n](auto&& fs)
      {
        fs.get_mds_map().set_session_timeout((uint32_t)n);
      });
    } else if (var == "session_autoclose") {
      if (interr.length()) {
       ss << var << " requires an integer value";
       return -EINVAL;
      }
      if (n < 30) {
       ss << var << " must be at least 30s";
       return -ERANGE;
      }
      modify_filesystem(fsmap, fsv,
          [n](auto&& fs)
      {
        fs.get_mds_map().set_session_autoclose((uint32_t)n);
      });
    } else if (var == "allow_standby_replay") {
      bool allow = false;
      int r = parse_bool(val, &allow, ss);
      if (r != 0) {
        return r;
      }

      if (!allow) {
        if (!mon->osdmon()->is_writeable()) {
          // not allowed to write yet, so retry when we can
          mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
          return -EAGAIN;
        }
        vector<mds_gid_t> to_fail;
        for (const auto& [gid, info]: fsp->get_mds_map().get_mds_info()) {
          if (info.state == MDSMap::STATE_STANDBY_REPLAY) {
            to_fail.push_back(gid);
          }
        }

        for (const auto& gid : to_fail) {
          mon->mdsmon()->fail_mds_gid(fsmap, gid);
        }
        if (!to_fail.empty()) {
          mon->osdmon()->propose_pending();
        }
      }

      auto f = [allow](auto&& fs) {
        if (allow) {
          fs.get_mds_map().set_standby_replay_allowed();
        } else {
          fs.get_mds_map().clear_standby_replay_allowed();
        }
      };
      fsmap.modify_filesystem(fsp->get_fscid(), std::move(f));
    } else if (var == "balance_automate") {
      bool allow = false;
      int r = parse_bool(val, &allow, ss);
      if (r != 0) {
        return r;
      }

      auto f = [allow](auto&& fs) {
        if (allow) {
          fs.get_mds_map().set_balance_automate();
        } else {
          fs.get_mds_map().clear_balance_automate();
        }
      };
      fsmap.modify_filesystem(fsp->get_fscid(), std::move(f));
    } else if (var == "min_compat_client") {
      auto vno = ceph_release_from_name(val.c_str());
      if (!vno) {
	ss << "version " << val << " is not recognized";
	return -EINVAL;
      }
      ss << "WARNING: setting min_compat_client is deprecated"
            " and may not do what you want.\n"
            "The oldest release to set is octopus.\n"
            "Please migrate to `ceph fs required_client_features ...`.";
      auto f = [vno](auto&& fs) {
        fs.get_mds_map().set_min_compat_client(vno);
      };
      modify_filesystem(fsmap, fsv, std::move(f));
    } else if (var == "refuse_client_session") {
      bool refuse_session = false;
      int r = parse_bool(val, &refuse_session, ss);
      if (r != 0) {
        return r;
      }

      if (refuse_session) {
        if (!(fsp->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION))) {
          modify_filesystem(fsmap, fsv,
            [](auto&& fs)
          {
            fs.get_mds_map().set_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION);
          });
          ss << "client(s) blocked from establishing new session(s)"; 
        } else {
          ss << "client(s) already blocked from establishing new session(s)";
        }     
      } else {
          if (fsp->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION)) {
            modify_filesystem(fsmap, fsv,
              [](auto&& fs)
            {
              fs.get_mds_map().clear_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION);
            });
            ss << "client(s) allowed to establish new session(s)"; 
          } else {
            ss << "client(s) already allowed to establish new session(s)";
          }
      }
    } else if (var == "refuse_standby_for_another_fs") {
      bool refuse_standby_for_another_fs = false;
      int r = parse_bool(val, &refuse_standby_for_another_fs, ss);
      if (r != 0) {
        return r;
      }

      if (refuse_standby_for_another_fs) {
        if (!(fsp->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS))) {
          fsmap.modify_filesystem(
            fsp->get_fscid(),
            [](auto&& fs)
          {
            fs.get_mds_map().set_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS);
          });
          ss << "set to refuse standby for another fs";
        } else {
          ss << "to refuse standby for another fs is already set";
        }
      } else {
          if (fsp->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS)) {
            fsmap.modify_filesystem(
              fsp->get_fscid(),
              [](auto&& fs)
            {
              fs.get_mds_map().clear_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS);
            });
            ss << "allowed to use standby for another fs";
          } else {
            ss << "to use standby for another fs is already allowed";
          }
      }
    } else {
      ss << "unknown variable " << var;
      return -EINVAL;
    }
  }
  return 0;
}

class CompatSetHandler : public FileSystemCommandHandler
{
  public:
    CompatSetHandler()
      : FileSystemCommandHandler("fs compat")
    {
    }

    int handle(
	Monitor *mon,
	FSMap &fsmap,
	MonOpRequestRef op,
	const cmdmap_t& cmdmap,
	ostream &ss) override
    {
      static const set<string> subops = {"rm_incompat", "rm_compat", "add_incompat", "add_compat"};

      string fs_name;
      if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
	ss << "Missing filesystem name";
	return -EINVAL;
      }
      auto* fsp = fsmap.get_filesystem(fs_name);
      if (fsp == nullptr) {
	ss << "Not found: '" << fs_name << "'";
	return -ENOENT;
      }

      string subop;
      if (!cmd_getval(cmdmap, "subop", subop) || subops.count(subop) == 0) {
	ss << "subop `" << subop << "' not recognized. Must be one of: " << subops;
	return -EINVAL;
      }

      int64_t feature;
      if (!cmd_getval(cmdmap, "feature", feature) || feature <= 0) {
        ss << "Invalid feature";
        return -EINVAL;
      }

      if (fsp->get_mds_map().get_num_up_mds() > 0) {
        ss << "file system must be failed or down; use `ceph fs fail` to bring down";
        return -EBUSY;
      }

      CompatSet cs = fsp->get_mds_map().compat;
      if (subop == "rm_compat") {
        if (cs.compat.contains(feature)) {
          ss << "removed compat feature " << feature;
          cs.compat.remove(feature);
        } else {
          ss << "already removed compat feature " << feature;
        }
      } else if (subop == "rm_incompat") {
        if (cs.incompat.contains(feature)) {
          ss << "removed incompat feature " << feature;
          cs.incompat.remove(feature);
        } else {
          ss << "already removed incompat feature " << feature;
        }
      } else if (subop == "add_compat" || subop == "add_incompat") {
        string feature_str;
        if (!cmd_getval(cmdmap, "feature_str", feature_str) || feature_str.empty()) {
          ss << "adding a feature requires a feature string";
          return -EINVAL;
        }
        auto f = CompatSet::Feature(feature, feature_str);
        if (subop == "add_compat") {
          if (cs.compat.contains(feature)) {
            auto name = cs.compat.get_name(feature);
            if (name == feature_str) {
              ss << "feature already exists";
            } else {
              ss << "feature with differing name `" << name << "' exists";
              return -EEXIST;
            }
          } else {
            cs.compat.insert(f);
            ss << "added compat feature " << f;
          }
        } else if (subop == "add_incompat") {
          if (cs.incompat.contains(feature)) {
            auto name = cs.incompat.get_name(feature);
            if (name == feature_str) {
              ss << "feature already exists";
            } else {
              ss << "feature with differing name `" << name << "' exists";
              return -EEXIST;
            }
          } else {
            cs.incompat.insert(f);
            ss << "added incompat feature " << f;
          }
        } else ceph_assert(0);
      } else ceph_assert(0);

      auto modifyf = [cs = std::move(cs)](auto&& fs) {
        fs.get_mds_map().compat = cs;
      };

      fsmap.modify_filesystem(fsp->get_fscid(), std::move(modifyf));
      return 0;
    }
};

class RequiredClientFeaturesHandler : public FileSystemCommandHandler
{
  public:
    RequiredClientFeaturesHandler()
      : FileSystemCommandHandler("fs required_client_features")
    {
    }

    int handle(
	Monitor *mon,
	FSMap &fsmap,
	MonOpRequestRef op,
	const cmdmap_t& cmdmap,
	ostream &ss) override
    {
      string fs_name;
      if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
	ss << "Missing filesystem name";
	return -EINVAL;
      }
      auto* fsp = fsmap.get_filesystem(fs_name);
      if (fsp == nullptr) {
	ss << "Not found: '" << fs_name << "'";
	return -ENOENT;
      }
      string subop;
      if (!cmd_getval(cmdmap, "subop", subop) ||
	  (subop != "add" && subop != "rm")) {
	ss << "Must either add or rm a feature; " << subop << " is not recognized";
	return -EINVAL;
      }
      string val;
      if (!cmd_getval(cmdmap, "val", val) || val.empty()) {
	ss << "Missing feature id/name";
	return -EINVAL;
      }

      int feature = cephfs_feature_from_name(val);
      if (feature < 0) {
	string err;
	feature = strict_strtol(val.c_str(), 10, &err);
	if (err.length()) {
	  ss << "Invalid feature name: " << val;
	  return -EINVAL;
	}
	if (feature < 0 || feature > CEPHFS_FEATURE_MAX) {
	  ss << "Invalid feature id: " << feature;
	  return -EINVAL;
	}
      }

      if (subop == "add") {
	bool ret = false;
	fsmap.modify_filesystem(
	    fsp->get_fscid(),
	    [feature, &ret](auto&& fs)
	{
	  if (fs.get_mds_map().get_required_client_features().test(feature))
	    return;
	  fs.get_mds_map().add_required_client_feature(feature);
	  ret = true;
	});
	if (ret) {
	  ss << "added feature '" << cephfs_feature_name(feature) << "' to required_client_features";
	} else {
	  ss << "feature '" << cephfs_feature_name(feature) << "' is already set";
	}
      } else {
	bool ret = false;
	fsmap.modify_filesystem(
	    fsp->get_fscid(),
	    [feature, &ret](auto&& fs)
	{
          if (!fs.get_mds_map().get_required_client_features().test(feature))
            return;
          fs.get_mds_map().remove_required_client_feature(feature);
          ret = true;
	});
	if (ret) {
	  ss << "removed feature '" << cephfs_feature_name(feature) << "' from required_client_features";
	} else {
	  ss << "feature '" << cephfs_feature_name(feature) << "' is already unset";
	}
      }
      return 0;
   }
};


class AddDataPoolHandler : public FileSystemCommandHandler
{
  public:
  explicit AddDataPoolHandler(Paxos *paxos)
    : FileSystemCommandHandler("fs add_data_pool"), m_paxos(paxos)
  {}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    ceph_assert(m_paxos->is_plugged());

    string poolname;
    cmd_getval(cmdmap, "pool", poolname);

    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name)
        || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	ss << "pool '" << poolname << "' does not exist";
	return -ENOENT;
      }
    }

    int r = _check_pool(mon->osdmon()->osdmap, poolid, POOL_DATA_EXTRA, false, &ss);
    if (r != 0) {
      return r;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        return -ENOENT;
    }

    // no-op when the data_pool already on fs
    if (fsp->get_mds_map().is_data_pool(poolid)) {
      ss << "data pool " << poolid << " is already on fs " << fs_name;
      return 0;
    }

    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }
    mon->osdmon()->do_application_enable(poolid, APP_NAME_CEPHFS, "data",
					 fs_name, true);
    mon->osdmon()->propose_pending();

    fsmap.modify_filesystem(
        fsp->get_fscid(),
        [poolid](auto&&  fs)
    {
      fs.get_mds_map().add_data_pool(poolid);
    });

    ss << "added data pool " << poolid << " to fsmap";

    return 0;
  }

private:
  Paxos *m_paxos;
};

class SetDefaultHandler : public FileSystemCommandHandler
{
  public:
  SetDefaultHandler()
    : FileSystemCommandHandler("fs set-default")
  {}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    string fs_name;
    cmd_getval(cmdmap, "fs_name", fs_name);
    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        return -ENOENT;
    }

    fsmap.set_legacy_client_fscid(fsp->get_fscid());
    return 0;
  }
};

class RemoveFilesystemHandler : public FileSystemCommandHandler
{
  public:
  RemoveFilesystemHandler()
    : FileSystemCommandHandler("fs rm")
  {}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    /* We may need to blocklist ranks. */
    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }

    // Check caller has correctly named the FS to delete
    // (redundant while there is only one FS, but command
    //  syntax should apply to multi-FS future)
    string fs_name;
    cmd_getval(cmdmap, "fs_name", fs_name);
    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
        // Consider absence success to make deletes idempotent
        ss << "filesystem '" << fs_name << "' does not exist";
        return 0;
    }

    // Check that no MDS daemons are active
    if (fsp->get_mds_map().get_num_up_mds() > 0) {
      ss << "all MDS daemons must be inactive/failed before removing filesystem. See `ceph fs fail`.";
      return -EINVAL;
    }

    // Check for confirmation flag
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "this is a DESTRUCTIVE operation and will make data in your filesystem permanently" \
            " inaccessible.  Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    if (fsmap.get_legacy_client_fscid() == fsp->get_fscid()) {
      fsmap.set_legacy_client_fscid(FS_CLUSTER_ID_NONE);
    }

    vector<mds_gid_t> to_fail;
    // There may be standby_replay daemons left here
    for (const auto &i : fsp->get_mds_map().get_mds_info()) {
      ceph_assert(i.second.state == MDSMap::STATE_STANDBY_REPLAY);
      to_fail.push_back(i.first);
    }

    for (const auto &gid : to_fail) {
      // Standby replays don't write, so it isn't important to
      // wait for an osdmap propose here: ignore return value.
      mon->mdsmon()->fail_mds_gid(fsmap, gid);
    }
    if (!to_fail.empty()) {
      mon->osdmon()->propose_pending(); /* maybe new blocklists */
    }

    fsmap.erase_filesystem(fsp->get_fscid());

    return 0;
  }
};

class ResetFilesystemHandler : public FileSystemCommandHandler
{
  public:
  ResetFilesystemHandler()
    : FileSystemCommandHandler("fs reset")
  {}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    string fs_name;
    cmd_getval(cmdmap, "fs_name", fs_name);
    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        // Unlike fs rm, we consider this case an error
        return -ENOENT;
    }

    // Check that no MDS daemons are active
    if (fsp->get_mds_map().get_num_up_mds() > 0) {
      ss << "all MDS daemons must be inactive before resetting filesystem: set the cluster_down flag"
            " and use `ceph mds fail` to make this so";
      return -EINVAL;
    }

    // Check for confirmation flag
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "this is a potentially destructive operation, only for use by experts in disaster recovery.  "
        "Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    fsmap.reset_filesystem(fsp->get_fscid());

    return 0;
  }
};

class RenameFilesystemHandler : public FileSystemCommandHandler
{
  public:
  explicit RenameFilesystemHandler(Paxos *paxos)
    : FileSystemCommandHandler("fs rename"), m_paxos(paxos)
  {
  }

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    ceph_assert(m_paxos->is_plugged());

    string fs_name;
    cmd_getval(cmdmap, "fs_name", fs_name);
    auto* fsp = fsmap.get_filesystem(fs_name);

    string new_fs_name;
    cmd_getval(cmdmap, "new_fs_name", new_fs_name);
    auto* new_fsp = fsmap.get_filesystem(new_fs_name);

    if (fsp == nullptr) {
        if (new_fsp) {
          // make 'fs rename' idempotent
	  ss << "File system may already have been renamed. Desired file system '"
	     << new_fs_name << "' exists.";
	  return 0;
	} else {
	  ss << "File system '" << fs_name << "' does not exist";
	  return -ENOENT;
	}
    }

    if (new_fsp) {
      ss << "Desired file system name '" << new_fs_name << "' already in use";
      return -EINVAL;
    }

    if (fsp->get_mirror_info().mirrored) {
      ss << "Mirroring is enabled on file system '"<< fs_name << "'. Disable mirroring on the "
        "file system after ensuring it's OK to do so, and then retry to rename.";
      return -EPERM;
    }

    // Check for confirmation flag
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "this is a potentially disruptive operation, clients' cephx credentials need reauthorized "
        "to access the file system and its pools with the new name. "
        "Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }

    // Check that no MDS daemons is up for this CephFS.
    if (fsp->get_mds_map().get_num_up_mds() > 0) {
      ss << "CephFS '" << fs_name << "' is not offline. Before renaming "
	 << "a CephFS, it must be marked as down. See `ceph fs fail`.";
      return -EPERM;
    }

    // Check that refuse_client_session is set.
    if (!fsp->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION)) {
      ss << "CephFS '" << fs_name << "' doesn't refuse clients. Before "
	 << "renaming a CephFS, flag 'refuse_client_session' must be set. "
	 << "See `ceph fs set`.";
      return -EPERM;
    }

    for (const auto p : fsp->get_mds_map().get_data_pools()) {
      mon->osdmon()->do_application_enable(p, APP_NAME_CEPHFS, "data",
					   new_fs_name, true);
    }

    mon->osdmon()->do_application_enable(
      fsp->get_mds_map().get_metadata_pool(), APP_NAME_CEPHFS, "metadata",
      new_fs_name, true);
    mon->osdmon()->propose_pending();

    auto f = [new_fs_name](auto&& fs) {
                    fs.get_mds_map().set_fs_name(new_fs_name);
             };
    fsmap.modify_filesystem(fsp->get_fscid(), std::move(f));

    ss << "File system is renamed. cephx credentials authorized to "
          "old file system name need to be reauthorized to new file "
          "system name.";

    return 0;
  }

private:
  Paxos *m_paxos;
};

class SwapFilesystemHandler : public FileSystemCommandHandler
{
  public:
  explicit SwapFilesystemHandler(Paxos *paxos)
    : FileSystemCommandHandler("fs swap"), m_paxos(paxos)
  {
  }

  int handle(Monitor *mon, FSMap& fsmap, MonOpRequestRef op,
	     const cmdmap_t& cmdmap, std::ostream &ss) override
  {
    ceph_assert(m_paxos->is_plugged());

    // Check for confirmation flag
    bool confirmation_flag = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", confirmation_flag);
    if (!confirmation_flag) {
      ss << "This is a potentially disruptive operation, client\'s cephx "
	"credentials may need to be reauthorized to access the file systems "
	"and its pools. Add --yes-i-really-mean-it if you are sure you wish "
	"to continue.";
      return -EPERM;
    }

    string fs1_name, fs2_name;
    int64_t fs1_id = FS_CLUSTER_ID_NONE;
    int64_t fs2_id = FS_CLUSTER_ID_NONE;
    string swap_fscids_flag;
    cmd_getval(cmdmap, "fs1_name", fs1_name);
    cmd_getval(cmdmap, "fs2_name", fs2_name);
    cmd_getval(cmdmap, "fs1_id", fs1_id);
    cmd_getval(cmdmap, "fs2_id", fs2_id);
    cmd_getval(cmdmap, "swap_fscids", swap_fscids_flag);
    auto fs1p = fsmap.get_filesystem(fs1_name);
    auto fs2p = fsmap.get_filesystem(fs2_name);

    // Check that CephFSs exists for both given names.
    if (fs1p == nullptr || fs2p == nullptr) {
      if (fs1p == nullptr && fs2p != nullptr) {
	ss << "File system '" << fs1_name << "' doesn\'t exist on this "
	      "Ceph cluster.";
	return -ENOENT;
      } else if (fs1p != nullptr && fs2p == nullptr) {
	ss << "File system '" << fs2_name << "' doesn\'t exist on this "
	      "Ceph cluster.";
	return -ENOENT;
      } else {
	ss << "Neither file system '" << fs1_name << "' nor file "
	      "system '" << fs2_name << "' exists on this Ceph cluster.";
	return -ENOENT;
      }
    }

    // Check that FSCID provided for both CephFSs is correct.
    if (fs1_id != fs1p->get_fscid() || fs2_id != fs2p->get_fscid()) {
      if (fs1_id != fs1p->get_fscid() && fs2_id == fs2p->get_fscid()) {
	ss << "FSCID provided for '" << fs1_name << "' is incorrect.";
	return -EINVAL;
      } else if (fs1_id == fs1p->get_fscid() && fs2_id != fs2p->get_fscid()) {
	ss << "FSCID provided for '" << fs2_name << "' is incorrect.";
	return -EINVAL;
      } else if (fs1_id != fs1p->get_fscid() && fs2_id != fs2p->get_fscid()) {
	if (fs1_id == fs2p->get_fscid() && fs2_id == fs1p->get_fscid()) {
	  ss << "FSCIDs provided in command arguments are swapped; perhaps "
	     << "`ceph fs swap` has been run before.";
	  return 0;
	} else {
	ss << "FSCIDs provided for both the CephFSs is incorrect.";
	return -EINVAL;
	}
      }
    }

    // Check that CephFS mirroring for both CephFSs is disabled.
    if (fs1p->get_mirror_info().mirrored || fs2p->get_mirror_info().mirrored) {
      if (fs1p->get_mirror_info().mirrored &&
	  !fs2p->get_mirror_info().mirrored) {
	ss << "Mirroring is enabled on file system '"<< fs1_name << "'. "
	   << "Disable mirroring on the file system after ensuring it's OK "
	   << "to do so, and then re-try swapping.";
	return -EPERM;
      } else if (!fs1p->get_mirror_info().mirrored &&
		 fs2p->get_mirror_info().mirrored) {
	ss << "Mirroring is enabled on file system '"<< fs2_name << "'. "
	   << "Disable mirroring on the file system after ensuring it's OK "
	   << "to do so, and then re-try swapping.";
	return -EPERM;
      } else {
	ss << "Mirroring is enabled on file systems '" << fs1_name << "' "
	   << "and '" << fs2_name << "'. Disable mirroring on both the "
	   << "file systems after ensuring it's OK to do so, and then re-try "
	   << "swapping.";
	return -EPERM;
      }
    }

    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(
	op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }

    // Check that both CephFS have been marked as down, IOW has no MDS
    // associated with it.
    if (fs1p->get_mds_map().get_num_up_mds() > 0 ||
        fs2p->get_mds_map().get_num_up_mds() > 0) {
      if (fs1p->get_mds_map().get_num_up_mds() > 0 &&
          fs2p->get_mds_map().get_num_up_mds() == 0) {
	ss << "CephFS '" << fs1_name << "' is not offline. Before swapping "
	   << "CephFS names, both CephFSs should be marked as failed. See "
	   << "`ceph fs fail`.";
	return -EPERM;
      } else if (fs1p->get_mds_map().get_num_up_mds() == 0 &&
		 fs2p->get_mds_map().get_num_up_mds() > 0) {
	ss << "CephFS '" << fs2_name << "' is not offline. Before swapping "
	   << "CephFS names, both CephFSs should be marked as failed. See "
	   << "`ceph fs fail`.";
	return -EPERM;
      } else {
	ss << "CephFSs '" << fs1_name << "' and '" << fs2_name << "' "
	   << "are not offline. Before swapping CephFS names, both CephFSs "
	   << "should be marked as failed. See `ceph fs fail`.";
	return -EPERM;
      }
    }

    // Check that refuse_client_session is set.
    if (!fs1p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION) ||
	!fs2p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION)) {
      if (!fs1p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION) &&
          fs2p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION)) {
	ss << "CephFS '" << fs1_name << "' doesn't refuse clients. Before "
	   << "swapping CephFS names, flag 'refuse_client_session' must be "
	    << "set. See `ceph fs set`.";
	return -EPERM;
      } else if (
          fs1p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION) &&
	  !fs2p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION)) {
	ss << "CephFS '" << fs2_name << "' doesn't refuse clients. Before "
	   << "swapping CephFS names, flag 'refuse_client_session' must be "
	    << "set. See `ceph fs set`.";
	return -EPERM;
      } else if (
          !fs1p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION) &&
	  !fs2p->get_mds_map().test_flag(CEPH_MDSMAP_REFUSE_CLIENT_SESSION)) {
	ss << "CephFSs '" << fs1_name << "' and '" << fs2_name << "' do not "
	   << "refuse clients. Before swapping CephFS names, flag "
	   << "'refuse_client_session' must be set. See `ceph fs set`.";
	return -EPERM;
      }
    }

    // Finally, the swap begins.
    // Swap CephFS names on OSD pool application tag
    for (const auto p : fs1p->get_mds_map().get_data_pools()) {
      mon->osdmon()->do_application_enable(p, APP_NAME_CEPHFS, "data",
					   fs2_name, true);
    }
    mon->osdmon()->do_application_enable(
      fs1p->get_mds_map().get_metadata_pool(), APP_NAME_CEPHFS, "metadata",
      fs2_name, true);

    for (const auto p : fs2p->get_mds_map().get_data_pools()) {
      mon->osdmon()->do_application_enable(p, APP_NAME_CEPHFS, "data",
					   fs1_name, true);
    }
    mon->osdmon()->do_application_enable(
      fs2p->get_mds_map().get_metadata_pool(), APP_NAME_CEPHFS, "metadata",
      fs1_name, true);
    mon->osdmon()->propose_pending();

    // Now swap CephFS names and, optionally, FSCIDs.
    auto renamefunc1 = [fs2_name](auto&& fs) {
      fs.get_mds_map().set_fs_name(fs2_name);
    };
    fsmap.modify_filesystem(fs1_id, std::move(renamefunc1));
    auto renamefunc2 = [fs1_name](auto&& fs) {
      fs.get_mds_map().set_fs_name(fs1_name);
    };
    fsmap.modify_filesystem(fs2_id, std::move(renamefunc2));

    if (swap_fscids_flag == "yes") {
      fsmap.swap_fscids(fs1_id, fs2_id);
    }

    ss << "File system names ";
    if (swap_fscids_flag == "yes") {
      ss << "and FSCIDs ";
    }
    ss << " have been swapped; cephx credentials may need an upgrade.";

    return 0;
  }

private:
  Paxos *m_paxos;
};

class RemoveDataPoolHandler : public FileSystemCommandHandler
{
  public:
  RemoveDataPoolHandler()
    : FileSystemCommandHandler("fs rm_data_pool")
  {}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    string poolname;
    cmd_getval(cmdmap, "pool", poolname);

    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name)
        || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	ss << "pool '" << poolname << "' does not exist";
        return -ENOENT;
      } else if (poolid < 0) {
        ss << "invalid pool id '" << poolid << "'";
        return -EINVAL;
      }
    }

    ceph_assert(poolid >= 0);  // Checked by parsing code above

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        return -ENOENT;
    }

    if (fsp->get_mds_map().get_first_data_pool() == poolid) {
      ss << "cannot remove default data pool";
      return -EINVAL;
    }

    int r = 0;
    fsmap.modify_filesystem(fsp->get_fscid(),
        [&r, poolid](auto&& fs)
    {
      r = fs.get_mds_map().remove_data_pool(poolid);
    });
    if (r == -ENOENT) {
      // It was already removed, succeed in silence
      return 0;
    } else if (r == 0) {
      // We removed it, succeed
      ss << "removed data pool " << poolid << " from fsmap";
      return 0;
    } else {
      // Unexpected error, bubble up
      return r;
    }
  }
};

/**
 * For commands with an alternative prefix
 */
template<typename T>
class AliasHandler : public T
{
  string alias_prefix;

  public:
  explicit AliasHandler(const string &new_prefix)
    : T()
  {
    alias_prefix = new_prefix;
  }

  string const &get_prefix() const override {return alias_prefix;}

  int handle(
      Monitor *mon,
      FSMap& fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      ostream &ss) override
  {
    return T::handle(mon, fsmap, op, cmdmap, ss);
  }
};

class MirrorHandlerEnable : public FileSystemCommandHandler
{
public:
  MirrorHandlerEnable()
    : FileSystemCommandHandler("fs mirror enable")
  {}

  int handle(Monitor *mon,
             FSMap &fsmap, MonOpRequestRef op,
             const cmdmap_t& cmdmap, ostream &ss) override {
    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
      ss << "Filesystem '" << fs_name << "' not found";
      return -ENOENT;
    }

    if (fsp->get_mirror_info().is_mirrored()) {
      return 0;
    }

    auto f = [](auto&& fs) {
      fs.get_mirror_info().enable_mirroring();
    };
    fsmap.modify_filesystem(fsp->get_fscid(), std::move(f));

    return 0;
  }
};

class MirrorHandlerDisable : public FileSystemCommandHandler
{
public:
  MirrorHandlerDisable()
    : FileSystemCommandHandler("fs mirror disable")
  {}

  int handle(Monitor *mon,
             FSMap &fsmap, MonOpRequestRef op,
             const cmdmap_t& cmdmap, ostream &ss) override {
    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
      ss << "Filesystem '" << fs_name << "' not found";
      return -ENOENT;
    }

    if (!fsp->get_mirror_info().is_mirrored()) {
      return 0;
    }

    auto f = [](auto&& fs) {
      fs.get_mirror_info().disable_mirroring();
    };
    fsmap.modify_filesystem(fsp->get_fscid(), std::move(f));

    return 0;
  }
};

class MirrorHandlerAddPeer : public FileSystemCommandHandler
{
public:
  MirrorHandlerAddPeer()
    : FileSystemCommandHandler("fs mirror peer_add")
  {}

  boost::optional<pair<string, string>>
  extract_remote_cluster_conf(const string &spec) {
    auto pos = spec.find("@");
    if (pos == string_view::npos) {
      return boost::optional<pair<string, string>>();
    }

    auto client = spec.substr(0, pos);
    auto cluster = spec.substr(pos+1);

    return make_pair(client, cluster);
  }

  bool peer_add(FSMap &fsmap, const Filesystem& fs,
                const cmdmap_t &cmdmap, ostream &ss) {
    string peer_uuid;
    string remote_spec;
    string remote_fs_name;
    cmd_getval(cmdmap, "uuid", peer_uuid);
    cmd_getval(cmdmap, "remote_cluster_spec", remote_spec);
    cmd_getval(cmdmap, "remote_fs_name", remote_fs_name);

    // verify (and extract) remote cluster specification
    auto remote_conf = extract_remote_cluster_conf(remote_spec);
    if (!remote_conf) {
      ss << "invalid remote cluster spec -- should be <client>@<cluster>";
      return false;
    }

    if (fs.get_mirror_info().has_peer(peer_uuid)) {
      ss << "peer already exists";
      return true;
    }
    if (fs.get_mirror_info().has_peer((*remote_conf).first, (*remote_conf).second,
                                 remote_fs_name)) {
      ss << "peer already exists";
      return true;
    }

    auto f = [peer_uuid, remote_conf, remote_fs_name](auto&& fs) {
               fs.get_mirror_info().peer_add(peer_uuid, (*remote_conf).first,
                                        (*remote_conf).second, remote_fs_name);
             };
    fsmap.modify_filesystem(fs.get_fscid(), std::move(f));
    return true;
  }

  int handle(Monitor *mon,
             FSMap &fsmap, MonOpRequestRef op,
             const cmdmap_t& cmdmap, ostream &ss) override {
    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
      ss << "Filesystem '" << fs_name << "' not found";
      return -ENOENT;
    }

    if (!fsp->get_mirror_info().is_mirrored()) {
      ss << "Mirroring not enabled for filesystem '" << fs_name << "'";
      return -EINVAL;
    }

    auto res = peer_add(fsmap, *fsp, cmdmap, ss);
    if (!res) {
      return -EINVAL;
    }

    return 0;
  }
};

class MirrorHandlerRemovePeer : public FileSystemCommandHandler
{
public:
  MirrorHandlerRemovePeer()
    : FileSystemCommandHandler("fs mirror peer_remove")
  {}

  bool peer_remove(FSMap &fsmap, const Filesystem& fs,
                   const cmdmap_t &cmdmap, ostream &ss) {
    string peer_uuid;
    cmd_getval(cmdmap, "uuid", peer_uuid);

    if (!fs.get_mirror_info().has_peer(peer_uuid)) {
      ss << "cannot find peer with uuid: " << peer_uuid;
      return true;
    }

    auto f = [peer_uuid](auto&& fs) {
               fs.get_mirror_info().peer_remove(peer_uuid);
             };
    fsmap.modify_filesystem(fs.get_fscid(), std::move(f));
    return true;
  }

  int handle(Monitor *mon,
             FSMap &fsmap, MonOpRequestRef op,
             const cmdmap_t& cmdmap, ostream &ss) override {
    string fs_name;
    if (!cmd_getval(cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto* fsp = fsmap.get_filesystem(fs_name);
    if (fsp == nullptr) {
      ss << "Filesystem '" << fs_name << "' not found";
      return -ENOENT;
    }

    if (!fsp->get_mirror_info().is_mirrored()) {
      ss << "Mirroring not enabled for filesystem '" << fs_name << "'";
      return -EINVAL;
    }

    auto res = peer_remove(fsmap, *fsp, cmdmap, ss);
    if (!res) {
      return -EINVAL;
    }

    return 0;
  }
};

list<std::shared_ptr<FileSystemCommandHandler> >
FileSystemCommandHandler::load(Paxos *paxos)
{
  list<std::shared_ptr<FileSystemCommandHandler> > handlers;

  handlers.push_back(std::make_shared<SetHandler>());
  handlers.push_back(std::make_shared<FailHandler>());
  handlers.push_back(std::make_shared<FlagSetHandler>());
  handlers.push_back(std::make_shared<CompatSetHandler>());
  handlers.push_back(std::make_shared<RequiredClientFeaturesHandler>());
  handlers.push_back(std::make_shared<AddDataPoolHandler>(paxos));
  handlers.push_back(std::make_shared<RemoveDataPoolHandler>());
  handlers.push_back(std::make_shared<FsNewHandler>(paxos));
  handlers.push_back(std::make_shared<RemoveFilesystemHandler>());
  handlers.push_back(std::make_shared<ResetFilesystemHandler>());
  handlers.push_back(std::make_shared<RenameFilesystemHandler>(paxos));
  handlers.push_back(std::make_shared<SwapFilesystemHandler>(paxos));

  handlers.push_back(std::make_shared<SetDefaultHandler>());
  handlers.push_back(std::make_shared<AliasHandler<SetDefaultHandler> >(
        "fs set_default"));
  handlers.push_back(std::make_shared<MirrorHandlerEnable>());
  handlers.push_back(std::make_shared<MirrorHandlerDisable>());
  handlers.push_back(std::make_shared<MirrorHandlerAddPeer>());
  handlers.push_back(std::make_shared<MirrorHandlerRemovePeer>());

  return handlers;
}

int FileSystemCommandHandler::_check_pool(
    OSDMap &osd_map,
    const int64_t pool_id,
    int type,
    bool force,
    ostream *ss,
    bool allow_overlay) const
{
  ceph_assert(ss != NULL);

  const pg_pool_t *pool = osd_map.get_pg_pool(pool_id);
  if (!pool) {
    *ss << "pool id '" << pool_id << "' does not exist";
    return -ENOENT;
  }

  if (pool->has_snaps()) {
    *ss << "pool(" << pool_id <<") already has mon-managed snaps; "
	   "can't attach pool to fs";
    return -EOPNOTSUPP;
  }

  const string& pool_name = osd_map.get_pool_name(pool_id);
  auto app_map = pool->application_metadata;

  if (!allow_overlay && !force && !app_map.empty()) {
    auto app = app_map.find(APP_NAME_CEPHFS);
    if (app != app_map.end()) {
      auto& [app_name, app_metadata] = *app;
      auto itr = app_metadata.find("data");
      if (itr == app_metadata.end()) {
	itr = app_metadata.find("metadata");
      }
      if (itr != app_metadata.end()) {
        auto& [type, filesystem] = *itr;
        *ss << "RADOS pool '" << pool_name << "' is already used by filesystem '"
            << filesystem << "' as a '" << type << "' pool for application '"
            << app_name << "'";
        return -EINVAL;
      }
    } else {
      *ss << "RADOS pool '" << pool_name
          << "' has another non-CephFS application enabled.";
      return -EINVAL;
    }
  }

  if (pool->is_erasure()) {
    if (type == POOL_METADATA) {
      *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
         << " is an erasure-coded pool.  Use of erasure-coded pools"
         << " for CephFS metadata is not permitted";
      return -EINVAL;
    } else if (type == POOL_DATA_DEFAULT && !force) {
      *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
             " is an erasure-coded pool."
             " Use of an EC pool for the default data pool is discouraged;"
             " see the online CephFS documentation for more information."
             " Use --force to override.";
      return -EINVAL;
    } else if (!pool->allows_ecoverwrites()) {
      // non-overwriteable EC pools are only acceptable with a cache tier overlay
      if (!pool->has_tiers() || !pool->has_read_tier() || !pool->has_write_tier()) {
        *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
            << " is an erasure-coded pool, with no overwrite support";
        return -EINVAL;
      }

      // That cache tier overlay must be writeback, not readonly (it's the
      // write operations like modify+truncate we care about support for)
      const pg_pool_t *write_tier = osd_map.get_pg_pool(
          pool->write_tier);
      ceph_assert(write_tier != NULL);  // OSDMonitor shouldn't allow DNE tier
      if (write_tier->cache_mode == pg_pool_t::CACHEMODE_FORWARD
          || write_tier->cache_mode == pg_pool_t::CACHEMODE_READONLY) {
        *ss << "EC pool '" << pool_name << "' has a write tier ("
            << osd_map.get_pool_name(pool->write_tier)
            << ") that is configured "
               "to forward writes.  Use a cache mode such as 'writeback' for "
               "CephFS";
        return -EINVAL;
      }
    }
  }

  if (pool->is_tier()) {
    *ss << " pool '" << pool_name << "' (id '" << pool_id
      << "') is already in use as a cache tier.";
    return -EINVAL;
  }

  if (!force && !pool->application_metadata.empty() &&
      pool->application_metadata.count(APP_NAME_CEPHFS) == 0) {
    *ss << " pool '" << pool_name << "' (id '" << pool_id
        << "') has a non-CephFS application enabled.";
    return -EINVAL;
  }

  if (type != POOL_METADATA && pool->pg_autoscale_mode == pg_pool_t::pg_autoscale_mode_t::ON && !pool->has_flag(pg_pool_t::FLAG_BULK)) {
    // TODO: consider issuing an info event in this case
    *ss << "  Pool '" << pool_name << "' (id '" << pool_id
	<< "') has pg autoscale mode 'on' but is not marked as bulk." << std::endl
	<< "  Consider setting the flag by running" << std::endl
	<< "    # ceph osd pool set " << pool_name << " bulk true" << std::endl;
  }

  // Nothing special about this pool, so it is permissible
  return 0;
}

int FileSystemCommandHandler::is_op_allowed(
    const MonOpRequestRef& op, const FSMap& fsmap, const cmdmap_t& cmdmap,
    ostream &ss) const
{
    string fs_name;
    cmd_getval(cmdmap, "fs_name", fs_name);

    // so that fsmap can filtered and the original copy is untouched.
    FSMap fsmap_copy = fsmap;
    fsmap_copy.filter(op->get_session()->get_allowed_fs_names());

    auto* fsp = fsmap_copy.get_filesystem(fs_name);
    if (fsp == nullptr) {
      auto prefix = get_prefix();
      /* let "fs rm" and "fs rename" handle idempotent cases where file systems do not exist */
      if (!(prefix == "fs rm" || prefix == "fs rename" || prefix == "fs swap") &&
	  fsmap.get_filesystem(fs_name) == nullptr) {
        ss << "Filesystem not found: '" << fs_name << "'";
        return -ENOENT;
      }
    }

    if (!op->get_session()->fs_name_capable(fs_name, MON_CAP_W)) {
      ss << "Permission denied: '" << fs_name << "'";
      return -EPERM;
    }

  return 1;
}
