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



static const string EXPERIMENTAL_WARNING("Warning! This feature is experimental."
"It may cause problems up to and including data loss."
"Consult the documentation at ceph.com, and if unsure, do not proceed."
"Add --yes-i-really-mean-it if you are certain.");



class FlagSetHandler : public FileSystemCommandHandler
{
  public:
  FlagSetHandler()
    : FileSystemCommandHandler("fs flag set")
  {
  }

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    string flag_name;
    cmd_getval(g_ceph_context, cmdmap, "flag_name", flag_name);

    string flag_val;
    cmd_getval(g_ceph_context, cmdmap, "val", flag_val);

    string confirm;
    cmd_getval(g_ceph_context, cmdmap, "confirm", confirm);

    if (flag_name == "enable_multiple") {
      bool flag_bool = false;
      int r = parse_bool(flag_val, &flag_bool, ss);
      if (r != 0) {
        ss << "Invalid boolean value '" << flag_val << "'";
        return r;
      }

      bool jewel = mon->get_quorum_con_features() & CEPH_FEATURE_SERVER_JEWEL;
      if (flag_bool && !jewel) {
        ss << "Multiple-filesystems are forbidden until all mons are updated";
        return -EINVAL;
      }
      if (confirm != "--yes-i-really-mean-it") {
	ss << EXPERIMENTAL_WARNING;
      }
      fsmap.set_enable_multiple(flag_bool);
      return 0;
    } else {
      ss << "Unknown flag '" << flag_name << "'";
      return -EINVAL;
    }
  }
};

class FsNewHandler : public FileSystemCommandHandler
{
  public:
  FsNewHandler(Paxos *paxos)
    : FileSystemCommandHandler("fs new"), m_paxos(paxos)
  {
  }

  bool batched_propose() override {
    return true;
  }

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    assert(m_paxos->is_plugged());

    string metadata_name;
    cmd_getval(g_ceph_context, cmdmap, "metadata", metadata_name);
    int64_t metadata = mon->osdmon()->osdmap.lookup_pg_pool_name(metadata_name);
    if (metadata < 0) {
      ss << "pool '" << metadata_name << "' does not exist";
      return -ENOENT;
    }

    string force_str;
    cmd_getval(g_ceph_context,cmdmap, "force", force_str);
    bool force = (force_str == "--force");
    const pool_stat_t *stat = mon->mgrstatmon()->get_pool_stat(metadata);
    if (stat) {
      int64_t metadata_num_objects = stat->stats.sum.num_objects;
      if (!force && metadata_num_objects > 0) {
	ss << "pool '" << metadata_name
	   << "' already contains some objects. Use an empty pool instead.";
	return -EINVAL;
      }
    }

    string data_name;
    cmd_getval(g_ceph_context, cmdmap, "data", data_name);
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
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    if (fs_name.empty()) {
        // Ensure fs name is not empty so that we can implement
        // commmands that refer to FS by name in future.
        ss << "Filesystem name may not be empty";
        return -EINVAL;
    }

    if (fsmap.get_filesystem(fs_name)) {
      auto fs = fsmap.get_filesystem(fs_name);
      if (*(fs->mds_map.get_data_pools().begin()) == data
          && fs->mds_map.get_metadata_pool() == metadata) {
        // Identical FS created already, this is a no-op
        ss << "filesystem '" << fs_name << "' already exists";
        return 0;
      } else {
        ss << "filesystem already exists with name '" << fs_name << "'";
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

    for (auto fs : fsmap.get_filesystems()) {
      const std::vector<int64_t> &data_pools = fs->mds_map.get_data_pools();
      string sure;
      if ((std::find(data_pools.begin(), data_pools.end(), data) != data_pools.end()
	   || fs->mds_map.get_metadata_pool() == metadata)
	  && ((!cmd_getval(g_ceph_context, cmdmap, "sure", sure)
	       || sure != "--allow-dangerous-metadata-overlay"))) {
	ss << "Filesystem '" << fs_name
	   << "' is already using one of the specified RADOS pools. This should ONLY be done in emergencies and after careful reading of the documentation. Pass --allow-dangerous-metadata-overlay to permit this.";
	return -EEXIST;
      }
    }

    pg_pool_t const *data_pool = mon->osdmon()->osdmap.get_pg_pool(data);
    assert(data_pool != NULL);  // Checked it existed above
    pg_pool_t const *metadata_pool = mon->osdmon()->osdmap.get_pg_pool(metadata);
    assert(metadata_pool != NULL);  // Checked it existed above

    int r = _check_pool(mon->osdmon()->osdmap, data, false, force, &ss);
    if (r < 0) {
      return r;
    }

    r = _check_pool(mon->osdmon()->osdmap, metadata, true, force, &ss);
    if (r < 0) {
      return r;
    }
    
    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }
    mon->osdmon()->do_application_enable(data,
					 pg_pool_t::APPLICATION_NAME_CEPHFS,
					 "data", fs_name);
    mon->osdmon()->do_application_enable(metadata,
					 pg_pool_t::APPLICATION_NAME_CEPHFS,
					 "metadata", fs_name);
    mon->osdmon()->propose_pending();

    // All checks passed, go ahead and create.
    fsmap.create_filesystem(fs_name, metadata, data,
                            mon->get_quorum_con_features());
    ss << "new fs with metadata pool " << metadata << " and data pool " << data;
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
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    std::string fs_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
    }

    string var;
    if (!cmd_getval(g_ceph_context, cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      return -EINVAL;
    }
    string val;
    string interr;
    int64_t n = 0;
    if (!cmd_getval(g_ceph_context, cmdmap, "val", val)) {
      return -EINVAL;
    }
    // we got a string.  see if it contains an int.
    n = strict_strtoll(val.c_str(), 10, &interr);
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

      if (!fs->mds_map.allows_multimds() && n > fs->mds_map.get_max_mds() &&
	  n > 1) {
	ss << "multi-MDS clusters are not enabled; set 'allow_multimds' to enable";
	return -EINVAL;
      }
      if (n > MAX_MDS) {
        ss << "may not have more than " << MAX_MDS << " MDS ranks";
        return -EINVAL;
      }
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_max_mds(n);
      });
    } else if (var == "inline_data") {
      bool enable_inline = false;
      int r = parse_bool(val, &enable_inline, ss);
      if (r != 0) {
        return r;
      }

      if (enable_inline) {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << EXPERIMENTAL_WARNING;
	  return -EPERM;
	}
	ss << "inline data enabled";

        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_inline_data_enabled(true);
        });

        // Update `compat`
        CompatSet c = fsmap.get_compat();
        c.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
        fsmap.update_compat(c);
      } else {
	ss << "inline data disabled";
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_inline_data_enabled(false);
        });
      }
    } else if (var == "balancer") {
      if (val.empty()) {
        ss << "unsetting the metadata load balancer";
      } else {
        ss << "setting the metadata load balancer to " << val;
      }
      fsmap.modify_filesystem(
	fs->fscid,
	[val](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_balancer(val);
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
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_max_filesize(n);
      });
    } else if (var == "allow_new_snaps") {
      bool enable_snaps = false;
      int r = parse_bool(val, &enable_snaps, ss);
      if (r != 0) {
        return r;
      }

      if (!enable_snaps) {
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.clear_snaps_allowed();
        });
	ss << "disabled new snapshots";
      } else {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << EXPERIMENTAL_WARNING;
	  return -EPERM;
	}
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_snaps_allowed();
        });
	ss << "enabled new snapshots";
      }
    } else if (var == "allow_multimds") {
      bool enable_multimds = false;
      int r = parse_bool(val, &enable_multimds, ss);
      if (r != 0) {
	return r;
      }

      if (!enable_multimds) {
	fsmap.modify_filesystem(fs->fscid,
	     [](std::shared_ptr<Filesystem> fs)
		{
		  fs->mds_map.clear_multimds_allowed();
		});
	ss << "disallowed increasing the cluster size past 1";
      } else {
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_multimds_allowed();
        });
	ss << "enabled creation of more than 1 active MDS";
      }
    } else if (var == "allow_dirfrags") {
      bool enable_dirfrags = false;
      int r = parse_bool(val, &enable_dirfrags, ss);
      if (r != 0) {
	return r;
      }

      if (!enable_dirfrags) {
	fsmap.modify_filesystem(fs->fscid,
	     [](std::shared_ptr<Filesystem> fs)
		{
		  fs->mds_map.clear_dirfrags_allowed();
		});
	ss << "disallowed new directory fragmentation";
      } else {
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_dirfrags_allowed();
        });
	ss << "enabled directory fragmentation";
      }
    } else if (var == "cluster_down") {
      bool is_down = false;
      int r = parse_bool(val, &is_down, ss);
      if (r != 0) {
        return r;
      }

      fsmap.modify_filesystem(
          fs->fscid,
          [is_down](std::shared_ptr<Filesystem> fs)
      {
        if (is_down) {
          fs->mds_map.set_flag(CEPH_MDSMAP_DOWN);
        } else {
          fs->mds_map.clear_flag(CEPH_MDSMAP_DOWN);
        }
      });

      ss << "marked " << (is_down ? "down" : "up");
    } else if (var == "standby_count_wanted") {
      if (interr.length()) {
       ss << var << " requires an integer value";
       return -EINVAL;
      }
      if (n < 0) {
       ss << var << " must be non-negative";
       return -ERANGE;
      }
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_standby_count_wanted(n);
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
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_session_timeout((uint32_t)n);
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
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_session_autoclose((uint32_t)n);
      });
    } else {
      ss << "unknown variable " << var;
      return -EINVAL;
    }

    return 0;
  }
};

class AddDataPoolHandler : public FileSystemCommandHandler
{
  public:
  AddDataPoolHandler(Paxos *paxos)
    : FileSystemCommandHandler("fs add_data_pool"), m_paxos(paxos)
  {}

  bool batched_propose() override {
    return true;
  }

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    assert(m_paxos->is_plugged());

    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);

    std::string fs_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name)
        || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
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

    int r = _check_pool(mon->osdmon()->osdmap, poolid, false, false, &ss);
    if (r != 0) {
      return r;
    }

    // no-op when the data_pool already on fs
    if (fs->mds_map.is_data_pool(poolid)) {
      ss << "data pool " << poolid << " is already on fs " << fs_name;
      return 0;
    }

    if (!mon->osdmon()->is_writeable()) {
      // not allowed to write yet, so retry when we can
      mon->osdmon()->wait_for_writeable(op, new PaxosService::C_RetryMessage(mon->mdsmon(), op));
      return -EAGAIN;
    }
    mon->osdmon()->do_application_enable(poolid,
					 pg_pool_t::APPLICATION_NAME_CEPHFS,
					 "data", poolname);
    mon->osdmon()->propose_pending();

    fsmap.modify_filesystem(
        fs->fscid,
        [poolid](std::shared_ptr<Filesystem> fs)
    {
      fs->mds_map.add_data_pool(poolid);
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
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    std::string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        return -ENOENT;
    }

    fsmap.set_legacy_client_fscid(fs->fscid);
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
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    // Check caller has correctly named the FS to delete
    // (redundant while there is only one FS, but command
    //  syntax should apply to multi-FS future)
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        // Consider absence success to make deletes idempotent
        ss << "filesystem '" << fs_name << "' does not exist";
        return 0;
    }

    // Check that no MDS daemons are active
    if (fs->mds_map.get_num_up_mds() > 0) {
      ss << "all MDS daemons must be inactive before removing filesystem";
      return -EINVAL;
    }

    // Check for confirmation flag
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is a DESTRUCTIVE operation and will make data in your filesystem permanently" \
            " inaccessible.  Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    if (fsmap.get_legacy_client_fscid() == fs->fscid) {
      fsmap.set_legacy_client_fscid(FS_CLUSTER_ID_NONE);
    }

    std::vector<mds_gid_t> to_fail;
    // There may be standby_replay daemons left here
    for (const auto &i : fs->mds_map.get_mds_info()) {
      assert(i.second.state == MDSMap::STATE_STANDBY_REPLAY);
      to_fail.push_back(i.first);
    }

    for (const auto &gid : to_fail) {
      // Standby replays don't write, so it isn't important to
      // wait for an osdmap propose here: ignore return value.
      mon->mdsmon()->fail_mds_gid(gid);
    }

    fsmap.erase_filesystem(fs->fscid);

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
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        // Unlike fs rm, we consider this case an error
        return -ENOENT;
    }

    // Check that no MDS daemons are active
    if (fs->mds_map.get_num_up_mds() > 0) {
      ss << "all MDS daemons must be inactive before resetting filesystem: set the cluster_down flag"
            " and use `ceph mds fail` to make this so";
      return -EINVAL;
    }

    // Check for confirmation flag
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is a potentially destructive operation, only for use by experts in disaster recovery.  "
        "Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    fsmap.reset_filesystem(fs->fscid);

    return 0;
  }
};

class RemoveDataPoolHandler : public FileSystemCommandHandler
{
  public:
  RemoveDataPoolHandler()
    : FileSystemCommandHandler("fs rm_data_pool")
  {}

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);

    std::string fs_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name)
        || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
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

    assert(poolid >= 0);  // Checked by parsing code above

    if (fs->mds_map.get_first_data_pool() == poolid) {
      ss << "cannot remove default data pool";
      return -EINVAL;
    }


    int r = 0;
    fsmap.modify_filesystem(fs->fscid,
        [&r, poolid](std::shared_ptr<Filesystem> fs)
    {
      r = fs->mds_map.remove_data_pool(poolid);
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
  std::string alias_prefix;

  public:
  AliasHandler(const std::string &new_prefix)
    : T()
  {
    alias_prefix = new_prefix;
  }

  std::string const &get_prefix() override {return alias_prefix;}

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      const cmdmap_t& cmdmap,
      std::stringstream &ss) override
  {
    return T::handle(mon, fsmap, op, cmdmap, ss);
  }
};


std::list<std::shared_ptr<FileSystemCommandHandler> >
FileSystemCommandHandler::load(Paxos *paxos)
{
  std::list<std::shared_ptr<FileSystemCommandHandler> > handlers;

  handlers.push_back(std::make_shared<SetHandler>());
  handlers.push_back(std::make_shared<FlagSetHandler>());
  handlers.push_back(std::make_shared<AddDataPoolHandler>(paxos));
  handlers.push_back(std::make_shared<RemoveDataPoolHandler>());
  handlers.push_back(std::make_shared<FsNewHandler>(paxos));
  handlers.push_back(std::make_shared<RemoveFilesystemHandler>());
  handlers.push_back(std::make_shared<ResetFilesystemHandler>());

  handlers.push_back(std::make_shared<SetDefaultHandler>());
  handlers.push_back(std::make_shared<AliasHandler<SetDefaultHandler> >(
        "fs set_default"));

  return handlers;
}

int FileSystemCommandHandler::parse_bool(
      const std::string &bool_str,
      bool *result,
      std::ostream &ss)
{
  assert(result != nullptr);

  string interr;
  int64_t n = strict_strtoll(bool_str.c_str(), 10, &interr);

  if (bool_str == "false" || bool_str == "no"
      || (interr.length() == 0 && n == 0)) {
    *result = false;
    return 0;
  } else if (bool_str == "true" || bool_str == "yes"
      || (interr.length() == 0 && n == 1)) {
    *result = true;
    return 0;
  } else {
    ss << "value must be false|no|0 or true|yes|1";
    return -EINVAL;
  }
}

int FileSystemCommandHandler::_check_pool(
    OSDMap &osd_map,
    const int64_t pool_id,
    bool metadata,
    bool force,
    std::stringstream *ss) const
{
  assert(ss != NULL);

  const pg_pool_t *pool = osd_map.get_pg_pool(pool_id);
  if (!pool) {
    *ss << "pool id '" << pool_id << "' does not exist";
    return -ENOENT;
  }

  const string& pool_name = osd_map.get_pool_name(pool_id);

  if (pool->is_erasure() && metadata) {
      *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
         << " is an erasure-coded pool.  Use of erasure-coded pools"
         << " for CephFS metadata is not permitted";
    return -EINVAL;
  } else if (pool->is_erasure() && !pool->allows_ecoverwrites()) {
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
    assert(write_tier != NULL);  // OSDMonitor shouldn't allow DNE tier
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

  if (pool->is_tier()) {
    *ss << " pool '" << pool_name << "' (id '" << pool_id
      << "') is already in use as a cache tier.";
    return -EINVAL;
  }

  if (!force && !pool->application_metadata.empty() &&
      pool->application_metadata.count(
        pg_pool_t::APPLICATION_NAME_CEPHFS) == 0) {
    *ss << " pool '" << pool_name << "' (id '" << pool_id
        << "') has a non-CephFS application enabled.";
    return -EINVAL;
  }

  // Nothing special about this pool, so it is permissible
  return 0;
}

