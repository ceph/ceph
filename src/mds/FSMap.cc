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


#include "FSMap.h"

#include <sstream>
using std::stringstream;


void Filesystem::dump(Formatter *f) const
{
  f->open_object_section("mdsmap");
  mds_map.dump(f);
  f->close_section();
  f->dump_int("id", fscid);
}

void FSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);

  f->open_object_section("compat");
  compat.dump(f);
  f->close_section();

  f->open_object_section("feature flags");
  f->dump_bool("enable_multiple", enable_multiple);
  f->dump_bool("ever_enabled_multiple", ever_enabled_multiple);
  f->close_section();

  f->open_array_section("standbys");
  for (const auto &i : standby_daemons) {
    f->open_object_section("info");
    i.second.dump(f);
    f->dump_int("epoch", standby_epochs.at(i.first));
    f->close_section();
  }
  f->close_section();

  f->open_array_section("filesystems");
  for (const auto fs : filesystems) {
    f->open_object_section("filesystem");
    fs.second->dump(f);
    f->close_section();
  }
  f->close_section();
}

void FSMap::generate_test_instances(list<FSMap*>& ls)
{
  FSMap *m = new FSMap();

  std::list<MDSMap*> mds_map_instances;
  MDSMap::generate_test_instances(mds_map_instances);

  int k = 20;
  for (auto i : mds_map_instances) {
    auto fs = std::make_shared<Filesystem>();
    fs->fscid = k++;
    fs->mds_map = *i;
    delete i;
    m->filesystems[fs->fscid] = fs;
  }
  mds_map_instances.clear();

  ls.push_back(m);
}

void FSMap::print(ostream& out) const
{
  out << "e" << epoch << std::endl;
  out << "enable_multiple, ever_enabled_multiple: " << enable_multiple << ","
      << ever_enabled_multiple << std::endl;
  out << "compat: " << compat << std::endl;
  out << " " << std::endl;

  if (filesystems.empty()) {
    out << "No filesystems configured" << std::endl;
    return;
  }

  for (const auto &fs : filesystems) {
    fs.second->print(out);
    out << " " << std::endl << " " << std::endl;  // Space out a bit
  }

  if (!standby_daemons.empty()) {
    out << "Standby daemons:" << std::endl << " " << std::endl;
  }

  for (const auto &p : standby_daemons) {
    p.second.print_summary(out);
    out << std::endl;
  }
}



void FSMap::print_summary(Formatter *f, ostream *out)
{
  map<mds_role_t,string> by_rank;
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    for (auto i : filesystems) {
      auto fs = i.second;
      f->dump_unsigned("id", fs->fscid);
      f->dump_unsigned("up", fs->mds_map.up.size());
      f->dump_unsigned("in", fs->mds_map.in.size());
      f->dump_unsigned("max", fs->mds_map.max_mds);
    }
  } else {
    *out << "e" << get_epoch() << ":";
    if (filesystems.size() == 1) {
      auto fs = filesystems.begin()->second;
      *out << " " << fs->mds_map.up.size() << "/" << fs->mds_map.in.size() << "/"
           << fs->mds_map.max_mds << " up";
    } else {
      for (auto i : filesystems) {
        auto fs = i.second;
        *out << " " << fs->mds_map.fs_name << "-" << fs->mds_map.up.size() << "/"
             << fs->mds_map.in.size() << "/" << fs->mds_map.max_mds << " up";
      }
    }
  }

  if (f) {
    f->open_array_section("by_rank");
  }

  const auto all_info = get_mds_info();
  for (const auto &p : all_info) {
    const auto &info = p.second;
    string s = ceph_mds_state_name(info.state);
    if (info.laggy()) {
      s += "(laggy or crashed)";
    }

    const fs_cluster_id_t fscid = mds_roles.at(info.global_id);

    if (info.rank != MDS_RANK_NONE) {
      if (f) {
        f->open_object_section("mds");
        f->dump_unsigned("filesystem_id", fscid);
        f->dump_unsigned("rank", info.rank);
        f->dump_string("name", info.name);
        f->dump_string("status", s);
        f->close_section();
      } else {
        by_rank[mds_role_t(fscid, info.rank)] = info.name + "=" + s;
      }
    } else {
      by_state[s]++;
    }
  }

  if (f) {
    f->close_section();
  } else {
    if (!by_rank.empty()) {
      if (filesystems.size() > 1) {
        // Disambiguate filesystems
        std::map<std::string, std::string> pretty;
        for (auto i : by_rank) {
          const auto &fs_name = filesystems.at(i.first.fscid)->mds_map.fs_name;
          std::ostringstream o;
          o << "[" << fs_name << ":" << i.first.rank << "]";
          pretty[o.str()] = i.second;
        }
        *out << " " << pretty;
      } else {
        *out << " " << by_rank;
      }
    }
  }

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); ++p) {
    if (f) {
      f->dump_unsigned(p->first.c_str(), p->second);
    } else {
      *out << ", " << p->second << " " << p->first;
    }
  }

  size_t failed = 0;
  size_t damaged = 0;
  for (auto i : filesystems) {
    auto fs = i.second;
    failed += fs->mds_map.failed.size();
    damaged += fs->mds_map.damaged.size();
  }

  if (failed > 0) {
    if (f) {
      f->dump_unsigned("failed", failed);
    } else {
      *out << ", " << failed << " failed";
    }
  }

  if (damaged > 0) {
    if (f) {
      f->dump_unsigned("damaged", damaged);
    } else {
      *out << ", " << damaged << " damaged";
    }
  }
  //if (stopped.size())
  //out << ", " << stopped.size() << " stopped";
}

void FSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  for (auto i : filesystems) {
    auto fs = i.second;

    // TODO: move get_health up into here so that we can qualify
    // all the messages with what filesystem they're talking about
    fs->mds_map.get_health(summary, detail);
  }
}

void FSMap::encode(bufferlist& bl, uint64_t features) const
{
  if (features & CEPH_FEATURE_SERVER_JEWEL) {
    ENCODE_START(7, 6, bl);
    ::encode(epoch, bl);
    ::encode(next_filesystem_id, bl);
    ::encode(legacy_client_fscid, bl);
    ::encode(compat, bl);
    ::encode(enable_multiple, bl);
    std::vector<Filesystem> fs_list;
    for (auto i : filesystems) {
      fs_list.push_back(*(i.second));
    }
    ::encode(fs_list, bl, features);
    ::encode(mds_roles, bl);
    ::encode(standby_daemons, bl, features);
    ::encode(standby_epochs, bl);
    ::encode(ever_enabled_multiple, bl);
    ENCODE_FINISH(bl);
  } else {
    if (filesystems.empty()) {
      MDSMap disabled_map;
      disabled_map.epoch = epoch;
      disabled_map.encode(bl, features);
    } else {
      // MDSMonitor should never have created multiple filesystems
      // until the quorum features indicated Jewel
      assert(filesystems.size() == 1);
      auto fs = filesystems.begin()->second;

      // Take the MDSMap for the enabled filesystem, and populated its
      // mds_info with the standbys to get a pre-jewel-style mon MDSMap.
      MDSMap full_mdsmap = fs->mds_map;
      full_mdsmap.epoch = epoch;
      for (const auto p : standby_daemons) {
        full_mdsmap.mds_info[p.first] = p.second;
      }
      full_mdsmap.encode(bl, features);
    }
  }
}

void FSMap::decode(bufferlist::iterator& p)
{
  // Because the mon used to store an MDSMap where we now
  // store an FSMap, FSMap knows how to decode the legacy
  // MDSMap format (it never needs to encode it though).
  MDSMap legacy_mds_map;
  
  // The highest MDSMap encoding version before we changed the
  // MDSMonitor to store an FSMap instead of an MDSMap was
  // 5, so anything older than 6 is decoded as an MDSMap,
  // and anything newer is decoded as an FSMap.
  DECODE_START_LEGACY_COMPAT_LEN_16(7, 4, 4, p);
  if (struct_v < 6) {
    // Decoding an MDSMap (upgrade)
    ::decode(epoch, p);
    ::decode(legacy_mds_map.flags, p);
    ::decode(legacy_mds_map.last_failure, p);
    ::decode(legacy_mds_map.root, p);
    ::decode(legacy_mds_map.session_timeout, p);
    ::decode(legacy_mds_map.session_autoclose, p);
    ::decode(legacy_mds_map.max_file_size, p);
    ::decode(legacy_mds_map.max_mds, p);
    ::decode(legacy_mds_map.mds_info, p);
    if (struct_v < 3) {
      __u32 n;
      ::decode(n, p);
      while (n--) {
        __u32 m;
        ::decode(m, p);
        legacy_mds_map.data_pools.insert(m);
      }
      __s32 s;
      ::decode(s, p);
      legacy_mds_map.cas_pool = s;
    } else {
      ::decode(legacy_mds_map.data_pools, p);
      ::decode(legacy_mds_map.cas_pool, p);
    }

    // kclient ignores everything from here
    __u16 ev = 1;
    if (struct_v >= 2)
      ::decode(ev, p);
    if (ev >= 3)
      ::decode(legacy_mds_map.compat, p);
    else
      legacy_mds_map.compat = get_mdsmap_compat_set_base();
    if (ev < 5) {
      __u32 n;
      ::decode(n, p);
      legacy_mds_map.metadata_pool = n;
    } else {
      ::decode(legacy_mds_map.metadata_pool, p);
    }
    ::decode(legacy_mds_map.created, p);
    ::decode(legacy_mds_map.modified, p);
    ::decode(legacy_mds_map.tableserver, p);
    ::decode(legacy_mds_map.in, p);
    ::decode(legacy_mds_map.inc, p);
    ::decode(legacy_mds_map.up, p);
    ::decode(legacy_mds_map.failed, p);
    ::decode(legacy_mds_map.stopped, p);
    if (ev >= 4)
      ::decode(legacy_mds_map.last_failure_osd_epoch, p);
    if (ev >= 6) {
      ::decode(legacy_mds_map.ever_allowed_snaps, p);
      ::decode(legacy_mds_map.explicitly_allowed_snaps, p);
    } else {
      legacy_mds_map.ever_allowed_snaps = true;
      legacy_mds_map.explicitly_allowed_snaps = false;
    }
    if (ev >= 7)
      ::decode(legacy_mds_map.inline_data_enabled, p);

    if (ev >= 8) {
      assert(struct_v >= 5);
      ::decode(legacy_mds_map.enabled, p);
      ::decode(legacy_mds_map.fs_name, p);
    } else {
      legacy_mds_map.fs_name = "default";
      if (epoch > 1) {
        // If an MDS has ever been started, epoch will be greater than 1,
        // assume filesystem is enabled.
        legacy_mds_map.enabled = true;
      } else {
        // Upgrading from a cluster that never used an MDS, switch off
        // filesystem until it's explicitly enabled.
        legacy_mds_map.enabled = false;
      }
    }

    if (ev >= 9) {
      ::decode(legacy_mds_map.damaged, p);
    }

    // We're upgrading, populate filesystems from the legacy fields
    filesystems.clear();
    standby_daemons.clear();
    standby_epochs.clear();
    mds_roles.clear();
    compat = legacy_mds_map.compat;
    enable_multiple = false;

    // Synthesise a Filesystem from legacy_mds_map, if enabled
    if (legacy_mds_map.enabled) {
      // Construct a Filesystem from the legacy MDSMap
      auto migrate_fs = std::make_shared<Filesystem>(); 
      migrate_fs->fscid = FS_CLUSTER_ID_ANONYMOUS;
      migrate_fs->mds_map = legacy_mds_map;
      migrate_fs->mds_map.epoch = epoch;
      filesystems[migrate_fs->fscid] = migrate_fs;

      // Construct mds_roles, standby_daemons, and remove
      // standbys from the MDSMap in the Filesystem.
      for (const auto &p : migrate_fs->mds_map.mds_info) {
        if (p.second.rank == MDS_RANK_NONE) {
          standby_daemons[p.first] = p.second;
          standby_epochs[p.first] = epoch;
          mds_roles[p.first] = FS_CLUSTER_ID_NONE;
        } else {
          mds_roles[p.first] = migrate_fs->fscid;
        }
      }
      for (const auto &p : standby_daemons) {
        migrate_fs->mds_map.mds_info.erase(p.first);
      }

      legacy_client_fscid = migrate_fs->fscid;
    } else {
      legacy_client_fscid = FS_CLUSTER_ID_NONE;
    }
  } else {
    ::decode(epoch, p);
    ::decode(next_filesystem_id, p);
    ::decode(legacy_client_fscid, p);
    ::decode(compat, p);
    ::decode(enable_multiple, p);
    std::vector<Filesystem> fs_list;
    ::decode(fs_list, p);
    filesystems.clear();
    for (std::vector<Filesystem>::const_iterator fs = fs_list.begin(); fs != fs_list.end(); ++fs) {
      filesystems[fs->fscid] = std::make_shared<Filesystem>(*fs);
    }

    ::decode(mds_roles, p);
    ::decode(standby_daemons, p);
    ::decode(standby_epochs, p);
    ::decode(ever_enabled_multiple, p);
  }

  DECODE_FINISH(p);
}


void Filesystem::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  ::encode(fscid, bl);
  bufferlist mdsmap_bl;
  mds_map.encode(mdsmap_bl, features);
  ::encode(mdsmap_bl, bl);
  ENCODE_FINISH(bl);
}

void Filesystem::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(fscid, p);
  bufferlist mdsmap_bl;
  ::decode(mdsmap_bl, p);
  bufferlist::iterator mdsmap_bl_iter = mdsmap_bl.begin();
  mds_map.decode(mdsmap_bl_iter);
  DECODE_FINISH(p);
}

int FSMap::parse_filesystem(
      std::string const &ns_str,
      std::shared_ptr<const Filesystem> *result
      ) const
{
  std::string ns_err;
  fs_cluster_id_t fscid = strict_strtol(ns_str.c_str(), 10, &ns_err);
  if (!ns_err.empty() || filesystems.count(fscid) == 0) {
    for (auto fs : filesystems) {
      if (fs.second->mds_map.fs_name == ns_str) {
        *result = std::const_pointer_cast<const Filesystem>(fs.second);
        return 0;
      }
    }
    return -ENOENT;
  } else {
    *result = get_filesystem(fscid);
    return 0;
  }
}

void Filesystem::print(std::ostream &out) const
{
  out << "Filesystem '" << mds_map.fs_name
      << "' (" << fscid << ")" << std::endl;
  mds_map.print(out);
}

mds_gid_t FSMap::find_standby_for(mds_role_t role, const std::string& name) const
{
  mds_gid_t result = MDS_GID_NONE;

  // First see if we have a STANDBY_REPLAY
  auto fs = get_filesystem(role.fscid);
  for (const auto &i : fs->mds_map.mds_info) {
    const auto &info = i.second;
    if (info.rank == role.rank && info.state == MDSMap::STATE_STANDBY_REPLAY) {
      return info.global_id;
    }
  }

  // See if there are any STANDBY daemons available
  for (const auto &i : standby_daemons) {
    const auto &gid = i.first;
    const auto &info = i.second;
    assert(info.state == MDSMap::STATE_STANDBY);
    assert(info.rank == MDS_RANK_NONE);

    if (info.laggy()) {
      continue;
    }

    if ((info.standby_for_rank == role.rank && info.standby_for_fscid == role.fscid)
        || (name.length() && info.standby_for_name == name)) {
      // It's a named standby for *me*, use it.
      return gid;
    } else if (
        info.standby_for_rank < 0 && info.standby_for_name.length() == 0 &&
        (info.standby_for_fscid == FS_CLUSTER_ID_NONE ||
         info.standby_for_fscid == role.fscid)) {
        // It's not a named standby for anyone, use it if we don't find
        // a named standby for me later, unless it targets another FSCID.
        result = gid;
      }
  }

  return result;
}

mds_gid_t FSMap::find_unused(bool force_standby_active) const {
  for (const auto &i : standby_daemons) {
    const auto &gid = i.first;
    const auto &info = i.second;
    assert(info.state == MDSMap::STATE_STANDBY);

    if (info.laggy() || info.rank >= 0)
      continue;

    if ((info.standby_for_rank == MDSMap::MDS_NO_STANDBY_PREF ||
         info.standby_for_rank == MDSMap::MDS_MATCHED_ACTIVE ||
         (info.standby_for_rank == MDSMap::MDS_STANDBY_ANY
          && force_standby_active))) {
      return gid;
    }
  }
  return MDS_GID_NONE;
}

mds_gid_t FSMap::find_replacement_for(mds_role_t role, const std::string& name,
                               bool force_standby_active) const {
  const mds_gid_t standby = find_standby_for(role, name);
  if (standby)
    return standby;
  else
    return find_unused(force_standby_active);
}

void FSMap::sanity() const
{
  if (legacy_client_fscid != FS_CLUSTER_ID_NONE) {
    assert(filesystems.count(legacy_client_fscid) == 1);
  }

  for (const auto &i : filesystems) {
    auto fs = i.second;
    assert(fs->mds_map.compat.compare(compat) == 0);
    assert(fs->fscid == i.first);
    for (const auto &j : fs->mds_map.mds_info) {
      assert(j.second.rank != MDS_RANK_NONE);
      assert(mds_roles.count(j.first) == 1);
      assert(standby_daemons.count(j.first) == 0);
      assert(standby_epochs.count(j.first) == 0);
      assert(mds_roles.at(j.first) == i.first);
      if (j.second.state != MDSMap::STATE_STANDBY_REPLAY) {
        assert(fs->mds_map.up.at(j.second.rank) == j.first);
        assert(fs->mds_map.failed.count(j.second.rank) == 0);
        assert(fs->mds_map.damaged.count(j.second.rank) == 0);
      }
    }

    for (const auto &j : fs->mds_map.up) {
      mds_rank_t rank = j.first;
      assert(fs->mds_map.in.count(rank) == 1);
      mds_gid_t gid = j.second;
      assert(fs->mds_map.mds_info.count(gid) == 1);
    }
  }

  for (const auto &i : standby_daemons) {
    assert(i.second.state == MDSMap::STATE_STANDBY);
    assert(i.second.rank == MDS_RANK_NONE);
    assert(i.second.global_id == i.first);
    assert(standby_epochs.count(i.first) == 1);
    assert(mds_roles.count(i.first) == 1);
    assert(mds_roles.at(i.first) == FS_CLUSTER_ID_NONE);
  }

  for (const auto &i : standby_epochs) {
    assert(standby_daemons.count(i.first) == 1);
  }

  for (const auto &i : mds_roles) {
    if (i.second == FS_CLUSTER_ID_NONE) {
      assert(standby_daemons.count(i.first) == 1);
    } else {
      assert(filesystems.count(i.second) == 1);
      assert(filesystems.at(i.second)->mds_map.mds_info.count(i.first) == 1);
    }
  }
}

void FSMap::promote(
    mds_gid_t standby_gid,
    std::shared_ptr<Filesystem> filesystem,
    mds_rank_t assigned_rank)
{
  assert(gid_exists(standby_gid));
  bool is_standby_replay = mds_roles.at(standby_gid) != FS_CLUSTER_ID_NONE;
  if (!is_standby_replay) {
    assert(standby_daemons.count(standby_gid));
    assert(standby_daemons.at(standby_gid).state == MDSMap::STATE_STANDBY);
  }

  MDSMap &mds_map = filesystem->mds_map;

  // Insert daemon state to Filesystem
  if (!is_standby_replay) {
    mds_map.mds_info[standby_gid] = standby_daemons.at(standby_gid);
  } else {
    assert(mds_map.mds_info.count(standby_gid));
    assert(mds_map.mds_info.at(standby_gid).state == MDSMap::STATE_STANDBY_REPLAY);
    assert(mds_map.mds_info.at(standby_gid).rank == assigned_rank);
  }
  MDSMap::mds_info_t &info = mds_map.mds_info[standby_gid];

  if (mds_map.stopped.count(assigned_rank)) {
    // The cluster is being expanded with a stopped rank
    info.state = MDSMap::STATE_STARTING;
    mds_map.stopped.erase(assigned_rank);
  } else if (!mds_map.is_in(assigned_rank)) {
    // The cluster is being expanded with a new rank
    info.state = MDSMap::STATE_CREATING;
  } else {
    // An existing rank is being assigned to a replacement
    info.state = MDSMap::STATE_REPLAY;
    mds_map.failed.erase(assigned_rank);
  }
  info.rank = assigned_rank;
  info.inc = ++mds_map.inc[assigned_rank];
  mds_roles[standby_gid] = filesystem->fscid;

  // Update the rank state in Filesystem
  mds_map.in.insert(assigned_rank);
  mds_map.up[assigned_rank] = standby_gid;

  // Remove from the list of standbys
  if (!is_standby_replay) {
    standby_daemons.erase(standby_gid);
    standby_epochs.erase(standby_gid);
  }

  // Indicate that Filesystem has been modified
  mds_map.epoch = epoch;
}

void FSMap::assign_standby_replay(
    const mds_gid_t standby_gid,
    const fs_cluster_id_t leader_ns,
    const mds_rank_t leader_rank)
{
  assert(mds_roles.at(standby_gid) == FS_CLUSTER_ID_NONE);
  assert(gid_exists(standby_gid));
  assert(!gid_has_rank(standby_gid));
  assert(standby_daemons.count(standby_gid));

  // Insert to the filesystem
  auto fs = filesystems.at(leader_ns);
  fs->mds_map.mds_info[standby_gid] = standby_daemons.at(standby_gid);
  fs->mds_map.mds_info[standby_gid].rank = leader_rank;
  fs->mds_map.mds_info[standby_gid].state = MDSMap::STATE_STANDBY_REPLAY;
  mds_roles[standby_gid] = leader_ns;

  // Remove from the list of standbys
  standby_daemons.erase(standby_gid);
  standby_epochs.erase(standby_gid);

  // Indicate that Filesystem has been modified
  fs->mds_map.epoch = epoch;
}

void FSMap::erase(mds_gid_t who, epoch_t blacklist_epoch)
{
  if (mds_roles.at(who) == FS_CLUSTER_ID_NONE) {
    standby_daemons.erase(who);
    standby_epochs.erase(who);
  } else {
    auto fs = filesystems.at(mds_roles.at(who));
    const auto &info = fs->mds_map.mds_info.at(who);
    if (info.state != MDSMap::STATE_STANDBY_REPLAY) {
      if (info.state == MDSMap::STATE_CREATING) {
        // If this gid didn't make it past CREATING, then forget
        // the rank ever existed so that next time it's handed out
        // to a gid it'll go back into CREATING.
        fs->mds_map.in.erase(info.rank);
      } else {
        // Put this rank into the failed list so that the next available
        // STANDBY will pick it up.
        fs->mds_map.failed.insert(info.rank);
      }
      assert(fs->mds_map.up.at(info.rank) == info.global_id);
      fs->mds_map.up.erase(info.rank);
    }
    fs->mds_map.mds_info.erase(who);
    fs->mds_map.last_failure_osd_epoch = blacklist_epoch;
    fs->mds_map.epoch = epoch;
  }

  mds_roles.erase(who);
}

void FSMap::damaged(mds_gid_t who, epoch_t blacklist_epoch)
{
  assert(mds_roles.at(who) != FS_CLUSTER_ID_NONE);
  auto fs = filesystems.at(mds_roles.at(who));
  mds_rank_t rank = fs->mds_map.mds_info[who].rank;

  erase(who, blacklist_epoch);
  fs->mds_map.failed.erase(rank);
  fs->mds_map.damaged.insert(rank);

  assert(fs->mds_map.epoch == epoch);
}

/**
 * Update to indicate that the rank `rank` is to be removed
 * from the damaged list of the filesystem `fscid`
 */
bool FSMap::undamaged(const fs_cluster_id_t fscid, const mds_rank_t rank)
{
  auto fs = filesystems.at(fscid);

  if (fs->mds_map.damaged.count(rank)) {
    fs->mds_map.damaged.erase(rank);
    fs->mds_map.failed.insert(rank);
    fs->mds_map.epoch = epoch;
    return true;
  } else {
    return false;
  }
}

void FSMap::insert(const MDSMap::mds_info_t &new_info)
{
  mds_roles[new_info.global_id] = FS_CLUSTER_ID_NONE;
  standby_daemons[new_info.global_id] = new_info;
  standby_epochs[new_info.global_id] = epoch;
}

void FSMap::stop(mds_gid_t who)
{
  assert(mds_roles.at(who) != FS_CLUSTER_ID_NONE);
  auto fs = filesystems.at(mds_roles.at(who));
  const auto &info = fs->mds_map.mds_info.at(who);
  fs->mds_map.up.erase(info.rank);
  fs->mds_map.in.erase(info.rank);
  fs->mds_map.stopped.insert(info.rank);

  fs->mds_map.mds_info.erase(who);
  mds_roles.erase(who);

  fs->mds_map.epoch = epoch;
}


/**
 * Given one of the following forms:
 *   <fs name>:<rank>
 *   <fs id>:<rank>
 *   <rank>
 *
 * Parse into a mds_role_t.  The rank-only form is only valid
 * if legacy_client_ns is set.
 */
int FSMap::parse_role(
    const std::string &role_str,
    mds_role_t *role,
    std::ostream &ss) const
{
  auto colon_pos = role_str.find(":");

  if (colon_pos != std::string::npos && colon_pos != role_str.size()) {
    auto fs_part = role_str.substr(0, colon_pos);
    auto rank_part = role_str.substr(colon_pos + 1);

    std::string err;
    fs_cluster_id_t fs_id = FS_CLUSTER_ID_NONE;
    long fs_id_i = strict_strtol(fs_part.c_str(), 10, &err);
    if (fs_id_i < 0 || !err.empty()) {
      // Try resolving as name
      auto fs = get_filesystem(fs_part);
      if (fs == nullptr) {
        ss << "Unknown filesystem name '" << fs_part << "'";
        return -EINVAL;
      } else {
        fs_id = fs->fscid;
      }
    } else {
      fs_id = fs_id_i;
    }

    mds_rank_t rank;
    long rank_i = strict_strtol(rank_part.c_str(), 10, &err);
    if (rank_i < 0 || !err.empty()) {
      ss << "Invalid rank '" << rank_part << "'";
      return -EINVAL;
    } else {
      rank = rank_i;
    }

    *role = {fs_id, rank};
  } else {
    std::string err;
    long who_i = strict_strtol(role_str.c_str(), 10, &err);
    if (who_i < 0 || !err.empty()) {
      ss << "Invalid rank '" << role_str << "'";
      return -EINVAL;
    }

    if (legacy_client_fscid == FS_CLUSTER_ID_NONE) {
      ss << "No filesystem selected";
      return -ENOENT;
    } else {
      *role = mds_role_t(legacy_client_fscid, who_i);
    }
  }

  // Now check that the role actually exists
  if (get_filesystem(role->fscid) == nullptr) {
    ss << "Filesystem with ID '" << role->fscid << "' not found";
    return -ENOENT;
  }

  auto fs = get_filesystem(role->fscid);
  if (fs->mds_map.in.count(role->rank) == 0) {
    ss << "Rank '" << role->rank << "' not found";
    return -ENOENT;
  }

  return 0;
}

