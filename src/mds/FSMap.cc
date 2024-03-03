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

#include <ostream>
#include <algorithm>
#include <ranges>

#include "FSMap.h"
#include "common/debug.h"
#include "common/StackStringStream.h"

#ifdef WITH_SEASTAR
#include "crimson/common/config_proxy.h"
#else
#include "common/config_proxy.h"
#endif
#include "global/global_context.h"
#include "mon/health_check.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "FSMap "

using std::list;
using std::pair;
using std::ostream;
using std::string;
using std::string_view;

using ceph::bufferlist;
using ceph::Formatter;

void ClusterInfo::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(client_name, bl);
  encode(cluster_name, bl);
  encode(fs_name, bl);
  ENCODE_FINISH(bl);
}

void ClusterInfo::decode(ceph::buffer::list::const_iterator &iter) {
  DECODE_START(1, iter);
  decode(client_name, iter);
  decode(cluster_name, iter);
  decode(fs_name, iter);
  DECODE_FINISH(iter);
}

void ClusterInfo::dump(ceph::Formatter *f) const {
  f->dump_string("client_name", client_name);
  f->dump_string("cluster_name", cluster_name);
  f->dump_string("fs_name", fs_name);
}

void ClusterInfo::print(std::ostream& out) const {
  out << "[client_name=" << client_name << ", cluster_name=" << cluster_name
      << ", fs_name=" << fs_name << "]" << std::endl;
}

void Peer::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(uuid, bl);
  encode(remote, bl);
  ENCODE_FINISH(bl);
}

void Peer::decode(ceph::buffer::list::const_iterator &iter) {
  DECODE_START(1, iter);
  decode(uuid, iter);
  decode(remote, iter);
  DECODE_FINISH(iter);
}

void Peer::dump(ceph::Formatter *f) const {
  f->open_object_section(uuid);
  f->dump_object("remote", remote);
  f->close_section();
}

void Peer::print(std::ostream& out) const {
  out << "[uuid=" << uuid << ", remote=" << remote << "]" << std::endl;
}

void MirrorInfo::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(mirrored, bl);
  encode(peers, bl);
  ENCODE_FINISH(bl);
}

void MirrorInfo::decode(ceph::buffer::list::const_iterator &iter) {
  DECODE_START(1, iter);
  decode(mirrored, iter);
  decode(peers, iter);
  DECODE_FINISH(iter);
}

void MirrorInfo::dump(ceph::Formatter *f) const {
  f->open_object_section("peers");
  for (auto &peer : peers) {
    peer.dump(f);
  }
  f->close_section(); // peers
}

void MirrorInfo::print(std::ostream& out) const {
  out << "[peers=" << peers << "]" << std::endl;
}

void Filesystem::dump(Formatter *f) const
{
  f->open_object_section("mdsmap");
  mds_map.dump(f);
  f->close_section();
  f->dump_int("id", fscid);
  if (mirror_info.is_mirrored()) {
    f->open_object_section("mirror_info");
    mirror_info.dump(f);
    f->close_section(); // mirror_info
  }
}

void FSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);
  // Use 'default' naming to match 'set-default' CLI
  f->dump_int("default_fscid", legacy_client_fscid);

  f->open_object_section("compat");
  default_compat.dump(f);
  f->close_section();

  f->open_object_section("feature_flags");
  f->dump_bool("enable_multiple", enable_multiple);
  f->dump_bool("ever_enabled_multiple", ever_enabled_multiple);
  f->close_section();

  f->open_array_section("standbys");
  for (const auto& [gid, info] : standby_daemons) {
    f->open_object_section("info");
    info.dump(f);
    f->dump_int("epoch", standby_epochs.at(gid));
    f->close_section();
  }
  f->close_section();

  f->open_array_section("filesystems");
  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    f->open_object_section("filesystem");
    fs.dump(f);
    f->close_section();
  }
  f->close_section();
}

FSMap &FSMap::operator=(const FSMap &rhs)
{
  epoch = rhs.epoch;
  next_filesystem_id = rhs.next_filesystem_id;
  legacy_client_fscid = rhs.legacy_client_fscid;
  default_compat = rhs.default_compat;
  enable_multiple = rhs.enable_multiple;
  mds_roles = rhs.mds_roles;
  standby_daemons = rhs.standby_daemons;
  standby_epochs = rhs.standby_epochs;

  filesystems.clear();
  for (const auto& [fscid, fs] : rhs.filesystems) {
    filesystems.emplace(std::piecewise_construct, std::forward_as_tuple(fscid), std::forward_as_tuple(fs));
  }

  return *this;
}

void FSMap::generate_test_instances(std::list<FSMap*>& ls)
{
  FSMap* fsmap = new FSMap();

  std::list<MDSMap*> mds_map_instances;
  MDSMap::generate_test_instances(mds_map_instances);

  int k = 20;
  for (auto& mdsmap : mds_map_instances) {
    auto fs = Filesystem();
    fs.fscid = k++;
    fs.mds_map = *mdsmap;
    fsmap->filesystems[fs.fscid] = fs;
    delete mdsmap;
  }

  ls.push_back(fsmap);
}

void FSMap::print(ostream& out) const
{
  out << "e" << epoch << std::endl;
  out << "enable_multiple, ever_enabled_multiple: " << enable_multiple << ","
      << ever_enabled_multiple << std::endl;
  out << "default compat: " << default_compat << std::endl;
  out << "legacy client fscid: " << legacy_client_fscid << std::endl;
  out << " " << std::endl;

  if (filesystems.empty()) {
    out << "No filesystems configured" << std::endl;
  }

  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    fs.print(out);
    out << " " << std::endl << " " << std::endl;  // Space out a bit
  }

  if (!standby_daemons.empty()) {
    out << "Standby daemons:" << std::endl << " " << std::endl;
  }

  for (const auto& p : standby_daemons) {
    out << p.second << std::endl;
  }
}

void FSMap::print_daemon_summary(ostream& out) const
{
  // this appears in the "services:" section of "ceph status"
  int num_up = 0, num_in = 0, num_failed = 0;
  int num_standby_replay = 0;
  for ([[maybe_unused]] auto& [fscid, fs] : filesystems) {
    num_up += fs.mds_map.get_num_up_mds();
    num_in += fs.mds_map.get_num_in_mds();
    num_failed += fs.mds_map.get_num_failed_mds();
    num_standby_replay += fs.mds_map.get_num_standby_replay_mds();
  }
  int num_standby = standby_daemons.size();
  out << num_up << "/" << num_in << " daemons up";
  if (num_failed) {
    out << " (" << num_failed << " failed)";
  }
  if (num_standby) {
    out << ", " << num_standby << " standby";
  }
  if (num_standby_replay) {
    out << ", " << num_standby_replay << " hot standby";
  }
}

void FSMap::print_fs_summary(ostream& out) const
{
  // this appears in the "data:" section of "ceph status"
  if (!filesystems.empty()) {
    int num_failed = 0, num_recovering = 0, num_stopped = 0, num_healthy = 0;
    int num_damaged = 0;
    for ([[maybe_unused]] auto& [fscid, fs] : filesystems) {
      if (fs.mds_map.is_any_damaged()) {
	++num_damaged;
      }
      if (fs.mds_map.is_any_failed()) {
	++num_failed;
      } else if (fs.mds_map.is_degraded()) {
	++num_recovering;
      } else if (fs.mds_map.get_max_mds() == 0) {
	++num_stopped;
      } else {
	++num_healthy;
      }
    }
    out << "    volumes: "
	<< num_healthy << "/" << filesystems.size() << " healthy";
    if (num_recovering) {
      out << ", " << num_recovering << " recovering";
    }
    if (num_failed) {
      out << ", " << num_failed << " failed";
    }
    if (num_stopped) {
      out << ", " << num_stopped << " stopped";
    }
    if (num_damaged) {
      out << "; " << num_damaged << " damaged";
    }
    out << "\n";
  }
}

void FSMap::print_summary(Formatter *f, ostream *out) const
{
  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    for (const auto& [fscid, fs] : filesystems) {
      f->dump_unsigned("id", fscid);
      f->dump_unsigned("up", fs.mds_map.up.size());
      f->dump_unsigned("in", fs.mds_map.in.size());
      f->dump_unsigned("max", fs.mds_map.max_mds);
    }
  } else {
    auto count = filesystems.size();
    if (count <= 3) {
      bool first = true;
      for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
        if (!first) {
          *out << " ";
        }
        if (fs.mds_map.is_degraded()) {
          *out << fs.mds_map.fs_name << ":" << fs.mds_map.up.size() << "/" << fs.mds_map.in.size();
        } else {
          *out << fs.mds_map.fs_name << ":" << fs.mds_map.in.size();
        }
        first = false;
      }
    } else {
      *out << count << " fs";
      unsigned degraded = 0;
      CachedStackStringStream css;
      *css << " (degraded: ";
      for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
        if (fs.mds_map.is_degraded()) {
          degraded++;
          if (degraded <= 3) {
            *css << fs.mds_map.fs_name << ":" << fs.mds_map.up.size() << "/" << fs.mds_map.in.size();
          }
        }
      }
      if (degraded > 0) {
        if (degraded <= 3) {
          *css << ")";
          *out << css->strv();
        } else {
          *out << " (degraded: " << degraded << " fs)";
        }
      }
    }
  }

  if (f) {
    f->open_array_section("by_rank");
  }

  std::map<MDSMap::DaemonState,unsigned> by_state;
  std::map<mds_role_t, std::pair<MDSMap::DaemonState, std::string>> by_rank;
  by_state[MDSMap::DaemonState::STATE_STANDBY] = standby_daemons.size();
  for (const auto& [gid, fscid] : mds_roles) {
    if (fscid == FS_CLUSTER_ID_NONE)
      continue;

    const auto& info = filesystems.at(fscid).mds_map.get_info_gid(gid);
    auto s = std::string(ceph_mds_state_name(info.state));
    if (info.laggy()) {
      s += "(laggy or crashed)";
    }

    if (f) {
      f->open_object_section("mds");
      f->dump_unsigned("filesystem_id", fscid);
      f->dump_unsigned("rank", info.rank);
      f->dump_string("name", info.name);
      f->dump_string("status", s);
      f->dump_unsigned("gid", gid);
      f->close_section();
    } else if (info.state != MDSMap::DaemonState::STATE_STANDBY_REPLAY) {
      by_rank[mds_role_t(fscid, info.rank)] = std::make_pair(info.state, info.name + "=" + s);
    }
    by_state[info.state]++;
  }

  if (f) {
    f->close_section();
  } else {
    if (0 < by_rank.size() && by_rank.size() < 5) {
      if (filesystems.size() > 1) {
        // Disambiguate filesystems
        std::map<std::string, std::string> pretty;
        for (const auto& [role,status] : by_rank) {
          const auto &fs_name = filesystems.at(role.fscid).mds_map.fs_name;
          CachedStackStringStream css;
          *css << fs_name << ":" << role.rank;
          pretty.emplace(std::piecewise_construct, std::forward_as_tuple(css->strv()), std::forward_as_tuple(status.second));
          --by_state[status.first]; /* already printed! */
        }
        *out << " " << pretty;
      } else {
        // Omit FSCID in output when only one filesystem exists
        std::map<mds_rank_t, std::string> shortened;
        for (const auto& [role,status] : by_rank) {
          shortened[role.rank] = status.second;
          --by_state[status.first]; /* already printed! */
        }
        *out << " " << shortened;
      }
    }
    for (const auto& [state, count] : by_state) {
      if (count > 0) {
        auto s = std::string_view(ceph_mds_state_name(state));
        *out << " " << count << " " << s;
      }
    }
  }

  if (f) {
    const auto state = MDSMap::DaemonState::STATE_STANDBY;
    auto&& name = ceph_mds_state_name(state);
    auto count = standby_daemons.size();
    f->dump_unsigned(name, count);
  }

  size_t failed = 0;
  size_t damaged = 0;
  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    failed += fs.mds_map.failed.size();
    damaged += fs.mds_map.damaged.size();
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

mds_gid_t Filesystem::get_standby_replay(mds_gid_t who) const
{
  for (const auto &i : mds_map.mds_info) {
    const auto &info = i.second;
    if (info.state == MDSMap::STATE_STANDBY_REPLAY
        && info.rank == mds_map.mds_info.at(who).rank) {
      return info.global_id;
    }
  }
  return MDS_GID_NONE;
}

const Filesystem& FSMap::create_filesystem(std::string_view name,
    int64_t metadata_pool, int64_t data_pool, uint64_t features,
    fs_cluster_id_t fscid, bool recover)
{
  auto fs = Filesystem();
  fs.mds_map.epoch = epoch;
  fs.mds_map.fs_name = name;
  fs.mds_map.data_pools.push_back(data_pool);
  fs.mds_map.metadata_pool = metadata_pool;
  fs.mds_map.cas_pool = -1;
  fs.mds_map.compat = default_compat;
  fs.mds_map.created = ceph_clock_now();
  fs.mds_map.modified = ceph_clock_now();
  fs.mds_map.enabled = true;

  if (recover) {
    // Populate rank 0 as existing (so don't go into CREATING)
    // but failed (so that next available MDS is assigned the rank)
    fs.mds_map.in.insert(mds_rank_t(0));
    fs.mds_map.failed.insert(mds_rank_t(0));

    fs.mds_map.set_flag(CEPH_MDSMAP_NOT_JOINABLE);
  }

  if (fscid == FS_CLUSTER_ID_NONE) {
    fs.fscid = next_filesystem_id++;
  } else {
    fs.fscid = fscid;
    next_filesystem_id = std::max(fscid,  (fs_cluster_id_t)next_filesystem_id) + 1;
  }

  // File system's ID can be FS_CLUSTER_ID_ANONYMOUS if we're recovering
  // a legacy file system by passing FS_CLUSTER_ID_ANONYMOUS as the desired
  // file system ID
  if (fscid != FS_CLUSTER_ID_ANONYMOUS) {
    // ANONYMOUS is only for upgrades from legacy mdsmaps, we should
    // have initialized next_filesystem_id such that it's never used here.
    ceph_assert(fs.fscid != FS_CLUSTER_ID_ANONYMOUS);
  }

  // Created first filesystem?  Set it as the one
  // for legacy clients to use
  if (filesystems.size() == 0) {
    legacy_client_fscid = fs.fscid;
  }

  auto [it, inserted] = filesystems.emplace(std::piecewise_construct, std::forward_as_tuple(fs.fscid), std::forward_as_tuple(std::move(fs)));
  ceph_assert(inserted);
  return it->second;
}

Filesystem const* FSMap::get_filesystem(std::string_view name) const
{
  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    if (fs.mds_map.fs_name == name) {
      return &fs;
    }
  }
  return nullptr;
}

void FSMap::reset_filesystem(fs_cluster_id_t fscid)
{
  auto fs = get_filesystem(fscid);
  auto new_fs = Filesystem();

  // Populate rank 0 as existing (so don't go into CREATING)
  // but failed (so that next available MDS is assigned the rank)
  new_fs.mds_map.in.insert(mds_rank_t(0));
  new_fs.mds_map.failed.insert(mds_rank_t(0));

  // Carry forward what makes sense
  new_fs.fscid = fs.fscid;
  new_fs.mds_map.inline_data_enabled = fs.mds_map.inline_data_enabled;
  new_fs.mds_map.data_pools = fs.mds_map.data_pools;
  new_fs.mds_map.metadata_pool = fs.mds_map.metadata_pool;
  new_fs.mds_map.cas_pool = fs.mds_map.cas_pool;
  new_fs.mds_map.fs_name = fs.mds_map.fs_name;
  new_fs.mds_map.compat = default_compat;
  new_fs.mds_map.created = ceph_clock_now();
  new_fs.mds_map.modified = ceph_clock_now();
  new_fs.mds_map.standby_count_wanted = fs.mds_map.standby_count_wanted;
  new_fs.mds_map.enabled = true;

  // Remember mds ranks that have ever started. (They should load old inotable
  // instead of creating new one if they start again.)
  new_fs.mds_map.stopped.insert(fs.mds_map.in.begin(), fs.mds_map.in.end());
  new_fs.mds_map.stopped.insert(fs.mds_map.stopped.begin(), fs.mds_map.stopped.end());
  new_fs.mds_map.stopped.erase(mds_rank_t(0));

  // Persist the new FSMap
  filesystems[new_fs.fscid] = new_fs;
}

void FSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  mds_rank_t standby_count_wanted = 0;
  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    // TODO: move get_health up into here so that we can qualify
    // all the messages with what filesystem they're talking about
    fs.mds_map.get_health(summary, detail);

    standby_count_wanted = std::max(standby_count_wanted, fs.mds_map.get_standby_count_wanted((mds_rank_t)standby_daemons.size()));
  }

  if (standby_count_wanted) {
    CachedStackStringStream css;
    *css << "insufficient standby daemons available: have " << standby_daemons.size() << "; want " << standby_count_wanted << " more";
    summary.push_back(make_pair(HEALTH_WARN, css->str()));
  }
}

bool FSMap::check_health(void)
{
  bool changed = false;
  for ([[maybe_unused]] auto& [fscid, fs] : filesystems) {
    changed |= fs.mds_map.check_health((mds_rank_t)standby_daemons.size());
  }
  return changed;
}

void FSMap::get_health_checks(health_check_map_t *checks) const
{
  mds_rank_t standby_count_wanted = 0;
  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    health_check_map_t fschecks;

    fs.mds_map.get_health_checks(&fschecks);

    // Some of the failed ranks might be transient (i.e. there are standbys
    // ready to replace them).  We will report only on "stuck" failed, i.e.
    // ranks which are failed and have no standby replacement available.
    std::set<mds_rank_t> stuck_failed;

    for (const auto &rank : fs.mds_map.failed) {
      auto rep_info = find_replacement_for({fs.fscid, rank});
      if (!rep_info) {
        stuck_failed.insert(rank);
      }
    }

    // FS_WITH_FAILED_MDS
    if (!stuck_failed.empty()) {
      health_check_t& fscheck = checks->get_or_add(
        "FS_WITH_FAILED_MDS", HEALTH_WARN,
        "%num% filesystem%plurals% %hasorhave% a failed mds daemon", 1);
      CachedStackStringStream css;
      *css << "fs " << fs.mds_map.fs_name << " has " << stuck_failed.size()
         << " failed mds" << (stuck_failed.size() > 1 ? "s" : "");
      fscheck.detail.push_back(css->str()); }

    checks->merge(fschecks);
    standby_count_wanted = std::max(
      standby_count_wanted,
      fs.mds_map.get_standby_count_wanted((mds_rank_t)standby_daemons.size()));
  }

  // MDS_INSUFFICIENT_STANDBY
  if (standby_count_wanted) {
    CachedStackStringStream css1, css2;
    *css1 << "insufficient standby MDS daemons available";
    auto& d = checks->get_or_add("MDS_INSUFFICIENT_STANDBY", HEALTH_WARN, css1->str(), 1);
    *css2 << "have " << standby_daemons.size() << "; want " << standby_count_wanted
	  << " more";
    d.detail.push_back(css2->str());
  }
}

void FSMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(STRUCT_VERSION, 6, bl);
  encode(epoch, bl);
  encode(next_filesystem_id, bl);
  encode(legacy_client_fscid, bl);
  encode(default_compat, bl);
  encode(enable_multiple, bl);
  {
    __u32 len = filesystems.size();
    encode(len, bl, features);
    for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
      encode(fs, bl, features);
    }
  }
  encode(mds_roles, bl);
  encode(standby_daemons, bl, features);
  encode(standby_epochs, bl);
  encode(ever_enabled_multiple, bl);
  ENCODE_FINISH(bl);
}

void FSMap::decode(bufferlist::const_iterator& p)
{
  struct_version = 0;
  DECODE_START(STRUCT_VERSION, p);
  DECODE_OLDEST(7);
  struct_version = struct_v;
  decode(epoch, p);
  decode(next_filesystem_id, p);
  decode(legacy_client_fscid, p);
  decode(default_compat, p);
  decode(enable_multiple, p);
  {
    __u32 len;
    decode(len, p);
    filesystems.clear();
    for (__u32 i = 0; i < len; i++) {
      auto fs = Filesystem();
      decode(fs, p); /* need fscid to insert into map */
      [[maybe_unused]] auto [it, inserted] = filesystems.emplace(std::piecewise_construct, std::forward_as_tuple(fs.fscid), std::forward_as_tuple(std::move(fs)));
      ceph_assert(inserted);
    }
  }
  decode(mds_roles, p);
  decode(standby_daemons, p);
  decode(standby_epochs, p);
  if (struct_v >= 7) {
    decode(ever_enabled_multiple, p);
  }
  DECODE_FINISH(p);
}

void FSMap::sanitize(const std::function<bool(int64_t pool)>& pool_exists)
{
  for ([[maybe_unused]] auto& [fscid, fs] : filesystems) {
    fs.mds_map.sanitize(pool_exists);
  }
}

void Filesystem::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(2, 1, bl);
  encode(fscid, bl);
  bufferlist mdsmap_bl;
  mds_map.encode(mdsmap_bl, features);
  encode(mdsmap_bl, bl);
  encode(mirror_info, bl);
  ENCODE_FINISH(bl);
}

void Filesystem::decode(bufferlist::const_iterator& p)
{
  DECODE_START(2, p);
  decode(fscid, p);
  bufferlist mdsmap_bl;
  decode(mdsmap_bl, p);
  auto mdsmap_bl_iter = mdsmap_bl.cbegin();
  mds_map.decode(mdsmap_bl_iter);
  if (struct_v >= 2) {
    decode(mirror_info, p);
  }
  DECODE_FINISH(p);
}

int FSMap::parse_filesystem(std::string_view ns_str, Filesystem const** result) const
{
  std::string ns_err;
  std::string s(ns_str);
  fs_cluster_id_t fscid = strict_strtol(s.c_str(), 10, &ns_err);
  if (!ns_err.empty() || filesystems.count(fscid) == 0) {
    for ([[maybe_unused]] auto& [fscid, fs] : filesystems) {
      if (fs.mds_map.fs_name == s) {
        *result = &fs;
        return 0;
      }
    }
    return -CEPHFS_ENOENT;
  } else {
    *result = &get_filesystem(fscid);
    return 0;
  }
}

void Filesystem::print(std::ostream &out) const
{
  out << "Filesystem '" << mds_map.fs_name
      << "' (" << fscid << ")" << std::endl;
  mds_map.print(out);
  if (mirror_info.is_mirrored()) {
    mirror_info.print(out);
  }
}

bool FSMap::is_any_degraded() const
{
  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    if (fs.mds_map.is_degraded()) {
      return true;
    }
  }
  return false;
}

std::map<mds_gid_t, MDSMap::mds_info_t> FSMap::get_mds_info() const
{
  std::map<mds_gid_t, mds_info_t> result;
  for (const auto &i : standby_daemons) {
    result[i.first] = i.second;
  }

  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    const auto &fs_info = fs.mds_map.get_mds_info();
    for (const auto &j : fs_info) {
      result[j.first] = j.second;
    }
  }

  return result;
}

const MDSMap::mds_info_t* FSMap::get_available_standby(const Filesystem& fs) const
{
  const bool upgradeable = fs.is_upgradeable();
  const mds_info_t* who = nullptr;
  for (const auto& [gid, info] : standby_daemons) {
    ceph_assert(info.rank == MDS_RANK_NONE);
    ceph_assert(info.state == MDSMap::STATE_STANDBY);

    if (info.laggy() || info.is_frozen()) {
      continue;
    } else if (!info.compat.writeable(fs.mds_map.compat)) {
      /* standby is not compatible with this fs */
      continue;
    } else if (!upgradeable && !fs.mds_map.compat.writeable(info.compat)) {
      /* promotion would change fs.mds_map.compat and we're not upgradeable */
      continue;
    }

    if (info.join_fscid == fs.fscid) {
      who = &info;
      break;
    } else if (info.join_fscid == FS_CLUSTER_ID_NONE) {
      who = &info; /* vanilla standby */
    } else if (who == nullptr &&
	       !fs.mds_map.test_flag(CEPH_MDSMAP_REFUSE_STANDBY_FOR_ANOTHER_FS)) {
      who = &info; /* standby for another fs, last resort */
    }
  }
  return who;
}

mds_gid_t FSMap::find_mds_gid_by_name(std::string_view s) const
{
  const auto info = get_mds_info();
  for (const auto &p : info) {
    if (p.second.name == s) {
      return p.first;
    }
  }
  return MDS_GID_NONE;
}

const MDSMap::mds_info_t* FSMap::find_by_name(std::string_view name) const
{
  std::map<mds_gid_t, mds_info_t> result;
  for (const auto &i : standby_daemons) {
    if (i.second.name == name) {
      return &(i.second);
    }
  }

  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    const auto &fs_info = fs.mds_map.get_mds_info();
    for (const auto &j : fs_info) {
      if (j.second.name == name) {
        return &(j.second);
      }
    }
  }

  return nullptr;
}

const MDSMap::mds_info_t* FSMap::find_replacement_for(mds_role_t role) const
{
  auto& fs = get_filesystem(role.fscid);

  // First see if we have a STANDBY_REPLAY
  for (const auto& [gid, info] : fs.mds_map.mds_info) {
    if (info.rank == role.rank && info.state == MDSMap::STATE_STANDBY_REPLAY) {
      if (info.is_frozen()) {
        /* the standby-replay is frozen, do nothing! */
        return nullptr;
      } else {
        ceph_assert(info.compat.writeable(fs.mds_map.compat));
        return &info;
      }
    }
  }

  return get_available_standby(fs);
}

void FSMap::sanity(bool pending) const
{
  /* Only do some sanity checks on **new** FSMaps. Older versions may not be
   * compliant.
   */

  if (legacy_client_fscid != FS_CLUSTER_ID_NONE) {
    ceph_assert(filesystems.count(legacy_client_fscid) == 1);
  }

  for ([[maybe_unused]] const auto& [fscid, fs] : filesystems) {
    ceph_assert(fscid  == fs.fscid);
    for (const auto& [gid, info] : fs.mds_map.mds_info) {
      ceph_assert(info.rank != MDS_RANK_NONE);
      ceph_assert(mds_roles.at(gid) == fscid);
      ceph_assert(standby_daemons.count(gid) == 0);
      ceph_assert(standby_epochs.count(gid) == 0);
      if (info.state != MDSMap::STATE_STANDBY_REPLAY) {
        ceph_assert(fs.mds_map.up.at(info.rank) == gid);
        ceph_assert(fs.mds_map.failed.count(info.rank) == 0);
        ceph_assert(fs.mds_map.damaged.count(info.rank) == 0);
      } else {
        ceph_assert(!pending || fs.mds_map.allows_standby_replay());
      }
      ceph_assert(info.compat.writeable(fs.mds_map.compat));
    }

    auto const& leader = fs.mds_map.get_quiesce_db_cluster_leader();
    auto const& members = fs.mds_map.get_quiesce_db_cluster_members();
    ceph_assert(leader == MDS_GID_NONE || members.contains(leader));
    ceph_assert(std::ranges::all_of(members, [&infos = fs.mds_map.mds_info](auto m){return infos.contains(m);}));

    for (const auto &j : fs.mds_map.up) {
      mds_rank_t rank = j.first;
      ceph_assert(fs.mds_map.in.count(rank) == 1);
      mds_gid_t gid = j.second;
      ceph_assert(fs.mds_map.mds_info.count(gid) == 1);
    }
  }

  for (const auto &i : standby_daemons) {
    ceph_assert(i.second.state == MDSMap::STATE_STANDBY);
    ceph_assert(i.second.rank == MDS_RANK_NONE);
    ceph_assert(i.second.global_id == i.first);
    ceph_assert(standby_epochs.count(i.first) == 1);
    ceph_assert(mds_roles.count(i.first) == 1);
    ceph_assert(mds_roles.at(i.first) == FS_CLUSTER_ID_NONE);
  }

  for (const auto &i : standby_epochs) {
    ceph_assert(standby_daemons.count(i.first) == 1);
  }

  for (const auto &i : mds_roles) {
    if (i.second == FS_CLUSTER_ID_NONE) {
      ceph_assert(standby_daemons.count(i.first) == 1);
    } else {
      ceph_assert(filesystems.count(i.second) == 1);
      ceph_assert(filesystems.at(i.second).mds_map.mds_info.count(i.first) == 1);
    }
  }
}

void FSMap::promote(
    mds_gid_t standby_gid,
    fs_cluster_id_t fscid,
    mds_rank_t assigned_rank)
{
  ceph_assert(gid_exists(standby_gid));
  bool is_standby_replay = mds_roles.at(standby_gid) != FS_CLUSTER_ID_NONE;
  if (!is_standby_replay) {
    ceph_assert(standby_daemons.count(standby_gid));
    ceph_assert(standby_daemons.at(standby_gid).state == MDSMap::STATE_STANDBY);
  }

  auto& fs = filesystems.at(fscid);
  MDSMap &mds_map = fs.mds_map;

  // Insert daemon state to Filesystem
  if (!is_standby_replay) {
    mds_map.mds_info[standby_gid] = standby_daemons.at(standby_gid);
  } else {
    ceph_assert(mds_map.mds_info.count(standby_gid));
    ceph_assert(mds_map.mds_info.at(standby_gid).state == MDSMap::STATE_STANDBY_REPLAY);
    ceph_assert(mds_map.mds_info.at(standby_gid).rank == assigned_rank);
  }
  auto& info = mds_map.mds_info.at(standby_gid);

  if (!fs.mds_map.compat.writeable(info.compat)) {
    ceph_assert(fs.is_upgradeable());
    fs.mds_map.compat.merge(info.compat);
  }

  if (mds_map.stopped.erase(assigned_rank)) {
    // The cluster is being expanded with a stopped rank
    info.state = MDSMap::STATE_STARTING;
  } else if (!mds_map.is_in(assigned_rank)) {
    // The cluster is being expanded with a new rank
    info.state = MDSMap::STATE_CREATING;
  } else {
    // An existing rank is being assigned to a replacement
    info.state = MDSMap::STATE_REPLAY;
    mds_map.failed.erase(assigned_rank);
  }
  info.rank = assigned_rank;
  info.inc = epoch;
  mds_roles.at(standby_gid) = fscid;

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
  mds_map.modified = ceph_clock_now();
}

void FSMap::assign_standby_replay(
    const mds_gid_t standby_gid,
    const fs_cluster_id_t leader_ns,
    const mds_rank_t leader_rank)
{
  ceph_assert(mds_roles.at(standby_gid) == FS_CLUSTER_ID_NONE);
  ceph_assert(gid_exists(standby_gid));
  ceph_assert(!gid_has_rank(standby_gid));
  ceph_assert(standby_daemons.count(standby_gid));

  // Insert to the filesystem
  auto& fs = filesystems.at(leader_ns);
  fs.mds_map.mds_info[standby_gid] = standby_daemons.at(standby_gid);
  fs.mds_map.mds_info[standby_gid].rank = leader_rank;
  fs.mds_map.mds_info[standby_gid].state = MDSMap::STATE_STANDBY_REPLAY;
  mds_roles[standby_gid] = leader_ns;

  // Remove from the list of standbys
  standby_daemons.erase(standby_gid);
  standby_epochs.erase(standby_gid);

  // Indicate that Filesystem has been modified
  fs.mds_map.epoch = epoch;
  fs.mds_map.modified = ceph_clock_now();
}

void FSMap::erase(mds_gid_t who, epoch_t blocklist_epoch)
{
  if (mds_roles.at(who) == FS_CLUSTER_ID_NONE) {
    standby_daemons.erase(who);
    standby_epochs.erase(who);
  } else {
    auto& fs = filesystems.at(mds_roles.at(who));
    const auto &info = fs.mds_map.mds_info.at(who);
    if (info.state != MDSMap::STATE_STANDBY_REPLAY) {
      if (info.state == MDSMap::STATE_CREATING) {
        // If this gid didn't make it past CREATING, then forget
        // the rank ever existed so that next time it's handed out
        // to a gid it'll go back into CREATING.
        fs.mds_map.in.erase(info.rank);
      } else {
        // Put this rank into the failed list so that the next available
        // STANDBY will pick it up.
        fs.mds_map.failed.insert(info.rank);
      }
      ceph_assert(fs.mds_map.up.at(info.rank) == info.global_id);
      fs.mds_map.up.erase(info.rank);
    }
    fs.mds_map.mds_info.erase(who);
    fs.mds_map.last_failure_osd_epoch = blocklist_epoch;
    fs.mds_map.epoch = epoch;
    fs.mds_map.modified = ceph_clock_now();
  }

  mds_roles.erase(who);
}

void FSMap::damaged(mds_gid_t who, epoch_t blocklist_epoch)
{
  ceph_assert(mds_roles.at(who) != FS_CLUSTER_ID_NONE);
  auto& fs = filesystems.at(mds_roles.at(who));
  mds_rank_t rank = fs.mds_map.mds_info.at(who).rank;

  erase(who, blocklist_epoch);
  fs.mds_map.failed.erase(rank);
  fs.mds_map.damaged.insert(rank);

  ceph_assert(fs.mds_map.epoch == epoch);
}

/**
 * Update to indicate that the rank `rank` is to be removed
 * from the damaged list of the filesystem `fscid`
 */
bool FSMap::undamaged(const fs_cluster_id_t fscid, const mds_rank_t rank)
{
  auto& fs = filesystems.at(fscid);

  if (fs.mds_map.damaged.erase(rank)) {
    fs.mds_map.failed.insert(rank);
    fs.mds_map.epoch = epoch;
    fs.mds_map.modified = ceph_clock_now();
    return true;
  } else {
    return false;
  }
}

void FSMap::insert(const MDSMap::mds_info_t &new_info)
{
  static const CompatSet empty;

  ceph_assert(new_info.state == MDSMap::STATE_STANDBY);
  ceph_assert(new_info.rank == MDS_RANK_NONE);
  mds_roles[new_info.global_id] = FS_CLUSTER_ID_NONE;
  auto& info = standby_daemons[new_info.global_id];
  info = new_info;
  if (empty.compare(info.compat) == 0) {
    // bootstrap old compat: boot beacon contains empty compat on old (v16.2.4
    // or older) MDS.
    info.compat = MDSMap::get_compat_set_v16_2_4();
  }
  /* TODO remove after R is released
   * Insert INLINE; see comment in MDSMap::decode.
   */
  info.compat.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
  standby_epochs[new_info.global_id] = epoch;
}

std::vector<mds_gid_t> FSMap::stop(mds_gid_t who)
{
  ceph_assert(mds_roles.at(who) != FS_CLUSTER_ID_NONE);
  auto& fs = filesystems.at(mds_roles.at(who));
  const auto &info = fs.mds_map.mds_info.at(who);
  fs.mds_map.up.erase(info.rank);
  fs.mds_map.in.erase(info.rank);
  fs.mds_map.stopped.insert(info.rank);

  // Also drop any standby replays that were following this rank
  std::vector<mds_gid_t> standbys;
  for (const auto &i : fs.mds_map.mds_info) {
    const auto &other_gid = i.first;
    const auto &other_info = i.second;
    if (other_info.rank == info.rank
        && other_info.state == MDSMap::STATE_STANDBY_REPLAY) {
      standbys.push_back(other_gid);
    }
  }

  for (const auto &other_gid : standbys) {
    erase(other_gid, 0);
  }

  fs.mds_map.mds_info.erase(who);
  mds_roles.erase(who);

  fs.mds_map.epoch = epoch;
  fs.mds_map.modified = ceph_clock_now();

  return standbys;
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
    std::string_view role_str,
    mds_role_t *role,
    std::ostream &ss,
    const std::vector<string> &filter) const
{
  int r = parse_role(role_str, role, ss);
  if (r < 0) return r;

  string_view fs_name = get_filesystem(role->fscid).mds_map.get_fs_name();

  if (!filter.empty() &&
      std::find(filter.begin(), filter.end(), fs_name) == filter.end()) {
    if (r >= 0) {
      ss << "Invalid file system";
    }
    return -CEPHFS_ENOENT;
  }

  return r;
}

int FSMap::parse_role(
    std::string_view role_str,
    mds_role_t *role,
    std::ostream &ss) const
{
  size_t colon_pos = role_str.find(":");
  size_t rank_pos;
  Filesystem const* fs;
  if (colon_pos == std::string::npos) {
    if (legacy_client_fscid == FS_CLUSTER_ID_NONE) {
      ss << "No filesystem selected";
      return -CEPHFS_ENOENT;
    }
    fs = &get_filesystem(legacy_client_fscid);
    rank_pos = 0;
  } else {
    if (parse_filesystem(role_str.substr(0, colon_pos), &fs) < 0) {
      ss << "Invalid filesystem";
      return -CEPHFS_ENOENT;
    }
    rank_pos = colon_pos+1;
  }

  mds_rank_t rank;
  std::string err;
  std::string rank_str(role_str.substr(rank_pos));
  long rank_i = strict_strtol(rank_str.c_str(), 10, &err);
  if (rank_i < 0 || !err.empty()) {
    ss << "Invalid rank '" << rank_str << "'";
    return -CEPHFS_EINVAL;
  } else {
    rank = rank_i;
  }

  if (fs->mds_map.in.count(rank) == 0) {
    ss << "Rank '" << rank << "' not found";
    return -CEPHFS_ENOENT;
  }

  *role = {fs->fscid, rank};

  return 0;
}

bool FSMap::pool_in_use(int64_t poolid) const
{
  for ([[maybe_unused]] auto const& [fscid, fs] : filesystems) {
    if (fs.mds_map.is_data_pool(poolid)
        || fs.mds_map.metadata_pool == poolid) {
      return true;
    }
  }
  return false;
}

void FSMap::erase_filesystem(fs_cluster_id_t fscid)
{
  filesystems.erase(fscid);
  for (auto& [gid, info] : standby_daemons) {
    if (info.join_fscid == fscid) {
      modify_daemon(gid, [](auto& info) {
        info.join_fscid = FS_CLUSTER_ID_NONE;
      });
    }
  }
  for ([[maybe_unused]] auto& [fscid, fs] : filesystems) {
    for (auto& [gid, info] : fs.mds_map.get_mds_info()) {
      if (info.join_fscid == fscid) {
        modify_daemon(gid, [](auto& info) {
          info.join_fscid = FS_CLUSTER_ID_NONE;
        });
      }
    }
  }
}

void FSMap::swap_fscids(fs_cluster_id_t fscid1, fs_cluster_id_t fscid2)
{
  auto fs1 = std::move(filesystems.at(fscid1));
  filesystems[fscid1] = std::move(filesystems.at(fscid2));
  filesystems[fscid2] = std::move(fs1);

  auto set_fs1_fscid = [fscid1](auto&& fs) {
    fs.set_fscid(fscid1);
  };
  modify_filesystem(fscid1, std::move(set_fs1_fscid));

  auto set_fs2_fscid = [fscid2](auto&& fs) {
    fs.set_fscid(fscid2);
  };
  modify_filesystem(fscid2, std::move(set_fs2_fscid));
}
