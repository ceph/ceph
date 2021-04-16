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

#include "MDSMapV1.h"

MDSMapV1::MDSMapV1() {}

MDSMapV1::~MDSMapV1() {}

void MDSMapV1::encode(bufferlist& bl, uint64_t features, bool encode_ev) const
{
  std::map<mds_rank_t,int32_t> inc;  // Legacy field, fake it so that
                                     // old-mon peers have something sane
                                     // during upgrade
  for (const auto rank : in) {
    inc.insert(std::make_pair(rank, epoch));
  }

  using ceph::encode;
  ENCODE_START(5, 4, bl);
  encode(epoch, bl);
  encode(flags, bl);
  encode(last_failure, bl);
  encode(root, bl);
  encode(session_timeout, bl);
  encode(session_autoclose, bl);
  encode(max_file_size, bl);
  encode(max_mds, bl);
  encode(mds_info, bl, features);
  encode(data_pools, bl);
  encode(cas_pool, bl);

  __u16 ev = 16;
  encode(ev, bl);
  encode(compat, bl);
  encode(metadata_pool, bl);
  encode(created, bl);
  encode(modified, bl);
  encode(tableserver, bl);
  encode(in, bl);
  encode(inc, bl);
  encode(up, bl);
  encode(failed, bl);
  encode(stopped, bl);
  encode(last_failure_osd_epoch, bl);
  encode(ever_allowed_features, bl);
  encode(explicitly_allowed_features, bl);
  encode(inline_data_enabled, bl);
  encode(enabled, bl);
  encode(fs_name, bl);
  encode(damaged, bl);
  encode(balancer, bl);
  encode(standby_count_wanted, bl);
  encode(old_max_mds, bl);
  {
    ceph_release_t min_compat_client = ceph_release_t::unknown;
    encode(min_compat_client, bl);
  }
  encode(required_client_features, bl);
  ENCODE_FINISH(bl);
}

void MDSMapV1::decode(bufferlist::const_iterator& p)
{
  std::map<mds_rank_t,int32_t> inc;  // Legacy field, parse and drop

  cached_up_features = 0;
  DECODE_START_LEGACY_COMPAT_LEN_16(5, 4, 4, p);
  decode(epoch, p);
  decode(flags, p);
  decode(last_failure, p);
  decode(root, p);
  decode(session_timeout, p);
  decode(session_autoclose, p);
  decode(max_file_size, p);
  decode(max_mds, p);
  decode(mds_info, p);
  if (struct_v < 3) {
    __u32 n;
    decode(n, p);
    while (n--) {
      __u32 m;
      decode(m, p);
      data_pools.push_back(m);
    }
    __s32 s;
    decode(s, p);
    cas_pool = s;
  } else {
    decode(data_pools, p);
    decode(cas_pool, p);
  }

  // kclient ignores everything from here
  __u16 ev = 1;
  if (struct_v >= 2)
    decode(ev, p);
  if (ev >= 3)
    decode(compat, p);
  else
    compat = get_compat_set_base();
  if (ev < 5) {
    __u32 n;
    decode(n, p);
    metadata_pool = n;
  } else {
    decode(metadata_pool, p);
  }
  decode(created, p);
  decode(modified, p);
  decode(tableserver, p);
  decode(in, p);
  decode(inc, p);
  decode(up, p);
  decode(failed, p);
  decode(stopped, p);
  if (ev >= 4)
    decode(last_failure_osd_epoch, p);
  if (ev >= 6) {
    if (ev < 10) {
      // previously this was a bool about snaps, not a flag map
      bool flag;
      decode(flag, p);
      ever_allowed_features = flag ? CEPH_MDSMAP_ALLOW_SNAPS : 0;
      decode(flag, p);
      explicitly_allowed_features = flag ? CEPH_MDSMAP_ALLOW_SNAPS : 0;
    } else {
      decode(ever_allowed_features, p);
      decode(explicitly_allowed_features, p);
    }
  } else {
    ever_allowed_features = 0;
    explicitly_allowed_features = 0;
  }
  if (ev >= 7)
    decode(inline_data_enabled, p);

  if (ev >= 8) {
    ceph_assert(struct_v >= 5);
    decode(enabled, p);
    decode(fs_name, p);
  } else {
    if (epoch > 1) {
      // If an MDS has ever been started, epoch will be greater than 1,
      // assume filesystem is enabled.
      enabled = true;
    } else {
      // Upgrading from a cluster that never used an MDS, switch off
      // filesystem until it's explicitly enabled.
      enabled = false;
    }
  }

  if (ev >= 9) {
    decode(damaged, p);
  }

  if (ev >= 11) {
    decode(balancer, p);
  }

  if (ev >= 12) {
    decode(standby_count_wanted, p);
  }

  if (ev >= 13) {
    decode(old_max_mds, p);
  }

  if (ev >= 14) {
    ceph_release_t min_compat_client;
    if (ev == 14) {
      int8_t r;
      decode(r, p);
      if (r < 0) {
	min_compat_client = ceph_release_t::unknown;
      } else {
	min_compat_client = ceph_release_t{static_cast<uint8_t>(r)};
      }
    } else if (ev >= 15) {
      decode(min_compat_client, p);
    }
    if (ev >= 16) {
      decode(required_client_features, p);
    } else {
      set_min_compat_client(min_compat_client);
    }
  }

  DECODE_FINISH(p);
}

void MDSMapV1::generate_test_instances(std::list<MDSMap*>& ls)
{
  MDSMap *m = new MDSMapV1();
  m->set_max_mds(1);
  m->add_data_pool(0);
  m->set_metadata_pool(1);
  m->set_cas_pool(2);
  m->compat = get_compat_set_all();

  // these aren't the defaults, just in case anybody gets confused
  m->set_session_timeout(61);
  m->set_session_autoclose(301);
  m->set_max_filesize(1<<24);
  ls.push_back(m);
}
