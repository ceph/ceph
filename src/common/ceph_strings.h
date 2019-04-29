// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>

const char *ceph_entity_type_name(int type);
const char *ceph_con_mode_name(int con_mode);
const char *ceph_osd_op_name(int op);
const char *ceph_osd_state_name(int s);
const char *ceph_release_name(int r);
std::uint64_t ceph_release_features(int r);
int ceph_release_from_features(std::uint64_t features);
int ceph_release_from_name(const char *s);
const char *ceph_osd_watch_op_name(int o);
const char *ceph_osd_alloc_hint_flag_name(int f);
const char *ceph_mds_state_name(int s);
const char *ceph_session_op_name(int op);
const char *ceph_mds_op_name(int op);
const char *ceph_cap_op_name(int op);
const char *ceph_lease_op_name(int o);
const char *ceph_snap_op_name(int o);
const char *ceph_watch_event_name(int e);
const char *ceph_pool_op_name(int op);
const char *ceph_osd_backoff_op_name(int op);
