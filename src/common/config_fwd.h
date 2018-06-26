// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

namespace ceph::internal {

enum class LockPolicy {
  SINGLE,
  MUTEX,
};

template<LockPolicy lp> struct md_config_impl;
template<LockPolicy lp> class md_config_obs_impl;
}

using md_config_t =
  ceph::internal::md_config_impl<ceph::internal::LockPolicy::MUTEX>;
using md_config_obs_t =
  ceph::internal::md_config_obs_impl<ceph::internal::LockPolicy::MUTEX>;
