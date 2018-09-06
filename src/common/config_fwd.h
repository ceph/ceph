// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include "lock_policy.h"

namespace ceph {
template<class ConfigProxy> class md_config_obs_impl;
}

struct md_config_t;
class ConfigProxy;
using md_config_obs_t = ceph::md_config_obs_impl<ConfigProxy>;
