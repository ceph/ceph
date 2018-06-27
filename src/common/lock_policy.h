// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

namespace ceph::internal {

enum class LockPolicy {
  SINGLE,
  MUTEX,
};

}
