// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Fwd.h"

class AuthAuthorizer;

namespace ceph::common {
class AuthService {
public:
  virtual AuthAuthorizer* get_authorizer(peer_type_t peer) const = 0;
  virtual ~AuthService() = default;
};
}
