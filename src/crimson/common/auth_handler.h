// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class EntityName;
class AuthCapsInfo;

namespace ceph::common {
class AuthHandler {
public:
  // the peer just got authorized
  virtual void handle_authentication(const EntityName& name,
				     uint64_t global_id,
				     const AuthCapsInfo& caps) = 0;
  virtual ~AuthHandler() = default;
};
}
