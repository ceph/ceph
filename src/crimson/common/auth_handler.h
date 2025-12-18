// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

class EntityName;
class AuthCapsInfo;

namespace crimson::common {
class AuthHandler {
public:
  // the peer just got authorized
  virtual void handle_authentication(const EntityName& name,
				     const AuthCapsInfo& caps) = 0;
  virtual ~AuthHandler() = default;
};
}
