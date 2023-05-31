// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messenger.h"
#include "SocketMessenger.h"

namespace crimson::net {

MessengerRef
Messenger::create(const entity_name_t& name,
                  const std::string& lname,
                  uint64_t nonce,
                  bool is_fixed_cpu)
{
  return seastar::make_shared<SocketMessenger>(
      name, lname, nonce, is_fixed_cpu);
}

} // namespace crimson::net
