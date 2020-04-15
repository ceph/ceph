// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messenger.h"
#include "SocketMessenger.h"

namespace crimson::net {

MessengerRef
Messenger::create(const entity_name_t& name,
                  const std::string& lname,
                  const uint64_t nonce)
{
  return seastar::make_shared<SocketMessenger>(name, lname, nonce);
}

} // namespace crimson::net
