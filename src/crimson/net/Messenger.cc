// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messenger.h"
#include "SocketMessenger.h"

namespace crimson::net {

MessengerRef
Messenger::create(const entity_name_t& name,
                  const std::string& lname,
                  uint64_t nonce,
                  bool dispatch_only_on_this_shard)
{
  return seastar::make_shared<SocketMessenger>(
      name, lname, nonce, dispatch_only_on_this_shard);
}

} // namespace crimson::net
