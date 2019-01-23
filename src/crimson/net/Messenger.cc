// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messenger.h"
#include "SocketMessenger.h"

namespace ceph::net {

seastar::future<Messenger*>
Messenger::create(const entity_name_t& name,
                  const std::string& lname,
                  const uint64_t nonce,
                  const int master_sid)
{
  return create_sharded<SocketMessenger>(name, lname, nonce, master_sid)
    .then([](Messenger *msgr) {
      return msgr;
    });
}

} // namespace ceph::net
