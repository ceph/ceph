// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Messenger.h"
#include "SocketMessenger.h"

namespace crimson::net {

seastar::future<Messenger*>
Messenger::create(const entity_name_t& name,
                  const std::string& lname,
                  const uint64_t nonce,
                  const int master_sid)
{
  // enforce the messenger to a specific core (master_sid)
  // TODO: drop the cross-core feature and cleanup the related interfaces in
  // the future.
  ceph_assert(master_sid >= 0);
  return create_sharded<SocketMessenger>(
      name, lname, nonce, static_cast<seastar::shard_id>(master_sid)
  ).then([](Messenger *msgr) {
    return msgr;
  });
}

} // namespace crimson::net
