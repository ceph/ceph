#include "auth/Auth.h"
#include "Dispatcher.h"

namespace ceph::net
{
seastar::future<std::unique_ptr<AuthAuthorizer>>
Dispatcher::ms_get_authorizer(peer_type_t)
{
  return seastar::make_ready_future<std::unique_ptr<AuthAuthorizer>>(nullptr);
}
}
