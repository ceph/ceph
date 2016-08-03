#include "MDSRank.h"
#include "MDCache.h"
#include "Locker.h"

#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"

#define dout_subsys ceph_subsys_mds

/* This function DOES put the passed message before returning */
void Locker::dispatch(Message *m)
{
  switch (m->get_type()) {
      // client sync
    case CEPH_MSG_CLIENT_CAPS:
      handle_client_caps(static_cast<MClientCaps*>(m));
      break;
    case CEPH_MSG_CLIENT_CAPRELEASE:
      handle_client_cap_release(static_cast<MClientCapRelease*>(m));
      break;
    case CEPH_MSG_CLIENT_LEASE:
      handle_client_lease(static_cast<MClientLease*>(m));
      break;
    default:
      derr << "locker unknown message " << m->get_type() << dendl;
      assert(0 == "locker unknown message");
  }
}

void Locker::handle_client_caps(MClientCaps *m)
{
    dout(10) << "handle_client_caps " << *m << dendl;
}

void Locker::handle_client_cap_release(MClientCapRelease *m)
{
    dout(10) << "handle_client_cap_release " << *m << dendl;
}

void Locker::handle_client_lease(MClientLease *m)
{
    dout(10) << "handle_client_lease " << *m << dendl;
}
