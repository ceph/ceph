// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_CAPS_IMPL_H
#define CEPH_CLIENT_CAPS_IMPL_H

#include "ClientCaps.h"
#include "Client.h"
#include "Inode.h"

template<typename Func>
void ClientCaps::process_delayed_caps(ceph::coarse_mono_time now, bool mount_aborted, Func&& func)
{
  std::vector<Inode*> to_process;

  {
    std::scoped_lock lock(caps_lock);
    xlist<Inode*>::iterator p = delayed_list.begin();
    while (!p.end()) {
      Inode *in = *p;
      ++p;
      if (!mount_aborted && !client->is_unmounting() &&
	  in->hold_caps_until > now)
        break;
      delayed_list.pop_front();
      to_process.push_back(in);
    }
  }

  for (Inode *in : to_process) {
    func(in);
  }
}

#endif // CEPH_CLIENT_CAPS_IMPL_H