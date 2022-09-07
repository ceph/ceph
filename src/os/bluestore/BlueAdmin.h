// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSD_BLUEADMIN_H
#define CEPH_OSD_BLUEADMIN_H


#include "BlueStore.h"
#include "common/admin_socket.h"

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;

class BlueStore::SocketHook : public AdminSocketHook {
  BlueStore& store;

public:
  SocketHook(BlueStore& store);
  virtual ~SocketHook();
  int call(std::string_view command,
	   const cmdmap_t& cmdmap,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& out) override;
};
#endif
