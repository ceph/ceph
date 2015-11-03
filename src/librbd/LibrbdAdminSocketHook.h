// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_LIBRBDADMINSOCKETHOOK_H
#define CEPH_LIBRBD_LIBRBDADMINSOCKETHOOK_H

#include <map>

#include "common/admin_socket.h"

namespace librbd {

  struct ImageCtx;
  class LibrbdAdminSocketCommand;

  class LibrbdAdminSocketHook : public AdminSocketHook {
  public:
    LibrbdAdminSocketHook(ImageCtx *ictx);
    ~LibrbdAdminSocketHook();

    bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	      bufferlist& out);

  private:
    typedef std::map<std::string,LibrbdAdminSocketCommand*> Commands;

    AdminSocket *admin_socket;
    Commands commands;
  };
}

#endif
