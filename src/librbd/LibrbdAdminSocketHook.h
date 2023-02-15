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
    ~LibrbdAdminSocketHook() override;

    int call(std::string_view command, const cmdmap_t& cmdmap,
	     const bufferlist&,
	     Formatter *f,
	     std::ostream& errss,
	     bufferlist& out) override;

  private:
    typedef std::map<std::string,LibrbdAdminSocketCommand*,
		     std::less<>> Commands;

    AdminSocket *admin_socket;
    Commands commands;
  };
}

#endif
