// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "RDMAStack.h"
#include "Device.h"
#include "RDMAConnTCP.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " RDMAConnTCP "

static const uint32_t TCP_MSG_LEN = sizeof("0000:00000000:00000000:00000000:00000000000000000000000000000000");

// 1 means no valid buffer read, 0 means got enough buffer
// else return < 0 means error
int RDMAConnTCP::recv_msg(CephContext *cct, int sd, IBSYNMsg& im)
{
  char msg[TCP_MSG_LEN];
  char gid[33];
  ssize_t r = ::read(sd, &msg, sizeof(msg));
  // Drop incoming qpt
  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      ldout(cct, 0) << __func__ << " injecting socket failure" << dendl;
      return -EINVAL;
    }
  }
  if (r < 0) {
    r = -errno;
    lderr(cct) << __func__ << " got error " << r << ": "
               << cpp_strerror(r) << dendl;
  } else if (r == 0) { // valid disconnect message of length 0
    ldout(cct, 10) << __func__ << " got disconnect message " << dendl;
  } else if ((size_t)r != sizeof(msg)) { // invalid message
    ldout(cct, 1) << __func__ << " got bad length (" << r << ") " << dendl;
    r = -EINVAL;
  } else { // valid message
    sscanf(msg, "%hu:%x:%x:%x:%s", &(im.lid), &(im.qpn), &(im.psn), &(im.peer_qpn),gid);
    wire_gid_to_gid(gid, &(im.gid));
    ldout(cct, 5) << __func__ << " recevd: " << im.lid << ", " << im.qpn << ", " << im.psn << ", " << im.peer_qpn << ", " << gid  << dendl;
  }
  return r;
}

int RDMAConnTCP::send_msg(CephContext *cct, int sd, IBSYNMsg& im)
{
  int retry = 0;
  ssize_t r;

  char msg[TCP_MSG_LEN];
  char gid[33];
retry:
  gid_to_wire_gid(&(im.gid), gid);
  sprintf(msg, "%04x:%08x:%08x:%08x:%s", im.lid, im.qpn, im.psn, im.peer_qpn, gid);
  ldout(cct, 10) << __func__ << " sending: " << im.lid << ", " << im.qpn << ", " << im.psn
                 << ", " << im.peer_qpn << ", "  << gid  << dendl;
  r = ::write(sd, msg, sizeof(msg));
  // Drop incoming qpt
  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      ldout(cct, 0) << __func__ << " injecting socket failure" << dendl;
      return -EINVAL;
    }
  }

  if ((size_t)r != sizeof(msg)) {
    // FIXME need to handle EAGAIN instead of retry
    if (r < 0 && (errno == EINTR || errno == EAGAIN) && retry < 3) {
      retry++;
      goto retry;
    }
    if (r < 0)
      lderr(cct) << __func__ << " send returned error " << errno << ": "
                 << cpp_strerror(errno) << dendl;
    else
      lderr(cct) << __func__ << " send got bad length (" << r << ") " << cpp_strerror(errno) << dendl;
    return -errno;
  }
  return 0;
}

void RDMAConnTCP::wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
  char tmp[9];
  uint32_t v32;
  int i;

  for (tmp[8] = 0, i = 0; i < 4; ++i) {
    memcpy(tmp, wgid + i * 8, 8);
    sscanf(tmp, "%x", &v32);
    *(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
  }
}

void RDMAConnTCP::gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
  for (int i = 0; i < 4; ++i)
    sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
}

ostream &RDMAConnTCP::print(ostream &out) const
{
  return out << "TCP {tcp_fd: " << tcp_fd << "}";
}
