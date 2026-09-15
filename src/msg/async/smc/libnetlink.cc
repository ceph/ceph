// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Copyright IBM Corp. 2026
 *
 * Author(s): Ursula Braun <ubraun@linux.ibm.com>
 *            Guvenc Gulce <guvenc@linux.ibm.com>
 *            Aliaksei Makarau <aliaksei.makarau@ibm.com>
 *
 * Userspace program for SMC Information display
 */
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <string.h>
#include <sys/socket.h>

#include <libnl3/netlink/msg.h>

#include "smctools_common.h"
#include "libnetlink.h"

#define MAGIC_SEQ 123456

int smc_id = 0;
struct nl_sock *sk;

NetlinkHandler::NetlinkHandler() : m_isOpen(false) {
  memset(&m_handle, 0, sizeof(m_handle));
  m_handle.fd = -1;
}

NetlinkHandler::~NetlinkHandler() {
  close();
}

NetlinkHandler::NetlinkHandler(NetlinkHandler&& other) noexcept
  : m_handle(other.m_handle), m_isOpen(other.m_isOpen) {
  other.m_handle.fd = -1;
  other.m_isOpen = false;
}

NetlinkHandler& NetlinkHandler::operator=(NetlinkHandler&& other) noexcept {
  if (this != &other) {
    close();
    m_handle = other.m_handle;
    m_isOpen = other.m_isOpen;
    other.m_handle.fd = -1;
    other.m_isOpen = false;
  }
  return *this;
}

int NetlinkHandler::open() {
  if (m_isOpen) {
    return EXIT_SUCCESS; // Already open
  }

  socklen_t addr_len;
  int rcvbuf = 1024 * 1024;
  int sndbuf = 32768;

  m_handle.fd = socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_SOCK_DIAG);
  if (m_handle.fd < 0) {
    return -errno;
  }

  if (::setsockopt(m_handle.fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf)) < 0) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
    return -errno;
  }

  if (::setsockopt(m_handle.fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
    return -errno;
  }

  memset(&m_handle.local, 0, sizeof(m_handle.local));
  m_handle.local.nl_family = AF_NETLINK;
  m_handle.local.nl_groups = 0;

  if (::bind(m_handle.fd, (struct sockaddr *)&m_handle.local, sizeof(m_handle.local)) < 0) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
    return -errno;
  }

  addr_len = sizeof(m_handle.local);
  if (::getsockname(m_handle.fd, (struct sockaddr *)&m_handle.local, &addr_len) < 0) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
    return -errno;
  }

  if (addr_len != sizeof(m_handle.local)) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
    return -errno;
  }

  if (m_handle.local.nl_family != AF_NETLINK) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
    return -errno;
  }

  m_handle.seq = time(NULL);
  m_isOpen = true;
  return EXIT_SUCCESS;
}

void NetlinkHandler::close() {
  if (m_handle.fd >= 0) {
    ::close(m_handle.fd);
    m_handle.fd = -1;
  }
  m_isOpen = false;
}

bool NetlinkHandler::isOpen() const {
  return m_isOpen;
}

int NetlinkHandler::dump(std::function<void(struct nlmsghdr*)> handler) {
  if (!m_isOpen) {
    return EXIT_FAILURE;
  }

  ssize_t msglen;
  int found_done = 0;
  struct sockaddr_nl nladdr;
  struct iovec iov;
  struct msghdr msg = {
    .msg_name = &nladdr,
    .msg_namelen = sizeof(nladdr),
    .msg_iov = &iov,
    .msg_iovlen = 1,
  };
  char buf[32768];
  struct nlmsghdr *h = (struct nlmsghdr *)buf;

  memset(buf, 0, sizeof(buf));
  iov.iov_base = buf;
  iov.iov_len = sizeof(buf);

again:
  msglen = recvmsg(m_handle.fd, &msg, 0);
  if (msglen < 0) {
    if (errno == EINTR || errno == EAGAIN)
      goto again;
    return -errno;
  }
  if (msglen == 0) {
      return -errno;
  }

  while (NLMSG_OK(h, static_cast<__u32>(msglen))) {
    if (h->nlmsg_type == NLMSG_DONE) {
      found_done = 1;
      break;
    }
    if (h->nlmsg_type == NLMSG_ERROR) {
      return EXIT_FAILURE;
    }
    handler(h);
    h = NLMSG_NEXT(h, msglen);
  }

  if (msg.msg_flags & MSG_TRUNC) {
    goto again;
  }
  if (!found_done) {
    h = (struct nlmsghdr *)buf;
    goto again;
  }

  return EXIT_SUCCESS;
}

int NetlinkHandler::sendDiagRequest(unsigned char cmd) {
  if (!m_isOpen) {
    return EXIT_FAILURE;
  }

  struct sockaddr_nl nladdr = {.nl_family = AF_NETLINK};
  DIAG_REQUEST(req, struct smc_diag_req r, MAGIC_SEQ);
  struct msghdr msg;
  struct iovec iov[1];
  size_t iovlen = 1;

  memset(&req.r, 0, sizeof(req.r));
  req.r.diag_family = PF_SMC;

  iov[0] = (struct iovec) {
    .iov_base = &req,
    .iov_len = sizeof(req)
  };

  msg = (struct msghdr) {
    .msg_name = (void *)&nladdr,
    .msg_namelen = sizeof(nladdr),
    .msg_iov = iov,
    .msg_iovlen = iovlen,
  };

  req.r.diag_ext = cmd;

  if (::sendmsg(m_handle.fd, &msg, 0) < 0) {
    return -errno;
  }

  return EXIT_SUCCESS;
}

int NetlinkHandler::getFd() const {
  return m_handle.fd;
}

void NetlinkHandler::setDumpSeq(__u32 seq) {
  m_handle.dump = seq;
}

// Utility function
void parse_rtattr(struct rtattr *tb[], int max, struct rtattr *rta, int len) {
  unsigned short type;
  memset(tb, 0, sizeof(struct rtattr *) * (max + 1));
  while (RTA_OK(rta, len)) {
    type = rta->rta_type;
    if ((type <= max) && (!tb[type]))
      tb[type] = rta;
    rta = RTA_NEXT(rta, len);
  }
}
