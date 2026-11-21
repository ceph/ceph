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
#pragma once

#include <linux/sock_diag.h>
#include <linux/rtnetlink.h>
#include <arpa/inet.h>
#include <functional>
#include <memory>

#define DIAG_REQUEST(_req, _r, _seq)             \
  struct                                         \
  {                                              \
    struct nlmsghdr nlh;                         \
    _r;                                          \
  } _req = {                                     \
    .nlh = {                                     \
      .nlmsg_len = sizeof(_req),                 \
      .nlmsg_type = SOCK_DIAG_BY_FAMILY,         \
      .nlmsg_flags = NLM_F_ROOT | NLM_F_REQUEST, \
      .nlmsg_seq = _seq,                         \
    },                                           \
  }

/**
 * @brief Netlink handler class for managing netlink socket operations
 * 
 * This class encapsulates netlink socket operations with RAII resource management.
 * It provides methods for opening, closing, and dumping netlink socket diagnostics.
 */
class NetlinkHandler {
public:
  NetlinkHandler();
  ~NetlinkHandler();

  NetlinkHandler(const NetlinkHandler&) = delete;
  NetlinkHandler& operator=(const NetlinkHandler&) = delete;

  NetlinkHandler(NetlinkHandler&& other) noexcept;
  NetlinkHandler& operator=(NetlinkHandler&& other) noexcept;

  int open();
  void close();
  bool isOpen() const;

  /**
   * @brief Dump netlink messages and process them with a handler
   * 
   * @param handler Function to process each netlink message
   * @return int EXIT_SUCCESS on success, EXIT_FAILURE on error
   */
  int dump(std::function<void(struct nlmsghdr*)> handler);

  /**
   * @brief Send a socket diagnostics request
   * 
   * @param cmd Command/extension flags to request
   * @return int 0 on success, EXIT_FAILURE on error
   */
  int sendDiagRequest(unsigned char cmd);

  /**
   * @brief Get the file descriptor of the netlink socket
   * 
   * @return int File descriptor or -1 if not open
   */
  int getFd() const;

  /**
   * @brief Set the dump sequence number
   * 
   * @param seq Sequence number to set
   */
  void setDumpSeq(__u32 seq);

private:
  struct rtnl_handle {
    int fd;
    struct sockaddr_nl local;
    struct sockaddr_nl peer;
    __u32 seq;
    __u32 dump;
    int proto;
    FILE *dump_fp;
    int flags;
  };

  rtnl_handle m_handle;
  bool m_isOpen;
};

/**
 * @brief Parse rtattr attributes into an array
 * 
 * @param tb Array to store parsed attributes
 * @param max Maximum attribute index
 * @param rta Pointer to rtattr structure
 * @param len Length of data to parse
 */
void parse_rtattr(struct rtattr *tb[], int max, struct rtattr *rta, int len);
