// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_TCP_H
#define CEPH_TCP_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>

#include <ostream>

using std::ostream;

class CephContext;

extern int tcp_read(CephContext *cct, int sd, char *buf, int len, int timeout=-1);
extern int tcp_read_wait(int sd, int timeout);
extern int tcp_read_nonblocking(CephContext *cct, int sd, char *buf, int len);
extern int tcp_write(CephContext *cct, int sd, const char *buf, int len);

inline bool operator==(const sockaddr_in& a, const sockaddr_in& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) == 0;
}
inline bool operator!=(const sockaddr_in& a, const sockaddr_in& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) != 0;
}

#endif
