// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef __TCP_H
#define __TCP_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>

typedef struct sockaddr_in tcpaddr_t;

using std::ostream;

inline ostream& operator<<(ostream& out, const tcpaddr_t &a)
{
  unsigned char addr[4];
  memcpy((char*)addr, (char*)&a.sin_addr.s_addr, 4);
  out << (unsigned)addr[0] << "."
      << (unsigned)addr[1] << "."
      << (unsigned)addr[2] << "."
      << (unsigned)addr[3] << ":"
      << ntohs(a.sin_port);
  return out;
}

inline bool tcp_read(int sd, char *buf, int len) {
  while (len > 0) {
    int got = ::recv( sd, buf, len, 0 );
    if (got <= 0) {
      //dout(18) << "tcp_read socket " << sd << " closed" << endl;
      return false;
    }
    len -= got;
    buf += got;
    //dout(DBL) << "tcp_read got " << got << ", " << len << " left" << endl;
  }
  return true;
}

inline int tcp_write(int sd, char *buf, int len) {
  //dout(DBL) << "tcp_write writing " << len << endl;
  assert(len > 0);
  while (len > 0) {
    int did = ::send( sd, buf, len, 0 );
    if (did < 0) {
      //dout(1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
      //cerr << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
      return did;
    }
    len -= did;
    buf += did;
    //dout(DBL) << "tcp_write did " << did << ", " << len << " left" << endl;
  }
  return 0;
}


extern int tcp_hostlookup(char *str, tcpaddr_t& ta);

inline bool operator==(const tcpaddr_t& a, const tcpaddr_t& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) == 0;
}
inline bool operator!=(const tcpaddr_t& a, const tcpaddr_t& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) != 0;
}


#endif
