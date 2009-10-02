// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef __TCP_H
#define __TCP_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>

using std::ostream;

inline ostream& operator<<(ostream& out, const sockaddr_storage &ss)
{
  char buf[NI_MAXHOST];
  getnameinfo((struct sockaddr *)&ss, sizeof(ss), buf, sizeof(buf), 0, 0, NI_NUMERICHOST);
  return out << buf;
}

inline ostream& operator<<(ostream& out, const sockaddr_in &ss)
{
  char buf[NI_MAXHOST];
  getnameinfo((struct sockaddr *)&ss, sizeof(ss), buf, sizeof(buf), 0, 0, NI_NUMERICHOST);
  return out << buf;
}


inline int tcp_read(int sd, char *buf, int len) {
  while (len > 0) {
    int got = ::recv( sd, buf, len, 0 );
    if (got <= 0) {
      //generic_dout(18) << "tcp_read socket " << sd << " closed" << dendl;
      return -1;
    }
    len -= got;
    buf += got;
    //generic_dout(DBL) << "tcp_read got " << got << ", " << len << " left" << dendl;
  }
  return len;
}

inline int tcp_write(int sd, const char *buf, int len) {
  //generic_dout(DBL) << "tcp_write writing " << len << dendl;
  assert(len > 0);
  while (len > 0) {
    int did = ::send( sd, buf, len, 0 );
    if (did < 0) {
      //generic_dout(1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << dendl;
      //generic_derr(1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << dendl;
      return did;
    }
    len -= did;
    buf += did;
    //generic_dout(DBL) << "tcp_write did " << did << ", " << len << " left" << dendl;
  }
  return 0;
}


extern int tcp_hostlookup(char *str, sockaddr_in& ta);

inline bool operator==(const sockaddr_in& a, const sockaddr_in& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) == 0;
}
inline bool operator!=(const sockaddr_in& a, const sockaddr_in& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) != 0;
}


#endif
