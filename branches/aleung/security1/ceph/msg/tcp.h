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

extern bool tcp_read(int sd, char *buf, int len);
extern int tcp_write(int sd, char *buf, int len, bool more=false);
extern int tcp_hostlookup(char *str, tcpaddr_t& ta);

inline bool operator==(const tcpaddr_t& a, const tcpaddr_t& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) == 0;
}
inline bool operator!=(const tcpaddr_t& a, const tcpaddr_t& b) {
  return strncmp((const char*)&a, (const char*)&b, sizeof(a)) != 0;
}


#endif
