#ifndef __TCP_H
#define __TCP_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

typedef struct sockaddr_in tcpaddr_t;

inline ostream& operator<<(ostream& out, tcpaddr_t &a)
{
  unsigned char addr[4];
  memcpy((char*)addr, (char*)&a.sin_addr.s_addr, 4);
  out << (unsigned)addr[0] << "."
	  << (unsigned)addr[1] << "."
	  << (unsigned)addr[2] << "."
	  << (unsigned)addr[3] << ":"
	  << (int)a.sin_port;
  return out;
}

extern bool tcp_read(int sd, char *buf, int len);
extern int tcp_write(int sd, char *buf, int len);


#endif
