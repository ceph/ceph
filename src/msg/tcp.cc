// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <poll.h>
#include "tcp.h"

/******************
 * tcp crap
 */

int tcp_read(int sd, char *buf, int len) {
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLIN | POLLHUP | POLLRDHUP | POLLNVAL | POLLERR;
  while (len > 0) {
    if (poll(&pfd, 1, -1) < 0)
      return -1;

    if (!(pfd.revents & POLLIN))
      return -1;

    /*
     * although we turn on the MSG_DONTWAIT flag, we don't expect
     * receivng an EAGAIN, as we polled on the socket, so there
     * should be data waiting for us.
     */
    int got = ::recv( sd, buf, len, MSG_DONTWAIT );
    if (got <= 0) {
      //char buf[100];
      //generic_dout(0) << "tcp_read socket " << sd << " returned " << got
      //<< " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      return -1;
    }
    len -= got;
    buf += got;
    //generic_dout(DBL) << "tcp_read got " << got << ", " << len << " left" << dendl;
  }
  return len;
}

int tcp_write(int sd, const char *buf, int len) {
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLOUT | POLLHUP | POLLRDHUP | POLLNVAL | POLLERR;

  if (poll(&pfd, 1, -1) < 0)
    return -1;

  if (!(pfd.revents & POLLOUT))
    return -1;

  //generic_dout(DBL) << "tcp_write writing " << len << dendl;
  assert(len > 0);
  while (len > 0) {
    int did = ::send( sd, buf, len, MSG_NOSIGNAL );
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

int tcp_hostlookup(char *str, sockaddr_in& ta)
{
  char *host = str;
  char *port = 0;
  
  for (int i=0; str[i]; i++) {
    if (str[i] == ':') {
      port = str+i+1;
      str[i] = 0;
      break;
    }
  }
  if (!port) {
    cerr << "addr '" << str << "' doesn't look like 'host:port'" << std::endl;
    return -1;
  } 
  //cout << "host '" << host << "' port '" << port << "'" << std::endl;

  int iport = atoi(port);
  
  struct hostent *myhostname = gethostbyname( host ); 
  if (!myhostname) {
    cerr << "host " << host << " not found" << std::endl;
    return -1;
  }

  memset(&ta, 0, sizeof(ta));

  //cout << "addrtype " << myhostname->h_addrtype << " len " << myhostname->h_length << std::endl;

  ta.sin_family = myhostname->h_addrtype;
  memcpy((char *)&ta.sin_addr,
         myhostname->h_addr, 
         myhostname->h_length);
  ta.sin_port = iport;
    
  cout << "lookup '" << host << ":" << port << "' -> " << ta << std::endl;

  return 0;
}
