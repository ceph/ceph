// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <poll.h>
#include "tcp.h"

/******************
 * tcp crap
 */
int tcp_read(int sd, char *buf, int len, int timeout) 
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLIN | POLLHUP | POLLRDHUP | POLLNVAL | POLLERR;
  while (len > 0) {
    if (poll(&pfd, 1, timeout) <= 0)
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

int tcp_wait(int sd, int timeout) 
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLIN | POLLHUP | POLLRDHUP | POLLNVAL | POLLERR;

  if (poll(&pfd, 1, timeout) <= 0)
    return -1;

  if (!(pfd.revents & POLLIN))
    return -1;

  return 0;
}

int tcp_read_nonblocking(int sd, char *buf, int len)
{
  return ::recv(sd, buf, len, MSG_DONTWAIT);
}

int tcp_write(int sd, const char *buf, int len)
{
  if (sd < 0)
    return -1;
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
