// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <poll.h>
#include "tcp.h"
#include "common/config.h"

/******************
 * tcp crap
 */
int tcp_read(int sd, char *buf, int len, int timeout) 
{
  if (sd < 0)
    return -1;

  while (len > 0) {

    if (g_conf.ms_inject_socket_failures && sd >= 0) {
      if (rand() % g_conf.ms_inject_socket_failures == 0) {
	generic_dout(0) << "injecting socket failure" << dendl;
	::shutdown(sd, SHUT_RDWR);
      }
    }

    if (tcp_read_wait(sd, timeout) < 0)
      return -1;

    int got = tcp_read_nonblocking(sd, buf, len);

    if (got < 0)
      return -1;

    len -= got;
    buf += got;
    //generic_dout(DBL) << "tcp_read got " << got << ", " << len << " left" << dendl;
  }
  return len;
}

int tcp_read_wait(int sd, int timeout) 
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLIN | POLLRDHUP;

  if (poll(&pfd, 1, timeout) <= 0)
    return -1;

  if (pfd.revents & (POLLERR | POLLHUP | POLLRDHUP | POLLNVAL))
    return -1;

  if (!(pfd.revents & POLLIN))
    return -1;

  return 0;
}

/* This function can only be called if poll/select says there is
 * data available.  Otherwise we cannot properly interpret a
 * read of 0 bytes.
 */
int tcp_read_nonblocking(int sd, char *buf, int len)
{
again:
  int got = ::recv( sd, buf, len, MSG_DONTWAIT );
  if (got < 0) {
    if (errno == EAGAIN || errno == EINTR) {
      goto again;
    } else {
      char buf[100];
      generic_dout(10) << "tcp_read_nonblocking socket " << sd << " returned "
        << got << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
      return -1;
    }
  } else if (got == 0) {
    /* poll() said there was data, but we didn't read any - peer
     * sent a FIN.  Maybe POLLRDHUP signals this, but this is
     * standard socket behavior as documented by Stevens.
     */
    return -1;
  }
  return got;
}

int tcp_write(int sd, const char *buf, int len)
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLOUT | POLLHUP | POLLRDHUP | POLLNVAL | POLLERR;

  if (g_conf.ms_inject_socket_failures && sd >= 0) {
    if (rand() % g_conf.ms_inject_socket_failures == 0) {
      generic_dout(0) << "injecting socket failure" << dendl;
      ::shutdown(sd, SHUT_RDWR);
    }
  }

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
