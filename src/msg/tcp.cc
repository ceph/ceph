// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <poll.h>
#include <errno.h>

#include "tcp.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "common/debug.h"

/******************
 * tcp crap
 *
 * These functions only propagate unrecoverable errors -- if you see an error,
 * close the socket. You can't know what has and hasn't been written out.
 */
/**
 * Read the specified amount of data, in a blocking fashion.
 * If there is an error, return -1 (unrecoverable).
 */
int tcp_read(CephContext *cct, int sd, char *buf, int len, int timeout) 
{
  if (sd < 0)
    return -1;

  while (len > 0) {

    if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
      if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
	lgeneric_dout(cct, 0) << "injecting socket failure sd " << sd << dendl;
	::shutdown(sd, SHUT_RDWR);
      }
    }

    if (tcp_read_wait(sd, timeout) < 0)
      return -1;

    int got = tcp_read_nonblocking(cct, sd, buf, len);

    if (got < 0)
      return -1;

    len -= got;
    buf += got;
    //lgeneric_dout(cct, DBL) << "tcp_read got " << got << ", " << len << " left" << dendl;
  }
  return len;
}

/**
 * Wait for data to become available for reading on the given socket. You
 * can specify a timeout in milliseconds, or -1 to wait forever.
 *
 * @return 0 when data is available, or -1 if there
 * is an error (unrecoverable).
 */
int tcp_read_wait(int sd, int timeout) 
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  short evmask;
  pfd.fd = sd;
  pfd.events = POLLIN;
#if defined(__linux__)
  pfd.events |= POLLRDHUP;
#endif

  if (poll(&pfd, 1, timeout) <= 0)
    return -1;

  evmask = POLLERR | POLLHUP | POLLNVAL;
#if defined(__linux__)
  evmask |= POLLRDHUP;
#endif
  if (pfd.revents & evmask)
    return -1;

  if (!(pfd.revents & POLLIN))
    return -1;

  return 0;
}

/**
 *  Read data off of a given socket.
 *
 * This function can only be called if poll/select says there is data
 * available -- otherwise we can't properly interpret a read of 0 bytes.
 *
 * @return The number of bytes read, or -1 on error (unrecoverable).
 */
int tcp_read_nonblocking(CephContext *cct, int sd, char *buf, int len)
{
again:
  int got = ::recv( sd, buf, len, MSG_DONTWAIT );
  if (got < 0) {
    if (errno == EAGAIN || errno == EINTR) {
      goto again;
    } else {
      lgeneric_dout(cct, 10) << "tcp_read_nonblocking socket " << sd << " returned "
			     << got << " errno " << errno << " " << cpp_strerror(errno) << dendl;
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

/**
 * Write the given data to the given socket. This function will loop until
 * all passed data has been written out.
 *
 * @return 0, or -1 on error (unrecoverable).
 */
int tcp_write(CephContext *cct, int sd, const char *buf, int len)
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLOUT | POLLHUP | POLLNVAL | POLLERR;
#if defined(__linux__)
  pfd.events |= POLLRDHUP;
#endif

  if (cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
      lgeneric_dout(cct, 0) << "injecting socket failure" << dendl;
      ::shutdown(sd, SHUT_RDWR);
    }
  }

  if (poll(&pfd, 1, -1) < 0)
    return -1;

  if (!(pfd.revents & POLLOUT))
    return -1;

  //lgeneric_dout(cct, DBL) << "tcp_write writing " << len << dendl;
  assert(len > 0);
  while (len > 0) {
    int did = ::send( sd, buf, len, MSG_NOSIGNAL );
    if (did < 0) {
      //lgeneric_dout(cct, 1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << dendl;
      //lgeneric_derr(cct, 1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << dendl;
      return did;
    }
    len -= did;
    buf += did;
    //lgeneric_dout(cct, DBL) << "tcp_write did " << did << ", " << len << " left" << dendl;
  }
  return 0;
}
