
#include "tcp.h"

/******************
 * tcp crap
 */

bool tcp_read(int sd, char *buf, int len)
{
  while (len > 0) {
	int got = ::recv( sd, buf, len, 0 );
	if (got == 0) {
	  dout(DBL) << "tcp_read socket " << sd << " closed" << endl;
	  return false;
	}
	if (got < 0) {
	  dout(DBL) << "tcp_read bailing with " << got << endl;
	  return false;
	}
	assert(got >= 0);
	len -= got;
	buf += got;
	//dout(DBL) << "tcp_read got " << got << ", " << len << " left" << endl;
  }
  return true;
}

int tcp_write(int sd, char *buf, int len)
{
  //dout(DBL) << "tcp_write writing " << len << endl;
  assert(len > 0);
  while (len > 0) {
	int did = ::send( sd, buf, len, 0 );
	if (did < 0) {
	  dout(1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
	  cerr << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
	}
	//assert(did >= 0);
	if (did < 0) return did;
	len -= did;
	buf += did;
	//dout(DBL) << "tcp_write did " << did << ", " << len << " left" << endl;
  }
  return 0;
}

