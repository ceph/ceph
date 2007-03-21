
#include "tcp.h"

/******************
 * tcp crap
 */

bool tcp_read(int sd, char *buf, int len)
{
  //dout(-20) << "tcp_read wants " << len << endl;
  while (len > 0) {
    int got = ::recv( sd, buf, len, 0 );
    if (got == 0) {
      dout(18) << "tcp_read socket " << sd << " closed" << endl;
      return false;
    }
    if (got < 0) {
      dout(18) << "tcp_read bailing with " << got << endl;
      return false;
    }
    assert(got >= 0);
    len -= got;
    buf += got;
    //dout(-20) << "tcp_read got " << got << ", " << len << " left" << endl;
  }
  return true;
}

int tcp_write(int sd, char *buf, int len, bool more)
{
  //dout(DBL) << "tcp_write writing " << len << endl;
  assert(len > 0);
  while (len > 0) {
    int did = ::send( sd, buf, len, more ? MSG_MORE:0 );
    if (did < 0) {
      dout(1) << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
      //cerr << "tcp_write error did = " << did << "  errno " << errno << " " << strerror(errno) << endl;
    }
    //assert(did >= 0);
    if (did < 0) return did;
    len -= did;
    buf += did;
    //dout(-20) << "tcp_write did " << did << ", " << len << " left" << endl;
  }
  return 0;
}


int tcp_hostlookup(char *str, tcpaddr_t& ta)
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
    cerr << "addr '" << str << "' doesn't look like 'host:port'" << endl;
    return -1;
  } 
  //cout << "host '" << host << "' port '" << port << "'" << endl;

  int iport = atoi(port);
  
  struct hostent *myhostname = gethostbyname( host ); 
  if (!myhostname) {
    cerr << "host " << host << " not found" << endl;
    return -1;
  }

  memset(&ta, 0, sizeof(ta));

  //cout << "addrtype " << myhostname->h_addrtype << " len " << myhostname->h_length << endl;

  ta.sin_family = myhostname->h_addrtype;
  memcpy((char *)&ta.sin_addr,
         myhostname->h_addr, 
         myhostname->h_length);
  ta.sin_port = iport;
    
  cout << "lookup '" << host << ":" << port << "' -> " << ta << endl;

  return 0;
}
