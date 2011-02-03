
#include "msg_types.h"

#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>

bool entity_addr_t::parse(const char *s, const char **end)
{
  memset(this, 0, sizeof(*this));

  const char *p = s;
  bool brackets = false;
  bool ipv6 = false;
  if (*p == '[') {
    p++;
    brackets = true;
    ipv6 = true;
  }
  
  char buf[39];
  char *o = buf;

  while (o < buf + sizeof(buf) &&
	 *p && ((*p == '.') ||
		(ipv6 && *p == ':') ||
		(*p >= '0' && *p <= '9') ||
		(*p >= 'a' && *p <= 'f') ||
		(*p >= 'A' && *p <= 'F'))) {
    if (*p == ':')
      ipv6 = true;
    *o++ = *p++;
  }
  *o = 0;
  //cout << "buf is '" << buf << "'" << std::endl;

  // ipv4?
  struct in_addr a4;
  struct in6_addr a6;
  if (inet_pton(AF_INET, buf, &a4)) {
    addr4.sin_addr.s_addr = a4.s_addr;
    addr.ss_family = AF_INET;
  } else if (inet_pton(AF_INET6, buf, &a6)) {
    addr.ss_family = AF_INET6;
    memcpy(&addr6.sin6_addr, &a6, sizeof(a6));
  } else {
    //cout << "couldn't parse '" << buf << "'" << std::endl;
    return false;
  }

  if (brackets) {
    if (*p != ']')
      return false;
    p++;
  }
  
  //cout << "p is " << *p << std::endl;
  if (*p == ':') {
    // parse a port, too!
    p++;
    int port = atoi(p);
    set_port(port);
    while (*p && *p >= '0' && *p <= '9')
      p++;
  }

  if (*p == '/') {
    // parse nonce, too
    p++;
    int nonce = atoi(p);
    set_nonce(nonce);
    while (*p && *p >= '0' && *p <= '9')
      p++;
  }

  if (end)
    *end = p;
  return true;
}
