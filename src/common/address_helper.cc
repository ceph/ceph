/*
 * address_helper.cc
 *
 *  Created on: Oct 27, 2013
 *      Author: matt
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "boost/regex.hpp"

#include "common/address_helper.h"

#include <arpa/inet.h>

// decode strings like "tcp://<host>:<port>"
int entity_addr_from_url(entity_addr_t *addr /* out */, const char *url)
{
	using namespace boost;
	using std::endl;

	struct addrinfo hints;
	struct addrinfo *res;

	regex expr("(tcp|rdma)://([^:]*):([\\d]+)");
	cmatch m;

	if (regex_match(url, m, expr)) {
		int error;
		string host(m[2].first, m[2].second);
		string port(m[3].first, m[3].second);
		memset(&hints, 0, sizeof(hints));
		hints.ai_family = PF_UNSPEC;
		error = getaddrinfo(host.c_str(), NULL, &hints, &res);
		if (! error) {
			addr->set_sockaddr((sockaddr*)res->ai_addr);
			addr->set_port(std::atoi(port.c_str()));
			freeaddrinfo(res);
			return 0;
		}
	}

	return 1;
}

int entity_addr_from_sockaddr(entity_addr_t *addr /* out */,
			      const struct sockaddr *sa)
{
    struct sockaddr_in *sin;
    struct sockaddr_in6 *sin6;

    if (! sa)
	return 0;

    addr->addr.ss_family = sa->sa_family;
    switch(sa->sa_family) {
    case AF_INET:
	sin = (struct sockaddr_in *) sa;
	memcpy(&addr->addr4.sin_addr, &sin->sin_addr,
	       sizeof(sin->sin_addr));
	addr->addr4.sin_port = sin->sin_port;
	break;
    case AF_INET6:
	sin6 = (struct sockaddr_in6 *) sa;
	memcpy(&addr->addr6.sin6_addr, &sin6->sin6_addr,
	       sizeof(sin6->sin6_addr));
	addr->addr6.sin6_port = sin6->sin6_port;
	break;
    default:
	break;
    };

    return 1;
}


