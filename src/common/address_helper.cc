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

	regex expr("tcp://([^:]*):([\\d]+)");
	cmatch m;

	if (regex_match(url, m, expr)) {
		int error;
		string host(m[1].first, m[1].second);
		string port(m[2].first, m[2].second);
		memset(&hints, 0, sizeof(hints));
		hints.ai_family = PF_UNSPEC;
		error = getaddrinfo(host.c_str(), NULL, &hints, &res);
		if (! error) {
			struct sockaddr_in *sin;
			struct sockaddr_in6 *sin6;
			addr->addr.ss_family = res->ai_family;
			switch(res->ai_family) {
			case AF_INET:
				sin = (struct sockaddr_in *) res->ai_addr;
				memcpy(&addr->addr4.sin_addr, &sin->sin_addr,
				       sizeof(sin->sin_addr));
				break;
			case AF_INET6:
				sin6 = (struct sockaddr_in6 *) res->ai_addr;
				memcpy(&addr->addr6.sin6_addr, &sin6->sin6_addr,
				       sizeof(sin6->sin6_addr));
				break;
			default:
				break;
			};
			addr->set_port(std::atoi(port.c_str()));
			return 0;
		}
	}

	return 1;
}
