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

	regex expr("(tcp|rdma)://([^:]*):([\\d]+)");
	cmatch m;

	if (regex_match(url, m, expr)) {
		string host(m[2].first, m[2].second);
		string port(m[3].first, m[3].second);
		addrinfo hints;
		memset(&hints, 0, sizeof(hints));
		hints.ai_family = PF_UNSPEC;
		addrinfo *res;
		int error = getaddrinfo(host.c_str(), NULL, &hints, &res);
		if (! error) {
			addr->set_sockaddr((sockaddr*)res->ai_addr);
			addr->set_port(std::atoi(port.c_str()));
			freeaddrinfo(res);
			return 0;
		}
	}

	return 1;
}

