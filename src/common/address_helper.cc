// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * address_helper.cc
 *
 *  Created on: Oct 27, 2013
 *      Author: matt
 */

#include <netdb.h>
#include <regex>

#include "common/address_helper.h"

// decode strings like "tcp://<host>:<port>"
int entity_addr_from_url(entity_addr_t *addr /* out */, const char *url)
{
	std::regex expr("(tcp|rdma)://([^:]*):([\\d]+)");
	std::cmatch m;

	if (std::regex_match(url, m, expr)) {
		string host(m[2].first, m[2].second);
		string port(m[3].first, m[3].second);
		addrinfo hints;
		memset(&hints, 0, sizeof(hints));
		hints.ai_family = PF_UNSPEC;
		addrinfo *res;
		if (!getaddrinfo(host.c_str(), nullptr, &hints, &res)) {
			addr->set_sockaddr((sockaddr*)res->ai_addr);
			addr->set_port(std::atoi(port.c_str()));
			freeaddrinfo(res);
			return 0;
		}
	}

	return 1;
}

