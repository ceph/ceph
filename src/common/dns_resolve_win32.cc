// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/scope_guard.h"
#include "dns_resolve.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_


namespace ceph {

int ResolvHWrapper::res_query(const char *hostname, int cls,
    int type, u_char *buf, int bufsz) {
  return -1;
}

int ResolvHWrapper::res_search(const char *hostname, int cls,
    int type, u_char *buf, int bufsz) {
  return -1;
}

DNSResolver::~DNSResolver()
{
  delete resolv_h;
}

int DNSResolver::resolve_cname(CephContext *cct, const string& hostname,
    string *cname, bool *found)
{
  return -ENOTSUP;
}

int DNSResolver::resolve_ip_addr(CephContext *cct, const string& hostname,
    entity_addr_t *addr)
{
  return -ENOTSUP;
}

int DNSResolver::resolve_srv_hosts(CephContext *cct, const string& service_name,
    const SRV_Protocol trans_protocol,
    map<string, DNSResolver::Record> *srv_hosts)
{
  return this->resolve_srv_hosts(cct, service_name, trans_protocol, "", srv_hosts);
}

int DNSResolver::resolve_srv_hosts(CephContext *cct, const string& service_name,
    const SRV_Protocol trans_protocol, const string& domain,
    map<string, DNSResolver::Record> *srv_hosts)
{
  return -ENOTSUP;
}

}
