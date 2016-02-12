// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "dns_resolve.h"

#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <arpa/inet.h>
#include <resolv.h>

#include "acconfig.h"
#include "common/debug.h"
#include "msg/msg_types.h"


#define dout_subsys ceph_subsys_

using namespace std;

namespace ceph {

#ifdef HAVE_RES_NQUERY

int ResolvHWrapper::res_nquery(res_state s, const char *hostname, int cls,
    int type, u_char *buf, int bufsz) {
  return ::res_nquery(s, hostname, cls, type, buf, bufsz);
}

int ResolvHWrapper::res_nsearch(res_state s, const char *hostname, int cls,
    int type, u_char *buf, int bufsz) {
  return ::res_nsearch(s, hostname, cls, type, buf, bufsz);
}

#else

int ResolvHWrapper::res_query(const char *hostname, int cls,
    int type, u_char *buf, int bufsz) {
  return ::res_query(hostname, cls, type, buf, bufsz);
}

int ResolvHWrapper::res_search(const char *hostname, int cls,
    int type, u_char *buf, int bufsz) {
  return ::res_search(hostname, cls, type, buf, bufsz);
}

#endif

DNSResolver::~DNSResolver()
{
#ifdef HAVE_RES_NQUERY
  list<res_state>::iterator iter;
  for (iter = states.begin(); iter != states.end(); ++iter) {
    struct __res_state *s = *iter;
    delete s;
  }
#endif
  delete resolv_h;
}

#ifdef HAVE_RES_NQUERY
int DNSResolver::get_state(CephContext *cct, res_state *ps)
{
  lock.Lock();
  if (!states.empty()) {
    res_state s = states.front();
    states.pop_front();
    lock.Unlock();
    *ps = s;
    return 0;
  }
  lock.Unlock();
  struct __res_state *s = new struct __res_state;
  s->options = 0;
  if (res_ninit(s) < 0) {
    delete s;
    lderr(cct) << "ERROR: failed to call res_ninit()" << dendl;
    return -EINVAL;
  }
  *ps = s;
  return 0;
}

void DNSResolver::put_state(res_state s)
{
  Mutex::Locker l(lock);
  states.push_back(s);
}
#endif

int DNSResolver::resolve_cname(CephContext *cct, const string& hostname,
    string *cname, bool *found)
{
  *found = false;

#ifdef HAVE_RES_NQUERY
  res_state res;
  int r = get_state(cct, &res);
  if (r < 0) {
    return r;
  }
#endif

  int ret;

#define LARGE_ENOUGH_DNS_BUFSIZE 1024
  unsigned char buf[LARGE_ENOUGH_DNS_BUFSIZE];

#define MAX_FQDN_SIZE 255
  char host[MAX_FQDN_SIZE + 1];
  const char *origname = hostname.c_str();
  unsigned char *pt, *answer;
  unsigned char *answend;
  int len;

#ifdef HAVE_RES_NQUERY
  len = resolv_h->res_nquery(res, origname, ns_c_in, ns_t_cname, buf, sizeof(buf));
#else
  {
# ifndef HAVE_THREAD_SAFE_RES_QUERY
    Mutex::Locker l(lock);
# endif
    len = resolv_h->res_query(origname, ns_c_in, ns_t_cname, buf, sizeof(buf));
  }
#endif
  if (len < 0) {
    lderr(cct) << "res_query() failed" << dendl;
    ret = 0;
    goto done;
  }

  answer = buf;
  pt = answer + NS_HFIXEDSZ;
  answend = answer + len;

  /* read query */
  if ((len = dn_expand(answer, answend, pt, host, sizeof(host))) < 0) {
    lderr(cct) << "ERROR: dn_expand() failed" << dendl;
    ret = -EINVAL;
    goto done;
  }
  pt += len;

  if (pt + 4 > answend) {
    lderr(cct) << "ERROR: bad reply" << dendl;
    ret = -EIO;
    goto done;
  }

  int type;
  NS_GET16(type, pt);

  if (type != ns_t_cname) {
    lderr(cct) << "ERROR: failed response type: type=" << type <<
      " (was expecting " << ns_t_cname << ")" << dendl;
    ret = -EIO;
    goto done;
  }

  pt += NS_INT16SZ; /* class */

  /* read answer */
  if ((len = dn_expand(answer, answend, pt, host, sizeof(host))) < 0) {
    ret = 0;
    goto done;
  }
  pt += len;
  ldout(cct, 20) << "name=" << host << dendl;

  if (pt + 10 > answend) {
    lderr(cct) << "ERROR: bad reply" << dendl;
    ret = -EIO;
    goto done;
  }

  NS_GET16(type, pt);
  pt += NS_INT16SZ; /* class */
  pt += NS_INT32SZ; /* ttl */
  pt += NS_INT16SZ; /* size */

  if ((len = dn_expand(answer, answend, pt, host, sizeof(host))) < 0) {
    ret = 0;
    goto done;
  }
  ldout(cct, 20) << "cname host=" << host << dendl;
  *cname = host;

  *found = true;
  ret = 0;
done:
#ifdef HAVE_RES_NQUERY
  put_state(res);
#endif
  return ret;
}


int DNSResolver::resolve_ip_addr(CephContext *cct, const string& hostname,
    entity_addr_t *addr) {

#ifdef HAVE_RES_NQUERY
  res_state res;
  int r = get_state(cct, &res);
  if (r < 0) {
    return r;
  }
  return this->resolve_ip_addr(cct, &res, hostname, addr);
#else
  return this->resolve_ip_addr(cct, NULL, hostname, addr);
#endif

}

int DNSResolver::resolve_ip_addr(CephContext *cct, res_state *res, const string& hostname, 
    entity_addr_t *addr) {

  u_char nsbuf[NS_PACKETSZ];
  int len;


#ifdef HAVE_RES_NQUERY
  len = resolv_h->res_nquery(*res, hostname.c_str(), ns_c_in, ns_t_a, nsbuf, sizeof(nsbuf));
#else
  {
# ifndef HAVE_THREAD_SAFE_RES_QUERY
    Mutex::Locker l(lock);
# endif
    len = resolv_h->res_query(hostname.c_str(), ns_c_in, ns_t_a, nsbuf, sizeof(nsbuf));
  }
#endif
  if (len < 0) {
    lderr(cct) << "res_query() failed" << dendl;
    return len;
  }
  else if (len == 0) {
    ldout(cct, 20) << "no address found for hostname " << hostname << dendl;
    return -1;
  }

  ns_msg handle;
  ns_initparse(nsbuf, len, &handle);

  if (ns_msg_count(handle, ns_s_an) == 0) {
    ldout(cct, 20) << "no address found for hostname " << hostname << dendl;
    return -1;
  }

  ns_rr rr;
  int r;
  if ((r = ns_parserr(&handle, ns_s_an, 0, &rr)) < 0) {
      lderr(cct) << "error while parsing DNS record" << dendl;
      return r;
  }

  char addr_buf[64];
  memset(addr_buf, 0, sizeof(addr_buf));
  inet_ntop(AF_INET, ns_rr_rdata(rr), addr_buf, sizeof(addr_buf));
  if (!addr->parse(addr_buf)) {
      lderr(cct) << "failed to parse address '" << (const char *)ns_rr_rdata(rr) 
        << "'" << dendl;
      return -1;
  }

  return 0;
}

int DNSResolver::resolve_srv_hosts(CephContext *cct, const string& service_name, 
    const SRV_Protocol trans_protocol, map<string, entity_addr_t> *srv_hosts) {
  return this->resolve_srv_hosts(cct, service_name, trans_protocol, "", srv_hosts);
}

int DNSResolver::resolve_srv_hosts(CephContext *cct, const string& service_name, 
    const SRV_Protocol trans_protocol, const string& domain,
    map<string, entity_addr_t> *srv_hosts) {

#ifdef HAVE_RES_NQUERY
  res_state res;
  int r = get_state(cct, &res);
  if (r < 0) {
    return r;
  }
#endif

  int ret;
  u_char nsbuf[NS_PACKETSZ];
  int num_hosts;

  string proto_str = srv_protocol_to_str(trans_protocol);
  string query_str = "_"+service_name+"._"+proto_str+(domain.empty() ? ""
      : "."+domain);
  int len;

#ifdef HAVE_RES_NQUERY
  len = resolv_h->res_nsearch(res, query_str.c_str(), ns_c_in, ns_t_srv, nsbuf,
      sizeof(nsbuf));
#else
  {
# ifndef HAVE_THREAD_SAFE_RES_QUERY
    Mutex::Locker l(lock);
# endif
    len = resolv_h->res_search(query_str.c_str(), ns_c_in, ns_t_srv, nsbuf,
        sizeof(nsbuf));
  }
#endif
  if (len < 0) {
    lderr(cct) << "res_search() failed" << dendl;
    ret = len;
    goto done;
  }
  else if (len == 0) {
    ldout(cct, 20) << "No hosts found for service " << query_str << dendl;
    ret = 0;
    goto done;
  }

  ns_msg handle;

  ns_initparse(nsbuf, len, &handle);

  num_hosts = ns_msg_count (handle, ns_s_an);
  if (num_hosts == 0) {
    ldout(cct, 20) << "No hosts found for service " << query_str << dendl;
    ret = 0;
    goto done;
  }

  ns_rr rr;
  char full_target[NS_MAXDNAME];

  for (int i = 0; i < num_hosts; i++) {
    int r;
    if ((r = ns_parserr(&handle, ns_s_an, i, &rr)) < 0) {
      lderr(cct) << "Error while parsing DNS record" << dendl;
      ret = r;
      goto done;
    }

    string full_srv_name = ns_rr_name(rr);
    string protocol = "_" + proto_str;
    string srv_domain = full_srv_name.substr(full_srv_name.find(protocol)
        + protocol.length());

    const u_char *p = ns_rr_rdata(rr);
    p += NS_INT16SZ; // priority
    p += NS_INT16SZ; // weight

    int port;
    NS_GET16(port, p);

    memset(full_target, 0, sizeof(full_target));
    ns_name_ntop(p, full_target, NS_MAXDNAME);

    entity_addr_t addr;
#ifdef HAVE_RES_NQUERY
    r = this->resolve_ip_addr(cct, &res, full_target, &addr);
#else
    r = this->resolve_ip_addr(cct, NULL, full_target, &addr);
#endif

    if (r == 0) {
      addr.set_port(port);
      string target = full_target;
      assert(target.find(srv_domain) != target.npos);
      target = target.substr(0, target.find(srv_domain));
      (*srv_hosts)[target] = addr;
    }

  }

  ret = 0;
done:
#ifdef HAVE_RES_NQUERY
  put_state(res);
#endif
  return ret;
}

}

