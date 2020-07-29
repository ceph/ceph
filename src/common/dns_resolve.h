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
#ifndef CEPH_DNS_RESOLVE_H
#define CEPH_DNS_RESOLVE_H

#include <netinet/in.h>
#include <resolv.h>

#include "common/ceph_mutex.h"
#include "msg/msg_types.h"		// for entity_addr_t

namespace ceph {

/**
 * this class is used to facilitate the testing of
 * resolv.h functions.
 */
class ResolvHWrapper {
  public:
    virtual ~ResolvHWrapper() {}

#ifdef HAVE_RES_NQUERY
    virtual int res_nquery(res_state s, const char *hostname, int cls, int type, 
        u_char *buf, int bufsz);

    virtual int res_nsearch(res_state s, const char *hostname, int cls, int type, 
        u_char *buf, int bufsz);
#else
    virtual int res_query(const char *hostname, int cls, int type,
        u_char *buf, int bufsz);

    virtual int res_search(const char *hostname, int cls, int type,
        u_char *buf, int bufsz);
#endif

};


/**
 * @class DNSResolver
 *
 * This is a singleton class that exposes the functionality of DNS querying.
 */
class DNSResolver {

  public:
    // singleton declaration
    static DNSResolver *get_instance()
    {
      static DNSResolver instance;
      return &instance;
    }
    DNSResolver(DNSResolver const&) = delete;
    void operator=(DNSResolver const&) = delete;

    // this function is used by the unit test
    static DNSResolver *get_instance(ResolvHWrapper *resolv_wrapper) {
      DNSResolver *resolv = DNSResolver::get_instance();
      delete resolv->resolv_h;
      resolv->resolv_h = resolv_wrapper;
      return resolv;
    }

    enum class SRV_Protocol {
      TCP, UDP
    };


    struct Record {
      uint16_t priority;
      uint16_t weight;
      entity_addr_t addr;
    };

    int resolve_cname(CephContext *cct, const std::string& hostname,
        std::string *cname, bool *found);

    /**
     * Resolves the address given a hostname.
     *
     * @param hostname the hostname to resolved
     * @param[out] addr the hostname's address
     * @returns 0 on success, negative error code on failure
     */
    int resolve_ip_addr(CephContext *cct, const std::string& hostname,
        entity_addr_t *addr);

    /**
     * Returns the list of hostnames and addresses that provide a given
     * service configured as DNS SRV records.
     *
     * @param service_name the service name
     * @param trans_protocol the IP protocol used by the service (TCP or UDP)
     * @param[out] srv_hosts the hostname to address map of available hosts
     *             providing the service. If no host exists the map is not
     *             changed.
     * @returns 0 on success, negative error code on failure
     */
    int resolve_srv_hosts(CephContext *cct, const std::string& service_name,
        const SRV_Protocol trans_protocol, std::map<std::string, Record> *srv_hosts);

    /**
     * Returns the list of hostnames and addresses that provide a given
     * service configured as DNS SRV records.
     *
     * @param service_name the service name
     * @param trans_protocol the IP protocol used by the service (TCP or UDP)
     * @param domain the domain of the service
     * @param[out] srv_hosts the hostname to address map of available hosts
     *             providing the service. If no host exists the map is not
     *             changed.
     * @returns 0 on success, negative error code on failure
     */
    int resolve_srv_hosts(CephContext *cct, const std::string& service_name,
        const SRV_Protocol trans_protocol, const std::string& domain,
        std::map<std::string, Record> *srv_hosts);

  private:
    DNSResolver() { resolv_h = new ResolvHWrapper(); }
    ~DNSResolver();

    ceph::mutex lock = ceph::make_mutex("DNSResolver::lock");
    ResolvHWrapper *resolv_h;
#ifdef HAVE_RES_NQUERY
    std::list<res_state> states;

    int get_state(CephContext *cct, res_state *ps);
    void put_state(res_state s);
#endif

#ifndef _WIN32
    /* this private function allows to reuse the res_state structure used
     * by other function of this class
     */
    int resolve_ip_addr(CephContext *cct, res_state *res,
        const std::string& hostname, entity_addr_t *addr);
#endif

    std::string srv_protocol_to_str(SRV_Protocol proto) {
      switch (proto) {
        case SRV_Protocol::TCP:
          return "tcp";
        case SRV_Protocol::UDP:
          return "udp";
      }
      return "";
    }

};

}

#endif

