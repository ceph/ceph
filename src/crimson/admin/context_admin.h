// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once

#include <memory>

class ContextConfigAdminImp;
class CephContext;

/*!
  \brief implementation of the configuration-related 'admin_socket' API of
         (Crimson) Ceph Context

  Main functionality:
  - manipulating Context-level configuraion
  - process-wide commands ('abort', 'assert')
  - ...
 */
class ContextConfigAdmin {
  std::unique_ptr<ContextConfigAdminImp> m_imp;
  CephContext* m_cct; //!< holding on to the owning CCT until our imp object is destructed
public:
  ContextConfigAdmin(CephContext* cct, ceph::common::ConfigProxy& conf);
  ~ContextConfigAdmin();
  /*!
    Note: the only reason of having register_admin_commands() provided as a public interface (and not just called
    from the ctor), is the (not just theoretical) race to register and unregister the same server block when creating
    a Context and immediately removing it
  */
  seastar::future<> register_admin_commands();
  seastar::future<> unregister_admin_commands();
};
