// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * create rgw admin user
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_ADMIN_USER_H
#define RGW_ADMIN_USER_H

#include <string>
#include "common/config.h"

#include <boost/intrusive_ptr.hpp>
#include "rgw_sal.h"

namespace rgw {

  class RGWLibAdmin
  {
    rgw::sal::RGWRadosStore *store;
    boost::intrusive_ptr<CephContext> cct;

  public:
    rgw::sal::RGWRadosStore* get_store()
    {
      return store;
    }

    int init();
    int init(std::vector<const char *>& args);
    int stop();
  };
}

#endif /*RGW_ADMIN_USER_H */
