// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "MDSUtility.h"
#include "RoleSelector.h"

#include "include/rados/librados.hpp"

/**
 * Command line tool for debugging the backing store of
 * MDSTable instances.
 */
class TableTool : public MDSUtility
{
  private:
    MDSRoleSelector role_selector;

    // I/O handles
    librados::Rados rados;
    librados::IoCtx io;

    int apply_role_fn(std::function<int(mds_role_t, Formatter *)> fptr, Formatter *f);

  public:
    void usage();
    int main(std::vector<const char*> &argv);

};

