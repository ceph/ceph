// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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


#include "include/rados/librados.hpp"

#include "MDSUtility.h"
#include "ProgressTracker.h"
#include "RoleSelector.h"

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

    std::unique_ptr<ProgressTracker> progress_tracker;

    // set when the monitors reply to our MRemoveSnaps
    bool remove_snaps_acked = false;
    ceph::condition_variable remove_snaps_cond;

    int apply_role_fn(std::function<int(mds_role_t, Formatter *)> fptr, Formatter *f);
    int purge_snaps(const std::string &pool_str, const std::string &snapids_str,
                    bool really, Formatter *f);

  public:
    static void usage();
    int main(std::vector<const char*> &argv);
    bool ms_dispatch(Message *m) override;

    TableTool()
    {
      progress_tracker = std::make_unique<ProgressTracker>("Table operations");
    }
};

