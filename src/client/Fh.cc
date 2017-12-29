
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "Inode.h"

#include "Fh.h"

Fh::Fh(InodeRef in, int flags, int cmode, const UserPerm &perms) :
    inode(in), _ref(1), pos(0), mds(0), mode(cmode), flags(flags), pos_locked(false),
    actor_perms(perms), readahead()
{
  inode->add_fh(this);
}

Fh::~Fh()
{
  inode->rm_fh(this);
}

