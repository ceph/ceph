
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

Fh::Fh(Client *client, InodeRef in, int flags, int cmode, uint64_t _gen,
       const UserPerm &perms) :
    client(client), inode(in), mode(cmode), gen(_gen), flags(flags),
    actor_perms(perms), readahead()
{
  inode->add_fh(this);
}

Fh::~Fh()
{
  inode->rm_fh(this);
}

