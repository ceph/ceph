// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



/* ceph_fuse_main
 * - start up fuse glue, attached to Client* cl.
 * - argc, argv should include a mount point, and 
 *   any weird fuse options you want.  by default,
 *   we will put fuse in the foreground so that it
 *   won't fork and we can see stdout.
 */
int ceph_fuse_main(Client *cl, int argc, const char *argv[]);
