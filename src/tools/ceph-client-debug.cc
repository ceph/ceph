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


#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "client/Inode.h"
#include "client/Dentry.h"
#include "client/Dir.h"
#include "include/cephfs/libcephfs.h"

#define dout_subsys ceph_subsys_client

void usage()
{
  std::cout << "Usage: ceph-client-debug [options] <inode number>" << std::endl;
  generic_client_usage();
}


/**
 * Given an inode, look up the path from the Client cache: assumes
 * client cache is fully populated.
 */
void traverse_dentries(Inode *ino, std::vector<Dentry*> &parts)
{
  if (ino->dn_set.empty()) {
    return;
  }
  
  Dentry* dn = *(ino->dn_set.begin());
  parts.push_back(dn);
  traverse_dentries(dn->dir->parent_inode, parts);
}


/**
 * Given an inode, send lookup requests to the MDS for
 * all its ancestors, such that the full trace will be
 * populated in client cache.
 */
int lookup_trace(ceph_mount_info *client, inodeno_t const ino)
{
  Inode *inode;
  int r = ceph_ll_lookup_inode(client, ino, &inode);
  if (r != 0) {
    return r;
  } else {
    if (!inode->dn_set.empty()) {
      Dentry *dn = *(inode->dn_set.begin());
      assert(dn->dir);
      assert(dn->dir->parent_inode);
      r = lookup_trace(client, dn->dir->parent_inode->ino);
      if (r) {
        return r;
      }
    } else {
      // We reached the root of the tree
      assert(inode->ino == CEPH_INO_ROOT);
    }
  }

  return r;
}


int main(int argc, const char **argv)
{
  // Argument handling
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  
  common_init_finish(g_ceph_context);

  // Expect exactly one positional argument (inode number)
  if (args.size() != 1) {
    usage();
  }
  char const *inode_str = args[0];
  inodeno_t inode = strtoll(inode_str, NULL, 0);
  if (inode <= 0) {
    derr << "Invalid inode: " << inode_str << dendl;
    return -1;
  }

  // Initialize filesystem client
  struct ceph_mount_info *client;
  int r = ceph_create_with_context(&client, g_ceph_context);
  if (r) {
    derr << "Error initializing libcephfs: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = ceph_mount(client, "/");
  if (r) {
    derr << "Error mounting: " << cpp_strerror(r) << dendl;
    ceph_shutdown(client);
    return r;
  }


  // Populate client cache with inode of interest & ancestors
  r = lookup_trace(client, inode);
  if (r) {
    derr << "Error looking up inode " << std::hex << inode << std::dec <<
      ": " << cpp_strerror(r) << dendl;
    return -1;
  }

  // Retrieve inode of interest
  struct vinodeno_t vinode;
  vinode.ino = inode;
  vinode.snapid = CEPH_NOSNAP;
  Inode *ino = ceph_ll_get_inode(client, vinode);

  // Retrieve dentry trace
  std::vector<Dentry*> path;
  traverse_dentries(ino, path);
  
  // Print inode and path as a JSON object
  JSONFormatter jf(true);
  jf.open_object_section("client_debug");
  {
    jf.open_object_section("inode");
    {
      ino->dump(&jf);
    }
    jf.close_section(); // inode
    jf.open_array_section("path");
    {
      for (std::vector<Dentry*>::reverse_iterator p = path.rbegin(); p != path.rend(); ++p) {
        jf.open_object_section("dentry");
        {
          (*p)->dump(&jf);
        }
        jf.close_section(); // dentry
      }
    }
    jf.close_section(); // path
  }
  jf.close_section(); // client_debug
  jf.flush(std::cout);
  std::cout << std::endl;

  // Release Inode references
  ceph_ll_forget(client, ino, 1);
  for (std::vector<Dentry*>::reverse_iterator p = path.rbegin(); p != path.rend(); ++p) {
    ceph_ll_forget(client, (*p)->inode.get(), 1);
  }
  ino = NULL;
  path.clear();  

  // Shut down
  r = ceph_unmount(client);
  if (r) {
    derr << "Error mounting: " << cpp_strerror(r) << dendl;
  }
  ceph_shutdown(client);
  
  return r;
}
