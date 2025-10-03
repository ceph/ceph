// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLIENT_INODEREF_H
#define CEPH_CLIENT_INODEREF_H

#include <boost/intrusive_ptr.hpp>
class Inode;
void intrusive_ptr_add_ref(Inode *in);
void intrusive_ptr_release(Inode *in);
typedef boost::intrusive_ptr<Inode> InodeRef;
#endif
