// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 New Dream Network
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <boost/scoped_ptr.hpp>
#include "common/debug.h"
#include "os/FileStore.h"
#include "common/config.h"

#include "FileStoreDiff.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "filestore_diff "

FileStoreDiff::FileStoreDiff(FileStore *a, FileStore *b)
    : a_store(a), b_store(b)
{
  int err;
  err = a_store->mount();
  ceph_assert(err == 0);

  err = b_store->mount();
  ceph_assert(err == 0);
}

FileStoreDiff::~FileStoreDiff()
{
  a_store->umount();
  b_store->umount();
}


bool FileStoreDiff::diff_attrs(std::map<std::string,bufferptr>& b,
    std::map<std::string,bufferptr>& a)
{
  std::map<std::string, bufferptr>::iterator b_it = b.begin();
  std::map<std::string, bufferptr>::iterator a_it = a.begin();
  for (; b_it != b.end(); ++b_it, ++a_it) {
    if (b_it->first != a_it->first) {
      dout(0) << "diff_attrs name mismatch (verify: " << b_it->first
          << ", store: " << a_it->first << ")" << dendl;
      return false;
    }

    if (!b_it->second.cmp(a_it->second)) {
      dout(0) << "diff_attrs contents mismatch on attr " << b_it->first << dendl;
      return false;
    }
  }
  return true;
}

bool FileStoreDiff::diff_objects_stat(struct stat& a, struct stat& b)
{
  if (a.st_uid != b.st_uid) {
    dout(0) << "diff_objects_stat uid mismatch (A: "
        << a.st_uid << " != B: " << b.st_uid << ")" << dendl;
    return false;
  }

  if (a.st_gid != b.st_gid) {
    dout(0) << "diff_objects_stat gid mismatch (A: "
        << a.st_gid << " != B: " << b.st_gid << ")" << dendl;
    return false;
  }

  if (a.st_mode != b.st_mode) {
    dout(0) << "diff_objects_stat mode mismatch (A: "
        << a.st_mode << " != B: " << b.st_mode << ")" << dendl;
    return false;
  }

  if (a.st_nlink != b.st_nlink) {
    dout(0) << "diff_objects_stat nlink mismatch (A: "
        << a.st_nlink << " != B: " << b.st_nlink << ")" << dendl;
    return false;
  }

  if (a.st_size != b.st_size) {
    dout(0) << "diff_objects_stat size mismatch (A: "
        << a.st_size << " != B: " << b.st_size << ")" << dendl;
    return false;
  }
  return true;
}

bool FileStoreDiff::diff_objects(FileStore *a_store, FileStore *b_store, coll_t coll)
{
  dout(0) << __func__ << " coll "  << coll << dendl;

  int err;
  std::vector<hobject_t> b_objects, a_objects;
  err = b_store->collection_list(coll, b_objects);
  if (err < 0) {
    dout(0) << "diff_objects list on verify coll " << coll.to_str()
        << " returns " << err << dendl;
    return false;
  }
  err = a_store->collection_list(coll, a_objects);
  if (err < 0) {
    dout(0) << "diff_objects list on store coll " << coll.to_str()
              << " returns " << err << dendl;
    return false;
  }

  if (b_objects.size() != a_objects.size()) {
    dout(0) << "diff_objects num objs mismatch (A: " << a_objects.size()
        << ", B: " << b_objects.size() << ")" << dendl;
    return false;
  }

  std::vector<hobject_t>::iterator b_it = b_objects.begin();
  std::vector<hobject_t>::iterator a_it = b_objects.begin();
  for (; b_it != b_objects.end(); ++b_it, ++a_it) {
    hobject_t b_obj = *b_it, a_obj = *a_it;
    if (b_obj.oid.name != a_obj.oid.name) {
      dout(0) << "diff_objects name mismatch on A object "
          << coll << "/" << a_obj << " and B object "
          << coll << "/" << b_obj << dendl;
      return false;
    }

    struct stat b_stat, a_stat;
    err = b_store->stat(coll, b_obj, &b_stat);
    if (err < 0) {
      dout(0) << "diff_objects error stating B object "
          << coll.to_str() << "/" << b_obj.oid.name << dendl;
      return false;
    }
    err = a_store->stat(coll, a_obj, &a_stat);
    if (err < 0) {
      dout(0) << "diff_objects error stating A object "
          << coll << "/" << a_obj << dendl;
      return false;
    }

    if (!diff_objects_stat(a_stat, b_stat)) {
      dout(0) << "diff_objects stat mismatch on "
          << coll << "/" << b_obj << dendl;
      return false;
    }

    bufferlist a_obj_bl, b_obj_bl;
    b_store->read(coll, b_obj, 0, b_stat.st_size, b_obj_bl);
    a_store->read(coll, a_obj, 0, a_stat.st_size, a_obj_bl);

    if (!a_obj_bl.contents_equal(b_obj_bl)) {
      dout(0) << "diff_objects content mismatch on "
          << coll << "/" << b_obj << dendl;
      return false;
    }

    std::map<std::string, bufferptr> a_obj_attrs_map, b_obj_attrs_map;
    err = a_store->getattrs(coll, a_obj, a_obj_attrs_map);
    if (err < 0) {
      dout(0) << "diff_objects getattrs on A object " << coll << "/" << a_obj
              << " returns " << err << dendl;
      return false;
    }
    err = b_store->getattrs(coll, b_obj, b_obj_attrs_map);
    if (err < 0) {
      dout(0) << "diff_objects getattrs on B object " << coll << "/" << b_obj
              << "returns " << err << dendl;
      return false;
    }

    if (!diff_attrs(b_obj_attrs_map, a_obj_attrs_map)) {
      dout(0) << "diff_objects attrs mismatch on A object "
          << coll << "/" << a_obj << " and B object "
          << coll << "/" << b_obj << dendl;
      return false;
    }
  }

  return true;
}

bool FileStoreDiff::diff_coll_attrs(FileStore *a_store, FileStore *b_store, coll_t coll)
{
  int err;
  std::map<std::string, bufferptr> b_coll_attrs, a_coll_attrs;
  err = b_store->collection_getattrs(coll, b_coll_attrs);
  if (err < 0) {
    dout(0) << "diff_attrs getattrs on verify coll " << coll.to_str()
        << "returns " << err << dendl;
    return false;
  }
  err = a_store->collection_getattrs(coll, a_coll_attrs);
  if (err < 0) {
    dout(0) << "diff_attrs getattrs on A coll " << coll.to_str()
              << "returns " << err << dendl;
    return false;
  }

  if (b_coll_attrs.size() != a_coll_attrs.size()) {
    dout(0) << "diff_attrs size mismatch (A: " << a_coll_attrs.size()
        << ", B: " << a_coll_attrs.size() << ")" << dendl;
    return false;
  }

  return diff_attrs(b_coll_attrs, a_coll_attrs);
}

bool FileStoreDiff::diff()
{
  bool ret = true;
  std::vector<coll_t> a_coll_list, b_coll_list;;
  b_store->list_collections(b_coll_list);

  std::vector<coll_t>::iterator it = b_coll_list.begin();
  for (; it != b_coll_list.end(); ++it) {
    coll_t b_coll = *it;
    if (!a_store->collection_exists(b_coll)) {
      dout(0) << "diff B coll " << b_coll.to_str() << " DNE on A" << dendl;
      return false;
    }

    ret = diff_coll_attrs(a_store, b_store, b_coll);
    if (!ret)
      break;

    ret = diff_objects(a_store, b_store, b_coll);
    if (!ret)
      break;
  }
  return ret;
}
