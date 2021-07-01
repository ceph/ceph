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
#include "os/filestore/FileStore.h"
#include "common/config.h"

#include "FileStoreDiff.h"

#define dout_context g_ceph_context
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


bool FileStoreDiff::diff_attrs(std::map<std::string,bufferptr,std::less<>>& b,
    std::map<std::string,bufferptr,std::less<>>& a)
{
  bool ret = false;
  auto b_it = b.begin();
  auto a_it = a.begin();
  for (; b_it != b.end(); ++b_it, ++a_it) {
    if (b_it->first != a_it->first) {
      cout << "diff_attrs name mismatch (verify: " << b_it->first
          << ", store: " << a_it->first << ")" << std::endl;
      ret = true;
      continue;
    }

    if (!b_it->second.cmp(a_it->second)) {
      cout << "diff_attrs contents mismatch on attr " << b_it->first << std::endl;
      ret = true;
      continue;
    }
  }
  return ret;
}

static bool diff_omap(std::map<std::string,bufferlist>& b,
		      std::map<std::string,bufferlist>& a)
{
  bool ret = false;
  std::map<std::string, bufferlist>::iterator b_it = b.begin();
  std::map<std::string, bufferlist>::iterator a_it = a.begin();
  for (; b_it != b.end(); ++b_it, ++a_it) {
    if (a_it == a.end()) {
      cout << __func__ << " a reached end before b, a missing " << b_it->first
	   << std::endl;
      ret = true;
      break;
    }
    if (b_it->first != a_it->first) {
      cout << "diff_attrs name mismatch (verify: " << b_it->first
          << ", store: " << a_it->first << ")" << std::endl;
      ret = true;
      continue;
    }

    if (!(b_it->second == a_it->second)) {
      cout << "diff_attrs contents mismatch on attr " << b_it->first << std::endl;
      ret = true;
      continue;
    }
  }
  return ret;
}

bool FileStoreDiff::diff_objects_stat(struct stat& a, struct stat& b)
{
  bool ret = false;

  if (a.st_uid != b.st_uid) {
    cout << "diff_objects_stat uid mismatch (A: "
        << a.st_uid << " != B: " << b.st_uid << ")" << std::endl;
    ret = true;
  }

  if (a.st_gid != b.st_gid) {
    cout << "diff_objects_stat gid mismatch (A: "
        << a.st_gid << " != B: " << b.st_gid << ")" << std::endl;
    ret = true;
  }

  if (a.st_mode != b.st_mode) {
    cout << "diff_objects_stat mode mismatch (A: "
        << a.st_mode << " != B: " << b.st_mode << ")" << std::endl;
    ret = true;
  }

  if (a.st_nlink != b.st_nlink) {
    cout << "diff_objects_stat nlink mismatch (A: "
        << a.st_nlink << " != B: " << b.st_nlink << ")" << std::endl;
    ret = true;
  }

  if (a.st_size != b.st_size) {
    cout << "diff_objects_stat size mismatch (A: "
        << a.st_size << " != B: " << b.st_size << ")" << std::endl;
    ret = true;
  }
  return ret;
}

bool FileStoreDiff::diff_objects(FileStore *a_store, FileStore *b_store, coll_t coll)
{
  bool ret = false;

  int err;
  std::vector<ghobject_t> b_objects, a_objects;
  err = b_store->collection_list(coll, ghobject_t(), ghobject_t::get_max(),
				 INT_MAX, &b_objects, NULL);
  if (err < 0) {
    cout << "diff_objects list on verify coll " << coll.to_str()
	    << " returns " << err << std::endl;
    return true;
  }
  err = a_store->collection_list(coll, ghobject_t(), ghobject_t::get_max(),
				 INT_MAX, &a_objects, NULL);
  if (err < 0) {
    cout << "diff_objects list on store coll " << coll.to_str()
              << " returns " << err << std::endl;
    return true;
  }

  if (b_objects.size() != a_objects.size()) {
    cout << "diff_objects " << coll << " num objs mismatch (A: " << a_objects.size()
        << ", B: " << b_objects.size() << ")" << std::endl;
    ret = true;
    cout << "a: " << a_objects << std::endl;
    cout << "b: " << b_objects << std::endl;
  }

  auto a_ch = a_store->open_collection(coll);
  auto b_ch = b_store->open_collection(coll);
  std::vector<ghobject_t>::iterator b_it = b_objects.begin();
  std::vector<ghobject_t>::iterator a_it = b_objects.begin();
  for (; b_it != b_objects.end(); ++b_it, ++a_it) {
    ghobject_t b_obj = *b_it, a_obj = *a_it;
    if (b_obj.hobj.oid.name != a_obj.hobj.oid.name) {
      cout << "diff_objects name mismatch on A object "
          << coll << "/" << a_obj << " and B object "
          << coll << "/" << b_obj << std::endl;
      ret = true;
      continue;
    }

    struct stat b_stat, a_stat;
    err = b_store->stat(b_ch, b_obj, &b_stat);
    if (err < 0) {
      cout << "diff_objects error stating B object "
	      << coll.to_str() << "/" << b_obj.hobj.oid.name << std::endl;
      ret = true;
    }
    err = a_store->stat(a_ch, a_obj, &a_stat);
    if (err < 0) {
      cout << "diff_objects error stating A object "
          << coll << "/" << a_obj << std::endl;
      ret = true;
    }

    if (diff_objects_stat(a_stat, b_stat)) {
      cout << "diff_objects stat mismatch on "
          << coll << "/" << b_obj << std::endl;
      ret = true;
    }

    bufferlist a_obj_bl, b_obj_bl;
    b_store->read(b_ch, b_obj, 0, b_stat.st_size, b_obj_bl);
    a_store->read(a_ch, a_obj, 0, a_stat.st_size, a_obj_bl);

    if (!a_obj_bl.contents_equal(b_obj_bl)) {
      cout << "diff_objects content mismatch on "
          << coll << "/" << b_obj << std::endl;
      ret = true;
    }

    std::map<std::string, bufferptr, std::less<>> a_obj_attrs_map, b_obj_attrs_map;
    err = a_store->getattrs(a_ch, a_obj, a_obj_attrs_map);
    if (err < 0) {
      cout << "diff_objects getattrs on A object " << coll << "/" << a_obj
              << " returns " << err << std::endl;
      ret = true;
    }
    err = b_store->getattrs(b_ch, b_obj, b_obj_attrs_map);
    if (err < 0) {
      cout << "diff_objects getattrs on B object " << coll << "/" << b_obj
              << "returns " << err << std::endl;
      ret = true;
    }

    if (diff_attrs(b_obj_attrs_map, a_obj_attrs_map)) {
      cout << "diff_objects attrs mismatch on A object "
          << coll << "/" << a_obj << " and B object "
          << coll << "/" << b_obj << std::endl;
      ret = true;
    }

    std::map<std::string, bufferlist> a_obj_omap, b_obj_omap;
    std::set<std::string> a_omap_keys, b_omap_keys;
    err = a_store->omap_get_keys(a_ch, a_obj, &a_omap_keys);
    if (err < 0) {
      cout << "diff_objects getomap on A object " << coll << "/" << a_obj
              << " returns " << err << std::endl;
      ret = true;
    }
    err = a_store->omap_get_values(a_ch, a_obj, a_omap_keys, &a_obj_omap);
    if (err < 0) {
      cout << "diff_objects getomap on A object " << coll << "/" << a_obj
              << " returns " << err << std::endl;
      ret = true;
    }
    err = b_store->omap_get_keys(b_ch, b_obj, &b_omap_keys);
    if (err < 0) {
      cout << "diff_objects getomap on A object " << coll << "/" << b_obj
              << " returns " << err << std::endl;
      ret = true;
    }
    err = b_store->omap_get_values(b_ch, b_obj, b_omap_keys, &b_obj_omap);
    if (err < 0) {
      cout << "diff_objects getomap on A object " << coll << "/" << b_obj
              << " returns " << err << std::endl;
      ret = true;
    }
    if (diff_omap(a_obj_omap, b_obj_omap)) {
      cout << "diff_objects omap mismatch on A object "
	      << coll << "/" << a_obj << " and B object "
	      << coll << "/" << b_obj << std::endl;
      cout << "a: " << a_obj_omap << std::endl;
      cout << "b: " << b_obj_omap << std::endl;
      ret = true;
    }
  }

  return ret;
}

bool FileStoreDiff::diff()
{
  bool ret = false;

  std::vector<coll_t> a_coll_list, b_coll_list;
  a_store->list_collections(a_coll_list);
  b_store->list_collections(b_coll_list);

  std::vector<coll_t>::iterator it = b_coll_list.begin();
  for (; it != b_coll_list.end(); ++it) {
    coll_t b_coll = *it;
    if (!a_store->collection_exists(b_coll)) {
      cout << "diff B coll " << b_coll.to_str() << " DNE on A" << std::endl;
      ret = true;
      continue;
    }
    for (std::vector<coll_t>::iterator j = a_coll_list.begin();
	 j != a_coll_list.end(); ++j) {
      if (*j == *it) {
	a_coll_list.erase(j);
	break;
      }
    }

    if (diff_objects(a_store, b_store, b_coll))
      ret = true;
  }
  for (std::vector<coll_t>::iterator it = a_coll_list.begin();
       it != a_coll_list.end(); ++it) {
    cout << "diff A coll " << *it << " DNE on B" << std::endl;
    ret = true;
  }

  return ret;
}
