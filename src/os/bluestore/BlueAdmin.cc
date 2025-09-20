// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BlueAdmin.h"
#include "Compression.h"
#include "common/pretty_binary.h"
#include "common/debug.h"
#include <asm-generic/errno-base.h>
#include <vector>
#include <limits>

#define dout_subsys ceph_subsys_bluestore
#define dout_context store.cct

using ceph::bufferlist;
using ceph::Formatter;
using ceph::common::cmd_getval;

BlueStore::SocketHook::SocketHook(BlueStore& store)
  : store(store)
{
  AdminSocket *admin_socket = store.cct->get_admin_socket();
  if (admin_socket) {
    int r = admin_socket->register_command(
      "bluestore collections",
      this,
      "list all collections");
    if (r != 0) {
      dout(1) << __func__ << " cannot register SocketHook" << dendl;
      return;
    }
    r = admin_socket->register_command(
      "bluestore list "
      "name=collection,type=CephString,req=true "
      "name=start,type=CephString,req=false "
      "name=max,type=CephInt,req=false",
      this,
      "list objects in specific collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore onode metadata "
      "name=object_name,type=CephString,req=true",
      this,
      "print object internals");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore compression stats "
      "name=collection,type=CephString,req=false",
      this,
      "print compression stats, per collection");
    ceph_assert(r == 0);
  }
}

BlueStore::SocketHook::~SocketHook()
{
  AdminSocket *admin_socket = store.cct->get_admin_socket();
  if (admin_socket) {
    admin_socket->unregister_commands(this);
  }
}

int BlueStore::SocketHook::call(
  std::string_view command,
  const cmdmap_t& cmdmap,
  const bufferlist& inbl,
  Formatter *f,
  std::ostream& ss,
  bufferlist& out)
{
  int r = 0;
  if (command == "bluestore collections") {
    std::vector<coll_t> collections;
    store.list_collections(collections);
    std::stringstream result;
    for (const auto& c : collections) {
      result << c << std::endl;
    }
    out.append(result.str());
    return 0;
  } else if (command == "bluestore list") {
    std::string coll;
    std::string start;
    int64_t max;
    cmd_getval(cmdmap, "collection", coll);
    cmd_getval(cmdmap, "start", start);
    if (!cmd_getval(cmdmap, "max", max)) {
      max = 100;
    }
    if (max == 0) {
      max = std::numeric_limits<int>::max();
    }
    coll_t c;
    if (c.parse(coll) == false) {
      ss << "Cannot parse collection" << std::endl;
      return -EINVAL;
    }
    BlueStore::CollectionRef col = store._get_collection(c);
    if (!col) {
      ss << "No such collection" << std::endl;
      return -ENOENT;
    }
    ghobject_t start_object;
    if (start.length() > 0) {
      if (start_object.parse(start) == false) {
        ss << "Cannot parse start object";
	return -EINVAL;
      }
    }
    std::vector<ghobject_t> list;
    {
      std::shared_lock l(col->lock);
      r = store._collection_list(col.get(), start_object, ghobject_t::get_max(),
        max, false, &list, nullptr);
    }
    if (r != 0) {
      return 0;
    }
    std::stringstream result;
    for (auto& obj : list) {
      result << obj << std::endl;
    }
    out.append(result.str());
    return 0;
  } else if (command == "bluestore onode metadata") {
    std::string object_name;
    cmd_getval(cmdmap, "object_name", object_name);
    ghobject_t object;
    if (!object.parse(object_name)) {
      ss << "Cannot parse object" << std::endl;
      return -EINVAL;
    }
    std::shared_lock l(store.coll_lock);
    for (const auto& cp : store.coll_map) {
      if (cp.second->contains(object)) {
        std::shared_lock l(cp.second->lock);
        OnodeRef o = cp.second->get_onode(object, false);
        if (!o || !o->exists) {
          ss << "Object not found" << std::endl;
          return -ENOENT;
        }
        o->extent_map.fault_range(store.db, 0, 0xffffffff);
        using P = BlueStore::printer;
        std::stringstream result;
        result << o->print(P::PTR + P::DISK + P::USE + P::BUF + P::CHK + P::ATTRS) << std::endl;
        out.append(result.str());
        return 0;
      }
    }
    r = -ENOENT;
    ss << "No collection that can hold such object" << std::endl;
  } else if (command == "bluestore compression stats") {
    std::vector<CollectionRef> copied;
    {
      std::shared_lock l(store.coll_lock);
      copied.reserve(store.coll_map.size());
      for (const auto& c : store.coll_map) {
        copied.push_back(c.second);
      }
    }
    std::string coll;
    cmd_getval(cmdmap, "collection", coll);
    f->open_array_section("compression");
    for (const auto& c : copied) {
      std::shared_lock l(c->lock);
      if ((coll.empty() && bool(c->estimator))
        || coll == c->get_cid().c_str()) {
        f->open_object_section("collection");
        f->dump_string("cid", c->get_cid().c_str());
        f->open_object_section("estimator");
        if (c->estimator) {
          c->estimator->dump(f);
        }
        f->close_section();
        f->close_section();
      }
    }
    f->close_section();
    return 0;
  } else {
    ss << "Invalid command" << std::endl;
    r = -ENOSYS;
  }
  return r;
}
