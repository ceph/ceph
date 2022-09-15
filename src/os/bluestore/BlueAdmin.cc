// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BlueAdmin.h"
#include "common/pretty_binary.h"
//#include "BlueStore.h"
#include "common/debug.h"
//#include "common/admin_socket.h"
#include <vector>

#define dout_subsys ceph_subsys_bluestore
#define dout_context store.cct

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;
using ceph::common::cmd_getval;

template<typename S>
void get_object_key(CephContext *cct, const ghobject_t& oid, S *key);


BlueStore::SocketHook::SocketHook(BlueStore& store)
    : store(store)
{
  AdminSocket *admin_socket = store.cct->get_admin_socket();
  if (admin_socket) {
    int r = admin_socket->register_command(
      "bluestore colls",
      this,
      "list all collections");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore list "
      "name=coll_name,type=CephString,req=true "
      "name=start,type=CephString,req=false",
      this,
      "list objects in specific collection");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore print "
      "name=object_name,type=CephString,req=true",
      this,
      "print object internals");
    ceph_assert(r == 0);
    r = admin_socket->register_command(
      "bluestore blob "
      "name=blob_id,type=CephInt,req=true",
      this,
      "print blob");
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
  Formatter *f,
  std::ostream& ss,
  bufferlist& out)
{
  int r = 0;
  if (command == "bluestore colls") {
    std::vector<coll_t> ls;
    store.list_collections(ls);
    std::stringstream s;
    for (auto& x : ls) {
      s << x << std::endl;
    }
    out.append(s.str());
    return 0;
  } else if (command == "bluestore list") {
    std::string coll_name;
    std::string start;
    cmd_getval(cmdmap, "coll_name", coll_name);
    cmd_getval(cmdmap, "start", start);
    coll_t c;
    if (c.parse(coll_name) == false) {
      return -EINVAL;
    }
    BlueStore::CollectionRef col = store._get_collection(c);
    if (!col) {
      return -ENOENT;
    }
    ghobject_t t;
    if (start.length() > 0) {
      if (t.parse(start) == false) {
	return -EINVAL;
      }
    }
    std::vector<ghobject_t> ls;
    ghobject_t pnext;
    {
      std::shared_lock l(col->lock);
      r = store._collection_list(col.get(), t, ghobject_t::get_max(), 30, false, &ls, &pnext);
    }
    std::stringstream s;
    for (auto& x : ls) {
      ghobject_t tttt;
      tttt.hobj.oid = x.hobj.oid;
      tttt.hobj.pool = x.hobj.pool;
      s << x << "<>" << x.hobj << "<>" << x.hobj.oid << "<>"
	<< std::hex << std::hash<hobject_t>()(tttt.hobj) << std::dec << std::endl;
    }
    out.append(s.str());
    return r;
  } else if (command == "bluestore print") {
    std::string object_name;
    cmd_getval(cmdmap, "object_name", object_name);
    ghobject_t object;
    if (!object.parse(object_name)) {
      return -ENOENT;
    }
    std::string key;
    get_object_key(store.cct, object, &key);
    dout(1) << __func__ << " key=" << pretty_binary_string(key) << dendl;
    std::string str;
    store.print_onode(key, &str);
    out.append(str);
  } else if (command == "bluestore blob") {
    int64_t blob_id;
    cmd_getval(cmdmap, "blob_id", blob_id);
    std::string str;
    r = store.print_shared_blob(blob_id, &str);
    if (r != 0) {
      return -ENOENT;
    }
    out.append(str);
  } else {
    ss << "Invalid command" << std::endl;
    r = -ENOSYS;
  }
  return r;
}
