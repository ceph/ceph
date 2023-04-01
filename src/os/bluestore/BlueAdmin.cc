// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "BlueAdmin.h"
#include "common/pretty_binary.h"
#include "common/debug.h"
//#include <vector>

#define dout_subsys ceph_subsys_bluestore
#define dout_context store.cct

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;
using ceph::common::cmd_getval;

BlueStore::SocketHook::SocketHook(BlueStore& store)
    : store(store)
{
  AdminSocket *admin_socket = store.cct->get_admin_socket();
  if (admin_socket) {
    admin_socket->register_command(
      "bluestore enable shared blob reuse",
      this,
      "enables the feature");
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
  const ceph::buffer::list& inbl,
  ceph::Formatter *f,
  std::ostream& ss,
  ceph::buffer::list& out)
{
  int r = 0;
  if (command == "bluestore enable shared blob reuse") {
    if (store.cct->_conf->bluestore_reuse_shared_blobs) {
      ss << "Reuse shared blobs already enabled" << std::endl;
      return 0;
    }
    store.write_meta("reuse_shared_blobs", "1");
    store.reuse_shared_blobs = true;
    store.cct->_conf->bluestore_reuse_shared_blobs = true;
    out.append("reuse_shared_blobs enabled");
    return 0;
  } else {
    ss << "Invalid command" << std::endl;
    r = -ENOSYS;
  }
  return r;
}
