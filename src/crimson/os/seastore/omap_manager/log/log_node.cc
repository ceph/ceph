// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

#include "crimson/os/seastore/seastore_types.h"
#include "log_node.h"

namespace crimson::os::seastore::log_manager{

void delta_t::replay(LogKVNodeLayout &l) {
  if (op == op_t::INSERT) {
    l._append(key, val);
    return;
  } else if (op == op_t::ADD_PREV) {
    l.set_prev_node(prev);
  } else if (op == op_t::INIT) {
    l.set_last_pos(0); 
    l.set_size(0);
    l.set_prev_node(L_ADDR_NULL);
    l.set_reserved_len(0);
  }
}

void LogNode::append_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append(key, val, p);
    return;
  }
  append(key, val);
}

void LogNode::set_prev_addr(laddr_t l) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append_prev_addr(l, p);
    return;
  }
  set_prev_node(l);
}

void LogNode::set_init_vars() {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append_init(p);
    return;
  }
  init_vars();
}

}
