// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "log_node.h"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::log_manager{

void delta_t::replay(LogKVNodeLayout &l) {
  if (op == op_t::APPEND) {
    l._append(key, val);
    return;
  } else if (op == op_t::ADD_PREV) {
    l.set_prev_node(prev);
  } else if (op == op_t::INIT) {
    l.set_last_pos(0); 
    l.set_size(0);
    l.set_prev_node(L_ADDR_NULL);
    l.set_reserved_len(0);
    l.set_reserved_size(0);
    l.init_bitmap();
  } else if (op == op_t::REMOVE) {
    d_bitmap_t bitmap;
    auto biter = val.cbegin();
    ceph::decode(bitmap, biter);
    l._set_d_bitmap(bitmap);
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

void LogNode::append_remove(ceph::bufferlist bl) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append_remove(p, bl);
    return;
  }
  d_bitmap_t bitmap;
  auto biter = bl.cbegin();
  decode(bitmap, biter);
  _set_d_bitmap(bitmap);
}

bool LogNode::is_removable() {
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_d_bitmap();
    if (ret) {
      d_bitmap_t bitmap;
      auto biter = (*ret).cbegin();
      decode(bitmap, biter);
      return bitmap.is_all_set(get_size() + get_reserved_size());
    }
  }
  auto bitmap = get_d_bitmap();
  return bitmap.is_all_set(get_size());
}

void LogNode::set_cur_bitmap(uint32_t begin, uint32_t end) {
  d_bitmap_t bitmap;
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_d_bitmap();
    if (ret) {
      auto biter = (*ret).cbegin();
      decode(bitmap, biter);
    }
  } else {
    bitmap = get_d_bitmap();
  }
  bitmap.set_bitmap_range(begin, end);
  bufferlist bl;
  encode(bitmap, bl);
  append_remove(bl);
}

template <typename F>
void LogNode::for_each_live_entry(F&& fn) {
  d_bitmap_t bitmap;
  if (auto p = maybe_get_delta_buffer()) {
    if (auto ret = p->get_latest_d_bitmap()) {
      auto it = (*ret).cbegin();
      decode(bitmap, it);
    }
  } else {
    bitmap = get_d_bitmap();
  }

  uint32_t index = 0;
  auto iter = iter_begin();
  while (iter != iter_end()) {
    if (!bitmap.is_set(index)) {
      if (fn(*iter, index)) {
	return;
      }
    }
    ++iter;
    ++index;
  }
}

void LogNode::list(const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  std::map<std::string, bufferlist> &kvs) {
  std::string_view s(*first);
  std::string last_str = last ? *last : iter_rbegin()->get_key(); 
  std::string_view e = last_str;
  for_each_live_entry([&](const auto& ent, uint32_t index) -> bool {
    const auto& k = ent.get_key();
    if (ent.get_key() >= s && ent.get_key() <= e) {
      kvs[k] = ent.get_val();
    }
    return false;
  });
}

LogNode::get_value_ret LogNode::get_value(const std::string &key)
{
  bufferlist bl;
  bool found = false;
  for_each_live_entry([&](const auto& ent, uint32_t index) -> bool {
    const auto& k = ent.get_key();
    if (k == key) {
      bl = ent.get_val();
      /* If key is time-series log,
       * duplicate does not exist. In this case, return latest one */
      if (is_log_key(k)) {
	found = true;
	return true;
      }
    }
    return false;
  });
  if (bl.length() > 0 || found) {
    return get_value_ret(
      interruptible::ready_future_marker{},
      std::move(bl));
  }

  return get_value_ret(
    interruptible::ready_future_marker{},
    std::nullopt);
}

bool LogNode::remove_entry(const std::string key)
{
  auto iter = iter_begin();
  uint32_t index = 0;
  while(iter != iter_end()) {
    if (iter->get_key() == key) {
      set_cur_bitmap(index, index);
      /* If key is time-series log,
       * duplicate key does not exist. In this case, return true */
      if (is_log_key(key)) {
	return true;
      }
    }
    index++;
    iter++;
  };
  return false;
}

void LogKVNodeLayout::journal_append_remove(
  delta_buffer_t *recorder, 
  ceph::bufferlist bl) {
  recorder->insert_remove(bl);
}

}
