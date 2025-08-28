// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "log_manager.h"
#include "log_node.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::log_manager{

LogManager::LogManager(
  TransactionManager &tm)
  : tm(tm) {}

LogManager::initialize_omap_ret
LogManager::initialize_omap(Transaction &t, laddr_t hint, omap_type_t omap_type) 
{
  LOG_PREFIX(LogManager::initialize_omap);
  DEBUGT("hint: {}", t, hint);
  auto&& extent = co_await tm.alloc_non_data_extent<LogNode>(
    t, hint, LOG_NODE_BLOCK_SIZE
  ).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    TransactionManager::alloc_extent_iertr::pass_further{}
  );

  omap_root_t omap_root;
  omap_root.update(extent->get_laddr(), 1, hint,
    omap_type_t::LOG);
  t.get_omap_tree_stats().extents_num_delta++;
  co_return std::move(omap_root);
}

LogManager::omap_set_keys_ret
LogManager::omap_set_keys(
  omap_root_t &log_root,
  Transaction &t, std::map<std::string, ceph::bufferlist>&& _kvs) 
{
  LOG_PREFIX(LogManager::omap_set_keys);
  DEBUGT("enter kv size {}", t, _kvs.size());
  assert(log_root.get_type() == omap_type_t::LOG);

  auto kvs = std::move(_kvs);
  auto ext = co_await log_load_extent<LogNode>(
    t, log_root.addr, BEGIN_KEY, END_KEY);
  ceph_assert(ext);
  for (auto &p : kvs) {
    CachedExtentRef node;
    Transaction::get_extent_ret ret;
    // To find mutable extent in the same transaction
    ret = t.get_extent(ext->get_paddr(), &node);
    assert(ret == Transaction::get_extent_ret::PRESENT);
    assert(node);
    LogNodeRef log_node = node->template cast<LogNode>();
    if (!is_log_key(p.first)) {
      // remove duplicate keys first
      co_await remove_kv(t, log_root.addr, p.first, nullptr);
    }
    co_await _log_set_key(log_root, t, log_node, p.first, p.second);
  }
  co_return;
}

LogManager::omap_set_key_ret 
LogManager::omap_set_key(
  omap_root_t &log_root,
  Transaction &t,
  const std::string &key, const ceph::bufferlist &value) 
{
  LOG_PREFIX(LogManager::omap_set_key);
  DEBUGT("enter k={}", t, key);
  assert(log_root.get_type() == omap_type_t::LOG);

  std::map<std::string, ceph::bufferlist> kvs;
  kvs.emplace(key, value);
  co_return co_await omap_set_keys(log_root, t, std::move(kvs));
}

LogManager::omap_set_key_ret
LogManager::_log_set_key(omap_root_t &log_root,
  Transaction &t, LogNodeRef tail,
  const std::string &key, const ceph::bufferlist &value)
{
  LOG_PREFIX(LogManager::_log_set_key);
  DEBUGT("enter key={}", t, key);
  assert(tail);
  if (!tail->expect_overflow(key.size(), value.length())) {
    auto mut = tm.get_mutable_extent(t, tail)->cast<LogNode>();
    mut->append_kv(t, key, value);
    co_return;
  }
  auto extent = co_await tm.alloc_non_data_extent<LogNode>(
    t, log_root.hint, LOG_NODE_BLOCK_SIZE
  ).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    omap_set_key_iertr::pass_further{}
  );
  assert(extent);
  log_root.update(extent->get_laddr(), log_root.depth,
    log_root.hint, log_root.type);
  extent->append_kv(t, key, value);
  extent->set_prev_addr(tail->get_laddr());
  co_return;
}

std::ostream &LogNode::print_detail_l(std::ostream &out) const
{
  laddr_t l = this->get_prev_addr();
  out << ", prev=" << l
      << ", num=" << this->get_size()
      << ", used_space=" << this->use_space()
      << ", capacity=" << this->get_capacity()
      << ", last_pos=" << this->get_last_pos();
  if (has_laddr()) {
    out << ", begin=" << get_begin()
	<< ", end=" << get_end();
  }
  return out;
}

template <typename T>
requires std::is_same_v<LogNode, T>
LogManager::log_load_extent_iertr::future<TCachedExtentRef<T>> 
LogManager::log_load_extent(
  Transaction &t,
  laddr_t laddr,
  std::string begin,
  std::string end)
{
  LOG_PREFIX(LogManager::log_load_extent);
  DEBUGT("laddr={}", t, laddr);
  assert(end <= END_KEY);
  auto size = LOG_NODE_BLOCK_SIZE;
  auto maybe_indirect_extent = co_await tm.read_extent<T>(t, laddr, size,
    [begin=std::move(begin), end=std::move(end)](T &extent) mutable {
      assert(!extent.is_seen_by_users());
      extent.init_range(std::move(begin), std::move(end));
    }
  ).handle_error_interruptible(
    log_load_extent_iertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error in log_load_extent" }
  );

  assert(!maybe_indirect_extent.is_indirect());
  assert(!maybe_indirect_extent.is_clone);
  co_return std::move(maybe_indirect_extent.extent);
}

LogManager::omap_get_value_ret
LogManager::omap_get_value(
  const omap_root_t &log_root, Transaction &t, const std::string &key)
{
  LOG_PREFIX(LogManager::omap_get_value);
  DEBUGT("key={}", t, key);
  assert(log_root.get_type() == omap_type_t::LOG);
  auto ret = co_await find_kv(t, log_root.addr, key);
  co_return ret;
}

LogManager::omap_list_ret
LogManager::omap_list(
  const omap_root_t &log_root,
  Transaction &t,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(LogManager::omap_list);
  DEBUGT("first={}, last={}", t, first, last);
  assert(log_root.get_type() == omap_type_t::LOG);
  std::map<std::string, bufferlist> kvs;
  co_await find_kvs(t, log_root.addr, first, last, kvs);
  auto ret = omap_list_bare_ret(false, {});
  auto &[complete, result] = ret;
  result.insert(kvs.begin(), kvs.end());
  co_return std::move(ret);
}

LogManager::omap_list_iertr::future<>
LogManager::find_kvs(Transaction &t, laddr_t dst,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  std::map<std::string, bufferlist> &kvs)
{
  LOG_PREFIX(LogManager::find_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
  if (dst == L_ADDR_NULL) {
    co_return;
  }
  auto extent = co_await log_load_extent<LogNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return;
  }
  extent->list(first, last, kvs);
  co_await find_kvs(t, extent->get_prev_addr(), first, last, kvs);
  co_return;
}


LogManager::omap_get_value_ret
LogManager::find_kv(Transaction &t, laddr_t dst, const std::string &key)
{
  LOG_PREFIX(LogManager::find_kv);
  DEBUGT("key={}, dst={}", t, key, dst);

  auto extent = co_await log_load_extent<LogNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return std::nullopt;
  }

  auto e = co_await extent->get_value(key);
  if (e == std::nullopt) {
    if(extent->get_prev_addr() == L_ADDR_NULL) {
      co_return std::nullopt;
    }
    auto ret = co_await find_kv(t, extent->get_prev_addr(), key);
    co_return ret;
  }
  co_return std::move(e);
}

LogManager::omap_rm_key_ret
LogManager::remove_node(Transaction &t, LogNodeRef mut, LogNodeRef prev)
{
  LOG_PREFIX(LogManager::remove_node);
  if (prev == nullptr) { 
    // this is a tail, so just re-inits the LogNode
    laddr_t prev_addr = mut->get_prev_addr();
    mut->set_init_vars();
    if (prev_addr != L_ADDR_NULL) {
      mut->set_prev_addr(prev_addr);
    }
    co_return;
  }
  assert(mut);
  DEBUGT("mut={}, prev={}", t, *mut, *prev);
  laddr_t prev_addr = mut->get_prev_addr();
  co_await tm.remove(t, mut->get_laddr()
  ).handle_error_interruptible(
    omap_rm_key_iertr::pass_further{},
    crimson::ct_error::assert_all{"Invalid error in remove_node"}
  );
  auto mut_prev = tm.get_mutable_extent(t, prev)->template cast<LogNode>();
  assert(mut_prev);
  mut_prev->set_prev_addr(prev_addr);
  co_return;
}

LogManager::omap_rm_key_ret
LogManager::remove_kv(Transaction &t, laddr_t dst, const std::string &key, LogNodeRef prev)
{
  LOG_PREFIX(LogManager::remove_kv);
  DEBUGT("key={}, dst={}", t, key, dst);

  auto extent = co_await log_load_extent<LogNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return;
  }

  auto e = co_await extent->get_value(key);
  if (e == std::nullopt) {
    if(extent->get_prev_addr() == L_ADDR_NULL) {
      co_return;
    }
    co_await remove_kv(t, extent->get_prev_addr(), key, extent);
    co_return;
  }

  auto mut = tm.get_mutable_extent(t, extent)->template cast<LogNode>();
  mut->remove_entry(key);
  if (mut->is_removable()) {
    co_await remove_node(t, mut, prev);
  }
  if (!is_log_key(key) && mut->get_prev_addr() != L_ADDR_NULL) {
    // Remove all duplicate keys
    co_await remove_kv(t, mut->get_prev_addr(), key, mut);
  }
  co_return;
}

LogManager::omap_rm_key_ret
LogManager::remove_kvs(Transaction &t, laddr_t dst, 
  std::optional<std::string> first, 
  std::optional<std::string> last,
  LogNodeRef prev)
{
  LOG_PREFIX(LogManager::remove_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
  if (dst == L_ADDR_NULL || first == std::nullopt) {
    co_return;
  }

  auto extent = co_await log_load_extent<LogNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return;
  }
  auto l = last;
  if (l && (*l).empty()) {
    l = std::nullopt;
  }

  laddr_t prev_addr = extent->get_prev_addr();
  // If time-seris log, we don't need traversal anymore
  if (is_log_key(*first) && extent->has_less_than(*first) &&
      *first != std::string()) {
    co_return;
  }
  LogNodeRef p = extent;
  if (extent->has_between(first, l)) {
    auto mut = tm.get_mutable_extent(t, extent)->template cast<LogNode>();
    assert(mut);
    auto ret = mut->remove_entries(first, l);
    assert(ret);
    DEBUGT("remove {}, extent's last key of deleted entries={}",
      t, *extent, extent->get_last_key());
    p = mut;
    if (mut->is_removable()) {
      co_await remove_node(t, mut, prev);
      if (prev != nullptr) {
	p = co_await log_load_extent<LogNode>(
	  t, prev->get_laddr(), BEGIN_KEY, END_KEY);
      }
    }
  }
  co_await remove_kvs(t, prev_addr, first, last, p);
  co_return;
}

LogManager::omap_rm_key_ret 
LogManager::omap_rm_key(
  omap_root_t &log_root,
  Transaction &t,
  const std::string &key)
{
  LOG_PREFIX(LogManager::omap_rm_key);
  DEBUGT("key={}", t, key);
  assert(log_root.get_type() == omap_type_t::LOG);
  co_await remove_kv(t, log_root.addr, key, nullptr);
  co_return;
}

LogManager::omap_rm_key_range_ret 
LogManager::omap_rm_key_range(
  omap_root_t &log_root,
  Transaction &t,
  const std::string &first,
  const std::string &last,
  OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(LogManager::omap_rm_key_range);
  DEBUGT("first={}, last={}", t, first, last);
  assert(log_root.get_type() == omap_type_t::LOG);
  co_await remove_kvs(t, log_root.addr, first, last, nullptr);
  co_return;
}

LogManager::omap_clear_ret
LogManager::omap_clear(omap_root_t &root, Transaction &t)
{
  LOG_PREFIX(LogManager::omap_clear);
  DEBUGT("enter", t);
  assert(root.get_type() == omap_type_t::LOG);
  co_await remove_kvs(t, root.addr,
    std::optional<std::string>(),
    std::optional<std::string>(std::nullopt), nullptr);
  co_await tm.remove(t, root.get_location()
  ).handle_error_interruptible(
    omap_clear_iertr::pass_further{},
    crimson::ct_error::assert_all{"Invalid error in omap_clear"}
  );
  root.update(
    L_ADDR_NULL,
    0, L_ADDR_MIN, root.get_type());
  co_return;
}

LogManager::omap_iterate_ret 
LogManager::omap_iterate(
  const omap_root_t &log_root,
  Transaction &t,
  ObjectStore::omap_iter_seek_t &start_from,
  omap_iterate_cb_t callback)
{
  LOG_PREFIX(LogManager::omap_iterate);
  DEBUGT("start={}", t, start_from.seek_position);
  assert(log_root.get_type() == omap_type_t::LOG);

  std::string s = start_from.seek_position;
  std::map<std::string, bufferlist> kvs;
  if (start_from.seek_type == ObjectStore::omap_iter_seek_t::LOWER_BOUND) {
    co_await find_kvs(t, log_root.addr, std::optional<std::string>(s),
      std::optional<std::string>(std::nullopt), kvs);
  } else {
    assert(start_from.seek_type == ObjectStore::omap_iter_seek_t::UPPER_BOUND);
    co_await find_kvs(t, log_root.addr, std::optional<std::string>(std::nullopt),
      std::optional<std::string>(s), kvs);
  }

  ObjectStore::omap_iter_ret_t ret;
  for (auto &p : kvs) {
    std::string result(p.second.c_str(), p.second.length());
    ret = callback(p.first, result);
    if (ret == ObjectStore::omap_iter_ret_t::STOP) {
      break;
    }
  }
  co_return co_await omap_iterate_iertr::make_ready_future<
    ObjectStore::omap_iter_ret_t>(std::move(ret));
}


}
