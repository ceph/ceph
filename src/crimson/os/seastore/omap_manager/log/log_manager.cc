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
  Onode &onode,
  Transaction &t, std::map<std::string, ceph::bufferlist>&& _kvs) 
{
  LOG_PREFIX(LogManager::omap_set_keys);
  DEBUGT("enter kv size {}", t, _kvs.size());
  assert(log_root.get_type() == omap_type_t::LOG);

  auto kvs = std::move(_kvs);
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));
  assert(!log_root.is_null());

  if (omap_root.is_null()) {
    crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
    auto new_root = co_await omap_manager.initialize_omap(
      t, onode.get_metadata_hint(tm.get_block_size()),
      omap_root.get_type());
    omap_root = new_root;
  }

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
    if (can_handle_by_logleaf(p.first)) {
      co_await _log_set_key(log_root, t, log_node, p.first, p.second);
    } else {
      crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
      co_await std::move(omap_manager).omap_set_key(omap_root, onode, t,
	p.first, p.second);
    }
  }

  if (!omap_root.is_null() && omap_root.must_update()) {
    onode.update_omap_root(t, omap_root);
  }
  co_return;
}

LogManager::omap_set_key_ret 
LogManager::omap_set_key(
  omap_root_t &log_root,
  Onode &onode, Transaction &t,
  const std::string &key, const ceph::bufferlist &value) 
{
  LOG_PREFIX(LogManager::omap_set_key);
  DEBUGT("enter k={}", t, key);
  assert(log_root.get_type() == omap_type_t::LOG);

  std::map<std::string, ceph::bufferlist> kvs;
  kvs.emplace(key, value);
  co_return co_await omap_set_keys(log_root, onode, t, std::move(kvs));
}

LogManager::omap_set_key_ret
LogManager::_log_set_key(omap_root_t &log_root,
  Transaction &t, LogNodeRef tail,
  const std::string &key, const ceph::bufferlist &value)
{
  LOG_PREFIX(LogManager::_log_set_key);
  DEBUGT("enter", t);
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
      << ", capacity=" << this->get_capacity();
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
  const omap_root_t &log_root, Onode &onode, Transaction &t, const std::string &key)
{
  LOG_PREFIX(LogManager::omap_get_value);
  DEBUGT("key={}", t, key);
  assert(log_root.get_type() == omap_type_t::LOG);
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));
  if (can_handle_by_logleaf(key)) {
    auto ret = co_await find_kv(t, log_root.addr, key);
    co_return ret;
  }
  crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
  auto ret = co_await std::move(omap_manager).omap_get_value(omap_root, onode, t, key);
  co_return ret;
}

LogManager::omap_list_ret
LogManager::omap_list(
  const omap_root_t &log_root,
  Onode &onode,
  Transaction &t,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(LogManager::omap_list);
  DEBUGT("first={}, last={}", t, first, last);
  assert(log_root.get_type() == omap_type_t::LOG);
  std::map<std::string, bufferlist> kvs;
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));
  co_await find_kvs(t, log_root.addr, first, last, kvs);
  auto ret = omap_list_bare_ret(false, {});
  auto &[complete, result] = ret;
  result.insert(kvs.begin(), kvs.end());
  crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
  auto ret2 = co_await std::move(omap_manager).omap_list(
    omap_root, onode, t, first, last, config);
  auto &[complete2, result2] = ret2;
  result.insert(result2.begin(), result2.end());
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
LogManager::concat_remove(Transaction &t, laddr_t dst)
{
  LOG_PREFIX(LogManager::concat_remove);
  DEBUGT("dst={}", t, dst);
  auto extent = co_await log_load_extent<LogNode>(
    t, dst, BEGIN_KEY, END_KEY);
  assert(extent);
  if (extent->get_prev_addr() == L_ADDR_NULL) {
    co_await tm.remove(t, extent->get_laddr()
    ).handle_error_interruptible(
      omap_rm_key_iertr::pass_further{},
      crimson::ct_error::assert_all{"Invalid error in concat_remove"}
    );
    co_return;
  }
  co_await concat_remove(t, extent->get_prev_addr());
  co_await tm.remove(t, extent->get_laddr()
  ).handle_error_interruptible(
    omap_rm_key_iertr::pass_further{},
    crimson::ct_error::assert_all{"Invalid error in concat_remove"}
  );
  co_return;
}

LogManager::omap_rm_key_ret
LogManager::remove_kvs(Transaction &t, laddr_t dst, 
  const std::optional<std::string> &first, 
  const std::optional<std::string> &last,
  LogNodeRef prev)
{
  LOG_PREFIX(LogManager::remove_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
  if (dst == L_ADDR_NULL) {
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
  std::map<std::string, bufferlist> kvs;
  extent->list(first, l, kvs);
  if ((!kvs.empty() && kvs.rbegin()->first == extent->get_last_key()) ||
      extent->last_is_less_than(l)) {
    DEBUGT("remove {}, extent's last key={}",
      t, *extent, extent->get_last_key());
    if (prev == nullptr) { 
      // this is a tail, so just re-inits the LogNode
      auto mut = tm.get_mutable_extent(t, extent)->template cast<LogNode>();
      assert(mut);
      mut->set_init_vars();
      if (mut->get_prev_addr() == L_ADDR_NULL) {
	co_return;
      }
    } 
    co_await concat_remove(t, extent->get_laddr());
    if (prev != nullptr) {
      auto mut = tm.get_mutable_extent(t, prev)->template cast<LogNode>();
      assert(mut);
      mut->set_prev_addr(L_ADDR_NULL);
    }
    co_return;
  }
  co_await remove_kvs(t, extent->get_prev_addr(), first, last, extent);
  co_return;
}

LogManager::omap_rm_key_ret 
LogManager::omap_rm_key(
  omap_root_t &log_root,
  Onode &onode,
  Transaction &t,
  const std::string &key)
{
  LOG_PREFIX(LogManager::omap_rm_key);
  DEBUGT("key={}", t, key);
  assert(log_root.get_type() == omap_type_t::LOG);
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));
  if (can_handle_by_logleaf(key)) {
    // TODO/FIXME: In the LOG case, we probably don't need log entries prior to the target.
    // Therefore, remove all log entries that precede the target.
    co_await remove_kvs(t, log_root.addr, std::string(), key, nullptr);
    co_return;
  }
  crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
  co_await std::move(omap_manager).omap_rm_key(omap_root, onode, t, key);
  if (!omap_root.is_null() && omap_root.must_update()) {
    onode.update_omap_root(t, omap_root);
  }
  co_return;
}

LogManager::omap_rm_keys_ret
LogManager::omap_rm_keys(
  omap_root_t& log_root,
  Onode& onode,
  Transaction& t,
  std::set<std::string>& keys)
{
  LOG_PREFIX(LogManager::omap_rm_key);
  DEBUGT("key size={}", t, keys.size());
  assert(log_root.get_type() == omap_type_t::LOG);
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));
  bool done = false;
  // Deletion of pg_log_entry_t entries is performed by omap_rm_keys using a set.
  // For example, omap_rm_keys might be called with a set containing
  // pg_log_entry_t entries ranging from 0011.0001 to 0011.0010.
  // In this case, calling omap_rm_key individually for each entry is inefficient,
  // because each call triggers a traversal of the entire list.
  for (auto iter = keys.rbegin(); iter != keys.rend(); ++iter) {
    if (can_handle_by_logleaf(*iter) && !done) {
      co_await remove_kvs(t, log_root.addr, std::string(), *iter, nullptr);
      done = true;
    } else if (can_handle_by_logleaf(*iter) && done) {
      continue;
    }
    crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
    co_await std::move(omap_manager).omap_rm_key(omap_root, onode, t, *iter);
  }
  if (!omap_root.is_null() && omap_root.must_update()) {
    onode.update_omap_root(t, omap_root);
  }
  co_return;
}

LogManager::omap_rm_key_range_ret 
LogManager::omap_rm_key_range(
  omap_root_t &log_root,
  Onode &onode,
  Transaction &t,
  const std::string &first,
  const std::string &last,
  OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(LogManager::omap_rm_key_range);
  DEBUGT("first={}, last={}", t, first, last);
  assert(log_root.get_type() == omap_type_t::LOG);
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));
  co_await remove_kvs(t, log_root.addr, first, last, nullptr);
  crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
  co_await std::move(omap_manager).omap_rm_key_range(
    omap_root, onode, t, first, last, config);
  if (!omap_root.is_null() && omap_root.must_update()) {
    onode.update_omap_root(t, omap_root);
  }
  co_return;
}

LogManager::omap_clear_ret
LogManager::omap_clear(omap_root_t &root, Onode &onode, Transaction &t)
{
  LOG_PREFIX(LogManager::omap_clear);
  DEBUGT("enter", t);
  // omap_clear is called multiple times with different types of roots,
  // so what LogManager needs to do is handle the corresponding root.
  if (root.get_type() == omap_type_t::LOG) {
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
  } else {
    crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
    co_await std::move(omap_manager).omap_clear(
      root, onode, t);
  }
  co_return;
}

LogManager::omap_iterate_ret 
LogManager::omap_iterate(
  const omap_root_t &log_root,
  Onode &onode,
  Transaction &t,
  ObjectStore::omap_iter_seek_t &start_from,
  omap_iterate_cb_t callback)
{
  LOG_PREFIX(LogManager::omap_iterate);
  DEBUGT("start={}", t, start_from.seek_position);
  assert(log_root.get_type() == omap_type_t::LOG);
  auto omap_root = onode.get_root(omap_type_t::OMAP).get(
        onode.get_metadata_hint(tm.get_block_size()));

  std::string s = start_from.seek_position;
  std::map<std::string, bufferlist> kvs;
  if (start_from.seek_type == ObjectStore::omap_iter_seek_t::LOWER_BOUND) {
    // TODO: Do we need a sorted result? 
    // Current implementation performs a traversal in reverse order
    co_await find_kvs(t, log_root.addr, std::optional<std::string>(s),
      std::optional<std::string>(std::nullopt), kvs);
  } else {
    assert(start_from.seek_type == ObjectStore::omap_iter_seek_t::UPPER_BOUND);
    co_await find_kvs(t, log_root.addr, std::optional<std::string>(),
      std::optional<std::string>(s), kvs);
  }

  for (auto &p : kvs) {
    std::string result(p.second.c_str(), p.second.length());
    auto ret = callback(p.first, result);
    if (ret == ObjectStore::omap_iter_ret_t::STOP) {
      break;
    }
  }

  crimson::os::seastore::omap_manager::BtreeOMapManager omap_manager(tm);
  co_return co_await std::move(omap_manager).omap_iterate(
    omap_root, onode, t, start_from, callback);
}


}
