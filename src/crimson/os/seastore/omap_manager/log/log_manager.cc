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
  std::pair<std::string, ceph::bufferlist> ow_kv;
  auto f = [&](const std::string &k, const bufferlist &v, bool has_ow_key) 
    -> omap_set_key_ret {
    CachedExtentRef node;
    Transaction::get_extent_ret ret;
    // To find mutable extent in the same transaction
    ret = t.get_extent(ext->get_paddr(), &node);
    assert(ret == Transaction::get_extent_ret::PRESENT);
    assert(node);
    LogNodeRef log_node = node->template cast<LogNode>();
    bool can_ow = has_ow_key && log_node->can_ow();
    co_await _log_set_key(log_root, t, log_node, k, v, can_ow);
    co_return;
  };
  /*
   * During a normal write transaction, pgmeta_oid receives two key–value pairs:
   * _fastinfo and pg_log_entry. Unlike pg_log_entry, _fastinfo is likely to be
   * overwritten in the near future. Storing _fastinfo in an append-only manner
   * within a LogNode causes unnecessary space overhead and requires garbage
   * collection.
   * To mitigate this, LogManager adjusts the write sequence of _fastinfo and
   * pg_log_entry by placing _fastinfo at the last position of the LogNode.
   * As a result, _fastinfo can be overwritten by the next pg_log_entry, and a new
   * _fastinfo is appended afterward.
   *
   * | pg_log_entry #1 | _fastinfo #1 | ->
   * | pg_log_entry #1 | pg_log_entry #2 | _fastinfo #2 |
   *
   * Furthermore, if we ensure that the last entry of each LogNode is always
   * _fastinfo, garbage collection is unnecessary, because the new _fastinfo
   * will be appended to a new LogNode.
   */
  bool has_ow_key = false;
  if (kvs.size() <= OW_SIZE) {
    for (auto &p : kvs) {
      if (is_ow_key(p.first)) {
	ow_kv.first = p.first;
	ow_kv.second = p.second;
	has_ow_key = true;
	break;
      }
    }
  }

  if (kvs.size() > BATCH_CREATE_SIZE) {
    auto alloc_log_node = [&](laddr_t prev_laddr) 
      -> omap_set_key_iertr::future<LogNodeRef> {
      return tm.alloc_non_data_extent<LogNode>(
	t, log_root.hint, LOG_NODE_BLOCK_SIZE
      ).handle_error_interruptible(
	crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	omap_set_key_iertr::pass_further{}
      ).si_then([prev_laddr](auto ext) {
	assert(ext);
	ext->set_prev_addr(prev_laddr);
	return omap_set_key_iertr::make_ready_future<LogNodeRef>(ext);
      });
    };

    LogNodeRef e = co_await alloc_log_node(ext->get_laddr());
    for (auto &p : kvs) {
      if (e->expect_overflow(p.first.size(), p.second.length())) {
	e = co_await alloc_log_node(e->get_laddr());
      }
      e->append_kv(t, p.first, p.second);
    }

    log_root.update(e->get_laddr(), log_root.depth,
      log_root.hint, log_root.type);
    co_return;
  }

  for (auto &p : kvs) {
    if (is_ow_key(p.first)) {
      continue;
    }
    co_await f(p.first, p.second, has_ow_key);
  }

  if (!ow_kv.first.empty()) {
    co_await f(ow_kv.first, ow_kv.second, has_ow_key);
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
  const std::string &key, const ceph::bufferlist &value, bool can_ow)
{
  LOG_PREFIX(LogManager::_log_set_key);
  DEBUGT("enter key={}", t, key);
  assert(tail);
  if (!tail->expect_overflow(key, value.length(), can_ow)) {
    auto mut = tm.get_mutable_extent(t, tail)->cast<LogNode>();
    if (can_ow) {
      mut->overwrite_kv(t, key, value);
    } else {
      mut->append_kv(t, key, value);
    }
    co_return;
  }

  // This means the first entry of the new LogNode is not _fastinfo
  if (!is_ow_key(key) && can_ow) {
    // remove _fastinfo in old LogNode
    auto e = co_await tail->get_value(key);
    if (e != std::nullopt) {
      auto mut = tm.get_mutable_extent(t, tail)->template cast<LogNode>();
      mut->remove_entry(get_ow_key());
    }
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

static inline bool add_decimal_string(std::string& s, size_t add)
{
  for (size_t step = 0; step < add; ++step) {
    for (int i = (int)s.size() - 1; i >= 0; --i) {
      if (s[i] < '0' || s[i] > '9') return false;
      if (s[i] != '9') {
        ++s[i];
        break;
      }
      s[i] = '0';
      if (i == 0) return false; // overflow
    }
  }
  return true;
}

bool is_continuous_fixed_width(const std::set<std::string>& keys) {
  const auto& first = *keys.begin();
  const auto& last  = *keys.rbegin();
  if (first.size() != last.size()) return false;
  std::string expected = first;
  if (!add_decimal_string(expected, keys.size() - 1)) return false;
  return expected == last;
}

LogManager::omap_rm_keys_ret
LogManager::omap_rm_keys(
  omap_root_t& log_root,
  Transaction& t,
  std::set<std::string>& keys)
{
  LOG_PREFIX(LogManager::omap_rm_keys);
  DEBUGT("key size={}", t, keys.size());
  assert(log_root.get_type() == omap_type_t::LOG);
  
  std::set<std::string> dup_keys;
  auto begin = keys.lower_bound("dup_");
  auto end   = keys.lower_bound("dup`");
  while (begin != end) {
    auto nh = keys.extract(begin++); 
    dup_keys.insert(std::move(nh)); 
  }


  // Deletion of pg_log_entry_t entries is performed by omap_rm_keys using a set.
  // For example, omap_rm_keys might be called with a set containing
  // pg_log_entry_t entries ranging from 0011.0001 to 0011.0010.
  // In this case, calling omap_rm_key individually for each entry is inefficient,
  // because each call triggers a traversal of the entire list.
  auto remove_key_set = [&](auto& key_set) -> omap_rm_key_ret {
    if (key_set.empty())
      co_return;

    bool continuous = is_continuous_fixed_width(key_set);
    if (continuous) {
      // fast path
      co_await remove_kvs(
	  t, log_root.addr,
	  *key_set.begin(),
	  *key_set.rbegin(),
	  nullptr);
    } else {
      for (auto& p : key_set) {
	co_await remove_kv(t, log_root.addr, p, nullptr);
      }
    }
  };
  co_await remove_key_set(keys);
  co_await remove_key_set(dup_keys);
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
