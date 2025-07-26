// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "log_node.h"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::logstore_manager{

LogStoreManager::LogStoreManager(
  TransactionManager &tm)
  : tm(tm) {}

LogStoreManager::initialize_lsm_ret
LogStoreManager::initialize_lsm(Transaction &t, laddr_t hint) 
{
  LOG_PREFIX(LogStoreManager::initialize_lsm);
  DEBUGT("hint: {}", t, hint);
  return tm.alloc_non_data_extent<LogNode>(t, hint, LOG_NODE_BLOCK_SIZE
  ).si_then([hint, &t](auto&& root_extent) {
      omap_root_t omap_root;
      omap_root.update(root_extent->get_laddr(), 1, hint,
	omap_type_t::LOG, root_extent->get_paddr());
      root_extent->set_head_leaf(L_ADDR_NULL);
      root_extent->tail_laddr = L_ADDR_NULL;
      root_extent->tail_paddr = P_ADDR_NULL;
      t.get_omap_tree_stats().extents_num_delta++;
      return initialize_lsm_iertr::make_ready_future<omap_root_t>(omap_root);
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    TransactionManager::alloc_extent_iertr::pass_further{}
  );
}

void delta_t::replay(LogKVNodeLayout &l) {
  auto iter = l.find_string_key(key);
  ceph_assert(iter->get_index() < MAX_NODE_ENTRY);
  if (op == op_t::INSERT) {
    l._insert(iter, key, val);
    return;
  } else if (op == op_t::UPDATE) {
    l._update(iter, key, val);
    return;
  } else if (op == op_t::REMOVE) {
    l._remove(iter);
    return;
  }
  ceph_assert(op == op_t::ADD_HEAD);
  l.set_head_leaf(next);
}

void delta_leaf_t::replay(LogKVLeafNodeLayout &l) {
  if (op == op_t::INSERT) {
    l._append(key, val);
    return;
  } 
  ceph_assert(op == op_t::ADD_NEXT);
  l.set_next_node(next);
}

void LogLeafNode::append_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val) {
  LOG_PREFIX(LogStoreManager::append_kv);
  DEBUGT("enter", t);
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append(key, val, p);
    return;
  }
  append(key, val);
}

void LogLeafNode::append_next_addr(laddr_t l) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append(l, p);
    return;
  }
  append(l);
}

LogStoreManager::log_set_keys_ret
LogStoreManager::log_set_keys(omap_root_t &log_root,
  Transaction &t, std::map<std::string, ceph::bufferlist>&& kvs) 
{
  LOG_PREFIX(LogStoreManager::log_set_keys);
  DEBUGT("enter kv size {}", t, kvs.size());
  return tm.get_extents_if_live(t, extent_types_t::LOG_NODE, 
    log_root.paddr, log_root.addr, LOG_NODE_BLOCK_SIZE
  ).si_then([&t, &log_root, this, kvs=std::move(kvs)](auto list) {
    auto handle_kv = [&t, &log_root, this](
      std::map<std::string, ceph::bufferlist>& kvs,
      LogNodeRef ext) {
      return trans_intr::do_for_each(
	kvs.begin(),
	kvs.end(),
	[&t, &log_root, ext=ext, this](auto &p) {
	CachedExtentRef node;
	Transaction::get_extent_ret ret;
	assert(ext->get_paddr().is_absolute());
	// To find mutable extent in the same transaction
	ret = t.get_extent(ext->get_paddr(), &node);
	assert(ret == Transaction::get_extent_ret::PRESENT);
	LogNodeRef log_node = node->template cast<LogNode>();
	if (can_handle_by_lognode(p.first)) {
	  // LogNode can contain a few key-value pairs.
	  // The sume of each pairs must not exceed LOG_NODE_BLOCK_SIZE. 
	  assert(!log_node->expect_overflow(p.first.size(), p.second.length()));
	  auto mut = tm.get_mutable_extent(t, log_node)->template cast<LogNode>();
	  mut->insert_kv(p.first, p.second);
	  return log_set_key_iertr::now();
	} else if (p.first.substr(0, 4) == std::string("dup_")) {
	  // TODO: handle dup_
	  return log_set_key_iertr::now();
	}
	return log_set_key(log_root, t, log_node, p.first, p.second);
      });
    };
    ceph_assert(list.size() <= 1);
    for (auto &e : list) {
      return seastar::do_with(std::move(kvs), std::move(e),
	[&t, &log_root, this, handle_kv=std::move(handle_kv)](auto& kvs, auto& e) {
	LogNodeRef ext = e->template cast<LogNode>();
	return handle_kv(kvs, ext);

      });
    }
    LOG_PREFIX(LogStoreManager::log_set_keys);
    INFOT("call load due to cache miss laddr={}", t, log_root.addr);
    return log_load_extent<LogNode>(
      t, log_root.addr, BEGIN_KEY, END_KEY
    ).si_then([&t, &log_root, this, kvs=std::move(kvs),
      handle_kv=std::move(handle_kv)](auto&& extent) {
      return seastar::do_with(std::move(kvs), std::move(extent),
	[&, this, handle_kv=std::move(handle_kv)](auto& kvs, auto& ext) {
	return handle_kv(kvs, ext);
      });
    });

  });
  return log_set_keys_iertr::now();
}

template <typename T>
requires std::is_same_v<LogNode, T> || std::is_same_v<LogLeafNode, T>
LogStoreManager::log_load_extent_iertr::future<TCachedExtentRef<T>>
LogStoreManager::find_tail(Transaction &t, laddr_t dst)
{
  return log_load_extent<T>(
    t, dst, BEGIN_KEY, END_KEY
  ).si_then([this, &t, dst](auto extent) {
    if (extent->get_next_leaf_addr() == L_ADDR_NULL) {
      return log_load_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
        std::move(extent));
    }
    return find_tail<T>(t, extent->get_next_leaf_addr());
  });
}

/***
 * TODO
 *  handling rollback_to 
 * 
 */

LogStoreManager::log_set_key_ret
LogStoreManager::log_set_key(omap_root_t &log_root,
  Transaction &t, LogNodeRef extent,
  const std::string &key, const ceph::bufferlist &value)
{
  auto add_func = [this, &t, &log_root](
    LogNodeRef r, LogLeafNodeRef tail, std::string &key, ceph::bufferlist &value) {
    LOG_PREFIX(LogStoreManager::log_set_key::add_func);
    DEBUGT("add_func enter {} ", t, *r);
    if ((r->get_head_leaf_laddr() != L_ADDR_NULL &&
	!tail->expect_overflow(key.size(), value.length())) ||
	(r->get_head_leaf_laddr() == L_ADDR_NULL && r->tail_laddr != L_ADDR_NULL)) {
      ceph_assert(tail);
      auto mut = tm.get_mutable_extent(t, tail)->cast<LogLeafNode>();
      mut->append_kv(t, key, value);
      return log_set_key_iertr::now();
    }
    return tm.alloc_non_data_extent<LogLeafNode>(
      t, log_root.hint, LOG_LEAF_NODE_BLOCK_SIZE
    ).si_then([&t, r=std::move(r), &key, &value, this, tail](auto&& extent) {
      if (r->get_head_leaf_laddr() == L_ADDR_NULL) {
	extent->append_kv(t, key, value);
	auto mut = tm.get_mutable_extent(t, r)->cast<LogNode>();
	mut->set_tail_addrs(extent->get_paddr(), extent->get_laddr());
	mut->append_head_laddr(extent->get_laddr());
      } else {
	ceph_assert(tail);
	auto mut = tm.get_mutable_extent(t, tail)->cast<LogLeafNode>();
	mut->append_next_addr(extent->get_laddr());
	extent->append_kv(t, key, value);
	auto mut_lognode = tm.get_mutable_extent(t, r)->cast<LogNode>();
	mut_lognode->set_tail_addrs(extent->get_paddr(), extent->get_laddr());
	// TODO: this is not necessary, just to update tail address correctly
	mut_lognode->append_head_laddr(r->get_head_leaf_laddr());
      }
      return log_set_key_iertr::now();
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      log_set_key_iertr::pass_further{}
    );
  };
  LOG_PREFIX(LogStoreManager::log_set_key);
  DEBUGT("enter lognode={}", t, *extent);
  return seastar::do_with(extent, key, value, std::move(add_func), [&t, this]
    (auto& r, auto& key, auto& value, auto&& add_func) {
    if (r->tail_laddr != L_ADDR_NULL) {
      return tm.get_extents_if_live(t, extent_types_t::LOG_LEAF, 
	r->tail_paddr, r->tail_laddr, LOG_LEAF_NODE_BLOCK_SIZE
      ).si_then([&t, r=std::move(r), this, add_func=std::move(add_func),
	&key, &value](auto list) {
	if (list.empty()) {
	  return log_load_extent<LogLeafNode>(
	    t, r->tail_laddr, BEGIN_KEY, END_KEY
	  ).si_then([&t, r=std::move(r), this, add_func=std::move(add_func),
	    &key, &value](auto extent) {
	    return add_func(r, extent, key, value);
	  });
	}
	for (auto &e : list) {
	  if (!e->is_valid()) {
	    continue;
	  }
	  LogLeafNodeRef logleaf = e->template cast<LogLeafNode>();
	  return add_func(r, std::move(logleaf), key, value);
	}
	ceph_assert(0 == "impossible");
	return log_set_key_iertr::now();
      });
    } else if (r->tail_laddr == L_ADDR_NULL && r->get_head_leaf_laddr() != L_ADDR_NULL) {
      // need to find tail leaf node to proceed
      LOG_PREFIX(LogStoreManager::log_set_key);
      INFOT("warning slow path {}", t, *r);
      return find_tail<LogLeafNode>(t, r->get_head_leaf_laddr()
      ).si_then([&t, r=std::move(r), add_func=std::move(add_func),
	&key, &value](auto ext) {
	// TODO: update tail addrs
	return add_func(r, std::move(ext), key, value);
      });
    }
    return add_func(r, nullptr, key, value);
  });
}


std::ostream &LogNode::print_detail_l(std::ostream &out) const
{
  out << ", size=" << get_size();
  out << ", head_laddr=" << get_head_leaf_laddr();
  out << ", tail_laddr=" << tail_laddr;
  out << ", tail_paddr=" << tail_paddr;
  if (get_size() > 0) {
    out << ", begin=" << get_begin()
	<< ", end=" << get_end();
  }
  return out;
}


std::ostream &LogLeafNode::print_detail_l(std::ostream &out) const
{
  laddr_t l = this->get_next_leaf_addr();
  out << ", next=" << l
      << ", num=" << this->get_size()
      << ", used_space=" << this->use_space()
      << ", capacity=" << this->get_capacity();
  out << ", begin=" << get_begin()
      << ", end=" << get_end();
  return out;
}

template <typename T>
requires std::is_same_v<LogNode, T> || std::is_same_v<LogLeafNode, T>
LogStoreManager::log_load_extent_iertr::future<TCachedExtentRef<T>> 
LogStoreManager::log_load_extent(
  Transaction &t,
  laddr_t laddr,
  std::string begin,
  std::string end)
{
  LOG_PREFIX(LogStoreManager::log_load_extent);
  DEBUGT("laddr={}", t, laddr);
  assert(end <= END_KEY);
  auto size = std::is_same_v<LogNode, T>
    ? LOG_NODE_BLOCK_SIZE : LOG_LEAF_NODE_BLOCK_SIZE;
  return tm.read_extent<T>(
    t, laddr, size,
    [begin=std::move(begin), end=std::move(end), FNAME,
    &t](T &extent) mutable {
      assert(!extent.is_seen_by_users());
      extent.init_range(std::move(begin), std::move(end));
    }
  ).handle_error_interruptible(
    log_load_extent_iertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error in log_load_extent" }
  ).si_then([](auto maybe_indirect_extent) {
    assert(!maybe_indirect_extent.is_indirect());
    assert(!maybe_indirect_extent.is_clone);
    return seastar::make_ready_future<TCachedExtentRef<T>>(
        std::move(maybe_indirect_extent.extent));
  });
}
LogStoreManager::log_get_value_ret
LogStoreManager::log_get_value(
  const omap_root_t &log_root, Transaction &t, const std::string &key)
{
  LOG_PREFIX(LogStoreManager::log_get_value);
  DEBUGT("key={}", t, key);
  return log_load_extent<LogNode>(
    t, log_root.addr, BEGIN_KEY, END_KEY
  ).si_then([this, &t, &key](auto extent) {
    if (can_handle_by_lognode(key)) {
      return extent->get_value(key);
    }
    return find_kv(t, extent->get_head_leaf_laddr(), key);
  });
}

LogStoreManager::log_list_ret
LogStoreManager::omap_list(
  const omap_root_t &log_root,
  Transaction &t,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(LogStoreManager::list);
  DEBUGT("first={}, last={}", t, first, last);
  return log_load_extent<LogNode>(
    t, log_root.addr, BEGIN_KEY, END_KEY
  ).si_then([this, &t, &first, &last](auto extent) {
    return seastar::do_with(std::map<std::string, bufferlist>(), std::move(extent),
      [&, this](auto &kvs, auto &extent) {
      extent->list(first, last, kvs);
      return find_kvs(t, extent->get_head_leaf_laddr(), first, last, kvs
      ).si_then([&, this]() {
	auto ret = log_list_bare_ret(false, {});
	auto &[complete, result] = ret;
	result.insert(kvs.begin(), kvs.end());
	return log_list_iertr::make_ready_future<log_list_bare_ret>(
	  std::move(ret));
      });
    });
  });
}

LogStoreManager::log_list_iertr::future<>
LogStoreManager::find_kvs(Transaction &t, laddr_t dst,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  std::map<std::string, bufferlist> &kvs)
{
  LOG_PREFIX(LogStoreManager::find_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
  if (dst == L_ADDR_NULL) {
      return log_list_iertr::now();
  }
  return log_load_extent<LogLeafNode>(
    t, dst, BEGIN_KEY, END_KEY
  ).si_then([this, &t, dst, &first, &last, &kvs](auto extent) {
    if (extent == nullptr) {
      return log_list_iertr::now();
    }
    return seastar::do_with(std::move(extent), [&, this](auto &extent) {
      extent->list(first, last, kvs);
      return find_kvs(t, extent->get_next_leaf_addr(), first, last, kvs);
    });
  });
}


LogStoreManager::log_get_value_ret
LogStoreManager::find_kv(Transaction &t, laddr_t dst, const std::string &key)
{
  LOG_PREFIX(LogStoreManager::find_kv);
  DEBUGT("key={}, dst={}", t, key, dst);
  return log_load_extent<LogLeafNode>(
    t, dst, BEGIN_KEY, END_KEY
  ).si_then([this, &t, dst, key](auto extent) {
    if (extent == nullptr) {
      return log_get_value_ret(
          interruptible::ready_future_marker{},
          std::nullopt);
    }
    return seastar::do_with(std::move(extent), [&, this](auto &extent) {
      return extent->get_value(key
      ).si_then([&, this, extent](auto &&e) {
	if (e == std::nullopt) {
	  if(extent->get_next_leaf_addr() == L_ADDR_NULL) {
	    return log_get_value_ret(
		interruptible::ready_future_marker{},
		std::nullopt);
	  }
	  return find_kv(t, extent->get_next_leaf_addr(), key);
	}
	return log_get_value_ret(
	    interruptible::ready_future_marker{},
	    std::move(e));
      });
    });
  });
}

LogStoreManager::log_rm_key_ret
LogStoreManager::remove_kvs(Transaction &t, laddr_t dst, 
  const std::string &first, const std::string &last, LogNodeRef root)
{
  LOG_PREFIX(LogStoreManager::remove_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
  if (dst == L_ADDR_NULL) {
      return log_rm_key_iertr::now();
  }
  return log_load_extent<LogLeafNode>(
    t, dst, BEGIN_KEY, END_KEY
  ).si_then([this, &t, dst, &first, &last, &root](auto extent) {
    if (extent == nullptr) {
      return log_rm_key_iertr::now();
    }
    return seastar::do_with(std::move(extent), [&, this](auto &extent) {
      auto f = std::make_optional<std::string>(first);
      auto l = last.empty() ? std::nullopt : std::make_optional<std::string>(last);
      std::map<std::string, bufferlist> kvs;
      if (extent->first_is_larger_than(l)) {
	return log_rm_key_iertr::now();
      }
      extent->list(f, l, kvs);
      if ((!kvs.empty() && kvs.rbegin()->first == extent->get_last_key()) ||
	  extent->last_is_less_than(l)) {
	laddr_t next = extent->get_next_leaf_addr();
	return tm.remove(t, extent->get_laddr()
	).si_then([&t, this, next=next, &first, &last, &root, &extent](auto &&ret) 
	  -> log_rm_key_ret {
	  CachedExtentRef node;
	  Transaction::get_extent_ret r;
	  r = t.get_extent(root->get_paddr(), &node);
	  assert(r == Transaction::get_extent_ret::PRESENT);
	  LogNodeRef log_node = node->template cast<LogNode>();
	  assert(log_node);
	  auto mut = tm.get_mutable_extent(t, log_node)->template cast<LogNode>();
	  assert(mut);
	  mut->append_head_laddr(next);
	  if (mut->tail_laddr == extent->get_laddr()) {
	    // this can be retreived if needed
	    mut->set_tail_addrs(P_ADDR_NULL, next);
	  }
	  return remove_kvs(t, next, first, last, root);
	}).handle_error_interruptible(
	  log_rm_key_iertr::pass_further{},
	  crimson::ct_error::assert_all{
	    "Invalid error in handle_root_merge"
	  }
	);
      }
      return remove_kvs(t, extent->get_next_leaf_addr(), first, last, root);
    });
  });
}

LogStoreManager::log_rm_key_ret LogStoreManager::log_rm_key(
  omap_root_t &log_root,
  Transaction &t,
  const std::string &key)
{
  LOG_PREFIX(LogStoreManager::log_rm_key);
  DEBUGT("key={}", t, key);
  return log_load_extent<LogNode>(
    t, log_root.addr, BEGIN_KEY, END_KEY
  ).si_then([this, &t, &key](auto extent) {
    if (can_handle_by_lognode(key)) {
      auto mut = tm.get_mutable_extent(t, extent)->template cast<LogNode>();
      assert(mut);
      mut->remove_kv(key);
      return log_rm_key_iertr::now();
    }
    return seastar::do_with(std::move(extent), [&, this](auto &extent) {
      return remove_kvs(t, extent->get_head_leaf_laddr(), std::string(), key, extent);
    });
  });
}

LogStoreManager::log_rm_key_range_ret 
LogStoreManager::log_rm_key_range(
    omap_root_t &log_root,
    Transaction &t,
    const std::string &first,
    const std::string &last,
    OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(LogStoreManager::log_rm_key_range);
  DEBUGT("first={}, last={}", t, first, last);
  return log_load_extent<LogNode>(
    t, log_root.addr, BEGIN_KEY, END_KEY
  ).si_then([this, &t, &first, &last](auto extent) {
    auto f = std::make_optional<std::string>(first);
    auto l = std::make_optional<std::string>(last);
    LogNodeRef lognode = extent;
    std::map<std::string, bufferlist> kvs;
    extent->list(f, l, kvs);
    if (kvs.size() > 0) {
      lognode = tm.get_mutable_extent(t, extent)->template cast<LogNode>();
      for (auto p : kvs) {
	lognode->remove_kv(p.first);	
      }
    }
    return seastar::do_with(std::move(lognode), [&, this](auto &extent) {
      return remove_kvs(t, extent->get_head_leaf_laddr(), first, last, extent);
    });
  });
}

}
