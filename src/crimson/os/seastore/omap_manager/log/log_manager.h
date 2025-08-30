// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

#include "include/denc.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/omap_manager.h"
#include "log_node.h"

namespace crimson::os::seastore::log_manager{

class LogNode;
using LogNodeRef = TCachedExtentRef<LogNode>;

/*
 * 
 * LogManager aims to handle key-value pairs for pg_log_entry_t.
 * All other pairs are delegated to the existing OMapManager.
 *
 */
class LogManager : public OMapManager {
public:
  LogManager(TransactionManager &tm);
  initialize_omap_ret initialize_omap(Transaction &t,
    laddr_t hint, omap_type_t type) final;

  /**
   * omap_set_keys
   *
   *  1) Resolve LOG/OMAP roots from onode.
   *  2) If base OMAP root is null, initialize it.
   *  3) Load the LOG head extent and, for each (key, value):
   *      - If the key belongs to the LOG node, write via _log_set_key().
   *      - Otherwise, delegate to the  OMAP manager.
   *  4) Persist root updates if either root reports must_update().
   *
   * @param root   LOG root the higher layer passed in.
   * @param onode  Onode 
   * @param t      Transaction context
   * @param _kvs   Batch of keys to set
   */
  omap_set_keys_ret omap_set_keys(omap_root_t &log_root, Onode &onode,
    Transaction &t, std::map<std::string, ceph::bufferlist>&& kv) final;

  // see omap_set_keys
  omap_set_key_ret omap_set_key(
    omap_root_t &log_root,
    Onode &onode,
    Transaction &t,
    const std::string &key,
    const ceph::bufferlist &value) final;

  /**
   * omap_get_value
   *
   * get a key-value pair in either object's LOG list or OMAP tree
   *
   *  - If the key can be satisfied by the LOG list — i.e., the LOG list
   *    contains the relevant entry — retrieve the entry from the LOG by walking the
   *    list and stop.
   *  - Otherwise, delegate to the OMAP manager to get key.
   *
   * @param root       LOG root the higher layer passed in.
   * @param onode      Onode 
   * @param t          Transaction context 
   * @param key        The key to retrieve
   *
   */
  omap_get_value_ret
  omap_get_value(const omap_root_t &log_root, Onode &onode, Transaction &t,
    const std::string &key) final;

  /**
   * omap_list
   *
   *  1) Resolve LOG and base OMAP roots from onode.
   *  2) Collect LOG list's key–values in the range [first, last] with find_kvs().
   *  3) Initialize an output pair (complete flag, result map), seed it with LOG entries.
   *  4) Delegate to BtreeOMapManager for base OMAP entries in the same range.
   *  5) Merge base entries into the result map.
   *
   * @param root    LOG root the higher layer passed in.
   * @param onode   Onode 
   * @param t       Transaction context
   * @param first   Optional lower bound key
   * @param last    Optional upper bound key
   * @param config  see OMapManager
   */
  omap_list_ret omap_list(
    const omap_root_t &log_root,
    Onode &onode,
    Transaction &t,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    OMapManager::omap_list_config_t config =
    OMapManager::omap_list_config_t()) final;

  /**
   * omap_rm_key_range
   *
   * - Remove entries in the LOG list within [first, last] by walking the LOG list.
   * - Delegate to the OMAP manager to remove the same key range.
   *
   * @param root    LOG root the higher layer passed in.
   * @param onode   Onode 
   * @param t       Transaction context 
   * @param first   Lower key bound for removal.
   * @param last    Upper key bound for removal.
   * @param config  see OMapManager
   */

  omap_rm_key_range_ret omap_rm_key_range(
    omap_root_t &log_root,
    Onode &onode,
    Transaction &t,
    const std::string &first,
    const std::string &last,
    OMapManager::omap_list_config_t config) final;

  /**
   * omap_rm_key
   *
   * clear a key in either object's LOG list or OMAP tree
   *
   *  - If the key can be satisfied by the LOG list — i.e., the LOG list
   *    contains the relevant entry — remove from the LOG by walking the
   *    list and stop.
   *  - Otherwise, delegate to the OMAP manager to remove key.
   *
   * @param root       LOG root the higher layer passed in.
   * @param onode      Onode 
   * @param t          Transaction context 
   * @param key        The key to remove.
   *
   */
  omap_rm_key_ret omap_rm_key(
    omap_root_t &log_root,
    Onode &onode,
    Transaction &t,
    const std::string &key) final;


  omap_rm_keys_ret omap_rm_keys(
    omap_root_t &omap_root,
    Onode &onode,
    Transaction &t,
    std::set<std::string>& keys) final;

  /**
   * omap_clear
   *
   * clear all entires in object's LOG list and OMAP tree
   *
   * @param root       LOG root the higher layer passed in.
   * @param onode      Onode 
   * @param t          Transaction context 
   *
   */
  omap_clear_ret omap_clear(omap_root_t &log_root,
    Onode &onode, Transaction &t) final;


  /**
   * omap_iterate
   *
   * This routine first consults the LOG list (omap_type_t::LOG) to
   * perform a traveral, invoking the user-provided callback on
   * those entries, and then delegates to the OMAP tree
   * (omap_type_t::OMAP) to continue iteration.
   *
   * Ordering & range:
   *  - If start_from.seek_type == LOWER_BOUND, we fetch keys in the half-open
   *    range [s, end) from the LOG list.
   *  - If start_from.seek_type == UPPER_BOUND, we fetch keys in the range
   *    (start, s] from the LOG list.
   *  - NOTE: We store results in std::map, which iterates in ascending key
   *    order regardless of insertion order. If reverse/strict ordering is
   *    required, this function may need adjustment (see TODO).
   *
   * @param root       LOG root the higher layer passed in.
   * @param onode      Onode 
   * @param t          Transaction context 
   * @param start_from Seek hint: position string and LOWER/UPPER bound type.
   * @param callback  
   *
   */
  omap_iterate_ret omap_iterate(
    const omap_root_t &log_root,
    Onode &onode,
    Transaction &t,
    ObjectStore::omap_iter_seek_t &start_from,
    omap_iterate_cb_t callback
  ) final;


  omap_list_iertr::future<>
  find_kvs(Transaction &t, laddr_t dst, const std::optional<std::string> &first,
    const std::optional<std::string> &last, std::map<std::string, bufferlist> &kvs);

  using log_load_extent_iertr = base_iertr;
  template <typename T>
  requires std::is_same_v<LogNode, T>
  log_load_extent_iertr::future<TCachedExtentRef<T>> log_load_extent(
    Transaction &t, laddr_t laddr, std::string begin, std::string end);

  bool can_handle_by_logleaf(std::string s) {
    pg_log_entry_t e;
    return (s.size() == e.get_key_name().size() &&
	(s[0] >= (0 + '0') && s[0] <= (9 + '0')));
  }
  omap_get_value_ret find_kv(Transaction &t, laddr_t dst, const std::string &key);

  /**
   * _log_set_key
   *
   *  - Fast path: if the current LOG node (tail) has enough space for (key,value),
   *    get a mutable view within this transaction and append in place.
   *  - Split path: if appending would overflow the LOG node, allocate
   *  	a fresh LogNode extent,
   *    make it the new LOG tail (update log_root), append the KV there, and link
   *    the previous head via prev_addr.
   *
   * @param log_root  LOG root descriptor
   * @param t         Transaction context
   * @param tail      Current append target
   * @param key       Key to set/append.
   * @param value     Value to set/append.
   *
   */
  omap_set_key_ret _log_set_key(omap_root_t &log_root,
    Transaction &t, LogNodeRef e, const std::string &key,
    const ceph::bufferlist &value);
  
  /**
   * remove_kvs
   *
   * Starting at logical address dst, this loads a LogNode extent,
   * gathers entries in the [first, last] range, and decides
   * whether the current extent can be removed
   * If so, it removes the extent and fixes the link pointer
   * with prev. Otherwise it recurses to the previous extent.
   *
   * @param t       Transaction context.
   * @param dst     Logical address of the starting extent
   * @param first   lower key bound (optional).
   * @param last    upper key bound (optional). Empty string => unbounded.
   * @param prev    The successor of dst in the forward direction (used to fix links
   *                when dst is removed). For the initial call at tail, pass nullptr.
   *
   * @return omap_rm_key_ret 
   */
  omap_rm_key_ret remove_kvs(Transaction &t, laddr_t dst,
    const std::optional<std::string> &first, 
    const std::optional<std::string> &last,
    LogNodeRef root);

  /**
   * concat_remove
   *
   * Starting from logical address dst, this walks backward via
   * prev_addr to the oldest extent (the one whose prev_addr == L_ADDR_NULL),
   * removes that oldest extent first, and then unwinds the recursion removing
   * each subsequent extent up to and including dst.
   *
   * @param t    Transaction context
   * @param dst  Logical address of the tail extent to delete 
   */
  omap_rm_key_ret concat_remove(Transaction &t, laddr_t dst);

  TransactionManager &tm;
};

}
