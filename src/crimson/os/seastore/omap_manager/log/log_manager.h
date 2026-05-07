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
constexpr uint8_t OW_SIZE = 2;
constexpr uint8_t BATCH_CREATE_SIZE = 50;

/*
 * 
 * LogManager aims to handle key-value pairs for pgmeta_oid,
 * specialized in handling pg_log_entry_t.
 *
 *
 * To support ranged operations (insertion and deletion),
 * LogManager maintains two singl linked lists:
 *
 *   1) pg_log_entry_t list (including all KV entries except pg_log_dup_t)
 *   2) pg_log_dup_t list (duplicate entries only)
 *
 * The ONode points to the LogNode that contains the latest
 * pg_log_entry_t. That pg_log_entry_t, in turn, holds a pointer
 * to the head of the pg_log_dup_t list.
 *
 * Layout:
 *
 *                +----------------+
 *                |     ONode      |
 *                +----------------+
 *                        |
 *                        v
 *        +----------------------------------+
 *    <-  | LogNode (latest pg_log_entry_t)  |
 *        +----------------------------------+
 *                        |
 *                        v
 *        +----------------------------------+
 *    <-  | LogNode (latest pg_log_dup_t)    |
 *        +----------------------------------+
 *
 * This separation allows efficient ranged updates while
 * avoiding full traversal of duplicate entries.
 */
class LogManager : public OMapManager {
public:
  LogManager(TransactionManager &tm);
  initialize_omap_ret initialize_omap(Transaction &t,
    laddr_t hint, omap_type_t type) final;

  /**
   * omap_set_keys
   *
   *  Load the LOG tail extent and, for each (key, value):
   *    - If the key belongs to the LOG node, write via _log_set_key().
   *
   * @param root   LOG root the higher layer passed in.
   * @param t      Transaction context
   * @param _kvs   Batch of keys to set
   */
  omap_set_keys_ret omap_set_keys(omap_root_t &log_root,
    Transaction &t, std::map<std::string, ceph::bufferlist> kvs) final;

  // see omap_set_keys
  omap_set_key_ret omap_set_key(
    omap_root_t &log_root,
    Transaction &t,
    std::string key,
    ceph::bufferlist value) final;

  /**
   * omap_get_value
   *
   * get a key-value pair in either object's LOG root
   *
   * @param root       LOG root the higher layer passed in.
   * @param t          Transaction context 
   * @param key        The key to retrieve
   *
   */
  omap_get_value_ret
  omap_get_value(const omap_root_t &log_root, Transaction &t,
    std::string key) final;

  /**
   * omap_list
   *
   *  1) Resolve LOG from onode.
   *  2) Collect LOG list's key–values in the range [first, last] with find_kvs().
   *  3) Initialize an output pair (complete flag, result map), seed it with LOG entries.
   *  4) Merge base entries into the result map.
   *
   * @param root    LOG root the higher layer passed in.
   * @param t       Transaction context
   * @param first   Optional lower bound key
   * @param last    Optional upper bound key
   * @param config  see OMapManager
   */
  omap_list_ret omap_list(
    const omap_root_t &log_root,
    Transaction &t,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    OMapManager::omap_list_config_t config =
    OMapManager::omap_list_config_t()) final;

  /**
   * omap_rm_key_range
   *
   * - Remove entries in the LOG list within [first, last] by walking the LOG list.
   *
   * @param root    LOG root the higher layer passed in.
   * @param t       Transaction context 
   * @param first   Lower key bound for removal.
   * @param last    Upper key bound for removal.
   * @param config  see OMapManager
   */

  omap_rm_key_range_ret omap_rm_key_range(
    omap_root_t &log_root,
    Transaction &t,
    const std::string &first,
    const std::string &last) final;

  /**
   * omap_rm_key
   *
   * clear a key in either object's LOG list 
   *
   *  - If the key can be satisfied by the LOG list — i.e., the LOG list
   *    contains the relevant entry — remove from the LOG by walking the
   *    list and stop.
   *
   * @param root       LOG root the higher layer passed in.
   * @param t          Transaction context 
   * @param key        The key to remove.
   *
   */
  omap_rm_key_ret omap_rm_key(
    omap_root_t &log_root,
    Transaction &t,
    std::string key) final;


  omap_rm_keys_ret omap_rm_keys(
    omap_root_t &omap_root,
    Transaction &t,
    std::set<std::string> keys) final;

  /**
   * omap_clear
   *
   * clear all entires in object's LOG list 
   *
   * @param root       LOG root the higher layer passed in.
   * @param t          Transaction context 
   *
   */
  omap_clear_ret omap_clear(omap_root_t &log_root,
    Transaction &t) final;


  /**
   * omap_iterate
   *
   * This routine first consults the LOG list (omap_type_t::LOG) to
   * perform a traveral, invoking the user-provided callback on
   * those entries
   *
   * Ordering & range:
   *  - If start_from.seek_type == LOWER_BOUND, we fetch keys in the half-open
   *    range [s, end) from the LOG list.
   *  - If start_from.seek_type == UPPER_BOUND, we fetch keys in the range
   *    (start, s] from the LOG list.
   *
   * @param root       LOG root the higher layer passed in.
   * @param t          Transaction context 
   * @param start_from Seek hint: position string and LOWER/UPPER bound type.
   * @param callback  
   *
   */
  omap_iterate_ret omap_iterate(
    const omap_root_t &log_root,
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
    const ceph::bufferlist &value, bool can_ow = false);

  /**
   * remove_kv
   *
   * This function searches for the given @key starting from the LogNode
   * identified by @dst and recursively traverses the prev chain until
   * the key is found or the chain is exhausted.
   *
   * When the key is found, the corresponding entry is marked as removed
   * in the node's deletion bitmap. If the node becomes empty as a result,
   * it may be removed from the chain via remove_node().
   * For non-log (non-time-series) keys, duplicate keys must not remain.
   * In this case, once the key is removed from the current node, the
   * function continues recursively to remove all remaining duplicates
   * in earlier nodes.
   *
   * @param t     Transaction context.
   * @param dst   Logical address of the LogNode to start searching from.
   * @param key   The key to be removed.
   * @param prev  The next LogNode in the chain (nullptr if @dst is the tail).
   */

  omap_rm_key_ret remove_kv(Transaction &t, laddr_t dst, const std::string &key,
    LogNodeRef prev);
  
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
    std::optional<std::string> first, 
    std::optional<std::string> last,
    LogNodeRef prev);

  
  /**
   * remove_node
   *
   * If @prev is nullptr, the node is the tail of the chain. In this case,
   * the node is not physically removed; instead, it is re-initialized
   * while preserving its prev pointer (if any).
   *
   * Otherwise, the node is physically removed from the transaction manager,
   * and the previous node (@prev) is updated to bypass the removed node
   * by inheriting its prev address.
   *
   * @param t     Transaction context.
   * @param mut   The LogNode to be removed or re-initialized.
   * @param prev  The next LogNode in the chain (nullptr if @mut is the tail).
   */
  LogManager::omap_rm_key_ret remove_node(Transaction &t,
    LogNodeRef mut,
    LogNodeRef prev);

  base_iertr::future<laddr_t> get_dup_addr_from_root(Transaction &t, laddr_t addr);

  /**
   *
   * Support for multi-block KV pairs
   *
   * Each log_key_t contains a chunk_idx field to manage values
   * that span multiple LogNodes when the value size exceeds the
   * maximum capacity of a single LogNode.
   * For simplicity, we always create separate blocks for each chunk,
   * even if this may introduce some internal fragmentation.
   * TODO: Implement block packing to improve space efficiency.
   *
   * Layout example:
   *
   *                     log_root
   *                         |
   *                         v
   *        +-------------------------------+
   *        |  LogNode (1 KV, chunk_idx:2)  |
   *        |        (later chunk)          |
   *        +-------------------------------+
   *                         |
   *                         v
   *        +-------------------------------+
   *        |  LogNode (1 KV, chunk_idx:1)  |
   *        |        (earlier chunk)        |
   *        +-------------------------------+
   *
   */
  omap_get_value_iertr::future<>
  find_multi_block_kv(Transaction &t, const std::string &key,
    LogNodeRef extent, bufferlist &buf);
  omap_list_iertr::future<>
  find_multi_block_kvs(Transaction &t, LogNodeRef extent,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    std::map<std::string, bufferlist> &kvs);
  omap_set_key_ret
  _log_set_multi_block_key(omap_root_t &log_root,
    Transaction &t, LogNodeRef tail,
    const std::string &key, const ceph::bufferlist &value);


  TransactionManager &tm;
};

inline bool is_log_key(const std::string &s) {
  pg_log_entry_t e;
  return (s.size() == e.get_key_name().size() &&
      (s[0] >= (0 + '0') && s[0] <= (9 + '0'))) ||
      s.starts_with("dup_");
}

inline bool is_pg_log_key(const std::string &s) {
  pg_log_entry_t e;
  return (s.size() == e.get_key_name().size() &&
      (s[0] >= (0 + '0') && s[0] <= (9 + '0')));
}

inline bool is_dup_log_key(const std::string &s) {
  return s.starts_with("dup_");
}

inline bool is_ow_key(const std::string &s) {
  return s == fastinfo_key;
}

inline std::string get_ow_key() {
  return std::string(fastinfo_key);
}

bool is_continuous_fixed_width(const std::set<std::string>& keys);

}
