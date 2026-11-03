// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <algorithm>
#include <set>
#include <string>
#include <string_view>
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

/**
 * pending_keys_t
 *
 * The set of keys remove_kv_set() is still looking for. It is probed once per
 * live entry of every LogNode it walks - a chain of a hundred nodes holding
 * ~62 entries each is thousands of probes per traversal - so the layout
 * matters more than the interface:
 *
 *  - the keys are views into storage the caller already owns, so building the
 *    set copies no strings and allocates once,
 *  - they sit in a single sorted vector, so a probe is a binary search over
 *    contiguous memory rather than a walk down a red-black tree whose nodes
 *    (and, for keys past the SSO limit, whose key buffers) are scattered,
 *  - striking a key off flips a bit instead of shifting the tail, so clearing
 *    all K keys is O(K) rather than the O(K^2) a flat_set erase() would cost.
 *
 * The referenced keys must outlive the pending_keys_t.
 */
class pending_keys_t {
public:
  explicit pending_keys_t(std::vector<std::string_view> &&keys)
    : keys(std::move(keys)) {
    // Sources are ordered containers, so this is a scan in the common case.
    std::sort(this->keys.begin(), this->keys.end());
    assert(std::adjacent_find(this->keys.begin(), this->keys.end()) ==
      this->keys.end());
    live.assign(this->keys.size(), true);
    remaining = this->keys.size();
  }

  bool empty() const { return remaining == 0; }
  size_t size() const { return remaining; }

  // Strike @key off the set, reporting whether it was still in it.
  bool take(std::string_view key) {
    auto iter = std::lower_bound(keys.begin(), keys.end(), key);
    if (iter == keys.end() || *iter != key) {
      return false;
    }
    auto index = iter - keys.begin();
    if (!live[index]) {
      return false;
    }
    live[index] = false;
    --remaining;
    return true;
  }

private:
  std::vector<std::string_view> keys;
  std::vector<bool> live;
  size_t remaining = 0;
};

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
 * Head and tail have the same meaning as in pg_log_t: the head is the
 * newest LogNode of a list and the tail is the oldest one. Each LogNode
 * points to the next older LogNode with its prev address, so a walk from
 * the head goes back in time and ends at the tail, whose prev address is
 * L_ADDR_NULL. New entries are always written to the head, either
 * appended or, for _fastinfo, overwritten in place.
 *
 * The ONode (omap_root_t::addr) points to the head of the pg_log_entry_t
 * list. That LogNode, in turn, holds the address of the head of the
 * pg_log_dup_t list (dup head).
 *
 * Layout:
 *
 *                +----------------+
 *                |     ONode      |
 *                +----------------+
 *                        |
 *                        v
 *        +----------------------------------+
 *    <-  | LogNode (latest pg_log_entry_t)  |  head of the log list
 *        +----------------------------------+
 *                        |
 *                        v
 *        +----------------------------------+
 *    <-  | LogNode (latest pg_log_dup_t)    |  head of the dup list
 *        +----------------------------------+
 *
 *    <-  : prev address, to the next older LogNode of the same list
 *
 * This separation allows efficient ranged updates while
 * avoiding full traversal of duplicate entries.
 */
class LogManager : public OMapManager {
public:
  LogManager(TransactionManager &tm);
  initialize_omap_ret initialize_omap(
    Transaction &t,
    laddr_hint_t hint,
    omap_type_t type) final;

  /**
   * omap_set_keys
   *
   *  Load the LOG head extent and, for each (key, value):
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
   *  - Fast path: if the current LOG node (head) has enough space for (key,value),
   *    get a mutable view within this transaction and append in place.
   *  - Split path: if appending would overflow the LOG node, allocate
   *  	a fresh LogNode extent, append the KV there, and link @head via
   *    prev_addr. For a normal log key, the new node becomes the new LOG
   *    head (update log_root) and inherits the dup head address. For a dup
   *    key, log_root does not change: the new node becomes the dup head,
   *    and the LOG head records its address.
   *
   * @param log_root  LOG root descriptor
   * @param t         Transaction context
   * @param head      Current append target
   * @param key       Key to set/append.
   * @param value     Value to set/append.
   *
   */
  omap_set_key_ret _log_set_key(omap_root_t &log_root,
    Transaction &t, LogNodeRef head, const std::string &key,
    const ceph::bufferlist &value);

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
   * For a single-block key, the search stops at the first (newest) match,
   * so callers must make sure that no duplicate copies of a non-log
   * (non-time-series) key are left in the chain. For a multi-block key,
   * every chunk node of the key is removed, including the chunks of older
   * multi-block copies, and the search continues until it removes a
   * single-block copy of the key or reaches the end of the chain.
   *
   * @param t     Transaction context.
   * @param dst   Logical address of the LogNode to start searching from.
   * @param key   The key to be removed.
   * @param next  The next newer LogNode, whose prev address is @dst
   *              (nullptr if @dst is the head).
   */

  omap_rm_key_ret remove_kv(Transaction &t, laddr_t dst, const std::string &key,
    LogNodeRef next);

  /**
   * remove_kv_set
   *
   * Batched form of remove_kv() for an arbitrary (non-contiguous) key set:
   * traverses the prev chain from @dst exactly once instead of once per key,
   * and stops as soon as every key has been accounted for. Removing K keys
   * with remove_kv() costs O(K * chain length) - and a miss always walks the
   * whole chain - which dominates large pgmeta transactions.
   *
   * Unlike remove_kvs(), this does not delete a key range: only keys present
   * in @keys are removed, so keys that merely sort between them are left
   * alone.
   *
   * @param t                Transaction context.
   * @param dst              Logical address of the LogNode to start from.
   * @param keys             In/out. Keys to remove; each key found and
   *                         removed is struck off, so on return the set holds
   *                         the keys that were not present.
   * @param multi_block_keys Out. Keys whose value spans several LogNodes.
   *                         These are not touched here; the caller must pass
   *                         each of them to remove_kv(), which knows how to
   *                         unlink an entire chunk chain.
   * @param next             The next newer LogNode, whose prev address is
   *                         @dst (nullptr if @dst is the head).
   */
  omap_rm_key_ret remove_kv_set(Transaction &t, laddr_t dst,
    pending_keys_t &keys,
    std::vector<std::string> &multi_block_keys,
    LogNodeRef next);

  /**
   * remove_kvs
   *
   * Starting at logical address dst, this loads a LogNode extent,
   * gathers entries in the [first, last] range, and decides
   * whether the current extent can be removed
   * If so, it removes the extent and fixes the prev address of
   * @next; if @next is nullptr (@dst is the head), the head is
   * reinitialized in place instead (see remove_node()). Then it
   * recurses to the next older extent, unless @first is a log key and
   * every log entry in this extent is older than @first.
   *
   * @param t       Transaction context.
   * @param dst     Logical address of the starting extent
   * @param first   lower key bound (optional).
   * @param last    upper key bound (optional). Empty string => unbounded.
   * @param next    The next newer LogNode, whose prev address is dst (used to
   *                fix the link when dst is removed). For the initial call at
   *                the head, pass nullptr.
   *
   * @return omap_rm_key_ret 
   */
  omap_rm_key_ret remove_kvs(Transaction &t, laddr_t dst,
    std::optional<std::string> first, 
    std::optional<std::string> last,
    LogNodeRef next);

  
  /**
   * remove_node
   *
   * If @next is nullptr, the node is the head of the chain. In this case,
   * the node is not physically removed; instead, it is re-initialized
   * while preserving its prev pointer (if any) and its dup head address.
   *
   * Otherwise, the node is physically removed from the transaction manager,
   * and the next newer node (@next) is updated to bypass the removed node
   * by inheriting its prev address.
   *
   * @param t     Transaction context.
   * @param mut   The LogNode to be removed or re-initialized.
   * @param next  The next newer LogNode, whose prev address is @mut
   *              (nullptr if @mut is the head).
   */
  LogManager::omap_rm_key_ret remove_node(Transaction &t,
    LogNodeRef mut,
    LogNodeRef next);

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
    Transaction &t, LogNodeRef head,
    const std::string &key, const ceph::bufferlist &value);


  TransactionManager &tm;
};

// length of eversion_t::get_key_name(), "%010u.%020llu"
constexpr size_t PG_LOG_KEY_LEN = 10 + 1 + 20;

inline bool is_log_key(std::string_view s) {
  return (s.size() == PG_LOG_KEY_LEN &&
      (s[0] >= (0 + '0') && s[0] <= (9 + '0'))) ||
      s.starts_with("dup_");
}

inline bool is_pg_log_key(const std::string &s) {
  return (s.size() == PG_LOG_KEY_LEN &&
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
