// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "crimson/osd/exceptions.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

#define OMAP_BLOCK_SIZE 4096

namespace crimson::os::seastore {

constexpr size_t MAX_SIZE = std::numeric_limits<size_t>::max();
std::ostream &operator<<(std::ostream &out, const std::list<std::string> &rhs);
std::ostream &operator<<(std::ostream &out, const std::map<std::string, std::string> &rhs);

class OMapManager {
 /* all OMapManager API use reference to transfer input string parameters,
  * the upper caller should guarantee the referenced string values alive (not freed)
  * until these functions future resolved.
  */
public:
  using base_ertr = TransactionManager::base_ertr;

  /**
   * allocate omap tree root node
   *
   * @param Transaction &t, current transaction
   * @retval return the omap_root_t structure.
   */
  using initialize_omap_ertr = base_ertr;
  using initialize_omap_ret = initialize_omap_ertr::future<omap_root_t>;
  virtual initialize_omap_ret initialize_omap(Transaction &t) = 0;

  /**
   * get value(string) by key(string)
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &key, omap string key
   * @retval return string key->string value mapping pair.
   */
  using omap_get_value_ertr = base_ertr;
  using omap_get_value_ret = omap_get_value_ertr::future<
    std::optional<bufferlist>>;
  virtual omap_get_value_ret omap_get_value(
    const omap_root_t &omap_root,
    Transaction &t,
    const std::string &key) = 0;

  /**
   * set key value mapping in omap
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &key, omap string key
   * @param string &value, mapped value corresponding key
   */
  using omap_set_key_ertr = base_ertr;
  using omap_set_key_ret = omap_set_key_ertr::future<>;
  virtual omap_set_key_ret omap_set_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key,
    const ceph::bufferlist &value) = 0;

  /**
   * remove key value mapping in omap tree
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &key, omap string key
   */
  using omap_rm_key_ertr = base_ertr;
  using omap_rm_key_ret = omap_rm_key_ertr::future<>;
  virtual omap_rm_key_ret omap_rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key) = 0;

  /**
   * Ordered scan of key-> value mapping in omap tree
   *
   * @param omap_root: omap btree root information
   * @param t: current transaction
   * @param start: the list keys range begin > start if present,
   *        at beginning if std::nullopt
   * @param max_result_size: the number of list keys,
   *        it it is not set, list all keys after string start.
   * @retval listed key->value mapping and next key
   */
  using omap_list_ertr = base_ertr;
  using omap_list_bare_ret = std::pair<
    bool,
    std::map<std::string, bufferlist>>;
  using omap_list_ret = omap_list_ertr::future<omap_list_bare_ret>;
  virtual omap_list_ret omap_list(
    const omap_root_t &omap_root,
    Transaction &t,
    const std::optional<std::string> &start,
    size_t max_result_size = MAX_SIZE) = 0;

  /**
   * clear all omap tree key->value mapping
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   */
  using omap_clear_ertr = base_ertr;
  using omap_clear_ret = omap_clear_ertr::future<>;
  virtual omap_clear_ret omap_clear(omap_root_t &omap_root, Transaction &t) = 0;

  virtual ~OMapManager() {}
};
using OMapManagerRef = std::unique_ptr<OMapManager>;

namespace omap_manager {

OMapManagerRef create_omap_manager (
  TransactionManager &trans_manager);
}

}
