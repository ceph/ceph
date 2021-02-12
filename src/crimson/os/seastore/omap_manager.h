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

struct list_keys_result_t {
  std::vector<std::string> keys;
  std::optional<std::string> next;  // if return std::nullopt, means list all keys in omap tree.
};

struct list_kvs_result_t {
  std::vector<std::pair<std::string, std::string>> kvs;
  std::optional<std::string> next;  // if return std::nullopt, means list all kv mapping in omap tree.
};
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
  using omap_get_value_ret = omap_get_value_ertr::future<std::pair<std::string, std::string>>;
  virtual omap_get_value_ret omap_get_value(const omap_root_t &omap_root, Transaction &t,
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
  virtual omap_set_key_ret omap_set_key(omap_root_t &omap_root, Transaction &t,
		                        const std::string &key, const std::string &value) = 0;

  /**
   * remove key value mapping in omap tree
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &key, omap string key
   */
  using omap_rm_key_ertr = base_ertr;
  using omap_rm_key_ret = omap_rm_key_ertr::future<>;
  virtual omap_rm_key_ret omap_rm_key(omap_root_t &omap_root, Transaction &t,
		                                    const std::string &key) = 0;

  /**
   * get all keys or partial keys in omap tree
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &start, the list keys range begin from start,
   *        if start is "", list from the first omap key
   * @param max_result_size, the number of list keys,
   *        it it is not set, list all keys after string start
   * @retval list_keys_result_t, listed keys and next key
   */
  using omap_list_keys_ertr = base_ertr;
  using omap_list_keys_ret = omap_list_keys_ertr::future<list_keys_result_t>;
  virtual omap_list_keys_ret omap_list_keys(const omap_root_t &omap_root, Transaction &t,
                             std::string &start,
                             size_t max_result_size = MAX_SIZE) = 0;

  /**
   * Get all or partial key-> value mapping in omap tree
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &start, the list keys range begin from start,
   *        if start is "" , list from the first omap key
   * @param max_result_size, the number of list keys,
   *        it it is not set, list all keys after string start.
   * @retval list_kvs_result_t, listed key->value mapping and next key.
   */
  using omap_list_ertr = base_ertr;
  using omap_list_ret = omap_list_ertr::future<list_kvs_result_t>;
  virtual omap_list_ret omap_list(const omap_root_t &omap_root, Transaction &t,
                                  std::string &start,
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
