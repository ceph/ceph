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

//TODO: calculate the max key and value sizes the current layout supports,
//	and return errors during insert if the max is exceeded.
#define OMAP_INNER_BLOCK_SIZE 8192
#define OMAP_LEAF_BLOCK_SIZE 65536

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const std::list<std::string> &rhs);
std::ostream &operator<<(std::ostream &out, const std::map<std::string, std::string> &rhs);

class OMapManager {
 /* all OMapManager API use reference to transfer input string parameters,
  * the upper caller should guarantee the referenced string values alive (not freed)
  * until these functions future resolved.
  */
public:
  using base_iertr = TransactionManager::base_iertr;

  /**
   * allocate omap tree root node
   *
   * @param Transaction &t, current transaction
   * @retval return the omap_root_t structure.
   */
  using initialize_omap_iertr = base_iertr;
  using initialize_omap_ret = initialize_omap_iertr::future<omap_root_t>;
  virtual initialize_omap_ret initialize_omap(Transaction &t, laddr_t hint) = 0;

  /**
   * get value(string) by key(string)
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &key, omap string key
   * @retval return string key->string value mapping pair.
   */
  using omap_get_value_iertr = base_iertr;
  using omap_get_value_ret = omap_get_value_iertr::future<
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
  using omap_set_key_iertr = base_iertr;
  using omap_set_key_ret = omap_set_key_iertr::future<>;
  virtual omap_set_key_ret omap_set_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key,
    const ceph::bufferlist &value) = 0;

  using omap_set_keys_iertr = base_iertr;
  using omap_set_keys_ret = omap_set_keys_iertr::future<>;
  virtual omap_set_keys_ret omap_set_keys(
    omap_root_t &omap_root,
    Transaction &t,
    std::map<std::string, ceph::bufferlist>&& keys) = 0;

  /**
   * remove key value mapping in omap tree
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &key, omap string key
   */
  using omap_rm_key_iertr = base_iertr;
  using omap_rm_key_ret = omap_rm_key_iertr::future<>;
  virtual omap_rm_key_ret omap_rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key) = 0;

  /**
   * omap_list
   *
   * Scans key/value pairs in order.
   *
   * @param omap_root: omap btree root information
   * @param t: current transaction
   * @param first: range start, nullopt sorts before any string,
   * 	    behavior based on config.inclusive,
   * 	    must alive during the call
   * @param last: range end, nullopt sorts after any string,
   * 	    behavior based on config.inclusive,
   * 	    must alive during the call
   * @param config: see below for params
   * @retval listed key->value and bool indicating complete
   */
  struct omap_list_config_t {
    /// max results to return
    size_t max_result_size = 128;

    /// true denotes behavior like lower_bound, upper_bound otherwise
    /// range start behavior
    bool first_inclusive = false;
    /// range end behavior
    bool last_inclusive = false;

    omap_list_config_t(
      size_t max_result_size,
      bool first_inclusive,
      bool last_inclusive)
      : max_result_size(max_result_size),
	first_inclusive(first_inclusive),
	last_inclusive(last_inclusive) {}
    omap_list_config_t() {}
    omap_list_config_t(const omap_list_config_t &) = default;
    omap_list_config_t(omap_list_config_t &&) = default;
    omap_list_config_t &operator=(const omap_list_config_t &) = default;
    omap_list_config_t &operator=(omap_list_config_t &&) = default;

    auto with_max(size_t max) {
      this->max_result_size = max;
      return *this;
    }

    auto without_max() {
      this->max_result_size = std::numeric_limits<size_t>::max();
      return *this;
    }

    auto with_inclusive(
      bool first_inclusive,
      bool last_inclusive) {
      this->first_inclusive = first_inclusive;
      this->last_inclusive = last_inclusive;
      return *this;
    }

    auto with_reduced_max(size_t reduced_by) const {
      assert(reduced_by <= max_result_size);
      return omap_list_config_t(
	max_result_size - reduced_by,
	first_inclusive,
	last_inclusive);
    }
  };
  using omap_list_iertr = base_iertr;
  using omap_list_bare_ret = std::tuple<
    bool,
    std::map<std::string, bufferlist, std::less<>>>;
  using omap_list_ret = omap_list_iertr::future<omap_list_bare_ret>;
  virtual omap_list_ret omap_list(
    const omap_root_t &omap_root,
    Transaction &t,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    omap_list_config_t config = omap_list_config_t()) = 0;

  /**
   * remove key value mappings in a key range from omap tree
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   * @param string &first, range start
   * @param string &last, range end
   */
  using omap_rm_key_range_iertr = base_iertr;
  using omap_rm_key_range_ret = omap_rm_key_range_iertr::future<>;
  virtual omap_rm_key_range_ret omap_rm_key_range(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &first,
    const std::string &last,
    omap_list_config_t config) = 0;

  /**
   * clear all omap tree key->value mapping
   *
   * @param omap_root_t &omap_root,  omap btree root information
   * @param Transaction &t,  current transaction
   */
  using omap_clear_iertr = base_iertr;
  using omap_clear_ret = omap_clear_iertr::future<>;
  virtual omap_clear_ret omap_clear(omap_root_t &omap_root, Transaction &t) = 0;

  virtual ~OMapManager() {}
};
using OMapManagerRef = std::unique_ptr<OMapManager>;

namespace omap_manager {

OMapManagerRef create_omap_manager (
  TransactionManager &trans_manager);
}

}
