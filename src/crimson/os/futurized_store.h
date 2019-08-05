// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <seastar/core/future.hh>

#include "include/buffer_fwd.h"
#include "include/uuid.h"
#include "osd/osd_types.h"

namespace ceph::os {

class Collection;
class Transaction;

class FuturizedStore {

public:
  template <class ConcreteExceptionT>
  class Exception : public std::logic_error {
  public:
    using std::logic_error::logic_error;

    // Throwing an exception isn't the sole way to signalize an error
    // with it. This approach nicely fits cold, infrequent issues but
    // when applied to a hot one (like ENOENT on write path), it will
    // likely hurt performance.
    // Alternative approach for hot errors is to create exception_ptr
    // on our own and place it in the future via make_exception_future.
    // When ::handle_exception is called, handler would inspect stored
    // exception whether it's hot-or-cold before rethrowing it.
    // The main advantage is both types flow through very similar path
    // based on future::handle_exception.
    static bool is_class_of(const std::exception_ptr& ep) {
      // Seastar offers hacks for making throwing lock-less but stack
      // unwinding still can be a problem so painful to justify going
      // with non-standard, obscure things like this one.
      return *ep.__cxa_exception_type() == typeid(ConcreteExceptionT);
    }
  };

  struct EnoentException : public Exception<EnoentException> {
    using Exception<EnoentException>::Exception;
  };
  static std::unique_ptr<FuturizedStore> create(const std::string& type,
                                                const std::string& data);
  FuturizedStore() = default;
  virtual ~FuturizedStore() = default;

  // no copying
  explicit FuturizedStore(const FuturizedStore& o) = delete;
  const FuturizedStore& operator=(const FuturizedStore& o) = delete;

  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> umount() = 0;

  virtual seastar::future<> mkfs(uuid_d new_osd_fsid) = 0;
  virtual store_statfs_t stat() const = 0;

  using CollectionRef = boost::intrusive_ptr<Collection>;
  virtual seastar::future<ceph::bufferlist> read(CollectionRef c,
				   const ghobject_t& oid,
				   uint64_t offset,
				   size_t len,
				   uint32_t op_flags = 0) = 0;
  virtual seastar::future<ceph::bufferptr> get_attr(CollectionRef c,
					    const ghobject_t& oid,
					    std::string_view name) = 0;

  using attrs_t = std::map<std::string, ceph::bufferptr, std::less<>>;
  virtual seastar::future<attrs_t> get_attrs(CollectionRef c,
                                             const ghobject_t& oid) = 0;
  using omap_values_t = std::map<std::string, bufferlist, std::less<>>;
  using omap_keys_t = std::set<std::string>;
  virtual seastar::future<omap_values_t> omap_get_values(
                                         CollectionRef c,
                                         const ghobject_t& oid,
                                         const omap_keys_t& keys) = 0;
  virtual seastar::future<std::vector<ghobject_t>, ghobject_t> list_objects(
                                         CollectionRef c,
                                         const ghobject_t& start,
                                         const ghobject_t& end,
                                         uint64_t limit) = 0;
  virtual seastar::future<bool, omap_values_t> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) = 0; ///< @return <done, values> values.empty() iff done

  virtual CollectionRef create_new_collection(const coll_t& cid) = 0;
  virtual CollectionRef open_collection(const coll_t& cid) = 0;
  virtual std::vector<coll_t> list_collections() = 0;

  virtual seastar::future<> do_transaction(CollectionRef ch,
				   Transaction&& txn) = 0;

  virtual void write_meta(const std::string& key,
		  const std::string& value) = 0;
  virtual int read_meta(const std::string& key, std::string* value) = 0;
  virtual uuid_d get_fsid() const  = 0;
  virtual unsigned get_max_attr_name_length() const = 0;
};

}
