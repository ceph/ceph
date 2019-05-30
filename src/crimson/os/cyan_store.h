// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <optional>
#include <seastar/core/future.hh>

#include "crimson/os/cyan_collection.h"
#include "osd/osd_types.h"
#include "include/uuid.h"

namespace ceph::os {

class Collection;
class Transaction;

// a just-enough store for reading/writing the superblock
class CyanStore {
  constexpr static unsigned MAX_KEYS_PER_OMAP_GET_CALL = 32;

  const std::string path;
  std::unordered_map<coll_t, CollectionRef> coll_map;
  std::map<coll_t,CollectionRef> new_coll_map;
  uint64_t used_bytes = 0;
  uuid_d osd_fsid;

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

  CyanStore(const std::string& path);
  ~CyanStore();

  seastar::future<> mount();
  seastar::future<> umount();

  seastar::future<> mkfs();
  seastar::future<ceph::bufferlist> read(CollectionRef c,
				   const ghobject_t& oid,
				   uint64_t offset,
				   size_t len,
				   uint32_t op_flags = 0);
  seastar::future<ceph::bufferptr> get_attr(CollectionRef c,
					    const ghobject_t& oid,
					    std::string_view name);
  using attrs_t = std::map<std::string, ceph::bufferptr, std::less<>>;
  seastar::future<attrs_t> get_attrs(CollectionRef c, const ghobject_t& oid);

  using omap_values_t = std::map<std::string,ceph::bufferlist, std::less<>>;
  seastar::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    std::vector<std::string>&& keys);

  seastar::future<std::vector<ghobject_t>, ghobject_t> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit);

  /// Retrieves paged set of values > start (if present)
  seastar::future<bool, omap_values_t> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ); ///< @return <done, values> values.empty() iff done

  CollectionRef create_new_collection(const coll_t& cid);
  CollectionRef open_collection(const coll_t& cid);
  std::vector<coll_t> list_collections();

  seastar::future<> do_transaction(CollectionRef ch,
				   Transaction&& txn);

  void write_meta(const std::string& key,
		  const std::string& value);
  int read_meta(const std::string& key, std::string* value);
  uuid_d get_fsid() const;

private:
  int _remove(const coll_t& cid, const ghobject_t& oid);
  int _touch(const coll_t& cid, const ghobject_t& oid);
  int _write(const coll_t& cid, const ghobject_t& oid,
	     uint64_t offset, size_t len, const ceph::bufferlist& bl,
	     uint32_t fadvise_flags);
  int _omap_set_values(
    const coll_t& cid,
    const ghobject_t& oid,
    std::map<std::string, ceph::bufferlist> &&aset);
  int _omap_set_header(
    const coll_t& cid,
    const ghobject_t& oid,
    const ceph::bufferlist &header);
  int _omap_rmkeys(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::set<std::string> &aset);
  int _omap_rmkeyrange(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string &first,
    const std::string &last);
  int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _setattrs(const coll_t& cid, const ghobject_t& oid,
                std::map<std::string,bufferptr>& aset);
  int _create_collection(const coll_t& cid, int bits);
};

}
