// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include <seastar/core/future.hh>
#include "osd/osd_types.h"
#include "include/uuid.h"

namespace ceph::os {

class Collection;
class Transaction;

// a just-enough store for reading/writing the superblock
class CyanStore {
  using CollectionRef = boost::intrusive_ptr<Collection>;
  const std::string path;
  std::unordered_map<coll_t, CollectionRef> coll_map;
  std::map<coll_t,CollectionRef> new_coll_map;
  uint64_t used_bytes = 0;

public:
  CyanStore(const std::string& path);
  ~CyanStore();

  seastar::future<> mount();
  seastar::future<> umount();

  seastar::future<> mkfs(uuid_d osd_fsid);
  seastar::future<bufferlist> read(CollectionRef c,
				   const ghobject_t& oid,
				   uint64_t offset,
				   size_t len,
				   uint32_t op_flags = 0);
  using omap_values_t = std::map<std::string,bufferlist, std::less<>>;
  seastar::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    std::vector<std::string>&& keys);
  CollectionRef create_new_collection(const coll_t& cid);
  CollectionRef open_collection(const coll_t& cid);
  std::vector<coll_t> list_collections();

  seastar::future<> do_transaction(CollectionRef ch,
				   Transaction&& txn);

  void write_meta(const std::string& key,
		  const std::string& value);
  int read_meta(const std::string& key, std::string* value);

private:
  int _write(const coll_t& cid, const ghobject_t& oid,
	     uint64_t offset, size_t len, const bufferlist& bl,
	     uint32_t fadvise_flags);
  int _create_collection(const coll_t& cid, int bits);
};

}
