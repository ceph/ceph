// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "block_driver.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

class FSDriver final : public BlockDriver {
public:
  FSDriver(config_t config)
  : config(config)
  {}
  ~FSDriver() final {}

  bufferptr get_buffer(size_t size) final {
    return ceph::buffer::create_page_aligned(size);
  }

  seastar::future<> write(
    off_t offset,
    bufferptr ptr) final;

  seastar::future<bufferlist> read(
    off_t offset,
    size_t size) final;

  size_t get_size() const {
    return size;
  }

  seastar::future<> mount() final;

  seastar::future<> close() final;

private:
  size_t size = 0;
  const config_t config;
  std::unique_ptr<crimson::os::FuturizedStore> fs;
  crimson::os::FuturizedStore::Shard* sharded_fs;

  struct pg_analogue_t {
    crimson::os::CollectionRef collection;

    ghobject_t log_object;
    unsigned log_tail = 0;
    unsigned log_head = 0;
  };
  std::map<unsigned, pg_analogue_t> collections;

  struct offset_mapping_t {
    pg_analogue_t &pg;
    ghobject_t object;
    off_t offset;
  };
  offset_mapping_t map_offset(off_t offset);

  seastar::future<> mkfs();
  seastar::future<> init();

  friend void populate_log(
    ceph::os::Transaction &,
    pg_analogue_t &,
    unsigned,
    unsigned);

  friend void update_log(
    ceph::os::Transaction &,
    FSDriver::pg_analogue_t &,
    unsigned,
    unsigned);
};
