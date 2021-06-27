// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "os/Transaction.h"
#include "fs_driver.h"

using namespace crimson;
using namespace crimson::os;

coll_t get_coll(unsigned num) {
  return coll_t(spg_t(pg_t(0, num)));
}

FSDriver::offset_mapping_t FSDriver::map_offset(off_t offset)
{
  uint32_t objid = offset / config.object_size;
  uint32_t collid = objid % config.num_collections;
  return offset_mapping_t{
    collections[collid],
    ghobject_t(
      shard_id_t::NO_SHARD,
      0,
      (objid << 16) | collid,
      "",
      "",
      0,
      ghobject_t::NO_GEN),
    offset % config.object_size
  };
}

seastar::future<> FSDriver::write(
  off_t offset,
  bufferptr ptr)
{
  auto mapping = map_offset(offset);
  ceph_assert(mapping.offset + ptr.length() <= config.object_size);
  ceph::os::Transaction t;
  bufferlist bl;
  bl.append(ptr);
  t.write(
    mapping.chandle->get_cid(), 
    mapping.object,
    mapping.offset,
    ptr.length(),
    bl,
    0);
  return fs->do_transaction(
    mapping.chandle,
    std::move(t));
}

seastar::future<bufferlist> FSDriver::read(
  off_t offset,
  size_t size)
{
  auto mapping = map_offset(offset);
  ceph_assert((mapping.offset + size) <= config.object_size);
  return fs->read(
    mapping.chandle,
    mapping.object,
    mapping.offset,
    size,
    0
  ).handle_error(
    crimson::ct_error::enoent::handle([size](auto &e) {
      bufferlist bl;
      bl.append_zero(size);
      return seastar::make_ready_future<bufferlist>(std::move(bl));
    }),
    crimson::ct_error::assert_all{"Unrecoverable error in FSDriver::read"}
  ).then([this, size](auto &&bl) {
    if (bl.length() < size) {
      bl.append_zero(size - bl.length());
    }
    return seastar::make_ready_future<bufferlist>(std::move(bl));
  });
}

seastar::future<> FSDriver::mkfs()
{
  init();
  assert(fs);
  return fs->start(
  ).then([this] {
    uuid_d uuid;
    uuid.generate_random();
    return fs->mkfs(uuid);
  }).then([this] {
    return fs->stop();
  }).then([this] {
    init();
    return fs->start();
  }).then([this] {
    return fs->mount();
  }).then([this] {
    return seastar::do_for_each(
      boost::counting_iterator<unsigned>(0),
      boost::counting_iterator<unsigned>(config.num_collections),
      [this](auto i) {
	return fs->create_new_collection(get_coll(i)
	).then([this, i](auto coll) {
	  ceph::os::Transaction t;
	  t.create_collection(get_coll(i), 0);
	  return fs->do_transaction(coll, std::move(t));
	});
      });
  }).then([this] {
    return fs->umount();
  }).then([this] {
    return fs->stop();
  }).then([this] {
    fs.reset();
    return seastar::now();
  });
}

seastar::future<> FSDriver::mount()
{
  ceph_assert(config.path);
  return (
    config.mkfs ? mkfs() : seastar::now()
  ).then([this] {
    init();
    return fs->start();
  }).then([this] {
    return fs->mount();
  }).then([this] {
    return seastar::do_for_each(
      boost::counting_iterator<unsigned>(0),
      boost::counting_iterator<unsigned>(config.num_collections),
      [this](auto i) {
	return fs->open_collection(get_coll(i)
	).then([this, i](auto ref) {
	  collections[i] = ref;
	  return seastar::now();
	});
      });
  }).then([this] {
    return fs->stat();
  }).then([this](auto s) {
    size = s.total;
  });
};

seastar::future<> FSDriver::close()
{
  collections.clear();
  return fs->umount(
  ).then([this] {
    return fs->stop();
  }).then([this] {
    fs.reset();
    return seastar::now();
  });
}

void FSDriver::init()
{
  fs.reset();
  fs = FuturizedStore::create(
    config.get_fs_type(),
    *config.path,
    crimson::common::local_conf().get_config_values());
}
