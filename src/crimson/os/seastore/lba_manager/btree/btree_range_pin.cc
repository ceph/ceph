// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include "crimson/os/seastore/lba_manager/btree/btree_range_pin.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

void btree_range_pin_t::take_pin(btree_range_pin_t &other)
{
  ceph_assert(other.extent);
  ceph_assert(other.pins);
  other.pins->replace_pin(*this, other);
  pins = other.pins;
  other.pins = nullptr;

  if (other.has_ref()) {
    other.drop_ref();
    acquire_ref();
  }
}

btree_range_pin_t::~btree_range_pin_t()
{
  ceph_assert(!pins == !is_linked());
  ceph_assert(!ref);
  if (pins) {
    logger().debug("{}: removing {}", __func__, *this);
    pins->remove_pin(*this, true);
  }
  extent = nullptr;
}

void btree_pin_set_t::replace_pin(btree_range_pin_t &to, btree_range_pin_t &from)
{
  pins.replace_node(pins.iterator_to(from), to);
}

void btree_pin_set_t::remove_pin(btree_range_pin_t &pin, bool do_check_parent)
{
  logger().debug("{}: {}", __func__, pin);
  ceph_assert(pin.is_linked());
  ceph_assert(pin.pins);
  ceph_assert(!pin.ref);

  pins.erase(pin);
  pin.pins = nullptr;

  if (do_check_parent) {
    check_parent(pin);
  }
}

btree_range_pin_t *btree_pin_set_t::maybe_get_parent(
  const lba_node_meta_t &meta)
{
  auto cmeta = meta;
  cmeta.depth++;
  auto iter = pins.upper_bound(cmeta, btree_range_pin_t::meta_cmp_t());
  if (iter == pins.begin()) {
    return nullptr;
  } else {
    --iter;
    if (iter->range.is_parent_of(meta)) {
      return &*iter;
    } else {
      return nullptr;
    }
  }
}

const btree_range_pin_t *btree_pin_set_t::maybe_get_first_child(
  const lba_node_meta_t &meta) const
{
  if (meta.depth == 0) {
    return nullptr;
  }

  auto cmeta = meta;
  cmeta.depth--;

  auto iter = pins.lower_bound(cmeta, btree_range_pin_t::meta_cmp_t());
  if (iter == pins.end()) {
    return nullptr;
  } else if (meta.is_parent_of(iter->range)) {
    return &*iter;
  } else {
    return nullptr;
  }
}

void btree_pin_set_t::release_if_no_children(btree_range_pin_t &pin)
{
  ceph_assert(pin.is_linked());
  if (maybe_get_first_child(pin.range) == nullptr) {
    pin.drop_ref();
  }
}

void btree_pin_set_t::add_pin(btree_range_pin_t &pin)
{
  ceph_assert(!pin.is_linked());
  ceph_assert(!pin.pins);
  ceph_assert(!pin.ref);

  auto [prev, inserted] = pins.insert(pin);
  if (!inserted) {
    logger().error("{}: unable to add {}, found {}", __func__, pin, *prev);
    ceph_assert(0 == "impossible");
    return;
  }
  pin.pins = this;
  if (!pin.is_root()) {
    auto *parent = maybe_get_parent(pin.range);
    ceph_assert(parent);
    if (!parent->has_ref()) {
      logger().debug("{}: acquiring parent {}", __func__,
		     static_cast<void*>(parent));
      parent->acquire_ref();
    } else {
      logger().debug("{}: parent has ref {}", __func__,
		     static_cast<void*>(parent));
    }
  }
  if (maybe_get_first_child(pin.range) != nullptr) {
    logger().debug("{}: acquiring self {}", __func__, pin);
    pin.acquire_ref();
  }
}

void btree_pin_set_t::retire(btree_range_pin_t &pin)
{
  pin.drop_ref();
  remove_pin(pin, false);
}

void btree_pin_set_t::check_parent(btree_range_pin_t &pin)
{
  auto parent = maybe_get_parent(pin.range);
  if (parent) {
    logger().debug("{}: releasing parent {}", __func__, *parent);
    release_if_no_children(*parent);
  }
}

}
