// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/collection_manager.h"

namespace crimson::os::seastore::collection_manager {
struct coll_context_t {
  TransactionManager &tm;
  Transaction &t;
};

using base_coll_map_t = std::map<denc_coll_t, uint32_t>;
struct coll_map_t : base_coll_map_t {
  auto insert(coll_t coll, unsigned bits) {
    return emplace(
      std::make_pair(denc_coll_t{coll}, bits)
    );
  }

  void update(coll_t coll, unsigned bits) {
    (*this)[denc_coll_t{coll}] = bits;
  }

  void remove(coll_t coll) {
    erase(denc_coll_t{coll});
  }
};

struct delta_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    UPDATE,
    REMOVE,
    INVALID
  } op = op_t::INVALID;

  denc_coll_t coll;
  uint32_t bits = 0;

  DENC(delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.coll, p);
    denc(v.bits, p);
    DENC_FINISH(p);
  }

  void replay(coll_map_t &l) const;
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::delta_t)

namespace crimson::os::seastore::collection_manager {
class delta_buffer_t {
  std::vector<delta_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }

  void insert(coll_t coll, uint32_t bits) {
    buffer.push_back(delta_t{delta_t::op_t::INSERT, denc_coll_t(coll), bits});
  }
  void update(coll_t coll, uint32_t bits) {
    buffer.push_back(delta_t{delta_t::op_t::UPDATE, denc_coll_t(coll), bits});
  }
  void remove(coll_t coll) {
    buffer.push_back(delta_t{delta_t::op_t::REMOVE, denc_coll_t(coll), 0});
  }
  void replay(coll_map_t &l) {
    for (auto &i: buffer) {
      i.replay(l);
    }
  }

  void clear() { buffer.clear(); }

  DENC(delta_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::delta_buffer_t)

namespace crimson::os::seastore::collection_manager {

struct CollectionNode
  : LogicalCachedExtent {
  using CollectionNodeRef = TCachedExtentRef<CollectionNode>;

  explicit CollectionNode(ceph::bufferptr &&ptr)
    : LogicalCachedExtent(std::move(ptr)) {}
  explicit CollectionNode(const CollectionNode &other)
    : LogicalCachedExtent(other),
      decoded(other.decoded) {}

  static constexpr extent_types_t type = extent_types_t::COLL_BLOCK;

  coll_map_t decoded;
  delta_buffer_t delta_buffer;

  CachedExtentRef duplicate_for_write(Transaction&) final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new CollectionNode(*this));
  }
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  using list_iertr = CollectionManager::list_iertr;
  using list_ret = CollectionManager::list_ret;
  list_ret list();


  enum class create_result_t : uint8_t {
    SUCCESS,
    OVERFLOW
  };
  using create_iertr = CollectionManager::create_iertr;
  using create_ret = create_iertr::future<create_result_t>;
  create_ret create(coll_context_t cc, coll_t coll, unsigned bits);

  using remove_iertr = CollectionManager::remove_iertr;
  using remove_ret = CollectionManager::remove_ret;
  remove_ret remove(coll_context_t cc, coll_t coll);

  using update_iertr = CollectionManager::update_iertr;
  using update_ret = CollectionManager::update_ret;
  update_ret update(coll_context_t cc, coll_t coll, unsigned bits);

  void on_clean_read() final {
    bufferlist bl;
    bl.append(get_bptr());
    auto iter = bl.cbegin();
    decode((base_coll_map_t&)decoded, iter);
  }

  void copy_to_node() {
    bufferlist bl;
    encode((base_coll_map_t&)decoded, bl);
    auto iter = bl.begin();
    auto size = encoded_sizeof((base_coll_map_t&)decoded);
    assert(size <= get_bptr().length());
    get_bptr().zero();
    iter.copy(size, get_bptr().c_str());

  }

  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    // FIXME: CollectionNodes are always first mutated and
    // 	      then checked whether they have enough space,
    // 	      and if not, new ones will be created and the
    // 	      mutation_pending ones are left untouched.
    //
    // 	      The above order should be reversed, nodes should
    // 	      be mutated only if there are enough space for new
    // 	      entries.
    if (!delta_buffer.empty()) {
      encode(delta_buffer, bl);
      delta_buffer.clear();
    }
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_buffer_t buffer;
    auto bptr = bl.begin();
    decode(buffer, bptr);
    buffer.replay(decoded);
    copy_to_node();
  }

  static constexpr extent_types_t TYPE = extent_types_t::COLL_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  std::ostream &print_detail_l(std::ostream &out) const final;
};
using CollectionNodeRef = CollectionNode::CollectionNodeRef;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::collection_manager::CollectionNode> : fmt::ostream_formatter {};
#endif
