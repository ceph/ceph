// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>

#include "include/buffer.h"
#include "crimson/common/type_helpers.h"

#include "fwd.h"
#include "node_extent_mutable.h"

namespace crimson::os::seastore::onode {

// value size up to 64 KiB
using value_size_t = uint16_t;
enum class value_magic_t : uint8_t {
  ONODE = 0x52,
  TEST_UNBOUND,
  TEST_BOUNDED,
  TEST_EXTENDED,
};
inline std::ostream& operator<<(std::ostream& os, const value_magic_t& magic) {
  switch (magic) {
  case value_magic_t::ONODE:
    return os << "ONODE";
  case value_magic_t::TEST_UNBOUND:
    return os << "TEST_UNBOUND";
  case value_magic_t::TEST_BOUNDED:
    return os << "TEST_BOUNDED";
  case value_magic_t::TEST_EXTENDED:
    return os << "TEST_EXTENDED";
  default:
    return os << "UNKNOWN(" << magic << ")";
  }
}

/**
 * value_config_t
 *
 * Parameters to create a value.
 */
struct value_config_t {
  value_magic_t magic;
  value_size_t payload_size;

  value_size_t allocation_size() const;

  void encode(ceph::bufferlist& encoded) const {
    ceph::encode(magic, encoded);
    ceph::encode(payload_size, encoded);
  }

  static value_config_t decode(ceph::bufferlist::const_iterator& delta) {
    value_magic_t magic;
    ceph::decode(magic, delta);
    value_size_t payload_size;
    ceph::decode(payload_size, delta);
    return {magic, payload_size};
  }
};
inline std::ostream& operator<<(std::ostream& os, const value_config_t& conf) {
  return os << "ValueConf(" << conf.magic
            << ", " << conf.payload_size << "B)";
}

/**
 * value_header_t
 *
 * The header structure in value layout.
 *
 * Value layout:
 *
 * # <- alloc size -> #
 * # header | payload #
 */
struct value_header_t {
  value_magic_t magic;
  value_size_t payload_size;

  bool operator==(const value_header_t& rhs) const {
    return (magic == rhs.magic && payload_size == rhs.payload_size);
  }
  bool operator!=(const value_header_t& rhs) const {
    return !(*this == rhs);
  }

  value_size_t allocation_size() const {
    return payload_size + sizeof(value_header_t);
  }

  const char* get_payload() const {
    return reinterpret_cast<const char*>(this) + sizeof(value_header_t);
  }

  NodeExtentMutable get_payload_mutable(NodeExtentMutable& node) const {
    return node.get_mutable_absolute(get_payload(), payload_size);
  }

  char* get_payload() {
    return reinterpret_cast<char*>(this) + sizeof(value_header_t);
  }

  void initiate(NodeExtentMutable& mut, const value_config_t& config) {
    value_header_t header{config.magic, config.payload_size};
    mut.copy_in_absolute(this, header);
    mut.set_absolute(get_payload(), 0, config.payload_size);
  }

  static value_size_t estimate_allocation_size(value_size_t payload_size) {
    return payload_size + sizeof(value_header_t);
  }
} __attribute__((packed));
inline std::ostream& operator<<(std::ostream& os, const value_header_t& header) {
  return os << "Value(" << header.magic
            << ", " << header.payload_size << "B)";
}

inline value_size_t value_config_t::allocation_size() const {
  return value_header_t::estimate_allocation_size(payload_size);
}

/**
 * ValueDeltaRecorder
 *
 * An abstracted class to handle user-defined value delta encode, decode and
 * replay.
 */
class ValueDeltaRecorder {
 public:
  virtual ~ValueDeltaRecorder() = default;
  ValueDeltaRecorder(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder(ValueDeltaRecorder&&) = delete;
  ValueDeltaRecorder& operator=(const ValueDeltaRecorder&) = delete;
  ValueDeltaRecorder& operator=(ValueDeltaRecorder&&) = delete;

  /// Returns the value header magic for validation purpose.
  virtual value_magic_t get_header_magic() const = 0;

  /// Called by DeltaRecorderT to apply user-defined value delta.
  virtual void apply_value_delta(ceph::bufferlist::const_iterator&,
                                 NodeExtentMutable&,
                                 laddr_t,
                                 node_offset_t) = 0;

 protected:
  ValueDeltaRecorder(ceph::bufferlist& encoded) : encoded{encoded} {}

  /// Get the delta buffer to encode user-defined value delta.
  ceph::bufferlist& get_encoded(NodeExtentMutable&);

 private:
  ceph::bufferlist& encoded;
};

/**
 * tree_conf_t
 *
 * Hard limits and compile-time configurations.
 */
struct tree_conf_t {
  value_magic_t value_magic;
  string_size_t max_ns_size;
  string_size_t max_oid_size;
  value_size_t max_value_payload_size;
  extent_len_t internal_node_size;
  extent_len_t leaf_node_size;
  bool do_split_check = true;
};

class tree_cursor_t;
/**
 * Value
 *
 * Value is a stateless view of the underlying value header and payload content
 * stored in a tree leaf node, with the support to implement user-defined value
 * deltas and to extend and trim the underlying payload data (not implemented
 * yet).
 *
 * In the current implementation, we don't guarantee any alignment for value
 * payload due to unaligned node layout and the according merge and split
 * operations.
 */
class Value {
 public:
  virtual ~Value();
  Value(const Value&) = default;
  Value(Value&&) = default;
  Value& operator=(const Value&) = delete;
  Value& operator=(Value&&) = delete;

  /// Returns whether the Value is still tracked in tree.
  bool is_tracked() const;

  /// Invalidate the Value before submitting transaction.
  void invalidate();

  /// Returns the value payload size.
  value_size_t get_payload_size() const {
    assert(is_tracked());
    return read_value_header()->payload_size;
  }

  laddr_t get_data_hint() const;

  bool operator==(const Value& v) const { return p_cursor == v.p_cursor; }
  bool operator!=(const Value& v) const { return !(*this == v); }

 protected:
  Value(NodeExtentManager&, const ValueBuilder&, Ref<tree_cursor_t>&);

  /// Extends the payload size.
  eagain_ifuture<> extend(Transaction&, value_size_t extend_size);

  /// Trim and shrink the payload.
  eagain_ifuture<> trim(Transaction&, value_size_t trim_size);

  /// Get the permission to mutate the payload with the optional value recorder.
  template <typename PayloadT, typename ValueDeltaRecorderT>
  std::pair<NodeExtentMutable&, ValueDeltaRecorderT*>
  prepare_mutate_payload(Transaction& t) {
    assert(is_tracked());
    assert(sizeof(PayloadT) <= get_payload_size());

    auto value_mutable = do_prepare_mutate_payload(t);
    assert(value_mutable.first.get_write() ==
           const_cast<const Value*>(this)->template read_payload<char>());
    assert(value_mutable.first.get_length() == get_payload_size());
    return {value_mutable.first,
            static_cast<ValueDeltaRecorderT*>(value_mutable.second)};
  }

  /// Get the latest payload pointer for read.
  template <typename PayloadT>
  const PayloadT* read_payload() const {
    assert(is_tracked());
    // see Value documentation
    static_assert(alignof(PayloadT) == 1);
    assert(sizeof(PayloadT) <= get_payload_size());
    return reinterpret_cast<const PayloadT*>(read_value_header()->get_payload());
  }

 private:
  const value_header_t* read_value_header() const;
  context_t get_context(Transaction& t) {
    return {nm, vb, t};
  }

  std::pair<NodeExtentMutable&, ValueDeltaRecorder*>
  do_prepare_mutate_payload(Transaction&);

  NodeExtentManager& nm;
  const ValueBuilder& vb;
  Ref<tree_cursor_t> p_cursor;

  template <typename ValueImpl>
  friend class Btree;
};

/**
 * ValueBuilder
 *
 * For tree nodes to build values without the need to depend on the actual
 * implementation.
 */
struct ValueBuilder {
  virtual value_magic_t get_header_magic() const = 0;
  virtual string_size_t get_max_ns_size() const = 0;
  virtual string_size_t get_max_oid_size() const = 0;
  virtual value_size_t get_max_value_payload_size() const = 0;
  virtual extent_len_t get_internal_node_size() const = 0;
  virtual extent_len_t get_leaf_node_size() const = 0;
  virtual std::unique_ptr<ValueDeltaRecorder>
  build_value_recorder(ceph::bufferlist&) const = 0;
};

/**
 * ValueBuilderImpl
 *
 * The concrete ValueBuilder implementation in Btree.
 */
template <typename ValueImpl>
struct ValueBuilderImpl final : public ValueBuilder {
  ValueBuilderImpl() {
    validate_tree_config(ValueImpl::TREE_CONF);
  }

  value_magic_t get_header_magic() const {
    return ValueImpl::TREE_CONF.value_magic;
  }
  string_size_t get_max_ns_size() const override {
    return ValueImpl::TREE_CONF.max_ns_size;
  }
  string_size_t get_max_oid_size() const override {
    return ValueImpl::TREE_CONF.max_oid_size;
  }
  value_size_t get_max_value_payload_size() const override {
    return ValueImpl::TREE_CONF.max_value_payload_size;
  }
  extent_len_t get_internal_node_size() const override {
    return ValueImpl::TREE_CONF.internal_node_size;
  }
  extent_len_t get_leaf_node_size() const override {
    return ValueImpl::TREE_CONF.leaf_node_size;
  }

  std::unique_ptr<ValueDeltaRecorder>
  build_value_recorder(ceph::bufferlist& encoded) const override {
    std::unique_ptr<ValueDeltaRecorder> ret =
      std::make_unique<typename ValueImpl::Recorder>(encoded);
    assert(ret->get_header_magic() == get_header_magic());
    return ret;
  }

  ValueImpl build_value(NodeExtentManager& nm,
                        const ValueBuilder& vb,
                        Ref<tree_cursor_t>& p_cursor) const {
    assert(vb.get_header_magic() == get_header_magic());
    return ValueImpl(nm, vb, p_cursor);
  }
};

void validate_tree_config(const tree_conf_t& conf);

/**
 * Get the value recorder by type (the magic value) when the ValueBuilder is
 * unavailable.
 */
std::unique_ptr<ValueDeltaRecorder>
build_value_recorder_by_type(ceph::bufferlist& encoded, const value_magic_t& magic);

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::onode::value_config_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::onode::value_header_t> : fmt::ostream_formatter {};
#endif
