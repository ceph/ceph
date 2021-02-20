// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <variant>

#include "common/hobject.h"
#include "crimson/common/layout.h"
#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/seastore_types.h"
#include "onode_delta.h"

namespace asci = absl::container_internal;

namespace boost::beast {
  template<class T>
  bool operator==(const span<T>& lhs, const span<T>& rhs) {
    return std::equal(
        lhs.begin(), lhs.end(),
        rhs.begin(), rhs.end());
  }
}

// on-disk onode
// it only keeps the bits necessary to rebuild an in-memory onode
struct [[gnu::packed]] onode_t {
  onode_t& operator=(const onode_t& onode) {
    len = onode.len;
    std::memcpy(data, onode.data, len);
    return *this;
  }
  size_t size() const {
    return sizeof(*this) + len;
  }
  OnodeRef decode() const {
    return new crimson::os::seastore::Onode(std::string_view{data, len});
  }
  uint8_t struct_v = 1;
  uint8_t struct_compat = 1;
  // TODO:
  // - use uint16_t for length, as the size of an onode should be less
  //   than a block (16K for now)
  // - drop struct_len
  uint32_t struct_len = 0;
  uint32_t len;
  char data[];
};

static inline std::ostream& operator<<(std::ostream& os, const onode_t& onode) {
  return os << *onode.decode();
}

using crimson::os::seastore::laddr_t;

struct [[gnu::packed]] child_addr_t {
  laddr_t data;
  child_addr_t(laddr_t data)
    : data{data}
  {}
  child_addr_t& operator=(laddr_t addr) {
    data = addr;
    return *this;
  }
  laddr_t get() const {
    return data;
  }
  operator laddr_t() const {
    return data;
  }
  size_t size() const {
    return sizeof(laddr_t);
  }
};

// poor man's operator<=>
enum class ordering_t {
  less,
  equivalent,
  greater,
};

template<class L, class R>
ordering_t compare_element(const L& x, const R& y)
{
  if constexpr (std::is_arithmetic_v<L>) {
    static_assert(std::is_arithmetic_v<R>);
    if (x < y) {
      return ordering_t::less;
    } else if (x > y) {
      return ordering_t::greater;
    } else {
      return ordering_t::equivalent;
    }
  } else {
    // string_view::compare(), string::compare(), ...
    auto result = x.compare(y);
    if (result < 0) {
      return ordering_t::less;
    } else if (result > 0) {
      return ordering_t::greater;
    } else {
      return ordering_t::equivalent;
    }
  }
}

template<typename L, typename R>
constexpr ordering_t tuple_cmp(const L&, const R&, std::index_sequence<>)
{
  return ordering_t::equivalent;
}

template<typename L, typename R,
         size_t Head, size_t... Tail>
constexpr ordering_t tuple_cmp(const L& x, const R& y,
                               std::index_sequence<Head, Tail...>)
{
  auto ordering = compare_element(std::get<Head>(x), std::get<Head>(y));
  if (ordering != ordering_t::equivalent) {
    return ordering;
  } else {
    return tuple_cmp(x, y, std::index_sequence<Tail...>());
  }
}

template<typename... Ls, typename... Rs>
constexpr ordering_t cmp(const std::tuple<Ls...>& x,
                         const std::tuple<Rs...>& y)
{
  static_assert(sizeof...(Ls) == sizeof...(Rs));
  return tuple_cmp(x, y, std::index_sequence_for<Ls...>());
}

enum class likes_t {
  yes,
  no,
  maybe,
};

struct [[gnu::packed]] variable_key_suffix {
  uint64_t snap;
  uint64_t gen;
  uint8_t nspace_len;
  uint8_t name_len;
  char data[];
  struct index_t {
    enum {
      nspace_data = 0,
      name_data = 1,
    };
  };
  using layout_type = asci::Layout<char, char>;
  layout_type cell_layout() const {
    return layout_type{nspace_len, name_len};
  }
  void set(const ghobject_t& oid) {
    snap = oid.hobj.snap;
    gen = oid.generation;
    nspace_len = oid.hobj.nspace.size();
    name_len = oid.hobj.oid.name.size();
    auto layout = cell_layout();
    std::memcpy(layout.Pointer<index_t::nspace_data>(data),
                oid.hobj.nspace.data(), oid.hobj.nspace.size());
    std::memcpy(layout.Pointer<index_t::name_data>(data),
                oid.hobj.oid.name.data(), oid.hobj.oid.name.size());
  }

  void update_oid(ghobject_t& oid) const {
    oid.hobj.snap = snap;
    oid.generation = gen;
    oid.hobj.nspace = nspace();
    oid.hobj.oid.name = name();
  }

  variable_key_suffix& operator=(const variable_key_suffix& key) {
    snap = key.snap;
    gen = key.gen;
    auto layout = cell_layout();
    auto nspace = key.nspace();
    std::copy_n(nspace.data(), nspace.size(),
                layout.Pointer<index_t::nspace_data>(data));
    auto name = key.name();
    std::copy_n(name.data(), name.size(),
                layout.Pointer<index_t::name_data>(data));
    return *this;
  }
  const std::string_view nspace() const {
    auto layout = cell_layout();
    auto nspace = layout.Slice<index_t::nspace_data>(data);
    return {nspace.data(), nspace.size()};
  }
  const std::string_view name() const {
    auto layout = cell_layout();
    auto name = layout.Slice<index_t::name_data>(data);
    return {name.data(), name.size()};
  }
  size_t size() const {
    return sizeof(*this) + nspace_len + name_len;
  }
  static size_t size_from(const ghobject_t& oid) {
    return (sizeof(variable_key_suffix) +
            oid.hobj.nspace.size() +
            oid.hobj.oid.name.size());
  }
  ordering_t compare(const ghobject_t& oid) const {
    return cmp(std::tie(nspace(), name(), snap, gen),
               std::tie(oid.hobj.nspace, oid.hobj.oid.name, oid.hobj.snap.val,
                        oid.generation));
  }
  bool likes(const variable_key_suffix& key) const {
    return nspace() == key.nspace() && name() == key.name();
  }
};

static inline std::ostream& operator<<(std::ostream& os, const variable_key_suffix& k) {
  if (k.snap != CEPH_NOSNAP) {
    os << "s" << k.snap << ",";
  }
  if (k.gen != ghobject_t::NO_GEN) {
    os << "g" << k.gen << ",";
  }
  return os << k.nspace() << "/" << k.name();
}

// should use [[no_unique_address]] in C++20
struct empty_key_suffix {
  static constexpr ordering_t compare(const ghobject_t&) {
    return ordering_t::equivalent;
  }
  static void set(const ghobject_t&) {}
  static constexpr size_t size() {
    return 0;
  }
  static size_t size_from(const ghobject_t&) {
    return 0;
  }
  static void update_oid(ghobject_t&) {}
};

static inline std::ostream& operator<<(std::ostream& os, const empty_key_suffix&)
{
  return os;
}

enum class ntype_t : uint8_t {
  leaf = 0u,
  inner,
};

constexpr ntype_t flip_ntype(ntype_t ntype) noexcept
{
  if (ntype == ntype_t::leaf) {
    return ntype_t::inner;
  } else {
    return ntype_t::leaf;
  }
}

template<int N, ntype_t NodeType>
struct FixedKeyPrefix {};

template<ntype_t NodeType>
struct FixedKeyPrefix<0, NodeType>
{
  static constexpr bool item_in_key = false;
  int8_t shard = -1;
  int64_t pool = -1;
  uint32_t hash = 0;
  uint16_t offset = 0;

  FixedKeyPrefix() = default;
  FixedKeyPrefix(const ghobject_t& oid, uint16_t offset)
    : shard{oid.shard_id},
      pool{oid.hobj.pool},
      hash{oid.hobj.get_hash()},
      offset{offset}
  {}

  void set(const ghobject_t& oid, uint16_t new_offset) {
    shard = oid.shard_id;
    pool = oid.hobj.pool;
    hash = oid.hobj.get_hash();
    offset = new_offset;
  }

  void set(const FixedKeyPrefix& k, uint16_t new_offset) {
    shard = k.shard;
    pool = k.pool;
    hash = k.hash;
    offset = new_offset;
  }

  void update(const ghobject_t& oid) {
    shard = oid.shard_id;
    pool = oid.hobj.pool;
    hash = oid.hobj.get_hash();
  }

  void update_oid(ghobject_t& oid) const {
    oid.set_shard(shard_id_t{shard});
    oid.hobj.pool = pool;
    oid.hobj.set_hash(hash);
  }

  ordering_t compare(const ghobject_t& oid) const {
    // so std::tie() can bind them  by reference
    int8_t rhs_shard = oid.shard_id;
    uint32_t rhs_hash = oid.hobj.get_hash();
    return cmp(std::tie(shard, pool, hash),
               std::tie(rhs_shard, oid.hobj.pool, rhs_hash));
  }
  // @return true if i likes @c k, we will can be pushed down to next level
  //              in the same node
  likes_t likes(const FixedKeyPrefix& k) const {
    if (shard == k.shard && pool == k.pool) {
      return likes_t::yes;
    } else {
      return likes_t::no;
    }
  }
};

template<ntype_t NodeType>
std::ostream& operator<<(std::ostream& os, const FixedKeyPrefix<0, NodeType>& k) {
  if (k.shard != shard_id_t::NO_SHARD) {
    os << "s" << k.shard;
  }
  return os << "p=" << k.pool << ","
            << "h=" << std::hex << k.hash << std::dec << ","
            << ">" << k.offset;
}

// all elements in this node share the same <shard, pool>
template<ntype_t NodeType>
struct FixedKeyPrefix<1, NodeType> {
  static constexpr bool item_in_key = false;
  uint32_t hash = 0;
  uint16_t offset = 0;

  FixedKeyPrefix() = default;
  FixedKeyPrefix(uint32_t hash, uint16_t offset)
    : hash{hash},
      offset{offset}
  {}
  FixedKeyPrefix(const ghobject_t& oid, uint16_t offset)
    : FixedKeyPrefix(oid.hobj.get_hash(), offset)
  {}
  void set(const ghobject_t& oid, uint16_t new_offset) {
    hash = oid.hobj.get_hash();
    offset = new_offset;
  }
  template<int N>
  void set(const FixedKeyPrefix<N, NodeType>& k, uint16_t new_offset) {
    static_assert(N < 2, "only N0, N1 have hash");
    hash = k.hash;
    offset = new_offset;
  }
  void update_oid(ghobject_t& oid) const {
    oid.hobj.set_hash(hash);
  }
  void update(const ghobject_t& oid) {
    hash = oid.hobj.get_hash();
  }
  ordering_t compare(const ghobject_t& oid) const {
    return compare_element(hash, oid.hobj.get_hash());
  }
  likes_t likes(const FixedKeyPrefix& k) const {
    return hash == k.hash ? likes_t::yes : likes_t::no;
  }
};

template<ntype_t NodeType>
std::ostream& operator<<(std::ostream& os, const FixedKeyPrefix<1, NodeType>& k) {
  return os << "0x" << std::hex << k.hash << std::dec << ","
            << ">" << k.offset;
}

// all elements in this node must share the same <shard, pool, hash>
template<ntype_t NodeType>
struct FixedKeyPrefix<2, NodeType> {
  static constexpr bool item_in_key = false;
  uint16_t offset = 0;

  FixedKeyPrefix() = default;

  static constexpr ordering_t compare(const ghobject_t& oid) {
    // need to compare the cell
    return ordering_t::equivalent;
  }
  // always defer to my cell for likeness
  constexpr likes_t likes(const FixedKeyPrefix&) const {
    return likes_t::maybe;
  }
  void set(const ghobject_t&, uint16_t new_offset) {
    offset = new_offset;
  }
  template<int N>
  void set(const FixedKeyPrefix<N, NodeType>&, uint16_t new_offset) {
    offset = new_offset;
  }
  void update(const ghobject_t&) {}
  void update_oid(ghobject_t&) const {}
};

template<ntype_t NodeType>
std::ostream& operator<<(std::ostream& os, const FixedKeyPrefix<2, NodeType>& k) {
  return os << ">" << k.offset;
}

struct fixed_key_3 {
  uint64_t snap = 0;
  uint64_t gen = 0;

  fixed_key_3() = default;
  fixed_key_3(const ghobject_t& oid)
  : snap{oid.hobj.snap}, gen{oid.generation}
  {}
  ordering_t compare(const ghobject_t& oid) const {
    return cmp(std::tie(snap, gen),
               std::tie(oid.hobj.snap.val, oid.generation));
  }
  // no object likes each other at this level
  constexpr likes_t likes(const fixed_key_3&) const {
    return likes_t::no;
  }
  void update_with_oid(const ghobject_t& oid) {
    snap = oid.hobj.snap;
    gen = oid.generation;
  }
  void update_oid(ghobject_t& oid) const {
    oid.hobj.snap = snap;
    oid.generation = gen;
  }
};

static inline std::ostream& operator<<(std::ostream& os, const fixed_key_3& k) {
  if (k.snap != CEPH_NOSNAP) {
    os << "s" << k.snap << ",";
  }
  if (k.gen != ghobject_t::NO_GEN) {
    os << "g" << k.gen << ",";
  }
  return os;
}

// all elements in this node must share the same <shard, pool, hash, namespace, oid>
// but the unlike other FixedKeyPrefix<>, a node with FixedKeyPrefix<3> does not have
// variable_sized_key, so if it is an inner node, we can just embed the child
// addr right in the key.
template<>
struct FixedKeyPrefix<3, ntype_t::inner> : public fixed_key_3 {
  // the item is embedded in the key
  static constexpr bool item_in_key = true;
  laddr_t child_addr = 0;

  FixedKeyPrefix() = default;
  void set(const ghobject_t& oid, laddr_t new_child_addr) {
    update_with_oid(oid);
    child_addr = new_child_addr;
  }
  // unlikely get called, though..
  void update(const ghobject_t& oid) {}
  template<int N>
  std::enable_if_t<N < 3> set(const FixedKeyPrefix<N, ntype_t::inner>&,
                              laddr_t new_child_addr) {
    child_addr = new_child_addr;
  }
  void set(const FixedKeyPrefix& k, laddr_t new_child_addr) {
    snap = k.snap;
    gen = k.gen;
    child_addr = new_child_addr;
  }
  void set(const variable_key_suffix& k, laddr_t new_child_addr) {
    snap = k.snap;
    gen = k.gen;
    child_addr = new_child_addr;
  }
};

template<>
struct FixedKeyPrefix<3, ntype_t::leaf> : public fixed_key_3 {
  static constexpr bool item_in_key = false;
  uint16_t offset = 0;

  FixedKeyPrefix() = default;
  void set(const ghobject_t& oid, uint16_t new_offset) {
    update_with_oid(oid);
    offset = new_offset;
  }
  void set(const FixedKeyPrefix& k, uint16_t new_offset) {
    snap = k.snap;
    gen = k.gen;
    offset = new_offset;
  }
  template<int N>
  std::enable_if_t<N < 3> set(const FixedKeyPrefix<N, ntype_t::leaf>&,
                              uint16_t new_offset) {
    offset = new_offset;
  }
};

struct tag_t {
  template<int N, ntype_t node_type>
  static constexpr tag_t create() {
    static_assert(std::clamp(N, 0, 3) == N);
    return tag_t{N, static_cast<uint8_t>(node_type)};
  }
  bool is_leaf() const {
    return type() == ntype_t::leaf;
  }
  int layout() const {
    return layout_type;
  }
  ntype_t type() const {
    return ntype_t{node_type};
  }
  int layout_type : 4;
  uint8_t node_type : 4;
};

static inline std::ostream& operator<<(std::ostream& os, const tag_t& tag) {
  return os << "n=" << tag.layout() << ", leaf=" << tag.is_leaf();
}

// for calculating size of variable-sized item/key
template<class T>
size_t size_of(const T& t) {
  using decayed_t = std::decay_t<T>;
  if constexpr (std::is_scalar_v<decayed_t>) {
    return sizeof(decayed_t);
  } else {
    return t.size();
  }
}

enum class size_state_t {
  okay,
  underflow,
  overflow,
};

// layout of a node of B+ tree
//
// it is different from a typical B+ tree in following ways
// - the size of keys is not necessarily fixed, neither is the size of value.
// - the max number of elements in a node is determined by the total size of
//   the keys and values in the node
// - in internal nodes, each key maps to the logical address of the child
//   node whose minimum key is greater or equal to that key.
template<size_t BlockSize,
         int N,
         ntype_t NodeType>
struct node_t {
  static_assert(std::clamp(N, 0, 3) == N);
  constexpr static ntype_t node_type = NodeType;
  constexpr static int node_n = N;

  using key_prefix_t = FixedKeyPrefix<N, NodeType>;
  using item_t = std::conditional_t<NodeType == ntype_t::leaf,
                                    onode_t,
                                    child_addr_t>;
  using const_item_t = std::conditional_t<NodeType == ntype_t::leaf,
                                          const onode_t&,
                                          child_addr_t>;
  static constexpr bool item_in_key = key_prefix_t::item_in_key;
  using key_suffix_t = std::conditional_t<N < 3,
                                           variable_key_suffix,
                                           empty_key_suffix>;

  std::pair<const key_prefix_t&, const key_suffix_t&>
  key_at(unsigned slot) const;

  // update an existing oid with the specified item
  ghobject_t get_oid_at(unsigned slot, const ghobject_t& oid) const;
  const_item_t item_at(const key_prefix_t& key) const;
  void dump(std::ostream& os) const;

  // for debugging only.
  static constexpr bool is_leaf() {
    return node_type == ntype_t::leaf;
  }

  bool _is_leaf() const {
    return tag.is_leaf();
  }

  char* from_end(uint16_t offset);
  const char* from_end(uint16_t offset) const;
  uint16_t used_space() const;
  uint16_t free_space() const {
    return capacity() - used_space();
  }
  static uint16_t capacity();
  // TODO: if it's allowed to update 2 siblings at the same time, we can have
  //       B* tree
  static constexpr uint16_t min_size();


  // calculate the allowable bounds on bytes to remove from an overflow node
  // with specified size
  // @param size the overflowed size
  // @return <minimum bytes to grab, maximum bytes to grab>
  static constexpr std::pair<int16_t, int16_t> bytes_to_remove(uint16_t size);

  // calculate the allowable bounds on bytes to add to an underflow node
  // with specified size
  // @param size the underflowed size
  // @return <minimum bytes to push, maximum bytes to push>
  static constexpr std::pair<int16_t, int16_t> bytes_to_add(uint16_t size);

  size_state_t size_state(uint16_t size) const;
  bool is_underflow(uint16_t size) const;
  int16_t size_with_key(unsigned slot, const ghobject_t& oid) const;
  ordering_t compare_with_slot(unsigned slot, const ghobject_t& oid) const;
  /// return the slot number of the first slot that is greater or equal to
  /// key
  std::pair<unsigned, bool> lower_bound(const ghobject_t& oid) const;
  static uint16_t size_of_item(const ghobject_t& oid, const item_t& item);
  bool is_overflow(const ghobject_t& oid, const item_t& item) const;
  bool is_overflow(const ghobject_t& oid, const OnodeRef& item) const;

  // inserts an item into the given slot, pushing all subsequent keys forward
  // @note if the item is not embedded in key, shift the right half as well
  void insert_at(unsigned slot, const ghobject_t& oid, const item_t& item);
  // used by InnerNode for updating the keys indexing its children when their lower boundaries
  // is updated
  void update_key_at(unsigned slot, const ghobject_t& oid);
  // try to figure out the number of elements and total size when trying to
  // rebalance by moving the elements from the front of this node when its
  // left sibling node is underflow
  //
  // @param min_grab lower bound of the number of bytes to move
  // @param max_grab upper bound of the number of bytes to move
  // @return the number of element to grab
  // @note return {0, 0} if current node would be underflow if
  //       @c min_grab bytes of elements are taken from it
  std::pair<unsigned, uint16_t> calc_grab_front(uint16_t min_grab, uint16_t max_grab) const;
  // try to figure out the number of elements and their total size when trying to
  // rebalance by moving the elements from the end of this node when its right
  // sibling node is underflow
  //
  // @param min_grab lower bound of the number of bytes to move
  // @param max_grab upper bound of the number of bytes to move
  // @return the number of element to grab
  // @note return {0, 0} if current node would be underflow if
  //       @c min_grab bytes of elements are taken from it
  std::pair<unsigned, uint16_t> calc_grab_back(uint16_t min_grab, uint16_t max_grab) const;
  template<int LeftN, class Mover> void grab_from_left(
    node_t<BlockSize, LeftN, NodeType>& left,
    unsigned n, uint16_t bytes,
    Mover& mover);
  template<int RightN, class Mover>
  delta_t acquire_right(node_t<BlockSize, RightN, NodeType>& right,
                        unsigned whoami, Mover& mover);
  // transfer n elements at the front of given node to me
  template<int RightN, class Mover>
  void grab_from_right(node_t<BlockSize, RightN, NodeType>& right,
                       unsigned n, uint16_t bytes,
                       Mover& mover);
  template<int LeftN, class Mover>
  void push_to_left(node_t<BlockSize, LeftN, NodeType>& left,
                    unsigned n, uint16_t bytes,
                    Mover& mover);
  template<int RightN, class Mover>
  void push_to_right(node_t<BlockSize, RightN, NodeType>& right,
                     unsigned n, uint16_t bytes,
                     Mover& mover);
  // [to, from) are removed, so we need to shift left
  // actually there are only two use cases:
  // - to = 0: for giving elements in bulk
  // - to = from - 1: for removing a single element
  // old: |////|.....|   |.....|/|........|
  // new: |.....|        |.....||........|
  void shift_left(unsigned from, unsigned to);
  void insert_front(const ceph::bufferptr& keys_buf, const ceph::bufferptr& cells_buf);
  void insert_back(const ceph::bufferptr& keys_buf, const ceph::bufferptr& cells_buf);
  // one or more elements are inserted, so we need to shift the elements right
  // actually there are only two use cases:
  // - bytes != 0: for inserting bytes before from
  // - bytes = 0: for inserting a single element before from
  // old: ||.....|
  // new: |/////|.....|
  void shift_right(unsigned n, unsigned bytes);
  // shift all keys after slot is removed.
  // @note if the item is not embdedded in key, all items sitting at the left
  //       side of it will be shifted right
  void remove_from(unsigned slot);
  void trim_right(unsigned n);
  void play_delta(const delta_t& delta);
  //         /-------------------------------|
  //         |                               V
  // |header|k0|k1|k2|...  | / / |k2'v2|k1'v1|k0'.v0| v_m |
  //        |<-- count  -->|
  tag_t tag = tag_t::create<N, NodeType>();
  // the count of values in the node
  uint16_t count = 0;
  key_prefix_t keys[];
};

template<class parent_t,
         class from_t,
         class to_t,
         typename=void>
class EntryMover {
public:
  // a "trap" mover
  EntryMover(const parent_t&, from_t&, to_t& dst, unsigned) {
    assert(0);
  }
  void move_from(unsigned, unsigned, unsigned) {
    assert(0);
  }
  delta_t get_delta() {
    return delta_t::nop();
  }
};

// lower the layout, for instance, from L0 to L1, no reference oid is used
template<class parent_t,
         class from_t,
         class to_t>
class EntryMover<parent_t,
                 from_t,
                 to_t,
                 std::enable_if_t<from_t::node_n < to_t::node_n>>
{
public:
  EntryMover(const parent_t&, from_t& src, to_t& dst, unsigned)
    : src{src}, dst{dst}
  {}
  void move_from(unsigned src_first, unsigned dst_first, unsigned n)
  {
    ceph::bufferptr keys_buf{n * sizeof(to_t::key_prefix_t)};
    ceph::bufferptr cells_buf;
    auto dst_keys = reinterpret_cast<typename to_t::key_prefix_t*>(keys_buf.c_str());
    if constexpr (to_t::item_in_key) {
      for (unsigned i = 0; i < n; i++) {
        const auto& [prefix, suffix] = src.key_at(src_first + i);
        dst_keys[i].set(suffix, src.item_at(prefix));
      }
    } else {
      // copy keys
      uint16_t src_offset = src_first > 0 ? src.keys[src_first - 1].offset : 0;
      uint16_t dst_offset = dst_first > 0 ? dst.keys[dst_first - 1].offset : 0;
      for (unsigned i = 0; i < n; i++) {
        auto& src_key = src.keys[src_first + i];
        uint16_t offset = src_key.offset - src_offset + dst_offset;
        dst_keys[i].set(src_key, offset);
      }
      // copy cells in bulk, yay!
      auto src_end = src.keys[src_first + n - 1].offset;
      uint16_t total_cell_size = src_end - src_offset;
      cells_buf = ceph::bufferptr{total_cell_size};
      cells_buf.copy_in(0, total_cell_size, src.from_end(src_end));
    }
    if (dst_first == dst.count) {
      dst_delta = delta_t::insert_back(keys_buf, cells_buf);
    } else {
      dst_delta = delta_t::insert_front(keys_buf, cells_buf);
    }
    if (src_first > 0 && src_first + n == src.count) {
      src_delta = delta_t::trim_right(src_first);
    } else if (src_first == 0 && n < src.count) {
      src_delta = delta_t::shift_left(n);
    } else if (src_first == 0 && n == src.count) {
      // the caller will retire the src extent
    } else {
      // grab in the middle?
      assert(0);
    }
  }

  delta_t from_delta() {
    return std::move(src_delta);
  }
  delta_t to_delta() {
    return std::move(dst_delta);
  }
private:
  const from_t& src;
  const to_t& dst;
  delta_t dst_delta;
  delta_t src_delta;
};

// lift the layout, for instance, from L2 to L0, need a reference oid
template<class parent_t,
         class from_t,
         class to_t>
class EntryMover<parent_t, from_t, to_t,
                 std::enable_if_t<(from_t::node_n > to_t::node_n)>>
{
public:
  EntryMover(const parent_t& parent, from_t& src, to_t& dst, unsigned from_slot)
    : src{src}, dst{dst}, ref_oid{parent->get_oid_at(from_slot, {})}
  {}
  void move_from(unsigned src_first, unsigned dst_first, unsigned n)
  {
    ceph::bufferptr keys_buf{n * sizeof(to_t::key_prefix_t)};
    ceph::bufferptr cells_buf;
    auto dst_keys = reinterpret_cast<typename to_t::key_prefix_t*>(keys_buf.c_str());
    uint16_t in_node_offset = dst_first > 0 ? dst.keys[dst_first - 1].offset : 0;
    static_assert(!std::is_same_v<typename to_t::key_suffix_t, empty_key_suffix>);
    // copy keys
    uint16_t buf_offset = 0;
    for (unsigned i = 0; i < n; i++) {
      auto& src_key = src.keys[src_first + i];
      if constexpr (std::is_same_v<typename from_t::key_suffix_t, empty_key_suffix>) {
        // heterogeneous partial key, have to rebuild dst partial key from oid
        src_key.update_oid(ref_oid);
        const auto& src_item = src.item_at(src_key);
        size_t key2_size = to_t::key_suffix_t::size_from(ref_oid);
        buf_offset += key2_size + size_of(src_item);
        dst_keys[i].set(ref_oid, in_node_offset + buf_offset);
        auto p = from_end(cells_buf, buf_offset);
        auto partial_key = reinterpret_cast<typename to_t::key_suffix_t*>(p);
        partial_key->set(ref_oid);
        p += key2_size;
        auto dst_item = reinterpret_cast<typename to_t::item_t*>(p);
        *dst_item = src_item;
      } else {
        // homogeneous partial key, just update the pointers
        uint16_t src_offset = src_first > 0 ? src.keys[src_first - 1].offset : 0;
        uint16_t dst_offset = dst_first > 0 ? dst.keys[dst_first - 1].offset : 0;
        uint16_t offset = src_key.offset - src_offset + dst_offset;
        dst_keys[i].set(ref_oid, in_node_offset + offset);
      }
    }
    if constexpr (std::is_same_v<typename to_t::key_suffix_t,
                                 typename from_t::key_suffix_t>) {
      // copy cells in bulk, yay!
      uint16_t src_offset = src_first > 0 ? src.keys[src_first - 1].offset : 0;
      uint16_t src_end = src.keys[src_first + n - 1].offset;
      uint16_t total_cell_size = src_end - src_offset;
      cells_buf.copy_in(0,  total_cell_size, src.from_end(src_end));
    }
    if (dst_first == dst.count) {
      dst_delta = delta_t::insert_back(keys_buf, cells_buf);
    } else {
      dst_delta = delta_t::insert_front(keys_buf, cells_buf);
    }
    if (src_first + n == src.count && src_first > 0) {
      src_delta = delta_t::trim_right(src_first);
    } else {
      // the caller will retire the src extent
      assert(src_first == 0);
    }
  }

  delta_t from_delta() {
    return std::move(src_delta);
  }
  delta_t to_delta() {
    return std::move(dst_delta);
  }
private:
  char* from_end(ceph::bufferptr& ptr, uint16_t offset) {
    return ptr.end_c_str() - static_cast<int>(offset);
  }
private:
  const from_t& src;
  const to_t& dst;
  delta_t dst_delta;
  delta_t src_delta;
  ghobject_t ref_oid;
};

// identical layout, yay!
template<class parent_t,
         class child_t>
class EntryMover<parent_t, child_t, child_t>
{
public:
  EntryMover(const parent_t&, child_t& src, child_t& dst, unsigned)
    : src{src}, dst{dst}
  {}

  void move_from(unsigned src_first, unsigned dst_first, unsigned n)
  {
    ceph::bufferptr keys_buf{static_cast<unsigned>(n * sizeof(typename child_t::key_prefix_t))};
    ceph::bufferptr cells_buf;
    auto dst_keys = reinterpret_cast<typename child_t::key_prefix_t*>(keys_buf.c_str());

    // copy keys
    std::copy(src.keys + src_first, src.keys + src_first + n,
              dst_keys);
    if constexpr (!child_t::item_in_key) {
      uint16_t src_offset = src_first > 0 ? src.keys[src_first - 1].offset : 0;
      uint16_t dst_offset = dst_first > 0 ? dst.keys[dst_first - 1].offset : 0;
      const int offset_delta = dst_offset - src_offset;
      // update the pointers
      for (unsigned i = 0; i < n; i++) {
        dst_keys[i].offset += offset_delta;
      }
      // copy cells in bulk, yay!
      auto src_end = src.keys[src_first + n - 1].offset;
      uint16_t total_cell_size = src_end - src_offset;
      cells_buf = ceph::bufferptr{total_cell_size};
      cells_buf.copy_in(0,  total_cell_size, src.from_end(src_end));
    }
    if (dst_first == dst.count) {
      dst_delta = delta_t::insert_back(std::move(keys_buf), std::move(cells_buf));
    } else {
      dst_delta = delta_t::insert_front(std::move(keys_buf), std::move(cells_buf));
    }
    if (src_first + n == src.count && src_first > 0) {
      src_delta = delta_t::trim_right(n);
    } else if (src_first == 0 && n < src.count) {
      src_delta = delta_t::shift_left(n);
    } else if (src_first == 0 && n == src.count) {
      // the caller will retire the src extent
    } else {
      // grab in the middle?
      assert(0);
    }
  }

  delta_t from_delta() {
    return std::move(src_delta);
  }

  delta_t to_delta() {
    return std::move(dst_delta);
  }
private:
  char* from_end(ceph::bufferptr& ptr, uint16_t offset) {
    return ptr.end_c_str() - static_cast<int>(offset);
  }
private:
  const child_t& src;
  const child_t& dst;
  delta_t src_delta;
  delta_t dst_delta;
};

template<class parent_t, class from_t, class to_t>
EntryMover<parent_t, from_t, to_t>
make_mover(const parent_t& parent, from_t& src, to_t& dst, unsigned from_slot) {
  return EntryMover<parent_t, from_t, to_t>(parent, src, dst, from_slot);
}
