#include "onode_node.h"

template<size_t BlockSize, int N, ntype_t NodeType>
auto node_t<BlockSize, N, NodeType>::key_at(unsigned slot) const
  -> std::pair<const key_prefix_t&, const key_suffix_t&>
{
  auto& key = keys[slot];
  if constexpr (item_in_key) {
    return {key, key_suffix_t{}};
  } else {
    auto p = from_end(key.offset);
    return {key, *reinterpret_cast<const key_suffix_t*>(p)};
  }
}

// update an existing oid with the specified item
template<size_t BlockSize, int N, ntype_t NodeType>
ghobject_t
node_t<BlockSize, N, NodeType>::get_oid_at(unsigned slot,
                                           const ghobject_t& oid) const
{
  auto [prefix, suffix] = key_at(slot);
  ghobject_t updated = oid;
  prefix.update_oid(updated);
  suffix.update_oid(updated);
  return updated;
}

template<size_t BlockSize, int N, ntype_t NodeType>
auto node_t<BlockSize, N, NodeType>::item_at(const key_prefix_t& key) const
  -> const_item_t
{
  if constexpr (item_in_key) {
    return key.child_addr;
  } else {
    assert(key.offset < BlockSize);
    auto p = from_end(key.offset);
    auto partial_key = reinterpret_cast<const key_suffix_t*>(p);
    p += size_of(*partial_key);
    return *reinterpret_cast<const item_t*>(p);
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::dump(std::ostream& os) const
{
  for (uint16_t i = 0; i < count; i++) {
    const auto& [prefix, suffix] = key_at(i);
    os << " [" << i << '/' << count - 1 << "]\n"
       << "  key1 = (" << prefix << ")\n"
       << "  key2 = (" << suffix << ")\n";
    const auto& item = item_at(prefix);
    if (_is_leaf()) {
      os << " item = " << item << "\n";
    } else {
      os << " child = " << std::hex << item << std::dec << "\n";
    }
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
char* node_t<BlockSize, N, NodeType>::from_end(uint16_t offset)
{
  auto end = reinterpret_cast<char*>(this) + BlockSize;
  return end - static_cast<int>(offset);
}

template<size_t BlockSize, int N, ntype_t NodeType>
const char* node_t<BlockSize, N, NodeType>::from_end(uint16_t offset) const
{
  auto end = reinterpret_cast<const char*>(this) + BlockSize;
  return end - static_cast<int>(offset);
}

template<size_t BlockSize, int N, ntype_t NodeType>
uint16_t node_t<BlockSize, N, NodeType>::used_space() const
{
  if constexpr (item_in_key) {
    return count * sizeof(key_prefix_t);
  } else {
    if (count) {
      return keys[count - 1].offset + count * sizeof(key_prefix_t);
    } else {
      return 0;
    }
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
uint16_t node_t<BlockSize, N, NodeType>::capacity()
{
  auto p = reinterpret_cast<node_t*>(0);
  return BlockSize - (reinterpret_cast<char*>(p->keys) -
                      reinterpret_cast<char*>(p));
}

// TODO: if it's allowed to update 2 siblings at the same time, we can have
//       B* tree
template<size_t BlockSize, int N, ntype_t NodeType>
constexpr uint16_t node_t<BlockSize, N, NodeType>::min_size()
{
  return capacity() / 2;
}

template<size_t BlockSize, int N, ntype_t NodeType>
constexpr std::pair<int16_t, int16_t>
node_t<BlockSize, N, NodeType>::bytes_to_add(uint16_t size)
{
  assert(size < min_size());
  return {min_size() - size, capacity() - size};
}

template<size_t BlockSize, int N, ntype_t NodeType>
constexpr std::pair<int16_t, int16_t>
node_t<BlockSize, N, NodeType>::bytes_to_remove(uint16_t size)
{
  assert(size > capacity());
  return {size - capacity(), size - min_size()};
}

template<size_t BlockSize, int N, ntype_t NodeType>
size_state_t node_t<BlockSize, N, NodeType>::size_state(uint16_t size) const
{
  if (size > capacity()) {
    return size_state_t::overflow;
  } else if (size < capacity() / 2) {
    return size_state_t::underflow;
  } else {
    return size_state_t::okay;
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
bool node_t<BlockSize, N, NodeType>::is_underflow(uint16_t size) const
{
  switch (size_state(size)) {
  case size_state_t::underflow:
    return true;
  case size_state_t::okay:
    return false;
  default:
    assert(0);
    return false;
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
int16_t node_t<BlockSize, N, NodeType>::size_with_key(unsigned slot,
                                                      const ghobject_t& oid) const
{
  if constexpr (item_in_key) {
    return capacity();
  } else {
    // the size of fixed key does not change
    [[maybe_unused]] const auto& [prefix, suffix] = key_at(slot);
    return capacity() + key_suffix_t::size_from(oid) - suffix.size();
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
ordering_t node_t<BlockSize, N, NodeType>::compare_with_slot(unsigned slot,
                                                             const ghobject_t& oid) const
{
  const auto& [prefix, suffix] = key_at(slot);
  if (auto result = prefix.compare(oid); result != ordering_t::equivalent) {
    return result;
  } else {
    return suffix.compare(oid);
  }
}

/// return the slot number of the first slot that is greater or equal to
/// key
template<size_t BlockSize, int N, ntype_t NodeType>
std::pair<unsigned, bool> node_t<BlockSize, N, NodeType>::lower_bound(const ghobject_t& oid) const
{
  unsigned s = 0, e = count;
  while (s != e) {
    unsigned mid = (s + e) / 2;
    switch (compare_with_slot(mid, oid)) {
    case ordering_t::less:
      s = ++mid;
      break;
    case ordering_t::greater:
      e = mid;
      break;
    case ordering_t::equivalent:
      assert(mid == 0 || mid < count);
      return {mid, true};
    }
  }
  return {s, false};
}

template<size_t BlockSize, int N, ntype_t NodeType>
uint16_t node_t<BlockSize, N, NodeType>::size_of_item(const ghobject_t& oid,
                                                      const item_t& item)
{
  if constexpr (item_in_key) {
    return sizeof(key_prefix_t);
  } else {
    return (sizeof(key_prefix_t) +
            key_suffix_t::size_from(oid) + size_of(item));
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
bool node_t<BlockSize, N, NodeType>::is_overflow(const ghobject_t& oid,
                                                 const item_t& item) const
{
  return free_space() < size_of_item(oid, item);
}

template<size_t BlockSize, int N, ntype_t NodeType>
bool node_t<BlockSize, N, NodeType>::is_overflow(const ghobject_t& oid,
                                                 const OnodeRef& item) const
{
  return free_space() < (sizeof(key_prefix_t) + key_suffix_t::size_from(oid) + item->size());
}

// inserts an item into the given slot, pushing all subsequent keys forward
// @note if the item is not embedded in key, shift the right half as well
template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::insert_at(unsigned slot,
                                               const ghobject_t& oid,
                                               const item_t& item)
{
  assert(!is_overflow(oid, item));
  assert(slot <= count);
  if constexpr (item_in_key) {
    // shift the keys right
    key_prefix_t* key = keys + slot;
    key_prefix_t* last_key = keys + count;
    std::copy_backward(key, last_key, last_key + 1);
    key->set(oid, item);
  } else {
    const uint16_t size = key_suffix_t::size_from(oid) + size_of(item);
    uint16_t offset = size;
    if (slot > 0) {
      offset += keys[slot - 1].offset;
    }
    if (slot < count) {
      //                                 V
      // |         |... //    ...|//////||    |
      // |         |... // ...|//////|   |    |
      // shift the partial keys and items left
      auto first = keys[slot - 1].offset;
      auto last = keys[count - 1].offset;
      std::memmove(from_end(last + size), from_end(last), last - first);
      // shift the keys right and update the pointers
      for (key_prefix_t* dst = keys + count; dst > keys + slot; dst--) {
        key_prefix_t* src = dst - 1;
        *dst = *src;
        dst->offset += size;
      }
    }
    keys[slot].set(oid, offset);
    auto p = from_end(offset);
    auto partial_key = reinterpret_cast<key_suffix_t*>(p);
    partial_key->set(oid);
    p += size_of(*partial_key);
    auto item_ptr = reinterpret_cast<item_t*>(p);
    *item_ptr = item;
  }
  count++;
  assert(used_space() <= capacity());
}

// used by InnerNode for updating the keys indexing its children when their lower boundaries
// is updated
template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::update_key_at(unsigned slot, const ghobject_t& oid)
{
  if constexpr (is_leaf()) {
    assert(0);
  } else if constexpr (item_in_key) {
    keys[slot].update(oid);
  } else {
    const auto& [prefix, suffix] = key_at(slot);
    int16_t delta = key_suffix_t::size_from(oid) - suffix.size();
    if (delta > 0) {
      // shift the cells sitting at its left side
      auto first = keys[slot].offset;
      auto last = keys[count - 1].offset;
      std::memmove(from_end(last + delta), from_end(last), last - first);
      // update the pointers
      for (key_prefix_t* key = keys + slot; key < keys + count; key++) {
        key->offset += delta;
      }
    }
    keys[slot].update(oid);
    auto p = from_end(keys[slot].offset);
    auto partial_key = reinterpret_cast<key_suffix_t*>(p);
    partial_key->set(oid);
    // we don't update item here
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
std::pair<unsigned, uint16_t>
node_t<BlockSize, N, NodeType>::calc_grab_front(uint16_t min_grab,
                                                uint16_t max_grab) const
{
  // TODO: split by likeness
  uint16_t grabbed = 0;
  uint16_t used = used_space();
  int n = 0;
  for (; n < count; n++) {
    const auto& [prefix, suffix] = key_at(n);
    uint16_t to_grab = sizeof(prefix) + size_of(suffix);
    if constexpr (!item_in_key) {
      const auto& item = item_at(prefix);
      to_grab += size_of(item);
    }
    if (grabbed + to_grab > max_grab) {
      break;
    }
    grabbed += to_grab;
  }
  if (grabbed >= min_grab) {
    if (n == count) {
      return {n, grabbed};
    } else if (!is_underflow(used - grabbed)) {
      return {n, grabbed};
    }
  }
  return {0, 0};
}

template<size_t BlockSize, int N, ntype_t NodeType>
std::pair<unsigned, uint16_t>
node_t<BlockSize, N, NodeType>::calc_grab_back(uint16_t min_grab,
                                               uint16_t max_grab) const
{
  // TODO: split by likeness
  uint16_t grabbed = 0;
  uint16_t used = used_space();
  for (int i = count - 1; i >= 0; i--) {
    const auto& [prefix, suffix] = key_at(i);
    uint16_t to_grab = sizeof(prefix) + size_of(suffix);
    if constexpr (!item_in_key) {
      const auto& item = item_at(prefix);
      to_grab += size_of(item);
    }
    grabbed += to_grab;
    if (is_underflow(used - grabbed)) {
      return {0, 0};
    } else if (grabbed > max_grab) {
      return {0, 0};
    } else if (grabbed >= min_grab) {
      return {i + 1, grabbed};
    }
  }
  return {0, 0};
}

template<size_t BlockSize, int N, ntype_t NodeType>
template<int LeftN, class Mover>
void node_t<BlockSize, N, NodeType>::grab_from_left(node_t<BlockSize, LeftN, NodeType>& left,
                                                    unsigned n, uint16_t bytes,
                                                    Mover& mover)
{
  // TODO: rebuild keys if moving across different layouts
  //       group by likeness
  shift_right(n, bytes);
  mover.move_from(left.count - n, 0, n);
}

template<size_t BlockSize, int N, ntype_t NodeType>
template<int RightN, class Mover>
delta_t node_t<BlockSize, N, NodeType>::acquire_right(node_t<BlockSize, RightN, NodeType>& right,
                                                      unsigned whoami, Mover& mover)
{
  mover.move_from(0, count, right.count);
  return mover.to_delta();
}

template<size_t BlockSize, int N, ntype_t NodeType>
template<int RightN, class Mover>
void node_t<BlockSize, N, NodeType>::grab_from_right(node_t<BlockSize, RightN, NodeType>& right,
                                                     unsigned n, uint16_t bytes,
                                                     Mover& mover)
{
  mover.move_from(0, count, n);
  right.shift_left(n, 0);
}

template<size_t BlockSize, int N, ntype_t NodeType>
template<int LeftN, class Mover>
void node_t<BlockSize, N, NodeType>::push_to_left(node_t<BlockSize, LeftN, NodeType>& left,
                                                  unsigned n, uint16_t bytes,
                                                  Mover& mover)
{
  left.grab_from_right(*this, n, bytes, mover);
}

template<size_t BlockSize, int N, ntype_t NodeType>
template<int RightN, class Mover>
void node_t<BlockSize, N, NodeType>::push_to_right(node_t<BlockSize, RightN, NodeType>& right,
                                                   unsigned n, uint16_t bytes,
                                                   Mover& mover)
{
  right.grab_from_left(*this, n, bytes, mover);
}

// [to, from) are removed, so we need to shift left
// actually there are only two use cases:
// - to = 0: for giving elements in bulk
// - to = from - 1: for removing a single element
// old: |////|.....|   |.....|/|........|
// new: |.....|        |.....||........|
template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::shift_left(unsigned from, unsigned to)
{
  assert(from < count);
  assert(to < from);
  if constexpr (item_in_key) {
    std::copy(keys + from, keys + count, keys + to);
  } else {
    const uint16_t cell_hi = keys[count - 1].offset;
    const uint16_t cell_lo = keys[from - 1].offset;
    const uint16_t offset_delta = keys[from].offset - keys[to].offset;
    for (auto src_key = keys + from, dst_key = keys + to;
         src_key != keys + count;
         ++src_key, ++dst_key) {
      // shift the keys left
      *dst_key = *src_key;
      // update the pointers
      dst_key->offset -= offset_delta;
    }
    // and cells
    auto dst = from_end(cell_hi);
    std::memmove(dst + offset_delta, dst, cell_hi - cell_lo);
  }
  count -= (from - to);
}

template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::insert_front(const ceph::bufferptr& keys_buf,
                                                  const ceph::bufferptr& cells_buf)
{
  unsigned n = keys_buf.length() / sizeof(key_prefix_t);
  shift_right(n, cells_buf.length());
  keys_buf.copy_out(0, keys_buf.length(), reinterpret_cast<char*>(keys));
  if constexpr (item_in_key) {
    assert(cells_buf.length() == 0);
  } else {
    cells_buf.copy_out(0, cells_buf.length(), from_end(keys[n - 1].offset));
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::insert_back(const ceph::bufferptr& keys_buf,
                                                 const ceph::bufferptr& cells_buf)
{
  keys_buf.copy_out(0, keys_buf.length(), reinterpret_cast<char*>(keys + count));
  count += keys_buf.length() / sizeof(key_prefix_t);
  if constexpr (item_in_key) {
    assert(cells_buf.length() == 0);
  } else {
    cells_buf.copy_out(0, cells_buf.length(), from_end(keys[count - 1].offset));
  }
}

// one or more elements are inserted, so we need to shift the elements right
// actually there are only two use cases:
// - bytes != 0: for inserting bytes before from
// - bytes = 0: for inserting a single element before from
// old: ||.....|
// new: |/////|.....|
template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::shift_right(unsigned n, unsigned bytes)
{
  assert(bytes + used_space() < capacity());
  // shift the keys left
  std::copy_backward(keys, keys + count, keys + count + n);
  count += n;
  if constexpr (!item_in_key) {
    uint16_t cells = keys[count - 1].offset;
    // copy the partial keys and items
    std::memmove(from_end(cells + bytes), from_end(cells), cells);
    // update the pointers
    for (auto key = keys + n; key < keys + count; ++key) {
      key->offset += bytes;
    }
  }
}

// shift all keys after slot is removed.
// @note if the item is not embdedded in key, all items sitting at the left
//       side of it will be shifted right
template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::remove_from(unsigned slot)
{
  assert(slot < count);
  if (unsigned next = slot + 1; next < count) {
    shift_left(next, slot);
  } else {
    // slot is the last one
    count--;
  }
}

template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::trim_right(unsigned n)
{
  count = n;
}

template<size_t BlockSize, int N, ntype_t NodeType>
void node_t<BlockSize, N, NodeType>::play_delta(const delta_t& delta)
{
  switch (delta.op) {
  case delta_t::op_t::insert_onode:
    if constexpr (is_leaf()) {
      auto [slot, found] = lower_bound(delta.oid);
      assert(!found);
      assert(delta.onode->size() <= std::numeric_limits<unsigned>::max());
      ceph::bufferptr buf{static_cast<unsigned>(delta.onode->size())};
      delta.onode->encode(buf.c_str(), buf.length());
      auto onode = reinterpret_cast<const onode_t*>(buf.c_str());
      return insert_at(slot, delta.oid, *onode);
    } else {
      throw std::invalid_argument("wrong node type");
    }
  case delta_t::op_t::update_onode:
    // TODO
    assert(0 == "not implemented");
    break;
  case delta_t::op_t::insert_child:
    if constexpr (is_leaf()) {
      throw std::invalid_argument("wrong node type");
    } else {
      auto [slot, found] = lower_bound(delta.oid);
      assert(!found);
      insert_at(slot, delta.oid, delta.addr);
    }
  case delta_t::op_t::update_key:
    if constexpr (is_leaf()) {
      throw std::invalid_argument("wrong node type");
    } else {
      return update_key_at(delta.n, delta.oid);
    }
  case delta_t::op_t::shift_left:
    return shift_left(delta.n, 0);
  case delta_t::op_t::trim_right:
    return trim_right(delta.n);
  case delta_t::op_t::insert_front:
    return insert_front(delta.keys, delta.cells);
  case delta_t::op_t::insert_back:
    return insert_back(delta.keys, delta.cells);
  case delta_t::op_t::remove_from:
    return remove_from(delta.n);
  default:
    assert(0 == "unknown onode delta");
  }
}

// explicit instantiate the node_t classes used by test_node.cc
template class node_t<512, 0, ntype_t::inner>;
template class node_t<512, 0, ntype_t::leaf>;
template class node_t<512, 1, ntype_t::inner>;
template class node_t<512, 1, ntype_t::leaf>;
template class node_t<512, 2, ntype_t::inner>;
template class node_t<512, 2, ntype_t::leaf>;
template class node_t<512, 3, ntype_t::inner>;
template class node_t<512, 3, ntype_t::leaf>;
