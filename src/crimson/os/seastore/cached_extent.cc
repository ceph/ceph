// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"

#include "crimson/common/log.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

namespace crimson::os::seastore {

#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *ptr)
{
  intrusive_ptr_add_ref(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
    logger().debug("intrusive_ptr_add_ref: {}", *ptr);
}

void intrusive_ptr_release(CachedExtent *ptr)
{
  logger().debug("intrusive_ptr_release: {}", *ptr);
  intrusive_ptr_release(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
}

#endif

bool is_backref_mapped_extent_node(const CachedExtentRef &extent) {
  return extent->is_logical()
    || is_lba_node(extent->get_type())
    || extent->get_type() == extent_types_t::TEST_BLOCK_PHYSICAL;
}

std::ostream &operator<<(std::ostream &out, CachedExtent::extent_state_t state)
{
  switch (state) {
  case CachedExtent::extent_state_t::INITIAL_WRITE_PENDING:
    return out << "INITIAL_WRITE_PENDING";
  case CachedExtent::extent_state_t::MUTATION_PENDING:
    return out << "MUTATION_PENDING";
  case CachedExtent::extent_state_t::CLEAN_PENDING:
    return out << "CLEAN_PENDING";
  case CachedExtent::extent_state_t::CLEAN:
    return out << "CLEAN";
  case CachedExtent::extent_state_t::DIRTY:
    return out << "DIRTY";
  case CachedExtent::extent_state_t::EXIST_CLEAN:
    return out << "EXIST_CLEAN";
  case CachedExtent::extent_state_t::EXIST_MUTATION_PENDING:
    return out << "EXIST_MUTATION_PENDING";
  case CachedExtent::extent_state_t::INVALID:
    return out << "INVALID";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const CachedExtent &ext)
{
  return ext.print(out);
}

CachedExtent::~CachedExtent()
{
  if (parent_index) {
    assert(is_linked());
    parent_index->erase(*this);
  }
}

std::ostream &LogicalCachedExtent::print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr;
  if (pin) {
    out << ", pin=" << *pin;
  } else {
    out << ", pin=empty";
  }
  return print_detail_l(out);
}

std::ostream &operator<<(std::ostream &out, const LBAPin &rhs)
{
  return out << "LBAPin(" << rhs.get_key() << "~" << rhs.get_length()
	     << "->" << rhs.get_val();
}

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << *i;
    first = false;
  }
  return out << ']';
}

void BufferSpace::_add_buffer(extent_len_t offset, ceph::bufferlist&& buffer)
{
  const extent_len_t length_added = buffer.length();
  const extent_len_t tail = offset + length_added;
  auto i = buffer_map.find(tail);
  if(i != buffer_map.end()) {
    buffer.append(*i->second.release());
    buffer_map.erase(i);
    }
  i = buffer_map.lower_bound(offset);
  ceph_assert(i == buffer_map.end() || i->first > tail);
  if (i != buffer_map.begin()) {
	  --i;
  }
  if(i != buffer_map.end() &&
    i->first + i->second->length() == offset) {
    i->second->append(buffer);
  }
  else {
    buffer_map[offset].reset(new bufferlist(std::move(buffer)));
  }
  space_length += length_added;
}

ceph::bufferlist BufferSpace::get_data(extent_len_t offset, extent_len_t length)
{
  auto i = buffer_map.upper_bound(offset);
  --i;
  ceph::bufferlist res;
  res.substr_of(*i->second.get(), offset - i->first, length);
  return res;
}

ceph::bufferptr BufferSpace::build_ptr()
{
  auto it = buffer_map[0].get();
  if(!it->is_contiguous()) {
    it->rebuild();
  }
  ceph::bufferptr ptr(it->buffers().front());
  ptr.set_length(extent_length);
  buffer_map.clear();
  space_length = 0;
  return ptr;
}

bool BufferSpace::check_buffer(extent_len_t offset, extent_len_t length)
{
  auto i = buffer_map.upper_bound(offset);
  if (i == buffer_map.begin()) {
    return false;
  }
  --i;
  if (i-> first + i->second->length() < offset + length) {
    return false;
  }
  return true;
}

region_list_t BufferSpace::read_buffer(extent_len_t offset, extent_len_t length)
{
  auto tail = offset + length;
  ceph_assert(tail <= get_extent_length());
  region_list_t r2r;
  auto i = buffer_map.lower_bound(offset);
  if (i != buffer_map.begin()) {
    --i;
    if(i->first + i->second->length() <= offset) {
      ++i;
    }
  }
  while(offset < tail){
    if (i == buffer_map.end()) {
      r2r.emplace_back(offset, length);
      break;
    }
    auto i_off = i->first;
    auto i_len = i->second->length();
    if (i_off <= offset) {
      extent_len_t skip = i_off + i_len - offset;
	    extent_len_t l = std::min(length, skip);
      offset += l;
	    length -= l;
    }
    else {
      extent_len_t hole = std::min(i_off - offset, length);
      r2r.emplace_back(offset, hole);
      extent_len_t l = std::min(length, i_len + hole);
      offset += l;
      length -= l;
    }
    ++i;
  }
  return r2r;
}

}
