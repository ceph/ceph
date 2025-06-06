// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "Types.h"
#include "common/ceph_context.h"
#include "common/Formatter.h"
#include "include/Context.h"
#include "include/stringify.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl::Types: " << this << " " \
                           <<  __func__ << ": "
using ceph::Formatter;

namespace librbd {
namespace cache {
namespace pwl {

DeferredContexts::~DeferredContexts() {
  finish_contexts(nullptr, contexts, 0);
}

void DeferredContexts::add(Context* ctx) {
  contexts.push_back(ctx);
}

/*
 * A BlockExtent identifies a range by first and last.
 *
 * An Extent ("image extent") identifies a range by start and length.
 *
 * The ImageDispatch interface is defined in terms of image extents, and
 * requires no alignment of the beginning or end of the extent. We
 * convert between image and block extents here using a "block size"
 * of 1.
 */
BlockExtent convert_to_block_extent(uint64_t offset_bytes, uint64_t length_bytes)
{
  return BlockExtent(offset_bytes,
                     offset_bytes + length_bytes);
}

BlockExtent WriteLogCacheEntry::block_extent() {
  return convert_to_block_extent(image_offset_bytes, write_bytes);
}

uint64_t WriteLogCacheEntry::get_offset_bytes() {
  return image_offset_bytes;
}

uint64_t WriteLogCacheEntry::get_write_bytes() {
  return write_bytes;
}

#ifdef WITH_RBD_SSD_CACHE
void WriteLogCacheEntry::dump(Formatter *f) const {
  f->dump_unsigned("sync_gen_number", sync_gen_number);
  f->dump_unsigned("write_sequence_number", write_sequence_number);
  f->dump_unsigned("image_offset_bytes", image_offset_bytes);
  f->dump_unsigned("write_bytes", write_bytes);
  f->dump_unsigned("write_data_pos", write_data_pos);
  f->dump_bool("entry_valid", is_entry_valid());
  f->dump_bool("sync_point", is_sync_point());
  f->dump_bool("sequenced", is_sequenced());
  f->dump_bool("has_data", has_data());
  f->dump_bool("discard", is_discard());
  f->dump_bool("writesame", is_writesame());
  f->dump_unsigned("ws_datalen", ws_datalen);
  f->dump_unsigned("entry_index", entry_index);
}

void WriteLogCacheEntry::generate_test_instances(std::list<WriteLogCacheEntry*>& ls) {
  ls.push_back(new WriteLogCacheEntry());
  ls.push_back(new WriteLogCacheEntry);
  ls.back()->sync_gen_number = 1;
  ls.back()->write_sequence_number = 1;
  ls.back()->image_offset_bytes = 1;
  ls.back()->write_bytes = 1;
  ls.back()->write_data_pos = 1;
  ls.back()->set_entry_valid(true);
  ls.back()->set_sync_point(true);
  ls.back()->set_sequenced(true);
  ls.back()->set_has_data(true);
  ls.back()->set_discard(true);
  ls.back()->set_writesame(true);
  ls.back()->ws_datalen = 1;
  ls.back()->entry_index = 1;
}

void WriteLogPoolRoot::dump(Formatter *f) const {
  f->dump_unsigned("layout_version", layout_version);
  f->dump_unsigned("cur_sync_gen", cur_sync_gen);
  f->dump_unsigned("pool_size", pool_size);
  f->dump_unsigned("flushed_sync_gen", flushed_sync_gen);
  f->dump_unsigned("block_size", block_size);
  f->dump_unsigned("num_log_entries", num_log_entries);
  f->dump_unsigned("first_free_entry", first_free_entry);
  f->dump_unsigned("first_valid_entry", first_valid_entry);
}

void WriteLogPoolRoot::generate_test_instances(std::list<WriteLogPoolRoot*>& ls) {
  ls.push_back(new WriteLogPoolRoot());
  ls.push_back(new WriteLogPoolRoot);
  ls.back()->layout_version = 2;
  ls.back()->cur_sync_gen = 1;
  ls.back()->pool_size = 1024;
  ls.back()->flushed_sync_gen = 1;
  ls.back()->block_size = 4096;
  ls.back()->num_log_entries = 10000000;
  ls.back()->first_free_entry = 1;
  ls.back()->first_valid_entry = 0;
}
#endif

std::ostream& operator<<(std::ostream& os,
                         const WriteLogCacheEntry &entry) {
  os << "entry_valid=" << entry.is_entry_valid()
     << ", sync_point=" << entry.is_sync_point()
     << ", sequenced=" << entry.is_sequenced()
     << ", has_data=" << entry.has_data()
     << ", discard=" << entry.is_discard()
     << ", writesame=" << entry.is_writesame()
     << ", sync_gen_number=" << entry.sync_gen_number
     << ", write_sequence_number=" << entry.write_sequence_number
     << ", image_offset_bytes=" << entry.image_offset_bytes
     << ", write_bytes=" << entry.write_bytes
     << ", ws_datalen=" << entry.ws_datalen
     << ", entry_index=" << entry.entry_index;
  return os;
}

template <typename ExtentsType>
ExtentsSummary<ExtentsType>::ExtentsSummary(const ExtentsType &extents)
  : total_bytes(0), first_image_byte(0), last_image_byte(0)
{
  if (extents.empty()) return;
  /* These extents refer to image offsets between first_image_byte
   * and last_image_byte, inclusive, but we don't guarantee here
   * that they address all of those bytes. There may be gaps. */
  first_image_byte = extents.front().first;
  last_image_byte = first_image_byte + extents.front().second;
  for (auto &extent : extents) {
    /* Ignore zero length extents */
    if (extent.second) {
      total_bytes += extent.second;
      if (extent.first < first_image_byte) {
        first_image_byte = extent.first;
      }
      if ((extent.first + extent.second) > last_image_byte) {
        last_image_byte = extent.first + extent.second;
      }
    }
  }
}

io::Extent whole_volume_extent() {
  return io::Extent({0, std::numeric_limits<uint64_t>::max()});
}

BlockExtent block_extent(const io::Extent& image_extent) {
  return convert_to_block_extent(image_extent.first, image_extent.second);
}

Context * override_ctx(int r, Context *ctx) {
  if (r < 0) {
    /* Override next_ctx status with this error */
    return new LambdaContext(
      [r, ctx](int _r) {
        ctx->complete(r);
      });
  } else {
    return ctx;
  }
}

std::string unique_lock_name(const std::string &name, void *address) {
  return name + " (" + stringify(address) + ")";
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ExtentsSummary<librbd::io::Extents>;
