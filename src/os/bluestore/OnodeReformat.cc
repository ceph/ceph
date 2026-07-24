// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/ceph_context.h"
#include "common/dout.h"

#include "include/intarith.h"
#include "include/ceph_assert.h"

#include "OnodeReformat.h"
#include "Allocator.h"

#define dout_subsys ceph_subsys_bluestore

/////////////////////////////////////////
/// OnodeReformatEngine
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatEngine "

bool OnodeReformatEngine::validate(OnodeReformatContext& ctx)
{
  ceph_assert(ctx.store.cct);
  bool will_reformat = false;
  auto min_alloc_size = ctx.store.get_min_alloc_size();
  if (ctx.op_flags & CEPH_OSD_OP_FLAG_SCRUB) {
    // for the sake of simplicity do not apply data reformatting to reads
    // unaligned at the beginning. Having unaligned tail is OK.
    will_reformat = p2nphase(ctx.offset, min_alloc_size) == 0;
    if (!will_reformat) {
      ldout(ctx.store.cct, 15) << "reformat '" << args << "'"
	<< " skipped due to unaligned read bounds "
	<< p2nphase(ctx.offset, min_alloc_size) << " "
	<< p2nphase(ctx.offset + ctx.length, min_alloc_size)
	<< dendl;
    }
    else {
      ldout(ctx.store.cct, 15) << "reformat '" << args << "'"
	<< " enabled "
	<< dendl;
    }
  }
  return will_reformat;
}

/////////////////////////////////////////
/// OnodeReformatRecompressEngine
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatRecompress "

bool OnodeReformatRecompressEngine::execute(OnodeReformatContext& ctx,
  PerfCounters& logger)
{
  ceph_assert(ctx.store.cct);
  auto* c = static_cast<BlueStore::Collection*>(ctx.ch.get());
  ceph_assert(c);

  const auto& span_stat = ctx.get_span_stats();
  auto& wctx = ctx.get_write_context();
  auto min_alloc_size = ctx.store.get_min_alloc_size();
  bool will_do = wctx.compress && span_stat.allocated > 0;
  if (will_do) {
    uint64_t need = 0;
    auto bl_it = ctx.bl.begin();
    uint64_t offs = ctx.offset;
    uint64_t old_allocated = span_stat.allocated + span_stat.allocated_compressed;
    while (bl_it != ctx.bl.end()) {

      BlueStore::BlobRef blob = c->new_blob();
      bufferlist from_bl;
      uint64_t l = std::min(wctx.target_blob_size, uint64_t(bl_it.get_remaining()));
      uint64_t res_len = l;
      if (l >= min_alloc_size) {
	l = p2align(l, min_alloc_size);
	bl_it.copy(l, from_bl);

	//FIXME: add zero detection
	auto& wi = wctx.write(offs, blob, l, 0, from_bl, 0, l, false, true);

	res_len = from_bl.length();
	if (l > min_alloc_size &&
	  wctx.compressor->compress(from_bl, wi.compressed_bl, wi.compressor_message) == 0) {

	  res_len = wi.compressed_bl.length();
	  // don't set wi.compress_len and wi.compressed as this is redundant
	  // at this point, to be assigned in _do_alloc_write if needed.
	}
	ldout(ctx.store.cct, 20) << " reformat: " << " precompress : 0x"
	  << std::hex << offs << "~" << l << "->" << res_len
	  << std::dec << " " << *blob
	  << dendl;
      } else {
	bl_it += l;
	ldout(ctx.store.cct, 20) << " reformat: " << " precompress : 0x"
	  << std::hex << offs << "~" << l << "-> remaining tail"
	  << dendl;
      }
      need += p2roundup(res_len, min_alloc_size);
      offs += l;
      will_do = need < old_allocated;
    }

    // At this point will_do indicates if we definitely want recompression,
    // will skip the remaining reformatting then.

    // Keep compressed blobs until final processing no matter if we decided to
    // enforce recompression or not. In the latter case they can be chosen
    // for different engine optimization(s) or be rejected prior to writing out.
    wctx.precompressed = true;
    ldout(ctx.store.cct, 10) << " reformat:'" << args << "'"
      << " need 0x"
      << std::hex << need << " vs. old_allocated 0x" << old_allocated << std::dec
      << " apply: " << will_do
      << dendl;

    logger.inc(l_bluestore_reformat_compress_attempted);
    if (!will_do)
      logger.inc(l_bluestore_reformat_compress_omitted);
  }
  return will_do;
}

/////////////////////////////////////////
/// OnodeReformatDefragmentEnging
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatDefragment "

bool OnodeReformatDefragmentEngine::execute(OnodeReformatContext& ctx,
  PerfCounters& logger)
{
  ceph_assert(ctx.store.cct);
  auto *c = static_cast<BlueStore::Collection*>(ctx.ch.get());
  ceph_assert(c);

  auto min_alloc_size = ctx.store.get_min_alloc_size();

  bool will_do = false;
  const auto& span_stat = ctx.get_span_stats();
  auto need = p2roundup(ctx.length, min_alloc_size);
  size_t frags = 0;
  int64_t allocated = 0;
  if (span_stat.frags > 1) {
    logger.inc(l_bluestore_reformat_defragment_attempted);
    will_do = ctx.maybe_allocate(need, min_alloc_size,
      [&](int64_t num_bytes, size_t num_frags) {
	allocated = num_bytes;
	frags = num_frags;
	return allocated >= (int64_t)need && frags < span_stat.frags;
      });
    if (!will_do) {
      logger.inc(l_bluestore_reformat_defragment_omitted);
    }
  }
  ldout(ctx.store.cct, 10) << " reformat:'" << args << "'"
    << " preallocated: 0x" << std::hex << allocated << std::dec
    << " old frags:" << span_stat.frags
    << " new frags:" << frags
    << " apply: " << will_do
    << dendl;
  return will_do;
};

/////////////////////////////////////////
/// OnodeReformatContext
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatContext "

OnodeReformatContext::OnodeReformatContext(const BlueStore::read_context_t& _ctx,
					   const reformat_engines_t& _engines)
  : read_context_t(_ctx)
{
  // Choose the engines that should be offered to execution
  for (size_t i = 0; i < _engines.size(); i++) {
    if (_engines[i] && _engines[i]->validate(*this)) {
      engines[i] = _engines[i];
      enabled_engines_count++;
    }
  }
}
OnodeReformatContext::~OnodeReformatContext()
{
  clear();
}
bool OnodeReformatContext::maybe_allocate(size_t need, size_t min_alloc_size,
  std::function<bool(int64_t, size_t)> acceptor)
{
  if (prealloc_slicer) {
    return false; // repetitive assignments aren't allowed
  }
  ceph_assert(store.cct);
  alloc = store.get_allocator();
  ceph_assert(alloc);

  PExtentVector alloc_vector;
  alloc_vector.reserve(need / min_alloc_size + 1);
  auto start = mono_clock::now();
  auto allocated = alloc->allocate(
    need, min_alloc_size, need,
    0, &alloc_vector);
  store.log_latency("allocator@_prepare_reformat",
    l_bluestore_allocator_lat,
    mono_clock::now() - start,
    store.cct->_conf->bluestore_log_op_age);
  bool will_do = acceptor(allocated, alloc_vector.size());
  if (will_do) {
    prealloc_slicer = &wctx.prealloc_slicer;
    prealloc_slicer->setup(std::move(alloc_vector), allocated);
  } else {
    alloc->release(alloc_vector);
  }
  return will_do;
}

void OnodeReformatContext::exec_engines(
  PerfCounters& logger)
{
  // Enumerate all the validated engines and try to execute them
  // in their priority order until the first success indicated
  for (auto& e : engines) {
    if (e.get() && e->execute(*this, logger)) {
      to_be_applied = true;
      break;
    }
  }
}
void OnodeReformatContext::clear()
{
  PExtentVector to_release;
  if (prealloc_slicer && alloc && !prealloc_slicer->end() && prealloc_slicer->slice(to_release) > 0) {
    alloc->release(to_release);
  }
  for (auto& e : engines) {
    e.reset();
  }
  enabled_engines_count = 0;
  to_be_applied = false;
  wctx.reset();
  alloc = nullptr;
}
