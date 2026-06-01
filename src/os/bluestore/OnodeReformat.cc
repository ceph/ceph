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
/// OnodeReformatBasicValidateAction
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatBasicValidate "

bool OnodeReformatBasicValidateAction::call(OnodeReformatContext* ctx,
  std::string_view args)
{
  ceph_assert(ctx);
  ceph_assert(ctx->store);
  ceph_assert(ctx->store->cct);
  bool will_reformat = false;
  if (op_flags & CEPH_OSD_OP_FLAG_SCRUB) {
    // for the sake of simplicity do not apply data reformatting to reads
    // unaligned at the beginning. Having unaligned tail is OK.
    will_reformat = p2nphase(offset, min_alloc_size) == 0;
    if (!will_reformat) {
      ldout(ctx->store->cct, 15) << "reformat '" << args << "'"
	<< " skipped due to unaligned read bounds "
	<< p2nphase(offset, min_alloc_size) << " "
	<< p2nphase(offset + length, min_alloc_size)
	<< dendl;
    } else {
      ldout(ctx->store->cct, 15) << "reformat '" << args << "'"
	<< " enabled "
	<< dendl;
    }
  }
  return will_reformat;
}

/////////////////////////////////////////
/// OnodeReformatRecompressAction
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatRecompress "

bool OnodeReformatRecompressAction::call(OnodeReformatContext* ctx,
  std::string_view args)
{
  ceph_assert(ctx);
  ceph_assert(ctx->store);
  ceph_assert(ctx->store->cct);
  ceph_assert(ctx->logger);

  const auto& span_stat = ctx->get_span_stats();
  auto& wctx = ctx->get_write_context();
  bool will_do = wctx.compress && span_stat.allocated > 0;
  if (will_do) {
    uint64_t need = 0;
    auto bl_it = bl.begin();
    uint64_t offs = offset;
    uint64_t old_allocated = span_stat.allocated + span_stat.allocated_compressed;
    while (bl_it != bl.end()) {

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
	ldout(ctx->store->cct, 20) << " reformat: " << " precompress : 0x"
	  << std::hex << offs << "~" << l << "->" << res_len
	  << std::dec << " " << *blob
	  << dendl;
      } else {
	bl_it += l;
	ldout(ctx->store->cct, 20) << " reformat: " << " precompress : 0x"
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
    ldout(ctx->store->cct, 10) << " reformat:'" << args << "'"
      << " need 0x"
      << std::hex << need << " vs. old_allocated 0x" << old_allocated << std::dec
      << " apply: " << will_do
      << dendl;

    ctx->logger->inc(l_bluestore_reformat_compress_attempted);
    if (!will_do)
      ctx->logger->inc(l_bluestore_reformat_compress_omitted);
  }
  return will_do;
}

/////////////////////////////////////////
/// OnodeReformatDefragmentAction
/////////////////////////////////////////
#undef dout_prefix
#define dout_prefix *_dout << "OnodeReformatDefragment "

bool OnodeReformatDefragmentAction::call(OnodeReformatContext* ctx,
  std::string_view args)
{
  ceph_assert(ctx);
  ceph_assert(ctx->store);
  ceph_assert(ctx->store->cct);
  ceph_assert(ctx->logger);

  bool will_do = false;
  const auto& span_stat = ctx->get_span_stats();
  auto need = p2roundup(length, min_alloc_size);
  size_t frags = 0;
  int64_t allocated = 0;
  if (span_stat.frags > 1) {
    ctx->logger->inc(l_bluestore_reformat_defragment_attempted);
    will_do = ctx->maybe_allocate(need, min_alloc_size,
      [&](int64_t num_bytes, size_t num_frags) {
	allocated = num_bytes;
	frags = num_frags;
	return allocated >= (int64_t)need && frags < span_stat.frags;
      });
    if (!will_do) {
      ctx->logger->inc(l_bluestore_reformat_defragment_omitted);
    }
  }
  ldout(ctx->store->cct, 10) << " reformat:'" << args << "'"
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

OnodeReformatContext::OnodeReformatContext(BlueStore* store,
  PerfCounters* _logger)
  : store(store)
{
  ceph_assert(store);

  alloc = store->get_allocator();
  logger = _logger;

  ceph_assert(alloc);
  ceph_assert(logger);
}
OnodeReformatContext::~OnodeReformatContext()
{
  clear();
}
void OnodeReformatContext::add_engine(int pri,
  OnodeReformatAction* _validate,
  OnodeReformatAction* _execute)
{
  ceph_assert(pri >= 0 && pri < MAX_ENGINES);
  OnodeReformatEngine e(_validate, _execute);
  std::swap(engines[pri], e);
}
bool OnodeReformatContext::maybe_allocate(size_t need, size_t min_alloc_size,
  std::function<bool(int64_t, size_t)> acceptor)
{
  if (prealloc_slicer) {
    return false; // repetitive assignments aren't allowed
  }
  PExtentVector alloc_vector;
  alloc_vector.reserve(need / min_alloc_size + 1);
  auto start = mono_clock::now();
  auto allocated = alloc->allocate(
    need, min_alloc_size, need,
    0, &alloc_vector);
  store->log_latency("allocator@_prepare_reformat",
    l_bluestore_allocator_lat,
    mono_clock::now() - start,
    store->cct->_conf->bluestore_log_op_age);
  bool will_do = acceptor(allocated, alloc_vector.size());
  if (will_do) {
    prealloc_slicer = &wctx.prealloc_slicer;
    prealloc_slicer->setup(std::move(alloc_vector), allocated);
  }
  else {
    alloc->release(alloc_vector);
  }
  return will_do;
}
void OnodeReformatContext::maybe_enable_engine(int pri, std::string_view args)
{
  ceph_assert(pri >= 0 && pri < MAX_ENGINES);
  if (!engines[pri].enabled && engines[pri].validate->call(this, args)) {
    engines[pri].enabled = true;
    engines[pri].args = args;
    enabled_engines_count++;
  }
}
void OnodeReformatContext::exec_engines()
{
  for (auto& e : engines) {
    if (e.enabled && e.execute->call(this, e.args)) {
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
  enabled_engines_count = 0;
  for (auto& e : engines) {
    OnodeReformatEngine e0;
    std::swap(e, e0);
  }
  to_be_applied = false;
  wctx.reset();
}
