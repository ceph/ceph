// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "rgw_compression.h"

class ut_get_sink : public RGWGetObj_Filter {
  bufferlist sink;
public:
  ut_get_sink() {}
  virtual ~ut_get_sink() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    auto& bl_buffers = bl.buffers();
    auto i = bl_buffers.begin();
    while (bl_len > 0)
    {
      ceph_assert(i != bl_buffers.end());
      off_t len = std::min<off_t>(bl_len, i->length());
      sink.append(*i, 0, len);
      bl_len -= len;
      i++;
    }
    return 0;
  }
  bufferlist& get_sink()
  {
    return sink;
  }
};

class ut_get_sink_size : public RGWGetObj_Filter {
  size_t max_size = 0;
public:
  ut_get_sink_size() {}
  virtual ~ut_get_sink_size() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    if (bl_len > (off_t)max_size)
      max_size = bl_len;
    return 0;
  }
  size_t get_size()
  {
    return max_size;
  }
};

class ut_put_sink: public rgw::sal::DataProcessor
{
  bufferlist sink;
public:
  int process(bufferlist&& bl, uint64_t ofs) override
  {
    sink.claim_append(bl);
    return 0;
  }
  bufferlist&  get_sink()
  {
    return sink;
  }
};


struct MockGetDataCB : public RGWGetObj_Filter {
  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    return 0;
  }
} cb;

using range_t = std::pair<off_t, off_t>;

// call filter->fixup_range() and return the range as a pair. this makes it easy
// to fit on a single line for ASSERT_EQ()
range_t fixup_range(RGWGetObj_Decompress *filter, off_t ofs, off_t end)
{
  filter->fixup_range(ofs, end);
  return {ofs, end};
}


TEST(Decompress, FixupRangePartial)
{
  RGWCompressionInfo cs_info;

  // array of blocks with original len=8, compressed to len=6
  auto& blocks = cs_info.blocks;
  blocks.emplace_back(compression_block{0, 0, 6});
  blocks.emplace_back(compression_block{8, 6, 6});
  blocks.emplace_back(compression_block{16, 12, 6});
  blocks.emplace_back(compression_block{24, 18, 6});

  const bool partial = true;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, partial, &cb);

  // test translation from logical ranges to compressed ranges
  ASSERT_EQ(range_t(0, 5), fixup_range(&decompress, 0, 1));
  ASSERT_EQ(range_t(0, 5), fixup_range(&decompress, 1, 7));
  ASSERT_EQ(range_t(0, 11), fixup_range(&decompress, 7, 8));
  ASSERT_EQ(range_t(0, 11), fixup_range(&decompress, 0, 9));
  ASSERT_EQ(range_t(0, 11), fixup_range(&decompress, 7, 9));
  ASSERT_EQ(range_t(6, 11), fixup_range(&decompress, 8, 9));
  ASSERT_EQ(range_t(6, 17), fixup_range(&decompress, 8, 16));
  ASSERT_EQ(range_t(6, 17), fixup_range(&decompress, 8, 17));
  ASSERT_EQ(range_t(12, 23), fixup_range(&decompress, 16, 24));
  ASSERT_EQ(range_t(12, 23), fixup_range(&decompress, 16, 999));
  ASSERT_EQ(range_t(18, 23), fixup_range(&decompress, 998, 999));
}

TEST(Compress, LimitedChunkSize)
{
  CompressorRef plugin;
  plugin = Compressor::create(g_ceph_context, Compressor::COMP_ALG_ZLIB);
  ASSERT_NE(plugin.get(), nullptr);

  for (size_t s = 100 ; s < 10000000 ; s = s*5/4)
  {
    bufferptr bp(s);
    bufferlist bl;
    bl.append(bp);

    ut_put_sink c_sink;
    RGWPutObj_Compress compressor(g_ceph_context, plugin, &c_sink);
    compressor.process(std::move(bl), 0);
    compressor.process({}, s); // flush

    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = s;
    cs_info.compressor_message = compressor.get_compressor_message();
    cs_info.blocks = std::move(compressor.get_compression_blocks());

    ut_get_sink_size d_sink;
    RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, false, &d_sink);

    off_t f_begin = 0;
    off_t f_end = s - 1;
    decompress.fixup_range(f_begin, f_end);

    decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
    bufferlist empty;
    decompress.handle_data(empty, 0, 0);

    ASSERT_LE(d_sink.get_size(), (size_t)g_ceph_context->_conf->rgw_max_chunk_size);
  }
}


TEST(Compress, BillionZeros)
{
  CompressorRef plugin;
  ut_put_sink c_sink;
  plugin = Compressor::create(g_ceph_context, Compressor::COMP_ALG_ZLIB);
  ASSERT_NE(plugin.get(), nullptr);
  RGWPutObj_Compress compressor(g_ceph_context, plugin, &c_sink);

  constexpr size_t size = 1000000;
  bufferptr bp(size);
  bufferlist bl;
  bl.append(bp);

  for (int i=0; i<1000;i++)
    compressor.process(bufferlist{bl}, size*i);
  compressor.process({}, size*1000); // flush

  RGWCompressionInfo cs_info;
  cs_info.compression_type = plugin->get_type_name();
  cs_info.orig_size = size*1000;
  cs_info.compressor_message = compressor.get_compressor_message();
  cs_info.blocks = std::move(compressor.get_compression_blocks());

  ut_get_sink d_sink;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, false, &d_sink);

  off_t f_begin = 0;
  off_t f_end = size*1000 - 1;
  decompress.fixup_range(f_begin, f_end);

  decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
  bufferlist empty;
  decompress.handle_data(empty, 0, 0);

  ASSERT_EQ(d_sink.get_sink().length() , size*1000);
}

// counts the calls it gets and fails once there are far more than a correct
// filter could make, so a filter that loops on zero-length chunks ends the
// test instead of hanging it
class ut_get_sink_count : public ut_get_sink {
public:
  int calls = 0;
  int empty_calls = 0;

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    if (++calls > 1000)
      return -ELOOP;
    if (bl_len == 0)
      ++empty_calls;
    return ut_get_sink::handle_data(bl, bl_ofs, bl_len);
  }
};

// a compression block can be larger than the current rgw_max_chunk_size if the
// option was lowered after the object was written. a range that ends well
// before the end of such a block must not keep calling the next filter once
// the whole range has been sent
TEST(Decompress, RangeInBlockLargerThanChunkSize)
{
  CompressorRef plugin;
  plugin = Compressor::create(g_ceph_context, Compressor::COMP_ALG_ZLIB);
  ASSERT_NE(plugin.get(), nullptr);

  constexpr size_t size = 1024 * 1024;
  bufferlist bl;
  bl.append_zero(size);

  // one process() call writes the whole object as a single block
  ut_put_sink c_sink;
  RGWPutObj_Compress compressor(g_ceph_context, plugin, &c_sink);
  compressor.process(std::move(bl), 0);
  compressor.process({}, size); // flush

  RGWCompressionInfo cs_info;
  cs_info.compression_type = plugin->get_type_name();
  cs_info.orig_size = size;
  cs_info.compressor_message = compressor.get_compressor_message();
  cs_info.blocks = std::move(compressor.get_compression_blocks());
  ASSERT_EQ(1u, cs_info.blocks.size());

  auto& conf = g_ceph_context->_conf;
  const auto old_chunk_size = conf.get_val<Option::size_t>("rgw_max_chunk_size");
  conf.set_val_or_die("rgw_max_chunk_size", "65536");
  conf.apply_changes(nullptr);

  ut_get_sink_count d_sink;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, true, &d_sink);

  off_t f_begin = 0;
  off_t f_end = 99;
  decompress.fixup_range(f_begin, f_end);

  int r = decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
  bufferlist empty;
  if (r == 0)
    r = decompress.handle_data(empty, 0, 0);

  conf.set_val_or_die("rgw_max_chunk_size", std::to_string(old_chunk_size));
  conf.apply_changes(nullptr);

  ASSERT_EQ(0, r);
  ASSERT_EQ(100u, d_sink.get_sink().length());
  ASSERT_EQ(0, d_sink.empty_calls);
}
