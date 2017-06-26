// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "rgw/rgw_compression.h"

class ut_get_sink : public RGWGetDataCB {
  bufferlist sink;
public:
  ut_get_sink() {}
  virtual ~ut_get_sink() {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override
  {
    auto bl_buffers = bl.buffers();
    auto i = bl_buffers.begin();
    while (bl_len > 0)
    {
      assert(i != bl_buffers.end());
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

class ut_get_sink_size : public RGWGetDataCB {
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

class ut_put_sink: public RGWPutObjDataProcessor
{
  bufferlist sink;
public:
  ut_put_sink(){}
  virtual ~ut_put_sink(){}
  int handle_data(bufferlist& bl, off_t ofs, void **phandle, rgw_raw_obj *pobj, bool *again) override
  {
    sink.append(bl);
    *again = false;
    return 0;
  }
  int throttle_data(void *handle, const rgw_raw_obj& obj, uint64_t size, bool need_to_wait) override
  {
    return 0;
  }
  bufferlist&  get_sink()
  {
    return sink;
  }
};


struct MockGetDataCB : public RGWGetDataCB {
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

    void* handle;
    rgw_raw_obj obj;
    bool again = false;

    ut_put_sink c_sink;
    RGWPutObj_Compress compressor(g_ceph_context, plugin, &c_sink);
    compressor.handle_data(bl, 0, &handle, &obj, &again);

    bufferlist empty;
    compressor.handle_data(empty, s, &handle, &obj, &again);

    RGWCompressionInfo cs_info;
    cs_info.compression_type = plugin->get_type_name();
    cs_info.orig_size = s;
    cs_info.blocks = move(compressor.get_compression_blocks());

    ut_get_sink_size d_sink;
    RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, false, &d_sink);

    off_t f_begin = 0;
    off_t f_end = s - 1;
    decompress.fixup_range(f_begin, f_end);

    decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
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

  void* handle;
  rgw_raw_obj obj;
  bool again = false;

  for (int i=0; i<1000;i++)
    compressor.handle_data(bl, size*i, &handle, &obj, &again);

  bufferlist empty;
  compressor.handle_data(empty, size*1000, &handle, &obj, &again);

  RGWCompressionInfo cs_info;
  cs_info.compression_type = plugin->get_type_name();
  cs_info.orig_size = size*1000;
  cs_info.blocks = move(compressor.get_compression_blocks());

  ut_get_sink d_sink;
  RGWGetObj_Decompress decompress(g_ceph_context, &cs_info, false, &d_sink);

  off_t f_begin = 0;
  off_t f_end = size*1000 - 1;
  decompress.fixup_range(f_begin, f_end);

  decompress.handle_data(c_sink.get_sink(), 0, c_sink.get_sink().length());
  decompress.handle_data(empty, 0, 0);

  ASSERT_EQ(d_sink.get_sink().length() , size*1000);
}
